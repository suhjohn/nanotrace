#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { spawn } from "node:child_process";
import path from "node:path";

const root = path.resolve(new URL("..", import.meta.url).pathname);
const args = process.argv.slice(2);

if (args.includes("--help") || args.includes("-h")) {
    usage();
    process.exit(0);
}

loadEnvFile(optionValue("--env") ?? process.env.NANOTRACE_ENV_FILE);

const clickhouseUrl = requiredEnv("CLICKHOUSE_URL");
const user = requiredEnv("CLICKHOUSE_USER");
const password = requiredEnv("CLICKHOUSE_PASSWORD");
const database = identifier(process.env.CLICKHOUSE_DATABASE || "observatory", "CLICKHOUSE_DATABASE");
const eventsTable = identifier(process.env.CLICKHOUSE_TABLE || "events", "CLICKHOUSE_TABLE");
const facetsTable = identifier(process.env.CLICKHOUSE_FACETS_TABLE || "event_facets", "CLICKHOUSE_FACETS_TABLE");
const batchSize = numberOption("--batch-size", numberEnv("NANOTRACE_FACET_BACKFILL_BATCH_SIZE", 50_000));
const sourceMode = optionValue("--source") ?? process.env.NANOTRACE_FACET_BACKFILL_SOURCE ?? "clickhouse";
const s3Bucket = optionValue("--bucket") ?? process.env.NANOTRACE_S3_BUCKET ?? process.env.S3_BUCKET ?? "";
const s3Prefix = optionValue("--prefix") ?? process.env.S3_PREFIX ?? "events/dt=";
const s3Concurrency = numberOption("--concurrency", numberEnv("NANOTRACE_FACET_BACKFILL_CONCURRENCY", 16));
const where = optionValue("--where") ?? process.env.NANOTRACE_FACET_BACKFILL_WHERE ?? "";
const limit = optionValue("--limit") ?? process.env.NANOTRACE_FACET_BACKFILL_LIMIT ?? "";
const truncate = args.includes("--truncate");

if (sourceMode !== "clickhouse" && sourceMode !== "s3") {
    throw new Error("--source must be clickhouse or s3");
}
if (sourceMode === "s3" && !s3Bucket) {
    throw new Error("--bucket, NANOTRACE_S3_BUCKET, or S3_BUCKET is required for --source s3");
}
if (where && !/^\s*(WHERE|PREWHERE)\s+/i.test(where)) {
    throw new Error("--where must start with WHERE or PREWHERE");
}
if (limit && !/^\d+$/.test(limit)) {
    throw new Error("--limit must be an integer");
}

const source = `${quoteIdentifier(database)}.${quoteIdentifier(eventsTable)}`;
const target = `${quoteIdentifier(database)}.${quoteIdentifier(facetsTable)}`;

console.log(`source=${database}.${eventsTable}`);
console.log(`target=${database}.${facetsTable}`);
console.log(`sourceMode=${sourceMode}`);
if (sourceMode === "s3") {
    console.log(`bucket=${s3Bucket}`);
    console.log(`prefix=${s3Prefix}`);
    console.log(`concurrency=${s3Concurrency}`);
}
console.log(`batchSize=${batchSize}`);

if (truncate) {
    console.log(`truncate=${database}.${facetsTable}`);
    await clickhouse(`TRUNCATE TABLE ${target}`);
}

let eventRows = 0;
let facetRows = 0;
let batches = 0;
let pending = new Map();

if (sourceMode === "s3") {
    await backfillFromS3();
} else {
    await backfillFromClickHouse();
}
await flush();

console.log(`backfill complete eventRows=${eventRows} facetRows=${facetRows} batches=${batches}`);

async function backfillFromClickHouse() {
    const query = [
        "SELECT event_id, timestamp, source_file, source_offset, tenant_id, event_type, signal, trace_id, span_id, data",
        `FROM ${source}`,
        where,
        limit ? `LIMIT ${limit}` : "",
        "FORMAT JSONEachRow",
    ].filter(Boolean).join(" ");

    await streamJsonEachRow(query, async (row) => {
        await indexEvent(row);
    });
}

async function backfillFromS3() {
    let objectCount = 0;
    const downloads = new Set();
    for await (const object of listS3Objects(s3Bucket, s3Prefix)) {
        if (!object.Key.endsWith(".ndjson")) {
            continue;
        }
        if (limit && eventRows >= Number(limit)) {
            break;
        }
        let download;
        download = awsStdout(["s3", "cp", `s3://${s3Bucket}/${object.Key}`, "-"]).then((body) => ({
            body,
            download,
            key: object.Key,
        }));
        downloads.add(download);
        if (downloads.size >= s3Concurrency) {
            const result = await Promise.race(downloads);
            downloads.delete(result.download);
            objectCount = await processDownloadedObject(result, objectCount);
        }
    }
    while (downloads.size > 0) {
        const result = await Promise.race(downloads);
        downloads.delete(result.download);
        objectCount = await processDownloadedObject(result, objectCount);
    }
    console.log(`s3Objects=${objectCount}`);
}

async function processDownloadedObject({ body }, objectCount) {
    for (const line of body.split(/\n/)) {
        if (!line.trim()) {
            continue;
        }
        await indexEvent(JSON.parse(line));
        if (limit && eventRows >= Number(limit)) {
            break;
        }
    }
    objectCount += 1;
    if (objectCount % 100 === 0) {
        console.log(`progress objects=${objectCount} eventRows=${eventRows} facetRows=${facetRows} batches=${batches}`);
    }
    return objectCount;
}

async function indexEvent(row) {
    eventRows += 1;
    for (const facet of facetRowsForEvent(row)) {
        const key = aggregateKey(facet);
        const existing = pending.get(key);
        if (existing) {
            existing.count += facet.count;
        } else {
            pending.set(key, facet);
        }
        if (pending.size >= batchSize) {
            await flush();
        }
    }
    if (eventRows % 100_000 === 0) {
        console.log(`progress eventRows=${eventRows} facetRows=${facetRows} batches=${batches}`);
    }
}

async function flush() {
    if (pending.size === 0) {
        return;
    }
    const rows = [...pending.values()];
    const body = `INSERT INTO ${target} FORMAT JSONEachRow\n${rows.map((row) => JSON.stringify(row)).join("\n")}\n`;
    pending = new Map();
    await clickhouse(body);
    facetRows += rows.length;
    batches += 1;
}

function aggregateKey(facet) {
    return `${facet.bucket_time}\0${facet.key}\0${facet.value_type}\0${facet.value}`;
}

async function streamJsonEachRow(sql, onRow) {
    const response = await clickhouseResponse(sql, {
        max_execution_time: "0",
        max_result_rows: "0",
        max_bytes_to_read: String(numberEnv("CLICKHOUSE_MAX_BYTES_TO_READ", 1_000_000_000_000)),
        readonly: "1",
    });
    if (!response.body) {
        return;
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffered = "";

    while (true) {
        const { done, value } = await reader.read();
        if (done) {
            break;
        }
        buffered += decoder.decode(value, { stream: true });
        let index;
        while ((index = buffered.indexOf("\n")) !== -1) {
            const line = buffered.slice(0, index);
            buffered = buffered.slice(index + 1);
            if (line.trim()) {
                await onRow(JSON.parse(line));
            }
        }
    }

    buffered += decoder.decode();
    if (buffered.trim()) {
        await onRow(JSON.parse(buffered));
    }
}

async function* listS3Objects(bucket, prefix) {
    let token = "";
    while (true) {
        const args = [
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--prefix",
            prefix,
            "--output",
            "json",
        ];
        if (token) {
            args.push("--continuation-token", token);
        }
        const page = JSON.parse(await awsStdout(args));
        for (const object of page.Contents ?? []) {
            yield object;
        }
        if (!page.IsTruncated || !page.NextContinuationToken) {
            break;
        }
        token = page.NextContinuationToken;
    }
}

async function awsStdout(args) {
    const result = await run("aws", args);
    return result.stdout;
}

function run(command, commandArgs) {
    return new Promise((resolve, reject) => {
        const child = spawn(command, commandArgs, {
            cwd: root,
            env: process.env,
            stdio: ["ignore", "pipe", "pipe"],
        });
        const stdout = [];
        const stderr = [];
        child.stdout.on("data", (chunk) => stdout.push(chunk));
        child.stderr.on("data", (chunk) => stderr.push(chunk));
        child.on("error", reject);
        child.on("close", (code) => {
            const result = {
                code,
                stdout: Buffer.concat(stdout).toString("utf8"),
                stderr: Buffer.concat(stderr).toString("utf8"),
            };
            if (code === 0) {
                resolve(result);
            } else {
                reject(new Error(`${command} ${commandArgs.join(" ")} failed with ${code}\n${result.stderr || result.stdout}`));
            }
        });
    });
}

async function clickhouse(sql) {
    const response = await clickhouseResponse(sql);
    await response.text();
}

async function clickhouseResponse(sql, settings = {}) {
    const url = new URL(clickhouseUrl);
    for (const [key, value] of Object.entries(settings)) {
        url.searchParams.set(key, value);
    }
    const response = await fetch(url, {
        method: "POST",
        headers: {
            authorization: `Basic ${Buffer.from(`${user}:${password}`).toString("base64")}`,
            "content-type": "text/plain; charset=utf-8",
        },
        body: sql,
    });
    if (!response.ok) {
        const text = await response.text();
        throw new Error(`ClickHouse query failed (${response.status}): ${text}`);
    }
    return response;
}

function facetRowsForEvent(row) {
    const timestamp = stringValue(row.timestamp);
    if (!timestamp) {
        return [];
    }
    const data = isPlainObject(row.data) ? row.data : {};
    const eventType = stringValue(row.event_type) || stringValue(data.event_type);
    const signal = stringValue(row.signal) || signalForEventType(eventType);
    const context = {
        bucket_time: minuteBucket(timestamp),
        signal,
    };

    const rows = [];
    const seen = new Set();
    for (const [key, value] of Object.entries(data)) {
        collectFacetRows(context, key, value, seen, rows);
    }
    pushFacetRow(context, "signal", signal, "string", seen, rows);
    return rows;
}

function collectFacetRows(context, key, value, seen, rows) {
    if (value === null || value === undefined) {
        return;
    }
    if (Array.isArray(value)) {
        for (const item of value) {
            collectFacetRows(context, key, item, seen, rows);
        }
        return;
    }
    if (isPlainObject(value)) {
        for (const [childKey, childValue] of Object.entries(value)) {
            collectFacetRows(context, key ? `${key}.${childKey}` : childKey, childValue, seen, rows);
        }
        return;
    }
    if (typeof value === "string") {
        pushFacetRow(context, key, value, "string", seen, rows);
        return;
    }
    if (typeof value === "number" || typeof value === "bigint") {
        pushFacetRow(context, key, String(value), "number", seen, rows);
        return;
    }
    if (typeof value === "boolean") {
        pushFacetRow(context, key, value ? "true" : "false", "bool", seen, rows);
    }
}

function pushFacetRow(context, key, value, valueType, seen, rows) {
    if (!key || !value) {
        return;
    }
    const dedupeKey = `${key}\0${valueType}\0${value}`;
    if (seen.has(dedupeKey)) {
        return;
    }
    seen.add(dedupeKey);
    rows.push({
        bucket_time: context.bucket_time,
        key,
        value,
        value_type: valueType,
        count: 1,
    });
}

function minuteBucket(timestamp) {
    const date = new Date(timestamp);
    if (Number.isFinite(date.getTime())) {
        date.setUTCSeconds(0, 0);
        return [
            date.getUTCFullYear(),
            "-",
            pad2(date.getUTCMonth() + 1),
            "-",
            pad2(date.getUTCDate()),
            " ",
            pad2(date.getUTCHours()),
            ":",
            pad2(date.getUTCMinutes()),
            ":00.000",
        ].join("");
    }
    const normalized = timestamp.trim().replace("T", " ");
    return normalized.length >= 16 ? `${normalized.slice(0, 16)}:00.000` : timestamp;
}

function pad2(value) {
    return String(value).padStart(2, "0");
}

function signalForEventType(eventType) {
    if (eventType === "span" || eventType === "span_start" || eventType === "span_end") return "trace";
    if (eventType === "metric") return "metric";
    if (eventType === "log") return "log";
    if (["analytics", "track", "page", "screen", "identify", "group", "alias"].includes(eventType)) return "analytics";
    return "other";
}

function stringValue(value) {
    if (typeof value === "string") return value;
    if (typeof value === "number" || typeof value === "bigint") return String(value);
    if (typeof value === "boolean") return value ? "true" : "false";
    return "";
}

function isPlainObject(value) {
    return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function loadEnvFile(file) {
    if (!file) {
        return;
    }
    const resolved = path.resolve(root, file);
    const contents = readFileSync(resolved, "utf8");
    for (const line of contents.split(/\r?\n/)) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith("#")) {
            continue;
        }
        const match = trimmed.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
        if (!match) {
            continue;
        }
        const [, key, rawValue] = match;
        if (process.env[key] !== undefined) {
            continue;
        }
        process.env[key] = unquote(rawValue.trim());
    }
}

function unquote(value) {
    if (
        (value.startsWith("\"") && value.endsWith("\"")) ||
        (value.startsWith("'") && value.endsWith("'"))
    ) {
        return value.slice(1, -1);
    }
    return value;
}

function requiredEnv(key) {
    const value = process.env[key];
    if (!value) {
        throw new Error(`${key} is required`);
    }
    return value;
}

function numberEnv(key, fallback) {
    const value = process.env[key];
    if (!value) {
        return fallback;
    }
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`${key} must be a positive number`);
    }
    return parsed;
}

function numberOption(name, fallback) {
    const value = optionValue(name);
    if (value === undefined) {
        return fallback;
    }
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`${name} must be a positive number`);
    }
    return parsed;
}

function optionValue(name) {
    const index = args.indexOf(name);
    if (index === -1) {
        return undefined;
    }
    const value = args[index + 1];
    if (!value || value.startsWith("--")) {
        throw new Error(`${name} requires a value`);
    }
    return value;
}

function identifier(value, key) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
        throw new Error(`${key} must be a simple ClickHouse identifier`);
    }
    return value;
}

function quoteIdentifier(value) {
    return `\`${value.replaceAll("`", "``")}\``;
}

function usage() {
    console.log(`Usage: node scripts/backfill-clickhouse-facets.mjs [--env .env.aws] [--truncate] [--source clickhouse|s3] [--bucket S3_BUCKET] [--prefix events/dt=] [--concurrency N] [--where "WHERE timestamp >= ..."] [--limit N] [--batch-size N]

Streams existing ClickHouse events and writes generated scalar JSON facets into CLICKHOUSE_FACETS_TABLE.

Examples:
  node scripts/backfill-clickhouse-facets.mjs --env .env.aws --truncate
  node scripts/backfill-clickhouse-facets.mjs --env .env.aws --source s3 --bucket nanotrace-prod-events-035300c --truncate
  node scripts/backfill-clickhouse-facets.mjs --env .env.aws --where "WHERE timestamp >= now64(3) - INTERVAL 1 DAY"
`);
}
