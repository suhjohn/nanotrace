#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { spawn } from "node:child_process";

const root = path.resolve(new URL("..", import.meta.url).pathname);
const pulumiCwd = path.join(root, "deploy/pulumi/nanotrace");

loadEnvFile(process.env.NANOTRACE_ENV_FILE);

const outputs = await pulumiOutputs();
const apiKey = requiredEnv("NANOTRACE_E2E_API_KEY");
const ingestUrl = trimTrailingSlash(
    process.env.NANOTRACE_E2E_INGEST_URL || requiredOutput(outputs, "ingestUrl"),
);
const bucketName = requiredOutput(outputs, "bucketName");
const waitMs = numberEnv("NANOTRACE_E2E_WAIT_MS", 180_000);
const pollMs = numberEnv("NANOTRACE_E2E_POLL_MS", 5_000);
const clickhouseWaitMs = numberEnv("NANOTRACE_E2E_CLICKHOUSE_WAIT_MS", 300_000);
const eventId = process.env.NANOTRACE_E2E_EVENT_ID ?? `e2e-${Date.now()}-${Math.random().toString(16).slice(2)}`;
const processorName =
    process.env.NANOTRACE_E2E_PROCESSOR_NAME ??
    `e2e-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const timestamp = new Date().toISOString();

console.log(`ingestUrl=${ingestUrl}`);
console.log(`bucketName=${bucketName}`);
console.log(`eventId=${eventId}`);

await expectUnauthorized(ingestUrl);

let processorRegistered = false;
try {
    console.log(`registering processor=${processorName}`);
    await putProcessor(ingestUrl, apiKey, processorName);
    processorRegistered = true;
    await waitForProcessorReady(ingestUrl, apiKey, processorName, clickhouseWaitMs, pollMs);
    await sleep(numberEnv("NANOTRACE_E2E_PROCESSOR_HOTLOAD_WAIT_MS", 45_000));

    const receipt = await postEvent(ingestUrl, apiKey, {
        event_id: eventId,
        timestamp,
        observed_timestamp: timestamp,
        data: {
            tenant_id: "e2e",
            service: "nanotrace-e2e",
            event_type: "pulumi_e2e",
            ok: true,
            event_id: eventId,
            timestamp,
        },
    });

    assert(receipt.event_id === eventId, `unexpected receipt event_id: ${receipt.event_id}`);
    assert(typeof receipt.source_file === "string" && receipt.source_file.length > 0, "missing source_file");
    assert(Number.isInteger(receipt.source_offset) && receipt.source_offset >= 0, "missing source_offset");
    assert(Number.isInteger(receipt.source_length) && receipt.source_length > 0, "missing source_length");

    console.log(`source_file=${receipt.source_file}`);
    console.log(`waiting up to ${waitMs}ms for S3 object upload`);

    const objectPath = await waitForS3Object(bucketName, receipt.source_file, waitMs, pollMs);
    const body = await readFile(objectPath, "utf8");
    const lines = body.trimEnd().split("\n").filter(Boolean);
    const row = lines.map((line) => JSON.parse(line)).find((candidate) => candidate.event_id === eventId);

    assert(row, `event ${eventId} not found in uploaded object`);
    assert(row.timestamp === timestamp, `timestamp mismatch: ${row.timestamp}`);
    assert(row.observed_timestamp === timestamp, `observed_timestamp mismatch: ${row.observed_timestamp}`);
    assert(row.source_file === receipt.source_file, `source_file mismatch: ${row.source_file}`);
    assert(Number.isInteger(row.source_offset) && row.source_offset >= 0, `source_offset mismatch: ${row.source_offset}`);
    assert(Number.isInteger(row.source_length) && row.source_length > 0, `source_length mismatch: ${row.source_length}`);
    assert(row.tenant_id === undefined, `tenant_id should live in data, got ${row.tenant_id}`);
    assert(row.service === undefined, `service should live in data, got ${row.service}`);
    assert(row.event_type === undefined, `event_type should live in data, got ${row.event_type}`);
    assert(row.data?.tenant_id === "org_default", `tenant_id mismatch: ${row.data?.tenant_id}`);
    assert(row.data?.service === "nanotrace-e2e", `service mismatch: ${row.data?.service}`);
    assert(row.data?.event_type === "pulumi_e2e", `event_type mismatch: ${row.data?.event_type}`);
    assert(row.data?.event_id === eventId, "data payload mismatch");
    assert(row.data?.modal_upload_field === "upload-ok", `upload field mismatch: ${row.data?.modal_upload_field}`);
    assert(row.data?.modal_loader_field === undefined, "raw S3 event should not include loader-only field");

    const actualLength = Buffer.byteLength(lines.find((line) => JSON.parse(line).event_id === eventId) + "\n");
    assert(actualLength === row.source_length, `source_length ${row.source_length} != actual byte length ${actualLength}`);

    if (hasClickHouseQueryEnv()) {
        console.log(`waiting up to ${clickhouseWaitMs}ms for processed ClickHouse row`);
        await waitForClickHouseRow(
            requiredEnv("CLICKHOUSE_URL"),
            requiredEnv("CLICKHOUSE_USER"),
            requiredEnv("CLICKHOUSE_PASSWORD"),
            process.env.CLICKHOUSE_DATABASE ?? outputs.clickhouseDatabase ?? outputs.clickhouseDatabaseOutput ?? "observatory",
            process.env.CLICKHOUSE_TABLE ?? outputs.clickhouseTable ?? outputs.clickhouseTableOutput ?? "events",
            eventId,
            clickhouseWaitMs,
            pollMs,
        );
    }
} finally {
    if (processorRegistered) {
        console.log(`deleting processor=${processorName}`);
        await deleteProcessor(ingestUrl, apiKey, processorName);
    }
}

console.log("E2E passed");

async function expectUnauthorized(baseUrl) {
    const response = await fetch(`${baseUrl}/events`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
            event_id: "unauthorized-probe",
            timestamp: new Date().toISOString(),
            data: {},
        }),
    });

    assert(response.status === 401, `expected unauthorized probe to return 401, got ${response.status}`);
}

async function postEvent(baseUrl, token, event) {
    const response = await fetch(`${baseUrl}/events`, {
        method: "POST",
        headers: {
            authorization: `Bearer ${token}`,
            "content-type": "application/json",
        },
        body: JSON.stringify(event),
    });
    const text = await response.text();
    assert(response.ok, `POST /events failed: ${response.status} ${text}`);
    return JSON.parse(text);
}

async function putProcessor(baseUrl, token, name) {
    const response = await fetch(`${baseUrl}/processors/${encodeURIComponent(name)}`, {
        method: "PUT",
        headers: {
            authorization: `Bearer ${token}`,
            "content-type": "application/json",
        },
        body: JSON.stringify({
            upload: {
                code: [
                    "use anyhow::Result;",
                    "use serde_json::Value;",
                    "",
                    "pub fn transform_event(mut event: Value, _config: &Value) -> Result<Value> {",
                    "    if let Some(data) = event.get_mut(\"data\").and_then(Value::as_object_mut) {",
                    "        data.insert(\"modal_upload_field\".to_string(), Value::String(\"upload-ok\".to_string()));",
                    "    }",
                    "    Ok(event)",
                    "}",
                ].join("\n"),
                config: {},
            },
            loader: {
                code: [
                    "use anyhow::Result;",
                    "use serde_json::Value;",
                    "",
                    "pub fn transform_event(mut event: Value, _config: &Value) -> Result<Value> {",
                    "    if let Some(data) = event.get_mut(\"data\").and_then(Value::as_object_mut) {",
                    "        data.insert(\"modal_loader_field\".to_string(), Value::String(\"loader-ok\".to_string()));",
                    "    }",
                    "    Ok(event)",
                    "}",
                ].join("\n"),
                config: {},
            },
        }),
    });
    const text = await response.text();
    assert(response.ok, `PUT /processors/${name} failed: ${response.status} ${text}`);
    return JSON.parse(text);
}

async function waitForProcessorReady(baseUrl, token, name, waitMs, pollMs) {
    const deadline = Date.now() + waitMs;
    let lastStatus = "";
    while (Date.now() < deadline) {
        const manifest = await getProcessor(baseUrl, token, name);
        if (manifest?.status === "ready" && manifest.artifacts?.upload?.key && manifest.artifacts?.loader?.key) {
            return manifest;
        }
        if (manifest?.status === "failed") {
            throw new Error(`processor ${name} failed: ${manifest.error}`);
        }
        lastStatus = manifest ? manifest.status : "missing";
        await sleep(pollMs);
    }
    throw new Error(`timed out waiting for processor ${name} to be ready; last status=${lastStatus}`);
}

async function getProcessor(baseUrl, token, name) {
    const response = await fetch(`${baseUrl}/processors`, {
        headers: { authorization: `Bearer ${token}` },
    });
    const text = await response.text();
    assert(response.ok, `GET /processors failed: ${response.status} ${text}`);
    const parsed = JSON.parse(text);
    return parsed.processors?.find((processor) => processor.name === name);
}

async function deleteProcessor(baseUrl, token, name) {
    const response = await fetch(`${baseUrl}/processors/${encodeURIComponent(name)}`, {
        method: "DELETE",
        headers: { authorization: `Bearer ${token}` },
    });
    const text = await response.text();
    assert(response.ok, `DELETE /processors/${name} failed: ${response.status} ${text}`);
    return JSON.parse(text);
}

async function waitForS3Object(bucket, key, waitMs, pollMs) {
    const deadline = Date.now() + waitMs;
    let lastError = "";

    while (Date.now() < deadline) {
        const head = await run("aws", ["s3api", "head-object", "--bucket", bucket, "--key", key], {
            allowFailure: true,
        });
        if (head.code === 0) {
            const dir = await mkdtemp(path.join(tmpdir(), "nanotrace-e2e-"));
            const output = path.join(dir, "object.ndjson");
            await run("aws", ["s3api", "get-object", "--bucket", bucket, "--key", key, output]);
            return output;
        }

        lastError = head.stderr || head.stdout;
        await sleep(pollMs);
    }

    throw new Error(`timed out waiting for s3://${bucket}/${key}\n${lastError}`);
}

async function waitForClickHouseRow(url, user, password, database, table, eventId, waitMs, pollMs) {
    const deadline = Date.now() + waitMs;
    let lastError = "";

    while (Date.now() < deadline) {
        try {
            const count = await clickHouseCount(url, user, password, database, table, eventId);
            if (count > 0) {
                return;
            }
            lastError = `row count is ${count}`;
        } catch (error) {
            lastError = error.message;
        }
        await sleep(pollMs);
    }

    throw new Error(`timed out waiting for ClickHouse row ${eventId}\n${lastError}`);
}

async function clickHouseCount(url, user, password, database, table, eventId) {
    const sql = [
        "SELECT countIf(JSON_VALUE(toJSONString(data), '$.modal_upload_field') = 'upload-ok' AND JSON_VALUE(toJSONString(data), '$.modal_loader_field') = 'loader-ok') AS count",
        `FROM ${quoteIdentifier(identifier(database, "CLICKHOUSE_DATABASE"))}.${quoteIdentifier(identifier(table, "CLICKHOUSE_TABLE"))}`,
        `WHERE event_id = '${escapeSqlString(eventId)}'`,
        "FORMAT JSON",
    ].join("\n");

    const response = await fetch(url, {
        method: "POST",
        headers: {
            authorization: `Basic ${Buffer.from(`${user}:${password}`).toString("base64")}`,
            "content-type": "text/plain; charset=utf-8",
        },
        body: sql,
    });
    const text = await response.text();
    if (!response.ok) {
        throw new Error(`ClickHouse query failed: ${response.status} ${text}`);
    }
    const parsed = JSON.parse(text);
    return Number(parsed.data?.[0]?.count ?? 0);
}

async function pulumiOutputs() {
    const result = await run("pulumi", ["stack", "output", "--json", "--show-secrets"], { cwd: pulumiCwd });
    return JSON.parse(result.stdout);
}

function requiredOutput(outputs, key) {
    const value = outputs[key];
    if (value === undefined || value === null || value === "") {
        throw new Error(`Pulumi output ${key} is required`);
    }
    return String(value);
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

function trimTrailingSlash(value) {
    return value.replace(/\/+$/, "");
}

function assert(condition, message) {
    if (!condition) {
        throw new Error(message);
    }
}

function hasClickHouseQueryEnv() {
    return Boolean(process.env.CLICKHOUSE_URL && process.env.CLICKHOUSE_USER && process.env.CLICKHOUSE_PASSWORD);
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

function escapeSqlString(value) {
    return String(value).replaceAll("\\", "\\\\").replaceAll("'", "\\'");
}

function loadEnvFile(file) {
    if (!file) {
        return;
    }
    const envPath = path.resolve(root, file);
    try {
        const text = readFileSync(envPath, "utf8");
        for (const line of text.split(/\r?\n/)) {
            const trimmed = line.trim();
            if (!trimmed || trimmed.startsWith("#")) {
                continue;
            }
            const match = trimmed.match(/^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
            if (!match) {
                continue;
            }
            const [, key, rawValue] = match;
            if (process.env[key] !== undefined) {
                continue;
            }
            process.env[key] = parseEnvValue(rawValue);
        }
    } catch (error) {
        if (error.code !== "ENOENT") {
            throw error;
        }
    }
}

function parseEnvValue(value) {
    const trimmed = value.trim();
    if (
        (trimmed.startsWith("\"") && trimmed.endsWith("\"")) ||
        (trimmed.startsWith("'") && trimmed.endsWith("'"))
    ) {
        return trimmed.slice(1, -1);
    }
    return trimmed;
}

async function run(command, args, options = {}) {
    return new Promise((resolve, reject) => {
        const child = spawn(command, args, {
            cwd: options.cwd ?? root,
            env: process.env,
            stdio: ["ignore", "pipe", "pipe"],
        });
        let stdout = "";
        let stderr = "";
        child.stdout.on("data", (chunk) => {
            stdout += chunk;
        });
        child.stderr.on("data", (chunk) => {
            stderr += chunk;
        });
        child.on("error", reject);
        child.on("close", (code) => {
            const result = { code, stdout, stderr };
            if (code === 0 || options.allowFailure) {
                resolve(result);
            } else {
                reject(new Error(`${command} ${args.join(" ")} failed with ${code}\n${stderr || stdout}`));
            }
        });
    });
}
