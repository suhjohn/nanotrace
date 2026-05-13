#!/usr/bin/env node
import { readFileSync } from "node:fs";
import http from "node:http";
import https from "node:https";
import path from "node:path";

const root = path.resolve(new URL("..", import.meta.url).pathname);
const args = process.argv.slice(2);

if (args.includes("--help") || args.includes("-h")) {
  console.log(`Usage: node scripts/benchmark-clickhouse-dense-groups.mjs [--env .env.aws] [--database nanotrace_bench_10m] [--rows 10000000] [--hot-rows 500000] [--days 21] [--skip-generate]`);
  process.exit(0);
}

loadEnvFile(optionValue("--env") ?? process.env.NANOTRACE_ENV_FILE);

const clickhouseUrl = requiredEnv("CLICKHOUSE_URL");
const user = requiredEnv("CLICKHOUSE_USER");
const password = requiredEnv("CLICKHOUSE_PASSWORD");
const database = identifier(optionValue("--database") ?? process.env.NANOTRACE_BENCH_DATABASE ?? "nanotrace_bench_10m", "database");
const rows = numberOption("--rows", 10_000_000);
const hotRows = numberOption("--hot-rows", 500_000);
const days = numberOption("--days", 21);
const insertChunkRows = numberOption("--insert-chunk-rows", 1_000_000);
const skipGenerate = args.includes("--skip-generate");

const events = qid(database, "events");
const facets = qid(database, "event_facets");
const eventIndex = qid(database, "event_facet_index");
const hotValue = "wide-hot";
const hotModulo = Math.max(1, Math.floor(rows / hotRows));
const rangeSeconds = days * 24 * 60 * 60;
const base = "2026-04-21 00:00:00.000";
const end = "2026-05-12 00:00:00.000";

if (!skipGenerate) {
  await recreateSchema();
  await insertEvents();
  await insertFacets();
  await insertEventIndex();
  await optimizeTables();
}

await printCounts();
await runBenchmarks();

async function recreateSchema() {
  console.log(`recreate=${database}`);
  await query(`DROP DATABASE IF EXISTS ${quoteIdentifier(database)}`);
  await query(`CREATE DATABASE ${quoteIdentifier(database)}`);
  const schema = readFileSync(path.join(root, "deploy/clickhouse/schema.sql"), "utf8")
    .replace(/\bobservatory\.events\b/g, events)
    .replace(/\bobservatory\.event_facets\b/g, facets)
    .replace(/\bobservatory\.event_facet_index\b/g, eventIndex)
    .replace(/\bobservatory\.hot_dimensions\b/g, qid(database, "hot_dimensions"));
  for (const statement of splitStatements(schema)) {
    await query(statement);
  }
}

async function insertEvents() {
  console.log(`insertEvents rows=${rows} hotRows=${hotRows} days=${days} chunkRows=${insertChunkRows}`);
  for (let offset = 0; offset < rows; offset += insertChunkRows) {
    const chunkRows = Math.min(insertChunkRows, rows - offset);
    await timedExec(`insert events offset=${offset} rows=${chunkRows}`, eventInsertSql(offset, chunkRows));
  }
}

function eventInsertSql(offset, chunkRows) {
  return `INSERT INTO ${events} (event_id, timestamp, observed_timestamp, source_file, source_offset, source_length, data)
SELECT
  concat('bench-', toString(number)) AS event_id,
  ts AS timestamp,
  ts AS observed_timestamp,
  'bench/generated.ndjson' AS source_file,
  number AS source_offset,
  0 AS source_length,
  CAST(concat(
    '{"tenant_id":"bench"',
    ',"service":"', service, '"',
    ',"environment":"prod"',
    ',"event_type":"', event_type, '"',
    ',"signal":"', signal, '"',
    ',"trace_id":"', trace_id, '"',
    ',"span_id":"', span_id, '"',
    ',"parent_span_id":"', parent_span_id, '"',
    ',"name":"', name, '"',
    ',"user_id":"', user_id, '"',
    ',"session_id":"', session_id, '"',
    ',"account_id":"', account_id, '"',
    ',"http.route":"', route, '"',
    ',"http.method":"', method, '"',
    ',"http.status_code":', toString(status_code),
    ',"start_time":"', start_time, '"',
    ',"end_time":"', end_time, '"',
    ',"duration_ms":', toString(duration_ms),
    ',"is_error":', toString(is_error),
    '}'
  ) AS JSON) AS data
FROM
(
  SELECT
    number,
    toDateTime64('${base}', 3, 'UTC') + toIntervalMillisecond(intDiv(number * ${rangeSeconds * 1000}, ${rows})) AS ts,
    (number % ${hotModulo}) = 0 AS is_hot,
    intDiv(number, ${hotModulo}) AS hot_ordinal,
    if(is_hot, '${hotValue}', concat('svc-', toString(number % 200))) AS service,
    multiIf(number % 10 IN (0, 1), 'span_start', number % 10 IN (2, 3), 'span_end', number % 10 IN (4, 5, 6), 'metric', 'log') AS event_type,
    multiIf(event_type IN ('span_start', 'span_end'), 'trace', event_type = 'metric', 'metric', 'log') AS signal,
    lower(concat(hex(sipHash64('trace', intDiv(number, 20))), hex(sipHash64('trace2', intDiv(number, 20))))) AS trace_id,
    lower(hex(sipHash64('span', number))) AS span_id,
    if(number % 20 IN (0, 1), '', lower(hex(sipHash64('span', intDiv(number, 20) * 20)))) AS parent_span_id,
    multiIf(number % 6 = 0, 'POST /checkout', number % 6 = 1, 'GET /cart', number % 6 = 2, 'POST /api/traces', number % 6 = 3, 'POST /api/canvas-events', number % 6 = 4, 'GET /health', 'POST /v1/chat/completions') AS name,
    if(is_hot, concat('bench_user_', toString(intDiv(hot_ordinal, 50000))), concat('user_', toString(number % 1000000))) AS user_id,
    if(is_hot, concat('bench_sess_', toString(intDiv(hot_ordinal, 5000))), concat('sess_', toString(number % 3000000))) AS session_id,
    concat('acct_', toString(intDiv(number, 100000))) AS account_id,
    multiIf(number % 6 = 0, '/checkout', number % 6 = 1, '/cart', number % 6 = 2, '/api/traces', number % 6 = 3, '/api/canvas-events', number % 6 = 4, '/health', '/v1/chat/completions') AS route,
    if(number % 6 IN (1, 4), 'GET', 'POST') AS method,
    if(number % 97 = 0, 500, 200) AS status_code,
    if(number % 97 = 0, 1, 0) AS is_error,
    10 + (number % 9000) AS duration_ms,
    formatDateTime(ts - toIntervalMillisecond(duration_ms), '%Y-%m-%dT%H:%i:%S.000Z', 'UTC') AS start_time,
    formatDateTime(ts, '%Y-%m-%dT%H:%i:%S.000Z', 'UTC') AS end_time
  FROM (SELECT number + ${offset} AS number FROM numbers(${chunkRows}))
)`;
}

async function insertFacets() {
  console.log("insertFacets");
  await query(`TRUNCATE TABLE ${facets}`);
  for (const key of ["tenant_id", "service", "event_type", "user_id", "session_id", "trace_id"]) {
    await timedExec(`facet ${key}`, `INSERT INTO ${facets}
	SELECT toStartOfMinute(timestamp) AS bucket_time, '${key}' AS key, toString(data.${key}) AS value, 'string' AS value_type, count() AS count, countIf(ifNull(data.is_error, 0) = 1) AS error_count
	FROM ${events}
	GROUP BY bucket_time, value`);
  }
}

async function insertEventIndex() {
  console.log("insertEventIndex");
  await query(`TRUNCATE TABLE ${eventIndex}`);
  for (const key of ["tenant_id", "service", "event_type", "user_id", "session_id", "trace_id"]) {
    await timedExec(`event index ${key}`, `INSERT INTO ${eventIndex}
SELECT
  '${key}' AS key,
  toString(data.${key}) AS value,
  'string' AS value_type,
  timestamp,
  toStartOfMinute(timestamp) AS bucket_time,
  event_id,
  event_type,
  signal,
  trace_id,
  span_id,
  ifNull(toString(data.parent_span_id), '') AS parent_span_id,
  ifNull(toString(data.name), '') AS name,
  data.start_time AS start_time,
  data.end_time AS end_time,
  ifNull(toFloat64(data.duration_ms), 0) AS duration_ms
FROM ${events}`);
  }
}

async function optimizeTables() {
  console.log("optimize");
  for (const table of [events, facets, eventIndex]) {
    try {
      await timedExec(`optimize ${table}`, `OPTIMIZE TABLE ${table} FINAL`);
    } catch (error) {
      console.log(`optimize ${table}: skipped ${error.message.split("\n")[0]}`);
    }
  }
}

async function printCounts() {
  await timed("counts", `SELECT 'events' AS table, count() AS rows FROM ${events}
UNION ALL SELECT 'facets', count() FROM ${facets}
UNION ALL SELECT 'event_index', count() FROM ${eventIndex}`);
  await timed("hot group distribution", `SELECT min(timestamp) AS first, max(timestamp) AS last, count() AS count FROM ${eventIndex} WHERE key = 'service' AND value = '${hotValue}'`);
}

async function runBenchmarks() {
  const windows = [
    ["5m", `toDateTime64('${end}', 3, 'UTC') - toIntervalMinute(5)`],
    ["1h", `toDateTime64('${end}', 3, 'UTC') - toIntervalHour(1)`],
    ["24h", `toDateTime64('${end}', 3, 'UTC') - toIntervalDay(1)`],
    ["7d", `toDateTime64('${end}', 3, 'UTC') - toIntervalDay(7)`],
    ["21d", `toDateTime64('${end}', 3, 'UTC') - toIntervalDay(21)`],
  ];
  for (const [label, startExpression] of windows) {
    console.log(`\nwindow=${label}`);
    const timeWhere = `timestamp >= ${startExpression} AND timestamp <= toDateTime64('${end}', 3, 'UTC')`;
    await timed(`scan summary ${label}`, `SELECT count() AS count FROM ${events} PREWHERE ifNull(toString(data.service), '') = '${hotValue}' AND ${timeWhere}`);
    await timed(`index summary ${label}`, `SELECT count() AS count FROM ${eventIndex} PREWHERE key = 'service' AND value = '${hotValue}' AND ${timeWhere}`);
    await timed(`scan event page ${label}`, `SELECT event_id, timestamp, data FROM ${events} PREWHERE ifNull(toString(data.service), '') = '${hotValue}' AND ${timeWhere} ORDER BY timestamp DESC, event_id DESC LIMIT 500`);
    await timed(`index event refs ${label}`, `SELECT event_id, timestamp FROM ${eventIndex} PREWHERE key = 'service' AND value = '${hotValue}' AND ${timeWhere} ORDER BY timestamp DESC, event_id DESC LIMIT 500`);
    await timed(`scan flame slim ${label}`, flameScanQuery(timeWhere));
    await timed(`index flame slim ${label}`, `SELECT event_id, timestamp, event_type, signal, trace_id, span_id, parent_span_id, name, start_time, end_time, duration_ms FROM ${eventIndex} PREWHERE key = 'service' AND value = '${hotValue}' AND ${timeWhere} ORDER BY timestamp ASC, event_id ASC LIMIT 20000`);
  }
}

function flameScanQuery(timeWhere) {
  return `SELECT event_id, timestamp, event_type, signal, trace_id, span_id, ifNull(toString(data.parent_span_id), '') AS parent_span_id, ifNull(toString(data.name), '') AS name, data.start_time AS start_time, data.end_time AS end_time, ifNull(toFloat64(data.duration_ms), 0) AS duration_ms FROM ${events} PREWHERE ifNull(toString(data.service), '') = '${hotValue}' AND ${timeWhere} ORDER BY timestamp ASC, event_id ASC LIMIT 20000`;
}

async function timed(name, sql) {
  const started = performance.now();
  const text = await queryText(`${sql} FORMAT JSON`);
  const elapsed = Math.round(performance.now() - started);
  const parsed = JSON.parse(text);
  const rows = parsed.rows ?? parsed.data?.length ?? 0;
  const stats = parsed.statistics ?? {};
  console.log(`${name}: ms=${elapsed} rows=${rows} readRows=${stats.rows_read ?? ""} readBytes=${stats.bytes_read ?? ""}`);
  if (parsed.data?.length && parsed.data.length <= 10) {
    console.table(parsed.data);
  }
  return parsed;
}

async function timedExec(name, sql) {
  const started = performance.now();
  await query(sql);
  const elapsed = Math.round(performance.now() - started);
  console.log(`${name}: ms=${elapsed}`);
}

async function query(sql) {
  await queryText(sql);
}

async function queryText(sql) {
  const target = new URL(clickhouseUrl);
  const client = target.protocol === "http:" ? http : https;
  return await new Promise((resolve, reject) => {
    const request = client.request(
      target,
      {
        method: "POST",
        headers: {
          authorization: `Basic ${Buffer.from(`${user}:${password}`).toString("base64")}`,
          "content-type": "text/plain; charset=utf-8",
        },
        timeout: 0,
      },
      (response) => {
        response.setEncoding("utf8");
        let body = "";
        response.on("data", (chunk) => {
          body += chunk;
        });
        response.on("end", () => {
          if (response.statusCode < 200 || response.statusCode >= 300) {
            reject(new Error(`ClickHouse query failed (${response.statusCode}): ${body}`));
          } else {
            resolve(body);
          }
        });
      },
    );
    request.on("error", reject);
    request.end(sql);
  });
}

function splitStatements(sql) {
  return sql
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/^\s*--.*$/gm, "")
    .split(";")
    .map((statement) => statement.trim())
    .filter(Boolean);
}

function loadEnvFile(file) {
  if (!file) return;
  const envPath = path.resolve(root, file);
  for (const line of readFileSync(envPath, "utf8").split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const match = /^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(trimmed);
    if (!match) continue;
    const [, key, raw] = match;
    if (process.env[key]) continue;
    process.env[key] = raw.replace(/^['"]|['"]$/g, "");
  }
}

function optionValue(name) {
  const index = args.indexOf(name);
  return index === -1 ? undefined : args[index + 1];
}

function numberOption(name, fallback) {
  const value = optionValue(name);
  return value === undefined ? fallback : Number(value);
}

function requiredEnv(key) {
  const value = process.env[key];
  if (!value) throw new Error(`${key} is required`);
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

function qid(databaseName, tableName) {
  return `${quoteIdentifier(databaseName)}.${quoteIdentifier(tableName)}`;
}
