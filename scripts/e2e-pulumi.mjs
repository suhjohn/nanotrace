#!/usr/bin/env node
import { readFileSync } from "node:fs";
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
const clickhouseUrl = trimTrailingSlash(
  process.env.CLICKHOUSE_URL || requiredOutput(outputs, "clickhouseUrlOutput"),
);
const clickhouseUser =
  process.env.CLICKHOUSE_USER || requiredOutput(outputs, "clickhouseUserOutput");
const clickhousePassword = requiredEnv("CLICKHOUSE_PASSWORD");
const clickhouseDatabase =
  process.env.CLICKHOUSE_DATABASE ||
  outputs.clickhouseDatabaseOutput ||
  "observatory";
const clickhouseTable =
  process.env.CLICKHOUSE_TABLE || outputs.clickhouseTableOutput || "events";
const clickhouseKvIndexTable =
  process.env.CLICKHOUSE_EVENT_KV_INDEX_TABLE || "event_kv_index";
const clickhouseServingWatermarksTable =
  process.env.CLICKHOUSE_SERVING_WATERMARKS_TABLE || "serving_watermarks";
const waitMs = numberEnv("NANOTRACE_E2E_WAIT_MS", 300_000);
const pollMs = numberEnv("NANOTRACE_E2E_POLL_MS", 5_000);
const suffix = `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
const runId = `pulumi_e2e_${suffix}`;
const eventId = process.env.NANOTRACE_E2E_EVENT_ID ?? `evt_${runId}`;
const timestamp = new Date().toISOString();

console.log(`ingestUrl=${ingestUrl}`);
console.log(`clickhouseDatabase=${clickhouseDatabase}`);
console.log(`eventId=${eventId}`);

await expectUnauthorized(ingestUrl);

await postEvents(ingestUrl, apiKey, [
  {
    event_id: eventId,
    timestamp,
    data: {
      event_type: "track",
      name: "Pulumi E2E",
      service: "nanotrace-e2e",
      plan: "enterprise",
      latency_ms: 175,
      ok: true,
      request_id: `req_${suffix}`,
      llm: {
        model: "gpt-4.1",
        usage: {
          prompt_tokens: 1200,
        },
      },
      tags: ["checkout", "mobile"],
      items: [
        { sku: "sku_1", price: 10 },
        { sku: "sku_2", price: 20 },
      ],
      _pulumi_e2e: {
        run_id: runId,
      },
    },
  },
]);

await waitForClickHouseEvent();
await waitForEventKvRows();
await waitForServingWatermark(clickhouseTable);
await waitForServingWatermark(clickhouseKvIndexTable);
await assertQueryBehavior();

console.log("E2E passed");

async function expectUnauthorized(baseUrl) {
  const response = await fetch(`${baseUrl}/v1/events`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      event_id: "unauthorized-probe",
      timestamp: new Date().toISOString(),
      data: {},
    }),
  });

  assert(
    response.status === 401,
    `expected unauthorized probe to return 401, got ${response.status}`,
  );
}

async function postEvents(baseUrl, token, events) {
  const response = await fetch(`${baseUrl}/v1/events`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(events),
  });
  const text = await response.text();
  assert(response.status === 202, `POST /v1/events failed: ${response.status} ${text}`);
  const parsed = JSON.parse(text);
  assert(parsed.mode === "kafka", `expected Kafka ingest mode, got ${text}`);
  assert(parsed.accepted === true, `expected accepted=true, got ${text}`);
  return parsed;
}

async function waitForClickHouseEvent() {
  await waitUntil(async () => {
    const rows = await clickhouseJson(`
SELECT
  event_id,
  event_type,
  signal,
  toString(getSubcolumn(data, 'service')) AS service,
  toString(getSubcolumn(data, 'plan')) AS plan
FROM ${ident(clickhouseDatabase)}.${ident(clickhouseTable)}
WHERE event_id = ${s(eventId)}
  AND getSubcolumn(data, '_pulumi_e2e.run_id') = ${s(runId)}
FORMAT JSON
`);
    return rows.length > 0 ? rows : null;
  }, "ClickHouse events row");

  const rows = await clickhouseJson(`
SELECT
  event_id,
  event_type,
  signal,
  toString(getSubcolumn(data, 'service')) AS service,
  toString(getSubcolumn(data, 'plan')) AS plan
FROM ${ident(clickhouseDatabase)}.${ident(clickhouseTable)}
WHERE event_id = ${s(eventId)}
  AND getSubcolumn(data, '_pulumi_e2e.run_id') = ${s(runId)}
FORMAT JSON
`);
  assert(rows.length === 1, `expected one ClickHouse event row, got ${rows.length}`);
  assert(rows[0].event_type === "track", `expected event_type track, got ${rows[0].event_type}`);
  assert(rows[0].signal === "analytics", `expected analytics signal, got ${rows[0].signal}`);
  assert(rows[0].service === "nanotrace-e2e", `service mismatch: ${rows[0].service}`);
  assert(rows[0].plan === "enterprise", `plan mismatch: ${rows[0].plan}`);
}

async function waitForEventKvRows() {
  await waitUntil(async () => {
    const rows = await clickhouseJson(`
SELECT count() AS count
FROM ${ident(clickhouseDatabase)}.${ident(clickhouseKvIndexTable)}
WHERE event_id = ${s(eventId)}
FORMAT JSON
`);
    return Number(rows[0]?.count || 0) >= 12 ? true : null;
  }, "event_kv_index rows");

  const rows = await clickhouseJson(`
SELECT DISTINCT path, value_type, string_value, number_value, bool_value, scope_path, scope_index
FROM ${ident(clickhouseDatabase)}.${ident(clickhouseKvIndexTable)}
WHERE event_id = ${s(eventId)}
ORDER BY path, scope_index, string_value
FORMAT JSON
`);
  assert(rows.some((row) => row.path === "plan" && row.string_value === "enterprise"), "missing plan KV row");
  assert(rows.some((row) => row.path === "latency_ms" && Number(row.number_value) === 175), "missing latency_ms KV row");
  assert(rows.some((row) => row.path === "ok" && Number(row.bool_value) === 1), "missing ok KV row");
  assert(rows.some((row) => row.path === "llm.model" && row.string_value === "gpt-4.1"), "missing llm.model KV row");
  assert(rows.some((row) => row.path === "llm.usage.prompt_tokens" && Number(row.number_value) === 1200), "missing prompt_tokens KV row");
  assert(rows.some((row) => row.path === "tags" && row.string_value === "mobile"), "missing tags KV row");
  assert(
    rows.some(
      (row) =>
        row.path === "items[].sku" &&
        row.string_value === "sku_1" &&
        row.scope_path === "items" &&
        Number(row.scope_index) === 0,
    ),
    "missing scoped sku KV row",
  );
  assert(
    rows.some(
      (row) =>
        row.path === "items[].price" &&
        Number(row.number_value) === 20 &&
        row.scope_path === "items" &&
        Number(row.scope_index) === 1,
    ),
    "missing scoped price KV row",
  );
}

async function waitForServingWatermark(servingTable) {
  await waitUntil(async () => {
    const rows = await clickhouseJson(`
SELECT count() AS count
FROM ${ident(clickhouseDatabase)}.${ident(clickhouseServingWatermarksTable)}
WHERE serving_table = ${s(servingTable)}
  AND source_namespace = 'nanotrace'
  AND source_table = 'events'
FORMAT JSON
`);
    return Number(rows[0]?.count || 0) > 0 ? true : null;
  }, `${servingTable} serving watermark`);
}

async function assertQueryBehavior() {
  const kvRows = await clickhouseQuery({
    query: `
SELECT countDistinct(event_id) AS count
FROM event_kv_index
WHERE event_id = {event_id:String}
  AND path = {path:String}
  AND value_type = 'string'
  AND string_value = {value:String}
`,
    parameters: {
      event_id: eventId,
      path: "llm.model",
      value: "gpt-4.1",
    },
  });
  assert(Number(kvRows[0]?.count ?? 0) === 1, "direct KV query did not find event");

  const structured = await postEventsQuery(apiKey, {
    view: "summary",
    filter: {
      facets: [
        { path: "llm.model", operator: "eq", value: "gpt-4.1" },
        { path: "llm.usage.prompt_tokens", operator: "gte", value: "1000" },
        { path: "items[].sku", operator: "eq", value: "sku_1" },
        { path: "items[].price", operator: "eq", value: "10" },
      ],
    },
    allow_stale_serving: true,
  });
  assert(
    Number((structured.data || [])[0]?.count ?? 0) === 1,
    "structured arbitrary KV query did not find event",
  );

  const mismatched = await postEventsQuery(apiKey, {
    view: "summary",
    filter: {
      facets: [
        { path: "items[].sku", operator: "eq", value: "sku_1" },
        { path: "items[].price", operator: "eq", value: "20" },
      ],
    },
    allow_stale_serving: true,
  });
  assert(
    Number((mismatched.data || [])[0]?.count ?? 0) === 0,
    "same-element array correlation accepted a mismatched item",
  );
}

async function postEventsQuery(token, body) {
  return fetchJson(`${ingestUrl}/v1/query`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ type: "events", ...body }),
  });
}

async function fetchJson(url, init = {}) {
  const response = await fetch(url, init);
  const text = await response.text();
  assert(response.ok, `${url} failed with ${response.status}: ${text}`);
  return text ? JSON.parse(text) : {};
}

async function waitUntil(fn, label) {
  const deadline = Date.now() + waitMs;
  let last = null;
  while (Date.now() < deadline) {
    last = await fn();
    if (last) return last;
    await sleep(pollMs);
  }
  throw new Error(`timed out waiting for ${label}`);
}

async function clickhouseJson(query) {
  const url = new URL(clickhouseUrl);
  url.searchParams.set("database", clickhouseDatabase);
  url.searchParams.set("date_time_input_format", "best_effort");
  url.searchParams.set("type_json_skip_duplicated_paths", "1");
  const response = await fetch(url, {
    method: "POST",
    headers: {
      authorization: `Basic ${Buffer.from(`${clickhouseUser}:${clickhousePassword}`).toString("base64")}`,
      "content-type": "text/plain; charset=utf-8",
    },
    body: query,
  });
  const text = await response.text();
  assert(response.ok, `ClickHouse query failed ${response.status}: ${text}`);
  return JSON.parse(text).data || [];
}

async function clickhouseQuery({ query, parameters = {} }) {
  return clickhouseJson(renderClickHouseQuery(query, parameters));
}

function renderClickHouseQuery(query, parameters) {
  const rendered = query.replace(/\{([A-Za-z_][A-Za-z0-9_]*):[^}]+}/g, (match, name) => {
    assert(Object.prototype.hasOwnProperty.call(parameters, name), `missing query parameter: ${name}`);
    const value = parameters[name];
    if (typeof value === "number" || typeof value === "bigint") return String(value);
    if (typeof value === "boolean") return value ? "1" : "0";
    return s(value);
  });
  return /\bFORMAT\s+JSON\b/i.test(rendered) ? rendered : `${rendered}\nFORMAT JSON\n`;
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

function s(value) {
  return `'${String(value).replaceAll("\\", "\\\\").replaceAll("'", "''")}'`;
}

function ident(value) {
  assert(/^[A-Za-z_][A-Za-z0-9_]*$/.test(value), `invalid identifier: ${value}`);
  return `\`${value.replaceAll("`", "``")}\``;
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
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
      if (code === 0) {
        resolve(result);
      } else {
        reject(new Error(`${command} ${args.join(" ")} failed with ${code}\n${stderr || stdout}`));
      }
    });
  });
}
