#!/usr/bin/env node
import { readFileSync } from "node:fs";
import path from "node:path";

const root = path.resolve(new URL("..", import.meta.url).pathname);

const clickhouseUrl = requiredEnv("CLICKHOUSE_URL");
const user = requiredEnv("CLICKHOUSE_USER");
const password = requiredEnv("CLICKHOUSE_PASSWORD");
const database = identifier(process.env.CLICKHOUSE_DATABASE || "observatory", "CLICKHOUSE_DATABASE");
const table = identifier(process.env.CLICKHOUSE_TABLE || "events", "CLICKHOUSE_TABLE");
const schemaPath = path.resolve(root, process.env.CLICKHOUSE_SCHEMA_PATH || "deploy/clickhouse/schema.sql");

const schema = readFileSync(schemaPath, "utf8").replace(
    /\bobservatory\.events\b/g,
    `${quoteIdentifier(database)}.${quoteIdentifier(table)}`,
);

await query(`CREATE DATABASE IF NOT EXISTS ${quoteIdentifier(database)}`);
for (const statement of splitStatements(schema)) {
    await query(statement);
}
for (const statement of compatibilityAlters(database, table)) {
    await query(statement);
}

console.log(`clickhouse_schema=${database}.${table}`);

async function query(sql) {
    const url = new URL(clickhouseUrl);
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
}

function requiredEnv(key) {
    const value = process.env[key];
    if (!value) {
        throw new Error(`${key} is required`);
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

function splitStatements(sql) {
    return stripSqlComments(sql)
        .split(";")
        .map((statement) => statement.trim())
        .filter(Boolean);
}

function stripSqlComments(sql) {
    return sql
        .replace(/\/\*[\s\S]*?\*\//g, "")
        .replace(/^\s*--.*$/gm, "");
}

function compatibilityAlters(database, table) {
    const target = `${quoteIdentifier(database)}.${quoteIdentifier(table)}`;
    const dataType = [
        "JSON(",
        "tenant_id LowCardinality(Nullable(String)),",
        "service LowCardinality(Nullable(String)),",
        "service.namespace LowCardinality(Nullable(String)),",
        "service.instance.id Nullable(String),",
        "service_version LowCardinality(Nullable(String)),",
        "event_type LowCardinality(Nullable(String)),",
        "environment LowCardinality(Nullable(String)),",
        "host.name LowCardinality(Nullable(String)),",
        "host.id Nullable(String),",
        "name LowCardinality(Nullable(String)),",
        "scope_name LowCardinality(Nullable(String)),",
        "scope_version LowCardinality(Nullable(String)),",
        "trace_id Nullable(String),",
        "span_id Nullable(String),",
        "parent_span_id Nullable(String),",
        "trace_state Nullable(String),",
        "span_kind LowCardinality(Nullable(String)),",
        "span_status_code LowCardinality(Nullable(String)),",
        "span_status_message Nullable(String),",
        "start_time Nullable(DateTime64(3, 'UTC')),",
        "end_time Nullable(DateTime64(3, 'UTC')),",
        "span_start_time Nullable(DateTime64(3, 'UTC')),",
        "span_end_time Nullable(DateTime64(3, 'UTC')),",
        "duration_ms Nullable(Float64),",
        "is_error Nullable(UInt8),",
        "http.method LowCardinality(Nullable(String)),",
        "http.route LowCardinality(Nullable(String)),",
        "http.status_code Nullable(UInt16),",
        "http.request.method LowCardinality(Nullable(String)),",
        "http.response.status_code Nullable(UInt16),",
        "url.path Nullable(String),",
        "url.full Nullable(String),",
        "user_agent.original Nullable(String),",
        "client.ip Nullable(String),",
        "client.port Nullable(UInt16),",
        "server.address LowCardinality(Nullable(String)),",
        "server.port Nullable(UInt16),",
        "exception.type LowCardinality(Nullable(String)),",
        "exception.message Nullable(String),",
        "exception.stacktrace Nullable(String),",
        "severity_text LowCardinality(Nullable(String)),",
        "severity_number Nullable(UInt8),",
        "body Nullable(String),",
        "logger.name LowCardinality(Nullable(String)),",
        "thread.name LowCardinality(Nullable(String)),",
        "db.system LowCardinality(Nullable(String)),",
        "db.operation LowCardinality(Nullable(String)),",
        "db.statement Nullable(String),",
        "rpc.system LowCardinality(Nullable(String)),",
        "rpc.service LowCardinality(Nullable(String)),",
        "rpc.method LowCardinality(Nullable(String)),",
        "messaging.system LowCardinality(Nullable(String)),",
        "messaging.destination.name LowCardinality(Nullable(String)),",
        "messaging.operation.name LowCardinality(Nullable(String)),",
        "metric_name LowCardinality(Nullable(String)),",
        "metric_type LowCardinality(Nullable(String)),",
        "metric_value Nullable(Float64),",
        "metric_unit LowCardinality(Nullable(String)),",
        "metric.temporality LowCardinality(Nullable(String)),",
        "metric.is_monotonic Nullable(Bool),",
        "count Nullable(UInt64),",
        "sum Nullable(Float64),",
        "metric_count Nullable(UInt64),",
        "metric_sum Nullable(Float64),",
        "metric_min Nullable(Float64),",
        "metric_max Nullable(Float64),",
        "user_id Nullable(String),",
        "anonymous_id Nullable(String),",
        "device_id Nullable(String),",
        "session_id Nullable(String),",
        "account_id Nullable(String),",
        "page_url Nullable(String),",
        "page_path LowCardinality(Nullable(String)),",
        "page_title Nullable(String),",
        "referrer Nullable(String),",
        "screen_name LowCardinality(Nullable(String)),",
        "utm_source LowCardinality(Nullable(String)),",
        "utm_medium LowCardinality(Nullable(String)),",
        "utm_campaign LowCardinality(Nullable(String)),",
        "utm_term LowCardinality(Nullable(String)),",
        "utm_content LowCardinality(Nullable(String)),",
        "country LowCardinality(Nullable(String)),",
        "region LowCardinality(Nullable(String)),",
        "city LowCardinality(Nullable(String)),",
        "continent LowCardinality(Nullable(String)),",
        "location_lat Nullable(Float64),",
        "location_lng Nullable(Float64),",
        "device_type LowCardinality(Nullable(String)),",
        "device_brand LowCardinality(Nullable(String)),",
        "device_manufacturer LowCardinality(Nullable(String)),",
        "device_model LowCardinality(Nullable(String)),",
        "browser LowCardinality(Nullable(String)),",
        "browser_version LowCardinality(Nullable(String)),",
        "os LowCardinality(Nullable(String)),",
        "os_version LowCardinality(Nullable(String)),",
        "app_version LowCardinality(Nullable(String)),",
        "locale LowCardinality(Nullable(String)),",
        "timezone LowCardinality(Nullable(String)),",
        "user_agent Nullable(String),",
        "screen_height Nullable(UInt32),",
        "screen_width Nullable(UInt32),",
        "viewport_height Nullable(UInt32),",
        "viewport_width Nullable(UInt32),",
        "screen_dpi Nullable(UInt32),",
        "ip Nullable(String),",
        "revenue Nullable(Float64),",
        "currency LowCardinality(Nullable(String)),",
        "price Nullable(Float64),",
        "quantity Nullable(Float64),",
        "product_id Nullable(String),",
        "revenue_type LowCardinality(Nullable(String)),",
        "experiment_id LowCardinality(Nullable(String)),",
        "variant LowCardinality(Nullable(String)),",
        "feature_flag LowCardinality(Nullable(String)),",
        "max_dynamic_paths = 8192,",
        "max_dynamic_types = 8",
        ")",
    ].join(" ");
    return [
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS event_id String DEFAULT '' CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS timestamp DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta(8), ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS observed_timestamp DateTime64(3, 'UTC') DEFAULT timestamp CODEC(Delta(8), ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS ingested_timestamp DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta(8), ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS source_file String DEFAULT '' CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS source_offset UInt64 DEFAULT 0 CODEC(Delta, ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS source_length UInt32 DEFAULT 0 CODEC(Delta, ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS data ${dataType}`,
        `ALTER TABLE ${target} MODIFY COLUMN data ${dataType}`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS tenant_id LowCardinality(String) MATERIALIZED ifNull(data.tenant_id, '')`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS event_type LowCardinality(String) MATERIALIZED ifNull(data.event_type, '')`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS trace_id String MATERIALIZED ifNull(data.trace_id, '') CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS span_id String MATERIALIZED ifNull(data.span_id, '') CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS signal LowCardinality(String) MATERIALIZED multiIf(ifNull(data.event_type, '') IN ('span', 'span_start', 'span_end'), 'trace', ifNull(data.event_type, '') = 'metric', 'metric', ifNull(data.event_type, '') = 'log', 'log', ifNull(data.event_type, '') IN ('analytics', 'track', 'page', 'screen', 'identify', 'group', 'alias'), 'analytics', 'other')`,
        `ALTER TABLE ${target} MODIFY COLUMN event_id String CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} MODIFY COLUMN timestamp DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1))`,
        `ALTER TABLE ${target} MODIFY COLUMN observed_timestamp DateTime64(3, 'UTC') DEFAULT timestamp CODEC(Delta(8), ZSTD(1))`,
        `ALTER TABLE ${target} MODIFY COLUMN ingested_timestamp DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta(8), ZSTD(1))`,
        `ALTER TABLE ${target} MODIFY COLUMN tenant_id LowCardinality(String) MATERIALIZED ifNull(data.tenant_id, '')`,
        `ALTER TABLE ${target} MODIFY COLUMN event_type LowCardinality(String) MATERIALIZED ifNull(data.event_type, '')`,
        `ALTER TABLE ${target} MODIFY COLUMN trace_id String MATERIALIZED ifNull(data.trace_id, '') CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} MODIFY COLUMN span_id String MATERIALIZED ifNull(data.span_id, '') CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} MODIFY COLUMN signal LowCardinality(String) MATERIALIZED multiIf(ifNull(data.event_type, '') IN ('span', 'span_start', 'span_end'), 'trace', ifNull(data.event_type, '') = 'metric', 'metric', ifNull(data.event_type, '') = 'log', 'log', ifNull(data.event_type, '') IN ('analytics', 'track', 'page', 'screen', 'identify', 'group', 'alias'), 'analytics', 'other')`,
        `ALTER TABLE ${target} ADD INDEX IF NOT EXISTS idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1`,
        `ALTER TABLE ${target} ADD INDEX IF NOT EXISTS idx_span_id span_id TYPE bloom_filter(0.01) GRANULARITY 1`,
    ];
}
