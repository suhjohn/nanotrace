#!/usr/bin/env node
import { readFileSync } from "node:fs";
import path from "node:path";

const root = path.resolve(new URL("..", import.meta.url).pathname);

const clickhouseUrl = requiredEnv("CLICKHOUSE_URL");
const user = requiredEnv("CLICKHOUSE_USER");
const password = requiredEnv("CLICKHOUSE_PASSWORD");
const database = identifier(process.env.CLICKHOUSE_DATABASE || "observatory", "CLICKHOUSE_DATABASE");
const table = identifier(process.env.CLICKHOUSE_TABLE || "events", "CLICKHOUSE_TABLE");
const facetsTable = identifier(process.env.CLICKHOUSE_FACETS_TABLE || "event_facets", "CLICKHOUSE_FACETS_TABLE");
const eventIndexTable = identifier(
    process.env.CLICKHOUSE_EVENT_INDEX_TABLE || "event_facet_index",
    "CLICKHOUSE_EVENT_INDEX_TABLE",
);
const hotDimensionsTable = identifier(
    process.env.CLICKHOUSE_HOT_DIMENSIONS_TABLE || "hot_dimensions",
    "CLICKHOUSE_HOT_DIMENSIONS_TABLE",
);
const schemaPath = path.resolve(root, process.env.CLICKHOUSE_SCHEMA_PATH || "deploy/clickhouse/schema.sql");
const defaultTenantId = process.env.NANOTRACE_DEFAULT_TENANT_ID || "org_default";

const eventTableToken = "__NANOTRACE_EVENTS_TABLE__";
const facetsTableToken = "__NANOTRACE_EVENT_FACETS_TABLE__";
const eventIndexTableToken = "__NANOTRACE_EVENT_FACET_INDEX_TABLE__";
const hotDimensionsTableToken = "__NANOTRACE_HOT_DIMENSIONS_TABLE__";
const schema = readFileSync(schemaPath, "utf8")
    .replace(/\bobservatory\.hot_dimensions\b/g, hotDimensionsTableToken)
    .replace(/\bobservatory\.event_facet_index\b/g, eventIndexTableToken)
    .replace(/\bobservatory\.event_facets\b/g, facetsTableToken)
    .replace(/\bobservatory\.events\b/g, eventTableToken)
    .replaceAll(eventTableToken, `${quoteIdentifier(database)}.${quoteIdentifier(table)}`)
    .replaceAll(facetsTableToken, `${quoteIdentifier(database)}.${quoteIdentifier(facetsTable)}`)
    .replaceAll(eventIndexTableToken, `${quoteIdentifier(database)}.${quoteIdentifier(eventIndexTable)}`)
    .replaceAll(hotDimensionsTableToken, `${quoteIdentifier(database)}.${quoteIdentifier(hotDimensionsTable)}`);

await query(`CREATE DATABASE IF NOT EXISTS ${quoteIdentifier(database)}`);
const tenantMigrations = [
    await renameTenantlessTable(database, facetsTable, ["tenant_id"], "tenant_id"),
    await renameTenantlessTable(database, eventIndexTable, ["tenant_id"], "tenant_id"),
    await renameTenantlessTable(database, hotDimensionsTable, ["tenant_id"], "tenant_id"),
].filter(Boolean);
await recreateLegacyFacetTable(database, facetsTable);
for (const statement of splitStatements(schema)) {
    await query(statement);
}
for (const migration of tenantMigrations) {
    await copyTenantlessRows(migration);
}
for (const statement of compatibilityAlters(database, table)) {
    await query(statement);
}
for (const statement of facetCompatibilityAlters(database, facetsTable)) {
    await query(statement);
}
for (const statement of eventIndexCompatibilityAlters(database, eventIndexTable)) {
    await query(statement);
}
for (const statement of hotDimensionsCompatibilityAlters(database, hotDimensionsTable)) {
    await query(statement);
}

console.log(`clickhouse_schema=${database}.${table}`);
console.log(`clickhouse_facets_schema=${database}.${facetsTable}`);
console.log(`clickhouse_event_index_schema=${database}.${eventIndexTable}`);
console.log(`clickhouse_hot_dimensions_schema=${database}.${hotDimensionsTable}`);

async function query(sql) {
    await queryResponse(sql);
}

async function queryText(sql) {
    const response = await queryResponse(sql);
    return await response.text();
}

async function queryResponse(sql) {
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
    return response;
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

function sqlString(value) {
    return `'${String(value).replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
}

async function recreateLegacyFacetTable(database, table) {
    const engine = (
        await queryText(
            `SELECT engine FROM system.tables WHERE database = ${sqlString(database)} AND name = ${sqlString(table)} FORMAT TabSeparated`,
        )
    ).trim();
    if (!engine) {
        return;
    }

    const hasBucketTime = Number(
        (
            await queryText(
                `SELECT count() FROM system.columns WHERE database = ${sqlString(database)} AND table = ${sqlString(table)} AND name = 'bucket_time' FORMAT TabSeparated`,
            )
        ).trim(),
    );
    const hasCount = Number(
        (
            await queryText(
                `SELECT count() FROM system.columns WHERE database = ${sqlString(database)} AND table = ${sqlString(table)} AND name = 'count' FORMAT TabSeparated`,
            )
        ).trim(),
    );

    if (engine.endsWith("SummingMergeTree") && hasBucketTime && hasCount) {
        return;
    }

    await query(`DROP TABLE ${quoteIdentifier(database)}.${quoteIdentifier(table)}`);
}

async function renameTenantlessTable(database, table, requiredColumns, requiredSortingKeyFragment) {
    const row = (
        await queryText(
            `SELECT engine, sorting_key FROM system.tables WHERE database = ${sqlString(database)} AND name = ${sqlString(table)} FORMAT TabSeparated`,
        )
    ).trim();
    if (!row) {
        return null;
    }
    const [engine, sortingKey = ""] = row.split("\t");
    if (!engine) {
        return null;
    }
    const missingColumns = [];
    for (const column of requiredColumns) {
        const count = Number(
            (
                await queryText(
                    `SELECT count() FROM system.columns WHERE database = ${sqlString(database)} AND table = ${sqlString(table)} AND name = ${sqlString(column)} FORMAT TabSeparated`,
                )
            ).trim(),
        );
        if (count === 0) {
            missingColumns.push(column);
        }
    }
    const hasRequiredSortingKey = sortingKey
        .split(",")
        .map((part) => part.trim().replaceAll("`", "").replace(/^[()]+|[()]+$/g, ""))
        .includes(requiredSortingKeyFragment);
    if (missingColumns.length === 0 && hasRequiredSortingKey) {
        return null;
    }

    const legacyTable = `${table}_legacy_${Date.now()}`;
    await query(
        `RENAME TABLE ${quoteIdentifier(database)}.${quoteIdentifier(table)} TO ${quoteIdentifier(database)}.${quoteIdentifier(legacyTable)}`,
    );
    return { database, table, legacyTable };
}

async function copyTenantlessRows({ database, table, legacyTable }) {
    const target = `${quoteIdentifier(database)}.${quoteIdentifier(table)}`;
    const source = `${quoteIdentifier(database)}.${quoteIdentifier(legacyTable)}`;
    const tenant = sqlString(defaultTenantId);
    if (table === facetsTable) {
        await query(
            `INSERT INTO ${target} (tenant_id, bucket_time, key, value, value_type, count, error_count)
             SELECT ${tenant}, bucket_time, key, value, value_type, count, error_count FROM ${source}`,
        );
    } else if (table === eventIndexTable) {
        await query(
            `INSERT INTO ${target} (tenant_id, key, value, value_type, timestamp, bucket_time, event_id, event_type, signal, trace_id, span_id, parent_span_id, name, start_time, end_time, duration_ms)
             SELECT ${tenant}, key, value, value_type, timestamp, bucket_time, event_id, event_type, signal, trace_id, span_id, parent_span_id, name, start_time, end_time, duration_ms FROM ${source}`,
        );
    } else if (table === hotDimensionsTable) {
        await query(
            `INSERT INTO ${target} (tenant_id, path, value_type, status, display_name, source, created_at, updated_at, created_by, error)
             SELECT ${tenant}, path, value_type, status, display_name, source, created_at, updated_at, created_by, error FROM ${source}`,
        );
    }
    await query(`DROP TABLE ${source}`);
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
        `ALTER TABLE ${target} ADD INDEX IF NOT EXISTS idx_event_id event_id TYPE bloom_filter(0.01) GRANULARITY 1`,
    ];
}

function facetCompatibilityAlters(database, table) {
    const target = `${quoteIdentifier(database)}.${quoteIdentifier(table)}`;
    return [
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS bucket_time DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta(8), ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS key String DEFAULT '' CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS value String DEFAULT '' CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS value_type LowCardinality(String) DEFAULT 'string'`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS count UInt64 DEFAULT 0 CODEC(Delta, ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS error_count UInt64 DEFAULT 0 CODEC(Delta, ZSTD(1))`,
        `ALTER TABLE ${target} MODIFY COLUMN bucket_time DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1))`,
        `ALTER TABLE ${target} MODIFY COLUMN key String CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} MODIFY COLUMN value String CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} MODIFY COLUMN count UInt64 CODEC(Delta, ZSTD(1))`,
        `ALTER TABLE ${target} MODIFY COLUMN error_count UInt64 CODEC(Delta, ZSTD(1))`,
    ];
}

function eventIndexCompatibilityAlters(database, table) {
    const target = `${quoteIdentifier(database)}.${quoteIdentifier(table)}`;
    return [
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS key LowCardinality(String) DEFAULT ''`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS value String DEFAULT '' CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS value_type LowCardinality(String) DEFAULT 'string'`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS timestamp DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta(8), ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS bucket_time DateTime64(3, 'UTC') DEFAULT timestamp CODEC(Delta(8), ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS event_id String DEFAULT '' CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS event_type LowCardinality(String) DEFAULT ''`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS signal LowCardinality(String) DEFAULT ''`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS trace_id String DEFAULT '' CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS span_id String DEFAULT '' CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS parent_span_id String DEFAULT '' CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS name String DEFAULT '' CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS start_time Nullable(DateTime64(3, 'UTC')) CODEC(Delta(8), ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS end_time Nullable(DateTime64(3, 'UTC')) CODEC(Delta(8), ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS duration_ms Float64 DEFAULT 0 CODEC(ZSTD(1))`,
    ];
}

function hotDimensionsCompatibilityAlters(database, table) {
    const target = `${quoteIdentifier(database)}.${quoteIdentifier(table)}`;
    return [
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS path String DEFAULT '' CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS value_type LowCardinality(String) DEFAULT 'string'`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS status LowCardinality(String) DEFAULT 'active'`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS display_name String DEFAULT ''`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS source LowCardinality(String) DEFAULT 'user'`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS created_at DateTime64(3, 'UTC') DEFAULT now64(3)`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS updated_at DateTime64(3, 'UTC') DEFAULT now64(3)`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS created_by String DEFAULT ''`,
        `ALTER TABLE ${target} ADD COLUMN IF NOT EXISTS error String DEFAULT ''`,
        `ALTER TABLE ${target} MODIFY COLUMN path String CODEC(ZSTD(1))`,
        `ALTER TABLE ${target} MODIFY COLUMN value_type LowCardinality(String)`,
        `ALTER TABLE ${target} MODIFY COLUMN status LowCardinality(String)`,
        `ALTER TABLE ${target} MODIFY COLUMN source LowCardinality(String) DEFAULT 'user'`,
    ];
}
