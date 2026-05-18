#!/usr/bin/env node
import { readFileSync } from "node:fs";
import path from "node:path";

const root = path.resolve(new URL("..", import.meta.url).pathname);

const clickhouseUrl = requiredEnv("CLICKHOUSE_URL");
const user = requiredEnv("CLICKHOUSE_USER");
const password = requiredEnv("CLICKHOUSE_PASSWORD");
const database = identifier(process.env.CLICKHOUSE_DATABASE || "observatory", "CLICKHOUSE_DATABASE");
const eventsTable = identifier(process.env.CLICKHOUSE_TABLE || "events", "CLICKHOUSE_TABLE");
const schemaPath = path.resolve(root, process.env.CLICKHOUSE_SCHEMA_PATH || "deploy/clickhouse/schema.sql");
const resetDatabase = boolEnv("CLICKHOUSE_RESET_DATABASE") || boolEnv("CLICKHOUSE_DROP_SCHEMA");

const eventsTableToken = "__NANOTRACE_EVENTS_TABLE__";
const schema = readFileSync(schemaPath, "utf8")
    .replace(/\bobservatory\.events\b/g, eventsTableToken)
    .replaceAll(eventsTableToken, `${quoteIdentifier(database)}.${quoteIdentifier(eventsTable)}`);

if (resetDatabase) {
    await query(`DROP DATABASE IF EXISTS ${quoteIdentifier(database)}`);
}
await query(`CREATE DATABASE IF NOT EXISTS ${quoteIdentifier(database)}`);
for (const statement of splitStatements(schema)) {
    await query(statement);
}
await applySchemaMigrations();

console.log(`clickhouse_schema=${database}.${eventsTable}`);
console.log(`clickhouse_schema_path=${path.relative(root, schemaPath)}`);
console.log(`clickhouse_reset_database=${resetDatabase ? "true" : "false"}`);

async function applySchemaMigrations() {
    const lakehouseCommitsTable = `${quoteIdentifier(database)}.${quoteIdentifier("lakehouse_commits")}`;
    await query(
        `ALTER TABLE ${lakehouseCommitsTable} ADD COLUMN IF NOT EXISTS data_files Array(String) DEFAULT [] CODEC(ZSTD(1)) AFTER data_file`
    );
    await query(
        `ALTER TABLE ${lakehouseCommitsTable} ADD COLUMN IF NOT EXISTS metadata_location String DEFAULT '' CODEC(ZSTD(1)) AFTER content_sha256`
    );
    await query(
        `ALTER TABLE ${lakehouseCommitsTable} ADD COLUMN IF NOT EXISTS source_batch_id String DEFAULT '' CODEC(ZSTD(1)) AFTER metadata_location`
    );
    await query(
        `ALTER TABLE ${lakehouseCommitsTable} ADD COLUMN IF NOT EXISTS deduplicated UInt8 DEFAULT 0 CODEC(T64, ZSTD(1)) AFTER source_batch_id`
    );
}

async function query(sql) {
    const url = new URL(clickhouseUrl);
    url.searchParams.set("type_json_skip_duplicated_paths", "1");
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

function boolEnv(key) {
    const value = process.env[key];
    return value === "1" || value === "true" || value === "TRUE" || value === "yes" || value === "YES";
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
