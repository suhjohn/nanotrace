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

const eventsTableToken = "__NANOTRACE_EVENTS_TABLE__";
const schema = readFileSync(schemaPath, "utf8")
    .replace(/\bobservatory\.events\b/g, eventsTableToken)
    .replaceAll(eventsTableToken, `${quoteIdentifier(database)}.${quoteIdentifier(eventsTable)}`);

await query(`CREATE DATABASE IF NOT EXISTS ${quoteIdentifier(database)}`);
for (const statement of splitStatements(schema)) {
    await query(statement);
}

console.log(`clickhouse_schema=${database}.${eventsTable}`);
console.log(`clickhouse_event_index_schema=${database}.event_index`);
console.log(`clickhouse_field_index_schema=${database}.field_index`);
console.log(`clickhouse_field_counts_schema=${database}.field_counts_5m`);
console.log(`clickhouse_event_measures_schema=${database}.event_measures`);

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
