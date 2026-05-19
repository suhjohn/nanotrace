use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlparser::{dialect::ClickHouseDialect, parser::Parser};
use tracing::warn;

use crate::config::Config;

#[derive(Clone)]
pub struct ReadStore {
    cfg: Arc<Config>,
    http: reqwest::Client,
    s3: S3Client,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QueryRequest {
    pub query: String,
    #[serde(default)]
    pub parameters: serde_json::Map<String, Value>,
    #[serde(default)]
    pub allow_stale_serving: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("ClickHouse is not configured")]
    ClickHouseNotConfigured,
    #[error("S3 bucket is not configured")]
    S3NotConfigured,
    #[error("{0}")]
    InvalidQuery(String),
    #[error("ClickHouse request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("ClickHouse query failed: {status} {body}")]
    ClickHouseResponse { status: StatusCode, body: String },
    #[error("event not found")]
    NotFound,
    #[error("event source range is missing")]
    MissingSourceRange,
    #[error("S3 get object failed: {0}")]
    S3(String),
    #[error("invalid stored event JSON: {0}")]
    InvalidStoredEvent(serde_json::Error),
    #[error("stored event_id does not match requested event_id")]
    EventIDMismatch,
    #[error("invalid ClickHouse response")]
    InvalidClickHouseResponse,
}

impl ReadStore {
    pub fn new(cfg: Arc<Config>, s3: S3Client) -> Self {
        Self {
            cfg,
            http: reqwest::Client::new(),
            s3,
        }
    }

    pub async fn query(&self, request: QueryRequest, tenant_id: &str) -> Result<Value, ReadError> {
        let query = checked_select_query(&request.query)?;
        let query = normalize_prewhere(&query);
        let sources = query_sources(&query);
        validate_query_sources(&query, &self.allowed_table_names())?;
        let usage_shape = query_usage_shape(&query);
        let usage_hash = query_shape_hash(&usage_shape);
        let parameter_types = parameter_types(&request.parameters);
        let started_at = Instant::now();
        if !request.allow_stale_serving {
            if let Err(err) = self.ensure_serving_fresh(&sources).await {
                self.record_query_usage(QueryUsageRecord {
                    tenant_id,
                    query_shape: &usage_shape,
                    query_hash: usage_hash,
                    source_tables: &sources,
                    parameter_types: &parameter_types,
                    elapsed_ms: elapsed_ms(started_at.elapsed()),
                    result_rows: 0,
                    read_rows: 0,
                    read_bytes: 0,
                    status: query_error_status(&err),
                    allow_stale_serving: request.allow_stale_serving,
                })
                .await;
                return Err(err);
            }
        }
        let query = self.scope_query(&query);
        let mut parameters = request.parameters;
        parameters.insert(
            "__nanotrace_tenant_id".to_string(),
            Value::String(tenant_id.to_string()),
        );
        let text = match self.clickhouse_query(&query, &parameters).await {
            Ok(text) => text,
            Err(err) => {
                self.record_query_usage(QueryUsageRecord {
                    tenant_id,
                    query_shape: &usage_shape,
                    query_hash: usage_hash,
                    source_tables: &sources,
                    parameter_types: &parameter_types,
                    elapsed_ms: elapsed_ms(started_at.elapsed()),
                    result_rows: 0,
                    read_rows: 0,
                    read_bytes: 0,
                    status: query_error_status(&err),
                    allow_stale_serving: request.allow_stale_serving,
                })
                .await;
                return Err(err);
            }
        };
        let response: Value = match serde_json::from_str(&text) {
            Ok(response) => response,
            Err(err) => {
                self.record_query_usage(QueryUsageRecord {
                    tenant_id,
                    query_shape: &usage_shape,
                    query_hash: usage_hash,
                    source_tables: &sources,
                    parameter_types: &parameter_types,
                    elapsed_ms: elapsed_ms(started_at.elapsed()),
                    result_rows: 0,
                    read_rows: 0,
                    read_bytes: 0,
                    status: "invalid_clickhouse_json",
                    allow_stale_serving: request.allow_stale_serving,
                })
                .await;
                return Err(ReadError::InvalidStoredEvent(err));
            }
        };
        let stats = query_response_stats(&response);
        self.record_query_usage(QueryUsageRecord {
            tenant_id,
            query_shape: &usage_shape,
            query_hash: usage_hash,
            source_tables: &sources,
            parameter_types: &parameter_types,
            elapsed_ms: elapsed_ms(started_at.elapsed()),
            result_rows: stats.result_rows,
            read_rows: stats.read_rows,
            read_bytes: stats.read_bytes,
            status: "ok",
            allow_stale_serving: request.allow_stale_serving,
        })
        .await;
        Ok(response)
    }

    pub async fn event_bytes(&self, event_id: &str, tenant_id: &str) -> Result<Bytes, ReadError> {
        if event_id.trim().is_empty() {
            return Err(ReadError::InvalidQuery("event_id is required".to_string()));
        }

        match self.event_bytes_from_s3(event_id, tenant_id).await {
            Ok(bytes) => Ok(bytes),
            Err(ReadError::NotFound) => Err(ReadError::NotFound),
            Err(_) => self.event_bytes_from_clickhouse(event_id, tenant_id).await,
        }
    }

    async fn event_bytes_from_s3(
        &self,
        event_id: &str,
        tenant_id: &str,
    ) -> Result<Bytes, ReadError> {
        let pointer = self.event_pointer(event_id, tenant_id).await?;
        let bucket = self
            .cfg
            .s3_bucket
            .as_deref()
            .ok_or(ReadError::S3NotConfigured)?;
        let end = pointer
            .source_offset
            .checked_add(u64::from(pointer.source_length))
            .and_then(|value| value.checked_sub(1))
            .ok_or(ReadError::MissingSourceRange)?;
        let range = format!("bytes={}-{}", pointer.source_offset, end);

        let response = self
            .s3
            .get_object()
            .bucket(bucket)
            .key(&pointer.source_file)
            .range(range)
            .send()
            .await
            .map_err(|err| ReadError::S3(err.to_string()))?;
        let bytes = response
            .body
            .collect()
            .await
            .map_err(|err| ReadError::S3(err.to_string()))?
            .into_bytes();

        validate_event_bytes(event_id, &bytes)?;
        Ok(bytes)
    }

    async fn event_bytes_from_clickhouse(
        &self,
        event_id: &str,
        tenant_id: &str,
    ) -> Result<Bytes, ReadError> {
        let mut parameters = serde_json::Map::new();
        parameters.insert("event_id".to_string(), Value::String(event_id.to_string()));
        parameters.insert(
            "__nanotrace_tenant_id".to_string(),
            Value::String(tenant_id.to_string()),
        );
        let query = format!(
            "SELECT event_id, timestamp, observed_timestamp, ingested_timestamp, source_file, source_offset, source_length, data FROM {} WHERE tenant_id = {{__nanotrace_tenant_id:String}} AND event_id = {{event_id:String}} ORDER BY timestamp ASC LIMIT 1",
            self.table_name()
        );
        let text = self.clickhouse_query(&query, &parameters).await?;
        let response: ClickHouseResponse<Value> =
            serde_json::from_str(&text).map_err(ReadError::InvalidStoredEvent)?;
        let event = response
            .data
            .into_iter()
            .next()
            .ok_or(ReadError::NotFound)?;
        let bytes = serde_json::to_vec(&event).map_err(ReadError::InvalidStoredEvent)?;
        validate_event_bytes(event_id, &bytes)?;
        Ok(Bytes::from(bytes))
    }

    async fn event_pointer(
        &self,
        event_id: &str,
        tenant_id: &str,
    ) -> Result<EventPointer, ReadError> {
        let mut parameters = serde_json::Map::new();
        parameters.insert("event_id".to_string(), Value::String(event_id.to_string()));
        parameters.insert(
            "__nanotrace_tenant_id".to_string(),
            Value::String(tenant_id.to_string()),
        );
        let query = format!(
            "SELECT source_file, source_offset, source_length FROM {} WHERE tenant_id = {{__nanotrace_tenant_id:String}} AND event_id = {{event_id:String}} ORDER BY timestamp ASC, source_file ASC, source_offset ASC LIMIT 1",
            self.table_name()
        );
        let text = self.clickhouse_query(&query, &parameters).await?;
        let response: ClickHouseResponse<EventPointer> =
            serde_json::from_str(&text).map_err(ReadError::InvalidStoredEvent)?;
        match response.data.len() {
            0 => Err(ReadError::NotFound),
            1 => {
                let pointer = response
                    .data
                    .into_iter()
                    .next()
                    .ok_or(ReadError::InvalidClickHouseResponse)?;
                if pointer.source_file.is_empty() || pointer.source_length == 0 {
                    return Err(ReadError::MissingSourceRange);
                }
                Ok(pointer)
            }
            _ => Err(ReadError::InvalidClickHouseResponse),
        }
    }

    async fn clickhouse_query(
        &self,
        query: &str,
        parameters: &serde_json::Map<String, Value>,
    ) -> Result<String, ReadError> {
        let url = self
            .cfg
            .clickhouse_url
            .as_deref()
            .ok_or(ReadError::ClickHouseNotConfigured)?;
        let mut request = self
            .http
            .post(url)
            .query(&[
                ("database", self.cfg.clickhouse_database.as_str()),
                ("readonly", "1"),
                ("type_json_skip_duplicated_paths", "1"),
                (
                    "max_execution_time",
                    &self.cfg.clickhouse_max_execution_secs.to_string(),
                ),
                (
                    "max_result_rows",
                    &self.cfg.clickhouse_max_result_rows.to_string(),
                ),
                ("result_overflow_mode", "break"),
                (
                    "max_bytes_to_read",
                    &self.cfg.clickhouse_max_bytes_to_read.to_string(),
                ),
            ])
            .body(format!("{query} FORMAT JSON"));

        if let Some(user) = self.cfg.clickhouse_user.as_deref() {
            request = request.basic_auth(user, self.cfg.clickhouse_password.as_deref());
        }

        for (key, value) in parameters {
            validate_parameter_name(key)?;
            request = request.query(&[(format!("param_{key}"), parameter_value(value)?)]);
        }

        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(ReadError::ClickHouseResponse { status, body });
        }
        Ok(body)
    }

    async fn record_query_usage(&self, record: QueryUsageRecord<'_>) {
        let Some(url) = self.cfg.clickhouse_url.as_deref() else {
            return;
        };

        let row = QueryUsageRow {
            tenant_id: record.tenant_id,
            query_id: query_usage_id(record.query_hash),
            query_hash: record.query_hash,
            query_shape: record.query_shape,
            source_tables: record.source_tables,
            result_rows: record.result_rows,
            read_rows: record.read_rows,
            read_bytes: record.read_bytes,
            elapsed_ms: record.elapsed_ms,
            status: record.status,
            error: "",
            attributes: serde_json::json!({
                "allow_stale_serving": record.allow_stale_serving,
                "parameter_types": record.parameter_types,
                "sanitizer": "sqlparser-clickhouse-token-shape-v1",
            }),
        };
        let mut row_body = match serde_json::to_vec(&row) {
            Ok(row_body) => row_body,
            Err(err) => {
                warn!(error = %err, "failed to serialize query usage row");
                return;
            }
        };
        row_body.push(b'\n');

        let table = format!("{}.query_usage", self.cfg.clickhouse_database);
        let query = format!("INSERT INTO {table} FORMAT JSONEachRow");
        let mut body = Vec::with_capacity(query.len() + 1 + row_body.len());
        body.extend_from_slice(query.as_bytes());
        body.push(b'\n');
        body.extend_from_slice(&row_body);
        let mut request = self
            .http
            .post(url)
            .query(&[("database", self.cfg.clickhouse_database.as_str())])
            .body(body);
        if let Some(user) = self.cfg.clickhouse_user.as_deref() {
            request = request.basic_auth(user, self.cfg.clickhouse_password.as_deref());
        }
        match request.send().await {
            Ok(response) if response.status().is_success() => {}
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                warn!(%status, body = %body, "failed to insert query usage row");
            }
            Err(err) => {
                warn!(error = %err, "failed to insert query usage row");
            }
        }
    }

    fn table_name(&self) -> String {
        format!(
            "{}.{}",
            self.cfg.clickhouse_database, self.cfg.clickhouse_table
        )
    }

    async fn ensure_serving_fresh(&self, sources: &[String]) -> Result<(), ReadError> {
        let requested = sources
            .iter()
            .filter_map(|source| self.guarded_serving_table(source))
            .collect::<BTreeSet<_>>();
        if requested.is_empty() {
            return Ok(());
        }

        let latest_sequence = self.latest_lakehouse_sequence().await?;
        if latest_sequence == 0 {
            return Ok(());
        }

        let watermarks = self.serving_sequences(&requested).await?;
        for table in requested {
            let serving_sequence = watermarks.get(&table).copied().unwrap_or(0);
            if serving_sequence < latest_sequence {
                return Err(ReadError::InvalidQuery(format!(
                    "serving table {table} is stale: source_sequence_number={serving_sequence}, lakehouse_sequence_number={latest_sequence}"
                )));
            }
        }
        Ok(())
    }

    async fn latest_lakehouse_sequence(&self) -> Result<u64, ReadError> {
        let query = format!(
            "SELECT ifNull(max(sequence_number), 0) AS sequence_number FROM {} WHERE namespace = 'nanotrace' AND table_name = 'events'",
            self.lakehouse_commits_table_name()
        );
        let parameters = serde_json::Map::new();
        let text = self.clickhouse_query(&query, &parameters).await?;
        let response: ClickHouseResponse<LakehouseSequenceRow> =
            serde_json::from_str(&text).map_err(ReadError::InvalidStoredEvent)?;
        Ok(response
            .data
            .into_iter()
            .next()
            .map(|row| row.sequence_number)
            .unwrap_or(0))
    }

    async fn serving_sequences(
        &self,
        requested: &BTreeSet<String>,
    ) -> Result<HashMap<String, u64>, ReadError> {
        let serving_tables = requested
            .iter()
            .map(|table| format!("'{}'", table.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(",");
        let query = format!(
            "SELECT serving_table, max(source_sequence_number) AS source_sequence_number FROM {} WHERE source_namespace = 'nanotrace' AND source_table = 'events' AND serving_table IN ({serving_tables}) GROUP BY serving_table",
            self.serving_watermarks_table_name()
        );
        let parameters = serde_json::Map::new();
        let text = self.clickhouse_query(&query, &parameters).await?;
        let response: ClickHouseResponse<ServingSequenceRow> =
            serde_json::from_str(&text).map_err(ReadError::InvalidStoredEvent)?;
        Ok(response
            .data
            .into_iter()
            .map(|row| (row.serving_table, row.source_sequence_number))
            .collect())
    }

    fn guarded_serving_table(&self, source: &str) -> Option<String> {
        let source = self.unqualified_source(source);
        let events_table = self.cfg.clickhouse_table.as_str();
        if source.eq_ignore_ascii_case(events_table) {
            return Some(events_table.to_string());
        }
        ["field_index", "event_measures", "entity_state_updates"]
            .into_iter()
            .find(|table| source.eq_ignore_ascii_case(table))
            .map(str::to_string)
    }

    fn unqualified_source<'a>(&self, source: &'a str) -> &'a str {
        source
            .strip_prefix(&format!("{}.", self.cfg.clickhouse_database))
            .unwrap_or(source)
    }

    fn lakehouse_commits_table_name(&self) -> String {
        format!("{}.lakehouse_commits", self.cfg.clickhouse_database)
    }

    fn serving_watermarks_table_name(&self) -> String {
        format!("{}.serving_watermarks", self.cfg.clickhouse_database)
    }

    fn scope_query(&self, query: &str) -> String {
        let tables = self.allowed_table_names();
        let mut scoped = query.to_string();
        for table in tables.iter() {
            let scoped_table = self.scoped_table_query(table);
            for keyword in ["FROM", "JOIN"] {
                scoped = scoped.replace(
                    &format!("{keyword} {table}"),
                    &format!("{keyword} {scoped_table}"),
                );
                scoped = scoped.replace(
                    &format!("{} {table}", keyword.to_ascii_lowercase()),
                    &format!("{} {scoped_table}", keyword.to_ascii_lowercase()),
                );
            }
        }
        scoped
    }

    fn scoped_table_query(&self, table: &str) -> String {
        let events_table = self.cfg.clickhouse_table.as_str();
        let qualified_events_table = self.table_name();
        let projection = if table.eq_ignore_ascii_case(events_table)
            || table.eq_ignore_ascii_case(&qualified_events_table)
        {
            "*, tenant_id, event_type, trace_id, span_id, signal"
        } else {
            "*"
        };
        format!(
            "(SELECT {projection} FROM {table} WHERE tenant_id = {{__nanotrace_tenant_id:String}})"
        )
    }

    fn allowed_table_names(&self) -> Vec<String> {
        const ANALYTICS_TABLES: &[&str] = &[
            "field_index",
            "field_values",
            "field_density_1s",
            "field_topk_1m",
            "flamegraph_rollups_1m",
            "event_density_1s",
            "definitions",
            "event_measures",
            "measure_rollups",
            "entity_state_updates",
            "report_results",
            "sequence_report_results",
            "cohort_memberships",
            "definition_stats",
            "query_usage",
            "optimization_recommendations",
            "materialization_jobs",
            "materialization_chunks",
            "materialization_versions",
            "materialization_watermarks",
            "pipeline_metrics",
            "lakehouse_commits",
            "serving_watermarks",
        ];

        [self.cfg.clickhouse_table.as_str()]
            .into_iter()
            .chain(ANALYTICS_TABLES.iter().copied())
            .flat_map(|table| {
                [
                    table.to_string(),
                    format!("{}.{}", self.cfg.clickhouse_database, table),
                ]
            })
            .collect()
    }
}

#[derive(Debug, Deserialize)]
struct ClickHouseResponse<T> {
    data: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct EventPointer {
    source_file: String,
    source_offset: u64,
    source_length: u32,
}

#[derive(Debug, Deserialize)]
struct LakehouseSequenceRow {
    sequence_number: u64,
}

#[derive(Debug, Deserialize)]
struct ServingSequenceRow {
    serving_table: String,
    source_sequence_number: u64,
}

struct QueryUsageRecord<'a> {
    tenant_id: &'a str,
    query_shape: &'a str,
    query_hash: u64,
    source_tables: &'a [String],
    parameter_types: &'a Value,
    elapsed_ms: u64,
    result_rows: u64,
    read_rows: u64,
    read_bytes: u64,
    status: &'static str,
    allow_stale_serving: bool,
}

#[derive(Serialize)]
struct QueryUsageRow<'a> {
    tenant_id: &'a str,
    query_id: String,
    query_hash: u64,
    query_shape: &'a str,
    source_tables: &'a [String],
    result_rows: u64,
    read_rows: u64,
    read_bytes: u64,
    elapsed_ms: u64,
    status: &'a str,
    error: &'a str,
    attributes: Value,
}

#[derive(Default)]
struct QueryResponseStats {
    result_rows: u64,
    read_rows: u64,
    read_bytes: u64,
}

fn query_usage_shape(query: &str) -> String {
    let parser_query = query_for_parser(query);
    let shape = sanitize_sql_shape(query);
    if Parser::parse_sql(&ClickHouseDialect {}, &parser_query).is_err() {
        return format!("parse_failed {shape}");
    }
    shape
}

fn query_for_parser(query: &str) -> String {
    replace_parameters_for_parser(query, "0")
}

fn replace_parameters_for_parser(query: &str, replacement: &str) -> String {
    let chars: Vec<char> = query.chars().collect();
    let mut out = String::with_capacity(query.len());
    let mut index = 0;
    while index < chars.len() {
        match chars[index] {
            '\'' => {
                out.push('\'');
                index += 1;
                while index < chars.len() {
                    let current = chars[index];
                    out.push(current);
                    if current == '\\' && index + 1 < chars.len() {
                        index += 1;
                        out.push(chars[index]);
                    } else if current == '\'' {
                        if chars.get(index + 1) == Some(&'\'') {
                            index += 1;
                            out.push('\'');
                        } else {
                            index += 1;
                            break;
                        }
                    }
                    index += 1;
                }
            }
            '-' if chars.get(index + 1) == Some(&'-') => {
                while index < chars.len() && chars[index] != '\n' {
                    out.push(chars[index]);
                    index += 1;
                }
            }
            '/' if chars.get(index + 1) == Some(&'*') => {
                out.push('/');
                out.push('*');
                index += 2;
                while index < chars.len() {
                    out.push(chars[index]);
                    if chars[index] == '*' && chars.get(index + 1) == Some(&'/') {
                        index += 1;
                        out.push('/');
                        index += 1;
                        break;
                    }
                    index += 1;
                }
            }
            '{' => {
                if let Some(end) = parameter_end(&chars, index) {
                    out.push_str(replacement);
                    index = end + 1;
                } else {
                    out.push(chars[index]);
                    index += 1;
                }
            }
            ch => {
                out.push(ch);
                index += 1;
            }
        }
    }
    out
}

fn sanitize_sql_shape(query: &str) -> String {
    let chars: Vec<char> = query.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < chars.len() {
        let ch = chars[index];
        if ch.is_whitespace() {
            index += 1;
            continue;
        }
        if ch == '-' && chars.get(index + 1) == Some(&'-') {
            index += 2;
            while index < chars.len() && chars[index] != '\n' {
                index += 1;
            }
            continue;
        }
        if ch == '/' && chars.get(index + 1) == Some(&'*') {
            index += 2;
            while index + 1 < chars.len() {
                if chars[index] == '*' && chars[index + 1] == '/' {
                    index += 2;
                    break;
                }
                index += 1;
            }
            continue;
        }
        if ch == '\'' {
            tokens.push("?string".to_string());
            index += 1;
            while index < chars.len() {
                let current = chars[index];
                if current == '\\' && index + 1 < chars.len() {
                    index += 2;
                    continue;
                }
                if current == '\'' {
                    if chars.get(index + 1) == Some(&'\'') {
                        index += 2;
                    } else {
                        index += 1;
                        break;
                    }
                } else {
                    index += 1;
                }
            }
            continue;
        }
        if ch == '"' || ch == '`' {
            tokens.push("?identifier".to_string());
            let quote = ch;
            index += 1;
            while index < chars.len() {
                let current = chars[index];
                index += 1;
                if current == quote {
                    break;
                }
            }
            continue;
        }
        if ch == '{' {
            if let Some(end) = parameter_end(&chars, index) {
                let raw = chars[index + 1..end].iter().collect::<String>();
                tokens.push(sanitize_parameter(&raw));
                index = end + 1;
                continue;
            }
        }
        if ch.is_ascii_digit()
            || (ch == '.'
                && chars
                    .get(index + 1)
                    .is_some_and(|next| next.is_ascii_digit()))
        {
            tokens.push("?number".to_string());
            index = consume_number(&chars, index);
            continue;
        }
        if is_identifier_start(ch) {
            let start = index;
            index += 1;
            while index < chars.len() && is_identifier_continue(chars[index]) {
                index += 1;
            }
            tokens.push(chars[start..index].iter().collect::<String>());
            continue;
        }
        if let Some(next) = chars.get(index + 1) {
            let pair = [ch, *next].iter().collect::<String>();
            if matches!(
                pair.as_str(),
                ">=" | "<=" | "!=" | "<>" | "==" | "||" | "&&" | "::" | "->"
            ) {
                tokens.push(pair);
                index += 2;
                continue;
            }
        }
        tokens.push(ch.to_string());
        index += 1;
    }
    compact_shape_tokens(tokens)
}

fn parameter_end(chars: &[char], start: usize) -> Option<usize> {
    let mut index = start + 1;
    while index < chars.len() {
        match chars[index] {
            '}' => return Some(index),
            '\n' | '\r' => return None,
            _ => index += 1,
        }
    }
    None
}

fn sanitize_parameter(raw: &str) -> String {
    let mut parts = raw.splitn(2, ':');
    let name = parts.next().unwrap_or_default().trim();
    let kind = parts.next().unwrap_or_default().trim();
    if valid_parameter_part(name) && !kind.is_empty() && valid_parameter_type(kind) {
        format!("{{{name}:{kind}}}")
    } else if valid_parameter_part(name) {
        format!("{{{name}}}")
    } else {
        "{param}".to_string()
    }
}

fn valid_parameter_part(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 80
        && value
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn valid_parameter_type(value: &str) -> bool {
    value.len() <= 120
        && value.chars().all(|ch| {
            ch == '_'
                || ch == '('
                || ch == ')'
                || ch == ','
                || ch == ' '
                || ch.is_ascii_alphanumeric()
        })
}

fn consume_number(chars: &[char], start: usize) -> usize {
    let mut index = start;
    if chars[index] == '0' && matches!(chars.get(index + 1), Some('x' | 'X')) {
        index += 2;
        while index < chars.len() && chars[index].is_ascii_hexdigit() {
            index += 1;
        }
        return index;
    }
    while index < chars.len() && (chars[index].is_ascii_digit() || chars[index] == '.') {
        index += 1;
    }
    if matches!(chars.get(index), Some('e' | 'E')) {
        let exp = index + 1;
        let digits = if matches!(chars.get(exp), Some('+' | '-')) {
            exp + 1
        } else {
            exp
        };
        if chars.get(digits).is_some_and(|ch| ch.is_ascii_digit()) {
            index = digits + 1;
            while index < chars.len() && chars[index].is_ascii_digit() {
                index += 1;
            }
        }
    }
    index
}

fn is_identifier_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_identifier_continue(ch: char) -> bool {
    ch == '_' || ch == '.' || ch.is_ascii_alphanumeric()
}

fn compact_shape_tokens(tokens: Vec<String>) -> String {
    let mut out = String::new();
    for token in tokens {
        let no_space_before = matches!(token.as_str(), ")" | "," | "." | "]");
        let no_space_after_previous = out
            .chars()
            .last()
            .is_some_and(|ch| matches!(ch, '(' | '.' | '['));
        if !out.is_empty() && !no_space_before && !no_space_after_previous {
            out.push(' ');
        }
        out.push_str(&token);
    }
    if out.len() > 4096 {
        out.truncate(4096);
        out.push_str("...");
    }
    out
}

fn query_shape_hash(query_shape: &str) -> u64 {
    let digest = Sha256::digest(query_shape.as_bytes());
    u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("sha256 digest has eight bytes"),
    )
}

fn query_usage_id(query_hash: u64) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!("qry_{millis}_{query_hash:016x}")
}

fn parameter_types(parameters: &serde_json::Map<String, Value>) -> Value {
    let values = parameters
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                Value::String(
                    match value {
                        Value::String(_) => "string",
                        Value::Number(_) => "number",
                        Value::Bool(_) => "bool",
                        Value::Null => "null",
                        Value::Array(_) => "array",
                        Value::Object(_) => "object",
                    }
                    .to_string(),
                ),
            )
        })
        .collect();
    Value::Object(values)
}

fn query_response_stats(response: &Value) -> QueryResponseStats {
    let result_rows = response
        .get("rows")
        .and_then(Value::as_u64)
        .or_else(|| {
            response
                .get("data")
                .and_then(Value::as_array)
                .map(|data| data.len() as u64)
        })
        .unwrap_or_default();
    let statistics = response.get("statistics");
    QueryResponseStats {
        result_rows,
        read_rows: statistics
            .and_then(|stats| stats.get("rows_read"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        read_bytes: statistics
            .and_then(|stats| stats.get("bytes_read"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    }
}

fn query_error_status(err: &ReadError) -> &'static str {
    match err {
        ReadError::InvalidQuery(_) => "invalid_query",
        ReadError::ClickHouseNotConfigured => "clickhouse_not_configured",
        ReadError::ClickHouseResponse { status, .. } if status.is_client_error() => {
            "clickhouse_client_error"
        }
        ReadError::ClickHouseResponse { .. } => "clickhouse_server_error",
        ReadError::Http(_) => "clickhouse_request_error",
        ReadError::InvalidStoredEvent(_) => "invalid_clickhouse_json",
        ReadError::S3NotConfigured => "s3_not_configured",
        ReadError::S3(_) => "s3_error",
        ReadError::NotFound => "not_found",
        ReadError::MissingSourceRange => "missing_source_range",
        ReadError::EventIDMismatch => "event_id_mismatch",
        ReadError::InvalidClickHouseResponse => "invalid_clickhouse_response",
    }
}

fn elapsed_ms(elapsed: std::time::Duration) -> u64 {
    elapsed.as_millis().try_into().unwrap_or(u64::MAX)
}

fn checked_select_query(query: &str) -> Result<String, ReadError> {
    let query = trim_single_statement(query)?;
    let tokens = sql_tokens(&query);
    let first_keyword = tokens
        .first()
        .map(String::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    if first_keyword != "SELECT" && first_keyword != "WITH" {
        return Err(ReadError::InvalidQuery(
            "query must start with SELECT or WITH".to_string(),
        ));
    }

    if tokens.iter().any(|token| token == "FORMAT") {
        return Err(ReadError::InvalidQuery(
            "query must not include FORMAT; the server adds FORMAT JSON".to_string(),
        ));
    }
    for forbidden in [
        "ALTER", "ATTACH", "CREATE", "DELETE", "DETACH", "DROP", "GRANT", "INSERT", "KILL",
        "OPTIMIZE", "RENAME", "REVOKE", "SET", "SYSTEM", "TRUNCATE", "USE",
    ] {
        if tokens.iter().any(|token| token == forbidden) {
            return Err(ReadError::InvalidQuery(format!(
                "query must not include {forbidden}"
            )));
        }
    }
    if query.contains('`') || query.contains('"') {
        return Err(ReadError::InvalidQuery(
            "query must not include quoted identifiers".to_string(),
        ));
    }

    Ok(query)
}

fn normalize_prewhere(query: &str) -> String {
    let mut normalized = replace_keyword(query, "PREWHERE", "WHERE");
    while let Some(index) = duplicate_where_index(&normalized) {
        normalized.replace_range(index..index + "WHERE".len(), "AND");
    }
    normalized
}

fn replace_keyword(query: &str, keyword: &str, replacement: &str) -> String {
    let mut out = query.to_string();
    loop {
        let code = sql_code(&out);
        let Some(index) = find_keyword(&code, keyword) else {
            break;
        };
        out.replace_range(index..index + keyword.len(), replacement);
    }
    out
}

fn duplicate_where_index(query: &str) -> Option<usize> {
    let code = sql_code(query);
    let bytes = code.as_bytes();
    let mut depth = 0usize;
    let mut seen_where_by_depth = vec![false];
    let mut index = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'(' => {
                depth += 1;
                if seen_where_by_depth.len() <= depth {
                    seen_where_by_depth.push(false);
                } else {
                    seen_where_by_depth[depth] = false;
                }
                index += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                index += 1;
            }
            _ if keyword_at(&code, index, "SELECT") => {
                seen_where_by_depth[depth] = false;
                index += "SELECT".len();
            }
            _ if keyword_at(&code, index, "WHERE") => {
                if seen_where_by_depth[depth] {
                    return Some(index);
                }
                seen_where_by_depth[depth] = true;
                index += "WHERE".len();
            }
            _ => index += 1,
        }
    }
    None
}

fn keyword_at(value: &str, index: usize, keyword: &str) -> bool {
    let bytes = value.as_bytes();
    let end = index + keyword.len();
    if end > bytes.len() || !value[index..end].eq_ignore_ascii_case(keyword) {
        return false;
    }
    let before = index.checked_sub(1).and_then(|idx| bytes.get(idx)).copied();
    let after = bytes.get(end).copied();
    before.is_none_or(|ch| !is_identifier_byte(ch))
        && after.is_none_or(|ch| !is_identifier_byte(ch))
}

fn validate_query_sources(query: &str, allowed_tables: &[String]) -> Result<(), ReadError> {
    for source in query_sources(query) {
        if !allowed_tables
            .iter()
            .any(|allowed| source.eq_ignore_ascii_case(allowed))
        {
            return Err(ReadError::InvalidQuery(format!(
                "query source is not allowed: {source}"
            )));
        }
    }
    Ok(())
}

fn query_sources(query: &str) -> Vec<String> {
    let code = sql_code(query);
    let bytes = code.as_bytes();
    let mut sources = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let Some((keyword_start, keyword)) = next_source_keyword(&code[index..]) else {
            break;
        };
        index += keyword_start + keyword.len();
        loop {
            while index < bytes.len() && bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            if index >= bytes.len() || bytes[index] == b'(' {
                break;
            }
            let source_start = index;
            while index < bytes.len()
                && (bytes[index].is_ascii_alphanumeric()
                    || bytes[index] == b'_'
                    || bytes[index] == b'.')
            {
                index += 1;
            }
            let source = code[source_start..index].trim();
            if source.is_empty() {
                break;
            }
            sources.push(source.to_string());
            while index < bytes.len() && bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            if index >= bytes.len() || bytes[index] != b',' {
                break;
            }
            index += 1;
        }
    }
    sources
}

fn next_source_keyword(value: &str) -> Option<(usize, &'static str)> {
    let from = find_keyword(value, "FROM");
    let join = find_keyword(value, "JOIN");
    match (from, join) {
        (Some(from), Some(join)) if from <= join => Some((from, "FROM")),
        (Some(_), Some(join)) => Some((join, "JOIN")),
        (Some(from), None) => Some((from, "FROM")),
        (None, Some(join)) => Some((join, "JOIN")),
        (None, None) => None,
    }
}

fn find_keyword(value: &str, keyword: &str) -> Option<usize> {
    let upper = value.to_ascii_uppercase();
    let mut offset = 0;
    while let Some(index) = upper[offset..].find(keyword) {
        let absolute = offset + index;
        let before = absolute
            .checked_sub(1)
            .and_then(|idx| upper.as_bytes().get(idx))
            .copied();
        let after = upper.as_bytes().get(absolute + keyword.len()).copied();
        let before_boundary = before.is_none_or(|ch| !is_identifier_byte(ch));
        let after_boundary = after.is_none_or(|ch| !is_identifier_byte(ch));
        if before_boundary && after_boundary {
            return Some(absolute);
        }
        offset = absolute + keyword.len();
    }
    None
}

fn is_identifier_byte(value: u8) -> bool {
    value.is_ascii_alphanumeric() || value == b'_'
}

fn trim_single_statement(query: &str) -> Result<String, ReadError> {
    let mut query = query.trim();
    if query.is_empty() {
        return Err(ReadError::InvalidQuery("query is required".to_string()));
    }
    let code = sql_code(query);
    let mut semicolons = code.match_indices(';');
    if let Some((first_semicolon, _)) = semicolons.next()
        && (semicolons.next().is_some() || !code[first_semicolon + 1..].trim().is_empty())
    {
        return Err(ReadError::InvalidQuery(
            "query must contain exactly one statement".to_string(),
        ));
    }
    if let Some(stripped) = query.strip_suffix(';') {
        query = stripped.trim_end();
    }
    Ok(query.to_string())
}

fn sql_tokens(query: &str) -> Vec<String> {
    sql_code(query)
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_uppercase())
        .collect()
}

fn sql_code(query: &str) -> String {
    let chars: Vec<char> = query.chars().collect();
    let mut out = String::with_capacity(query.len());
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];
        if ch == '-' && chars.get(i + 1) == Some(&'-') {
            out.push(' ');
            out.push(' ');
            i += 2;
            while i < chars.len() && chars[i] != '\n' {
                out.push(' ');
                i += 1;
            }
            continue;
        }
        if ch == '/' && chars.get(i + 1) == Some(&'*') {
            out.push(' ');
            out.push(' ');
            i += 2;
            while i < chars.len() {
                if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    out.push(' ');
                    out.push(' ');
                    i += 2;
                    break;
                }
                out.push(if chars[i] == '\n' { '\n' } else { ' ' });
                i += 1;
            }
            continue;
        }
        if ch == '\'' || ch == '"' || ch == '`' {
            let quote = ch;
            out.push(' ');
            i += 1;
            while i < chars.len() {
                let current = chars[i];
                out.push(if current == '\n' { '\n' } else { ' ' });
                if current == '\\' && quote != '`' && i + 1 < chars.len() {
                    i += 1;
                    out.push(if chars[i] == '\n' { '\n' } else { ' ' });
                } else if current == quote {
                    if quote == '\'' && chars.get(i + 1) == Some(&'\'') {
                        i += 1;
                        out.push(' ');
                    } else {
                        i += 1;
                        break;
                    }
                }
                i += 1;
            }
            continue;
        }

        out.push(ch);
        i += 1;
    }

    out
}

fn validate_parameter_name(name: &str) -> Result<(), ReadError> {
    let valid = !name.is_empty()
        && name.chars().enumerate().all(|(index, ch)| {
            ch == '_' || ch.is_ascii_alphanumeric() && (index > 0 || ch.is_ascii_alphabetic())
        });
    if valid {
        Ok(())
    } else {
        Err(ReadError::InvalidQuery(format!(
            "invalid parameter name: {name}"
        )))
    }
}

fn parameter_value(value: &Value) -> Result<String, ReadError> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Null => Err(ReadError::InvalidQuery(
            "query parameters must not be null".to_string(),
        )),
        Value::Array(_) | Value::Object(_) => Err(ReadError::InvalidQuery(
            "query parameters must be scalar values".to_string(),
        )),
    }
}

fn validate_event_bytes(event_id: &str, bytes: &[u8]) -> Result<(), ReadError> {
    let value: Value = serde_json::from_slice(bytes).map_err(ReadError::InvalidStoredEvent)?;
    if value.get("event_id").and_then(Value::as_str) != Some(event_id) {
        return Err(ReadError::EventIDMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ReadError, checked_select_query, normalize_prewhere, query_sources, query_usage_shape,
        validate_parameter_name, validate_query_sources,
    };

    #[test]
    fn accepts_select_and_with_queries() {
        assert_eq!(
            checked_select_query("SELECT count() FROM observatory.events;").unwrap(),
            "SELECT count() FROM observatory.events"
        );
        assert!(checked_select_query("WITH 1 AS x SELECT x").is_ok());
    }

    #[test]
    fn rejects_mutating_or_multi_statement_queries() {
        assert!(matches!(
            checked_select_query("DELETE FROM observatory.events"),
            Err(ReadError::InvalidQuery(_))
        ));
        assert!(matches!(
            checked_select_query("SELECT 1; SELECT 2"),
            Err(ReadError::InvalidQuery(_))
        ));
    }

    #[test]
    fn allows_forbidden_words_and_semicolons_inside_strings() {
        assert!(
            checked_select_query("SELECT 'delete; drop' AS message FROM observatory.events")
                .is_ok()
        );
    }

    #[test]
    fn rejects_format_clause() {
        assert!(matches!(
            checked_select_query("SELECT 1 FORMAT JSON"),
            Err(ReadError::InvalidQuery(_))
        ));
    }

    #[test]
    fn normalizes_prewhere_for_wrapped_tables() {
        assert_eq!(
            normalize_prewhere(
                "SELECT value FROM observatory.field_index PREWHERE field_name = 'service' WHERE value != ''"
            ),
            "SELECT value FROM observatory.field_index WHERE field_name = 'service' AND value != ''"
        );
        assert_eq!(
            normalize_prewhere(
                "SELECT * FROM observatory.events WHERE event_id IN (SELECT event_id FROM observatory.field_index PREWHERE field_name = 'service' WHERE value = 'api')"
            ),
            "SELECT * FROM observatory.events WHERE event_id IN (SELECT event_id FROM observatory.field_index WHERE field_name = 'service' AND value = 'api')"
        );
    }

    #[test]
    fn validates_parameter_names() {
        assert!(validate_parameter_name("event_id").is_ok());
        assert!(validate_parameter_name("_from").is_ok());
        assert!(validate_parameter_name("1bad").is_err());
        assert!(validate_parameter_name("bad-name").is_err());
    }

    #[test]
    fn query_usage_shape_removes_literals_but_keeps_structure() {
        assert_eq!(
            query_usage_shape(
                "SELECT count(), service FROM observatory.events WHERE service = 'api' AND duration_ms >= 123.4 AND timestamp >= {from:String} GROUP BY service LIMIT 10"
            ),
            "SELECT count (), service FROM observatory.events WHERE service = ?string AND duration_ms >= ?number AND timestamp >= {from:String} GROUP BY service LIMIT ?number"
        );
    }

    #[test]
    fn query_usage_shape_drops_comments_and_string_contents() {
        let shape = query_usage_shape(
            "SELECT 'alice@example.com' AS email FROM observatory.events -- account_id=acct_123\nWHERE request_id = 'req_secret'",
        );
        assert_eq!(
            shape,
            "SELECT ?string AS email FROM observatory.events WHERE request_id = ?string"
        );
        assert!(!shape.contains("alice"));
        assert!(!shape.contains("acct_123"));
        assert!(!shape.contains("req_secret"));
    }

    #[test]
    fn rejects_unapproved_query_sources() {
        let allowed = vec![
            "events".to_string(),
            "observatory.events".to_string(),
            "observatory.field_index".to_string(),
        ];
        assert!(validate_query_sources("SELECT * FROM observatory.events", &allowed).is_ok());
        assert!(
            validate_query_sources(
                "SELECT * FROM observatory.events JOIN system.tables ON 1",
                &allowed
            )
            .is_err()
        );
        assert!(validate_query_sources("SELECT * FROM numbers(10)", &allowed).is_err());
        assert!(
            validate_query_sources("SELECT * FROM observatory.events, system.tables", &allowed)
                .is_err()
        );
    }

    #[test]
    fn extracts_query_sources_for_freshness_checks() {
        assert_eq!(
            query_sources(
                "SELECT * FROM observatory.events JOIN observatory.field_index ON events.event_id = field_index.event_id"
            ),
            vec![
                "observatory.events".to_string(),
                "observatory.field_index".to_string()
            ]
        );
        assert_eq!(
            query_sources(
                "SELECT * FROM observatory.events WHERE event_id IN (SELECT event_id FROM observatory.event_measures)"
            ),
            vec![
                "observatory.events".to_string(),
                "observatory.event_measures".to_string()
            ]
        );
    }
}
