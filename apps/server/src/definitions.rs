use std::sync::Arc;

use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::config::Config;

#[derive(Clone)]
pub struct DefinitionStore {
    cfg: Arc<Config>,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize)]
pub struct CreateDefinitionRequest {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub capabilities: Value,
    #[serde(default)]
    pub backfill: Option<BackfillRequest>,
}

#[derive(Debug, Deserialize)]
pub struct BackfillRequest {
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub to: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DefinitionRecord {
    pub tenant_id: String,
    pub definition_id: String,
    pub name: String,
    pub kind: String,
    pub mode: String,
    pub enabled: u8,
    pub config: Value,
    pub capabilities: Value,
    pub created_at: String,
    pub updated_at: String,
    pub deleted_at: Option<String>,
    pub version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backfill: Option<DefinitionBackfillStatus>,
}

#[derive(Debug, Serialize)]
pub struct DefinitionListResponse {
    pub definitions: Vec<DefinitionRecord>,
}

#[derive(Debug, Serialize)]
pub struct DefinitionMutationResponse {
    pub definition: DefinitionRecord,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backfill: Option<BackfillResponse>,
}

#[derive(Debug, Serialize)]
pub struct BackfillResponse {
    pub definition_id: String,
    pub kind: String,
    pub mode: String,
    pub from: String,
    pub to: String,
    pub rows_matched: u64,
    pub distinct_values: u64,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DefinitionBackfillStatus {
    pub status: String,
    pub from: String,
    pub to: String,
    pub rows_matched: u64,
    pub distinct_values: u64,
    pub updated_at: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DefinitionStoreError {
    #[error("ClickHouse is not configured")]
    ClickHouseNotConfigured,
    #[error("invalid definition name")]
    InvalidName,
    #[error("invalid definition kind")]
    InvalidKind,
    #[error("invalid definition mode")]
    InvalidMode,
    #[error("invalid field path")]
    InvalidPath,
    #[error("invalid JSON config")]
    InvalidConfig,
    #[error("definition not found")]
    NotFound,
    #[error("ClickHouse request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("ClickHouse query failed: {status} {body}")]
    ClickHouseResponse { status: StatusCode, body: String },
    #[error("invalid ClickHouse response: {0}")]
    InvalidClickHouseResponse(#[from] serde_json::Error),
}

impl DefinitionStore {
    pub fn new(cfg: Arc<Config>) -> Self {
        Self {
            cfg,
            http: reqwest::Client::new(),
        }
    }

    pub async fn list(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<DefinitionRecord>, DefinitionStoreError> {
        let query = format!(
            "SELECT tenant_id, definition_id, name, kind, mode, enabled, config, capabilities, created_at, updated_at, deleted_at, version FROM {} FINAL WHERE tenant_id = {{tenant_id:String}} AND kind IN ('field', 'measure', 'rollup', 'state') AND isNull(deleted_at) ORDER BY updated_at DESC",
            self.table("definitions")
        );
        let response: ClickHouseResponse<DefinitionRecord> = self
            .query_json(&query, &[("tenant_id", tenant_id.to_string())])
            .await?;
        let mut definitions = response.data;
        let backfills = self.latest_backfills(tenant_id).await?;
        for definition in &mut definitions {
            definition.backfill = backfills
                .iter()
                .find(|backfill| backfill.definition_id == definition.definition_id)
                .map(|backfill| DefinitionBackfillStatus {
                    status: backfill.status.clone(),
                    from: backfill.from.clone(),
                    to: backfill.to.clone(),
                    rows_matched: backfill.rows_matched,
                    distinct_values: backfill.distinct_values,
                    updated_at: backfill.updated_at.clone(),
                });
        }
        Ok(definitions)
    }

    pub async fn create(
        &self,
        tenant_id: &str,
        request: CreateDefinitionRequest,
    ) -> Result<DefinitionMutationResponse, DefinitionStoreError> {
        let name = normalized_name(&request.name)?;
        let kind = normalized_kind(&request.kind)?;
        let mode = normalized_mode(&kind, &request.mode);
        validate_mode(&kind, &mode)?;
        let config = normalize_config(&kind, &mode, request.config)?;
        let capabilities = normalize_capabilities(&kind, &mode, request.capabilities);
        let now = clickhouse_now();
        let version = Utc::now().timestamp_millis().max(0) as u64;
        let definition = DefinitionRecord {
            tenant_id: tenant_id.to_string(),
            definition_id: format!("def_{}_{}", slug(&name), version),
            name,
            kind,
            mode,
            enabled: 1,
            config,
            capabilities,
            created_at: now.clone(),
            updated_at: now,
            deleted_at: None,
            version,
            backfill: None,
        };
        self.insert_json_each_row("definitions", &[&definition])
            .await?;

        let backfill = match request.backfill {
            Some(backfill) => Some(self.backfill_definition(&definition, backfill).await?),
            None => None,
        };
        Ok(DefinitionMutationResponse {
            definition,
            backfill,
        })
    }

    pub async fn delete(
        &self,
        tenant_id: &str,
        definition_id: &str,
    ) -> Result<DefinitionRecord, DefinitionStoreError> {
        let mut definition = self.get(tenant_id, definition_id).await?;
        let now = clickhouse_now();
        definition.enabled = 0;
        definition.updated_at = now.clone();
        definition.deleted_at = Some(now);
        definition.version = Utc::now().timestamp_millis().max(0) as u64;
        self.insert_json_each_row("definitions", &[&definition])
            .await?;
        Ok(definition)
    }

    pub async fn backfill(
        &self,
        tenant_id: &str,
        definition_id: &str,
        request: BackfillRequest,
    ) -> Result<BackfillResponse, DefinitionStoreError> {
        let definition = self.get(tenant_id, definition_id).await?;
        self.backfill_definition(&definition, request).await
    }

    async fn get(
        &self,
        tenant_id: &str,
        definition_id: &str,
    ) -> Result<DefinitionRecord, DefinitionStoreError> {
        let query = format!(
            "SELECT tenant_id, definition_id, name, kind, mode, enabled, config, capabilities, created_at, updated_at, deleted_at, version FROM {} FINAL WHERE tenant_id = {{tenant_id:String}} AND definition_id = {{definition_id:String}} AND kind IN ('field', 'measure', 'rollup', 'state') AND isNull(deleted_at) ORDER BY updated_at DESC LIMIT 1",
            self.table("definitions")
        );
        let response: ClickHouseResponse<DefinitionRecord> = self
            .query_json(
                &query,
                &[
                    ("tenant_id", tenant_id.to_string()),
                    ("definition_id", definition_id.to_string()),
                ],
            )
            .await?;
        response
            .data
            .into_iter()
            .next()
            .ok_or(DefinitionStoreError::NotFound)
    }

    async fn latest_backfills(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<LatestBackfillRow>, DefinitionStoreError> {
        let query = format!(
            "SELECT definition_id, argMax(decision, measured_at) AS status, toString(argMax(window_start, measured_at)) AS from, toString(argMax(window_end, measured_at)) AS to, argMax(rows_matched, measured_at) AS rows_matched, argMax(distinct_values, measured_at) AS distinct_values, toString(max(measured_at)) AS updated_at FROM {} WHERE tenant_id = {{tenant_id:String}} GROUP BY definition_id",
            self.table("definition_stats")
        );
        let response: ClickHouseResponse<LatestBackfillRow> = self
            .query_json(&query, &[("tenant_id", tenant_id.to_string())])
            .await?;
        Ok(response.data)
    }

    async fn backfill_definition(
        &self,
        definition: &DefinitionRecord,
        request: BackfillRequest,
    ) -> Result<BackfillResponse, DefinitionStoreError> {
        let from = request
            .from
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "1970-01-01 00:00:00.000".to_string());
        let to = request
            .to
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| Utc::now().to_rfc3339());

        match definition.kind.as_str() {
            "field" => self.backfill_field(definition, &from, &to).await,
            "measure" => self.backfill_measure(definition, &from, &to).await,
            "rollup" => self.backfill_measure(definition, &from, &to).await,
            "state" => self.backfill_state(definition, &from, &to).await,
            _ => Err(DefinitionStoreError::InvalidKind),
        }
    }

    async fn backfill_field(
        &self,
        definition: &DefinitionRecord,
        from: &str,
        to: &str,
    ) -> Result<BackfillResponse, DefinitionStoreError> {
        let path = config_string(&definition.config, "path")?;
        let value_type = config_string_default(&definition.config, "value_type", "string");
        let value_expr = value_expression(&path)?;
        let where_value = format!("{value_expr} != ''");
        let stats = self
            .field_stats(&definition.tenant_id, &value_expr, &where_value, from, to)
            .await?;
        let query = format!(
            "INSERT INTO {} (tenant_id, mode, field_name, value, value_type, timestamp, bucket_time, event_id, event_type, signal, is_error, trace_id, span_id, parent_span_id, name, start_time, end_time, duration_ms, definition_id, definition_version)
SELECT tenant_id, {{mode:String}}, {{field_name:String}}, {value_expr}, {{value_type:String}}, timestamp, toStartOfInterval(timestamp, INTERVAL 5 MINUTE), event_id, event_type, signal, toUInt8({}), trace_id, span_id, ifNull(toString(data.parent_span_id), ''), ifNull(toString(data.name), ''), parseDateTime64BestEffortOrNull(ifNull(toString(data.start_time), '')), parseDateTime64BestEffortOrNull(ifNull(toString(data.end_time), '')), toFloat64OrNull(ifNull(toString(data.duration_ms), '')), {{definition_id:String}}, {{definition_version:UInt64}}
FROM {}
WHERE tenant_id = {{tenant_id:String}} AND timestamp >= parseDateTime64BestEffort({{from:String}}, 3, 'UTC') AND timestamp <= parseDateTime64BestEffort({{to:String}}, 3, 'UTC') AND {where_value}",
            self.table("field_index"),
            error_expression(),
            self.events_table()
        );
        self.execute(
            &query,
            &[
                ("tenant_id", definition.tenant_id.clone()),
                ("mode", definition.mode.clone()),
                ("field_name", definition.name.clone()),
                ("value_type", value_type),
                ("definition_id", definition.definition_id.clone()),
                ("definition_version", definition.version.to_string()),
                ("from", from.to_string()),
                ("to", to.to_string()),
            ],
        )
        .await?;
        self.insert_stats(definition, from, to, &stats, "completed")
            .await?;
        Ok(BackfillResponse {
            definition_id: definition.definition_id.clone(),
            kind: definition.kind.clone(),
            mode: definition.mode.clone(),
            from: from.to_string(),
            to: to.to_string(),
            rows_matched: stats.rows_matched,
            distinct_values: stats.distinct_values,
            status: "completed".to_string(),
        })
    }

    async fn backfill_state(
        &self,
        definition: &DefinitionRecord,
        from: &str,
        to: &str,
    ) -> Result<BackfillResponse, DefinitionStoreError> {
        let path = config_string(&definition.config, "path")?;
        let entity_type = config_string(&definition.config, "entity_type")?;
        let entity_id_path = config_string(&definition.config, "entity_id_path")?;
        let value_type = config_string_default(&definition.config, "value_type", "string");
        let value_expr = value_expression(&path)?;
        let entity_expr = value_expression(&entity_id_path)?;
        let where_value = format!("{value_expr} != '' AND {entity_expr} != ''");
        let stats = self
            .field_stats(&definition.tenant_id, &value_expr, &where_value, from, to)
            .await?;
        let query = format!(
            "INSERT INTO {} (tenant_id, definition_id, definition_version, entity_type, entity_id, state_name, value, value_type, timestamp, event_id, event_type, signal)
SELECT tenant_id, {{definition_id:String}}, {{definition_version:UInt64}}, {{entity_type:String}}, {entity_expr}, {{state_name:String}}, {value_expr}, {{value_type:String}}, timestamp, event_id, event_type, signal
FROM {}
WHERE tenant_id = {{tenant_id:String}} AND timestamp >= parseDateTime64BestEffort({{from:String}}, 3, 'UTC') AND timestamp <= parseDateTime64BestEffort({{to:String}}, 3, 'UTC') AND {where_value}",
            self.table("entity_state_updates"),
            self.events_table()
        );
        self.execute(
            &query,
            &[
                ("tenant_id", definition.tenant_id.clone()),
                ("definition_id", definition.definition_id.clone()),
                ("definition_version", definition.version.to_string()),
                ("entity_type", entity_type),
                ("state_name", definition.name.clone()),
                ("value_type", value_type),
                ("from", from.to_string()),
                ("to", to.to_string()),
            ],
        )
        .await?;
        self.insert_stats(definition, from, to, &stats, "completed")
            .await?;
        Ok(BackfillResponse {
            definition_id: definition.definition_id.clone(),
            kind: definition.kind.clone(),
            mode: definition.mode.clone(),
            from: from.to_string(),
            to: to.to_string(),
            rows_matched: stats.rows_matched,
            distinct_values: stats.distinct_values,
            status: "completed".to_string(),
        })
    }

    async fn backfill_measure(
        &self,
        definition: &DefinitionRecord,
        from: &str,
        to: &str,
    ) -> Result<BackfillResponse, DefinitionStoreError> {
        let path = config_string(&definition.config, "path")?;
        let value_expr = numeric_expression(&path)?;
        let dimension_name = config_string_default(&definition.config, "dimension", "");
        let dimension_expr = if dimension_name.is_empty() {
            "''".to_string()
        } else {
            value_expression(&dimension_name)?
        };
        let where_value = format!("{value_expr} IS NOT NULL");
        let stats = self
            .field_stats(&definition.tenant_id, &value_expr, &where_value, from, to)
            .await?;
        let query = format!(
            "INSERT INTO {} (tenant_id, definition_id, definition_version, measure_name, value, unit, timestamp, bucket_time, bucket_seconds, event_id, event_type, signal, dimension_name, dimension_value)
SELECT tenant_id, {{definition_id:String}}, {{definition_version:UInt64}}, {{measure_name:String}}, {value_expr}, {{unit:String}}, timestamp, toStartOfInterval(timestamp, INTERVAL 5 MINUTE), 300, event_id, event_type, signal, {{dimension_name:String}}, {dimension_expr}
FROM {}
WHERE tenant_id = {{tenant_id:String}} AND timestamp >= parseDateTime64BestEffort({{from:String}}, 3, 'UTC') AND timestamp <= parseDateTime64BestEffort({{to:String}}, 3, 'UTC') AND {where_value}",
            self.table("event_measures"),
            self.events_table()
        );
        self.execute(
            &query,
            &[
                ("tenant_id", definition.tenant_id.clone()),
                ("definition_id", definition.definition_id.clone()),
                ("definition_version", definition.version.to_string()),
                ("measure_name", definition.name.clone()),
                (
                    "unit",
                    config_string_default(&definition.config, "unit", ""),
                ),
                ("dimension_name", dimension_name),
                ("from", from.to_string()),
                ("to", to.to_string()),
            ],
        )
        .await?;
        self.insert_stats(definition, from, to, &stats, "completed")
            .await?;
        Ok(BackfillResponse {
            definition_id: definition.definition_id.clone(),
            kind: definition.kind.clone(),
            mode: definition.mode.clone(),
            from: from.to_string(),
            to: to.to_string(),
            rows_matched: stats.rows_matched,
            distinct_values: stats.distinct_values,
            status: "completed".to_string(),
        })
    }

    async fn field_stats(
        &self,
        tenant_id: &str,
        value_expr: &str,
        where_value: &str,
        from: &str,
        to: &str,
    ) -> Result<BackfillStats, DefinitionStoreError> {
        let query = format!(
            "SELECT count() AS rows_matched, uniqCombined64(value) AS distinct_values FROM (SELECT {value_expr} AS value FROM {} WHERE tenant_id = {{tenant_id:String}} AND timestamp >= parseDateTime64BestEffort({{from:String}}, 3, 'UTC') AND timestamp <= parseDateTime64BestEffort({{to:String}}, 3, 'UTC') AND {where_value})",
            self.events_table()
        );
        self.stats_query(&query, tenant_id, from, to).await
    }

    async fn stats_query(
        &self,
        query: &str,
        tenant_id: &str,
        from: &str,
        to: &str,
    ) -> Result<BackfillStats, DefinitionStoreError> {
        let response: ClickHouseResponse<BackfillStats> = self
            .query_json(
                query,
                &[
                    ("tenant_id", tenant_id.to_string()),
                    ("from", from.to_string()),
                    ("to", to.to_string()),
                ],
            )
            .await?;
        Ok(response.data.into_iter().next().unwrap_or_default())
    }

    async fn insert_stats(
        &self,
        definition: &DefinitionRecord,
        from: &str,
        to: &str,
        stats: &BackfillStats,
        decision: &str,
    ) -> Result<(), DefinitionStoreError> {
        let row = DefinitionStatsRow {
            tenant_id: definition.tenant_id.clone(),
            definition_id: definition.definition_id.clone(),
            definition_version: definition.version,
            window_start: clickhouse_datetime(from),
            window_end: clickhouse_datetime(to),
            rows_scanned: stats.rows_matched,
            rows_matched: stats.rows_matched,
            distinct_values: stats.distinct_values,
            estimated_rows_per_sec: 0.0,
            estimated_storage_bytes_per_day: 0,
            cardinality_class: cardinality_class(stats.distinct_values).to_string(),
            decision: decision.to_string(),
            warnings: Vec::new(),
        };
        self.insert_json_each_row("definition_stats", &[&row]).await
    }

    async fn query_json<T: for<'de> Deserialize<'de>>(
        &self,
        query: &str,
        parameters: &[(&str, String)],
    ) -> Result<ClickHouseResponse<T>, DefinitionStoreError> {
        let body = self
            .execute_body(&format!("{query} FORMAT JSON"), parameters)
            .await?;
        serde_json::from_str(&body).map_err(DefinitionStoreError::InvalidClickHouseResponse)
    }

    async fn execute(
        &self,
        query: &str,
        parameters: &[(&str, String)],
    ) -> Result<(), DefinitionStoreError> {
        self.execute_body(query, parameters).await?;
        Ok(())
    }

    async fn execute_body(
        &self,
        query: &str,
        parameters: &[(&str, String)],
    ) -> Result<String, DefinitionStoreError> {
        let url = self
            .cfg
            .clickhouse_url
            .as_deref()
            .ok_or(DefinitionStoreError::ClickHouseNotConfigured)?;
        let max_execution_time = self.cfg.clickhouse_max_execution_secs.to_string();
        let max_bytes_to_read = self.cfg.clickhouse_max_bytes_to_read.to_string();
        let mut request = self
            .http
            .post(url)
            .query(&[
                ("database", self.cfg.clickhouse_database.as_str()),
                ("type_json_skip_duplicated_paths", "1"),
                ("max_execution_time", max_execution_time.as_str()),
                ("max_bytes_to_read", max_bytes_to_read.as_str()),
            ])
            .body(query.to_string());
        if let Some(user) = self.cfg.clickhouse_user.as_deref() {
            request = request.basic_auth(user, self.cfg.clickhouse_password.as_deref());
        }
        for (key, value) in parameters {
            request = request.query(&[(format!("param_{key}"), value)]);
        }
        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(DefinitionStoreError::ClickHouseResponse { status, body });
        }
        Ok(body)
    }

    async fn insert_json_each_row<T: Serialize>(
        &self,
        table: &str,
        rows: &[&T],
    ) -> Result<(), DefinitionStoreError> {
        let mut body = format!("INSERT INTO {} FORMAT JSONEachRow\n", self.table(table));
        for row in rows {
            body.push_str(
                &serde_json::to_string(row).map_err(|_| DefinitionStoreError::InvalidConfig)?,
            );
            body.push('\n');
        }
        self.execute(&body, &[]).await
    }

    fn events_table(&self) -> String {
        format!(
            "{}.{}",
            self.cfg.clickhouse_database, self.cfg.clickhouse_table
        )
    }

    fn table(&self, table: &str) -> String {
        format!("{}.{}", self.cfg.clickhouse_database, table)
    }
}

#[derive(Debug, Deserialize)]
struct ClickHouseResponse<T> {
    data: Vec<T>,
}

#[derive(Debug, Deserialize, Default)]
struct BackfillStats {
    #[serde(default, deserialize_with = "deserialize_u64_or_zero")]
    rows_matched: u64,
    #[serde(default, deserialize_with = "deserialize_u64_or_zero")]
    distinct_values: u64,
}

#[derive(Debug, Deserialize)]
struct LatestBackfillRow {
    definition_id: String,
    status: String,
    from: String,
    to: String,
    rows_matched: u64,
    distinct_values: u64,
    updated_at: String,
}

#[derive(Debug, Serialize)]
struct DefinitionStatsRow {
    tenant_id: String,
    definition_id: String,
    definition_version: u64,
    window_start: String,
    window_end: String,
    rows_scanned: u64,
    rows_matched: u64,
    distinct_values: u64,
    estimated_rows_per_sec: f64,
    estimated_storage_bytes_per_day: u64,
    cardinality_class: String,
    decision: String,
    warnings: Vec<String>,
}

fn normalize_config(
    kind: &str,
    mode: &str,
    mut config: Value,
) -> Result<Value, DefinitionStoreError> {
    if !config.is_object() {
        return Err(DefinitionStoreError::InvalidConfig);
    }
    match kind {
        "field" => {
            let path = config_string(&config, "path")?;
            validate_path(&path)?;
            config["mode"] = Value::String(mode.to_string());
            config["value_type"] = Value::String(normalized_value_type(&config_string_default(
                &config,
                "value_type",
                "string",
            ))?);
        }
        "measure" => {
            let path = config_string(&config, "path")?;
            validate_path(&path)?;
            if let Some(dimension) = config
                .get("dimension")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
            {
                validate_path(dimension)?;
            }
            config["value_type"] = Value::String("number".to_string());
        }
        "rollup" => {
            let path = config_string(&config, "path")?;
            validate_path(&path)?;
            if let Some(dimension) = config
                .get("dimension")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
            {
                validate_path(dimension)?;
            }
            config["value_type"] = Value::String("number".to_string());
            config["bucket"] = Value::String("5m".to_string());
            if !config
                .get("aggregates")
                .and_then(Value::as_array)
                .is_some_and(|values| {
                    !values.is_empty()
                        && values.iter().all(|value| {
                            value.as_str().is_some_and(|aggregate| {
                                matches!(
                                    aggregate,
                                    "count"
                                        | "sum"
                                        | "avg"
                                        | "min"
                                        | "max"
                                        | "p50"
                                        | "p90"
                                        | "p95"
                                        | "p99"
                                )
                            })
                        })
                })
            {
                config["aggregates"] =
                    serde_json::json!(["count", "sum", "avg", "min", "max", "p50", "p95", "p99"]);
            }
        }
        "state" => {
            let path = config_string(&config, "path")?;
            validate_path(&path)?;
            let entity_type = config_string(&config, "entity_type")?;
            validate_entity_type(&entity_type)?;
            let entity_id_path = config_string(&config, "entity_id_path")?;
            validate_path(&entity_id_path)?;
            config["mode"] = Value::String(mode.to_string());
            config["entity_type"] = Value::String(entity_type);
            config["entity_id_path"] = Value::String(entity_id_path);
            config["value_type"] = Value::String(normalized_value_type(&config_string_default(
                &config,
                "value_type",
                "string",
            ))?);
        }
        _ => return Err(DefinitionStoreError::InvalidKind),
    }
    Ok(config)
}

fn normalize_capabilities(kind: &str, mode: &str, capabilities: Value) -> Value {
    if capabilities.is_object() {
        return capabilities;
    }
    match (kind, mode) {
        ("field", "facet") => serde_json::json!({
            "filter": true,
            "facet": true,
            "values": true,
            "rollup_dimension": true
        }),
        ("field", "lookup") => serde_json::json!({
            "exact_lookup": true,
            "filter": true,
            "facet": false,
            "values": false,
            "rollup_dimension": false
        }),
        ("measure", _) => serde_json::json!({
            "aggregate": true,
            "rollup_measure": true
        }),
        ("rollup", _) => serde_json::json!({
            "aggregate": true,
            "precomputed": true
        }),
        ("state", _) => serde_json::json!({
            "state_history": true,
            "as_of": true,
            "filter": true
        }),
        _ => Value::Object(serde_json::Map::new()),
    }
}

fn normalized_name(value: &str) -> Result<String, DefinitionStoreError> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 128
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        return Err(DefinitionStoreError::InvalidName);
    }
    Ok(value.to_string())
}

fn normalized_kind(value: &str) -> Result<String, DefinitionStoreError> {
    match value.trim() {
        "field" | "measure" | "rollup" | "state" => Ok(value.trim().to_string()),
        _ => Err(DefinitionStoreError::InvalidKind),
    }
}

fn normalized_mode(kind: &str, value: &str) -> String {
    let value = value.trim();
    if !value.is_empty() {
        return value.to_string();
    }
    match kind {
        "field" => "facet".to_string(),
        "measure" => "measure".to_string(),
        "rollup" => "measure_rollup".to_string(),
        "state" => "state_transition".to_string(),
        _ => String::new(),
    }
}

fn validate_mode(kind: &str, mode: &str) -> Result<(), DefinitionStoreError> {
    let valid = matches!(
        (kind, mode),
        ("field", "facet")
            | ("field", "lookup")
            | ("measure", "measure")
            | ("rollup", "measure_rollup")
            | ("state", "state_transition")
    );
    if valid {
        Ok(())
    } else {
        Err(DefinitionStoreError::InvalidMode)
    }
}

fn config_string(config: &Value, key: &str) -> Result<String, DefinitionStoreError> {
    config
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or(DefinitionStoreError::InvalidConfig)
}

fn config_string_default(config: &Value, key: &str, default: &str) -> String {
    config
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default)
        .to_string()
}

fn validate_path(path: &str) -> Result<(), DefinitionStoreError> {
    let valid = !path.is_empty()
        && path.len() <= 160
        && path.split('.').all(|part| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        });
    if valid {
        Ok(())
    } else {
        Err(DefinitionStoreError::InvalidPath)
    }
}

fn validate_entity_type(value: &str) -> Result<(), DefinitionStoreError> {
    let valid = !value.is_empty()
        && value.len() <= 80
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-');
    if valid {
        Ok(())
    } else {
        Err(DefinitionStoreError::InvalidConfig)
    }
}

fn normalized_value_type(value: &str) -> Result<String, DefinitionStoreError> {
    match value.trim() {
        "string" | "number" | "bool" => Ok(value.trim().to_string()),
        _ => Err(DefinitionStoreError::InvalidConfig),
    }
}

fn value_expression(path: &str) -> Result<String, DefinitionStoreError> {
    validate_path(path)?;
    Ok(format!("ifNull(toString(data.{}), '')", path))
}

fn numeric_expression(path: &str) -> Result<String, DefinitionStoreError> {
    validate_path(path)?;
    Ok(format!(
        "toFloat64OrNull(ifNull(toString(data.{}), ''))",
        path
    ))
}

fn error_expression() -> &'static str {
    "lowerUTF8(ifNull(toString(data.is_error), '')) IN ('1', 'true') OR lowerUTF8(ifNull(toString(data.span_status_code), '')) = 'error' OR endsWith(lowerUTF8(ifNull(toString(data.event_type), '')), '_error')"
}

fn cardinality_class(distinct_values: u64) -> &'static str {
    match distinct_values {
        0..=100 => "low",
        101..=10_000 => "medium",
        10_001..=1_000_000 => "high",
        _ => "unbounded",
    }
}

fn slug(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn clickhouse_now() -> String {
    Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

fn clickhouse_datetime(value: &str) -> String {
    DateTime::parse_from_rfc3339(value)
        .map(|date| {
            date.with_timezone(&Utc)
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string()
        })
        .unwrap_or_else(|_| value.to_string())
}

fn deserialize_u64_or_zero<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<u64>::deserialize(deserializer)?.unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::{BackfillStats, ClickHouseResponse};

    #[test]
    fn backfill_stats_treats_null_distinct_values_as_zero() {
        let response: ClickHouseResponse<BackfillStats> =
            serde_json::from_str(r#"{"data":[{"rows_matched":0,"distinct_values":null}]}"#)
                .expect("response should deserialize");

        let stats = response.data.into_iter().next().expect("stats row");
        assert_eq!(stats.rows_matched, 0);
        assert_eq!(stats.distinct_values, 0);
    }
}
