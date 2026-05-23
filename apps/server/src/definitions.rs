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

#[derive(Debug, Deserialize, utoipa::ToSchema)]
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

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct BackfillRequest {
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub to: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
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

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct DefinitionListResponse {
    pub definitions: Vec<DefinitionRecord>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct DefinitionMutationResponse {
    pub definition: DefinitionRecord,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backfill: Option<BackfillResponse>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
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

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
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
            "SELECT tenant_id, definition_id, name, kind, mode, enabled, config, capabilities, created_at, updated_at, deleted_at, version FROM {} FINAL WHERE tenant_id = {{tenant_id:String}} AND kind IN ('field', 'measure', 'rollup', 'metric_rollup', 'state', 'report', 'sequence', 'cohort') AND isNull(deleted_at) ORDER BY updated_at DESC",
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

    pub async fn seed_sdk_defaults(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<DefinitionRecord>, DefinitionStoreError> {
        let desired = sdk_metric_definition(tenant_id);
        match self.get(tenant_id, &desired.definition_id).await {
            Ok(existing)
                if existing.enabled == 1
                    && existing.config == desired.config
                    && existing.capabilities == desired.capabilities =>
            {
                Ok(vec![existing])
            }
            Ok(existing) => {
                let mut updated = desired;
                updated.created_at = existing.created_at;
                self.insert_json_each_row("definitions", &[&updated])
                    .await?;
                Ok(vec![updated])
            }
            Err(DefinitionStoreError::NotFound) => {
                self.insert_json_each_row("definitions", &[&desired])
                    .await?;
                Ok(vec![desired])
            }
            Err(err) => Err(err),
        }
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
            "SELECT tenant_id, definition_id, name, kind, mode, enabled, config, capabilities, created_at, updated_at, deleted_at, version FROM {} FINAL WHERE tenant_id = {{tenant_id:String}} AND definition_id = {{definition_id:String}} AND kind IN ('field', 'measure', 'rollup', 'metric_rollup', 'state', 'report', 'sequence', 'cohort') AND isNull(deleted_at) ORDER BY updated_at DESC LIMIT 1",
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
        if definition.config.get("outputs").is_some() {
            return self.backfill_generalized_field(definition, from, to).await;
        }
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
        if definition.config.get("outputs").is_some() {
            return self.backfill_generalized_state(definition, from, to).await;
        }
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
        if definition.config.get("outputs").is_some() {
            return self
                .backfill_generalized_measure(definition, from, to)
                .await;
        }
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

    async fn backfill_generalized_field(
        &self,
        definition: &DefinitionRecord,
        from: &str,
        to: &str,
    ) -> Result<BackfillResponse, DefinitionStoreError> {
        let matcher_clause = matcher_where_clause(&definition.config)?;
        let mut stats = BackfillStats::default();
        for output in generalized_outputs(&definition.config, "field_index")? {
            let field_name = json_string(output, "field_name")?;
            let value_type = json_string_default(output, "value_type", "string");
            let mode = json_string_default(output, "mode", &definition.mode);
            let value_expr = string_sql_expr(
                output
                    .get("value")
                    .ok_or(DefinitionStoreError::InvalidConfig)?,
            )?;
            let where_value =
                join_sql_clauses([Some(format!("{value_expr} != ''")), matcher_clause.clone()]);
            let output_stats = self
                .field_stats(&definition.tenant_id, &value_expr, &where_value, from, to)
                .await?;
            stats.add(output_stats);
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
                    ("mode", mode),
                    ("field_name", field_name),
                    ("value_type", value_type),
                    ("definition_id", definition.definition_id.clone()),
                    ("definition_version", definition.version.to_string()),
                    ("from", from.to_string()),
                    ("to", to.to_string()),
                ],
            )
            .await?;
        }
        self.insert_stats(definition, from, to, &stats, "completed")
            .await?;
        Ok(backfill_response(definition, from, to, &stats))
    }

    async fn backfill_generalized_measure(
        &self,
        definition: &DefinitionRecord,
        from: &str,
        to: &str,
    ) -> Result<BackfillResponse, DefinitionStoreError> {
        let matcher_clause = matcher_where_clause(&definition.config)?;
        let mut stats = BackfillStats::default();
        for output in generalized_outputs(&definition.config, "event_measures")? {
            let measure_name_expr = string_sql_expr(
                output
                    .get("measure_name")
                    .ok_or(DefinitionStoreError::InvalidConfig)?,
            )?;
            let value_expr = number_sql_expr(
                output
                    .get("value")
                    .ok_or(DefinitionStoreError::InvalidConfig)?,
            )?;
            let unit_expr = output
                .get("unit")
                .map(string_sql_expr)
                .transpose()?
                .unwrap_or_else(|| "''".to_string());
            let dimensions = dimension_sql_outputs(output)?;
            if dimensions.is_empty() {
                let where_value = join_sql_clauses([
                    Some(format!("{value_expr} IS NOT NULL")),
                    Some(format!("{measure_name_expr} != ''")),
                    matcher_clause.clone(),
                ]);
                let output_stats = self
                    .field_stats(&definition.tenant_id, &value_expr, &where_value, from, to)
                    .await?;
                stats.add(output_stats);
                self.execute_measure_backfill(
                    definition,
                    from,
                    to,
                    &where_value,
                    &measure_name_expr,
                    &value_expr,
                    &unit_expr,
                    "''",
                    "''",
                    json_u32_default(output, "bucket_seconds", 300),
                )
                .await?;
                continue;
            }

            for dimension in &dimensions {
                let where_value = join_sql_clauses([
                    Some(format!("{value_expr} IS NOT NULL")),
                    Some(format!("{measure_name_expr} != ''")),
                    Some(format!("{} != ''", dimension.value_expr)),
                    matcher_clause.clone(),
                ]);
                let output_stats = self
                    .field_stats(&definition.tenant_id, &value_expr, &where_value, from, to)
                    .await?;
                stats.add(output_stats);
                self.execute_measure_backfill(
                    definition,
                    from,
                    to,
                    &where_value,
                    &measure_name_expr,
                    &value_expr,
                    &unit_expr,
                    &quote_sql_string(&dimension.name),
                    &dimension.value_expr,
                    json_u32_default(output, "bucket_seconds", 300),
                )
                .await?;
            }

            let missing_dimension_clause = dimensions
                .iter()
                .map(|dimension| format!("{} = ''", dimension.value_expr))
                .collect::<Vec<_>>()
                .join(" AND ");
            let where_value = join_sql_clauses([
                Some(format!("{value_expr} IS NOT NULL")),
                Some(format!("{measure_name_expr} != ''")),
                Some(missing_dimension_clause),
                matcher_clause.clone(),
            ]);
            let output_stats = self
                .field_stats(&definition.tenant_id, &value_expr, &where_value, from, to)
                .await?;
            stats.add(output_stats);
            self.execute_measure_backfill(
                definition,
                from,
                to,
                &where_value,
                &measure_name_expr,
                &value_expr,
                &unit_expr,
                "''",
                "''",
                json_u32_default(output, "bucket_seconds", 300),
            )
            .await?;
        }
        self.insert_stats(definition, from, to, &stats, "completed")
            .await?;
        Ok(backfill_response(definition, from, to, &stats))
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_measure_backfill(
        &self,
        definition: &DefinitionRecord,
        from: &str,
        to: &str,
        where_value: &str,
        measure_name_expr: &str,
        value_expr: &str,
        unit_expr: &str,
        dimension_name_expr: &str,
        dimension_value_expr: &str,
        bucket_seconds: u32,
    ) -> Result<(), DefinitionStoreError> {
        let query = format!(
            "INSERT INTO {} (tenant_id, definition_id, definition_version, measure_name, value, unit, timestamp, bucket_time, bucket_seconds, event_id, event_type, signal, dimension_name, dimension_value)
SELECT tenant_id, {{definition_id:String}}, {{definition_version:UInt64}}, {measure_name_expr}, {value_expr}, {unit_expr}, timestamp, toStartOfInterval(timestamp, INTERVAL 5 MINUTE), {{bucket_seconds:UInt32}}, event_id, event_type, signal, {dimension_name_expr}, {dimension_value_expr}
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
                ("bucket_seconds", bucket_seconds.to_string()),
                ("from", from.to_string()),
                ("to", to.to_string()),
            ],
        )
        .await
    }

    async fn backfill_generalized_state(
        &self,
        definition: &DefinitionRecord,
        from: &str,
        to: &str,
    ) -> Result<BackfillResponse, DefinitionStoreError> {
        let matcher_clause = matcher_where_clause(&definition.config)?;
        let mut stats = BackfillStats::default();
        for output in generalized_outputs(&definition.config, "entity_state_updates")? {
            let entity_type_expr = string_sql_expr(
                output
                    .get("entity_type")
                    .ok_or(DefinitionStoreError::InvalidConfig)?,
            )?;
            let entity_id_expr = string_sql_expr(
                output
                    .get("entity_id")
                    .ok_or(DefinitionStoreError::InvalidConfig)?,
            )?;
            let state_name_expr = string_sql_expr(
                output
                    .get("state_name")
                    .ok_or(DefinitionStoreError::InvalidConfig)?,
            )?;
            let value_expr = string_sql_expr(
                output
                    .get("value")
                    .ok_or(DefinitionStoreError::InvalidConfig)?,
            )?;
            let value_type = json_string_default(output, "value_type", "string");
            let where_value = join_sql_clauses([
                Some(format!("{entity_type_expr} != ''")),
                Some(format!("{entity_id_expr} != ''")),
                Some(format!("{state_name_expr} != ''")),
                Some(format!("{value_expr} != ''")),
                matcher_clause.clone(),
            ]);
            let output_stats = self
                .field_stats(&definition.tenant_id, &value_expr, &where_value, from, to)
                .await?;
            stats.add(output_stats);
            let query = format!(
                "INSERT INTO {} (tenant_id, definition_id, definition_version, entity_type, entity_id, state_name, value, value_type, timestamp, event_id, event_type, signal)
SELECT tenant_id, {{definition_id:String}}, {{definition_version:UInt64}}, {entity_type_expr}, {entity_id_expr}, {state_name_expr}, {value_expr}, {{value_type:String}}, timestamp, event_id, event_type, signal
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
                    ("value_type", value_type),
                    ("from", from.to_string()),
                    ("to", to.to_string()),
                ],
            )
            .await?;
        }
        self.insert_stats(definition, from, to, &stats, "completed")
            .await?;
        Ok(backfill_response(definition, from, to, &stats))
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
            decision: decision.to_string(),
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

impl BackfillStats {
    fn add(&mut self, other: BackfillStats) {
        self.rows_matched = self.rows_matched.saturating_add(other.rows_matched);
        self.distinct_values = self.distinct_values.saturating_add(other.distinct_values);
    }
}

struct DimensionSqlOutput {
    name: String,
    value_expr: String,
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
    decision: String,
}

fn normalize_config(
    kind: &str,
    mode: &str,
    mut config: Value,
) -> Result<Value, DefinitionStoreError> {
    if !config.is_object() {
        return Err(DefinitionStoreError::InvalidConfig);
    }
    if config.get("outputs").is_some() {
        validate_generalized_config(kind, mode, &mut config)?;
        return Ok(config);
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
        "report" | "sequence" | "cohort" => return Err(DefinitionStoreError::InvalidConfig),
        _ => return Err(DefinitionStoreError::InvalidKind),
    }
    Ok(config)
}

fn backfill_response(
    definition: &DefinitionRecord,
    from: &str,
    to: &str,
    stats: &BackfillStats,
) -> BackfillResponse {
    BackfillResponse {
        definition_id: definition.definition_id.clone(),
        kind: definition.kind.clone(),
        mode: definition.mode.clone(),
        from: from.to_string(),
        to: to.to_string(),
        rows_matched: stats.rows_matched,
        distinct_values: stats.distinct_values,
        status: "completed".to_string(),
    }
}

fn generalized_outputs<'a>(
    config: &'a Value,
    target: &'static str,
) -> Result<Vec<&'a serde_json::Map<String, Value>>, DefinitionStoreError> {
    Ok(config
        .get("outputs")
        .and_then(Value::as_array)
        .ok_or(DefinitionStoreError::InvalidConfig)?
        .iter()
        .filter_map(Value::as_object)
        .filter(|output| {
            output
                .get("target")
                .and_then(Value::as_str)
                .is_none_or(|value| value == target)
        })
        .collect())
}

fn matcher_where_clause(config: &Value) -> Result<Option<String>, DefinitionStoreError> {
    let Some(matcher) = config.get("match") else {
        return Ok(None);
    };
    let predicates = matcher
        .get("all")
        .and_then(Value::as_array)
        .ok_or(DefinitionStoreError::InvalidConfig)?;
    let mut clauses = Vec::new();
    for predicate in predicates {
        let object = predicate
            .as_object()
            .ok_or(DefinitionStoreError::InvalidConfig)?;
        let path = json_string(object, "path")?;
        let value_expr = value_expression(&path)?;
        let numeric_expr = numeric_expression(&path)?;
        let clause = match object.get("op").and_then(Value::as_str).unwrap_or("eq") {
            "exists" => format!("{value_expr} != ''"),
            "eq" => format!(
                "{value_expr} = {}",
                quote_sql_string(&scalar_json_string(
                    object
                        .get("value")
                        .ok_or(DefinitionStoreError::InvalidConfig)?
                ))
            ),
            "neq" => format!(
                "{value_expr} != '' AND {value_expr} != {}",
                quote_sql_string(&scalar_json_string(
                    object
                        .get("value")
                        .ok_or(DefinitionStoreError::InvalidConfig)?
                ))
            ),
            "is_number" => format!("{numeric_expr} IS NOT NULL"),
            "in" => {
                let values = object
                    .get("value")
                    .and_then(Value::as_array)
                    .ok_or(DefinitionStoreError::InvalidConfig)?;
                let values = values
                    .iter()
                    .map(|value| quote_sql_string(&scalar_json_string(value)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{value_expr} IN ({values})")
            }
            _ => return Err(DefinitionStoreError::InvalidConfig),
        };
        clauses.push(clause);
    }
    if clauses.is_empty() {
        Ok(None)
    } else {
        Ok(Some(clauses.join(" AND ")))
    }
}

fn string_sql_expr(value: &Value) -> Result<String, DefinitionStoreError> {
    match value {
        Value::String(value) => Ok(quote_sql_string(value)),
        Value::Number(_) | Value::Bool(_) => Ok(quote_sql_string(&scalar_json_string(value))),
        Value::Object(object) => {
            let path = json_string(object, "path")?;
            let expr = value_expression(&path)?;
            let default = object
                .get("default")
                .map(scalar_json_string)
                .unwrap_or_default();
            if default.is_empty() {
                Ok(expr)
            } else {
                Ok(format!(
                    "if({expr} = '', {}, {expr})",
                    quote_sql_string(&default)
                ))
            }
        }
        _ => Err(DefinitionStoreError::InvalidConfig),
    }
}

fn number_sql_expr(value: &Value) -> Result<String, DefinitionStoreError> {
    match value {
        Value::Number(value) => Ok(value.to_string()),
        Value::String(value) if value.parse::<f64>().is_ok() => Ok(value.clone()),
        Value::Object(object) => {
            let path = json_string(object, "path")?;
            numeric_expression(&path)
        }
        _ => Err(DefinitionStoreError::InvalidConfig),
    }
}

fn dimension_sql_outputs(
    output: &serde_json::Map<String, Value>,
) -> Result<Vec<DimensionSqlOutput>, DefinitionStoreError> {
    output
        .get("dimensions")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|dimensions| dimensions.iter())
        .map(|dimension| {
            let dimension = dimension
                .as_object()
                .ok_or(DefinitionStoreError::InvalidConfig)?;
            Ok(DimensionSqlOutput {
                name: json_string(dimension, "name")?,
                value_expr: string_sql_expr(
                    dimension
                        .get("value")
                        .ok_or(DefinitionStoreError::InvalidConfig)?,
                )?,
            })
        })
        .collect()
}

fn join_sql_clauses<const N: usize>(clauses: [Option<String>; N]) -> String {
    clauses
        .into_iter()
        .flatten()
        .filter(|clause| !clause.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn json_string(
    object: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<String, DefinitionStoreError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or(DefinitionStoreError::InvalidConfig)
}

fn json_string_default(
    object: &serde_json::Map<String, Value>,
    key: &str,
    default: &str,
) -> String {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default)
        .to_string()
}

fn json_u32_default(object: &serde_json::Map<String, Value>, key: &str, default: u32) -> u32 {
    object
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|value| value.try_into().ok())
        .unwrap_or(default)
}

fn scalar_json_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => {
            if *value {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        _ => String::new(),
    }
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn validate_generalized_config(
    kind: &str,
    mode: &str,
    config: &mut Value,
) -> Result<(), DefinitionStoreError> {
    validate_matcher(config.get("match"))?;
    let outputs = config
        .get_mut("outputs")
        .and_then(Value::as_array_mut)
        .ok_or(DefinitionStoreError::InvalidConfig)?;
    if outputs.is_empty() {
        return Err(DefinitionStoreError::InvalidConfig);
    }
    for output in outputs {
        validate_generalized_output(kind, mode, output)?;
    }
    Ok(())
}

fn validate_matcher(matcher: Option<&Value>) -> Result<(), DefinitionStoreError> {
    let Some(matcher) = matcher else {
        return Ok(());
    };
    let all = matcher
        .get("all")
        .and_then(Value::as_array)
        .ok_or(DefinitionStoreError::InvalidConfig)?;
    for predicate in all {
        let object = predicate
            .as_object()
            .ok_or(DefinitionStoreError::InvalidConfig)?;
        let path = object
            .get("path")
            .and_then(Value::as_str)
            .ok_or(DefinitionStoreError::InvalidConfig)?;
        validate_path(path)?;
        let op = object.get("op").and_then(Value::as_str).unwrap_or("eq");
        match op {
            "exists" | "is_number" => {}
            "eq" | "neq" => {
                if !object.contains_key("value") {
                    return Err(DefinitionStoreError::InvalidConfig);
                }
            }
            "in" => {
                if !object
                    .get("value")
                    .and_then(Value::as_array)
                    .is_some_and(|values| !values.is_empty())
                {
                    return Err(DefinitionStoreError::InvalidConfig);
                }
            }
            _ => return Err(DefinitionStoreError::InvalidConfig),
        }
    }
    Ok(())
}

fn validate_generalized_output(
    kind: &str,
    mode: &str,
    output: &mut Value,
) -> Result<(), DefinitionStoreError> {
    let object = output
        .as_object_mut()
        .ok_or(DefinitionStoreError::InvalidConfig)?;
    match kind {
        "field" => {
            let target = object
                .get("target")
                .and_then(Value::as_str)
                .unwrap_or("field_index");
            if target != "field_index" {
                return Err(DefinitionStoreError::InvalidConfig);
            }
            let field_name = object
                .get("field_name")
                .and_then(Value::as_str)
                .ok_or(DefinitionStoreError::InvalidConfig)?;
            validate_path(field_name)?;
            validate_value_expr(object.get("value"))?;
            let output_mode = object.get("mode").and_then(Value::as_str).unwrap_or(mode);
            if !matches!(output_mode, "facet" | "lookup") {
                return Err(DefinitionStoreError::InvalidMode);
            }
            object.insert("mode".to_string(), Value::String(output_mode.to_string()));
            let value_type = normalized_value_type(
                object
                    .get("value_type")
                    .and_then(Value::as_str)
                    .unwrap_or("string"),
            )?;
            object.insert("value_type".to_string(), Value::String(value_type));
        }
        "measure" | "rollup" => {
            let target = object
                .get("target")
                .and_then(Value::as_str)
                .unwrap_or("event_measures");
            if target != "event_measures" {
                return Err(DefinitionStoreError::InvalidConfig);
            }
            validate_string_expr(object.get("measure_name"))?;
            validate_number_expr(object.get("value"))?;
            if let Some(unit) = object.get("unit") {
                validate_string_expr(Some(unit))?;
            }
            if let Some(dimensions) = object.get("dimensions") {
                let dimensions = dimensions
                    .as_array()
                    .ok_or(DefinitionStoreError::InvalidConfig)?;
                for dimension in dimensions {
                    let dimension = dimension
                        .as_object()
                        .ok_or(DefinitionStoreError::InvalidConfig)?;
                    let name = dimension
                        .get("name")
                        .and_then(Value::as_str)
                        .ok_or(DefinitionStoreError::InvalidConfig)?;
                    validate_path(name)?;
                    validate_string_expr(dimension.get("value"))?;
                }
            }
            if !object.contains_key("bucket_seconds") {
                object.insert(
                    "bucket_seconds".to_string(),
                    Value::Number(serde_json::Number::from(300)),
                );
            }
        }
        "metric_rollup" => {
            let target = object
                .get("target")
                .and_then(Value::as_str)
                .unwrap_or("metric_rollups");
            if target != "metric_rollups" {
                return Err(DefinitionStoreError::InvalidConfig);
            }
            validate_string_expr(
                object
                    .get("metric_name")
                    .or_else(|| object.get("measure_name")),
            )?;
            validate_string_expr(object.get("metric_kind"))?;
            validate_number_expr(object.get("value"))?;
            if let Some(unit) = object.get("unit") {
                validate_string_expr(Some(unit))?;
            }
            if let Some(dimensions) = object.get("dimensions") {
                let dimensions = dimensions
                    .as_array()
                    .ok_or(DefinitionStoreError::InvalidConfig)?;
                for dimension in dimensions {
                    let dimension = dimension
                        .as_object()
                        .ok_or(DefinitionStoreError::InvalidConfig)?;
                    let name = dimension
                        .get("name")
                        .and_then(Value::as_str)
                        .ok_or(DefinitionStoreError::InvalidConfig)?;
                    validate_path(name)?;
                    validate_string_expr(dimension.get("value"))?;
                }
            }
            if !object.contains_key("bucket_seconds") {
                object.insert(
                    "bucket_seconds".to_string(),
                    Value::Number(serde_json::Number::from(60)),
                );
            }
        }
        "state" => {
            let target = object
                .get("target")
                .and_then(Value::as_str)
                .unwrap_or("entity_state_updates");
            if target != "entity_state_updates" {
                return Err(DefinitionStoreError::InvalidConfig);
            }
            validate_string_expr(object.get("entity_type"))?;
            validate_string_expr(object.get("entity_id"))?;
            validate_string_expr(object.get("state_name"))?;
            validate_string_expr(object.get("value"))?;
            let value_type = normalized_value_type(
                object
                    .get("value_type")
                    .and_then(Value::as_str)
                    .unwrap_or("string"),
            )?;
            object.insert("value_type".to_string(), Value::String(value_type));
        }
        "report" => {
            let target = object
                .get("target")
                .and_then(Value::as_str)
                .unwrap_or("report_results");
            if target != "report_results" {
                return Err(DefinitionStoreError::InvalidConfig);
            }
            if let Some(report_id) = object.get("report_id") {
                validate_string_expr(Some(report_id))?;
            }
            validate_dimensions(object.get("dimensions"))?;
            if mode == "trace_summary" {
                // Trace summaries derive trace_id/root/duration metrics from span-shaped events.
            } else if mode == "retention" {
                validate_string_expr(object.get("cohort_id"))?;
                validate_string_expr(object.get("entity_type"))?;
                validate_string_expr(object.get("entity_id"))?;
                if !object.contains_key("retention_bucket_seconds") {
                    object.insert(
                        "retention_bucket_seconds".to_string(),
                        Value::Number(serde_json::Number::from(86_400)),
                    );
                }
            } else if let Some(metrics) = object.get("metrics") {
                let metrics = metrics
                    .as_array()
                    .ok_or(DefinitionStoreError::InvalidConfig)?;
                if metrics.is_empty() {
                    return Err(DefinitionStoreError::InvalidConfig);
                }
                for metric in metrics {
                    let metric = metric
                        .as_object()
                        .ok_or(DefinitionStoreError::InvalidConfig)?;
                    let name = metric
                        .get("name")
                        .and_then(Value::as_str)
                        .ok_or(DefinitionStoreError::InvalidConfig)?;
                    validate_path(name)?;
                    match metric.get("op").and_then(Value::as_str).unwrap_or("count") {
                        "count" | "error_count" | "count_errors" => {}
                        "sum" => validate_number_expr(metric.get("value"))?,
                        _ => return Err(DefinitionStoreError::InvalidConfig),
                    }
                }
            }
            if !object.contains_key("bucket_seconds") {
                object.insert(
                    "bucket_seconds".to_string(),
                    Value::Number(serde_json::Number::from(60)),
                );
            }
        }
        "cohort" => {
            let target = object
                .get("target")
                .and_then(Value::as_str)
                .unwrap_or("cohort_memberships");
            if target != "cohort_memberships" {
                return Err(DefinitionStoreError::InvalidConfig);
            }
            if let Some(cohort_id) = object.get("cohort_id") {
                validate_string_expr(Some(cohort_id))?;
            }
            validate_string_expr(object.get("entity_type"))?;
            validate_string_expr(object.get("entity_id"))?;
        }
        "sequence" => {
            let target = object
                .get("target")
                .and_then(Value::as_str)
                .unwrap_or("sequence_report_results");
            if target != "sequence_report_results" {
                return Err(DefinitionStoreError::InvalidConfig);
            }
            if let Some(report_id) = object.get("report_id") {
                validate_string_expr(Some(report_id))?;
            }
            validate_string_expr(object.get("entity_id"))?;
            validate_dimensions(object.get("dimensions"))?;
            if let Some(segment) = object.get("segment") {
                validate_dimensions(Some(segment))?;
            }
            let steps = object
                .get("steps")
                .and_then(Value::as_array)
                .ok_or(DefinitionStoreError::InvalidConfig)?;
            if steps.is_empty() {
                return Err(DefinitionStoreError::InvalidConfig);
            }
            for step in steps {
                let step = step
                    .as_object()
                    .ok_or(DefinitionStoreError::InvalidConfig)?;
                let name = step
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or(DefinitionStoreError::InvalidConfig)?;
                validate_path(name)?;
                validate_matcher(step.get("match"))?;
            }
            if !object.contains_key("bucket_seconds") {
                object.insert(
                    "bucket_seconds".to_string(),
                    Value::Number(serde_json::Number::from(60)),
                );
            }
        }
        _ => return Err(DefinitionStoreError::InvalidKind),
    }
    Ok(())
}

fn validate_dimensions(value: Option<&Value>) -> Result<(), DefinitionStoreError> {
    let Some(dimensions) = value else {
        return Ok(());
    };
    let dimensions = dimensions
        .as_array()
        .ok_or(DefinitionStoreError::InvalidConfig)?;
    for dimension in dimensions {
        let dimension = dimension
            .as_object()
            .ok_or(DefinitionStoreError::InvalidConfig)?;
        let name = dimension
            .get("name")
            .and_then(Value::as_str)
            .ok_or(DefinitionStoreError::InvalidConfig)?;
        validate_path(name)?;
        validate_string_expr(dimension.get("value"))?;
    }
    Ok(())
}

fn validate_string_expr(value: Option<&Value>) -> Result<(), DefinitionStoreError> {
    match value {
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(()),
        Some(Value::Number(_)) | Some(Value::Bool(_)) => Ok(()),
        Some(Value::Object(object)) => {
            let path = object
                .get("path")
                .and_then(Value::as_str)
                .ok_or(DefinitionStoreError::InvalidConfig)?;
            validate_path(path)
        }
        _ => Err(DefinitionStoreError::InvalidConfig),
    }
}

fn validate_value_expr(value: Option<&Value>) -> Result<(), DefinitionStoreError> {
    match value {
        Some(Value::Object(object)) => {
            let path = object
                .get("path")
                .and_then(Value::as_str)
                .ok_or(DefinitionStoreError::InvalidConfig)?;
            validate_path(path)
        }
        Some(Value::String(_)) | Some(Value::Number(_)) | Some(Value::Bool(_)) => Ok(()),
        _ => Err(DefinitionStoreError::InvalidConfig),
    }
}

fn validate_number_expr(value: Option<&Value>) -> Result<(), DefinitionStoreError> {
    match value {
        Some(Value::Object(object)) => {
            let path = object
                .get("path")
                .and_then(Value::as_str)
                .ok_or(DefinitionStoreError::InvalidConfig)?;
            validate_path(path)
        }
        Some(Value::Number(_)) => Ok(()),
        Some(Value::String(value)) if value.parse::<f64>().is_ok() => Ok(()),
        _ => Err(DefinitionStoreError::InvalidConfig),
    }
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
        ("report", _) => serde_json::json!({
            "report_results": true,
            "aggregate": true,
            "materialized": true
        }),
        ("sequence", _) => serde_json::json!({
            "sequence_report_results": true,
            "materialized": true
        }),
        ("cohort", _) => serde_json::json!({
            "cohort_memberships": true,
            "materialized": true
        }),
        _ => Value::Object(serde_json::Map::new()),
    }
}

fn sdk_metric_definition(tenant_id: &str) -> DefinitionRecord {
    let now = clickhouse_now();
    DefinitionRecord {
        tenant_id: tenant_id.to_string(),
        definition_id: "sdk_metric_default_v1".to_string(),
        name: "sdk.metrics".to_string(),
        kind: "metric_rollup".to_string(),
        mode: "managed".to_string(),
        enabled: 1,
        config: serde_json::json!({
            "managed_by": "sdk",
            "sdk_surface": "metric",
            "match": {
                "all": [
                    { "path": "event_type", "op": "eq", "value": "metric" },
                    { "path": "metric_name", "op": "exists" },
                    { "path": "metric_value", "op": "is_number" }
                ]
            },
            "outputs": [
                {
                    "target": "metric_rollups",
                    "metric_name": { "path": "metric_name" },
                    "metric_kind": { "path": "metric_type", "default": "counter" },
                    "value": { "path": "metric_value" },
                    "unit": { "path": "metric_unit", "default": "" },
                    "dimensions": [
                        { "name": "service", "value": { "path": "service" } },
                        { "name": "environment", "value": { "path": "environment" } },
                        { "name": "signal", "value": { "path": "signal" } },
                        { "name": "metric_type", "value": { "path": "metric_type" } },
                        { "name": "llm.model", "value": { "path": "llm.model" } },
                        { "name": "llm.provider", "value": { "path": "llm.provider" } },
                        { "name": "loadtest_run_id", "value": { "path": "_loadtest.run_id" } }
                    ],
                    "bucket_seconds": 60
                }
            ]
        }),
        capabilities: serde_json::json!({
            "aggregate": true,
            "metric_rollup": true,
            "managed": true,
            "sdk_surface": "metric"
        }),
        created_at: now.clone(),
        updated_at: now,
        deleted_at: None,
        version: 1,
        backfill: None,
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
        "field" | "measure" | "rollup" | "metric_rollup" | "state" | "report" | "sequence"
        | "cohort" => Ok(value.trim().to_string()),
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
        "metric_rollup" => "managed".to_string(),
        "state" => "state_transition".to_string(),
        "report" => "summary".to_string(),
        "sequence" => "funnel".to_string(),
        "cohort" => "membership".to_string(),
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
            | ("metric_rollup", "managed")
            | ("state", "state_transition")
            | ("report", "summary")
            | ("report", "retention")
            | ("report", "trace_summary")
            | ("sequence", "funnel")
            | ("cohort", "membership")
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
    Ok(format!(
        "ifNull(toString({}), '')",
        payload_value_expression(path)
    ))
}

fn numeric_expression(path: &str) -> Result<String, DefinitionStoreError> {
    validate_path(path)?;
    Ok(format!(
        "toFloat64OrNull(ifNull(toString({}), ''))",
        payload_value_expression(path)
    ))
}

fn payload_value_expression(path: &str) -> String {
    if path.contains('.') {
        format!("getSubcolumn(data, '{}')", path.replace('\'', "''"))
    } else {
        format!("data.{path}")
    }
}

fn error_expression() -> &'static str {
    "lowerUTF8(ifNull(toString(data.is_error), '')) IN ('1', 'true') OR lowerUTF8(ifNull(toString(data.span_status_code), '')) = 'error' OR endsWith(lowerUTF8(ifNull(toString(data.event_type), '')), '_error')"
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
    use serde_json::json;

    use super::{
        BackfillStats, ClickHouseResponse, dimension_sql_outputs, matcher_where_clause,
        normalize_config, number_sql_expr, numeric_expression, sdk_metric_definition,
        string_sql_expr, value_expression,
    };

    #[test]
    fn backfill_stats_treats_null_distinct_values_as_zero() {
        let response: ClickHouseResponse<BackfillStats> =
            serde_json::from_str(r#"{"data":[{"rows_matched":0,"distinct_values":null}]}"#)
                .expect("response should deserialize");

        let stats = response.data.into_iter().next().expect("stats row");
        assert_eq!(stats.rows_matched, 0);
        assert_eq!(stats.distinct_values, 0);
    }

    #[test]
    fn backfill_expressions_use_nested_clickhouse_subcolumns() {
        assert_eq!(
            value_expression("llm.model").unwrap(),
            "ifNull(toString(getSubcolumn(data, 'llm.model')), '')"
        );
        assert_eq!(
            numeric_expression("llm.cost").unwrap(),
            "toFloat64OrNull(ifNull(toString(getSubcolumn(data, 'llm.cost')), ''))"
        );
        assert_eq!(
            value_expression("service").unwrap(),
            "ifNull(toString(data.service), '')"
        );
    }

    #[test]
    fn generalized_field_config_validates_outputs() {
        let config = normalize_config(
            "field",
            "facet",
            json!({
                "match": {
                    "all": [
                        { "path": "event_type", "op": "eq", "value": "llm.call" }
                    ]
                },
                "outputs": [
                    {
                        "target": "field_index",
                        "field_name": "llm.model",
                        "value": { "path": "llm.model" },
                        "value_type": "string"
                    }
                ]
            }),
        )
        .expect("generalized field config");

        assert_eq!(config["outputs"][0]["mode"], "facet");
        assert_eq!(config["outputs"][0]["value_type"], "string");
    }

    #[test]
    fn generalized_metric_rollup_config_validates_dynamic_sdk_shape() {
        let definition = sdk_metric_definition("fixture");
        let config = normalize_config("metric_rollup", "managed", definition.config)
            .expect("sdk metric config");

        assert_eq!(config["outputs"][0]["bucket_seconds"], 60);
        assert_eq!(config["outputs"][0]["metric_name"]["path"], "metric_name");
        assert_eq!(config["outputs"][0]["metric_kind"]["path"], "metric_type");
        assert_eq!(config["outputs"][0]["value"]["path"], "metric_value");
    }

    #[test]
    fn generalized_report_config_validates_summary_output() {
        let config = normalize_config(
            "report",
            "summary",
            json!({
                "match": {
                    "all": [
                        { "path": "event_type", "op": "eq", "value": "metric" }
                    ]
                },
                "outputs": [
                    {
                        "target": "report_results",
                        "report_id": "metrics_by_type",
                        "dimensions": [
                            { "name": "metric_type", "value": { "path": "metric_type" } }
                        ],
                        "metrics": [
                            { "name": "events", "op": "count" },
                            { "name": "value_sum", "op": "sum", "value": { "path": "metric_value" } }
                        ]
                    }
                ]
            }),
        )
        .expect("report config");

        assert_eq!(config["outputs"][0]["bucket_seconds"], 60);
        assert_eq!(config["outputs"][0]["target"], "report_results");
    }

    #[test]
    fn generalized_report_config_validates_retention_output() {
        let config = normalize_config(
            "report",
            "retention",
            json!({
                "match": {
                    "all": [
                        { "path": "event_type", "op": "eq", "value": "app_opened" }
                    ]
                },
                "outputs": [
                    {
                        "target": "report_results",
                        "report_id": "weekly_retention",
                        "cohort_id": "june_signups",
                        "entity_type": "user",
                        "entity_id": { "path": "user_id" },
                        "dimensions": [
                            { "name": "plan", "value": { "path": "plan" } }
                        ]
                    }
                ]
            }),
        )
        .expect("retention report config");

        assert_eq!(config["outputs"][0]["target"], "report_results");
        assert_eq!(config["outputs"][0]["retention_bucket_seconds"], 86_400);
    }

    #[test]
    fn generalized_report_config_validates_trace_summary_output() {
        let config = normalize_config(
            "report",
            "trace_summary",
            json!({
                "match": {
                    "all": [
                        { "path": "trace_id", "op": "exists" }
                    ]
                },
                "outputs": [
                    {
                        "target": "report_results",
                        "report_id": "top_slow_traces",
                        "dimensions": [
                            { "name": "service", "value": { "path": "service" } }
                        ]
                    }
                ]
            }),
        )
        .expect("trace summary config");

        assert_eq!(config["outputs"][0]["target"], "report_results");
        assert_eq!(config["outputs"][0]["bucket_seconds"], 60);
    }

    #[test]
    fn generalized_cohort_config_validates_membership_output() {
        let config = normalize_config(
            "cohort",
            "membership",
            json!({
                "match": {
                    "all": [
                        { "path": "account.plan", "op": "eq", "value": "pro" }
                    ]
                },
                "outputs": [
                    {
                        "target": "cohort_memberships",
                        "cohort_id": "pro_accounts",
                        "entity_type": "account",
                        "entity_id": { "path": "account.id" }
                    }
                ]
            }),
        )
        .expect("cohort config");

        assert_eq!(config["outputs"][0]["target"], "cohort_memberships");
        assert_eq!(config["outputs"][0]["cohort_id"], "pro_accounts");
    }

    #[test]
    fn generalized_sequence_config_validates_funnel_output() {
        let config = normalize_config(
            "sequence",
            "funnel",
            json!({
                "outputs": [
                    {
                        "target": "sequence_report_results",
                        "report_id": "signup_invite_checkout",
                        "entity_id": { "path": "user_id" },
                        "dimensions": [
                            { "name": "plan", "value": { "path": "plan" } }
                        ],
                        "steps": [
                            {
                                "name": "signup",
                                "match": { "all": [
                                    { "path": "event_type", "op": "eq", "value": "signup_completed" }
                                ] }
                            },
                            {
                                "name": "checkout",
                                "match": { "all": [
                                    { "path": "event_type", "op": "eq", "value": "checkout_completed" }
                                ] }
                            }
                        ]
                    }
                ]
            }),
        )
        .expect("sequence config");

        assert_eq!(config["outputs"][0]["target"], "sequence_report_results");
        assert_eq!(config["outputs"][0]["bucket_seconds"], 60);
    }

    #[test]
    fn generalized_config_rejects_invalid_matchers() {
        assert!(
            normalize_config(
                "measure",
                "measure",
                json!({
                    "match": {
                        "all": [
                            { "path": "event_type", "op": "contains", "value": "metric" }
                        ]
                    },
                    "outputs": [
                        {
                            "target": "event_measures",
                            "measure_name": "metric",
                            "value": { "path": "metric_value" }
                        }
                    ]
                }),
            )
            .is_err()
        );
    }

    #[test]
    fn generalized_backfill_sql_builds_matchers_and_expressions() {
        let config = json!({
            "match": {
                "all": [
                    { "path": "event_type", "op": "eq", "value": "metric" },
                    { "path": "metric_value", "op": "is_number" },
                    { "path": "metric_type", "op": "in", "value": ["counter", "gauge"] }
                ]
            }
        });
        let clause = matcher_where_clause(&config)
            .expect("matcher clause")
            .unwrap();

        assert!(clause.contains("ifNull(toString(data.event_type), '') = 'metric'"));
        assert!(
            clause.contains("toFloat64OrNull(ifNull(toString(data.metric_value), '')) IS NOT NULL")
        );
        assert!(clause.contains("ifNull(toString(data.metric_type), '') IN ('counter', 'gauge')"));

        assert_eq!(
            string_sql_expr(&json!({ "path": "metric_unit", "default": "1" })).unwrap(),
            "if(ifNull(toString(data.metric_unit), '') = '', '1', ifNull(toString(data.metric_unit), ''))"
        );
        assert_eq!(
            number_sql_expr(&json!({ "path": "metric_value" })).unwrap(),
            "toFloat64OrNull(ifNull(toString(data.metric_value), ''))"
        );
    }

    #[test]
    fn generalized_backfill_dimensions_build_value_expressions() {
        let output = json!({
            "dimensions": [
                { "name": "llm.model", "value": { "path": "llm.model" } },
                { "name": "environment", "value": { "path": "environment" } }
            ]
        });
        let dimensions = dimension_sql_outputs(output.as_object().unwrap()).unwrap();

        assert_eq!(dimensions.len(), 2);
        assert_eq!(dimensions[0].name, "llm.model");
        assert_eq!(
            dimensions[0].value_expr,
            "ifNull(toString(getSubcolumn(data, 'llm.model')), '')"
        );
        assert_eq!(dimensions[1].name, "environment");
        assert_eq!(
            dimensions[1].value_expr,
            "ifNull(toString(data.environment), '')"
        );
    }
}
