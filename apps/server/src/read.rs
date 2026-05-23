use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
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

#[derive(Debug, Deserialize, Serialize, utoipa::ToSchema)]
pub struct QueryRequest {
    pub query: String,
    #[serde(default)]
    pub parameters: Map<String, Value>,
    #[serde(default)]
    pub allow_stale_serving: bool,
}

#[derive(Debug, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EventsQueryRequest {
    #[serde(default)]
    pub view: EventsQueryView,
    #[serde(default)]
    pub filter: EventFilter,
    #[serde(default)]
    pub group_by: String,
    #[serde(default)]
    pub selected_group_value: String,
    #[serde(default)]
    pub time_range: Option<EventTimeRange>,
    #[serde(default)]
    pub page: EventPage,
    #[serde(default = "default_events_query_limit")]
    pub limit: u64,
    #[serde(default)]
    pub offset: u64,
    #[serde(default = "default_events_query_buckets")]
    pub buckets: u64,
    #[serde(default, alias = "orderBy")]
    pub sort: EventsQuerySort,
    #[serde(default)]
    pub search: String,
    #[serde(default)]
    pub allow_stale_serving: bool,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventsQueryView {
    GroupOptions,
    Groups,
    Latest,
    Summary,
    #[default]
    Events,
    Density,
    Flamegraph,
    Event,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EventFilter {
    #[serde(default)]
    pub created_after: String,
    #[serde(default)]
    pub created_before: String,
    #[serde(default)]
    pub facets: Vec<EventFacetFilter>,
    #[serde(default)]
    pub text: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EventFacetFilter {
    #[serde(default)]
    pub join: EventFacetJoin,
    #[serde(default)]
    pub negated: bool,
    #[serde(default)]
    pub operator: EventFacetOperator,
    pub path: String,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventFacetJoin {
    #[default]
    And,
    Or,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventFacetOperator {
    Contains,
    #[default]
    Eq,
    In,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EventTimeRange {
    #[serde(default)]
    pub created_after: String,
    #[serde(default)]
    pub created_before: String,
    #[serde(default)]
    pub lookback_minutes: u64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EventPage {
    #[serde(default)]
    pub after: String,
    #[serde(default)]
    pub around: String,
    #[serde(default)]
    pub before: String,
    #[serde(default)]
    pub event_id: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EventsQuerySort {
    #[serde(default)]
    pub direction: EventSortDirection,
    #[serde(default)]
    pub group: GroupSortKey,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventSortDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum GroupSortKey {
    #[default]
    Count,
    Duration,
    Recent,
    Value,
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

    pub async fn events_query(
        &self,
        mut request: EventsQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        request.limit = request.limit.clamp(1, 10_000);
        request.buckets = request.buckets.clamp(1, 2_000);

        let catalog = match self
            .event_field_catalog(tenant_id, request.allow_stale_serving)
            .await
        {
            Ok(catalog) => catalog,
            Err(err) => {
                warn!(error = %err, "failed to load promoted field catalog; using raw fallback planning");
                EventFieldCatalog::default()
            }
        };

        match request.view {
            EventsQueryView::GroupOptions => {
                let mut parameters = Map::new();
                parameters.insert("limit".to_string(), Value::from(request.limit));
                self.query(
                    QueryRequest {
                        query: group_options_query(),
                        parameters,
                        allow_stale_serving: true,
                    },
                    tenant_id,
                )
                .await
            }
            EventsQueryView::Groups => self.groups_query(&request, &catalog, tenant_id).await,
            EventsQueryView::Latest => self.latest_query(&request, &catalog, tenant_id).await,
            EventsQueryView::Summary => self.summary_query(&request, &catalog, tenant_id).await,
            EventsQueryView::Events => self.events_page_query(&request, &catalog, tenant_id).await,
            EventsQueryView::Density => self.density_query(&request, &catalog, tenant_id).await,
            EventsQueryView::Flamegraph => {
                self.flamegraph_query(&request, &catalog, tenant_id).await
            }
            EventsQueryView::Event => self.event_query(&request, tenant_id).await,
        }
    }

    async fn run_events_query_sql(
        &self,
        query: String,
        parameters: Map<String, Value>,
        request: &EventsQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        self.query(
            QueryRequest {
                query,
                parameters,
                allow_stale_serving: request.allow_stale_serving,
            },
            tenant_id,
        )
        .await
    }

    async fn event_field_catalog(
        &self,
        tenant_id: &str,
        allow_stale_serving: bool,
    ) -> Result<EventFieldCatalog, ReadError> {
        let response = self
            .query(
                QueryRequest {
                    query: "SELECT name, config FROM definitions WHERE kind = 'field' AND enabled = 1 AND isNull(deleted_at)".to_string(),
                    parameters: Map::new(),
                    allow_stale_serving,
                },
                tenant_id,
            )
            .await?;
        let mut catalog = EventFieldCatalog::default();
        if let Some(rows) = response.get("data").and_then(Value::as_array) {
            for row in rows {
                if let Some(name) = row.get("name").and_then(Value::as_str) {
                    catalog.promoted.insert(normalized_payload_path(name));
                }
                if let Some(outputs) = row
                    .get("config")
                    .and_then(|config| config.get("outputs"))
                    .and_then(Value::as_array)
                {
                    for output in outputs {
                        if output
                            .get("target")
                            .and_then(Value::as_str)
                            .is_some_and(|target| target != "field_index")
                        {
                            continue;
                        }
                        if let Some(field_name) = output.get("field_name").and_then(Value::as_str) {
                            catalog.promoted.insert(normalized_payload_path(field_name));
                        }
                    }
                }
            }
        }
        Ok(catalog)
    }

    async fn groups_query(
        &self,
        request: &EventsQueryRequest,
        catalog: &EventFieldCatalog,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        let group_by = facet_key(&request.group_by)?;
        let (primary_query, primary_parameters) = if catalog.core_rollup_contains(&group_by) {
            grouped_rollup_query(request, &group_by)
        } else if catalog.lookup_contains(&group_by) {
            grouped_index_query(request, &group_by, "field_values", false)
        } else if catalog.promoted_contains(&group_by) {
            grouped_index_query(request, &group_by, "field_index", true)
        } else if catalog.raw_groupable_contains(&group_by) {
            raw_groups_query(request, &group_by)?
        } else {
            raw_groups_query(request, &group_by)?
        };

        let response = self
            .run_events_query_sql(primary_query, primary_parameters, request, tenant_id)
            .await?;
        if response_data_len(&response) > 0 || !catalog.has_indexed_path(&group_by) {
            return Ok(response);
        }

        let (query, parameters) = raw_groups_query(request, &group_by)?;
        self.run_events_query_sql(query, parameters, request, tenant_id)
            .await
    }

    async fn latest_query(
        &self,
        request: &EventsQueryRequest,
        catalog: &EventFieldCatalog,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        if request.filter.facets.is_empty()
            && request.filter.text.trim().is_empty()
            && !request.group_by.trim().is_empty()
            && !request.selected_group_value.is_empty()
        {
            let group_by = facet_key(&request.group_by)?;
            if catalog.core_rollup_contains(&group_by) {
                let (query, parameters) = latest_grouped_rollup_query(request, &group_by);
                return self
                    .run_events_query_sql(query, parameters, request, tenant_id)
                    .await;
            }
            if catalog.lookup_contains(&group_by) {
                let (query, parameters) =
                    latest_grouped_index_query(request, &group_by, "field_values", false);
                return self
                    .run_events_query_sql(query, parameters, request, tenant_id)
                    .await;
            }
            if catalog.promoted_contains(&group_by) {
                let (query, parameters) =
                    latest_grouped_index_query(request, &group_by, "field_index", true);
                return self
                    .run_events_query_sql(query, parameters, request, tenant_id)
                    .await;
            }
        }

        let plan = EventPredicatePlan::new(request, catalog, "e")?;
        let where_clause = plan.where_clause();
        self.run_events_query_sql(
            [
                "SELECT max(e.timestamp) AS lastCreatedAt".to_string(),
                "FROM events AS e".to_string(),
                where_clause,
            ]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" "),
            plan.parameters,
            request,
            tenant_id,
        )
        .await
    }

    async fn summary_query(
        &self,
        request: &EventsQueryRequest,
        catalog: &EventFieldCatalog,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        if request.selected_group_value.is_empty()
            && request.group_by.is_empty()
            && request.filter.facets.is_empty()
            && request.filter.text.trim().is_empty()
        {
            let (time_clause, parameters) = time_where_clause(
                &request.filter,
                request.time_range.as_ref(),
                "d.bucket_time",
            );
            return self
                .run_events_query_sql(
                    [
                        "SELECT sum(count) AS count".to_string(),
                        "FROM event_density_1s AS d".to_string(),
                        where_keyword(time_clause),
                    ]
                    .into_iter()
                    .filter(|part| !part.is_empty())
                    .collect::<Vec<_>>()
                    .join(" "),
                    parameters,
                    request,
                    tenant_id,
                )
                .await;
        }

        if request.filter.facets.is_empty()
            && request.filter.text.trim().is_empty()
            && !request.group_by.is_empty()
            && !request.selected_group_value.is_empty()
        {
            let group_by = facet_key(&request.group_by)?;
            if catalog.core_rollup_contains(&group_by) {
                let (time_clause, mut parameters) = time_where_clause(
                    &request.filter,
                    request.time_range.as_ref(),
                    "d.bucket_time",
                );
                parameters.insert("group_key".to_string(), Value::from(group_by));
                parameters.insert(
                    "group_value".to_string(),
                    Value::from(request.selected_group_value.clone()),
                );
                return self
                    .run_events_query_sql(
                        [
                            "SELECT sum(d.count) AS count".to_string(),
                            "FROM field_rollups AS d".to_string(),
                            where_keyword(join_clauses(vec![
                                "d.field_name = {group_key:String}".to_string(),
                                "d.value = {group_value:String}".to_string(),
                                "d.bucket_seconds = 1".to_string(),
                                time_clause,
                            ])),
                        ]
                        .into_iter()
                        .filter(|part| !part.is_empty())
                        .collect::<Vec<_>>()
                        .join(" "),
                        parameters,
                        request,
                        tenant_id,
                    )
                    .await;
            }
        }

        let plan = EventPredicatePlan::new(request, catalog, "e")?;
        self.run_events_query_sql(
            [
                "SELECT count() AS count".to_string(),
                "FROM events AS e".to_string(),
                plan.where_clause(),
            ]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" "),
            plan.parameters,
            request,
            tenant_id,
        )
        .await
    }

    async fn events_page_query(
        &self,
        request: &EventsQueryRequest,
        catalog: &EventFieldCatalog,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        let mut plan = EventPredicatePlan::new(request, catalog, "e")?;
        let page_filter = event_page_filter(request, "e", &mut plan.parameters);
        if !page_filter.is_empty() {
            plan.clauses.push(page_filter);
        }
        let query_order = event_query_order(request);
        let table_order = event_table_order(request);
        let query = [
            event_metadata_select("e"),
            "FROM events AS e".to_string(),
            "WHERE (e.event_id, e.timestamp) IN (".to_string(),
            "SELECT event_id, timestamp FROM (".to_string(),
            "SELECT e.event_id AS event_id, e.timestamp AS timestamp".to_string(),
            "FROM events AS e".to_string(),
            plan.where_clause(),
            format!("ORDER BY e.timestamp {query_order}, e.event_id {query_order}"),
            "LIMIT {limit:UInt64}".to_string(),
            ")".to_string(),
            ")".to_string(),
            format!("ORDER BY e.timestamp {table_order}, e.event_id {table_order}"),
        ]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
        plan.parameters
            .insert("limit".to_string(), Value::from(request.limit));
        self.run_events_query_sql(query, plan.parameters, request, tenant_id)
            .await
    }

    async fn density_query(
        &self,
        request: &EventsQueryRequest,
        catalog: &EventFieldCatalog,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        if request.selected_group_value.is_empty()
            && request.group_by.is_empty()
            && request.filter.facets.is_empty()
            && request.filter.text.trim().is_empty()
        {
            let (time_clause, mut parameters) = time_where_clause(
                &request.filter,
                request.time_range.as_ref(),
                "d.bucket_time",
            );
            parameters.insert("buckets".to_string(), Value::from(request.buckets));
            let range = self
                .run_events_query_sql(
                    [
                        "SELECT min(d.bucket_time) AS from, max(d.bucket_time) AS to, sum(d.count) AS count".to_string(),
                        "FROM event_density_1s AS d".to_string(),
                        where_keyword(time_clause.clone()),
                    ]
                    .into_iter()
                    .filter(|part| !part.is_empty())
                    .collect::<Vec<_>>()
                    .join(" "),
                    parameters.clone(),
                    request,
                    tenant_id,
                )
                .await?;
            let bucket_ms = density_bucket_ms(&range, request.buckets, 1_000);
            parameters.insert("bucket_ms".to_string(), Value::from(bucket_ms));
            return self
                .run_events_query_sql(
                    [
                        "WITH intDiv(toUnixTimestamp64Milli(d.bucket_time), {bucket_ms:UInt64}) * {bucket_ms:UInt64} AS bucket".to_string(),
                        "SELECT bucket, sum(d.count) AS count, sum(d.error_count) AS errorCount".to_string(),
                        "FROM event_density_1s AS d".to_string(),
                        where_keyword(time_clause),
                        "GROUP BY bucket ORDER BY bucket ASC".to_string(),
                    ]
                    .into_iter()
                    .filter(|part| !part.is_empty())
                    .collect::<Vec<_>>()
                    .join(" "),
                    parameters,
                    request,
                    tenant_id,
                )
                .await;
        }

        if request.filter.facets.is_empty()
            && request.filter.text.trim().is_empty()
            && !request.group_by.is_empty()
            && !request.selected_group_value.is_empty()
        {
            let group_by = facet_key(&request.group_by)?;
            if catalog.core_rollup_contains(&group_by) {
                let (time_clause, mut parameters) = time_where_clause(
                    &request.filter,
                    request.time_range.as_ref(),
                    "d.bucket_time",
                );
                parameters.insert("group_key".to_string(), Value::from(group_by));
                parameters.insert(
                    "group_value".to_string(),
                    Value::from(request.selected_group_value.clone()),
                );
                let base = [
                    "FROM field_rollups AS d".to_string(),
                    where_keyword(join_clauses(vec![
                        "d.field_name = {group_key:String}".to_string(),
                        "d.value = {group_value:String}".to_string(),
                        "d.bucket_seconds = 1".to_string(),
                        time_clause.clone(),
                    ])),
                ]
                .into_iter()
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>()
                .join(" ");
                let range = self
                    .run_events_query_sql(
                        format!("SELECT min(d.bucket_time) AS from, max(d.bucket_time) AS to, sum(d.count) AS count {base}"),
                        parameters.clone(),
                        request,
                        tenant_id,
                    )
                    .await?;
                let bucket_ms = density_bucket_ms(&range, request.buckets, 1);
                parameters.insert("bucket_ms".to_string(), Value::from(bucket_ms));
                return self
                    .run_events_query_sql(
                        [
                            "WITH intDiv(toUnixTimestamp64Milli(d.bucket_time), {bucket_ms:UInt64}) * {bucket_ms:UInt64} AS bucket".to_string(),
                            "SELECT bucket, sum(d.count) AS count, sum(d.error_count) AS errorCount".to_string(),
                            base,
                            "GROUP BY bucket ORDER BY bucket ASC".to_string(),
                        ]
                        .join(" "),
                        parameters,
                        request,
                        tenant_id,
                    )
                    .await;
            }
        }

        let plan = EventPredicatePlan::new(request, catalog, "e")?;
        let range = self
            .run_events_query_sql(
                [
                    "SELECT min(e.timestamp) AS from, max(e.timestamp) AS to, count() AS count"
                        .to_string(),
                    "FROM events AS e".to_string(),
                    plan.where_clause(),
                ]
                .into_iter()
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>()
                .join(" "),
                plan.parameters.clone(),
                request,
                tenant_id,
            )
            .await?;
        let mut parameters = plan.parameters;
        parameters.insert(
            "bucket_ms".to_string(),
            Value::from(density_bucket_ms(&range, request.buckets, 1)),
        );
        self.run_events_query_sql(
            [
                "WITH intDiv(toUnixTimestamp64Milli(e.timestamp), {bucket_ms:UInt64}) * {bucket_ms:UInt64} AS bucket".to_string(),
                "SELECT bucket, count() AS count".to_string(),
                format!(", countIf({}) AS errorCount", error_expression("e")),
                "FROM events AS e".to_string(),
                EventPredicatePlan { parameters: parameters.clone(), ..plan }.where_clause(),
                "GROUP BY bucket ORDER BY bucket ASC".to_string(),
            ]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" "),
            parameters,
            request,
            tenant_id,
        )
        .await
    }

    async fn flamegraph_query(
        &self,
        request: &EventsQueryRequest,
        catalog: &EventFieldCatalog,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        let mut plan = EventPredicatePlan::new(request, catalog, "e")?;
        plan.parameters
            .insert("limit".to_string(), Value::from(request.limit));
        self.run_events_query_sql(
            [
                flamegraph_select("e"),
                "FROM events AS e".to_string(),
                plan.where_clause(),
                "ORDER BY e.timestamp ASC, e.event_id ASC".to_string(),
                "LIMIT {limit:UInt64}".to_string(),
            ]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" "),
            plan.parameters,
            request,
            tenant_id,
        )
        .await
    }

    async fn event_query(
        &self,
        request: &EventsQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        if request.page.event_id.trim().is_empty() {
            return Err(ReadError::InvalidQuery("event_id is required".to_string()));
        }
        let mut parameters = Map::new();
        parameters.insert(
            "event_id".to_string(),
            Value::from(request.page.event_id.clone()),
        );
        self.run_events_query_sql(
            [
                "SELECT e.event_id AS event_id, e.timestamp AS timestamp, e.data AS data".to_string(),
                ", e.event_type AS event_type, e.signal AS signal, e.trace_id AS trace_id, e.span_id AS span_id".to_string(),
                "FROM events AS e".to_string(),
                "WHERE e.event_id = {event_id:String}".to_string(),
                "ORDER BY e.timestamp ASC LIMIT 1".to_string(),
            ]
            .join(" "),
            parameters,
            request,
            tenant_id,
        )
        .await
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
        [
            "field_index",
            "event_measures",
            "counter_rollups",
            "gauge_rollups",
            "histogram_rollups",
            "entity_state_updates",
            "report_results",
            "sequence_report_results",
            "cohort_memberships",
        ]
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
            "field_rollups",
            "flamegraph_rollups_1m",
            "event_density_1s",
            "definitions",
            "event_measures",
            "measure_rollups",
            "counter_rollups",
            "gauge_rollups",
            "histogram_rollups",
            "entity_state_updates",
            "report_results",
            "sequence_report_results",
            "cohort_memberships",
            "definition_stats",
            "query_usage",
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

fn default_events_query_limit() -> u64 {
    100
}

fn default_events_query_buckets() -> u64 {
    120
}

#[derive(Clone, Default)]
struct EventFieldCatalog {
    promoted: BTreeSet<String>,
}

impl EventFieldCatalog {
    fn promoted_contains(&self, path: &str) -> bool {
        self.promoted.contains(path)
    }

    fn lookup_contains(&self, path: &str) -> bool {
        LOOKUP_FIELD_NAMES.contains(&path)
    }

    fn core_rollup_contains(&self, path: &str) -> bool {
        CORE_ROLLUP_FIELD_NAMES.contains(&path)
    }

    fn raw_groupable_contains(&self, path: &str) -> bool {
        RAW_GROUPABLE_FIELD_NAMES.contains(&path)
    }

    fn has_indexed_path(&self, path: &str) -> bool {
        self.promoted_contains(path) || self.lookup_contains(path)
    }

    fn index_table(&self, path: &str) -> Option<IndexTable> {
        if self.lookup_contains(path) {
            Some(IndexTable::FieldValues)
        } else if self.promoted_contains(path) {
            Some(IndexTable::FieldIndex)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy)]
enum IndexTable {
    FieldIndex,
    FieldValues,
}

impl IndexTable {
    fn table_name(self) -> &'static str {
        match self {
            Self::FieldIndex => "field_index",
            Self::FieldValues => "field_values",
        }
    }

    fn include_mode(self) -> bool {
        matches!(self, Self::FieldIndex)
    }
}

const CORE_ROLLUP_FIELD_NAMES: &[&str] = &[
    "signal",
    "event_type",
    "name",
    "service",
    "environment",
    "is_error",
];

const RAW_GROUPABLE_FIELD_NAMES: &[&str] = &[
    "signal",
    "event_type",
    "name",
    "service",
    "environment",
    "is_error",
    "http.method",
    "http.route",
    "http.status_code",
    "http.response.status_code",
    "severity_text",
    "severity_number",
    "llm.model",
    "llm.provider",
    "tool_name",
    "processor_name",
    "plan",
    "country",
    "region",
    "user_group",
    "user_id",
    "account_id",
    "anonymous_id",
    "session_id",
    "group_id",
    "organization_id",
    "thread_id",
    "conversation_id",
    "request_id",
    "duration_ms",
    "metric_name",
    "metric_type",
    "metric_unit",
    "metric_value",
    "revenue",
    "currency",
];

const LOOKUP_FIELD_NAMES: &[&str] = &[
    "trace_id",
    "span_id",
    "request_id",
    "user_id",
    "anonymous_id",
    "account_id",
    "session_id",
    "group_id",
    "organization_id",
    "thread_id",
    "conversation_id",
];

#[derive(Clone)]
struct EventPredicatePlan {
    clauses: Vec<String>,
    parameters: Map<String, Value>,
}

impl EventPredicatePlan {
    fn new(
        request: &EventsQueryRequest,
        catalog: &EventFieldCatalog,
        alias: &str,
    ) -> Result<Self, ReadError> {
        let mut builder = SqlBuilder::default();
        let mut clauses = Vec::new();
        let (time_clause, time_parameters) = time_where_clause(
            &request.filter,
            request.time_range.as_ref(),
            &format!("{alias}.timestamp"),
        );
        clauses.push(time_clause);
        builder.parameters.extend(time_parameters);

        if !request.group_by.trim().is_empty() && !request.selected_group_value.is_empty() {
            let path = facet_key(&request.group_by)?;
            let value_param = builder.push_string("group_value", &request.selected_group_value);
            let group_clause = if let Some(index_table) = catalog.index_table(&path) {
                indexed_value_clause(
                    alias,
                    index_table,
                    &path,
                    vec![value_param],
                    &request.filter,
                    request.time_range.as_ref(),
                    &mut builder,
                )
            } else {
                format!(
                    "{} = {{{value_param}:String}}",
                    event_value_expression(&path, alias)?
                )
            };
            clauses.push(group_clause);
        }

        let facet_expr = filter_facets_expression(
            &request.filter.facets,
            catalog,
            alias,
            &request.filter,
            request.time_range.as_ref(),
            &mut builder,
        )?;
        clauses.push(facet_expr);

        if !request.filter.text.trim().is_empty() {
            let param = builder.push_string("event_filter", request.filter.text.trim());
            clauses.push(format!(
                "(positionCaseInsensitive(toJSONString({alias}.data), {{{param}:String}}) > 0 OR positionCaseInsensitive({alias}.event_id, {{{param}:String}}) > 0)"
            ));
        }

        Ok(Self {
            clauses: clauses
                .into_iter()
                .filter(|clause| !clause.is_empty())
                .collect(),
            parameters: builder.parameters,
        })
    }

    fn where_clause(&self) -> String {
        where_keyword(join_clauses(self.clauses.clone()))
    }
}

#[derive(Default)]
struct SqlBuilder {
    parameters: Map<String, Value>,
    next_parameter: usize,
}

impl SqlBuilder {
    fn push_string(&mut self, prefix: &str, value: &str) -> String {
        let key = self.next_key(prefix);
        self.parameters
            .insert(key.clone(), Value::String(value.to_string()));
        key
    }

    fn next_key(&mut self, prefix: &str) -> String {
        let key = format!("{prefix}_{}", self.next_parameter);
        self.next_parameter += 1;
        key
    }
}

fn filter_facets_expression(
    facets: &[EventFacetFilter],
    catalog: &EventFieldCatalog,
    alias: &str,
    filter: &EventFilter,
    time_range: Option<&EventTimeRange>,
    builder: &mut SqlBuilder,
) -> Result<String, ReadError> {
    let mut branches: Vec<Vec<&EventFacetFilter>> = vec![Vec::new()];
    for facet in facets {
        if facet.join == EventFacetJoin::Or
            && branches.last().is_some_and(|branch| !branch.is_empty())
        {
            branches.push(Vec::new());
        }
        if let Some(branch) = branches.last_mut() {
            branch.push(facet);
        }
    }

    let mut branch_sql = Vec::new();
    for branch in branches {
        let mut clauses = Vec::new();
        for facet in branch {
            clauses.push(facet_clause(
                facet, catalog, alias, filter, time_range, builder,
            )?);
        }
        let joined = join_clauses(clauses);
        if !joined.is_empty() {
            branch_sql.push(format!("({joined})"));
        }
    }

    Ok(match branch_sql.len() {
        0 => String::new(),
        1 => branch_sql.remove(0),
        _ => format!("({})", branch_sql.join(" OR ")),
    })
}

fn facet_clause(
    facet: &EventFacetFilter,
    catalog: &EventFieldCatalog,
    alias: &str,
    filter: &EventFilter,
    time_range: Option<&EventTimeRange>,
    builder: &mut SqlBuilder,
) -> Result<String, ReadError> {
    let path = facet_key(&facet.path)?;
    let operator = facet.operator;
    let values = facet_values(facet);
    if !facet.negated && matches!(operator, EventFacetOperator::Eq | EventFacetOperator::In) {
        if let Some(index_table) = catalog.index_table(&path) {
            let params = values
                .iter()
                .map(|value| builder.push_string("facet_value", value))
                .collect::<Vec<_>>();
            return Ok(indexed_value_clause(
                alias,
                index_table,
                &path,
                params,
                filter,
                time_range,
                builder,
            ));
        }
    }

    let expression = event_value_expression(&path, alias)?;
    let mut clause = match operator {
        EventFacetOperator::Contains => {
            let value = values.first().cloned().unwrap_or_default();
            let param = builder.push_string("facet_value", &value);
            format!("positionCaseInsensitive({expression}, {{{param}:String}}) > 0")
        }
        EventFacetOperator::In => {
            if values.is_empty() {
                "0".to_string()
            } else {
                let placeholders = values
                    .iter()
                    .map(|value| {
                        let param = builder.push_string("facet_value", value);
                        format!("{{{param}:String}}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{expression} IN ({placeholders})")
            }
        }
        EventFacetOperator::Eq => {
            let value = values.first().cloned().unwrap_or_default();
            let param = builder.push_string("facet_value", &value);
            format!("{expression} = {{{param}:String}}")
        }
    };
    if facet.negated {
        clause = format!("NOT ({clause})");
    }
    Ok(format!("({clause})"))
}

fn facet_values(facet: &EventFacetFilter) -> Vec<String> {
    if facet.operator == EventFacetOperator::In {
        facet
            .values
            .iter()
            .filter(|value| !value.is_empty())
            .cloned()
            .collect()
    } else if facet.value.is_empty() {
        Vec::new()
    } else {
        vec![facet.value.clone()]
    }
}

fn indexed_value_clause(
    alias: &str,
    index_table: IndexTable,
    path: &str,
    value_params: Vec<String>,
    filter: &EventFilter,
    time_range: Option<&EventTimeRange>,
    builder: &mut SqlBuilder,
) -> String {
    let field_param = builder.push_string("facet_key", path);
    let value_clause = if value_params.len() == 1 {
        format!("idx.value = {{{}:String}}", value_params[0])
    } else {
        let placeholders = value_params
            .iter()
            .map(|param| format!("{{{param}:String}}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("idx.value IN ({placeholders})")
    };
    let (time_clause, time_parameters) = time_where_clause(filter, time_range, "idx.timestamp");
    builder.parameters.extend(time_parameters);
    let mode_clause = if index_table.include_mode() {
        "idx.mode IN ('facet', 'lookup')".to_string()
    } else {
        String::new()
    };
    let subquery_where = join_clauses(vec![
        format!("idx.field_name = {{{field_param}:String}}"),
        value_clause,
        mode_clause,
        time_clause,
    ]);
    format!(
        "{alias}.event_id IN (SELECT idx.event_id FROM {} AS idx WHERE {subquery_where})",
        index_table.table_name()
    )
}

fn group_options_query() -> String {
    let core_fields = CORE_ROLLUP_FIELD_NAMES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|field| format!("'{}'", field.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    let lookup_fields = LOOKUP_FIELD_NAMES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|field| format!("'{}'", field.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    let raw_fields = RAW_GROUPABLE_FIELD_NAMES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|field| format!("'{}'", field.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT path, max(cardinality) AS cardinality, argMin(valueType, priority) AS valueType, toBool(0) AS capped, argMin(source, priority) AS source, argMin(servingMode, priority) AS servingMode, max(aggregateEnabled) AS aggregateEnabled, max(indexEnabled) AS indexEnabled FROM (SELECT arrayJoin([{core_fields}]) AS path, toUInt64(0) AS cardinality, 'string' AS valueType, 'builtin' AS source, 'rollup' AS servingMode, toBool(1) AS aggregateEnabled, toBool(0) AS indexEnabled, toUInt8(1) AS priority UNION ALL SELECT arrayJoin([{lookup_fields}]) AS path, toUInt64(0) AS cardinality, 'string' AS valueType, 'builtin' AS source, 'index' AS servingMode, toBool(0) AS aggregateEnabled, toBool(1) AS indexEnabled, toUInt8(2) AS priority UNION ALL SELECT name AS path, toUInt64(0) AS cardinality, ifNull(toString(config.value_type), 'string') AS valueType, 'definition' AS source, 'index' AS servingMode, toBool(1) AS aggregateEnabled, toBool(1) AS indexEnabled, toUInt8(3) AS priority FROM definitions WHERE kind = 'field' AND enabled = 1 AND isNull(deleted_at) UNION ALL SELECT JSONExtractString(output, 'field_name') AS path, toUInt64(0) AS cardinality, ifNull(JSONExtractString(output, 'value_type'), 'string') AS valueType, 'definition' AS source, 'index' AS servingMode, toBool(1) AS aggregateEnabled, toBool(1) AS indexEnabled, toUInt8(3) AS priority FROM (SELECT arrayJoin(JSONExtractArrayRaw(ifNull(toJSONString(config.outputs), '[]'))) AS output FROM definitions WHERE kind = 'field' AND enabled = 1 AND isNull(deleted_at)) WHERE path != '' UNION ALL SELECT arrayJoin([{raw_fields}]) AS path, toUInt64(0) AS cardinality, 'string' AS valueType, 'raw' AS source, 'raw' AS servingMode, toBool(0) AS aggregateEnabled, toBool(0) AS indexEnabled, toUInt8(4) AS priority) GROUP BY path ORDER BY cardinality DESC, path ASC LIMIT {{limit:UInt64}}"
    )
}

fn grouped_rollup_query(
    request: &EventsQueryRequest,
    group_by: &str,
) -> (String, Map<String, Value>) {
    let mut parameters = time_parameters(request.time_range.as_ref());
    parameters.insert("group_key".to_string(), Value::from(group_by.to_string()));
    parameters.insert("limit".to_string(), Value::from(request.limit + 1));
    parameters.insert("offset".to_string(), Value::from(request.offset));
    let mut clauses = vec![
        "field_name = {group_key:String}".to_string(),
        "bucket_seconds = 60".to_string(),
        time_range_clause(request.time_range.as_ref(), "bucket_time"),
    ];
    if !request.search.is_empty() {
        parameters.insert(
            "group_value".to_string(),
            Value::from(request.search.clone()),
        );
        clauses.push("value = {group_value:String}".to_string());
    }
    (
        [
            "SELECT value".to_string(),
            ", min(bucket_time) AS startedAt, max(bucket_time) AS endedAt".to_string(),
            ", dateDiff('millisecond', min(bucket_time), max(bucket_time)) AS durationMs"
                .to_string(),
            ", sum(count) AS count, sum(error_count) AS errorCount".to_string(),
            "FROM field_rollups".to_string(),
            where_keyword(join_clauses(clauses)),
            "GROUP BY value".to_string(),
            group_order_by_clause(group_by, true, request.sort.group),
            "LIMIT {limit:UInt64} OFFSET {offset:UInt64}".to_string(),
        ]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" "),
        parameters,
    )
}

fn grouped_index_query(
    request: &EventsQueryRequest,
    group_by: &str,
    table: &str,
    include_mode: bool,
) -> (String, Map<String, Value>) {
    let mut parameters = time_parameters(request.time_range.as_ref());
    parameters.insert("group_key".to_string(), Value::from(group_by.to_string()));
    parameters.insert("limit".to_string(), Value::from(request.limit + 1));
    parameters.insert("offset".to_string(), Value::from(request.offset));
    let mut clauses = vec![
        "field_name = {group_key:String}".to_string(),
        "value != ''".to_string(),
        time_range_clause(request.time_range.as_ref(), "timestamp"),
    ];
    if include_mode {
        clauses.push("mode IN ('facet', 'lookup')".to_string());
    }
    if !request.search.is_empty() {
        parameters.insert(
            "group_value".to_string(),
            Value::from(request.search.clone()),
        );
        clauses.push("value = {group_value:String}".to_string());
    }
    let count_expression = if table == "field_index" {
        "uniqExact(event_id)"
    } else {
        "count()"
    };
    (
        [
            "SELECT value".to_string(),
            ", min(timestamp) AS startedAt, max(timestamp) AS endedAt".to_string(),
            ", dateDiff('millisecond', min(timestamp), max(timestamp)) AS durationMs".to_string(),
            format!(", {count_expression} AS count"),
            ", sum(toUInt64(is_error)) AS errorCount".to_string(),
            format!("FROM {table}"),
            where_keyword(join_clauses(clauses)),
            "GROUP BY value".to_string(),
            group_order_by_clause(group_by, true, request.sort.group),
            "LIMIT {limit:UInt64} OFFSET {offset:UInt64}".to_string(),
        ]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" "),
        parameters,
    )
}

fn latest_grouped_rollup_query(
    request: &EventsQueryRequest,
    group_by: &str,
) -> (String, Map<String, Value>) {
    let mut parameters = time_parameters(request.time_range.as_ref());
    parameters.insert("group_key".to_string(), Value::from(group_by.to_string()));
    parameters.insert(
        "group_value".to_string(),
        Value::from(request.selected_group_value.clone()),
    );
    let clauses = vec![
        "field_name = {group_key:String}".to_string(),
        "value = {group_value:String}".to_string(),
        "bucket_seconds = 60".to_string(),
        time_range_clause(request.time_range.as_ref(), "bucket_time"),
    ];
    (
        [
            "SELECT max(bucket_time) AS lastCreatedAt".to_string(),
            "FROM field_rollups".to_string(),
            where_keyword(join_clauses(clauses)),
        ]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" "),
        parameters,
    )
}

fn latest_grouped_index_query(
    request: &EventsQueryRequest,
    group_by: &str,
    table: &str,
    include_mode: bool,
) -> (String, Map<String, Value>) {
    let mut parameters = time_parameters(request.time_range.as_ref());
    parameters.insert("group_key".to_string(), Value::from(group_by.to_string()));
    parameters.insert(
        "group_value".to_string(),
        Value::from(request.selected_group_value.clone()),
    );
    let mut clauses = vec![
        "field_name = {group_key:String}".to_string(),
        "value = {group_value:String}".to_string(),
        time_range_clause(request.time_range.as_ref(), "timestamp"),
    ];
    if include_mode {
        clauses.push("mode IN ('facet', 'lookup')".to_string());
    }
    (
        [
            "SELECT max(timestamp) AS lastCreatedAt".to_string(),
            format!("FROM {table}"),
            where_keyword(join_clauses(clauses)),
        ]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" "),
        parameters,
    )
}

fn raw_groups_query(
    request: &EventsQueryRequest,
    group_by: &str,
) -> Result<(String, Map<String, Value>), ReadError> {
    let mut parameters = time_parameters(request.time_range.as_ref());
    parameters.insert("limit".to_string(), Value::from(request.limit + 1));
    parameters.insert("offset".to_string(), Value::from(request.offset));
    let value_expression = event_value_expression(group_by, "e")?;
    let mut clauses = vec![
        format!("{value_expression} != ''"),
        time_range_clause(request.time_range.as_ref(), "e.timestamp"),
    ];
    if !request.search.is_empty() {
        parameters.insert(
            "group_value".to_string(),
            Value::from(request.search.clone()),
        );
        clauses.push(format!("{value_expression} = {{group_value:String}}"));
    }
    Ok((
        [
            format!("SELECT {value_expression} AS value"),
            ", min(e.timestamp) AS startedAt, max(e.timestamp) AS endedAt".to_string(),
            ", dateDiff('millisecond', min(e.timestamp), max(e.timestamp)) AS durationMs"
                .to_string(),
            ", count() AS count".to_string(),
            format!(", countIf({}) AS errorCount", error_expression("e")),
            "FROM events AS e".to_string(),
            where_keyword(join_clauses(clauses)),
            "GROUP BY value".to_string(),
            group_order_by_clause(group_by, true, request.sort.group),
            "LIMIT {limit:UInt64} OFFSET {offset:UInt64}".to_string(),
        ]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" "),
        parameters,
    ))
}

fn event_page_filter(
    request: &EventsQueryRequest,
    alias: &str,
    parameters: &mut Map<String, Value>,
) -> String {
    let cursor = if !request.page.before.is_empty() {
        Some(("<", request.page.before.as_str()))
    } else if !request.page.after.is_empty() {
        Some((">", request.page.after.as_str()))
    } else if !request.page.around.is_empty() {
        Some((
            match request.sort.direction {
                EventSortDirection::Asc => ">=",
                EventSortDirection::Desc => "<=",
            },
            request.page.around.as_str(),
        ))
    } else {
        None
    };
    let Some((operator, value)) = cursor else {
        return String::new();
    };
    parameters.insert(
        "cursor".to_string(),
        Value::from(clickhouse_datetime64(value)),
    );
    let cursor_event_id = request.page.event_id.trim();
    if cursor_event_id.is_empty() {
        return format!("{alias}.timestamp {operator} {{cursor:DateTime64(3, 'UTC')}}");
    }
    parameters.insert(
        "cursor_event_id".to_string(),
        Value::from(cursor_event_id.to_string()),
    );
    format!(
        "({alias}.timestamp, {alias}.event_id) {operator} ({{cursor:DateTime64(3, 'UTC')}}, {{cursor_event_id:String}})"
    )
}

fn event_query_order(request: &EventsQueryRequest) -> &'static str {
    match request.sort.direction {
        EventSortDirection::Desc if !request.page.after.is_empty() => "ASC",
        EventSortDirection::Desc => "DESC",
        EventSortDirection::Asc if !request.page.before.is_empty() => "DESC",
        EventSortDirection::Asc => "ASC",
    }
}

fn event_table_order(request: &EventsQueryRequest) -> &'static str {
    match request.sort.direction {
        EventSortDirection::Asc => "ASC",
        EventSortDirection::Desc => "DESC",
    }
}

fn time_where_clause(
    filter: &EventFilter,
    time_range: Option<&EventTimeRange>,
    column: &str,
) -> (String, Map<String, Value>) {
    let mut clauses = Vec::new();
    let mut parameters = Map::new();
    if !filter.created_after.is_empty() {
        parameters.insert(
            "created_after".to_string(),
            Value::from(clickhouse_datetime64(&filter.created_after)),
        );
        clauses.push(format!(
            "{column} >= {{created_after:DateTime64(3, 'UTC')}}"
        ));
    }
    if !filter.created_before.is_empty() {
        parameters.insert(
            "created_before".to_string(),
            Value::from(clickhouse_datetime64(&filter.created_before)),
        );
        clauses.push(format!(
            "{column} <= {{created_before:DateTime64(3, 'UTC')}}"
        ));
    }
    if filter.created_after.is_empty() && filter.created_before.is_empty() {
        let range_clause = time_range_clause(time_range, column);
        if !range_clause.is_empty() {
            clauses.push(range_clause);
            parameters.extend(time_parameters(time_range));
        }
    }
    (join_clauses(clauses), parameters)
}

fn time_range_clause(time_range: Option<&EventTimeRange>, column: &str) -> String {
    let Some(time_range) = time_range else {
        return String::new();
    };
    if time_range.lookback_minutes > 0 {
        return format!("{column} >= now64(3) - toIntervalMinute({{lookback_minutes:UInt64}})");
    }
    join_clauses(vec![
        if time_range.created_after.is_empty() {
            String::new()
        } else {
            format!("{column} >= {{created_after:DateTime64(3, 'UTC')}}")
        },
        if time_range.created_before.is_empty() {
            String::new()
        } else {
            format!("{column} <= {{created_before:DateTime64(3, 'UTC')}}")
        },
    ])
}

fn time_parameters(time_range: Option<&EventTimeRange>) -> Map<String, Value> {
    let mut parameters = Map::new();
    let Some(time_range) = time_range else {
        return parameters;
    };
    if time_range.lookback_minutes > 0 {
        parameters.insert(
            "lookback_minutes".to_string(),
            Value::from(time_range.lookback_minutes),
        );
    }
    if !time_range.created_after.is_empty() {
        parameters.insert(
            "created_after".to_string(),
            Value::from(clickhouse_datetime64(&time_range.created_after)),
        );
    }
    if !time_range.created_before.is_empty() {
        parameters.insert(
            "created_before".to_string(),
            Value::from(clickhouse_datetime64(&time_range.created_before)),
        );
    }
    parameters
}

fn where_keyword(clause: String) -> String {
    if clause.is_empty() {
        String::new()
    } else {
        format!("WHERE {clause}")
    }
}

fn join_clauses(clauses: Vec<String>) -> String {
    clauses
        .into_iter()
        .filter(|clause| !clause.is_empty())
        .map(|clause| format!("({clause})"))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn event_metadata_select(alias: &str) -> String {
    flamegraph_select(alias)
}

fn flamegraph_select(alias: &str) -> String {
    format!(
        "SELECT {alias}.event_id AS event_id, {alias}.timestamp AS timestamp, {alias}.event_type AS event_type, {alias}.signal AS signal, {alias}.trace_id AS trace_id, {alias}.span_id AS span_id, ifNull(toString({alias}.data.parent_span_id), '') AS parent_span_id, ifNull(toString({alias}.data.name), '') AS name, ifNull(toString({alias}.data.start_time), '') AS start_time, ifNull(toString({alias}.data.end_time), '') AS end_time, toFloat64OrZero(toString({alias}.data.duration_ms)) AS duration_ms"
    )
}

fn event_value_expression(path: &str, alias: &str) -> Result<String, ReadError> {
    let column = promoted_string_column(path, alias);
    if !column.is_empty() {
        return Ok(format!("ifNull(toString(nullIf({column}, '')), '')"));
    }
    if !is_supported_path(path) {
        return Err(ReadError::InvalidQuery(format!(
            "unsupported field path: {path}"
        )));
    }
    Ok(format!(
        "ifNull(toString({}), '')",
        event_payload_expression(path, alias)
    ))
}

fn promoted_string_column(path: &str, alias: &str) -> String {
    let prefix = if alias.is_empty() {
        String::new()
    } else {
        format!("{alias}.")
    };
    match path {
        "tenant_id" => format!("{prefix}tenant_id"),
        "trace_id" => format!("{prefix}trace_id"),
        "span_id" => format!("{prefix}span_id"),
        "event_type" => format!("{prefix}event_type"),
        "signal" => format!("{prefix}signal"),
        _ => String::new(),
    }
}

fn error_expression(alias: &str) -> String {
    [
        format!("lowerUTF8(ifNull(toString({alias}.data.is_error), '')) IN ('1', 'true')"),
        format!("lowerUTF8(ifNull(toString({alias}.data.span_status_code), '')) = 'error'"),
        format!("endsWith(lowerUTF8(ifNull(toString({alias}.data.event_type), '')), '_error')"),
    ]
    .join(" OR ")
}

fn facet_key(path: &str) -> Result<String, ReadError> {
    let path = match path {
        "traceId" => "trace_id".to_string(),
        "spanId" => "span_id".to_string(),
        "parentSpanId" => "parent_span_id".to_string(),
        "startedAt" => "start_time".to_string(),
        "endedAt" => "end_time".to_string(),
        "durationMs" => "duration_ms".to_string(),
        other => normalized_payload_path(other),
    };
    if is_supported_path(&path) {
        Ok(path)
    } else {
        Err(ReadError::InvalidQuery(format!(
            "unsupported field path: {path}"
        )))
    }
}

fn normalized_payload_path(path: &str) -> String {
    path.trim().replace('-', ".")
}

fn is_supported_path(path: &str) -> bool {
    let mut segments = path.split('.');
    segments.next().is_some_and(is_supported_path_segment)
        && segments.all(is_supported_path_segment)
}

fn is_supported_path_segment(segment: &str) -> bool {
    let mut chars = segment.chars();
    chars
        .next()
        .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn event_payload_expression(path: &str, alias: &str) -> String {
    let data_column = if alias.is_empty() {
        "data".to_string()
    } else {
        format!("{alias}.data")
    };
    if path.contains('.') {
        format!(
            "getSubcolumn({data_column}, '{}')",
            path.replace('\'', "''")
        )
    } else {
        format!("{data_column}.{path}")
    }
}

fn group_order_by_clause(group_by: &str, has_error_count: bool, sort_key: GroupSortKey) -> String {
    let error_tie = if has_error_count {
        ", errorCount DESC"
    } else {
        ""
    };
    match sort_key {
        GroupSortKey::Count => format!("ORDER BY count DESC{error_tie}, value ASC"),
        GroupSortKey::Duration => {
            format!("ORDER BY durationMs DESC, count DESC{error_tie}, value ASC")
        }
        GroupSortKey::Value => "ORDER BY value ASC".to_string(),
        GroupSortKey::Recent => {
            if is_trace_like_group(group_by) {
                format!("ORDER BY endedAt DESC, count DESC{error_tie}, value ASC")
            } else {
                format!("ORDER BY count DESC{error_tie}, value ASC")
            }
        }
    }
}

fn is_trace_like_group(path: &str) -> bool {
    matches!(
        path,
        "trace_id" | "span_id" | "request_id" | "session_id" | "thread_id" | "conversation_id"
    )
}

fn response_data_len(response: &Value) -> usize {
    response
        .get("data")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default()
}

fn density_bucket_ms(response: &Value, buckets: u64, floor_ms: u64) -> u64 {
    let Some(row) = response
        .get("data")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
    else {
        return floor_ms.max(1);
    };
    let count = row.get("count").and_then(value_as_u64).unwrap_or_default();
    if count == 0 {
        return floor_ms.max(1);
    }
    let from = row.get("from").and_then(Value::as_str).unwrap_or_default();
    let to = row.get("to").and_then(Value::as_str).unwrap_or_default();
    let Some(from_ms) = parse_clickhouse_time_ms(from) else {
        return floor_ms.max(1);
    };
    let Some(to_ms) = parse_clickhouse_time_ms(to) else {
        return floor_ms.max(1);
    };
    let span = to_ms.saturating_sub(from_ms).max(1);
    nice_time_interval((span / buckets.max(1)).max(floor_ms)).max(floor_ms)
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn parse_clickhouse_time_ms(value: &str) -> Option<u64> {
    let normalized = if value.contains('T') {
        value.to_string()
    } else {
        format!("{}Z", value.replace(' ', "T"))
    };
    chrono::DateTime::parse_from_rfc3339(&normalized)
        .ok()
        .and_then(|timestamp| timestamp.timestamp_millis().try_into().ok())
}

fn nice_time_interval(target_ms: u64) -> u64 {
    const INTERVALS: &[u64] = &[
        1,
        2,
        5,
        10,
        20,
        50,
        100,
        200,
        500,
        1_000,
        2_000,
        5_000,
        10_000,
        15_000,
        30_000,
        60_000,
        120_000,
        300_000,
        600_000,
        900_000,
        1_800_000,
        3_600_000,
        7_200_000,
        21_600_000,
        43_200_000,
        86_400_000,
        604_800_000,
    ];
    INTERVALS
        .iter()
        .copied()
        .find(|interval| *interval >= target_ms)
        .unwrap_or(*INTERVALS.last().unwrap_or(&604_800_000))
}

fn clickhouse_datetime64(value: &str) -> String {
    let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(value) else {
        return value.to_string();
    };
    timestamp
        .with_timezone(&chrono::Utc)
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
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
        CORE_ROLLUP_FIELD_NAMES, EventFacetFilter, EventFacetOperator, EventFieldCatalog,
        EventFilter, EventPage, EventPredicatePlan, EventSortDirection, EventTimeRange,
        EventsQueryRequest, EventsQuerySort, RAW_GROUPABLE_FIELD_NAMES, ReadError,
        checked_select_query, event_page_filter, event_table_order, event_value_expression,
        group_options_query, grouped_index_query, grouped_rollup_query, latest_grouped_index_query,
        latest_grouped_rollup_query, normalize_prewhere, query_sources, query_usage_shape,
        raw_groups_query, validate_parameter_name, validate_query_sources,
    };
    use std::collections::BTreeSet;

    #[test]
    fn events_query_request_accepts_order_by_alias() {
        let request: EventsQueryRequest = serde_json::from_value(serde_json::json!({
            "view": "events",
            "filter": { "text": "timeout" },
            "groupBy": "trace_id",
            "orderBy": { "direction": "asc" }
        }))
        .unwrap();

        assert_eq!(request.sort.direction, EventSortDirection::Asc);
        assert_eq!(request.group_by, "trace_id");
        assert_eq!(request.filter.text, "timeout");
    }

    #[test]
    fn events_query_request_defaults_to_ascending_timestamp() {
        let request: EventsQueryRequest = serde_json::from_value(serde_json::json!({
            "view": "events"
        }))
        .unwrap();

        assert_eq!(request.sort.direction, EventSortDirection::Asc);
    }

    #[test]
    fn event_page_filter_uses_tuple_cursor_for_selected_event() {
        let request = EventsQueryRequest {
            page: EventPage {
                around: "2026-05-22T00:00:00.000Z".to_string(),
                event_id: "evt_123".to_string(),
                ..Default::default()
            },
            sort: EventsQuerySort {
                direction: EventSortDirection::Asc,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut parameters = serde_json::Map::new();
        let filter = event_page_filter(&request, "e", &mut parameters);

        assert!(filter.contains("(e.timestamp, e.event_id) >= "));
        assert_eq!(event_table_order(&request), "ASC");
        assert_eq!(
            parameters
                .get("cursor_event_id")
                .and_then(|value| value.as_str()),
            Some("evt_123")
        );
    }

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

    #[test]
    fn event_predicate_plan_uses_indexed_candidates_and_raw_residuals() {
        let request = EventsQueryRequest {
            filter: EventFilter {
                facets: vec![
                    EventFacetFilter {
                        path: "service".to_string(),
                        value: "api".to_string(),
                        ..Default::default()
                    },
                    EventFacetFilter {
                        operator: EventFacetOperator::Contains,
                        path: "message".to_string(),
                        value: "timeout".to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            ..Default::default()
        };
        let catalog = EventFieldCatalog {
            promoted: BTreeSet::from(["service".to_string()]),
        };
        let plan = EventPredicatePlan::new(&request, &catalog, "e").unwrap();
        let sql = plan.where_clause();
        assert!(sql.contains("e.event_id IN (SELECT idx.event_id FROM field_index AS idx"));
        assert!(sql.contains("positionCaseInsensitive"));
        assert!(sql.contains("message"));
    }

    #[test]
    fn group2_case_7_promoted_facet_grouping_reads_field_index() {
        let request = EventsQueryRequest {
            limit: 50,
            time_range: Some(EventTimeRange {
                created_after: "2026-05-01T00:00:00Z".to_string(),
                created_before: "2026-05-02T00:00:00Z".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let (query, parameters) = grouped_index_query(&request, "browser", "field_index", true);

        assert!(query.contains("FROM field_index"));
        assert!(query.contains("field_name = {group_key:String}"));
        assert!(query.contains("mode IN ('facet', 'lookup')"));
        assert!(query.contains("uniqExact(event_id) AS count"));
        assert!(!query.contains("FROM events"));
        assert_eq!(
            parameters.get("group_key").and_then(|value| value.as_str()),
            Some("browser")
        );
    }

    #[test]
    fn group2_case_7_core_grouping_prefers_field_rollup() {
        let request = EventsQueryRequest {
            limit: 50,
            time_range: Some(EventTimeRange {
                lookback_minutes: 60,
                ..Default::default()
            }),
            ..Default::default()
        };

        let (query, parameters) = grouped_rollup_query(&request, "service");

        assert!(query.contains("FROM field_rollups"));
        assert!(query.contains("bucket_seconds = 60"));
        assert!(query.contains("sum(count) AS count"));
        assert!(!query.contains("FROM events"));
        assert_eq!(
            parameters.get("group_key").and_then(|value| value.as_str()),
            Some("service")
        );
    }

    #[test]
    fn raw_groupable_non_core_field_groups_from_events() {
        let request = EventsQueryRequest {
            limit: 50,
            time_range: Some(EventTimeRange {
                lookback_minutes: 60,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(RAW_GROUPABLE_FIELD_NAMES.contains(&"http.route"));
        assert!(!CORE_ROLLUP_FIELD_NAMES.contains(&"http.route"));
        let (query, parameters) = raw_groups_query(&request, "http.route").unwrap();

        assert!(query.contains("FROM events AS e"));
        assert!(query.contains("getSubcolumn(e.data, 'http.route')"));
        assert_eq!(
            parameters.get("limit").and_then(|value| value.as_u64()),
            Some(51)
        );
    }

    #[test]
    fn grouped_latest_uses_rollup_or_index_instead_of_raw_events() {
        let request = EventsQueryRequest {
            selected_group_value: "llm-gateway".to_string(),
            time_range: Some(EventTimeRange {
                created_after: "2026-05-17T09:30:00Z".to_string(),
                created_before: "2026-05-17T09:35:00Z".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let (rollup_query, rollup_parameters) = latest_grouped_rollup_query(&request, "service");
        assert!(rollup_query.contains("FROM field_rollups"));
        assert!(rollup_query.contains("bucket_seconds = 60"));
        assert!(rollup_query.contains("max(bucket_time) AS lastCreatedAt"));
        assert!(!rollup_query.contains("FROM events"));
        assert_eq!(
            rollup_parameters
                .get("group_value")
                .and_then(|value| value.as_str()),
            Some("llm-gateway")
        );

        let (index_query, index_parameters) =
            latest_grouped_index_query(&request, "browser", "field_index", true);
        assert!(index_query.contains("FROM field_index"));
        assert!(index_query.contains("max(timestamp) AS lastCreatedAt"));
        assert!(index_query.contains("mode IN ('facet', 'lookup')"));
        assert!(!index_query.contains("FROM events"));
        assert_eq!(
            index_parameters
                .get("group_value")
                .and_then(|value| value.as_str()),
            Some("llm-gateway")
        );
    }

    #[test]
    fn group2_case_8_multi_field_filters_intersect_promoted_indexes() {
        let request = EventsQueryRequest {
            filter: EventFilter {
                facets: vec![
                    EventFacetFilter {
                        path: "plan".to_string(),
                        value: "pro".to_string(),
                        ..Default::default()
                    },
                    EventFacetFilter {
                        path: "country".to_string(),
                        value: "US".to_string(),
                        ..Default::default()
                    },
                    EventFacetFilter {
                        path: "account_tier".to_string(),
                        value: "enterprise".to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            time_range: Some(EventTimeRange {
                lookback_minutes: 60,
                ..Default::default()
            }),
            ..Default::default()
        };
        let catalog = EventFieldCatalog {
            promoted: BTreeSet::from([
                "plan".to_string(),
                "country".to_string(),
                "account_tier".to_string(),
            ]),
        };

        let plan = EventPredicatePlan::new(&request, &catalog, "e").unwrap();
        let sql = plan.where_clause();

        assert_eq!(sql.matches("field_index AS idx").count(), 3);
        assert_eq!(
            sql.matches("idx.timestamp >= now64(3) - toIntervalMinute")
                .count(),
            3
        );
        assert!(sql.contains("idx.mode IN ('facet', 'lookup')"));
        assert!(!sql.contains("e.data.plan"));
        assert!(!sql.contains("e.data.country"));
        assert!(!sql.contains("e.data.account_tier"));
    }

    #[test]
    fn group2_case_8_indexed_in_filter_uses_single_index_membership_clause() {
        let request = EventsQueryRequest {
            filter: EventFilter {
                facets: vec![EventFacetFilter {
                    operator: EventFacetOperator::In,
                    path: "llm.model".to_string(),
                    values: vec!["gpt-4.1".to_string(), "gpt-4.1-mini".to_string()],
                    ..Default::default()
                }],
                ..Default::default()
            },
            time_range: Some(EventTimeRange {
                lookback_minutes: 15,
                ..Default::default()
            }),
            ..Default::default()
        };
        let catalog = EventFieldCatalog {
            promoted: BTreeSet::from(["llm.model".to_string()]),
        };

        let plan = EventPredicatePlan::new(&request, &catalog, "e").unwrap();
        let sql = plan.where_clause();

        assert_eq!(sql.matches("field_index AS idx").count(), 1);
        assert!(sql.contains("idx.value IN ({facet_value_0:String}, {facet_value_1:String})"));
        assert!(!sql.contains("e.data.llm.model"));
    }

    #[test]
    fn nested_payload_paths_use_clickhouse_subcolumn_lookup() {
        assert_eq!(
            event_value_expression("llm.model", "e").unwrap(),
            "ifNull(toString(getSubcolumn(e.data, 'llm.model')), '')"
        );
        assert_eq!(
            event_value_expression("service", "e").unwrap(),
            "ifNull(toString(e.data.service), '')"
        );
        assert_eq!(
            event_value_expression("llm.provider", "").unwrap(),
            "ifNull(toString(getSubcolumn(data, 'llm.provider')), '')"
        );
    }

    #[test]
    fn group2_cases_9_through_20_use_materialized_serving_tables() {
        let cases = [
            (
                9,
                "dimensioned alert",
                "SELECT bucket_time, toFloat64(metrics.error_rate) AS error_rate FROM observatory.report_results WHERE report_id = 'checkout_error_rate_by_plan'",
                vec!["observatory.report_results"],
            ),
            (
                10,
                "promoted arbitrary predicate alert",
                "SELECT bucket_time, toUInt64(metrics.matching_events) AS matching_events FROM observatory.report_results WHERE report_id = 'foo_bar_region_us_east_alert'",
                vec!["observatory.report_results"],
            ),
            (
                11,
                "numeric percentile rollup",
                "SELECT bucket_time, dimension_value AS service, quantilesTDigestMerge(0.5, 0.9, 0.95, 0.99)(quantiles_state)[3] AS p95_ms FROM observatory.measure_rollups WHERE measure_name = 'duration_ms' GROUP BY bucket_time, service",
                vec!["observatory.measure_rollups"],
            ),
            (
                12,
                "revenue rollup",
                "SELECT bucket_time, dimension_value AS product_id, sumMerge(sum_state) AS revenue FROM observatory.measure_rollups WHERE measure_name = 'revenue' GROUP BY bucket_time, product_id",
                vec!["observatory.measure_rollups"],
            ),
            (
                13,
                "active users report",
                "SELECT bucket_time, toUInt64(metrics.active_users) AS active_users FROM observatory.report_results WHERE report_id = 'daily_active_users_by_plan'",
                vec!["observatory.report_results"],
            ),
            (
                14,
                "top entities report",
                "SELECT bucket_time, dimensions.account_id AS account_id, toUInt64(metrics.events) AS events FROM observatory.report_results WHERE report_id = 'top_accounts_by_revenue'",
                vec!["observatory.report_results"],
            ),
            (
                15,
                "sequence funnel report",
                "SELECT bucket_time, step_index, step_name, entity_count, conversion_count FROM observatory.sequence_report_results WHERE report_id = 'signup_invite_checkout_7d'",
                vec!["observatory.sequence_report_results"],
            ),
            (
                16,
                "retention report",
                "SELECT bucket_time, dimensions.retention_week AS retention_week, toUInt64(metrics.retained_users) AS retained_users FROM observatory.report_results WHERE report_id = 'june_signup_weekly_retention'",
                vec!["observatory.report_results"],
            ),
            (
                16,
                "cohort membership drilldown",
                "SELECT entity_id, first_seen, last_seen FROM observatory.cohort_memberships WHERE cohort_id = 'june_signups'",
                vec!["observatory.cohort_memberships"],
            ),
            (
                17,
                "entity state at time",
                "SELECT entity_id, argMax(value, timestamp) AS plan_at_time FROM observatory.entity_state_updates WHERE entity_type = 'account' AND state_name = 'account.plan' GROUP BY entity_id",
                vec!["observatory.entity_state_updates"],
            ),
            (
                18,
                "experiment report",
                "SELECT dimensions.variant AS variant, toFloat64(metrics.conversion_rate) AS conversion_rate FROM observatory.report_results WHERE report_id = 'checkout_flow_experiment'",
                vec!["observatory.report_results"],
            ),
            (
                19,
                "trace summary report",
                "SELECT bucket_time, dimensions.trace_id AS trace_id, toFloat64(metrics.duration_ms) AS duration_ms FROM observatory.report_results WHERE report_id = 'top_slow_traces'",
                vec!["observatory.report_results"],
            ),
            (
                20,
                "reusable dashboard report",
                "SELECT bucket_time, dimensions.plan AS plan, toUInt64(metrics.events) AS events FROM observatory.report_results WHERE report_id = 'api_health_by_plan_country'",
                vec!["observatory.report_results"],
            ),
        ];
        let allowed = group2_allowed_sources();

        for (number, name, query, expected_sources) in cases {
            assert!(
                validate_query_sources(query, &allowed).is_ok(),
                "case {number} {name} should be allowed"
            );
            assert_eq!(
                query_sources(query),
                expected_sources,
                "case {number} {name}"
            );
            assert!(
                !query_sources(query)
                    .iter()
                    .any(|source| source.eq_ignore_ascii_case("observatory.events")),
                "case {number} {name} should not use raw events"
            );
        }
    }

    #[test]
    fn group2_promoted_measure_and_state_read_sources_are_allowed() {
        let cases = [
            (
                "bounded event measure drilldown",
                "SELECT timestamp, event_id, value, dimension_value FROM observatory.event_measures WHERE measure_name = 'duration_ms' AND dimension_name = 'service'",
                vec!["observatory.event_measures"],
            ),
            (
                "entity state read",
                "SELECT entity_id, argMax(value, timestamp) AS value FROM observatory.entity_state_updates WHERE entity_type = 'account' AND state_name = 'account.plan' GROUP BY entity_id",
                vec!["observatory.entity_state_updates"],
            ),
        ];
        let allowed = group2_allowed_sources();

        for (name, query, expected_sources) in cases {
            assert!(
                validate_query_sources(query, &allowed).is_ok(),
                "{name} should be allowed"
            );
            assert_eq!(query_sources(query), expected_sources, "{name}");
        }
    }

    #[test]
    fn group_options_query_does_not_require_field_values_value_type() {
        let query = group_options_query();
        assert!(query.contains("arrayJoin"));
        assert!(query.contains("FROM definitions"));
        assert!(query.contains("JSONExtractString(output, 'field_name')"));
        assert!(query.contains("argMin(source, priority) AS source"));
        assert!(query.contains("argMin(servingMode, priority) AS servingMode"));
        assert!(query.contains("'builtin' AS source"));
        assert!(query.contains("'rollup' AS servingMode"));
        assert!(query.contains("'index' AS servingMode"));
        assert!(query.contains("'raw' AS source"));
        assert!(!query.contains("FROM field_values"));
        assert!(!query.contains("any(value_type) AS valueType FROM field_values"));
    }

    fn group2_allowed_sources() -> Vec<String> {
        [
            "observatory.events",
            "observatory.field_index",
            "observatory.event_measures",
            "observatory.measure_rollups",
            "observatory.counter_rollups",
            "observatory.gauge_rollups",
            "observatory.histogram_rollups",
            "observatory.entity_state_updates",
            "observatory.report_results",
            "observatory.sequence_report_results",
            "observatory.cohort_memberships",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    }
}
