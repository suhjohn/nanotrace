use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use chrono::{Duration as ChronoDuration, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlparser::{
    ast::{ObjectName, TableFactor, Visit, Visitor},
    dialect::ClickHouseDialect,
    parser::Parser,
};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct Config {
    pub clickhouse_url: Option<String>,
    pub clickhouse_user: Option<String>,
    pub clickhouse_password: Option<String>,
    pub clickhouse_database: String,
    pub clickhouse_table: String,
    pub clickhouse_max_result_rows: u64,
    pub clickhouse_max_execution_secs: u64,
    pub clickhouse_max_bytes_to_read: u64,
}

#[derive(Clone)]
pub struct ReadStore {
    cfg: Arc<Config>,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize, Serialize, utoipa::ToSchema)]
pub struct QueryRequest {
    pub query: String,
    #[serde(default)]
    pub parameters: Map<String, Value>,
    #[serde(default)]
    pub allow_stale_serving: bool,
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct QueryRecommendationRecord {
    pub query_id: String,
    pub query_hash: u64,
    pub query_shape: String,
    pub surface: String,
    pub plan_kind: String,
    pub shape_class: String,
    pub source_tables: Vec<String>,
    pub filter_paths: Vec<String>,
    pub group_by_paths: Vec<String>,
    pub time_range_start: Option<String>,
    pub time_range_end: Option<String>,
    pub result_rows: u64,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub elapsed_ms: u64,
    pub recommendations: Vec<Value>,
    pub observed_at: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct QueryRecommendationListResponse {
    pub recommendations: Vec<QueryRecommendationRecord>,
}

#[derive(Debug, Clone, Copy)]
struct QueryContext {
    surface: &'static str,
}

impl QueryContext {
    const RAW_SQL: Self = Self { surface: "sql" };
    const ADMIN_SQL: Self = Self {
        surface: "admin_sql",
    };
    const EVENTS: Self = Self { surface: "events" };
    const MEASURE: Self = Self { surface: "measure" };
    const FUNNEL: Self = Self { surface: "funnel" };
    const COHORT: Self = Self { surface: "cohort" };
    const REPORT: Self = Self { surface: "report" };
    const STATE: Self = Self { surface: "state" };
    const SEARCH: Self = Self { surface: "search" };
    const ALERTS: Self = Self { surface: "alerts" };
}

#[derive(Debug, Clone, Copy)]
enum QueryTenantScope {
    Tenant,
    Global,
}

impl QueryTenantScope {
    fn name(self) -> &'static str {
        match self {
            Self::Tenant => "tenant",
            Self::Global => "global",
        }
    }
}

#[derive(Debug, Serialize)]
pub enum QueryApiRequest {
    Events(EventsQueryRequest),
    Search(SearchQueryRequest),
    Measure(MeasureQueryRequest),
    Funnel(FunnelQueryRequest),
    Cohort(CohortQueryRequest),
    Report(ReportQueryRequest),
    State(StateQueryRequest),
    Alerts(AlertQueryRequest),
}

impl<'de> Deserialize<'de> for QueryApiRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let mut object = value.as_object().cloned().ok_or_else(|| {
            serde::de::Error::custom("query request must be a JSON object with a type field")
        })?;
        let query_type = object
            .remove("type")
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .ok_or_else(|| serde::de::Error::custom("query request missing type field"))?;
        let payload = Value::Object(object);
        match query_type.as_str() {
            "events" => serde_json::from_value(payload)
                .map(QueryApiRequest::Events)
                .map_err(serde::de::Error::custom),
            "search" => serde_json::from_value(payload)
                .map(QueryApiRequest::Search)
                .map_err(serde::de::Error::custom),
            "measure" => serde_json::from_value(payload)
                .map(QueryApiRequest::Measure)
                .map_err(serde::de::Error::custom),
            "funnel" => serde_json::from_value(payload)
                .map(QueryApiRequest::Funnel)
                .map_err(serde::de::Error::custom),
            "cohort" => serde_json::from_value(payload)
                .map(QueryApiRequest::Cohort)
                .map_err(serde::de::Error::custom),
            "report" => serde_json::from_value(payload)
                .map(QueryApiRequest::Report)
                .map_err(serde::de::Error::custom),
            "state" => serde_json::from_value(payload)
                .map(QueryApiRequest::State)
                .map_err(serde::de::Error::custom),
            "alerts" => serde_json::from_value(payload)
                .map(QueryApiRequest::Alerts)
                .map_err(serde::de::Error::custom),
            other => Err(serde::de::Error::custom(format!(
                "unsupported query type: {other}"
            ))),
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SearchQueryRequest {
    pub query: String,
    #[serde(default)]
    pub mode: SearchMode,
    #[serde(default)]
    pub require_all_terms: bool,
    #[serde(default)]
    pub include_snippets: bool,
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub to: String,
    #[serde(default = "default_search_query_limit")]
    pub limit: u64,
    #[serde(default)]
    pub offset: u64,
    #[serde(default)]
    pub event_type: String,
    #[serde(default)]
    pub allow_stale_serving: bool,
}

#[derive(Debug, Default, Clone, Copy, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum SearchMode {
    #[default]
    Token,
    Prefix,
    Fuzzy,
    Phrase,
}

#[allow(dead_code)]
#[derive(Debug, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MeasureQueryRequest {
    pub measure_name: String,
    pub from: String,
    pub to: String,
    #[serde(default = "default_measure_bucket_seconds")]
    pub bucket_seconds: u32,
    #[serde(default)]
    pub group_by: Vec<String>,
    #[serde(default)]
    pub filters: Map<String, Value>,
    #[serde(default)]
    pub definition_id: String,
    #[serde(default)]
    pub allow_stale_serving: bool,
}

#[allow(dead_code)]
#[derive(Debug, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FunnelQueryRequest {
    pub report_id: String,
    #[serde(default)]
    pub report_version: u64,
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub to: String,
    #[serde(default)]
    pub filters: Map<String, Value>,
    #[serde(default)]
    pub allow_stale_serving: bool,
}

#[allow(dead_code)]
#[derive(Debug, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CohortQueryRequest {
    pub cohort_id: String,
    #[serde(default)]
    pub cohort_version: u64,
    #[serde(default)]
    pub entity_type: String,
    #[serde(default = "default_cohort_query_limit")]
    pub limit: u64,
    #[serde(default)]
    pub allow_stale_serving: bool,
}

#[allow(dead_code)]
#[derive(Debug, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReportQueryRequest {
    pub report_id: String,
    #[serde(default)]
    pub report_version: u64,
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub to: String,
    #[serde(default)]
    pub filters: Map<String, Value>,
    #[serde(default = "default_report_query_limit")]
    pub limit: u64,
    #[serde(default)]
    pub allow_stale_serving: bool,
}

#[allow(dead_code)]
#[derive(Debug, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct StateQueryRequest {
    pub entity_type: String,
    pub state_name: String,
    #[serde(default)]
    pub entity_id: String,
    #[serde(default)]
    pub as_of: String,
    #[serde(default = "default_state_query_limit")]
    pub limit: u64,
    #[serde(default)]
    pub allow_stale_serving: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AlertQueryMode {
    #[default]
    Events,
    Notifications,
}

#[allow(dead_code)]
#[derive(Debug, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AlertQueryRequest {
    #[serde(default)]
    pub mode: AlertQueryMode,
    #[serde(default)]
    pub alert_id: String,
    #[serde(default)]
    pub event_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub to: String,
    #[serde(default = "default_alert_query_limit")]
    pub limit: u64,
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
    pub scope: String,
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
    Exists,
    Gt,
    Gte,
    In,
    Lt,
    Lte,
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
    #[error("{0}")]
    InvalidQuery(String),
    #[error("ClickHouse request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("ClickHouse query failed: {status} {body}")]
    ClickHouseResponse { status: StatusCode, body: String },
    #[error("event not found")]
    NotFound,
    #[error("invalid stored event JSON: {0}")]
    InvalidStoredEvent(serde_json::Error),
    #[error("stored event_id does not match requested event_id")]
    EventIDMismatch,
}

impl ReadStore {
    pub fn new(cfg: Arc<Config>) -> Self {
        Self {
            cfg,
            http: reqwest::Client::new(),
        }
    }

    pub async fn query(&self, request: QueryRequest, tenant_id: &str) -> Result<Value, ReadError> {
        self.query_with_context(request, tenant_id, QueryContext::RAW_SQL)
            .await
    }

    pub async fn admin_query(
        &self,
        request: QueryRequest,
        actor_tenant_id: &str,
    ) -> Result<Value, ReadError> {
        let freshness_overrides = BTreeSet::new();
        self.query_with_scope(
            request,
            actor_tenant_id,
            &freshness_overrides,
            QueryContext::ADMIN_SQL,
            QueryTenantScope::Global,
            QueryPlanningMetadata::default(),
        )
        .await
    }

    async fn query_with_context(
        &self,
        request: QueryRequest,
        tenant_id: &str,
        context: QueryContext,
    ) -> Result<Value, ReadError> {
        let freshness_overrides = BTreeSet::new();
        self.query_with_freshness_overrides_and_metadata(
            request,
            tenant_id,
            &freshness_overrides,
            context,
            QueryPlanningMetadata::default(),
        )
        .await
    }

    async fn query_with_context_and_metadata(
        &self,
        request: QueryRequest,
        tenant_id: &str,
        context: QueryContext,
        metadata: QueryPlanningMetadata,
    ) -> Result<Value, ReadError> {
        let freshness_overrides = BTreeSet::new();
        self.query_with_freshness_overrides_and_metadata(
            request,
            tenant_id,
            &freshness_overrides,
            context,
            metadata,
        )
        .await
    }

    pub async fn api_query(
        &self,
        request: QueryApiRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        match request {
            QueryApiRequest::Events(request) => self.events_query(request, tenant_id).await,
            QueryApiRequest::Search(request) => self.search_query(request, tenant_id).await,
            QueryApiRequest::Measure(request) => self.measures_query(request, tenant_id).await,
            QueryApiRequest::Funnel(request) => self.funnels_query(request, tenant_id).await,
            QueryApiRequest::Cohort(request) => self.cohorts_query(request, tenant_id).await,
            QueryApiRequest::Report(request) => self.reports_query(request, tenant_id).await,
            QueryApiRequest::State(request) => self.states_query(request, tenant_id).await,
            QueryApiRequest::Alerts(request) => self.alerts_query(request, tenant_id).await,
        }
    }

    pub async fn recent_query_recommendations(
        &self,
        tenant_id: &str,
        limit: u64,
    ) -> Result<QueryRecommendationListResponse, ReadError> {
        let limit = limit.clamp(1, 100);
        let query = format!(
            "SELECT query_id, query_hash, query_shape, surface, plan_kind, ifNull(toString(attributes.query_shape_class), '') AS shape_class, source_tables, filter_paths, group_by_paths, toString(time_range_start) AS time_range_start, toString(time_range_end) AS time_range_end, result_rows, read_rows, read_bytes, elapsed_ms, ifNull(toJSONString(attributes.recommendations), '[]') AS recommendations_json, toString(observed_at) AS observed_at FROM {}.query_usage WHERE tenant_id = {{tenant_id:String}} AND status = 'ok' AND length(JSONExtractArrayRaw(ifNull(toJSONString(attributes.recommendations), '[]'))) > 0 ORDER BY observed_at DESC LIMIT {{limit:UInt64}}",
            self.cfg.clickhouse_database
        );
        let mut parameters = Map::new();
        parameters.insert(
            "tenant_id".to_string(),
            Value::String(tenant_id.to_string()),
        );
        parameters.insert("limit".to_string(), Value::Number(limit.into()));
        let text = self.clickhouse_query(&query, &parameters).await?;
        let response: ClickHouseResponse<QueryRecommendationRow> =
            serde_json::from_str(&text).map_err(ReadError::InvalidStoredEvent)?;
        Ok(QueryRecommendationListResponse {
            recommendations: response
                .data
                .into_iter()
                .filter_map(QueryRecommendationRecord::from_row)
                .collect(),
        })
    }

    async fn search_query(
        &self,
        mut request: SearchQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        request.limit = request.limit.clamp(1, 200);
        match request.mode {
            SearchMode::Token => self.token_search_query(request, tenant_id).await,
            SearchMode::Prefix => self.prefix_search_query(request, tenant_id).await,
            SearchMode::Fuzzy => self.fuzzy_search_query(request, tenant_id).await,
            SearchMode::Phrase => self.phrase_search_query(request, tenant_id).await,
        }
    }

    async fn token_search_query(
        &self,
        request: SearchQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        let terms = search_query_terms(&request.query);
        if terms.is_empty() {
            return Err(ReadError::InvalidQuery(
                "search query must contain at least one token".to_string(),
            ));
        }
        let terms_sql = terms
            .iter()
            .map(|term| quote_sql_string(term))
            .collect::<Vec<_>>()
            .join(", ");
        let mut where_clauses = vec![format!("term IN ({terms_sql})")];
        let mut parameters = search_base_parameters(&request);
        add_search_time_and_type_filters(&request, &mut where_clauses, &mut parameters);
        add_search_inner_limit(&request, &mut parameters);
        parameters.insert(
            "search_snippet_needle".to_string(),
            Value::from(terms.first().cloned().unwrap_or_default()),
        );
        let having_clause =
            token_search_require_all_terms_having(&request, terms.len(), &mut parameters);
        let where_clause = where_clauses.join(" AND ");
        let include_snippets = request.include_snippets;
        let snippet_expr = if include_snippets {
            search_snippet_sql("doc.text", "search_snippet_needle")
        } else {
            "''".to_string()
        };
        let query = token_search_query_sql(
            &where_clause,
            having_clause,
            &snippet_expr,
            include_snippets,
        );
        self.query_with_context_and_metadata(
            QueryRequest {
                query,
                parameters,
                allow_stale_serving: request.allow_stale_serving,
            },
            tenant_id,
            QueryContext::SEARCH,
            token_search_planning_metadata(&request, &terms),
        )
        .await
    }

    async fn prefix_search_query(
        &self,
        request: SearchQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        let terms = search_query_terms(&request.query);
        if terms.is_empty() {
            return Err(ReadError::InvalidQuery(
                "prefix search query must contain at least one token".to_string(),
            ));
        }
        let mut where_clauses = vec![prefix_search_where_clause(&terms)];
        let mut parameters = search_base_parameters(&request);
        add_search_time_and_type_filters(&request, &mut where_clauses, &mut parameters);
        add_search_inner_limit(&request, &mut parameters);
        parameters.insert(
            "search_snippet_needle".to_string(),
            Value::from(terms.first().cloned().unwrap_or_default()),
        );
        let having_clause = search_require_all_terms_having(&request, terms.len(), &mut parameters);
        let snippet_expr = if request.include_snippets {
            search_snippet_sql("doc.text", "search_snippet_needle")
        } else {
            "''".to_string()
        };
        let query = indexed_search_query_sql(
            &where_clauses.join(" AND "),
            &prefix_search_match_expr(&terms),
            "toUInt64(weight)",
            having_clause,
            &snippet_expr,
            request.include_snippets,
        );
        self.query_with_context_and_metadata(
            QueryRequest {
                query,
                parameters,
                allow_stale_serving: request.allow_stale_serving,
            },
            tenant_id,
            QueryContext::SEARCH,
            indexed_search_planning_metadata(&request, &terms, "prefix"),
        )
        .await
    }

    async fn fuzzy_search_query(
        &self,
        request: SearchQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        let terms = search_query_terms(&request.query);
        if terms.is_empty() {
            return Err(ReadError::InvalidQuery(
                "fuzzy search query must contain at least one token".to_string(),
            ));
        }
        let mut where_clauses = vec![fuzzy_search_where_clause(&terms)];
        let mut parameters = search_base_parameters(&request);
        add_search_time_and_type_filters(&request, &mut where_clauses, &mut parameters);
        add_search_inner_limit(&request, &mut parameters);
        parameters.insert(
            "search_snippet_needle".to_string(),
            Value::from(terms.first().cloned().unwrap_or_default()),
        );
        let having_clause = search_require_all_terms_having(&request, terms.len(), &mut parameters);
        let snippet_expr = if request.include_snippets {
            search_snippet_sql("doc.text", "search_snippet_needle")
        } else {
            "''".to_string()
        };
        let query = indexed_search_query_sql(
            &where_clauses.join(" AND "),
            &fuzzy_search_match_expr(&terms),
            &fuzzy_search_score_expr(&terms),
            having_clause,
            &snippet_expr,
            request.include_snippets,
        );
        self.query_with_context_and_metadata(
            QueryRequest {
                query,
                parameters,
                allow_stale_serving: request.allow_stale_serving,
            },
            tenant_id,
            QueryContext::SEARCH,
            indexed_search_planning_metadata(&request, &terms, "fuzzy"),
        )
        .await
    }

    async fn phrase_search_query(
        &self,
        request: SearchQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        let phrase = request.query.trim();
        if phrase.len() < 2 {
            return Err(ReadError::InvalidQuery(
                "phrase search query must contain at least two characters".to_string(),
            ));
        }
        let mut where_clauses =
            vec!["positionCaseInsensitive(search.text, {search_phrase:String}) > 0".to_string()];
        let mut parameters = Map::new();
        parameters.insert("limit".to_string(), Value::from(request.limit));
        parameters.insert("offset".to_string(), Value::from(request.offset));
        parameters.insert("search_phrase".to_string(), Value::from(phrase.to_string()));
        if !request.from.trim().is_empty() {
            where_clauses.push("timestamp >= {search_from:DateTime64(3, 'UTC')}".to_string());
            parameters.insert(
                "search_from".to_string(),
                Value::from(clickhouse_datetime64(&request.from)),
            );
        }
        if !request.to.trim().is_empty() {
            where_clauses.push("timestamp <= {search_to:DateTime64(3, 'UTC')}".to_string());
            parameters.insert(
                "search_to".to_string(),
                Value::from(clickhouse_datetime64(&request.to)),
            );
        }
        if !request.event_type.trim().is_empty() {
            where_clauses.push("event_type = {search_event_type:String}".to_string());
            parameters.insert(
                "search_event_type".to_string(),
                Value::from(request.event_type.trim().to_string()),
            );
        }
        parameters.insert(
            "search_inner_limit".to_string(),
            Value::from(request.limit.saturating_add(request.offset).min(10_000)),
        );
        let where_clause = where_clauses.join(" AND ");
        let snippet_expr = search_snippet_sql("search.text", "search_phrase");
        let query = phrase_search_query_sql(&where_clause, &snippet_expr);
        self.query_with_context_and_metadata(
            QueryRequest {
                query,
                parameters,
                allow_stale_serving: request.allow_stale_serving,
            },
            tenant_id,
            QueryContext::SEARCH,
            phrase_search_planning_metadata(&request),
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn measures_query(
        &self,
        request: MeasureQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        let dimension_set = self
            .resolve_measure_dimension_set(&request, tenant_id)
            .await?;
        for filter in request.filters.keys() {
            if !dimension_set.dimension_names.contains(filter) {
                return Err(ReadError::InvalidQuery(format!(
                    "measure_cube_rollups filters must be part of the selected materialized dimension set; unsupported filter: {filter}"
                )));
            }
        }

        let mut parameters = Map::new();
        parameters.insert(
            "measure_name".to_string(),
            Value::String(request.measure_name.clone()),
        );
        parameters.insert(
            "definition_id".to_string(),
            Value::String(dimension_set.definition_id.clone()),
        );
        parameters.insert(
            "dimension_set_id".to_string(),
            Value::String(dimension_set.id.clone()),
        );
        parameters.insert(
            "from".to_string(),
            Value::String(clickhouse_datetime64(&request.from)),
        );
        parameters.insert(
            "to".to_string(),
            Value::String(clickhouse_datetime64(&request.to)),
        );
        parameters.insert(
            "bucket_seconds".to_string(),
            Value::Number(serde_json::Number::from(request.bucket_seconds)),
        );

        let mut filter_clauses = Vec::new();
        for (index, (field, value)) in request.filters.iter().enumerate() {
            let parameter = format!("filter_{index}");
            parameters.insert(parameter.clone(), Value::String(json_scalar_string(value)));
            filter_clauses.push(format!(
                "dimension_values[indexOf(dimension_names, {})] = {{{parameter}:String}}",
                quote_sql_string(field)
            ));
        }
        let filters = if filter_clauses.is_empty() {
            String::new()
        } else {
            format!(" AND {}", filter_clauses.join(" AND "))
        };
        let query = format!(
            "SELECT bucket_time AS bucketTime, mapFromArrays(dimension_names, dimension_values) AS dimensions, sumMerge(count_state) AS count, sumMerge(sum_state) AS sum, minMerge(min_state) AS min, maxMerge(max_state) AS max, avgMerge(avg_state) AS avg, arrayElement(quantilesTDigestMerge(0.5, 0.9, 0.95, 0.99)(quantiles_state), 1) AS p50, arrayElement(quantilesTDigestMerge(0.5, 0.9, 0.95, 0.99)(quantiles_state), 2) AS p90, arrayElement(quantilesTDigestMerge(0.5, 0.9, 0.95, 0.99)(quantiles_state), 3) AS p95, arrayElement(quantilesTDigestMerge(0.5, 0.9, 0.95, 0.99)(quantiles_state), 4) AS p99, 'measure_cube_rollups' AS source FROM measure_cube_rollups WHERE measure_name = {{measure_name:String}} AND definition_id = {{definition_id:String}} AND dimension_set_id = {{dimension_set_id:String}} AND bucket_seconds = {{bucket_seconds:UInt32}} AND bucket_time >= parseDateTime64BestEffort({{from:String}}, 3, 'UTC') AND bucket_time <= parseDateTime64BestEffort({{to:String}}, 3, 'UTC'){filters} GROUP BY bucket_time, dimension_names, dimension_values ORDER BY bucket_time ASC"
        );
        let response = self
            .query_with_context(
                QueryRequest {
                    query,
                    parameters,
                    allow_stale_serving: request.allow_stale_serving,
                },
                tenant_id,
                QueryContext::MEASURE,
            )
            .await?;
        Ok(serde_json::json!({
            "source": "measure_cube_rollups",
            "definitionId": dimension_set.definition_id,
            "definitionVersion": dimension_set.definition_version,
            "dimensionSet": {
                "id": dimension_set.id,
                "dimensions": dimension_set.dimension_names,
            },
            "rows": response.get("data").cloned().unwrap_or(Value::Array(Vec::new())),
            "nanotrace": response.get("nanotrace").cloned().unwrap_or(Value::Null),
        }))
    }

    #[allow(dead_code)]
    pub async fn funnels_query(
        &self,
        request: FunnelQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        validate_definition_id(&request.report_id)?;
        let report_id = request.report_id.clone();
        let version_selection = self
            .resolve_output_version(
                tenant_id,
                MaterializedOutputTarget {
                    target_type: "sequence",
                    target_id: &report_id,
                    requested_version: request.report_version,
                    serving_table: "sequence_report_results",
                    id_column: "report_id",
                    version_column: "report_version",
                },
            )
            .await?;
        let mut parameters = Map::new();
        parameters.insert("report_id".to_string(), Value::String(report_id.clone()));
        parameters.insert(
            "report_version".to_string(),
            Value::Number(serde_json::Number::from(version_selection.version)),
        );
        let mut clauses = vec![
            "report_id = {report_id:String}".to_string(),
            "report_version = {report_version:UInt64}".to_string(),
        ];
        if !request.from.trim().is_empty() {
            parameters.insert(
                "from".to_string(),
                Value::String(clickhouse_datetime64(&request.from)),
            );
            clauses.push(
                "bucket_time >= parseDateTime64BestEffort({from:String}, 3, 'UTC')".to_string(),
            );
        }
        if !request.to.trim().is_empty() {
            parameters.insert(
                "to".to_string(),
                Value::String(clickhouse_datetime64(&request.to)),
            );
            clauses.push(
                "bucket_time <= parseDateTime64BestEffort({to:String}, 3, 'UTC')".to_string(),
            );
        }
        for (index, (field, value)) in request.filters.iter().enumerate() {
            validate_dimension_name(field)?;
            let parameter = format!("filter_{index}");
            parameters.insert(parameter.clone(), Value::String(json_scalar_string(value)));
            clauses.push(format!(
                "ifNull(toString(getSubcolumn(segment, {})), '') = {{{parameter}:String}}",
                quote_sql_string(field)
            ));
        }
        let query = format!(
            "SELECT bucket_time AS bucketTime, segment AS dimensions, step_index AS stepIndex, step_name AS stepName, sum(entity_count) AS entityCount, sum(conversion_count) AS conversionCount, 'sequence_report_results' AS source FROM sequence_report_results WHERE {} GROUP BY bucket_time, segment, step_index, step_name ORDER BY bucket_time ASC, step_index ASC",
            clauses.join(" AND ")
        );
        let response = self
            .query_with_context(
                QueryRequest {
                    query,
                    parameters,
                    allow_stale_serving: request.allow_stale_serving,
                },
                tenant_id,
                QueryContext::FUNNEL,
            )
            .await?;
        Ok(serde_json::json!({
            "source": "sequence_report_results",
            "reportId": report_id,
            "reportVersion": version_selection.version,
            "rows": response.get("data").cloned().unwrap_or(Value::Array(Vec::new())),
            "nanotrace": nanotrace_with_materialization(
                &response,
                MaterializationSelectionMetadata {
                    target_type: "sequence",
                    target_id: &report_id,
                    target_version: version_selection.version,
                    selector: version_selection.selector,
                },
            ),
        }))
    }

    #[allow(dead_code)]
    pub async fn cohorts_query(
        &self,
        request: CohortQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        validate_definition_id(&request.cohort_id)?;
        let cohort_id = request.cohort_id.clone();
        let version_selection = self
            .resolve_output_version(
                tenant_id,
                MaterializedOutputTarget {
                    target_type: "cohort",
                    target_id: &cohort_id,
                    requested_version: request.cohort_version,
                    serving_table: "cohort_memberships",
                    id_column: "cohort_id",
                    version_column: "cohort_version",
                },
            )
            .await?;
        let mut parameters = Map::new();
        parameters.insert("cohort_id".to_string(), Value::String(cohort_id.clone()));
        parameters.insert(
            "cohort_version".to_string(),
            Value::Number(serde_json::Number::from(version_selection.version)),
        );
        parameters.insert(
            "limit".to_string(),
            Value::Number(serde_json::Number::from(request.limit.min(10_000))),
        );
        let mut clauses = vec![
            "cohort_id = {cohort_id:String}".to_string(),
            "cohort_version = {cohort_version:UInt64}".to_string(),
        ];
        if !request.entity_type.trim().is_empty() {
            parameters.insert(
                "entity_type".to_string(),
                Value::String(request.entity_type),
            );
            clauses.push("entity_type = {entity_type:String}".to_string());
        }
        let query = format!(
            "SELECT entity_type AS entityType, entity_id AS entityId, first_seen AS firstSeen, last_seen AS lastSeen, 'cohort_memberships' AS source FROM cohort_memberships WHERE {} ORDER BY first_seen ASC, entity_id ASC LIMIT {{limit:UInt64}}",
            clauses.join(" AND ")
        );
        let response = self
            .query_with_context(
                QueryRequest {
                    query,
                    parameters,
                    allow_stale_serving: request.allow_stale_serving,
                },
                tenant_id,
                QueryContext::COHORT,
            )
            .await?;
        let rows = response
            .get("data")
            .cloned()
            .unwrap_or(Value::Array(Vec::new()));
        let count = rows.as_array().map(Vec::len).unwrap_or_default();
        Ok(serde_json::json!({
            "source": "cohort_memberships",
            "cohortId": cohort_id,
            "cohortVersion": version_selection.version,
            "count": count,
            "rows": rows,
            "nanotrace": nanotrace_with_materialization(
                &response,
                MaterializationSelectionMetadata {
                    target_type: "cohort",
                    target_id: &cohort_id,
                    target_version: version_selection.version,
                    selector: version_selection.selector,
                },
            ),
        }))
    }

    #[allow(dead_code)]
    pub async fn reports_query(
        &self,
        mut request: ReportQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        validate_definition_id(&request.report_id)?;
        request.limit = request.limit.clamp(1, 10_000);
        let report_id = request.report_id.clone();
        let version_selection = self
            .resolve_output_version(
                tenant_id,
                MaterializedOutputTarget {
                    target_type: "report",
                    target_id: &report_id,
                    requested_version: request.report_version,
                    serving_table: "report_results",
                    id_column: "report_id",
                    version_column: "report_version",
                },
            )
            .await?;

        let mut parameters = Map::new();
        parameters.insert("report_id".to_string(), Value::String(report_id.clone()));
        parameters.insert(
            "report_version".to_string(),
            Value::Number(serde_json::Number::from(version_selection.version)),
        );
        parameters.insert(
            "limit".to_string(),
            Value::Number(serde_json::Number::from(request.limit)),
        );
        let mut clauses = vec![
            "report_id = {report_id:String}".to_string(),
            "report_version = {report_version:UInt64}".to_string(),
        ];
        if !request.from.trim().is_empty() {
            parameters.insert(
                "from".to_string(),
                Value::String(clickhouse_datetime64(&request.from)),
            );
            clauses.push(
                "bucket_time >= parseDateTime64BestEffort({from:String}, 3, 'UTC')".to_string(),
            );
        }
        if !request.to.trim().is_empty() {
            parameters.insert(
                "to".to_string(),
                Value::String(clickhouse_datetime64(&request.to)),
            );
            clauses.push(
                "bucket_time <= parseDateTime64BestEffort({to:String}, 3, 'UTC')".to_string(),
            );
        }
        for (index, (field, value)) in request.filters.iter().enumerate() {
            validate_dimension_name(field)?;
            let parameter = format!("filter_{index}");
            parameters.insert(parameter.clone(), Value::String(json_scalar_string(value)));
            clauses.push(format!(
                "ifNull(toString(getSubcolumn(dimensions, {})), '') = {{{parameter}:String}}",
                quote_sql_string(field)
            ));
        }

        let response = self
            .query_with_context(
                QueryRequest {
                    query: format!(
                        "SELECT report_id AS reportId, report_version AS reportVersion, bucket_time AS bucketTime, dimensions, metrics, refreshed_at AS refreshedAt, 'report_results' AS source FROM report_results WHERE {} ORDER BY bucket_time ASC LIMIT {{limit:UInt64}}",
                        clauses.join(" AND ")
                    ),
                    parameters,
                    allow_stale_serving: request.allow_stale_serving,
                },
                tenant_id,
                QueryContext::REPORT,
            )
            .await?;
        Ok(serde_json::json!({
            "source": "report_results",
            "reportId": report_id,
            "reportVersion": version_selection.version,
            "rows": response.get("data").cloned().unwrap_or(Value::Array(Vec::new())),
            "nanotrace": nanotrace_with_materialization(
                &response,
                MaterializationSelectionMetadata {
                    target_type: "report",
                    target_id: &report_id,
                    target_version: version_selection.version,
                    selector: version_selection.selector,
                },
            ),
        }))
    }

    #[allow(dead_code)]
    pub async fn states_query(
        &self,
        mut request: StateQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        validate_dimension_name(&request.entity_type)?;
        validate_dimension_name(&request.state_name)?;
        request.limit = request.limit.clamp(1, 10_000);

        let mut parameters = Map::new();
        parameters.insert(
            "entity_type".to_string(),
            Value::String(request.entity_type),
        );
        parameters.insert("state_name".to_string(), Value::String(request.state_name));
        parameters.insert(
            "limit".to_string(),
            Value::Number(serde_json::Number::from(request.limit)),
        );
        let mut clauses = vec![
            "entity_type = {entity_type:String}".to_string(),
            "state_name = {state_name:String}".to_string(),
        ];
        if !request.entity_id.trim().is_empty() {
            parameters.insert("entity_id".to_string(), Value::String(request.entity_id));
            clauses.push("entity_id = {entity_id:String}".to_string());
        }
        if !request.as_of.trim().is_empty() {
            parameters.insert(
                "as_of".to_string(),
                Value::String(clickhouse_datetime64(&request.as_of)),
            );
            clauses.push(
                "timestamp <= parseDateTime64BestEffort({as_of:String}, 3, 'UTC')".to_string(),
            );
        }

        let current_state = request.as_of.trim().is_empty();
        let source_table = if current_state {
            "entity_state_current"
        } else {
            "entity_state_updates"
        };
        let select = format!(
            "SELECT entity_type AS entityType, entity_id AS entityId, state_name AS stateName, argMax(value, timestamp) AS value, argMax(value_type, timestamp) AS valueType, max(timestamp) AS updatedAt, argMax(event_id, timestamp) AS eventId, '{source_table}' AS source"
        );
        let response = self
            .query_with_context(
                QueryRequest {
                    query: format!(
                        "{select} FROM {source_table} WHERE {} GROUP BY entity_type, entity_id, state_name ORDER BY entity_id ASC LIMIT {{limit:UInt64}}",
                        clauses.join(" AND "),
                    ),
                    parameters,
                    allow_stale_serving: request.allow_stale_serving,
                },
                tenant_id,
                QueryContext::STATE,
            )
            .await?;
        Ok(serde_json::json!({
            "source": source_table,
            "rows": response.get("data").cloned().unwrap_or(Value::Array(Vec::new())),
            "nanotrace": response.get("nanotrace").cloned().unwrap_or(Value::Null),
        }))
    }

    #[allow(dead_code)]
    pub async fn alerts_query(
        &self,
        mut request: AlertQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        request.limit = request.limit.clamp(1, 1000);

        let mut parameters = Map::new();
        parameters.insert(
            "limit".to_string(),
            Value::Number(serde_json::Number::from(request.limit)),
        );
        let mut clauses = Vec::new();
        if !request.alert_id.trim().is_empty() {
            validate_definition_id(&request.alert_id)?;
            parameters.insert("alert_id".to_string(), Value::String(request.alert_id));
            clauses.push("alert_id = {alert_id:String}".to_string());
        }
        if !request.event_id.trim().is_empty() {
            validate_alert_filter_value(&request.event_id, "eventId")?;
            parameters.insert("event_id".to_string(), Value::String(request.event_id));
            clauses.push("event_id = {event_id:String}".to_string());
        }
        if !request.status.trim().is_empty() {
            validate_alert_filter_value(&request.status, "status")?;
            parameters.insert("status".to_string(), Value::String(request.status));
            clauses.push("status = {status:String}".to_string());
        }
        if !request.from.trim().is_empty() {
            parameters.insert(
                "from".to_string(),
                Value::String(clickhouse_datetime64(&request.from)),
            );
            clauses.push(
                "triggered_at >= parseDateTime64BestEffort({from:String}, 3, 'UTC')".to_string(),
            );
        }
        if !request.to.trim().is_empty() {
            parameters.insert(
                "to".to_string(),
                Value::String(clickhouse_datetime64(&request.to)),
            );
            clauses.push(
                "triggered_at <= parseDateTime64BestEffort({to:String}, 3, 'UTC')".to_string(),
            );
        }

        let (mode, source_table, select) = match request.mode {
            AlertQueryMode::Events => (
                "events",
                "alert_events",
                "SELECT alert_id AS alertId, alert_version AS alertVersion, alert_name AS alertName, severity, triggered_at AS triggeredAt, event_timestamp AS eventTimestamp, event_id AS eventId, event_type AS eventType, dedupe_key AS dedupeKey, source_file AS sourceFile, matched, data, 'alert_events' AS source FROM alert_events",
            ),
            AlertQueryMode::Notifications => (
                "notifications",
                "alert_notifications",
                "SELECT notification_id AS notificationId, alert_id AS alertId, alert_version AS alertVersion, alert_name AS alertName, channel, target, headers, status, attempt, max_attempts AS maxAttempts, next_attempt_at AS nextAttemptAt, delivered_at AS deliveredAt, updated_at AS updatedAt, last_error AS lastError, event_id AS eventId, triggered_at AS triggeredAt, payload, 'alert_notifications' AS source FROM alert_notifications",
            ),
        };
        if matches!(request.mode, AlertQueryMode::Events) && parameters.contains_key("status") {
            return Err(ReadError::InvalidQuery(
                "status filter is only supported for alert notifications".to_string(),
            ));
        }
        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let response = self
            .query_with_context(
                QueryRequest {
                    query: format!(
                        "{select}{where_clause} ORDER BY triggered_at DESC LIMIT {{limit:UInt64}}"
                    ),
                    parameters,
                    allow_stale_serving: request.allow_stale_serving,
                },
                tenant_id,
                QueryContext::ALERTS,
            )
            .await?;
        Ok(serde_json::json!({
            "source": source_table,
            "mode": mode,
            "rows": response.get("data").cloned().unwrap_or(Value::Array(Vec::new())),
            "nanotrace": response.get("nanotrace").cloned().unwrap_or(Value::Null),
        }))
    }

    async fn resolve_output_version(
        &self,
        tenant_id: &str,
        target: MaterializedOutputTarget<'_>,
    ) -> Result<VersionSelection, ReadError> {
        if target.requested_version > 0 {
            return Ok(VersionSelection {
                version: target.requested_version,
                selector: "requested",
            });
        }

        let mut parameters = Map::new();
        parameters.insert(
            "tenant_id".to_string(),
            Value::String(tenant_id.to_string()),
        );
        parameters.insert(
            "target_type".to_string(),
            Value::String(target.target_type.to_string()),
        );
        parameters.insert(
            "target_id".to_string(),
            Value::String(target.target_id.to_string()),
        );
        let response = self
            .clickhouse_query(
                "SELECT target_version FROM materialization_versions WHERE tenant_id = {tenant_id:String} AND target_type = {target_type:String} AND target_id = {target_id:String} AND active = 1 AND status IN ('active', 'completed', 'materialized', 'loaded') ORDER BY completed_at DESC, updated_at DESC, target_version DESC LIMIT 1",
                &parameters,
            )
            .await?;
        let response: ClickHouseResponse<MaterializationVersionRow> =
            serde_json::from_str(&response).map_err(ReadError::InvalidStoredEvent)?;
        if let Some(row) = response.data.into_iter().next() {
            return Ok(VersionSelection {
                version: row.target_version,
                selector: "active_version",
            });
        }

        let response = self
            .clickhouse_query(
                &format!(
                    "SELECT ifNull(max({}), 0) AS target_version FROM {} WHERE tenant_id = {{tenant_id:String}} AND {} = {{target_id:String}}",
                    target.version_column, target.serving_table, target.id_column
                ),
                &parameters,
            )
            .await?;
        let response: ClickHouseResponse<MaterializationVersionRow> =
            serde_json::from_str(&response).map_err(ReadError::InvalidStoredEvent)?;
        Ok(VersionSelection {
            version: response
                .data
                .into_iter()
                .next()
                .map(|row| row.target_version)
                .unwrap_or(0),
            selector: "serving_table_latest",
        })
    }

    async fn query_with_freshness_overrides_and_metadata(
        &self,
        request: QueryRequest,
        tenant_id: &str,
        freshness_overrides: &BTreeSet<String>,
        context: QueryContext,
        metadata: QueryPlanningMetadata,
    ) -> Result<Value, ReadError> {
        self.query_with_scope(
            request,
            tenant_id,
            freshness_overrides,
            context,
            QueryTenantScope::Tenant,
            metadata,
        )
        .await
    }

    async fn query_with_scope(
        &self,
        request: QueryRequest,
        tenant_id: &str,
        freshness_overrides: &BTreeSet<String>,
        context: QueryContext,
        scope: QueryTenantScope,
        metadata: QueryPlanningMetadata,
    ) -> Result<Value, ReadError> {
        let query = checked_select_query(&request.query)?;
        let query = normalize_prewhere(&query);
        let sources = query_sources(&query);
        validate_query_sources(&query, &self.allowed_table_names())?;
        let plan_kind = query_plan_kind(&query, &sources);
        let shape_class = query_shape_class(context, plan_kind, &metadata);
        let recommendations = query_recommendations(context, plan_kind, &sources, &metadata);
        let usage_shape = query_usage_shape(&query);
        let usage_hash = query_shape_hash(&usage_shape);
        let parameter_types = parameter_types(&request.parameters);
        let started_at = Instant::now();
        if !request.allow_stale_serving
            && let Err(err) = self
                .ensure_serving_fresh(&sources, freshness_overrides)
                .await
        {
            self.record_query_usage(QueryUsageRecord {
                tenant_id,
                query_shape: &usage_shape,
                query_hash: usage_hash,
                surface: context.surface,
                plan_kind,
                source_tables: &sources,
                parameter_types: &parameter_types,
                elapsed_ms: elapsed_ms(started_at.elapsed()),
                result_rows: 0,
                read_rows: 0,
                read_bytes: 0,
                status: query_error_status(&err),
                allow_stale_serving: request.allow_stale_serving,
                metadata: metadata.clone(),
                shape_class,
                recommendations: recommendations.clone(),
            })
            .await;
            return Err(err);
        }
        let query = match scope {
            QueryTenantScope::Tenant => self.scope_query(&query)?,
            QueryTenantScope::Global => query,
        };
        let mut parameters = request.parameters;
        if matches!(scope, QueryTenantScope::Tenant) {
            parameters.insert(
                "__nanotrace_tenant_id".to_string(),
                Value::String(tenant_id.to_string()),
            );
        }
        let text = match self.clickhouse_query(&query, &parameters).await {
            Ok(text) => text,
            Err(err) => {
                self.record_query_usage(QueryUsageRecord {
                    tenant_id,
                    query_shape: &usage_shape,
                    query_hash: usage_hash,
                    surface: context.surface,
                    plan_kind,
                    source_tables: &sources,
                    parameter_types: &parameter_types,
                    elapsed_ms: elapsed_ms(started_at.elapsed()),
                    result_rows: 0,
                    read_rows: 0,
                    read_bytes: 0,
                    status: query_error_status(&err),
                    allow_stale_serving: request.allow_stale_serving,
                    metadata: metadata.clone(),
                    shape_class,
                    recommendations: recommendations.clone(),
                })
                .await;
                return Err(err);
            }
        };
        let mut response: Value = match serde_json::from_str(&text) {
            Ok(response) => response,
            Err(err) => {
                self.record_query_usage(QueryUsageRecord {
                    tenant_id,
                    query_shape: &usage_shape,
                    query_hash: usage_hash,
                    surface: context.surface,
                    plan_kind,
                    source_tables: &sources,
                    parameter_types: &parameter_types,
                    elapsed_ms: elapsed_ms(started_at.elapsed()),
                    result_rows: 0,
                    read_rows: 0,
                    read_bytes: 0,
                    status: "invalid_clickhouse_json",
                    allow_stale_serving: request.allow_stale_serving,
                    metadata: metadata.clone(),
                    shape_class,
                    recommendations: recommendations.clone(),
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
            surface: context.surface,
            plan_kind,
            source_tables: &sources,
            parameter_types: &parameter_types,
            elapsed_ms: elapsed_ms(started_at.elapsed()),
            result_rows: stats.result_rows,
            read_rows: stats.read_rows,
            read_bytes: stats.read_bytes,
            status: "ok",
            allow_stale_serving: request.allow_stale_serving,
            metadata: metadata.clone(),
            shape_class,
            recommendations: recommendations.clone(),
        })
        .await;
        attach_query_explanation(
            &mut response,
            QueryExplanation {
                surface: context.surface,
                plan_kind,
                source_tables: sources.clone(),
                allow_stale_serving: request.allow_stale_serving,
                freshness_overrides: freshness_overrides.iter().cloned().collect(),
                tenant_scope: scope.name(),
                shape_class,
                recommendations,
            },
        );
        Ok(response)
    }

    #[allow(dead_code)]
    async fn resolve_measure_dimension_set(
        &self,
        request: &MeasureQueryRequest,
        tenant_id: &str,
    ) -> Result<ResolvedMeasureDimensionSet, ReadError> {
        for group in &request.group_by {
            validate_dimension_name(group)?;
        }
        if !request.definition_id.trim().is_empty() {
            validate_definition_id(&request.definition_id)?;
        }
        let definition_filter = if request.definition_id.trim().is_empty() {
            String::new()
        } else {
            " AND definition_id = {definition_id:String}".to_string()
        };
        let mut parameters = Map::new();
        if !request.definition_id.trim().is_empty() {
            parameters.insert(
                "definition_id".to_string(),
                Value::String(request.definition_id.clone()),
            );
        }
        let response = self
            .query(
                QueryRequest {
                    query: format!(
                        "SELECT definition_id, name, version, config FROM definitions WHERE kind = 'measure' AND mode = 'cube' AND enabled = 1 AND isNull(deleted_at){definition_filter} ORDER BY updated_at DESC"
                    ),
                    parameters,
                    allow_stale_serving: true,
                },
                tenant_id,
            )
            .await?;
        let rows: Vec<MeasureDefinitionCatalogRow> = serde_json::from_value(
            response
                .get("data")
                .cloned()
                .unwrap_or(Value::Array(Vec::new())),
        )
        .map_err(ReadError::InvalidStoredEvent)?;
        let mut available = Vec::new();
        for row in rows {
            for output in measure_cube_outputs(&row.config) {
                if !measure_output_matches(&row, output, request) {
                    continue;
                }
                for dimension_set in output
                    .get("dimension_sets")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flat_map(|sets| sets.iter())
                    .filter_map(Value::as_object)
                {
                    let id = dimension_set
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    let names = dimension_set
                        .get("dimensions")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flat_map(|dimensions| dimensions.iter())
                        .filter_map(|dimension| {
                            dimension
                                .as_object()
                                .and_then(|object| object.get("name"))
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned)
                        })
                        .collect::<Vec<_>>();
                    if id.is_empty() || names.is_empty() {
                        continue;
                    }
                    let available_set = AvailableMeasureDimensionSet {
                        definition_id: row.definition_id.clone(),
                        definition_version: row.version,
                        id,
                        dimension_names: names,
                    };
                    if available_set.dimension_names == request.group_by {
                        return Ok(ResolvedMeasureDimensionSet {
                            definition_id: available_set.definition_id,
                            definition_version: available_set.definition_version,
                            id: available_set.id,
                            dimension_names: available_set.dimension_names,
                        });
                    }
                    available.push(available_set);
                }
            }
        }
        Err(ReadError::InvalidQuery(format!(
            "requested measure grouping is not materialized; available dimension sets: {}",
            serde_json::to_string(&available).unwrap_or_else(|_| "[]".to_string())
        )))
    }

    pub async fn events_query(
        &self,
        mut request: EventsQueryRequest,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        request.limit = request.limit.clamp(1, 10_000);
        request.buckets = request.buckets.clamp(1, 2_000);

        let catalog = match self.event_field_catalog(tenant_id, &request).await {
            Ok(catalog) => catalog,
            Err(err) => {
                warn!(error = %err, "failed to load promoted field catalog; using generic filter fallback planning");
                EventFieldCatalog::default()
            }
        };

        match request.view {
            EventsQueryView::GroupOptions => {
                let mut parameters = Map::new();
                parameters.insert("limit".to_string(), Value::from(request.limit));
                self.query_with_context(
                    QueryRequest {
                        query: group_options_query(),
                        parameters,
                        allow_stale_serving: true,
                    },
                    tenant_id,
                    QueryContext::EVENTS,
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
        catalog: &EventFieldCatalog,
        tenant_id: &str,
    ) -> Result<Value, ReadError> {
        let freshness_overrides = catalog.freshness_proven_tables();
        let mut response = self
            .query_with_freshness_overrides_and_metadata(
                QueryRequest {
                    query,
                    parameters,
                    allow_stale_serving: request.allow_stale_serving,
                },
                tenant_id,
                &freshness_overrides,
                QueryContext::EVENTS,
                event_query_planning_metadata(request, catalog),
            )
            .await?;
        attach_event_filter_explanation(&mut response, request, catalog);
        Ok(response)
    }

    async fn event_field_catalog(
        &self,
        tenant_id: &str,
        request: &EventsQueryRequest,
    ) -> Result<EventFieldCatalog, ReadError> {
        let window = MaterializationWindow::from_request(request);
        let indexed_paths = self.indexed_field_paths(tenant_id).await?;
        let fresh_definition_versions = if request.allow_stale_serving {
            BTreeSet::new()
        } else {
            self.fresh_materialized_definition_versions(tenant_id, "field", &window)
                .await?
        };
        let field_index_serving_fresh = if request.allow_stale_serving {
            false
        } else {
            self.serving_table_fresh("field_index").await?
        };
        let mut parameters = Map::new();
        parameters.insert(
            "tenant_id".to_string(),
            Value::String(tenant_id.to_string()),
        );
        let response = self
            .clickhouse_query(
                "SELECT definition_id, name, version, config FROM definitions FINAL WHERE tenant_id = {tenant_id:String} AND kind = 'field' AND enabled = 1 AND isNull(deleted_at)",
                &parameters,
            )
            .await?;
        let response: ClickHouseResponse<DefinitionFieldCatalogRow> =
            serde_json::from_str(&response).map_err(ReadError::InvalidStoredEvent)?;
        let mut catalog = EventFieldCatalog::default();
        for row in response.data {
            let definition_key = DefinitionVersionKey {
                definition_id: row.definition_id.clone(),
                version: row.version,
            };
            let definition_is_fresh = fresh_definition_versions.contains(&definition_key);
            let mut paths = vec![normalized_payload_path(&row.name)];
            if let Some(outputs) = row.config.get("outputs").and_then(Value::as_array) {
                for output in outputs {
                    if output
                        .get("target")
                        .and_then(Value::as_str)
                        .is_some_and(|target| target != "field_index")
                    {
                        continue;
                    }
                    if let Some(field_name) = output.get("field_name").and_then(Value::as_str) {
                        paths.push(normalized_payload_path(field_name));
                    }
                }
            }
            for path in paths {
                let indexed_key = IndexedFieldPath {
                    path: path.clone(),
                    definition_id: row.definition_id.clone(),
                    version: row.version,
                };
                let index_rows_match_definition = indexed_paths.contains(&indexed_key);
                if definition_is_fresh
                    || (field_index_serving_fresh && index_rows_match_definition)
                    || (request.allow_stale_serving
                        && indexed_paths.iter().any(|indexed| indexed.path == path))
                {
                    catalog.add_promoted(path, definition_key.clone());
                }
            }
        }
        Ok(catalog)
    }

    async fn indexed_field_paths(
        &self,
        tenant_id: &str,
    ) -> Result<BTreeSet<IndexedFieldPath>, ReadError> {
        let mut parameters = Map::new();
        parameters.insert(
            "tenant_id".to_string(),
            Value::String(tenant_id.to_string()),
        );
        let response = self
            .clickhouse_query(
                "SELECT field_name, definition_id, max(definition_version) AS definition_version FROM field_index WHERE tenant_id = {tenant_id:String} AND mode IN ('facet', 'lookup') GROUP BY field_name, definition_id",
                &parameters,
            )
            .await?;
        let response: ClickHouseResponse<IndexedFieldPathRow> =
            serde_json::from_str(&response).map_err(ReadError::InvalidStoredEvent)?;
        let mut paths = BTreeSet::new();
        for row in response.data {
            paths.insert(IndexedFieldPath {
                path: normalized_payload_path(&row.field_name),
                definition_id: row.definition_id,
                version: row.definition_version,
            });
        }
        Ok(paths)
    }

    async fn fresh_materialized_definition_versions(
        &self,
        tenant_id: &str,
        target_type: &str,
        window: &MaterializationWindow,
    ) -> Result<BTreeSet<DefinitionVersionKey>, ReadError> {
        let mut parameters = Map::new();
        parameters.insert(
            "tenant_id".to_string(),
            Value::String(tenant_id.to_string()),
        );
        parameters.insert(
            "target_type".to_string(),
            Value::String(target_type.to_string()),
        );
        parameters.insert("from".to_string(), Value::String(window.from.clone()));
        parameters.insert("to".to_string(), Value::String(window.to.clone()));
        let response = self
            .clickhouse_query(
                "SELECT target_id, target_version FROM materialization_watermarks WHERE tenant_id = {tenant_id:String} AND target_type = {target_type:String} AND source_table = 'events' AND status IN ('active', 'completed', 'materialized', 'loaded') AND low_watermark <= parseDateTime64BestEffort({from:String}, 3, 'UTC') AND high_watermark >= parseDateTime64BestEffort({to:String}, 3, 'UTC') GROUP BY target_id, target_version",
                &parameters,
            )
            .await?;
        let response: ClickHouseResponse<MaterializedDefinitionRow> =
            serde_json::from_str(&response).map_err(ReadError::InvalidStoredEvent)?;
        Ok(response
            .data
            .into_iter()
            .map(|row| DefinitionVersionKey {
                definition_id: row.target_id,
                version: row.target_version,
            })
            .collect())
    }

    async fn serving_table_fresh(&self, table: &str) -> Result<bool, ReadError> {
        let latest_sequence = self.latest_lakehouse_sequence().await?;
        if latest_sequence == 0 {
            return Ok(false);
        }
        let requested = BTreeSet::from([table.to_string()]);
        let watermarks = self.serving_sequences(&requested).await?;
        Ok(watermarks.get(table).copied().unwrap_or(0) >= latest_sequence)
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
        } else if let Some(index_access) = catalog.index_table(&group_by) {
            grouped_index_query(request, &group_by, index_access)
        } else {
            raw_groups_query(request, &group_by)?
        };

        let response = self
            .run_events_query_sql(
                primary_query,
                primary_parameters,
                request,
                catalog,
                tenant_id,
            )
            .await?;
        if response_data_len(&response) > 0 || !catalog.has_indexed_path(&group_by) {
            return Ok(response);
        }

        let (query, parameters) = raw_groups_query(request, &group_by)?;
        self.run_events_query_sql(query, parameters, request, catalog, tenant_id)
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
                    .run_events_query_sql(query, parameters, request, catalog, tenant_id)
                    .await;
            }
            if catalog.lookup_contains(&group_by) {
                let (query, parameters) =
                    latest_grouped_index_query(request, &group_by, IndexAccess::FieldValues);
                return self
                    .run_events_query_sql(query, parameters, request, catalog, tenant_id)
                    .await;
            }
            if let Some(IndexAccess::FieldIndex(definitions)) = catalog.index_table(&group_by) {
                let (query, parameters) = latest_grouped_index_query(
                    request,
                    &group_by,
                    IndexAccess::FieldIndex(definitions),
                );
                return self
                    .run_events_query_sql(query, parameters, request, catalog, tenant_id)
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
            catalog,
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
                    catalog,
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
                        catalog,
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
            catalog,
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
        self.run_events_query_sql(query, plan.parameters, request, catalog, tenant_id)
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
                    catalog,
                    tenant_id,
                )
                .await?;
            let bucket_ms = density_bucket_ms(&range, request.buckets, 1_000);
            parameters.insert("bucket_ms".to_string(), Value::from(bucket_ms));
            return self
                .run_events_query_sql(
                    [
                        format!(
                            "WITH intDiv((toUnixTimestamp({}) * 1000), {{bucket_ms:UInt64}}) * {{bucket_ms:UInt64}} AS bucket",
                            "d.bucket_time"
                        ),
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
                    catalog,
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
                        catalog,
                        tenant_id,
                    )
                    .await?;
                let bucket_ms = density_bucket_ms(&range, request.buckets, 1);
                parameters.insert("bucket_ms".to_string(), Value::from(bucket_ms));
                return self
                    .run_events_query_sql(
                        [
                            format!(
                                "WITH intDiv((toUnixTimestamp({}) * 1000), {{bucket_ms:UInt64}}) * {{bucket_ms:UInt64}} AS bucket",
                                "d.bucket_time"
                            ),
                            "SELECT bucket, sum(d.count) AS count, sum(d.error_count) AS errorCount".to_string(),
                            base,
                            "GROUP BY bucket ORDER BY bucket ASC".to_string(),
                        ]
                        .join(" "),
                        parameters,
                        request,
                        catalog,
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
                catalog,
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
                format!(
                    "WITH intDiv((toUnixTimestamp({}) * 1000), {{bucket_ms:UInt64}}) * {{bucket_ms:UInt64}} AS bucket",
                    "e.timestamp"
                ),
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
            catalog,
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
            catalog,
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
        let catalog = EventFieldCatalog::default();
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
            &catalog,
            tenant_id,
        )
        .await
    }

    pub async fn event_bytes(&self, event_id: &str, tenant_id: &str) -> Result<Bytes, ReadError> {
        if event_id.trim().is_empty() {
            return Err(ReadError::InvalidQuery("event_id is required".to_string()));
        }

        self.event_bytes_from_clickhouse(event_id, tenant_id).await
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
            surface: record.surface,
            plan_kind: record.plan_kind,
            is_raw_fallback: u8::from(is_raw_fallback_plan(record.plan_kind)),
            source_tables: record.source_tables,
            json_paths: &record.metadata.json_paths,
            filter_paths: &record.metadata.filter_paths,
            group_by_paths: &record.metadata.group_by_paths,
            time_range_start: &record.metadata.time_range_start,
            time_range_end: &record.metadata.time_range_end,
            result_rows: record.result_rows,
            read_rows: record.read_rows,
            read_bytes: record.read_bytes,
            elapsed_ms: record.elapsed_ms,
            status: record.status,
            error: "",
            attributes: serde_json::json!({
                "allow_stale_serving": record.allow_stale_serving,
                "event_view": record.metadata.event_view,
                "filter_routes": record.metadata.filter_routes,
                "parameter_types": record.parameter_types,
                "query_shape_class": record.shape_class,
                "recommendations": record.recommendations,
                "sanitizer": "sqlparser-clickhouse-token-shape-v1",
                "text_search": record.metadata.text_search,
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

    async fn ensure_serving_fresh(
        &self,
        sources: &[String],
        freshness_overrides: &BTreeSet<String>,
    ) -> Result<(), ReadError> {
        let requested = sources
            .iter()
            .filter_map(|source| self.guarded_serving_table(source))
            .filter(|table| !freshness_overrides.contains(table))
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
            "event_text_index",
            "event_search_terms",
            "event_kv_index",
            "field_index",
            "event_measures",
            "measure_cube_points",
            "measure_cube_rollups",
            "counter_rollups",
            "gauge_rollups",
            "histogram_rollups",
            "entity_state_updates",
            "entity_state_current",
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

    fn scope_query(&self, query: &str) -> Result<String, ReadError> {
        let tables = self.allowed_table_names();
        scope_query_with_allowed_tables(query, &tables, |table| self.scoped_table_query(table))
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
            "event_text_index",
            "event_search_terms",
            "event_kv_index",
            "field_index",
            "field_values",
            "field_rollups",
            "event_density_1s",
            "definitions",
            "event_measures",
            "measure_rollups",
            "measure_cube_points",
            "measure_cube_rollups",
            "counter_rollups",
            "gauge_rollups",
            "histogram_rollups",
            "entity_state_updates",
            "entity_state_current",
            "report_results",
            "sequence_report_results",
            "cohort_memberships",
            "alert_events",
            "alert_notifications",
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

fn default_search_query_limit() -> u64 {
    50
}

fn default_events_query_buckets() -> u64 {
    120
}

#[allow(dead_code)]
fn default_measure_bucket_seconds() -> u32 {
    300
}

#[allow(dead_code)]
fn default_cohort_query_limit() -> u64 {
    1000
}

#[allow(dead_code)]
fn default_report_query_limit() -> u64 {
    1000
}

#[allow(dead_code)]
fn default_state_query_limit() -> u64 {
    1000
}

#[allow(dead_code)]
fn default_alert_query_limit() -> u64 {
    100
}

#[derive(Clone, Default)]
struct EventFieldCatalog {
    promoted: BTreeMap<String, Vec<DefinitionVersionKey>>,
}

impl EventFieldCatalog {
    fn promoted_contains(&self, path: &str) -> bool {
        self.promoted.contains_key(path)
    }

    fn lookup_contains(&self, path: &str) -> bool {
        LOOKUP_FIELD_NAMES.contains(&path)
    }

    fn core_rollup_contains(&self, path: &str) -> bool {
        CORE_ROLLUP_FIELD_NAMES.contains(&path)
    }

    fn has_indexed_path(&self, path: &str) -> bool {
        self.promoted_contains(path) || self.lookup_contains(path)
    }

    fn index_table(&self, path: &str) -> Option<IndexAccess> {
        if self.lookup_contains(path) {
            Some(IndexAccess::FieldValues)
        } else if let Some(definitions) = self.promoted.get(path) {
            if definitions.is_empty() {
                None
            } else {
                Some(IndexAccess::FieldIndex(definitions.clone()))
            }
        } else {
            None
        }
    }

    fn add_promoted(&mut self, path: String, definition: DefinitionVersionKey) {
        let definitions = self.promoted.entry(path).or_default();
        if !definitions.contains(&definition) {
            definitions.push(definition);
            definitions.sort();
        }
    }

    fn freshness_proven_tables(&self) -> BTreeSet<String> {
        if self.promoted.is_empty() {
            BTreeSet::new()
        } else {
            BTreeSet::from(["field_index".to_string()])
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum IndexAccess {
    FieldIndex(Vec<DefinitionVersionKey>),
    FieldValues,
}

impl IndexAccess {
    fn table_name(&self) -> &'static str {
        match self {
            Self::FieldIndex(_) => "field_index",
            Self::FieldValues => "field_values",
        }
    }

    fn include_mode(&self) -> bool {
        matches!(self, Self::FieldIndex(_))
    }

    fn definition_refs(&self) -> &[DefinitionVersionKey] {
        match self {
            Self::FieldIndex(definitions) => definitions,
            Self::FieldValues => &[],
        }
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
            let group_clause = if let Some(index_table) = catalog.index_table(&path) {
                let value_param = builder.push_string("group_value", &request.selected_group_value);
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
                event_kv_index_clause(
                    alias,
                    &path,
                    EventFacetOperator::Eq,
                    vec![request.selected_group_value.clone()],
                    None,
                    &request.filter,
                    request.time_range.as_ref(),
                    &mut builder,
                )?
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
            clauses.push(event_text_index_clause(
                alias,
                param,
                &request.filter,
                request.time_range.as_ref(),
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

    fn push_u64(&mut self, prefix: &str, value: u64) -> String {
        let key = self.next_key(prefix);
        self.parameters.insert(key.clone(), Value::from(value));
        key
    }

    fn push_f64(&mut self, prefix: &str, value: f64) -> String {
        let key = self.next_key(prefix);
        self.parameters.insert(key.clone(), Value::from(value));
        key
    }

    fn push_u8(&mut self, prefix: &str, value: u8) -> String {
        let key = self.next_key(prefix);
        self.parameters.insert(key.clone(), Value::from(value));
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
        let mut scoped_facets: BTreeMap<String, Vec<&EventFacetFilter>> = BTreeMap::new();
        for facet in branch {
            let path = facet_key(&facet.path)?;
            if !facet.negated
                && kv_index_operator(facet.operator)
                && let Some(scope) = facet_scope(facet, &path)?
            {
                scoped_facets.entry(scope).or_default().push(facet);
                continue;
            }
            clauses.push(facet_clause_with_path(
                facet, catalog, alias, filter, time_range, builder, path, None,
            )?);
        }
        for (scope, facets) in scoped_facets {
            if facets.len() > 1 {
                clauses.push(scoped_event_kv_index_clause(
                    alias, &scope, &facets, filter, time_range, builder,
                )?);
            } else if let Some(facet) = facets.first() {
                let path = facet_key(&facet.path)?;
                clauses.push(facet_clause_with_path(
                    facet,
                    catalog,
                    alias,
                    filter,
                    time_range,
                    builder,
                    path,
                    Some(scope.as_str()),
                )?);
            }
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

#[allow(clippy::too_many_arguments)]
fn facet_clause_with_path(
    facet: &EventFacetFilter,
    catalog: &EventFieldCatalog,
    alias: &str,
    filter: &EventFilter,
    time_range: Option<&EventTimeRange>,
    builder: &mut SqlBuilder,
    path: String,
    forced_scope: Option<&str>,
) -> Result<String, ReadError> {
    let operator = facet.operator;
    let values = facet_values(facet);
    let scope = match forced_scope {
        Some(scope) => Some(scope.to_string()),
        None => facet_scope(facet, &path)?,
    };
    if scope.is_none()
        && !path.contains("[]")
        && !facet.negated
        && matches!(operator, EventFacetOperator::Eq | EventFacetOperator::In)
        && let Some(index_table) = catalog.index_table(&path)
    {
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
    if kv_index_operator(operator) {
        let mut clause = event_kv_index_clause(
            alias,
            &path,
            operator,
            values,
            scope.as_deref(),
            filter,
            time_range,
            builder,
        )?;
        if facet.negated {
            clause = format!("NOT ({clause})");
        }
        return Ok(format!("({clause})"));
    }

    let expression = event_value_expression(&path, alias)?;
    let mut clause = match operator {
        EventFacetOperator::Contains => {
            let value = values.first().cloned().unwrap_or_default();
            let param = builder.push_string("facet_value", &value);
            format!("positionCaseInsensitive({expression}, {{{param}:String}}) > 0")
        }
        _ => {
            return Err(ReadError::InvalidQuery(format!(
                "unsupported facet operator for raw filter: {operator:?}"
            )));
        }
    };
    if facet.negated {
        clause = format!("NOT ({clause})");
    }
    Ok(format!("({clause})"))
}

fn kv_index_operator(operator: EventFacetOperator) -> bool {
    matches!(
        operator,
        EventFacetOperator::Eq
            | EventFacetOperator::Exists
            | EventFacetOperator::Gt
            | EventFacetOperator::Gte
            | EventFacetOperator::In
            | EventFacetOperator::Lt
            | EventFacetOperator::Lte
    )
}

fn event_text_index_clause(
    alias: &str,
    param: String,
    filter: &EventFilter,
    time_range: Option<&EventTimeRange>,
) -> String {
    let time_clause = time_where_clause(filter, time_range, "search.timestamp").0;
    format!(
        "{alias}.event_id IN (SELECT search.event_id FROM event_text_index AS search WHERE {})",
        join_clauses(vec![
            format!("positionCaseInsensitive(search.text, {{{param}:String}}) > 0"),
            time_clause,
        ])
    )
}

#[allow(clippy::too_many_arguments)]
fn event_kv_index_clause(
    alias: &str,
    path: &str,
    operator: EventFacetOperator,
    values: Vec<String>,
    scope: Option<&str>,
    filter: &EventFilter,
    time_range: Option<&EventTimeRange>,
    builder: &mut SqlBuilder,
) -> Result<String, ReadError> {
    let path_param = builder.push_string("kv_path", path);
    let mut clauses = vec![format!("kv.path = {{{path_param}:String}}")];
    if let Some(scope) = scope {
        let scope_param = builder.push_string("kv_scope", scope);
        clauses.push(format!("kv.scope_path = {{{scope_param}:String}}"));
    }
    clauses.push(event_kv_value_predicate("kv", operator, &values, builder)?);
    let (time_clause, time_parameters) = time_where_clause(filter, time_range, "kv.timestamp");
    builder.parameters.extend(time_parameters);
    clauses.push(time_clause);
    let subquery_where = join_clauses(clauses);
    Ok(format!(
        "{alias}.event_id IN (SELECT kv.event_id FROM event_kv_index AS kv WHERE {subquery_where})"
    ))
}

fn scoped_event_kv_index_clause(
    alias: &str,
    scope: &str,
    facets: &[&EventFacetFilter],
    filter: &EventFilter,
    time_range: Option<&EventTimeRange>,
    builder: &mut SqlBuilder,
) -> Result<String, ReadError> {
    let scope_param = builder.push_string("kv_scope", scope);
    let mut where_options = Vec::new();
    let mut having = Vec::new();
    for facet in facets {
        let path = facet_key(&facet.path)?;
        let path_param = builder.push_string("kv_path", &path);
        let condition = join_clauses(vec![
            format!("kv.path = {{{path_param}:String}}"),
            event_kv_value_predicate("kv", facet.operator, &facet_values(facet), builder)?,
        ]);
        where_options.push(format!("({condition})"));
        having.push(format!("countIf({condition}) > 0"));
    }
    let (time_clause, time_parameters) = time_where_clause(filter, time_range, "kv.timestamp");
    builder.parameters.extend(time_parameters);
    let where_clause = join_clauses(vec![
        format!("kv.scope_path = {{{scope_param}:String}}"),
        time_clause,
        format!("({})", where_options.join(" OR ")),
    ]);
    Ok(format!(
        "{alias}.event_id IN (SELECT kv.event_id FROM event_kv_index AS kv WHERE {where_clause} GROUP BY kv.event_id, kv.scope_path, kv.scope_index HAVING {})",
        having.join(" AND ")
    ))
}

fn event_kv_value_predicate(
    alias: &str,
    operator: EventFacetOperator,
    values: &[String],
    builder: &mut SqlBuilder,
) -> Result<String, ReadError> {
    match operator {
        EventFacetOperator::Exists => Ok("1".to_string()),
        EventFacetOperator::Eq => Ok(values
            .first()
            .map(|value| exact_kv_value_predicate(alias, value, builder))
            .unwrap_or_else(|| "0".to_string())),
        EventFacetOperator::In => {
            if values.is_empty() {
                return Ok("0".to_string());
            }
            let predicates = values
                .iter()
                .map(|value| exact_kv_value_predicate(alias, value, builder))
                .collect::<Vec<_>>();
            Ok(format!("({})", predicates.join(" OR ")))
        }
        EventFacetOperator::Gt
        | EventFacetOperator::Gte
        | EventFacetOperator::Lt
        | EventFacetOperator::Lte => {
            let raw_value = values.first().map(String::as_str).unwrap_or_default();
            let value = raw_value.parse::<f64>().map_err(|_| {
                ReadError::InvalidQuery(format!(
                    "numeric facet operator requires a numeric value: {raw_value}"
                ))
            })?;
            let param = builder.push_f64("kv_number", value);
            let op = match operator {
                EventFacetOperator::Gt => ">",
                EventFacetOperator::Gte => ">=",
                EventFacetOperator::Lt => "<",
                EventFacetOperator::Lte => "<=",
                _ => unreachable!(),
            };
            Ok(format!(
                "{alias}.value_type = 'number' AND {alias}.number_value {op} {{{param}:Float64}}"
            ))
        }
        EventFacetOperator::Contains => Err(ReadError::InvalidQuery(
            "contains is not supported by event_kv_index".to_string(),
        )),
    }
}

fn exact_kv_value_predicate(alias: &str, value: &str, builder: &mut SqlBuilder) -> String {
    let string_param = builder.push_string("kv_string", value);
    let mut predicates = vec![format!(
        "({alias}.value_type = 'string' AND {alias}.string_value = {{{string_param}:String}})"
    )];
    if let Ok(number) = value.parse::<f64>() {
        let number_param = builder.push_f64("kv_number", number);
        predicates.push(format!(
            "({alias}.value_type = 'number' AND {alias}.number_value = {{{number_param}:Float64}})"
        ));
    }
    if let Some(bool_value) = parse_bool_filter_value(value) {
        let bool_param = builder.push_u8("kv_bool", bool_value);
        predicates.push(format!(
            "({alias}.value_type = 'bool' AND {alias}.bool_value = {{{bool_param}:UInt8}})"
        ));
    }
    if value.eq_ignore_ascii_case("null") {
        predicates.push(format!("{alias}.value_type = 'null'"));
    }
    format!("({})", predicates.join(" OR "))
}

fn parse_bool_filter_value(value: &str) -> Option<u8> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Some(1),
        "false" | "0" => Some(0),
        _ => None,
    }
}

fn facet_scope(facet: &EventFacetFilter, path: &str) -> Result<Option<String>, ReadError> {
    if !facet.scope.trim().is_empty() {
        let scope = normalized_payload_path(&facet.scope);
        if is_supported_path(&scope) {
            return Ok(Some(scope));
        }
        return Err(ReadError::InvalidQuery(format!(
            "unsupported facet scope: {scope}"
        )));
    }
    Ok(array_scope_from_path(path))
}

fn array_scope_from_path(path: &str) -> Option<String> {
    let mut parts = Vec::new();
    let mut scope = None;
    for segment in path.split('.') {
        if let Some(base) = segment.strip_suffix("[]") {
            if base.is_empty() {
                return None;
            }
            parts.push(base.to_string());
            scope = Some(parts.join("."));
        } else {
            parts.push(segment.to_string());
        }
    }
    scope
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
    index_access: IndexAccess,
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
    let mode_clause = if index_access.include_mode() {
        "idx.mode IN ('facet', 'lookup')".to_string()
    } else {
        String::new()
    };
    let definition_clause =
        field_index_definition_clause(index_access.definition_refs(), "idx.", builder);
    let subquery_where = join_clauses(vec![
        format!("idx.field_name = {{{field_param}:String}}"),
        value_clause,
        mode_clause,
        definition_clause,
        time_clause,
    ]);
    format!(
        "{alias}.event_id IN (SELECT idx.event_id FROM {} AS idx WHERE {subquery_where})",
        index_access.table_name()
    )
}

fn field_index_definition_clause(
    definitions: &[DefinitionVersionKey],
    prefix: &str,
    builder: &mut SqlBuilder,
) -> String {
    if definitions.is_empty() {
        return String::new();
    }

    let clauses = definitions
        .iter()
        .map(|definition| {
            let id_param = builder.push_string("definition_id", &definition.definition_id);
            let version_param = builder.push_u64("definition_version", definition.version);
            format!(
                "({prefix}definition_id = {{{id_param}:String}} AND {prefix}definition_version = {{{version_param}:UInt64}})"
            )
        })
        .collect::<Vec<_>>();
    format!("({})", clauses.join(" OR "))
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
    index_access: IndexAccess,
) -> (String, Map<String, Value>) {
    let mut builder = SqlBuilder {
        parameters: time_parameters(request.time_range.as_ref()),
        next_parameter: 0,
    };
    builder
        .parameters
        .insert("group_key".to_string(), Value::from(group_by.to_string()));
    builder
        .parameters
        .insert("limit".to_string(), Value::from(request.limit + 1));
    builder
        .parameters
        .insert("offset".to_string(), Value::from(request.offset));
    let mut clauses = vec![
        "field_name = {group_key:String}".to_string(),
        "value != ''".to_string(),
        time_range_clause(request.time_range.as_ref(), "timestamp"),
    ];
    if index_access.include_mode() {
        clauses.push("mode IN ('facet', 'lookup')".to_string());
        clauses.push(field_index_definition_clause(
            index_access.definition_refs(),
            "",
            &mut builder,
        ));
    }
    if !request.search.is_empty() {
        builder.parameters.insert(
            "group_value".to_string(),
            Value::from(request.search.clone()),
        );
        clauses.push("value = {group_value:String}".to_string());
    }
    let count_expression = if matches!(&index_access, IndexAccess::FieldIndex(_)) {
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
            format!("FROM {}", index_access.table_name()),
            where_keyword(join_clauses(clauses)),
            "GROUP BY value".to_string(),
            group_order_by_clause(group_by, true, request.sort.group),
            "LIMIT {limit:UInt64} OFFSET {offset:UInt64}".to_string(),
        ]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" "),
        builder.parameters,
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
    index_access: IndexAccess,
) -> (String, Map<String, Value>) {
    let mut builder = SqlBuilder {
        parameters: time_parameters(request.time_range.as_ref()),
        next_parameter: 0,
    };
    builder
        .parameters
        .insert("group_key".to_string(), Value::from(group_by.to_string()));
    builder.parameters.insert(
        "group_value".to_string(),
        Value::from(request.selected_group_value.clone()),
    );
    let mut clauses = vec![
        "field_name = {group_key:String}".to_string(),
        "value = {group_value:String}".to_string(),
        time_range_clause(request.time_range.as_ref(), "timestamp"),
    ];
    if index_access.include_mode() {
        clauses.push("mode IN ('facet', 'lookup')".to_string());
        clauses.push(field_index_definition_clause(
            index_access.definition_refs(),
            "",
            &mut builder,
        ));
    }
    (
        [
            "SELECT max(timestamp) AS lastCreatedAt".to_string(),
            format!("FROM {}", index_access.table_name()),
            where_keyword(join_clauses(clauses)),
        ]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" "),
        builder.parameters,
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
    if path.contains("[]") {
        return Err(ReadError::InvalidQuery(format!(
            "array path requires indexed filtering: {path}"
        )));
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

const PROMOTED_STRING_COLUMNS: &[&str] =
    &["tenant_id", "trace_id", "span_id", "event_type", "signal"];

fn promoted_string_column(path: &str, alias: &str) -> String {
    if !PROMOTED_STRING_COLUMNS.contains(&path) {
        return String::new();
    }
    if alias.is_empty() {
        path.to_string()
    } else {
        format!("{alias}.{path}")
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

const FACET_PATH_ALIASES: &[(&str, &str)] = &[
    ("durationMs", "duration_ms"),
    ("endedAt", "end_time"),
    ("parentSpanId", "parent_span_id"),
    ("spanId", "span_id"),
    ("startedAt", "start_time"),
    ("traceId", "trace_id"),
];

fn facet_key(path: &str) -> Result<String, ReadError> {
    let path = FACET_PATH_ALIASES
        .iter()
        .find_map(|(display_path, stored_path)| (*display_path == path).then_some(*stored_path))
        .map(str::to_string)
        .unwrap_or_else(|| normalized_payload_path(path));
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
    let segment = segment.strip_suffix("[]").unwrap_or(segment);
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
    clickhouse_datetime64_from_utc(timestamp.with_timezone(&chrono::Utc))
}

fn clickhouse_datetime64_from_utc(timestamp: chrono::DateTime<chrono::Utc>) -> String {
    timestamp.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

#[allow(dead_code)]
fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn search_snippet_sql(text_expr: &str, needle_param: &str) -> String {
    format!(
        "substring({text_expr}, greatest(toInt64(positionCaseInsensitive({text_expr}, {{{needle_param}:String}})) - 80, 1), 240)"
    )
}

fn search_base_parameters(request: &SearchQueryRequest) -> Map<String, Value> {
    let mut parameters = Map::new();
    parameters.insert("limit".to_string(), Value::from(request.limit));
    parameters.insert("offset".to_string(), Value::from(request.offset));
    parameters
}

fn add_search_inner_limit(request: &SearchQueryRequest, parameters: &mut Map<String, Value>) {
    parameters.insert(
        "search_inner_limit".to_string(),
        Value::from(request.limit.saturating_add(request.offset).min(10_000)),
    );
}

fn add_search_time_and_type_filters(
    request: &SearchQueryRequest,
    where_clauses: &mut Vec<String>,
    parameters: &mut Map<String, Value>,
) {
    if !request.from.trim().is_empty() {
        where_clauses.push("timestamp >= {search_from:DateTime64(3, 'UTC')}".to_string());
        parameters.insert(
            "search_from".to_string(),
            Value::from(clickhouse_datetime64(&request.from)),
        );
    }
    if !request.to.trim().is_empty() {
        where_clauses.push("timestamp <= {search_to:DateTime64(3, 'UTC')}".to_string());
        parameters.insert(
            "search_to".to_string(),
            Value::from(clickhouse_datetime64(&request.to)),
        );
    }
    if !request.event_type.trim().is_empty() {
        where_clauses.push("event_type = {search_event_type:String}".to_string());
        parameters.insert(
            "search_event_type".to_string(),
            Value::from(request.event_type.trim().to_string()),
        );
    }
}

fn token_search_require_all_terms_having(
    request: &SearchQueryRequest,
    term_count: usize,
    parameters: &mut Map<String, Value>,
) -> &'static str {
    if request.require_all_terms {
        parameters.insert(
            "search_min_matched_terms".to_string(),
            Value::from(u64::try_from(term_count).unwrap_or(u64::MAX)),
        );
        " HAVING uniqExact(term) >= {search_min_matched_terms:UInt64}"
    } else {
        ""
    }
}

fn search_require_all_terms_having(
    request: &SearchQueryRequest,
    term_count: usize,
    parameters: &mut Map<String, Value>,
) -> &'static str {
    if request.require_all_terms {
        parameters.insert(
            "search_min_matched_terms".to_string(),
            Value::from(u64::try_from(term_count).unwrap_or(u64::MAX)),
        );
        " HAVING uniqExact(query_term) >= {search_min_matched_terms:UInt64}"
    } else {
        ""
    }
}

fn token_search_query_sql(
    where_clause: &str,
    having_clause: &str,
    snippet_expr: &str,
    include_snippets: bool,
) -> String {
    let doc_join = if include_snippets {
        " LEFT ANY JOIN event_text_index AS doc ON doc.event_id = search.event_id"
    } else {
        ""
    };
    format!(
        "SELECT e.event_id AS event_id, e.timestamp AS timestamp, e.event_type AS event_type, e.trace_id AS trace_id, e.span_id AS span_id, e.signal AS signal, search.score AS score, search.matched_terms AS matched_terms, search.matched_paths AS matched_paths, {snippet_expr} AS snippet, e.data AS data FROM (SELECT event_id, sum(toUInt64(weight)) AS score, groupUniqArray(term) AS matched_terms, groupUniqArray(path) AS matched_paths FROM event_search_terms WHERE {where_clause} GROUP BY event_id{having_clause} ORDER BY score DESC, event_id ASC LIMIT {{search_inner_limit:UInt64}}) AS search INNER JOIN events AS e ON e.event_id = search.event_id{doc_join} ORDER BY score DESC, timestamp DESC, event_id ASC LIMIT {{limit:UInt64}} OFFSET {{offset:UInt64}}"
    )
}

fn indexed_search_query_sql(
    where_clause: &str,
    query_term_expr: &str,
    score_expr: &str,
    having_clause: &str,
    snippet_expr: &str,
    include_snippets: bool,
) -> String {
    let doc_join = if include_snippets {
        " LEFT ANY JOIN event_text_index AS doc ON doc.event_id = search.event_id"
    } else {
        ""
    };
    format!(
        "SELECT e.event_id AS event_id, e.timestamp AS timestamp, e.event_type AS event_type, e.trace_id AS trace_id, e.span_id AS span_id, e.signal AS signal, search.score AS score, search.matched_terms AS matched_terms, search.matched_paths AS matched_paths, {snippet_expr} AS snippet, e.data AS data FROM (SELECT event_id, sum(match_score) AS score, groupUniqArray(term) AS matched_terms, groupUniqArray(path) AS matched_paths FROM (SELECT event_id, term, path, {query_term_expr} AS query_term, {score_expr} AS match_score FROM event_search_terms WHERE {where_clause}) WHERE query_term != '' GROUP BY event_id{having_clause} ORDER BY score DESC, event_id ASC LIMIT {{search_inner_limit:UInt64}}) AS search INNER JOIN events AS e ON e.event_id = search.event_id{doc_join} ORDER BY score DESC, timestamp DESC, event_id ASC LIMIT {{limit:UInt64}} OFFSET {{offset:UInt64}}"
    )
}

fn phrase_search_query_sql(where_clause: &str, snippet_expr: &str) -> String {
    format!(
        "SELECT e.event_id AS event_id, e.timestamp AS timestamp, e.event_type AS event_type, e.trace_id AS trace_id, e.span_id AS span_id, e.signal AS signal, search.score AS score, [{{search_phrase:String}}] AS matched_terms, [] AS matched_paths, {snippet_expr} AS snippet, e.data AS data FROM (SELECT event_id, timestamp, event_type, text, 1000000 AS score FROM event_text_index AS search WHERE {where_clause} ORDER BY timestamp DESC, event_id ASC LIMIT {{search_inner_limit:UInt64}}) AS search INNER JOIN events AS e ON e.event_id = search.event_id ORDER BY score DESC, timestamp DESC, event_id ASC LIMIT {{limit:UInt64}} OFFSET {{offset:UInt64}}"
    )
}

fn prefix_search_where_clause(terms: &[String]) -> String {
    parenthesized_sql(
        terms
            .iter()
            .map(|term| format!("startsWith(term, {})", quote_sql_string(term)))
            .collect::<Vec<_>>()
            .join(" OR "),
    )
}

fn prefix_search_match_expr(terms: &[String]) -> String {
    search_match_expr(terms, |term| {
        format!("startsWith(term, {})", quote_sql_string(term))
    })
}

fn fuzzy_search_where_clause(terms: &[String]) -> String {
    parenthesized_sql(
        terms
            .iter()
            .map(|term| {
                let distance = fuzzy_term_distance(term);
                let len = term.len();
                let min_len = len.saturating_sub(distance);
                let max_len = len.saturating_add(distance);
                format!(
                    "(length(term) BETWEEN {min_len} AND {max_len} AND editDistance(term, {}) <= {distance})",
                    quote_sql_string(term)
                )
            })
            .collect::<Vec<_>>()
            .join(" OR "),
    )
}

fn fuzzy_search_match_expr(terms: &[String]) -> String {
    search_match_expr(terms, |term| {
        let distance = fuzzy_term_distance(term);
        format!(
            "editDistance(term, {}) <= {distance}",
            quote_sql_string(term)
        )
    })
}

fn fuzzy_search_score_expr(terms: &[String]) -> String {
    let mut args = Vec::new();
    for term in terms {
        let distance = fuzzy_term_distance(term);
        let quoted = quote_sql_string(term);
        args.push(format!("editDistance(term, {quoted}) <= {distance}"));
        args.push(format!(
            "toUInt64(greatest(toInt64(weight) * 100 - toInt64(editDistance(term, {quoted})) * 40, 1))"
        ));
    }
    args.push("toUInt64(weight)".to_string());
    format!("multiIf({})", args.join(", "))
}

fn search_match_expr(terms: &[String], predicate: impl Fn(&str) -> String) -> String {
    let mut args = Vec::new();
    for term in terms {
        args.push(predicate(term));
        args.push(quote_sql_string(term));
    }
    args.push("''".to_string());
    format!("multiIf({})", args.join(", "))
}

fn fuzzy_term_distance(term: &str) -> usize {
    if term.len() <= 4 { 1 } else { 2 }
}

fn parenthesized_sql(value: String) -> String {
    format!("({value})")
}

fn search_query_terms(query: &str) -> Vec<String> {
    let mut terms = BTreeSet::new();
    let mut token = String::new();
    for ch in query.chars() {
        if ch.is_ascii_alphanumeric() {
            token.push(ch.to_ascii_lowercase());
            if token.len() >= 64 {
                terms.insert(token.clone());
                token.clear();
            }
        } else if token.len() >= 2 {
            terms.insert(std::mem::take(&mut token));
        } else {
            token.clear();
        }
    }
    if token.len() >= 2 {
        terms.insert(token);
    }
    terms.into_iter().take(16).collect()
}

#[allow(dead_code)]
fn json_scalar_string(value: &Value) -> String {
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
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[allow(dead_code)]
fn validate_dimension_name(value: &str) -> Result<(), ReadError> {
    let valid = !value.trim().is_empty()
        && value.len() <= 128
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'));
    if valid {
        Ok(())
    } else {
        Err(ReadError::InvalidQuery(format!(
            "invalid dimension name: {value}"
        )))
    }
}

#[allow(dead_code)]
fn validate_definition_id(value: &str) -> Result<(), ReadError> {
    let valid = !value.trim().is_empty()
        && value.len() <= 256
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'));
    if valid {
        Ok(())
    } else {
        Err(ReadError::InvalidQuery("invalid definitionId".to_string()))
    }
}

#[allow(dead_code)]
fn validate_alert_filter_value(value: &str, label: &str) -> Result<(), ReadError> {
    let valid =
        !value.trim().is_empty() && value.len() <= 512 && value.chars().all(|ch| !ch.is_control());
    if valid {
        Ok(())
    } else {
        Err(ReadError::InvalidQuery(format!("invalid alert {label}")))
    }
}

#[allow(dead_code)]
fn measure_cube_outputs(config: &Value) -> Vec<&Map<String, Value>> {
    config
        .get("outputs")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|outputs| outputs.iter())
        .filter_map(Value::as_object)
        .filter(|output| {
            output
                .get("target")
                .and_then(Value::as_str)
                .is_some_and(|target| target == "measure_cube_rollups")
        })
        .collect()
}

#[allow(dead_code)]
fn measure_output_matches(
    row: &MeasureDefinitionCatalogRow,
    output: &Map<String, Value>,
    request: &MeasureQueryRequest,
) -> bool {
    if !request.definition_id.trim().is_empty() && row.definition_id != request.definition_id {
        return false;
    }
    match output.get("measure_name") {
        Some(Value::String(value)) => value == &request.measure_name,
        Some(Value::Object(_)) => !request.definition_id.trim().is_empty(),
        _ => row.name == request.measure_name,
    }
}

#[derive(Debug, Deserialize)]
struct ClickHouseResponse<T> {
    data: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct QueryRecommendationRow {
    query_id: String,
    query_hash: u64,
    query_shape: String,
    surface: String,
    plan_kind: String,
    shape_class: String,
    source_tables: Vec<String>,
    filter_paths: Vec<String>,
    group_by_paths: Vec<String>,
    time_range_start: Option<String>,
    time_range_end: Option<String>,
    result_rows: u64,
    read_rows: u64,
    read_bytes: u64,
    elapsed_ms: u64,
    recommendations_json: String,
    observed_at: String,
}

impl QueryRecommendationRecord {
    fn from_row(row: QueryRecommendationRow) -> Option<Self> {
        let recommendations =
            serde_json::from_str::<Vec<Value>>(&row.recommendations_json).unwrap_or_default();
        if recommendations.is_empty() {
            return None;
        }
        Some(Self {
            query_id: row.query_id,
            query_hash: row.query_hash,
            query_shape: row.query_shape,
            surface: row.surface,
            plan_kind: row.plan_kind,
            shape_class: row.shape_class,
            source_tables: row.source_tables,
            filter_paths: row.filter_paths,
            group_by_paths: row.group_by_paths,
            time_range_start: row.time_range_start,
            time_range_end: row.time_range_end,
            result_rows: row.result_rows,
            read_rows: row.read_rows,
            read_bytes: row.read_bytes,
            elapsed_ms: row.elapsed_ms,
            recommendations,
            observed_at: row.observed_at,
        })
    }
}

struct MaterializedOutputTarget<'a> {
    target_type: &'static str,
    target_id: &'a str,
    requested_version: u64,
    serving_table: &'static str,
    id_column: &'static str,
    version_column: &'static str,
}

struct VersionSelection {
    version: u64,
    selector: &'static str,
}

struct MaterializationSelectionMetadata<'a> {
    target_type: &'static str,
    target_id: &'a str,
    target_version: u64,
    selector: &'static str,
}

#[derive(Debug, Deserialize)]
struct MaterializationVersionRow {
    target_version: u64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct MeasureDefinitionCatalogRow {
    definition_id: String,
    name: String,
    version: u64,
    config: Value,
}

#[derive(Debug)]
#[allow(dead_code)]
struct ResolvedMeasureDimensionSet {
    definition_id: String,
    definition_version: u64,
    id: String,
    dimension_names: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct AvailableMeasureDimensionSet {
    definition_id: String,
    definition_version: u64,
    id: String,
    dimension_names: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct DefinitionFieldCatalogRow {
    definition_id: String,
    name: String,
    version: u64,
    config: Value,
}

#[derive(Debug, Deserialize)]
struct IndexedFieldPathRow {
    field_name: String,
    definition_id: String,
    definition_version: u64,
}

#[derive(Debug, Deserialize)]
struct MaterializedDefinitionRow {
    target_id: String,
    target_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DefinitionVersionKey {
    definition_id: String,
    version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct IndexedFieldPath {
    path: String,
    definition_id: String,
    version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MaterializationWindow {
    from: String,
    to: String,
}

impl MaterializationWindow {
    fn from_request(request: &EventsQueryRequest) -> Self {
        let now = Utc::now();
        if !request.filter.created_after.is_empty() || !request.filter.created_before.is_empty() {
            return Self {
                from: if request.filter.created_after.is_empty() {
                    "1970-01-01 00:00:00.000".to_string()
                } else {
                    clickhouse_datetime64(&request.filter.created_after)
                },
                to: if request.filter.created_before.is_empty() {
                    clickhouse_datetime64_from_utc(now)
                } else {
                    clickhouse_datetime64(&request.filter.created_before)
                },
            };
        }

        if let Some(time_range) = request.time_range.as_ref() {
            if time_range.lookback_minutes > 0 {
                return Self {
                    from: clickhouse_datetime64_from_utc(
                        now - ChronoDuration::minutes(time_range.lookback_minutes as i64),
                    ),
                    to: clickhouse_datetime64_from_utc(now),
                };
            }
            return Self {
                from: if time_range.created_after.is_empty() {
                    "1970-01-01 00:00:00.000".to_string()
                } else {
                    clickhouse_datetime64(&time_range.created_after)
                },
                to: if time_range.created_before.is_empty() {
                    clickhouse_datetime64_from_utc(now)
                } else {
                    clickhouse_datetime64(&time_range.created_before)
                },
            };
        }

        Self {
            from: "1970-01-01 00:00:00.000".to_string(),
            to: clickhouse_datetime64_from_utc(now),
        }
    }
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
    surface: &'static str,
    plan_kind: &'static str,
    shape_class: &'static str,
    source_tables: &'a [String],
    parameter_types: &'a Value,
    elapsed_ms: u64,
    result_rows: u64,
    read_rows: u64,
    read_bytes: u64,
    status: &'static str,
    allow_stale_serving: bool,
    metadata: QueryPlanningMetadata,
    recommendations: Vec<Value>,
}

#[derive(Serialize)]
struct QueryUsageRow<'a> {
    tenant_id: &'a str,
    query_id: String,
    query_hash: u64,
    query_shape: &'a str,
    surface: &'a str,
    plan_kind: &'a str,
    is_raw_fallback: u8,
    source_tables: &'a [String],
    json_paths: &'a [String],
    filter_paths: &'a [String],
    group_by_paths: &'a [String],
    time_range_start: &'a Option<String>,
    time_range_end: &'a Option<String>,
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

#[derive(Clone, Debug, Default)]
struct QueryPlanningMetadata {
    event_view: Option<&'static str>,
    filter_routes: Vec<Value>,
    filter_paths: Vec<String>,
    group_by_paths: Vec<String>,
    json_paths: Vec<String>,
    time_range_start: Option<String>,
    time_range_end: Option<String>,
    text_search: Option<Value>,
}

struct QueryExplanation {
    surface: &'static str,
    plan_kind: &'static str,
    shape_class: &'static str,
    source_tables: Vec<String>,
    allow_stale_serving: bool,
    freshness_overrides: Vec<String>,
    tenant_scope: &'static str,
    recommendations: Vec<Value>,
}

fn attach_query_explanation(response: &mut Value, explanation: QueryExplanation) {
    let Some(object) = response.as_object_mut() else {
        return;
    };
    object.insert(
        "nanotrace".to_string(),
        serde_json::json!({
            "query": {
                "surface": explanation.surface,
                "planKind": explanation.plan_kind,
                "shapeClass": explanation.shape_class,
                "sourceTables": explanation.source_tables,
                "allowStaleServing": explanation.allow_stale_serving,
                "freshnessOverrides": explanation.freshness_overrides,
                "tenantScope": explanation.tenant_scope,
                "recommendations": explanation.recommendations,
            }
        }),
    );
}

fn attach_event_filter_explanation(
    response: &mut Value,
    request: &EventsQueryRequest,
    catalog: &EventFieldCatalog,
) {
    let Ok(event_filters) = event_filter_explanations(request, catalog) else {
        return;
    };
    if event_filters.is_empty() {
        return;
    }
    let Some(object) = response.as_object_mut() else {
        return;
    };
    let nanotrace = object
        .entry("nanotrace")
        .or_insert_with(|| serde_json::json!({}));
    let Some(nanotrace_object) = nanotrace.as_object_mut() else {
        return;
    };
    let query = nanotrace_object
        .entry("query")
        .or_insert_with(|| serde_json::json!({}));
    let Some(query_object) = query.as_object_mut() else {
        return;
    };
    query_object.insert("eventFilters".to_string(), Value::Array(event_filters));
}

fn event_filter_explanations(
    request: &EventsQueryRequest,
    catalog: &EventFieldCatalog,
) -> Result<Vec<Value>, ReadError> {
    let mut explanations = Vec::new();
    if !request.group_by.trim().is_empty() && !request.selected_group_value.is_empty() {
        let path = facet_key(&request.group_by)?;
        explanations.push(event_filter_explanation(
            "selected_group",
            &path,
            EventFacetOperator::Eq,
            false,
            None,
            event_filter_route_for_path(catalog, &path, EventFacetOperator::Eq, false, None),
        ));
    }
    for facet in &request.filter.facets {
        let path = facet_key(&facet.path)?;
        let scope = facet_scope(facet, &path)?;
        explanations.push(event_filter_explanation(
            "facet",
            &path,
            facet.operator,
            facet.negated,
            scope.as_deref(),
            event_filter_route_for_path(
                catalog,
                &path,
                facet.operator,
                facet.negated,
                scope.as_deref(),
            ),
        ));
    }
    if !request.filter.text.trim().is_empty() {
        explanations.push(serde_json::json!({
            "role": "text",
            "path": "event.text",
            "operator": "contains",
            "route": "event_text_index",
            "strategy": "text_index",
        }));
    }
    Ok(explanations)
}

fn event_filter_explanation(
    role: &'static str,
    path: &str,
    operator: EventFacetOperator,
    negated: bool,
    scope: Option<&str>,
    route: EventFilterRoute,
) -> Value {
    let mut value = serde_json::json!({
        "role": role,
        "path": path,
        "operator": event_facet_operator_name(operator),
        "route": route.table,
        "strategy": route.strategy,
    });
    if negated {
        value["negated"] = Value::Bool(true);
    }
    if let Some(scope) = scope {
        value["scope"] = Value::String(scope.to_string());
    }
    value
}

struct EventFilterRoute {
    table: &'static str,
    strategy: &'static str,
}

fn event_filter_route_for_path(
    catalog: &EventFieldCatalog,
    path: &str,
    operator: EventFacetOperator,
    negated: bool,
    scope: Option<&str>,
) -> EventFilterRoute {
    if scope.is_none()
        && !path.contains("[]")
        && !negated
        && matches!(operator, EventFacetOperator::Eq | EventFacetOperator::In)
        && let Some(index_access) = catalog.index_table(path)
    {
        return EventFilterRoute {
            table: index_access.table_name(),
            strategy: match &index_access {
                IndexAccess::FieldValues => "lookup_index",
                IndexAccess::FieldIndex(_) => "promoted_index",
            },
        };
    }
    if kv_index_operator(operator) {
        return EventFilterRoute {
            table: "event_kv_index",
            strategy: if scope.is_some() {
                "scoped_kv_index"
            } else {
                "kv_index"
            },
        };
    }
    EventFilterRoute {
        table: "events",
        strategy: "raw_expression",
    }
}

fn event_facet_operator_name(operator: EventFacetOperator) -> &'static str {
    match operator {
        EventFacetOperator::Contains => "contains",
        EventFacetOperator::Eq => "eq",
        EventFacetOperator::Exists => "exists",
        EventFacetOperator::Gt => "gt",
        EventFacetOperator::Gte => "gte",
        EventFacetOperator::In => "in",
        EventFacetOperator::Lt => "lt",
        EventFacetOperator::Lte => "lte",
    }
}

fn event_query_planning_metadata(
    request: &EventsQueryRequest,
    catalog: &EventFieldCatalog,
) -> QueryPlanningMetadata {
    let filter_routes = event_filter_explanations(request, catalog).unwrap_or_default();
    let mut filter_paths = Vec::new();
    if !request.group_by.trim().is_empty()
        && !request.selected_group_value.is_empty()
        && let Ok(path) = facet_key(&request.group_by)
    {
        filter_paths.push(path);
    }
    for facet in &request.filter.facets {
        if let Ok(path) = facet_key(&facet.path) {
            filter_paths.push(path);
        }
    }
    if !request.filter.text.trim().is_empty() {
        filter_paths.push("event.text".to_string());
    }

    let group_by_paths = if request.group_by.trim().is_empty() {
        Vec::new()
    } else {
        facet_key(&request.group_by).into_iter().collect()
    };
    let json_paths = unique_strings(
        filter_paths
            .iter()
            .chain(group_by_paths.iter())
            .filter(|path| path.as_str() != "event.text")
            .cloned()
            .collect(),
    );
    let (time_range_start, time_range_end) = event_time_range_metadata(request);
    QueryPlanningMetadata {
        event_view: Some(event_query_view_name(request.view)),
        filter_routes,
        filter_paths: unique_strings(filter_paths),
        group_by_paths,
        json_paths,
        time_range_start,
        time_range_end,
        text_search: if request.filter.text.trim().is_empty() {
            None
        } else {
            Some(serde_json::json!({
                "mode": "contains",
                "queryLength": request.filter.text.chars().count(),
                "source": "event_text_index",
            }))
        },
    }
}

fn token_search_planning_metadata(
    request: &SearchQueryRequest,
    terms: &[String],
) -> QueryPlanningMetadata {
    indexed_search_planning_metadata(request, terms, "token")
}

fn indexed_search_planning_metadata(
    request: &SearchQueryRequest,
    terms: &[String],
    mode: &'static str,
) -> QueryPlanningMetadata {
    let (time_range_start, time_range_end) =
        explicit_time_range_metadata(&request.from, &request.to);
    QueryPlanningMetadata {
        filter_paths: if request.event_type.trim().is_empty() {
            Vec::new()
        } else {
            vec!["event_type".to_string()]
        },
        time_range_start,
        time_range_end,
        text_search: Some(serde_json::json!({
            "mode": mode,
            "termCount": terms.len(),
            "requireAllTerms": request.require_all_terms,
            "includeSnippets": request.include_snippets,
            "source": if request.include_snippets {
                "event_search_terms+event_text_index"
            } else {
                "event_search_terms"
            },
        })),
        ..Default::default()
    }
}

fn phrase_search_planning_metadata(request: &SearchQueryRequest) -> QueryPlanningMetadata {
    let (time_range_start, time_range_end) =
        explicit_time_range_metadata(&request.from, &request.to);
    QueryPlanningMetadata {
        filter_paths: if request.event_type.trim().is_empty() {
            Vec::new()
        } else {
            vec!["event_type".to_string()]
        },
        time_range_start,
        time_range_end,
        text_search: Some(serde_json::json!({
            "mode": "phrase",
            "queryLength": request.query.trim().chars().count(),
            "includeSnippets": true,
            "source": "event_text_index",
        })),
        ..Default::default()
    }
}

fn event_time_range_metadata(request: &EventsQueryRequest) -> (Option<String>, Option<String>) {
    if request.filter.created_after.trim().is_empty()
        && request.filter.created_before.trim().is_empty()
        && request.time_range.is_none()
    {
        return (None, None);
    }
    let window = MaterializationWindow::from_request(request);
    (Some(window.from), Some(window.to))
}

fn explicit_time_range_metadata(from: &str, to: &str) -> (Option<String>, Option<String>) {
    (
        if from.trim().is_empty() {
            None
        } else {
            Some(clickhouse_datetime64(from))
        },
        if to.trim().is_empty() {
            None
        } else {
            Some(clickhouse_datetime64(to))
        },
    )
}

fn event_query_view_name(view: EventsQueryView) -> &'static str {
    match view {
        EventsQueryView::GroupOptions => "group_options",
        EventsQueryView::Groups => "groups",
        EventsQueryView::Latest => "latest",
        EventsQueryView::Summary => "summary",
        EventsQueryView::Events => "events",
        EventsQueryView::Density => "density",
        EventsQueryView::Flamegraph => "flamegraph",
        EventsQueryView::Event => "event",
    }
}

fn unique_strings(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn nanotrace_with_materialization(
    response: &Value,
    materialization: MaterializationSelectionMetadata<'_>,
) -> Value {
    let mut nanotrace = response.get("nanotrace").cloned().unwrap_or_else(|| {
        serde_json::json!({
            "query": {}
        })
    });
    if let Some(object) = nanotrace.as_object_mut() {
        object.insert(
            "materialization".to_string(),
            serde_json::json!({
                "targetType": materialization.target_type,
                "targetId": materialization.target_id,
                "targetVersion": materialization.target_version,
                "selector": materialization.selector,
            }),
        );
    }
    nanotrace
}

fn query_shape_class(
    context: QueryContext,
    plan_kind: &str,
    metadata: &QueryPlanningMetadata,
) -> &'static str {
    match context.surface {
        "measure" => "numeric_measure",
        "state" => "entity_state",
        "report" => "report",
        "funnel" => "sequence_funnel",
        "cohort" => "cohort_retention",
        "search" => "search",
        "admin_sql" => "global_admin_rollup",
        "events" if metadata.text_search.is_some() => "search",
        "events" if !metadata.group_by_paths.is_empty() => "field_facet",
        "events" if !metadata.filter_paths.is_empty() => "exact_lookup",
        _ if matches!(plan_kind, "measure_rollup") => "numeric_measure",
        _ if matches!(plan_kind, "state_snapshot" | "state_updates") => "entity_state",
        _ if matches!(plan_kind, "report_result") => "report",
        _ if matches!(plan_kind, "sequence_report_result") => "sequence_funnel",
        _ if matches!(plan_kind, "cohort_membership") => "cohort_retention",
        _ if matches!(
            plan_kind,
            "search_terms"
                | "search_terms_join"
                | "search_terms_text_join"
                | "text_index"
                | "text_index_join"
                | "raw_text_scan"
        ) =>
        {
            "search"
        }
        _ if is_raw_fallback_plan(plan_kind) => "unsupported",
        _ => "exact_lookup",
    }
}

fn query_recommendations(
    context: QueryContext,
    plan_kind: &str,
    sources: &[String],
    metadata: &QueryPlanningMetadata,
) -> Vec<Value> {
    let mut recommendations = Vec::new();
    let mut seen = BTreeSet::new();

    if plan_kind == "raw_text_scan" {
        push_unique_recommendation(
            &mut recommendations,
            &mut seen,
            serde_json::json!({
                "kind": "route",
                "targetType": "search",
                "targetTable": "event_search_terms",
                "reason": "query scans raw event JSON with text matching",
                "action": "use_search_query_or_event_text_filter",
            }),
        );
    }

    for route in &metadata.filter_routes {
        let path = route.get("path").and_then(Value::as_str).unwrap_or("");
        let strategy = route.get("strategy").and_then(Value::as_str).unwrap_or("");
        let role = route.get("role").and_then(Value::as_str).unwrap_or("facet");
        match strategy {
            "raw_expression" if !path.is_empty() => {
                let operator = route.get("operator").and_then(Value::as_str).unwrap_or("");
                let target_type = if operator == "contains" {
                    "search"
                } else {
                    "field"
                };
                let target_table = if operator == "contains" {
                    "event_search_terms"
                } else {
                    "field_index"
                };
                push_unique_recommendation(
                    &mut recommendations,
                    &mut seen,
                    serde_json::json!({
                        "kind": "promotion",
                        "targetType": target_type,
                        "targetTable": target_table,
                        "path": path,
                        "reason": "event filter uses a raw expression",
                        "source": role,
                    }),
                );
                if event_filter_operator_suggests_measure(operator) {
                    push_unique_recommendation(
                        &mut recommendations,
                        &mut seen,
                        serde_json::json!({
                            "kind": "promotion",
                            "targetType": "measure",
                            "targetTable": "event_measures",
                            "path": path,
                            "reason": "numeric event filter should become an explicit measure when reused",
                            "source": role,
                            "operator": operator,
                        }),
                    );
                }
            }
            "kv_index" | "scoped_kv_index" if !path.is_empty() => {
                let operator = route.get("operator").and_then(Value::as_str).unwrap_or("");
                push_unique_recommendation(
                    &mut recommendations,
                    &mut seen,
                    serde_json::json!({
                        "kind": "promotion",
                        "targetType": "field",
                        "targetTable": "field_index",
                        "path": path,
                        "reason": "event filter uses the generic KV index",
                        "source": role,
                    }),
                );
                if event_filter_operator_suggests_measure(operator) {
                    push_unique_recommendation(
                        &mut recommendations,
                        &mut seen,
                        serde_json::json!({
                            "kind": "promotion",
                            "targetType": "measure",
                            "targetTable": "event_measures",
                            "path": path,
                            "reason": "numeric event filter uses the generic KV index and should become an explicit measure when reused",
                            "source": role,
                            "operator": operator,
                        }),
                    );
                }
            }
            _ => {}
        }
    }

    if context.surface == "events"
        && !metadata.group_by_paths.is_empty()
        && matches!(
            plan_kind,
            "raw_events" | "raw_text_scan" | "table_scan" | "kv_index_join" | "mixed"
        )
    {
        push_unique_recommendation(
            &mut recommendations,
            &mut seen,
            serde_json::json!({
                "kind": "materialization",
                "targetType": "report",
                "targetTable": "report_results",
                "groupBy": metadata.group_by_paths,
                "reason": "grouped event query should become an explicit report when repeated or expensive",
                "source": metadata.event_view.unwrap_or("events"),
            }),
        );
    }

    if context.surface == "sql"
        && is_raw_fallback_plan(plan_kind)
        && normalized_source_names(sources).contains(&"events")
    {
        push_unique_recommendation(
            &mut recommendations,
            &mut seen,
            serde_json::json!({
                "kind": "materialization",
                "targetType": "report",
                "targetTable": "report_results",
                "reason": "raw SQL over events should be promoted to a typed report, measure, state, cohort, or field definition before it becomes a product surface",
            }),
        );
    }

    recommendations
}

fn event_filter_operator_suggests_measure(operator: &str) -> bool {
    matches!(operator, "gt" | "gte" | "lt" | "lte")
}

fn push_unique_recommendation(
    recommendations: &mut Vec<Value>,
    seen: &mut BTreeSet<String>,
    recommendation: Value,
) {
    let key = serde_json::to_string(&recommendation).unwrap_or_default();
    if seen.insert(key) {
        recommendations.push(recommendation);
    }
}

fn query_plan_kind(query: &str, sources: &[String]) -> &'static str {
    let source_names = normalized_source_names(sources);
    if source_names.is_empty() {
        return "constants";
    }

    if source_names.len() > 1 {
        if source_names.contains(&"events") && source_names.contains(&"event_search_terms") {
            if source_names.contains(&"event_text_index") {
                return "search_terms_text_join";
            }
            return "search_terms_join";
        }
        if source_names.contains(&"events") && source_names.contains(&"event_text_index") {
            return "text_index_join";
        }
        if source_names.contains(&"events") && source_names.contains(&"field_index") {
            return "promoted_index_join";
        }
        if source_names.contains(&"events") && source_names.contains(&"field_values") {
            return "lookup_index_join";
        }
        if source_names.contains(&"events") && source_names.contains(&"event_kv_index") {
            return "kv_index_join";
        }
        return "mixed";
    }

    let Some(source) = source_names.first().copied() else {
        return "constants";
    };
    match source {
        "events" if query_uses_raw_text_scan(query) => "raw_text_scan",
        "events" => "raw_events",
        "event_text_index" => "text_index",
        "event_search_terms" => "search_terms",
        "event_kv_index" => "kv_index",
        "field_index" => "promoted_index",
        "field_values" => "lookup_index",
        "field_rollups" | "event_density_1s" => "event_rollup",
        "event_measures"
        | "measure_rollups"
        | "measure_cube_points"
        | "measure_cube_rollups"
        | "counter_rollups"
        | "gauge_rollups"
        | "histogram_rollups" => "measure_rollup",
        "report_results" => "report_result",
        "sequence_report_results" => "sequence_report_result",
        "cohort_memberships" => "cohort_membership",
        "alert_events" => "alert_event",
        "alert_notifications" => "alert_notification",
        "entity_state_current" => "state_snapshot",
        "entity_state_updates" => "state_updates",
        "definitions" | "definition_stats" => "catalog",
        "query_usage" => "query_usage",
        "materialization_jobs"
        | "materialization_chunks"
        | "materialization_versions"
        | "materialization_watermarks"
        | "pipeline_metrics"
        | "lakehouse_commits"
        | "serving_watermarks" => "control_plane",
        _ => "table_scan",
    }
}

fn normalized_source_names(sources: &[String]) -> Vec<&str> {
    let names = sources
        .iter()
        .map(|source| normalized_source_name(source))
        .collect::<BTreeSet<_>>();
    names.into_iter().collect()
}

fn normalized_source_name(source: &str) -> &str {
    source
        .rsplit('.')
        .next()
        .unwrap_or(source)
        .trim_matches('`')
        .trim_matches('"')
}

fn query_uses_raw_text_scan(query: &str) -> bool {
    query
        .to_ascii_lowercase()
        .contains("positioncaseinsensitive")
}

fn is_raw_fallback_plan(plan_kind: &str) -> bool {
    matches!(plan_kind, "raw_events" | "raw_text_scan" | "table_scan")
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
        if ch == '{'
            && let Some(end) = parameter_end(&chars, index)
        {
            let raw = chars[index + 1..end].iter().collect::<String>();
            tokens.push(sanitize_parameter(&raw));
            index = end + 1;
            continue;
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
        ReadError::NotFound => "not_found",
        ReadError::EventIDMismatch => "event_id_mismatch",
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
    let source_refs = query_source_refs(query);
    validate_query_source_refs(&source_refs, allowed_tables)?;
    validate_parser_source_refs(query, &source_refs, allowed_tables)
}

fn validate_query_source_refs(
    sources: &[QuerySourceRef],
    allowed_tables: &[String],
) -> Result<(), ReadError> {
    for source in sources {
        if !source_is_allowed(&source.name, allowed_tables) {
            return Err(ReadError::InvalidQuery(format!(
                "query source is not allowed: {}",
                source.name
            )));
        }
    }
    Ok(())
}

fn query_sources(query: &str) -> Vec<String> {
    query_source_refs(query)
        .into_iter()
        .map(|source| source.name)
        .collect()
}

fn validate_parser_source_refs(
    query: &str,
    source_refs: &[QuerySourceRef],
    allowed_tables: &[String],
) -> Result<(), ReadError> {
    let parser_sources = parsed_query_sources(query)?;
    validate_query_source_refs(
        &parser_sources
            .iter()
            .map(|name| QuerySourceRef {
                name: name.clone(),
                start: 0,
                end: 0,
            })
            .collect::<Vec<_>>(),
        allowed_tables,
    )?;

    let scanner = source_refs
        .iter()
        .map(|source| source.name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let parser = parser_sources
        .iter()
        .map(|source| source.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    if scanner != parser {
        return Err(ReadError::InvalidQuery(format!(
            "query source parser mismatch: scanner=[{}] parser=[{}]",
            scanner.into_iter().collect::<Vec<_>>().join(", "),
            parser.into_iter().collect::<Vec<_>>().join(", ")
        )));
    }
    Ok(())
}

fn parsed_query_sources(query: &str) -> Result<Vec<String>, ReadError> {
    let parser_query = query_for_parser(query);
    let statements = Parser::parse_sql(&ClickHouseDialect {}, &parser_query).map_err(|err| {
        ReadError::InvalidQuery(format!("query could not be parsed safely: {err}"))
    })?;
    let mut visitor = SourceVisitor::default();
    if let std::ops::ControlFlow::Break(message) = statements.visit(&mut visitor) {
        return Err(ReadError::InvalidQuery(message));
    }
    Ok(visitor.sources)
}

#[derive(Default)]
struct SourceVisitor {
    sources: Vec<String>,
}

impl Visitor for SourceVisitor {
    type Break = String;

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &TableFactor,
    ) -> std::ops::ControlFlow<Self::Break> {
        match table_factor {
            TableFactor::Table { name, args, .. } => {
                if args.is_some() {
                    return std::ops::ControlFlow::Break(format!(
                        "table functions are not allowed in raw queries: {}",
                        object_name(name)
                    ));
                }
                self.sources.push(object_name(name));
                std::ops::ControlFlow::Continue(())
            }
            TableFactor::Derived { .. } | TableFactor::NestedJoin { .. } => {
                std::ops::ControlFlow::Continue(())
            }
            _ => std::ops::ControlFlow::Break(
                "raw queries may only read named tables, joins, and derived subqueries".to_string(),
            ),
        }
    }
}

fn object_name(name: &ObjectName) -> String {
    name.to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QuerySourceRef {
    name: String,
    start: usize,
    end: usize,
}

fn scope_query_with_allowed_tables<F>(
    query: &str,
    allowed_tables: &[String],
    mut scoped_table: F,
) -> Result<String, ReadError>
where
    F: FnMut(&str) -> String,
{
    let sources = query_source_refs(query);
    validate_query_source_refs(&sources, allowed_tables)?;
    let mut scoped = String::with_capacity(query.len() + sources.len() * 96);
    let mut last = 0;
    for source in sources {
        if source.start < last {
            continue;
        }
        scoped.push_str(&query[last..source.start]);
        scoped.push_str(&scoped_table(&source.name));
        last = source.end;
    }
    scoped.push_str(&query[last..]);
    Ok(scoped)
}

fn source_is_allowed(source: &str, allowed_tables: &[String]) -> bool {
    allowed_tables
        .iter()
        .any(|allowed| source.eq_ignore_ascii_case(allowed))
}

fn query_source_refs(query: &str) -> Vec<QuerySourceRef> {
    let code = sql_code(query);
    let mut sources = Vec::new();
    query_source_refs_in_code(&code, 0, &mut sources);
    sources
}

fn query_source_refs_in_code(code: &str, base_offset: usize, sources: &mut Vec<QuerySourceRef>) {
    let bytes = code.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let Some((keyword_start, keyword)) = next_source_keyword(&code[index..]) else {
            break;
        };
        index += keyword_start + keyword.len();
        index = collect_sources_after_keyword(code, index, base_offset, sources);
    }
}

fn collect_sources_after_keyword(
    code: &str,
    mut index: usize,
    base_offset: usize,
    sources: &mut Vec<QuerySourceRef>,
) -> usize {
    let bytes = code.as_bytes();
    loop {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index >= bytes.len() {
            return index;
        }
        if bytes[index] == b'(' {
            if let Some(close) = matching_paren(code, index) {
                query_source_refs_in_code(
                    &code[index + 1..close],
                    base_offset + index + 1,
                    sources,
                );
                index = skip_source_suffix(code, close + 1);
                if index < bytes.len() && bytes[index] == b',' {
                    index += 1;
                    continue;
                }
            }
            return index;
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
            return index;
        }
        sources.push(QuerySourceRef {
            name: source.to_string(),
            start: base_offset + source_start,
            end: base_offset + index,
        });
        index = skip_source_suffix(code, index);
        if index >= bytes.len() || bytes[index] != b',' {
            return index;
        }
        index += 1;
    }
}

fn skip_source_suffix(code: &str, mut index: usize) -> usize {
    let bytes = code.as_bytes();
    let mut depth = 0usize;
    while index < bytes.len() {
        if depth == 0
            && (bytes[index] == b','
                || SOURCE_TERMINATORS
                    .iter()
                    .any(|terminator| keyword_at(code, index, terminator)))
        {
            break;
        }
        match bytes[index] {
            b'(' => {
                depth += 1;
                index += 1;
            }
            b')' => {
                if depth == 0 {
                    break;
                }
                depth -= 1;
                index += 1;
            }
            _ => index += 1,
        }
    }
    index
}

fn matching_paren(code: &str, open: usize) -> Option<usize> {
    let bytes = code.as_bytes();
    if bytes.get(open) != Some(&b'(') {
        return None;
    }
    let mut depth = 0usize;
    for (index, byte) in bytes.iter().enumerate().skip(open) {
        match byte {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

const SOURCE_TERMINATORS: &[&str] = &[
    "WHERE",
    "PREWHERE",
    "JOIN",
    "ON",
    "USING",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "OFFSET",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "SETTINGS",
    "FORMAT",
    "QUALIFY",
    "WINDOW",
];

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
            push_masked_sql_char(&mut out, ch);
            push_masked_sql_char(&mut out, chars[i + 1]);
            i += 2;
            while i < chars.len() && chars[i] != '\n' {
                push_masked_sql_char(&mut out, chars[i]);
                i += 1;
            }
            continue;
        }
        if ch == '/' && chars.get(i + 1) == Some(&'*') {
            push_masked_sql_char(&mut out, ch);
            push_masked_sql_char(&mut out, chars[i + 1]);
            i += 2;
            while i < chars.len() {
                if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    push_masked_sql_char(&mut out, chars[i]);
                    push_masked_sql_char(&mut out, chars[i + 1]);
                    i += 2;
                    break;
                }
                push_masked_sql_char(&mut out, chars[i]);
                i += 1;
            }
            continue;
        }
        if ch == '\'' || ch == '"' || ch == '`' {
            let quote = ch;
            push_masked_sql_char(&mut out, ch);
            i += 1;
            while i < chars.len() {
                let current = chars[i];
                push_masked_sql_char(&mut out, current);
                if current == '\\' && quote != '`' && i + 1 < chars.len() {
                    i += 1;
                    push_masked_sql_char(&mut out, chars[i]);
                } else if current == quote {
                    if quote == '\'' && chars.get(i + 1) == Some(&'\'') {
                        i += 1;
                        push_masked_sql_char(&mut out, chars[i]);
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

fn push_masked_sql_char(out: &mut String, ch: char) {
    if ch == '\n' {
        out.push('\n');
        return;
    }
    for _ in 0..ch.len_utf8() {
        out.push(' ');
    }
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
        AlertQueryMode, CORE_ROLLUP_FIELD_NAMES, DefinitionVersionKey, EventFacetFilter,
        EventFacetOperator, EventFieldCatalog, EventFilter, EventPage, EventPredicatePlan,
        EventSortDirection, EventTimeRange, EventsQueryRequest, EventsQuerySort, EventsQueryView,
        IndexAccess, MaterializationSelectionMetadata, QueryApiRequest, QueryContext,
        QueryExplanation, RAW_GROUPABLE_FIELD_NAMES, ReadError, ReportQueryRequest, SearchMode,
        attach_query_explanation, checked_select_query, event_filter_explanations,
        event_page_filter, event_query_planning_metadata, event_table_order,
        event_value_expression, fuzzy_search_match_expr, fuzzy_search_score_expr,
        fuzzy_search_where_clause, group_options_query, grouped_index_query, grouped_rollup_query,
        indexed_search_query_sql, is_raw_fallback_plan, latest_grouped_index_query,
        latest_grouped_rollup_query, nanotrace_with_materialization, normalize_prewhere,
        phrase_search_query_sql, prefix_search_match_expr, prefix_search_where_clause,
        query_plan_kind, query_recommendations, query_shape_class, query_sources,
        query_usage_shape, raw_groups_query, scope_query_with_allowed_tables, search_query_terms,
        search_snippet_sql, token_search_query_sql, validate_parameter_name,
        validate_query_sources,
    };
    use serde_json::Value;

    fn definition_key(path: &str) -> DefinitionVersionKey {
        DefinitionVersionKey {
            definition_id: format!("def_{}", path.replace('.', "_")),
            version: 7,
        }
    }

    fn catalog_with_promoted(paths: &[&str]) -> EventFieldCatalog {
        let mut catalog = EventFieldCatalog::default();
        for path in paths {
            catalog.add_promoted((*path).to_string(), definition_key(path));
        }
        catalog
    }

    fn field_index_access(path: &str) -> IndexAccess {
        IndexAccess::FieldIndex(vec![definition_key(path)])
    }

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
    fn report_query_request_accepts_version_pin() {
        let request: ReportQueryRequest = serde_json::from_value(serde_json::json!({
            "reportId": "checkout_summary",
            "reportVersion": 42
        }))
        .unwrap();

        assert_eq!(request.report_id, "checkout_summary");
        assert_eq!(request.report_version, 42);
    }

    #[test]
    fn query_api_request_accepts_search_type() {
        let request: QueryApiRequest = serde_json::from_value(serde_json::json!({
            "type": "search",
            "query": "checkout timeout",
            "from": "2026-06-04T00:00:00Z",
            "limit": 25
        }))
        .unwrap();

        let QueryApiRequest::Search(request) = request else {
            panic!("expected search request");
        };
        assert_eq!(request.query, "checkout timeout");
        assert_eq!(request.from, "2026-06-04T00:00:00Z");
        assert_eq!(request.limit, 25);
        assert!(matches!(request.mode, SearchMode::Token));
        assert!(!request.require_all_terms);
        assert!(!request.include_snippets);
    }

    #[test]
    fn query_api_request_accepts_alerts_type() {
        let request: QueryApiRequest = serde_json::from_value(serde_json::json!({
            "type": "alerts",
            "mode": "notifications",
            "alertId": "payment_failed_hot",
            "eventId": "evt_123",
            "status": "retry",
            "limit": 25
        }))
        .unwrap();

        let QueryApiRequest::Alerts(request) = request else {
            panic!("expected alerts request");
        };
        assert_eq!(request.mode, AlertQueryMode::Notifications);
        assert_eq!(request.alert_id, "payment_failed_hot");
        assert_eq!(request.event_id, "evt_123");
        assert_eq!(request.status, "retry");
        assert_eq!(request.limit, 25);
    }

    #[test]
    fn query_api_request_accepts_advanced_search_options() {
        let request: QueryApiRequest = serde_json::from_value(serde_json::json!({
            "type": "search",
            "query": "checkout timeout",
            "mode": "phrase",
            "requireAllTerms": true,
            "includeSnippets": true
        }))
        .unwrap();

        let QueryApiRequest::Search(request) = request else {
            panic!("expected search request");
        };
        assert!(matches!(request.mode, SearchMode::Phrase));
        assert!(request.require_all_terms);
        assert!(request.include_snippets);
    }

    #[test]
    fn query_api_request_accepts_prefix_and_fuzzy_search_modes() {
        let request: QueryApiRequest = serde_json::from_value(serde_json::json!({
            "type": "search",
            "query": "chekout timeot",
            "mode": "fuzzy",
            "requireAllTerms": true
        }))
        .unwrap();

        let QueryApiRequest::Search(request) = request else {
            panic!("expected search request");
        };
        assert!(matches!(request.mode, SearchMode::Fuzzy));
        assert!(request.require_all_terms);

        let request: QueryApiRequest = serde_json::from_value(serde_json::json!({
            "type": "search",
            "query": "check time",
            "mode": "prefix"
        }))
        .unwrap();

        let QueryApiRequest::Search(request) = request else {
            panic!("expected search request");
        };
        assert!(matches!(request.mode, SearchMode::Prefix));
    }

    #[test]
    fn search_query_terms_tokenize_and_deduplicate() {
        assert_eq!(
            search_query_terms("Checkout timeout checkout gpt-5.5"),
            vec!["checkout", "gpt", "timeout"]
        );
        assert!(search_query_terms("x !").is_empty());
    }

    #[test]
    fn search_sql_supports_required_terms_and_snippets() {
        let snippet = search_snippet_sql("doc.text", "search_snippet_needle");
        let sql = token_search_query_sql(
            "term IN ('checkout', 'timeout')",
            " HAVING uniqExact(term) >= {search_min_matched_terms:UInt64}",
            &snippet,
            true,
        );

        assert!(sql.contains("FROM event_search_terms"));
        assert!(sql.contains("LEFT ANY JOIN event_text_index AS doc"));
        assert!(sql.contains("HAVING uniqExact(term) >= {search_min_matched_terms:UInt64}"));
        assert!(sql.contains("positionCaseInsensitive(doc.text, {search_snippet_needle:String})"));
        assert!(sql.contains(" AS snippet"));
    }

    #[test]
    fn default_token_search_sql_avoids_text_document_join() {
        let sql = token_search_query_sql("term IN ('checkout')", "", "''", false);

        assert!(sql.contains("FROM event_search_terms"));
        assert!(!sql.contains("event_text_index AS doc"));
        assert!(sql.contains("'' AS snippet"));
    }

    #[test]
    fn phrase_search_sql_uses_text_index_and_snippet() {
        let snippet = search_snippet_sql("search.text", "search_phrase");
        let sql = phrase_search_query_sql(
            "positionCaseInsensitive(search.text, {search_phrase:String}) > 0",
            &snippet,
        );

        assert!(sql.contains("FROM event_text_index AS search"));
        assert!(sql.contains("positionCaseInsensitive(search.text, {search_phrase:String}) > 0"));
        assert!(sql.contains("[{search_phrase:String}] AS matched_terms"));
        assert!(sql.contains(" AS snippet"));
    }

    #[test]
    fn prefix_search_sql_uses_term_prefixes_and_query_term_having() {
        let terms = vec!["check".to_string(), "time".to_string()];
        let sql = indexed_search_query_sql(
            &prefix_search_where_clause(&terms),
            &prefix_search_match_expr(&terms),
            "toUInt64(weight)",
            " HAVING uniqExact(query_term) >= {search_min_matched_terms:UInt64}",
            "''",
            false,
        );

        assert!(sql.contains("FROM event_search_terms"));
        assert!(sql.contains("startsWith(term, 'check')"));
        assert!(sql.contains("startsWith(term, 'time')"));
        assert!(sql.contains("multiIf(startsWith(term, 'check'), 'check'"));
        assert!(sql.contains("HAVING uniqExact(query_term) >= {search_min_matched_terms:UInt64}"));
        assert!(!sql.contains("event_text_index AS doc"));
    }

    #[test]
    fn fuzzy_search_sql_uses_bounded_edit_distance_and_snippet_join() {
        let terms = vec!["chekout".to_string(), "timeot".to_string()];
        let snippet = search_snippet_sql("doc.text", "search_snippet_needle");
        let sql = indexed_search_query_sql(
            &fuzzy_search_where_clause(&terms),
            &fuzzy_search_match_expr(&terms),
            &fuzzy_search_score_expr(&terms),
            "",
            &snippet,
            true,
        );

        assert!(sql.contains("FROM event_search_terms"));
        assert!(sql.contains("editDistance(term, 'chekout') <= 2"));
        assert!(sql.contains("length(term) BETWEEN 5 AND 9"));
        assert!(sql.contains("LEFT ANY JOIN event_text_index AS doc"));
        assert!(sql.contains("positionCaseInsensitive(doc.text, {search_snippet_needle:String})"));
        assert!(sql.contains("sum(match_score) AS score"));
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
    fn rejects_table_functions_even_when_name_is_allowed() {
        let allowed = vec!["numbers".to_string()];

        assert!(validate_query_sources("SELECT * FROM numbers(10)", &allowed).is_err());
    }

    #[test]
    fn query_sources_handle_spacing_comments_and_comma_sources() {
        let query = "SELECT e.event_id FROM\n /* tenant scoped */ events AS e, observatory.field_index fi WHERE e.event_id = fi.event_id";
        assert_eq!(
            query_sources(query),
            vec!["events".to_string(), "observatory.field_index".to_string()]
        );

        let scoped = scope_query_with_allowed_tables(
            query,
            &[
                "events".to_string(),
                "observatory.events".to_string(),
                "observatory.field_index".to_string(),
            ],
            |table| format!("<scoped:{table}>"),
        )
        .unwrap();
        assert!(scoped.contains("FROM\n /* tenant scoped */ <scoped:events> AS e"));
        assert!(scoped.contains(", <scoped:observatory.field_index> fi WHERE"));
    }

    #[test]
    fn query_scoping_preserves_offsets_after_non_ascii_literals() {
        let query = "SELECT 'café' AS label FROM\n events WHERE event_id != ''";
        let scoped = scope_query_with_allowed_tables(query, &["events".to_string()], |table| {
            format!("<scoped:{table}>")
        })
        .unwrap();

        assert_eq!(
            scoped,
            "SELECT 'café' AS label FROM\n <scoped:events> WHERE event_id != ''"
        );
    }

    #[test]
    fn query_sources_reach_inside_derived_tables_but_reject_cte_refs() {
        assert_eq!(
            query_sources(
                "SELECT * FROM (SELECT * FROM observatory.events) AS e, field_index fi WHERE e.event_id = fi.event_id"
            ),
            vec!["observatory.events".to_string(), "field_index".to_string()]
        );

        let allowed = vec!["events".to_string(), "observatory.events".to_string()];
        assert!(
            validate_query_sources(
                "WITH scoped AS (SELECT * FROM events) SELECT * FROM scoped",
                &allowed
            )
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
    fn query_plan_kind_classifies_serving_paths() {
        assert_eq!(
            query_plan_kind(
                "SELECT * FROM observatory.events WHERE positionCaseInsensitive(toJSONString(data), {text:String}) > 0",
                &["observatory.events".to_string()]
            ),
            "raw_text_scan"
        );
        assert_eq!(
            query_plan_kind(
                "SELECT * FROM events WHERE event_id IN (SELECT event_id FROM event_text_index)",
                &["events".to_string(), "event_text_index".to_string()]
            ),
            "text_index_join"
        );
        assert_eq!(
            query_plan_kind(
                "SELECT * FROM events JOIN event_search_terms USING event_id",
                &["events".to_string(), "event_search_terms".to_string()]
            ),
            "search_terms_join"
        );
        assert_eq!(
            query_plan_kind(
                "SELECT * FROM events JOIN event_search_terms USING event_id JOIN event_text_index USING event_id",
                &[
                    "events".to_string(),
                    "event_search_terms".to_string(),
                    "event_text_index".to_string()
                ]
            ),
            "search_terms_text_join"
        );
        assert_eq!(
            query_plan_kind(
                "SELECT count() FROM field_index WHERE field_name = 'service'",
                &["field_index".to_string()]
            ),
            "promoted_index"
        );
        assert_eq!(
            query_plan_kind(
                "SELECT * FROM events WHERE event_id IN (SELECT event_id FROM event_kv_index)",
                &["events".to_string(), "event_kv_index".to_string()]
            ),
            "kv_index_join"
        );
        assert_eq!(
            query_plan_kind(
                "SELECT bucket_time, metrics FROM report_results",
                &["report_results".to_string()]
            ),
            "report_result"
        );
        assert_eq!(
            query_plan_kind(
                "SELECT entity_id, argMax(value, timestamp) FROM entity_state_current GROUP BY entity_id",
                &["entity_state_current".to_string()]
            ),
            "state_snapshot"
        );
        assert_eq!(
            query_plan_kind(
                "SELECT entity_id, argMax(value, timestamp) FROM entity_state_updates GROUP BY entity_id",
                &["entity_state_updates".to_string()]
            ),
            "state_updates"
        );
    }

    #[test]
    fn event_filter_explanation_reports_filter_routes() {
        let request = EventsQueryRequest {
            filter: EventFilter {
                text: "timeout".to_string(),
                facets: vec![
                    EventFacetFilter {
                        path: "browser".to_string(),
                        operator: EventFacetOperator::Eq,
                        value: "Chrome".to_string(),
                        ..Default::default()
                    },
                    EventFacetFilter {
                        path: "duration_ms".to_string(),
                        operator: EventFacetOperator::Gt,
                        value: "250".to_string(),
                        ..Default::default()
                    },
                    EventFacetFilter {
                        path: "message".to_string(),
                        operator: EventFacetOperator::Contains,
                        value: "retry".to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            ..Default::default()
        };
        let explanations =
            event_filter_explanations(&request, &catalog_with_promoted(&["browser"])).unwrap();

        assert!(explanations.iter().any(|item| {
            item.get("path").and_then(Value::as_str) == Some("browser")
                && item.get("route").and_then(Value::as_str) == Some("field_index")
                && item.get("strategy").and_then(Value::as_str) == Some("promoted_index")
        }));
        assert!(explanations.iter().any(|item| {
            item.get("path").and_then(Value::as_str) == Some("duration_ms")
                && item.get("route").and_then(Value::as_str) == Some("event_kv_index")
        }));
        assert!(explanations.iter().any(|item| {
            item.get("path").and_then(Value::as_str) == Some("message")
                && item.get("strategy").and_then(Value::as_str) == Some("raw_expression")
        }));
        assert!(explanations.iter().any(|item| {
            item.get("role").and_then(Value::as_str) == Some("text")
                && item.get("route").and_then(Value::as_str) == Some("event_text_index")
        }));
    }

    #[test]
    fn event_planning_metadata_classifies_and_recommends_promotions() {
        let request = EventsQueryRequest {
            view: EventsQueryView::Groups,
            group_by: "browser".to_string(),
            filter: EventFilter {
                facets: vec![EventFacetFilter {
                    path: "duration_ms".to_string(),
                    operator: EventFacetOperator::Gt,
                    value: "250".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            time_range: Some(EventTimeRange {
                lookback_minutes: 60,
                ..Default::default()
            }),
            ..Default::default()
        };
        let metadata = event_query_planning_metadata(&request, &EventFieldCatalog::default());

        assert_eq!(metadata.event_view, Some("groups"));
        assert_eq!(metadata.group_by_paths, vec!["browser".to_string()]);
        assert!(metadata.filter_paths.contains(&"duration_ms".to_string()));
        assert!(metadata.time_range_start.is_some());

        let recommendations =
            query_recommendations(QueryContext::EVENTS, "kv_index_join", &[], &metadata);
        assert!(recommendations.iter().any(|item| {
            item.get("targetType").and_then(Value::as_str) == Some("field")
                && item.get("path").and_then(Value::as_str) == Some("duration_ms")
        }));
        assert!(recommendations.iter().any(|item| {
            item.get("targetType").and_then(Value::as_str) == Some("measure")
                && item.get("targetTable").and_then(Value::as_str) == Some("event_measures")
                && item.get("path").and_then(Value::as_str) == Some("duration_ms")
        }));
        assert!(recommendations.iter().any(|item| {
            item.get("targetType").and_then(Value::as_str) == Some("report")
                && item.get("targetTable").and_then(Value::as_str) == Some("report_results")
        }));
        assert_eq!(
            query_shape_class(QueryContext::EVENTS, "kv_index_join", &metadata),
            "field_facet"
        );
    }

    #[test]
    fn raw_fallback_classifier_marks_only_raw_scan_plans() {
        assert!(is_raw_fallback_plan("raw_events"));
        assert!(is_raw_fallback_plan("raw_text_scan"));
        assert!(is_raw_fallback_plan("table_scan"));
        assert!(!is_raw_fallback_plan("text_index_join"));
        assert!(!is_raw_fallback_plan("report_result"));
    }

    #[test]
    fn attach_query_explanation_adds_nanotrace_metadata() {
        let mut response = serde_json::json!({
            "meta": [],
            "data": [],
            "rows": 0,
        });
        attach_query_explanation(
            &mut response,
            QueryExplanation {
                surface: "events",
                plan_kind: "raw_text_scan",
                shape_class: "search",
                source_tables: vec!["events".to_string()],
                allow_stale_serving: true,
                freshness_overrides: vec!["field_index".to_string()],
                tenant_scope: "tenant",
                recommendations: vec![serde_json::json!({
                    "kind": "route",
                    "targetType": "search",
                })],
            },
        );

        assert_eq!(
            response.pointer("/nanotrace/query/surface"),
            Some(&serde_json::Value::String("events".to_string()))
        );
        assert_eq!(
            response.pointer("/nanotrace/query/planKind"),
            Some(&serde_json::Value::String("raw_text_scan".to_string()))
        );
        assert_eq!(
            response.pointer("/nanotrace/query/shapeClass"),
            Some(&serde_json::Value::String("search".to_string()))
        );
        assert_eq!(
            response.pointer("/nanotrace/query/sourceTables/0"),
            Some(&serde_json::Value::String("events".to_string()))
        );
        assert_eq!(
            response.pointer("/nanotrace/query/allowStaleServing"),
            Some(&serde_json::Value::Bool(true))
        );
        assert_eq!(
            response.pointer("/nanotrace/query/freshnessOverrides/0"),
            Some(&serde_json::Value::String("field_index".to_string()))
        );
        assert_eq!(
            response.pointer("/nanotrace/query/tenantScope"),
            Some(&serde_json::Value::String("tenant".to_string()))
        );
        assert_eq!(
            response.pointer("/nanotrace/query/recommendations/0/targetType"),
            Some(&serde_json::Value::String("search".to_string()))
        );
    }

    #[test]
    fn nanotrace_materialization_metadata_preserves_query_explanation() {
        let response = serde_json::json!({
            "nanotrace": {
                "query": {
                    "surface": "report",
                    "planKind": "report_result"
                }
            }
        });

        let nanotrace = nanotrace_with_materialization(
            &response,
            MaterializationSelectionMetadata {
                target_type: "report",
                target_id: "checkout_summary",
                target_version: 42,
                selector: "active_version",
            },
        );

        assert_eq!(
            nanotrace.pointer("/query/surface"),
            Some(&serde_json::Value::String("report".to_string()))
        );
        assert_eq!(
            nanotrace.pointer("/materialization/targetType"),
            Some(&serde_json::Value::String("report".to_string()))
        );
        assert_eq!(
            nanotrace.pointer("/materialization/targetVersion"),
            Some(&serde_json::Value::Number(serde_json::Number::from(42)))
        );
        assert_eq!(
            nanotrace.pointer("/materialization/selector"),
            Some(&serde_json::Value::String("active_version".to_string()))
        );
    }

    #[test]
    fn event_text_filter_uses_text_index_membership_clause() {
        let request = EventsQueryRequest {
            filter: EventFilter {
                text: "checkout timeout".to_string(),
                ..Default::default()
            },
            time_range: Some(EventTimeRange {
                lookback_minutes: 60,
                ..Default::default()
            }),
            ..Default::default()
        };

        let plan = EventPredicatePlan::new(&request, &EventFieldCatalog::default(), "e").unwrap();
        let sql = plan.where_clause();

        assert!(sql.contains("event_text_index AS search"));
        assert!(sql.contains("positionCaseInsensitive(search.text, {event_filter_"));
        assert!(sql.contains("search.timestamp >= now64(3) - toIntervalMinute"));
        assert!(!sql.contains("toJSONString(e.data)"));
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
        let catalog = catalog_with_promoted(&["service"]);
        let plan = EventPredicatePlan::new(&request, &catalog, "e").unwrap();
        let sql = plan.where_clause();
        assert!(sql.contains("e.event_id IN (SELECT idx.event_id FROM field_index AS idx"));
        assert!(sql.contains("idx.definition_id = {definition_id_"));
        assert!(sql.contains("idx.definition_version = {definition_version_"));
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

        let (query, parameters) =
            grouped_index_query(&request, "browser", field_index_access("browser"));

        assert!(query.contains("FROM field_index"));
        assert!(query.contains("field_name = {group_key:String}"));
        assert!(query.contains("mode IN ('facet', 'lookup')"));
        assert!(query.contains("definition_id = {definition_id_"));
        assert!(query.contains("definition_version = {definition_version_"));
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
            latest_grouped_index_query(&request, "browser", field_index_access("browser"));
        assert!(index_query.contains("FROM field_index"));
        assert!(index_query.contains("max(timestamp) AS lastCreatedAt"));
        assert!(index_query.contains("mode IN ('facet', 'lookup')"));
        assert!(index_query.contains("definition_id = {definition_id_"));
        assert!(index_query.contains("definition_version = {definition_version_"));
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
        let catalog = catalog_with_promoted(&["plan", "country", "account_tier"]);

        let plan = EventPredicatePlan::new(&request, &catalog, "e").unwrap();
        let sql = plan.where_clause();

        assert_eq!(sql.matches("field_index AS idx").count(), 3);
        assert_eq!(
            sql.matches("idx.timestamp >= now64(3) - toIntervalMinute")
                .count(),
            3
        );
        assert!(sql.contains("idx.mode IN ('facet', 'lookup')"));
        assert_eq!(
            sql.matches("idx.definition_id = {definition_id_").count(),
            3
        );
        assert_eq!(
            sql.matches("idx.definition_version = {definition_version_")
                .count(),
            3
        );
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
        let catalog = catalog_with_promoted(&["llm.model"]);

        let plan = EventPredicatePlan::new(&request, &catalog, "e").unwrap();
        let sql = plan.where_clause();

        assert_eq!(sql.matches("field_index AS idx").count(), 1);
        assert!(sql.contains("idx.value IN ({facet_value_0:String}, {facet_value_1:String})"));
        assert!(sql.contains("idx.definition_id = {definition_id_"));
        assert!(sql.contains("idx.definition_version = {definition_version_"));
        assert!(!sql.contains("e.data.llm.model"));
    }

    #[test]
    fn unmaterialized_definition_paths_filter_event_kv_index() {
        let request = EventsQueryRequest {
            filter: EventFilter {
                facets: vec![EventFacetFilter {
                    path: "plan".to_string(),
                    value: "pro".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..Default::default()
        };
        let catalog = EventFieldCatalog::default();

        let plan = EventPredicatePlan::new(&request, &catalog, "e").unwrap();
        let sql = plan.where_clause();

        assert!(!sql.contains("field_index AS idx"));
        assert!(sql.contains("event_kv_index AS kv"));
        assert!(sql.contains("kv.path = {kv_path_"));
        assert!(sql.contains("kv.value_type = 'string'"));
        assert!(sql.contains("kv.string_value = {kv_string_"));
        assert!(!sql.contains("e.data.plan"));
    }

    #[test]
    fn numeric_facet_filter_uses_event_kv_index_range_predicate() {
        let request = EventsQueryRequest {
            filter: EventFilter {
                facets: vec![EventFacetFilter {
                    operator: EventFacetOperator::Gte,
                    path: "latency_ms".to_string(),
                    value: "100".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..Default::default()
        };

        let plan = EventPredicatePlan::new(&request, &EventFieldCatalog::default(), "e").unwrap();
        let sql = plan.where_clause();

        assert!(sql.contains("event_kv_index AS kv"));
        assert!(sql.contains("kv.value_type = 'number'"));
        assert!(sql.contains("kv.number_value >= {kv_number_"));
        assert!(!sql.contains("e.data.latency_ms"));
    }

    #[test]
    fn array_object_facets_correlate_on_same_scope_index() {
        let request = EventsQueryRequest {
            filter: EventFilter {
                facets: vec![
                    EventFacetFilter {
                        path: "items[].sku".to_string(),
                        value: "sku_1".to_string(),
                        ..Default::default()
                    },
                    EventFacetFilter {
                        operator: EventFacetOperator::Gt,
                        path: "items[].price".to_string(),
                        value: "15".to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            ..Default::default()
        };

        let plan = EventPredicatePlan::new(&request, &EventFieldCatalog::default(), "e").unwrap();
        let sql = plan.where_clause();

        assert!(sql.contains("event_kv_index AS kv"));
        assert!(sql.contains("kv.scope_path = {kv_scope_"));
        assert!(sql.contains("GROUP BY kv.event_id, kv.scope_path, kv.scope_index"));
        assert!(sql.contains("HAVING countIf"));
        assert!(sql.contains("kv.number_value > {kv_number_"));
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
            "observatory.measure_cube_points",
            "observatory.measure_cube_rollups",
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
