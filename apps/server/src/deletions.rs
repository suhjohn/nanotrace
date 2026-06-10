use std::sync::Arc;

use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    config::Config,
    read::{EventFacetJoin, EventFacetOperator, EventFilter, ProjectScope},
};

#[derive(Clone)]
pub struct DeletionStore {
    cfg: Arc<Config>,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateDeletionRequest {
    pub project_scope: ProjectScope,
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub filter: EventFilter,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct DeletionJobResponse {
    pub deletion: DeletionJobRecord,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct DeletionJobListResponse {
    pub deletions: Vec<DeletionJobRecord>,
}

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub struct DeletionJobRecord {
    pub tenant_id: String,
    pub deletion_id: String,
    pub status: String,
    pub requested_by: String,
    pub project_ids: Vec<String>,
    pub source_start: String,
    pub source_end: String,
    pub filter: Value,
    pub rows_matched: u64,
    pub rows_deleted: u64,
    pub derived_tables_deleted: Vec<String>,
    pub materializations_marked_stale: u64,
    pub error: String,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
    pub attributes: Value,
}

#[derive(Debug, thiserror::Error)]
pub enum DeletionStoreError {
    #[error("ClickHouse is not configured")]
    ClickHouseNotConfigured,
    #[error("{0}")]
    InvalidRequest(String),
    #[error("deletion job not found")]
    NotFound,
    #[error("ClickHouse request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("ClickHouse query failed: {status} {body}")]
    ClickHouseResponse { status: StatusCode, body: String },
    #[error("invalid ClickHouse response: {0}")]
    InvalidClickHouseResponse(#[from] serde_json::Error),
}

impl DeletionStore {
    pub fn new(cfg: Arc<Config>) -> Self {
        Self {
            cfg,
            http: reqwest::Client::new(),
        }
    }

    pub async fn create_deletion(
        &self,
        tenant_id: &str,
        requested_by: &str,
        request: CreateDeletionRequest,
    ) -> Result<DeletionJobResponse, DeletionStoreError> {
        let source_start = parse_time(&request.from)?;
        let source_end = parse_time(&request.to)?;
        if source_end <= source_start {
            return Err(DeletionStoreError::InvalidRequest(
                "deletion time range must be bounded and non-empty".to_string(),
            ));
        }
        let project_ids = normalized_project_ids(&request.project_scope)?;
        if project_ids.is_empty() {
            return Err(DeletionStoreError::InvalidRequest(
                "deletion projectScope.projectIds is required".to_string(),
            ));
        }
        validate_filter(&request.filter)?;

        let now = Utc::now();
        let source_start_string = format_time(source_start);
        let source_end_string = format_time(source_end);
        let filter = serde_json::to_value(&request.filter).map_err(|_| {
            DeletionStoreError::InvalidRequest("invalid deletion filter".to_string())
        })?;
        let deletion_id = deletion_job_id(
            tenant_id,
            &project_ids,
            &source_start_string,
            &source_end_string,
            &filter,
            now.timestamp_millis(),
        );
        let job = DeletionJobRecord {
            tenant_id: tenant_id.to_string(),
            deletion_id: deletion_id.clone(),
            status: "pending".to_string(),
            requested_by: requested_by.to_string(),
            project_ids,
            source_start: source_start_string,
            source_end: source_end_string,
            filter,
            rows_matched: 0,
            rows_deleted: 0,
            derived_tables_deleted: Vec::new(),
            materializations_marked_stale: 0,
            error: String::new(),
            created_at: format_time(now),
            updated_at: format_time(now),
            completed_at: None,
            attributes: serde_json::json!({
                "iceberg_deletion": "pending_future_implementation"
            }),
        };
        self.insert_job(&job).await?;

        let runner = self.clone();
        let runner_job = job.clone();
        tokio::spawn(async move {
            if let Err(err) = runner.run_deletion(runner_job).await {
                tracing::error!(error = %err, "deletion job runner failed");
            }
        });

        Ok(DeletionJobResponse { deletion: job })
    }

    pub async fn list_deletions(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<DeletionJobRecord>, DeletionStoreError> {
        let query = format!(
            "SELECT {} FROM {} FINAL WHERE tenant_id = {{tenant_id:String}} ORDER BY updated_at DESC, deletion_id ASC LIMIT 200",
            DELETION_JOB_COLUMNS,
            self.table("deletion_jobs")
        );
        let response: ClickHouseResponse<DeletionJobRecord> = self
            .query_json(&query, &[("tenant_id", tenant_id.to_string())])
            .await?;
        Ok(response.data)
    }

    pub async fn get_deletion(
        &self,
        tenant_id: &str,
        deletion_id: &str,
    ) -> Result<DeletionJobRecord, DeletionStoreError> {
        validate_id(deletion_id)?;
        let query = format!(
            "SELECT {} FROM {} FINAL WHERE tenant_id = {{tenant_id:String}} AND deletion_id = {{deletion_id:String}} ORDER BY updated_at DESC LIMIT 1",
            DELETION_JOB_COLUMNS,
            self.table("deletion_jobs")
        );
        let response: ClickHouseResponse<DeletionJobRecord> = self
            .query_json(
                &query,
                &[
                    ("tenant_id", tenant_id.to_string()),
                    ("deletion_id", deletion_id.to_string()),
                ],
            )
            .await?;
        response
            .data
            .into_iter()
            .next()
            .ok_or(DeletionStoreError::NotFound)
    }

    async fn run_deletion(&self, mut job: DeletionJobRecord) -> Result<(), DeletionStoreError> {
        job.status = "running".to_string();
        job.updated_at = format_time(Utc::now());
        self.insert_job(&job).await?;

        let result = self.execute_deletion(&job).await;
        match result {
            Ok(summary) => {
                job.status = "completed".to_string();
                job.rows_matched = summary.rows_matched;
                job.rows_deleted = summary.rows_deleted;
                job.derived_tables_deleted = summary.derived_tables_deleted;
                job.materializations_marked_stale = summary.materializations_marked_stale;
                job.updated_at = format_time(Utc::now());
                job.completed_at = Some(job.updated_at.clone());
                self.insert_job(&job).await
            }
            Err(err) => {
                job.status = "failed".to_string();
                job.error = err.to_string();
                job.updated_at = format_time(Utc::now());
                job.completed_at = Some(job.updated_at.clone());
                self.insert_job(&job).await?;
                Err(err)
            }
        }
    }

    async fn execute_deletion(
        &self,
        job: &DeletionJobRecord,
    ) -> Result<DeletionSummary, DeletionStoreError> {
        let filter: EventFilter = serde_json::from_value(job.filter.clone())?;
        let predicate = EventDeletionPredicate::new(job, &filter)?;
        let count_query = format!(
            "SELECT count() AS count FROM {} WHERE {}",
            self.events_table(),
            predicate.events_where
        );
        let count: ClickHouseResponse<CountRow> = self
            .query_json_with_params(&count_query, &predicate.parameters)
            .await?;
        let rows_matched = count.data.first().map(|row| row.count).unwrap_or(0);

        let derived_tables = [
            DerivedDeletionTable {
                name: "event_text_index",
                time_column: "timestamp",
                event_id_scoped: true,
            },
            DerivedDeletionTable {
                name: "event_kv_index",
                time_column: "timestamp",
                event_id_scoped: true,
            },
            DerivedDeletionTable {
                name: "field_values",
                time_column: "timestamp",
                event_id_scoped: true,
            },
            DerivedDeletionTable {
                name: "field_index",
                time_column: "timestamp",
                event_id_scoped: true,
            },
            DerivedDeletionTable {
                name: "event_measures",
                time_column: "timestamp",
                event_id_scoped: true,
            },
            DerivedDeletionTable {
                name: "measure_cube_points",
                time_column: "timestamp",
                event_id_scoped: true,
            },
            DerivedDeletionTable {
                name: "entity_state_updates",
                time_column: "timestamp",
                event_id_scoped: true,
            },
            DerivedDeletionTable {
                name: "entity_state_current",
                time_column: "timestamp",
                event_id_scoped: true,
            },
            DerivedDeletionTable {
                name: "event_density_1s",
                time_column: "bucket_time",
                event_id_scoped: false,
            },
            DerivedDeletionTable {
                name: "field_rollups",
                time_column: "bucket_time",
                event_id_scoped: false,
            },
            DerivedDeletionTable {
                name: "measure_rollups",
                time_column: "bucket_time",
                event_id_scoped: false,
            },
            DerivedDeletionTable {
                name: "measure_cube_rollups",
                time_column: "bucket_time",
                event_id_scoped: false,
            },
            DerivedDeletionTable {
                name: "counter_rollups",
                time_column: "bucket_time",
                event_id_scoped: false,
            },
            DerivedDeletionTable {
                name: "gauge_rollups",
                time_column: "bucket_time",
                event_id_scoped: false,
            },
            DerivedDeletionTable {
                name: "histogram_rollups",
                time_column: "bucket_time",
                event_id_scoped: false,
            },
            DerivedDeletionTable {
                name: "report_results",
                time_column: "bucket_time",
                event_id_scoped: false,
            },
            DerivedDeletionTable {
                name: "sequence_report_results",
                time_column: "bucket_time",
                event_id_scoped: false,
            },
        ];
        let mut derived_deleted = Vec::new();
        for table in derived_tables {
            self.delete_from_derived_table(table, &predicate).await?;
            derived_deleted.push(table.name.to_string());
        }
        self.delete_from_cohort_memberships(&predicate).await?;
        derived_deleted.push("cohort_memberships".to_string());
        self.execute_mutation(
            &format!(
                "ALTER TABLE {} DELETE WHERE {}",
                self.events_table(),
                predicate.events_where
            ),
            &predicate.parameters,
        )
        .await?;
        let materializations_marked_stale = self.mark_materializations_stale(job).await?;
        Ok(DeletionSummary {
            rows_matched,
            rows_deleted: rows_matched,
            derived_tables_deleted: derived_deleted,
            materializations_marked_stale,
        })
    }

    async fn delete_from_derived_table(
        &self,
        table: DerivedDeletionTable,
        predicate: &EventDeletionPredicate,
    ) -> Result<(), DeletionStoreError> {
        let table_name = self.table(table.name);
        let where_clause = if table.event_id_scoped {
            format!(
                "tenant_id = {{tenant_id:String}} AND project_id IN {{project_ids:Array(String)}} AND {time_column} >= parseDateTime64BestEffort({{source_start:String}}, 3, 'UTC') AND {time_column} < parseDateTime64BestEffort({{source_end:String}}, 3, 'UTC') AND event_id IN (SELECT event_id FROM {events_table} WHERE {events_where})",
                time_column = table.time_column,
                events_table = self.events_table(),
                events_where = predicate.events_where,
            )
        } else {
            format!(
                "tenant_id = {{tenant_id:String}} AND project_id IN {{project_ids:Array(String)}} AND {time_column} >= parseDateTime64BestEffort({{source_start:String}}, 3, 'UTC') AND {time_column} < parseDateTime64BestEffort({{source_end:String}}, 3, 'UTC')",
                time_column = table.time_column,
            )
        };
        self.execute_mutation(
            &format!("ALTER TABLE {table_name} DELETE WHERE {where_clause}"),
            &predicate.parameters,
        )
        .await
    }

    async fn delete_from_cohort_memberships(
        &self,
        predicate: &EventDeletionPredicate,
    ) -> Result<(), DeletionStoreError> {
        let table_name = self.table("cohort_memberships");
        let where_clause = "tenant_id = {tenant_id:String} AND project_id IN {project_ids:Array(String)} AND first_seen < parseDateTime64BestEffort({source_end:String}, 3, 'UTC') AND last_seen >= parseDateTime64BestEffort({source_start:String}, 3, 'UTC')";
        self.execute_mutation(
            &format!("ALTER TABLE {table_name} DELETE WHERE {where_clause}"),
            &predicate.parameters,
        )
        .await
    }

    async fn mark_materializations_stale(
        &self,
        job: &DeletionJobRecord,
    ) -> Result<u64, DeletionStoreError> {
        let parameters = vec![
            ("tenant_id".to_string(), job.tenant_id.clone()),
            ("source_start".to_string(), job.source_start.clone()),
            ("source_end".to_string(), job.source_end.clone()),
        ];
        let count_query = format!(
            "SELECT count() AS count FROM {} FINAL WHERE tenant_id = {{tenant_id:String}} AND source_table = 'events' AND status IN ('active', 'completed', 'materialized', 'loaded') AND (isNull(low_watermark) OR low_watermark < parseDateTime64BestEffort({{source_end:String}}, 3, 'UTC')) AND (isNull(high_watermark) OR high_watermark >= parseDateTime64BestEffort({{source_start:String}}, 3, 'UTC'))",
            self.table("materialization_watermarks")
        );
        let count: ClickHouseResponse<CountRow> = self
            .query_json_with_params(&count_query, &parameters)
            .await?;
        let marked = count.data.first().map(|row| row.count).unwrap_or(0);
        let watermark_update = format!(
            "ALTER TABLE {} UPDATE status = 'stale' WHERE tenant_id = {{tenant_id:String}} AND source_table = 'events' AND status IN ('active', 'completed', 'materialized', 'loaded') AND (isNull(low_watermark) OR low_watermark < parseDateTime64BestEffort({{source_end:String}}, 3, 'UTC')) AND (isNull(high_watermark) OR high_watermark >= parseDateTime64BestEffort({{source_start:String}}, 3, 'UTC'))",
            self.table("materialization_watermarks"),
        );
        self.execute_mutation(&watermark_update, &parameters)
            .await?;
        let version_update = format!(
            "ALTER TABLE {} UPDATE active = 0 WHERE tenant_id = {{tenant_id:String}} AND status IN ('active', 'completed', 'materialized', 'loaded') AND active = 1 AND source_start < parseDateTime64BestEffort({{source_end:String}}, 3, 'UTC') AND source_end >= parseDateTime64BestEffort({{source_start:String}}, 3, 'UTC')",
            self.table("materialization_versions"),
        );
        self.execute_mutation(&version_update, &parameters).await?;
        Ok(marked)
    }

    async fn insert_job(&self, job: &DeletionJobRecord) -> Result<(), DeletionStoreError> {
        self.insert_json_each_row("deletion_jobs", &[job]).await
    }

    async fn query_json<T: for<'de> Deserialize<'de>>(
        &self,
        query: &str,
        parameters: &[(&str, String)],
    ) -> Result<ClickHouseResponse<T>, DeletionStoreError> {
        let parameters = parameters
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect::<Vec<_>>();
        self.query_json_with_params(query, &parameters).await
    }

    async fn query_json_with_params<T: for<'de> Deserialize<'de>>(
        &self,
        query: &str,
        parameters: &[(String, String)],
    ) -> Result<ClickHouseResponse<T>, DeletionStoreError> {
        let body = self
            .execute_body(&format!("{query} FORMAT JSON"), parameters, false)
            .await?;
        serde_json::from_str(&body).map_err(DeletionStoreError::InvalidClickHouseResponse)
    }

    async fn insert_json_each_row<T: Serialize>(
        &self,
        table: &str,
        rows: &[T],
    ) -> Result<(), DeletionStoreError> {
        let mut body = format!("INSERT INTO {} FORMAT JSONEachRow\n", self.table(table));
        for row in rows {
            body.push_str(
                &serde_json::to_string(row).map_err(|_| {
                    DeletionStoreError::InvalidRequest("invalid job row".to_string())
                })?,
            );
            body.push('\n');
        }
        self.execute_body(&body, &[], false).await?;
        Ok(())
    }

    async fn execute_mutation(
        &self,
        query: &str,
        parameters: &[(String, String)],
    ) -> Result<(), DeletionStoreError> {
        self.execute_body(query, parameters, true).await?;
        Ok(())
    }

    async fn execute_body(
        &self,
        query: &str,
        parameters: &[(String, String)],
        mutations_sync: bool,
    ) -> Result<String, DeletionStoreError> {
        let url = self
            .cfg
            .clickhouse_url
            .as_deref()
            .ok_or(DeletionStoreError::ClickHouseNotConfigured)?;
        let max_execution_time = self.cfg.clickhouse_max_execution_secs.to_string();
        let max_bytes_to_read = self.cfg.clickhouse_max_bytes_to_read.to_string();
        let mut request = self
            .http
            .post(url)
            .query(&[
                ("database", self.cfg.clickhouse_database.as_str()),
                ("type_json_skip_duplicated_paths", "1"),
                ("date_time_input_format", "best_effort"),
                ("max_execution_time", max_execution_time.as_str()),
                ("max_bytes_to_read", max_bytes_to_read.as_str()),
            ])
            .body(query.to_string());
        if mutations_sync {
            request = request.query(&[("mutations_sync", "1")]);
        }
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
            return Err(DeletionStoreError::ClickHouseResponse { status, body });
        }
        Ok(body)
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

const DELETION_JOB_COLUMNS: &str = "tenant_id, deletion_id, status, requested_by, project_ids, toString(source_start) AS source_start, toString(source_end) AS source_end, filter, rows_matched, rows_deleted, derived_tables_deleted, materializations_marked_stale, error, toString(created_at) AS created_at, toString(updated_at) AS updated_at, toString(completed_at) AS completed_at, attributes";

#[derive(Debug, Deserialize)]
struct ClickHouseResponse<T> {
    data: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct CountRow {
    count: u64,
}

struct DeletionSummary {
    rows_matched: u64,
    rows_deleted: u64,
    derived_tables_deleted: Vec<String>,
    materializations_marked_stale: u64,
}

#[derive(Clone, Copy)]
struct DerivedDeletionTable {
    name: &'static str,
    time_column: &'static str,
    event_id_scoped: bool,
}

struct EventDeletionPredicate {
    events_where: String,
    parameters: Vec<(String, String)>,
}

impl EventDeletionPredicate {
    fn new(job: &DeletionJobRecord, filter: &EventFilter) -> Result<Self, DeletionStoreError> {
        let mut builder = PredicateBuilder {
            parameters: vec![
                ("tenant_id".to_string(), job.tenant_id.clone()),
                ("source_start".to_string(), job.source_start.clone()),
                ("source_end".to_string(), job.source_end.clone()),
                (
                    "project_ids".to_string(),
                    clickhouse_string_array_literal(&job.project_ids),
                ),
            ],
            next_parameter: 0,
        };
        let mut clauses = vec![
            "tenant_id = {tenant_id:String}".to_string(),
            "project_id IN {project_ids:Array(String)}".to_string(),
            "timestamp >= parseDateTime64BestEffort({source_start:String}, 3, 'UTC')".to_string(),
            "timestamp < parseDateTime64BestEffort({source_end:String}, 3, 'UTC')".to_string(),
        ];
        if !filter.created_after.trim().is_empty() {
            let parameter = builder.push(filter.created_after.trim());
            clauses.push(format!(
                "timestamp >= parseDateTime64BestEffort({{{parameter}:String}}, 3, 'UTC')"
            ));
        }
        if !filter.created_before.trim().is_empty() {
            let parameter = builder.push(filter.created_before.trim());
            clauses.push(format!(
                "timestamp <= parseDateTime64BestEffort({{{parameter}:String}}, 3, 'UTC')"
            ));
        }
        if !filter.text.trim().is_empty() {
            let parameter = builder.push(filter.text.trim());
            clauses.push(format!("event_id IN (SELECT event_id FROM event_text_index WHERE tenant_id = {{tenant_id:String}} AND project_id IN {{project_ids:Array(String)}} AND timestamp >= parseDateTime64BestEffort({{source_start:String}}, 3, 'UTC') AND timestamp < parseDateTime64BestEffort({{source_end:String}}, 3, 'UTC') AND positionCaseInsensitive(text, {{{parameter}:String}}) > 0)"));
        }
        for facet in &filter.facets {
            if matches!(facet.join, EventFacetJoin::Or) {
                return Err(DeletionStoreError::InvalidRequest(
                    "deletion filters do not support OR facets yet".to_string(),
                ));
            }
            clauses.push(facet_clause(facet, &mut builder)?);
        }
        Ok(Self {
            events_where: clauses
                .into_iter()
                .filter(|clause| !clause.is_empty())
                .map(|clause| format!("({clause})"))
                .collect::<Vec<_>>()
                .join(" AND "),
            parameters: builder.parameters,
        })
    }
}

struct PredicateBuilder {
    parameters: Vec<(String, String)>,
    next_parameter: usize,
}

impl PredicateBuilder {
    fn push(&mut self, value: &str) -> String {
        let name = format!("filter_{}", self.next_parameter);
        self.next_parameter += 1;
        self.parameters.push((name.clone(), value.to_string()));
        name
    }
}

fn facet_clause(
    facet: &crate::read::EventFacetFilter,
    builder: &mut PredicateBuilder,
) -> Result<String, DeletionStoreError> {
    let expression = event_value_expression(&facet.path)?;
    let clause = match facet.operator {
        EventFacetOperator::Exists => format!("{expression} != ''"),
        EventFacetOperator::Contains => {
            let parameter = builder.push(&facet.value);
            format!("positionCaseInsensitive({expression}, {{{parameter}:String}}) > 0")
        }
        EventFacetOperator::Eq => {
            let parameter = builder.push(&facet.value);
            format!("{expression} = {{{parameter}:String}}")
        }
        EventFacetOperator::In => {
            let values = if facet.values.is_empty() {
                vec![facet.value.clone()]
            } else {
                facet.values.clone()
            };
            let parameter = builder.push(&clickhouse_string_array_literal(&values));
            format!("{expression} IN {{{parameter}:Array(String)}}")
        }
        EventFacetOperator::Gt
        | EventFacetOperator::Gte
        | EventFacetOperator::Lt
        | EventFacetOperator::Lte => {
            let parameter = builder.push(&facet.value);
            let op = match facet.operator {
                EventFacetOperator::Gt => ">",
                EventFacetOperator::Gte => ">=",
                EventFacetOperator::Lt => "<",
                EventFacetOperator::Lte => "<=",
                _ => unreachable!(),
            };
            format!("toFloat64OrNull({expression}) {op} toFloat64OrNull({{{parameter}:String}})")
        }
    };
    if facet.negated {
        Ok(format!("NOT ({clause})"))
    } else {
        Ok(clause)
    }
}

fn event_value_expression(path: &str) -> Result<String, DeletionStoreError> {
    let path = path.trim();
    if !valid_path(path) {
        return Err(DeletionStoreError::InvalidRequest(format!(
            "unsupported deletion filter path: {path}"
        )));
    }
    let expression = match path {
        "tenant_id" | "project_id" | "event_id" | "event_type" | "trace_id" | "span_id"
        | "signal" => path.to_string(),
        _ => format!("ifNull(toString(data.{path}), '')"),
    };
    Ok(expression)
}

fn validate_filter(filter: &EventFilter) -> Result<(), DeletionStoreError> {
    if filter.facets.len() > 50 {
        return Err(DeletionStoreError::InvalidRequest(
            "deletion filter has too many facets".to_string(),
        ));
    }
    for facet in &filter.facets {
        event_value_expression(&facet.path)?;
        if !facet.scope.trim().is_empty() {
            return Err(DeletionStoreError::InvalidRequest(
                "deletion filters do not support scoped array facets yet".to_string(),
            ));
        }
    }
    Ok(())
}

fn normalized_project_ids(project_scope: &ProjectScope) -> Result<Vec<String>, DeletionStoreError> {
    let mut project_ids = project_scope
        .project_ids
        .iter()
        .map(|project_id| project_id.trim())
        .filter(|project_id| !project_id.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    project_ids.sort();
    project_ids.dedup();
    for project_id in &project_ids {
        validate_id(project_id)?;
    }
    Ok(project_ids)
}

fn parse_time(value: &str) -> Result<DateTime<Utc>, DeletionStoreError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .map(|value| value.with_timezone(&Utc))
        })
        .map_err(|_| DeletionStoreError::InvalidRequest("invalid deletion time range".to_string()))
}

fn format_time(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn deletion_job_id(
    tenant_id: &str,
    project_ids: &[String],
    source_start: &str,
    source_end: &str,
    filter: &Value,
    nonce: i64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(tenant_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(project_ids.join("\0").as_bytes());
    hasher.update(b"\0");
    hasher.update(source_start.as_bytes());
    hasher.update(b"\0");
    hasher.update(source_end.as_bytes());
    hasher.update(b"\0");
    hasher.update(filter.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(nonce.to_string().as_bytes());
    format!("del_{}", hex_lower(&hasher.finalize()[..16]))
}

fn validate_id(value: &str) -> Result<(), DeletionStoreError> {
    if value.trim().is_empty()
        || value.len() > 160
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | ':' | '.'))
    {
        return Err(DeletionStoreError::InvalidRequest(
            "invalid deletion identifier".to_string(),
        ));
    }
    Ok(())
}

fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 160
        && path.split('.').all(|part| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        })
}

fn clickhouse_string_array_literal(values: &[String]) -> String {
    let items = values
        .iter()
        .map(|value| format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'")))
        .collect::<Vec<_>>()
        .join(",");
    format!("[{items}]")
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deletion_predicate_requires_project_scope_and_time_bounds() {
        let request = CreateDeletionRequest {
            project_scope: ProjectScope {
                project_ids: vec!["proj_b".to_string(), "proj_a".to_string()],
            },
            from: "2026-06-01T00:00:00Z".to_string(),
            to: "2026-06-02T00:00:00Z".to_string(),
            filter: EventFilter::default(),
        };
        assert_eq!(
            normalized_project_ids(&request.project_scope).unwrap(),
            vec!["proj_a".to_string(), "proj_b".to_string()]
        );
        assert!(parse_time(&request.from).is_ok());
        assert!(parse_time(&request.to).is_ok());
    }

    #[test]
    fn event_filter_predicate_uses_parameters() {
        let job = DeletionJobRecord {
            tenant_id: "org".to_string(),
            deletion_id: "del_1".to_string(),
            status: "pending".to_string(),
            requested_by: "user".to_string(),
            project_ids: vec!["proj_1".to_string()],
            source_start: "2026-06-01T00:00:00.000Z".to_string(),
            source_end: "2026-06-02T00:00:00.000Z".to_string(),
            filter: serde_json::json!({}),
            rows_matched: 0,
            rows_deleted: 0,
            derived_tables_deleted: Vec::new(),
            materializations_marked_stale: 0,
            error: String::new(),
            created_at: "2026-06-01T00:00:00.000Z".to_string(),
            updated_at: "2026-06-01T00:00:00.000Z".to_string(),
            completed_at: None,
            attributes: serde_json::json!({}),
        };
        let filter = EventFilter {
            text: "timeout".to_string(),
            facets: vec![crate::read::EventFacetFilter {
                path: "event_type".to_string(),
                operator: EventFacetOperator::Eq,
                value: "log".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let predicate = EventDeletionPredicate::new(&job, &filter).unwrap();
        assert!(predicate.events_where.contains("project_id IN"));
        assert!(predicate.events_where.contains("event_text_index"));
        assert!(
            predicate
                .events_where
                .contains("event_type = {filter_1:String}")
        );
    }
}
