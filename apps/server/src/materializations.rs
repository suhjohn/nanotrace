use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::Config;

type ChunkWindow = (DateTime<Utc>, DateTime<Utc>);

#[derive(Clone)]
pub struct MaterializationStore {
    cfg: Arc<Config>,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct CreateBackfillRequest {
    #[serde(default)]
    pub target_id: Option<String>,
    pub source_start: String,
    pub source_end: String,
    #[serde(default)]
    pub chunk_seconds: Option<u32>,
    #[serde(default)]
    pub priority: Option<u8>,
    #[serde(default)]
    pub max_attempts: Option<u32>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct BackfillJobResponse {
    pub backfill: MaterializationJobRecord,
    pub chunks: Vec<MaterializationChunkRecord>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct BackfillJobListResponse {
    pub backfills: Vec<MaterializationJobRecord>,
}

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub struct MaterializationJobRecord {
    pub tenant_id: String,
    pub job_id: String,
    pub job_kind: String,
    pub status: String,
    pub priority: u8,
    pub target_type: String,
    pub target_table: String,
    pub target_id: String,
    pub target_version: u64,
    pub source_table: String,
    pub source_start: String,
    pub source_end: String,
    pub chunk_seconds: u32,
    pub total_chunks: u64,
    pub completed_chunks: u64,
    pub failed_chunks: u64,
    pub rows_scanned: u64,
    pub rows_written: u64,
    pub bytes_scanned: u64,
    pub bytes_written: u64,
    pub lease_owner: String,
    pub leased_until: Option<String>,
    pub attempt: u32,
    pub max_attempts: u32,
    pub error: String,
    pub config: Value,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub struct MaterializationChunkRecord {
    pub tenant_id: String,
    pub job_id: String,
    pub chunk_id: String,
    pub chunk_index: u64,
    pub status: String,
    pub target_type: String,
    pub target_table: String,
    pub target_id: String,
    pub target_version: u64,
    pub source_table: String,
    pub source_start: String,
    pub source_end: String,
    pub rows_scanned: u64,
    pub rows_written: u64,
    pub bytes_scanned: u64,
    pub bytes_written: u64,
    pub lease_owner: String,
    pub leased_until: Option<String>,
    pub attempt: u32,
    pub max_attempts: u32,
    pub error: String,
    pub started_at: Option<String>,
    pub updated_at: String,
    pub completed_at: Option<String>,
    pub attributes: Value,
}

#[derive(Debug, thiserror::Error)]
pub enum MaterializationStoreError {
    #[error("ClickHouse is not configured")]
    ClickHouseNotConfigured,
    #[error("invalid materialization request")]
    InvalidRequest,
    #[error("invalid materialization target")]
    InvalidTarget,
    #[error(
        "definition kind '{kind}' uses synchronous /v1/definitions/{{definition_id}}/backfill instead of queued /backfills"
    )]
    UnsupportedQueuedBackfillKind { kind: String },
    #[error("materialization definition not found")]
    DefinitionNotFound,
    #[error("materialization job not found")]
    NotFound,
    #[error("ClickHouse request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("ClickHouse query failed: {status} {body}")]
    ClickHouseResponse { status: StatusCode, body: String },
    #[error("invalid ClickHouse response: {0}")]
    InvalidClickHouseResponse(#[from] serde_json::Error),
}

impl MaterializationStore {
    pub fn new(cfg: Arc<Config>) -> Self {
        Self {
            cfg,
            http: reqwest::Client::new(),
        }
    }

    pub async fn create_backfill(
        &self,
        tenant_id: &str,
        definition_id: &str,
        request: CreateBackfillRequest,
    ) -> Result<BackfillJobResponse, MaterializationStoreError> {
        validate_id(definition_id)?;
        let source_start = parse_time(&request.source_start)?;
        let source_end = parse_time(&request.source_end)?;
        if source_end <= source_start {
            return Err(MaterializationStoreError::InvalidRequest);
        }
        let chunk_seconds = request.chunk_seconds.unwrap_or(3600);
        if chunk_seconds == 0 {
            return Err(MaterializationStoreError::InvalidRequest);
        }
        let priority = request.priority.unwrap_or(50);
        let max_attempts = request.max_attempts.unwrap_or(5).clamp(1, 100);

        let definition = self.definition(tenant_id, definition_id).await?;
        let target = queued_target_from_definition(&definition, request.target_id.as_deref())?;
        let chunks = chunk_windows(source_start, source_end, chunk_seconds)?;
        let now = Utc::now();
        let now_string = format_time(now);
        let source_start_string = format_time(source_start);
        let source_end_string = format_time(source_end);
        let job_id = materialization_job_id(
            &definition.definition_id,
            target.target_type,
            &target.target_id,
            definition.version,
            &source_start_string,
            &source_end_string,
            now.timestamp_millis(),
        );
        let job = MaterializationJobRecord {
            tenant_id: tenant_id.to_string(),
            job_id: job_id.clone(),
            job_kind: "definition_backfill".to_string(),
            status: "pending".to_string(),
            priority,
            target_type: target.target_type.to_string(),
            target_table: target.target_table.to_string(),
            target_id: target.target_id.clone(),
            target_version: definition.version,
            source_table: self.cfg.clickhouse_table.clone(),
            source_start: source_start_string.clone(),
            source_end: source_end_string.clone(),
            chunk_seconds,
            total_chunks: chunks.len().try_into().unwrap_or(u64::MAX),
            completed_chunks: 0,
            failed_chunks: 0,
            rows_scanned: 0,
            rows_written: 0,
            bytes_scanned: 0,
            bytes_written: 0,
            lease_owner: String::new(),
            leased_until: None,
            attempt: 0,
            max_attempts,
            error: String::new(),
            config: serde_json::json!({
                "definition_id": definition.definition_id,
                "definition_name": definition.name,
                "definition_kind": definition.kind,
                "definition_mode": definition.mode,
                "definition_version": definition.version,
            }),
            created_at: now_string.clone(),
            updated_at: now_string.clone(),
            completed_at: None,
        };
        let chunk_rows = chunks
            .into_iter()
            .enumerate()
            .map(|(index, (start, end))| {
                let chunk_index = u64::try_from(index).unwrap_or(u64::MAX);
                MaterializationChunkRecord {
                    tenant_id: tenant_id.to_string(),
                    job_id: job_id.clone(),
                    chunk_id: materialization_chunk_id(&job_id, chunk_index),
                    chunk_index,
                    status: "pending".to_string(),
                    target_type: target.target_type.to_string(),
                    target_table: target.target_table.to_string(),
                    target_id: target.target_id.clone(),
                    target_version: definition.version,
                    source_table: self.cfg.clickhouse_table.clone(),
                    source_start: format_time(start),
                    source_end: format_time(end),
                    rows_scanned: 0,
                    rows_written: 0,
                    bytes_scanned: 0,
                    bytes_written: 0,
                    lease_owner: String::new(),
                    leased_until: None,
                    attempt: 0,
                    max_attempts,
                    error: String::new(),
                    started_at: None,
                    updated_at: now_string.clone(),
                    completed_at: None,
                    attributes: serde_json::json!({
                        "definition_id": definition.definition_id,
                        "definition_kind": definition.kind,
                        "definition_mode": definition.mode,
                        "requested_source_start": source_start_string,
                        "requested_source_end": source_end_string,
                    }),
                }
            })
            .collect::<Vec<_>>();

        self.insert_json_each_row("materialization_jobs", &[&job])
            .await?;
        let chunk_refs = chunk_rows.iter().collect::<Vec<_>>();
        self.insert_json_each_row("materialization_chunks", &chunk_refs)
            .await?;
        Ok(BackfillJobResponse {
            backfill: job,
            chunks: chunk_rows,
        })
    }

    pub async fn list_backfills(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<MaterializationJobRecord>, MaterializationStoreError> {
        let query = format!(
            "SELECT {} FROM {} FINAL WHERE tenant_id = {{tenant_id:String}} ORDER BY updated_at DESC, priority ASC, job_id ASC LIMIT 200",
            MATERIALIZATION_JOB_COLUMNS,
            self.table("materialization_jobs")
        );
        let response: ClickHouseResponse<MaterializationJobRecord> = self
            .query_json(&query, &[("tenant_id", tenant_id.to_string())])
            .await?;
        Ok(response.data)
    }

    pub async fn get_backfill(
        &self,
        tenant_id: &str,
        job_id: &str,
    ) -> Result<BackfillJobResponse, MaterializationStoreError> {
        validate_id(job_id)?;
        let query = format!(
            "SELECT {} FROM {} FINAL WHERE tenant_id = {{tenant_id:String}} AND job_id = {{job_id:String}} ORDER BY updated_at DESC LIMIT 1",
            MATERIALIZATION_JOB_COLUMNS,
            self.table("materialization_jobs")
        );
        let response: ClickHouseResponse<MaterializationJobRecord> = self
            .query_json(
                &query,
                &[
                    ("tenant_id", tenant_id.to_string()),
                    ("job_id", job_id.to_string()),
                ],
            )
            .await?;
        let job = response
            .data
            .into_iter()
            .next()
            .ok_or(MaterializationStoreError::NotFound)?;
        let chunks_query = format!(
            "SELECT {} FROM {} FINAL WHERE tenant_id = {{tenant_id:String}} AND job_id = {{job_id:String}} ORDER BY chunk_index ASC, chunk_id ASC",
            MATERIALIZATION_CHUNK_COLUMNS,
            self.table("materialization_chunks")
        );
        let chunks: ClickHouseResponse<MaterializationChunkRecord> = self
            .query_json(
                &chunks_query,
                &[
                    ("tenant_id", tenant_id.to_string()),
                    ("job_id", job_id.to_string()),
                ],
            )
            .await?;
        Ok(BackfillJobResponse {
            backfill: job,
            chunks: chunks.data,
        })
    }

    async fn definition(
        &self,
        tenant_id: &str,
        definition_id: &str,
    ) -> Result<DefinitionRow, MaterializationStoreError> {
        let query = format!(
            "SELECT tenant_id, definition_id, name, kind, mode, config, version FROM {} FINAL WHERE tenant_id = {{tenant_id:String}} AND definition_id = {{definition_id:String}} AND enabled = 1 AND kind IN ('field', 'measure', 'rollup', 'metric_rollup', 'state', 'search', 'report', 'sequence', 'cohort', 'alert') AND isNull(deleted_at) ORDER BY updated_at DESC LIMIT 1",
            self.table("definitions")
        );
        let response: ClickHouseResponse<DefinitionRow> = self
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
            .ok_or(MaterializationStoreError::DefinitionNotFound)
    }

    async fn query_json<T: for<'de> Deserialize<'de>>(
        &self,
        query: &str,
        parameters: &[(&str, String)],
    ) -> Result<ClickHouseResponse<T>, MaterializationStoreError> {
        let body = self
            .execute_body(&format!("{query} FORMAT JSON"), parameters)
            .await?;
        serde_json::from_str(&body).map_err(MaterializationStoreError::InvalidClickHouseResponse)
    }

    async fn insert_json_each_row<T: Serialize>(
        &self,
        table: &str,
        rows: &[&T],
    ) -> Result<(), MaterializationStoreError> {
        let mut body = format!("INSERT INTO {} FORMAT JSONEachRow\n", self.table(table));
        for row in rows {
            body.push_str(
                &serde_json::to_string(row)
                    .map_err(|_| MaterializationStoreError::InvalidRequest)?,
            );
            body.push('\n');
        }
        self.execute_body(&body, &[]).await?;
        Ok(())
    }

    async fn execute_body(
        &self,
        query: &str,
        parameters: &[(&str, String)],
    ) -> Result<String, MaterializationStoreError> {
        let url = self
            .cfg
            .clickhouse_url
            .as_deref()
            .ok_or(MaterializationStoreError::ClickHouseNotConfigured)?;
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
            return Err(MaterializationStoreError::ClickHouseResponse { status, body });
        }
        Ok(body)
    }

    fn table(&self, table: &str) -> String {
        format!("{}.{}", self.cfg.clickhouse_database, table)
    }
}

const MATERIALIZATION_JOB_COLUMNS: &str = "tenant_id, job_id, job_kind, status, priority, target_type, target_table, target_id, target_version, source_table, toString(source_start) AS source_start, toString(source_end) AS source_end, chunk_seconds, total_chunks, completed_chunks, failed_chunks, rows_scanned, rows_written, bytes_scanned, bytes_written, lease_owner, toString(leased_until) AS leased_until, attempt, max_attempts, error, config, toString(created_at) AS created_at, toString(updated_at) AS updated_at, toString(completed_at) AS completed_at";
const MATERIALIZATION_CHUNK_COLUMNS: &str = "tenant_id, job_id, chunk_id, chunk_index, status, target_type, target_table, target_id, target_version, source_table, toString(source_start) AS source_start, toString(source_end) AS source_end, rows_scanned, rows_written, bytes_scanned, bytes_written, lease_owner, toString(leased_until) AS leased_until, attempt, max_attempts, error, toString(started_at) AS started_at, toString(updated_at) AS updated_at, toString(completed_at) AS completed_at, attributes";

#[derive(Debug, Deserialize)]
struct ClickHouseResponse<T> {
    data: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct DefinitionRow {
    definition_id: String,
    name: String,
    kind: String,
    mode: String,
    config: Value,
    version: u64,
}

struct QueuedTarget {
    target_type: &'static str,
    target_table: &'static str,
    target_id: String,
}

fn queued_target_from_definition(
    definition: &DefinitionRow,
    requested_target_id: Option<&str>,
) -> Result<QueuedTarget, MaterializationStoreError> {
    let requested_target_id = requested_target_id.map(validate_id).transpose()?;
    let (target_type, target_table, id_key) = match definition.kind.as_str() {
        "report" => ("report", "report_results", "report_id"),
        "sequence" => ("sequence", "sequence_report_results", "report_id"),
        "cohort" => ("cohort", "cohort_memberships", "cohort_id"),
        kind => {
            return Err(MaterializationStoreError::UnsupportedQueuedBackfillKind {
                kind: kind.to_string(),
            });
        }
    };
    let outputs = definition
        .config
        .get("outputs")
        .and_then(Value::as_array)
        .ok_or(MaterializationStoreError::InvalidTarget)?;
    let mut ids = outputs
        .iter()
        .filter_map(Value::as_object)
        .filter(|output| {
            output
                .get("target")
                .and_then(Value::as_str)
                .is_none_or(|value| value == target_table)
        })
        .filter_map(|output| output.get(id_key).and_then(Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    let target_id = match requested_target_id {
        Some(target_id) if ids.iter().any(|candidate| candidate == target_id) => {
            target_id.to_string()
        }
        Some(_) => return Err(MaterializationStoreError::InvalidTarget),
        None if ids.len() == 1 => ids.remove(0),
        None => return Err(MaterializationStoreError::InvalidTarget),
    };
    Ok(QueuedTarget {
        target_type,
        target_table,
        target_id,
    })
}

fn chunk_windows(
    source_start: DateTime<Utc>,
    source_end: DateTime<Utc>,
    chunk_seconds: u32,
) -> Result<Vec<ChunkWindow>, MaterializationStoreError> {
    let mut chunks = Vec::new();
    let chunk = Duration::seconds(i64::from(chunk_seconds));
    let mut start = source_start;
    while start < source_end {
        if chunks.len() >= 10_000 {
            return Err(MaterializationStoreError::InvalidRequest);
        }
        let end = (start + chunk).min(source_end);
        chunks.push((start, end));
        start = end;
    }
    Ok(chunks)
}

fn materialization_job_id(
    definition_id: &str,
    target_type: &str,
    target_id: &str,
    target_version: u64,
    source_start: &str,
    source_end: &str,
    nonce: i64,
) -> String {
    let mut hasher = Sha256::new();
    for part in [
        definition_id,
        target_type,
        target_id,
        &target_version.to_string(),
        source_start,
        source_end,
        &nonce.to_string(),
    ] {
        hasher.update(part.as_bytes());
        hasher.update(b"\0");
    }
    format!("mat_{:x}", hasher.finalize())
}

fn materialization_chunk_id(job_id: &str, chunk_index: u64) -> String {
    format!("{job_id}:chunk:{chunk_index}")
}

fn parse_time(value: &str) -> Result<DateTime<Utc>, MaterializationStoreError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f %z")
                .map(|value| value.with_timezone(&Utc))
        })
        .map_err(|_| MaterializationStoreError::InvalidRequest)
}

fn format_time(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

fn validate_id(value: &str) -> Result<&str, MaterializationStoreError> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 180
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(MaterializationStoreError::InvalidRequest);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::{
        DefinitionRow, MaterializationStoreError, chunk_windows, queued_target_from_definition,
    };

    #[test]
    fn queued_target_requires_literal_matching_definition_output() {
        let definition = DefinitionRow {
            definition_id: "def_checkout".to_string(),
            name: "checkout".to_string(),
            kind: "report".to_string(),
            mode: "summary".to_string(),
            config: json!({
                "outputs": [
                    {
                        "target": "report_results",
                        "report_id": "checkout_by_plan"
                    }
                ]
            }),
            version: 7,
        };

        let target = queued_target_from_definition(&definition, None).expect("target");
        assert_eq!(target.target_type, "report");
        assert_eq!(target.target_table, "report_results");
        assert_eq!(target.target_id, "checkout_by_plan");

        assert!(matches!(
            queued_target_from_definition(&definition, Some("other")),
            Err(MaterializationStoreError::InvalidTarget)
        ));
    }

    #[test]
    fn chunk_windows_split_half_open_ranges() {
        let start = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let end = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 2, 30, 0).unwrap();

        let chunks = chunk_windows(start, end, 3600).expect("chunks");

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].0, start);
        assert_eq!(
            chunks[0].1,
            chrono::Utc.with_ymd_and_hms(2026, 6, 1, 1, 0, 0).unwrap()
        );
        assert_eq!(
            chunks[2].0,
            chrono::Utc.with_ymd_and_hms(2026, 6, 1, 2, 0, 0).unwrap()
        );
        assert_eq!(chunks[2].1, end);
    }
}
