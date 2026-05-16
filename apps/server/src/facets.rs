use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::OnceCell;
use tracing::{error, info, warn};

use crate::config::Config;

#[derive(Clone)]
pub struct FacetStore {
    cfg: Arc<Config>,
    http: reqwest::Client,
    pg: Option<PgPool>,
    table_ready: Arc<OnceCell<()>>,
    control_ready: Arc<OnceCell<()>>,
    worker_id: String,
}

#[derive(Debug, Deserialize)]
pub struct PutFacetRequest {
    pub path: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub value_type: Option<String>,
    #[serde(default = "default_true")]
    pub lookup_enabled: bool,
    #[serde(default = "default_true")]
    pub aggregate_enabled: bool,
}

#[derive(Debug, Serialize)]
pub struct FacetListResponse {
    pub facets: Vec<HotDimension>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FacetBackfillResponse {
    pub job_id: String,
    pub path: String,
    pub status: String,
    pub total_chunks: u64,
    pub completed_chunks: u64,
    pub failed_chunks: u64,
    pub indexed_events: u64,
    pub values: u64,
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct FacetBackfillListResponse {
    pub backfills: Vec<FacetBackfillResponse>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HotDimension {
    pub path: String,
    pub value_type: String,
    pub status: String,
    pub lookup_enabled: bool,
    pub aggregate_enabled: bool,
    pub display_name: String,
    pub source: String,
    pub removable: bool,
}

#[derive(Debug)]
struct BackfillChunk {
    chunk_id: i64,
    job_id: String,
    organization_id: String,
    path: String,
    value_type: String,
    chunk_start: DateTime<Utc>,
    chunk_end: DateTime<Utc>,
    attempts: i32,
}

#[derive(Debug, Deserialize)]
struct ClickHouseResponse<T> {
    data: Vec<T>,
}

#[derive(Debug, thiserror::Error)]
pub enum FacetError {
    #[error("ClickHouse is not configured")]
    ClickHouseNotConfigured,
    #[error("Postgres is not configured")]
    PostgresNotConfigured,
    #[error("invalid facet path")]
    InvalidPath,
    #[error("invalid facet value_type")]
    InvalidValueType,
    #[error("built-in facets cannot be removed")]
    BuiltinFacet,
    #[error("ClickHouse request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Postgres request failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("ClickHouse query failed: {status} {body}")]
    ClickHouseResponse { status: StatusCode, body: String },
    #[error("invalid ClickHouse response: {0}")]
    Json(#[from] serde_json::Error),
}

impl FacetStore {
    pub async fn connect(cfg: Arc<Config>) -> Result<Self, FacetError> {
        let pg = match cfg.auth.postgres_url.clone() {
            Some(postgres_url) => Some(
                PgPoolOptions::new()
                    .max_connections(4)
                    .connect(&postgres_url)
                    .await?,
            ),
            None => None,
        };
        Ok(Self {
            cfg,
            http: reqwest::Client::new(),
            pg,
            table_ready: Arc::new(OnceCell::new()),
            control_ready: Arc::new(OnceCell::new()),
            worker_id: backfill_worker_id(),
        })
    }

    pub async fn list(&self, tenant_id: &str) -> Result<Vec<HotDimension>, FacetError> {
        self.ensure_table().await?;
        let mut facets = builtin_dimensions();
        for custom in self.active_custom_dimensions(tenant_id).await? {
            if !facets.iter().any(|facet| facet.path == custom.path) {
                facets.push(custom);
            }
        }
        Ok(facets)
    }

    pub async fn put(
        &self,
        tenant_id: &str,
        request: PutFacetRequest,
    ) -> Result<HotDimension, FacetError> {
        self.ensure_table().await?;
        let path = validate_path(&request.path)?;
        if builtin_dimensions().iter().any(|facet| facet.path == path) {
            return Err(FacetError::BuiltinFacet);
        }
        let value_type = validate_value_type(request.value_type.as_deref().unwrap_or("string"))?;
        let display_name = request
            .display_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(&path)
            .to_string();

        let facet = HotDimension {
            path,
            value_type: value_type.to_string(),
            status: "active".to_string(),
            lookup_enabled: request.lookup_enabled,
            aggregate_enabled: request.aggregate_enabled,
            display_name,
            source: "user".to_string(),
            removable: true,
        };
        self.insert_dimension(tenant_id, &facet).await?;
        Ok(facet)
    }

    pub async fn delete(&self, tenant_id: &str, path: &str) -> Result<HotDimension, FacetError> {
        self.ensure_table().await?;
        let path = validate_path(path)?;
        if builtin_dimensions().iter().any(|facet| facet.path == path) {
            return Err(FacetError::BuiltinFacet);
        }

        let facet = HotDimension {
            path,
            value_type: "string".to_string(),
            status: "disabled".to_string(),
            lookup_enabled: false,
            aggregate_enabled: false,
            display_name: String::new(),
            source: "user".to_string(),
            removable: true,
        };
        self.insert_dimension(tenant_id, &facet).await?;
        Ok(facet)
    }

    pub async fn enqueue_backfill(
        &self,
        organization_id: &str,
        path: &str,
    ) -> Result<FacetBackfillResponse, FacetError> {
        self.ensure_table().await?;
        self.ensure_control_plane().await?;
        let path = validate_path(path)?;
        let dimensions = self.list(organization_id).await?;
        let Some(dimension) = dimensions.iter().find(|dimension| dimension.path == path) else {
            return Err(FacetError::InvalidPath);
        };
        let index_value_type = event_index_value_type(&dimension.value_type)?;
        let value_expr = facet_value_expression(&path);

        #[derive(Deserialize)]
        struct Bounds {
            events: u64,
            values: u64,
            min_ms: i64,
            max_ms: i64,
        }

        let bounds_query = format!(
            "SELECT events, values, \
                    if(events = 0, 0, toInt64(toUnixTimestamp64Milli(min_ts))) AS min_ms, \
                    if(events = 0, 0, toInt64(toUnixTimestamp64Milli(max_ts))) AS max_ms \
             FROM (SELECT count() AS events, uniqExact(value) AS values, min(timestamp) AS min_ts, max(timestamp) AS max_ts \
                   FROM (SELECT timestamp, {} AS value FROM {} WHERE tenant_id = {}) WHERE value != '')",
            value_expr,
            self.events_table(),
            quote_literal(organization_id)
        );
        let response: ClickHouseResponse<Bounds> =
            serde_json::from_str(&self.clickhouse_query(&bounds_query).await?)?;
        let bounds = response.data.into_iter().next().unwrap_or(Bounds {
            events: 0,
            values: 0,
            min_ms: 0,
            max_ms: 0,
        });

        let job_id = backfill_job_id(organization_id, &path);
        let chunks = backfill_chunks(bounds.min_ms, bounds.max_ms);
        let total_chunks = chunks.len() as i64;
        let status = if total_chunks == 0 {
            "completed"
        } else {
            "queued"
        };

        let pg = self.pg_pool()?;
        let mut tx = pg.begin().await?;
        sqlx::query(
            "INSERT INTO nanotrace_facet_backfill_jobs
             (job_id, organization_id, path, value_type, status, total_chunks, completed_chunks, failed_chunks, indexed_events, values, error)
             VALUES ($1, $2, $3, $4, $5, $6, 0, 0, $7, $8, '')",
        )
        .bind(&job_id)
        .bind(organization_id)
        .bind(&path)
        .bind(index_value_type)
        .bind(status)
        .bind(total_chunks)
        .bind(if total_chunks == 0 {
            bounds.events as i64
        } else {
            0
        })
        .bind(if total_chunks == 0 {
            bounds.values as i64
        } else {
            0
        })
        .execute(&mut *tx)
        .await?;

        for (chunk_start, chunk_end) in chunks {
            sqlx::query(
                "INSERT INTO nanotrace_facet_backfill_chunks
                 (job_id, organization_id, path, chunk_start, chunk_end, status)
                 VALUES ($1, $2, $3, $4, $5, 'queued')",
            )
            .bind(&job_id)
            .bind(organization_id)
            .bind(&path)
            .bind(chunk_start)
            .bind(chunk_end)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;

        self.backfill_status(organization_id, &job_id).await
    }

    pub async fn backfill_status(
        &self,
        organization_id: &str,
        job_id: &str,
    ) -> Result<FacetBackfillResponse, FacetError> {
        self.ensure_control_plane().await?;
        let job_id = validate_job_id(job_id)?;
        let row = sqlx::query_as::<_, (String, String, String, i64, i64, i64, i64, i64, String)>(
            "SELECT job_id, path, status, total_chunks, completed_chunks, failed_chunks, indexed_events, values, error
             FROM nanotrace_facet_backfill_jobs
             WHERE organization_id = $1 AND job_id = $2",
        )
        .bind(organization_id)
        .bind(job_id)
        .fetch_optional(self.pg_pool()?)
        .await?;
        row.map(backfill_response_from_row)
            .ok_or(FacetError::InvalidPath)
    }

    pub async fn backfill_list(
        &self,
        organization_id: &str,
    ) -> Result<Vec<FacetBackfillResponse>, FacetError> {
        self.ensure_control_plane().await?;
        let rows = sqlx::query_as::<_, (String, String, String, i64, i64, i64, i64, i64, String)>(
            "SELECT job_id, path, status, total_chunks, completed_chunks, failed_chunks, indexed_events, values, error
             FROM nanotrace_facet_backfill_jobs
             WHERE organization_id = $1
             ORDER BY updated_at DESC, created_at DESC
             LIMIT 100",
        )
        .bind(organization_id)
        .fetch_all(self.pg_pool()?)
        .await?;
        Ok(rows.into_iter().map(backfill_response_from_row).collect())
    }

    pub async fn run_backfill_worker(self: Arc<Self>) {
        if self.pg.is_none() {
            warn!("facet backfill worker disabled because Postgres is not configured");
            return;
        }
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            match self.process_next_backfill_chunk().await {
                Ok(Some(job_id)) => info!(%job_id, "processed facet backfill chunk"),
                Ok(None) => {}
                Err(err) => warn!(error = %err, "facet backfill worker iteration failed"),
            }
        }
    }

    async fn process_next_backfill_chunk(&self) -> Result<Option<String>, FacetError> {
        self.ensure_table().await?;
        self.ensure_control_plane().await?;
        let Some(chunk) = self.claim_backfill_chunk().await? else {
            return Ok(None);
        };
        let job_id = chunk.job_id.clone();
        if let Err(err) = self.process_backfill_chunk(&chunk).await {
            error!(job_id = %chunk.job_id, path = %chunk.path, error = %err, "facet backfill chunk failed");
            self.mark_backfill_chunk_failed(&chunk, &err.to_string())
                .await?;
        }
        self.refresh_backfill_job(&job_id).await?;
        Ok(Some(job_id))
    }

    async fn claim_backfill_chunk(&self) -> Result<Option<BackfillChunk>, FacetError> {
        let row = sqlx::query_as::<
            _,
            (
                i64,
                String,
                String,
                String,
                String,
                DateTime<Utc>,
                DateTime<Utc>,
                i32,
            ),
        >(
            "WITH next_chunk AS (
                SELECT c.chunk_id
                FROM nanotrace_facet_backfill_chunks c
                INNER JOIN nanotrace_facet_backfill_jobs j ON j.job_id = c.job_id
                WHERE j.status IN ('queued', 'running')
                  AND c.attempts < 3
                  AND (
                    c.status = 'queued'
                    OR (c.status = 'running' AND c.lease_expires_at < now())
                  )
                ORDER BY c.updated_at ASC, c.chunk_id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
             )
             UPDATE nanotrace_facet_backfill_chunks c
             SET status = 'running',
                 attempts = c.attempts + 1,
                 lease_owner = $1,
                 lease_expires_at = now() + interval '10 minutes',
                 updated_at = now()
             FROM next_chunk, nanotrace_facet_backfill_jobs j
             WHERE c.chunk_id = next_chunk.chunk_id
               AND j.job_id = c.job_id
             RETURNING c.chunk_id, c.job_id, c.organization_id, c.path, j.value_type, c.chunk_start, c.chunk_end, c.attempts",
        )
        .bind(&self.worker_id)
        .fetch_optional(self.pg_pool()?)
        .await?;
        let Some((
            chunk_id,
            job_id,
            organization_id,
            path,
            value_type,
            chunk_start,
            chunk_end,
            attempts,
        )) = row
        else {
            return Ok(None);
        };
        sqlx::query(
            "UPDATE nanotrace_facet_backfill_jobs
             SET status = 'running', updated_at = now(), error = ''
             WHERE job_id = $1 AND status = 'queued'",
        )
        .bind(&job_id)
        .execute(self.pg_pool()?)
        .await?;
        Ok(Some(BackfillChunk {
            chunk_id,
            job_id,
            organization_id,
            path,
            value_type,
            chunk_start,
            chunk_end,
            attempts,
        }))
    }

    async fn process_backfill_chunk(&self, chunk: &BackfillChunk) -> Result<(), FacetError> {
        let dimensions = self.list(&chunk.organization_id).await?;
        let Some(dimension) = dimensions
            .iter()
            .find(|dimension| dimension.path == chunk.path)
        else {
            return Err(FacetError::InvalidPath);
        };
        let lookup_enabled = dimension.lookup_enabled;
        let aggregate_enabled = dimension.aggregate_enabled;
        let value_expr = facet_value_expression(&chunk.path);
        let error_expr = error_count_expression();
        let (bucket_delete_start, bucket_delete_end) =
            clickhouse_minute_bucket_bounds(chunk.chunk_start, chunk.chunk_end);
        self.clickhouse_exec_sync_mutation(&format!(
            "ALTER TABLE {} DELETE WHERE tenant_id = {} AND key = {} AND bucket_time >= {} AND bucket_time < {}",
            self.facets_table(),
            quote_literal(&chunk.organization_id),
            quote_literal(&chunk.path),
            bucket_delete_start,
            bucket_delete_end
        ))
        .await?;
        self.clickhouse_exec_sync_mutation(&format!(
            "ALTER TABLE {} DELETE WHERE tenant_id = {} AND key = {} AND timestamp >= {} AND timestamp < {}",
            self.event_index_table(),
            quote_literal(&chunk.organization_id),
            quote_literal(&chunk.path),
            clickhouse_datetime_literal(chunk.chunk_start),
            clickhouse_datetime_literal(chunk.chunk_end)
        ))
        .await?;
        if aggregate_enabled {
            self.clickhouse_exec(&format!(
            "INSERT INTO {} (tenant_id, bucket_time, key, value, value_type, count, error_count) \
             SELECT {} AS tenant_id, toStartOfMinute(timestamp) AS bucket_time, {} AS key, value, {} AS value_type, count() AS count, sum(error) AS error_count \
             FROM (SELECT timestamp, {} AS value, if({}, 1, 0) AS error FROM {} \
                   WHERE tenant_id = {} AND timestamp >= {} AND timestamp < {}) \
             WHERE value != '' \
             GROUP BY bucket_time, value",
            self.facets_table(),
            quote_literal(&chunk.organization_id),
            quote_literal(&chunk.path),
            quote_literal(&chunk.value_type),
            value_expr,
            error_expr,
            self.events_table(),
            quote_literal(&chunk.organization_id),
            clickhouse_datetime_literal(chunk.chunk_start),
            clickhouse_datetime_literal(chunk.chunk_end)
            ))
            .await?;
        }
        if lookup_enabled {
            self.clickhouse_exec(&format!(
            "INSERT INTO {} \
             (tenant_id, key, value, value_type, timestamp, bucket_time, event_id, event_type, signal, trace_id, span_id, parent_span_id, name, start_time, end_time, duration_ms) \
             SELECT {}, {}, value, {}, timestamp, toStartOfMinute(timestamp), event_id, event_type, signal, trace_id, span_id, parent_span_id, name, start_time, end_time, duration_ms \
             FROM (SELECT timestamp, event_id, {} AS value, \
                    ifNull(toString(data.event_type), '') AS event_type, \
                    multiIf( \
                        ifNull(toString(data.event_type), '') IN ('span', 'span_start', 'span_end'), 'trace', \
                        ifNull(toString(data.event_type), '') = 'metric', 'metric', \
                        ifNull(toString(data.event_type), '') = 'log', 'log', \
                        ifNull(toString(data.event_type), '') IN ('analytics', 'track', 'page', 'screen', 'identify', 'group', 'alias'), 'analytics', \
                        'other' \
                    ) AS signal, \
                    ifNull(toString(data.trace_id), '') AS trace_id, ifNull(toString(data.span_id), '') AS span_id, \
                    ifNull(toString(data.parent_span_id), '') AS parent_span_id, ifNull(toString(data.name), '') AS name, \
                    parseDateTime64BestEffortOrNull(ifNull(toString(data.start_time), '')) AS start_time, \
                    parseDateTime64BestEffortOrNull(ifNull(toString(data.end_time), '')) AS end_time, \
                    toFloat64OrNull(toString(data.duration_ms)) AS duration_ms \
                   FROM {} WHERE tenant_id = {} AND timestamp >= {} AND timestamp < {}) \
             WHERE value != ''",
            self.event_index_table(),
            quote_literal(&chunk.organization_id),
            quote_literal(&chunk.path),
            quote_literal(&chunk.value_type),
            value_expr,
            self.events_table(),
            quote_literal(&chunk.organization_id),
            clickhouse_datetime_literal(chunk.chunk_start),
            clickhouse_datetime_literal(chunk.chunk_end)
            ))
            .await?;
            self.clickhouse_exec(&format!(
                "INSERT INTO {} (tenant_id, key, value, value_type, first_seen, last_seen) \
                 SELECT {} AS tenant_id, {} AS key, value, {} AS value_type, min(timestamp) AS first_seen, max(timestamp) AS last_seen \
                 FROM (SELECT timestamp, {} AS value FROM {} \
                       WHERE tenant_id = {} AND timestamp >= {} AND timestamp < {}) \
                 WHERE value != '' \
                 GROUP BY value",
                self.field_values_table(),
                quote_literal(&chunk.organization_id),
                quote_literal(&chunk.path),
                quote_literal(&chunk.value_type),
                value_expr,
                self.events_table(),
                quote_literal(&chunk.organization_id),
                clickhouse_datetime_literal(chunk.chunk_start),
                clickhouse_datetime_literal(chunk.chunk_end)
            ))
            .await?;
        }

        #[derive(Deserialize)]
        struct ChunkRows {
            rows: u64,
        }
        let rows = if lookup_enabled {
            let query = format!(
                "SELECT count() AS rows FROM {} WHERE tenant_id = {} AND key = {} AND timestamp >= {} AND timestamp < {}",
                self.event_index_table(),
                quote_literal(&chunk.organization_id),
                quote_literal(&chunk.path),
                clickhouse_datetime_literal(chunk.chunk_start),
                clickhouse_datetime_literal(chunk.chunk_end)
            );
            let response: ClickHouseResponse<ChunkRows> =
                serde_json::from_str(&self.clickhouse_query(&query).await?)?;
            response.data.first().map(|row| row.rows).unwrap_or(0)
        } else if aggregate_enabled {
            let query = format!(
                "SELECT ifNull(sum(count), 0) AS rows FROM {} WHERE tenant_id = {} AND key = {} AND bucket_time >= {} AND bucket_time < {}",
                self.facets_table(),
                quote_literal(&chunk.organization_id),
                quote_literal(&chunk.path),
                bucket_delete_start,
                bucket_delete_end
            );
            let response: ClickHouseResponse<ChunkRows> =
                serde_json::from_str(&self.clickhouse_query(&query).await?)?;
            response.data.first().map(|row| row.rows).unwrap_or(0)
        } else {
            0
        };
        sqlx::query(
            "UPDATE nanotrace_facet_backfill_chunks
             SET status = 'completed',
                 rows = $2,
                 error = '',
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 updated_at = now()
             WHERE chunk_id = $1",
        )
        .bind(chunk.chunk_id)
        .bind(rows as i64)
        .execute(self.pg_pool()?)
        .await?;
        Ok(())
    }

    async fn mark_backfill_chunk_failed(
        &self,
        chunk: &BackfillChunk,
        error: &str,
    ) -> Result<(), FacetError> {
        let status = if chunk.attempts >= 3 {
            "failed"
        } else {
            "queued"
        };
        sqlx::query(
            "UPDATE nanotrace_facet_backfill_chunks
             SET status = $2,
                 error = $3,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 updated_at = now()
             WHERE chunk_id = $1",
        )
        .bind(chunk.chunk_id)
        .bind(status)
        .bind(truncate_error(error))
        .execute(self.pg_pool()?)
        .await?;
        Ok(())
    }

    async fn refresh_backfill_job(&self, job_id: &str) -> Result<(), FacetError> {
        let progress = sqlx::query_as::<_, (String, i64, i64, i64, i64, Option<String>)>(
            "SELECT
                j.path,
                count(c.chunk_id)::bigint AS total_chunks,
                count(*) FILTER (WHERE c.status = 'completed')::bigint AS completed_chunks,
                count(*) FILTER (WHERE c.status = 'failed')::bigint AS failed_chunks,
                COALESCE(sum(c.rows), 0)::bigint AS indexed_events,
                max(NULLIF(c.error, '')) AS error
             FROM nanotrace_facet_backfill_jobs j
             LEFT JOIN nanotrace_facet_backfill_chunks c ON c.job_id = j.job_id
             WHERE j.job_id = $1
             GROUP BY j.job_id, j.path",
        )
        .bind(job_id)
        .fetch_optional(self.pg_pool()?)
        .await?;
        let Some((path, total_chunks, completed_chunks, failed_chunks, indexed_events, error)) =
            progress
        else {
            return Ok(());
        };
        let status = if failed_chunks > 0 {
            "failed"
        } else if completed_chunks >= total_chunks {
            "completed"
        } else {
            "running"
        };
        let values = if status == "completed" {
            self.count_backfill_values(&job_id, &path).await? as i64
        } else {
            0
        };
        sqlx::query(
            "UPDATE nanotrace_facet_backfill_jobs
             SET status = $2,
                 total_chunks = $3,
                 completed_chunks = $4,
                 failed_chunks = $5,
                 indexed_events = $6,
                 values = $7,
                 error = $8,
                 updated_at = now()
             WHERE job_id = $1",
        )
        .bind(job_id)
        .bind(status)
        .bind(total_chunks)
        .bind(completed_chunks)
        .bind(failed_chunks)
        .bind(indexed_events)
        .bind(values)
        .bind(error.unwrap_or_default())
        .execute(self.pg_pool()?)
        .await?;
        Ok(())
    }

    async fn count_backfill_values(&self, job_id: &str, path: &str) -> Result<u64, FacetError> {
        let tenant_id = sqlx::query_scalar::<_, String>(
            "SELECT organization_id FROM nanotrace_facet_backfill_jobs WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_one(self.pg_pool()?)
        .await?;
        #[derive(Deserialize)]
        struct ValueCount {
            values: u64,
        }
        let query = format!(
            "SELECT max(values) AS values \
             FROM (\
               SELECT uniqExact(value) AS values FROM {} WHERE tenant_id = {} AND key = {} \
               UNION ALL \
               SELECT uniqExact(value) AS values FROM {} WHERE tenant_id = {} AND key = {} \
               UNION ALL \
               SELECT uniqExact(value) AS values FROM {} WHERE tenant_id = {} AND key = {}\
             )",
            self.field_values_table(),
            quote_literal(&tenant_id),
            quote_literal(path),
            self.event_index_table(),
            quote_literal(&tenant_id),
            quote_literal(path),
            self.facets_table(),
            quote_literal(&tenant_id),
            quote_literal(path)
        );
        let response: ClickHouseResponse<ValueCount> =
            serde_json::from_str(&self.clickhouse_query(&query).await?)?;
        Ok(response.data.first().map(|row| row.values).unwrap_or(0))
    }

    async fn active_custom_dimensions(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<HotDimension>, FacetError> {
        let query = format!(
            "SELECT path, value_type, status, toBool(lookup_enabled) AS lookup_enabled, \
                    toBool(aggregate_enabled) AS aggregate_enabled, \
                    display_name, source, toBool(1) AS removable \
             FROM (SELECT path, value_type, status, lookup_enabled, aggregate_enabled, display_name, source, updated_at \
                   FROM {} WHERE tenant_id = {} ORDER BY updated_at DESC LIMIT 1 BY path) \
             WHERE status = 'active' AND source = 'user' \
             ORDER BY path ASC",
            self.hot_dimensions_table(),
            quote_literal(tenant_id)
        );
        let response: ClickHouseResponse<HotDimension> =
            serde_json::from_str(&self.clickhouse_query(&query).await?)?;
        Ok(response.data)
    }

    async fn insert_dimension(
        &self,
        tenant_id: &str,
        dimension: &HotDimension,
    ) -> Result<(), FacetError> {
        let body = serde_json::json!({
            "tenant_id": tenant_id,
            "path": dimension.path,
            "value_type": dimension.value_type,
            "status": dimension.status,
            "lookup_enabled": dimension.lookup_enabled,
            "aggregate_enabled": dimension.aggregate_enabled,
            "display_name": dimension.display_name,
            "source": dimension.source,
        });
        let query = format!(
            "INSERT INTO {} FORMAT JSONEachRow\n{}",
            self.hot_dimensions_table(),
            serde_json::to_string(&body)?
        );
        self.clickhouse_exec(&query).await
    }

    async fn ensure_table(&self) -> Result<(), FacetError> {
        self.table_ready
            .get_or_try_init(|| async { self.create_table().await })
            .await?;
        Ok(())
    }

    async fn ensure_control_plane(&self) -> Result<(), FacetError> {
        self.control_ready
            .get_or_try_init(|| async { self.create_control_plane().await })
            .await?;
        Ok(())
    }

    fn pg_pool(&self) -> Result<&PgPool, FacetError> {
        self.pg.as_ref().ok_or(FacetError::PostgresNotConfigured)
    }

    async fn create_control_plane(&self) -> Result<(), FacetError> {
        let pg = self.pg_pool()?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nanotrace_facet_backfill_jobs (
                job_id text PRIMARY KEY,
                organization_id text NOT NULL DEFAULT 'org_default',
                path text NOT NULL,
                value_type text NOT NULL,
                status text NOT NULL,
                total_chunks bigint NOT NULL DEFAULT 0,
                completed_chunks bigint NOT NULL DEFAULT 0,
                failed_chunks bigint NOT NULL DEFAULT 0,
                indexed_events bigint NOT NULL DEFAULT 0,
                values bigint NOT NULL DEFAULT 0,
                error text NOT NULL DEFAULT '',
                created_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now()
            )",
        )
        .execute(pg)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nanotrace_facet_backfill_chunks (
                chunk_id bigserial PRIMARY KEY,
                job_id text NOT NULL REFERENCES nanotrace_facet_backfill_jobs(job_id) ON DELETE CASCADE,
                organization_id text NOT NULL DEFAULT 'org_default',
                path text NOT NULL,
                chunk_start timestamptz NOT NULL,
                chunk_end timestamptz NOT NULL,
                status text NOT NULL DEFAULT 'queued',
                attempts integer NOT NULL DEFAULT 0,
                rows bigint NOT NULL DEFAULT 0,
                error text NOT NULL DEFAULT '',
                lease_owner text,
                lease_expires_at timestamptz,
                updated_at timestamptz NOT NULL DEFAULT now(),
                UNIQUE (job_id, chunk_start)
            )",
        )
        .execute(pg)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS nanotrace_facet_backfill_chunks_claim_idx
             ON nanotrace_facet_backfill_chunks (status, lease_expires_at, updated_at, chunk_id)",
        )
        .execute(pg)
        .await?;
        Ok(())
    }

    async fn create_table(&self) -> Result<(), FacetError> {
        let hot_dimensions = format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             tenant_id String DEFAULT 'org_default', \
             path String, \
             value_type LowCardinality(String), \
             status LowCardinality(String), \
             lookup_enabled UInt8 DEFAULT 1, \
             aggregate_enabled UInt8 DEFAULT 1, \
             display_name String DEFAULT '', \
             source LowCardinality(String) DEFAULT 'user', \
             created_at DateTime64(3, 'UTC') DEFAULT now64(3), \
             updated_at DateTime64(3, 'UTC') DEFAULT now64(3), \
             created_by String DEFAULT '', \
             error String DEFAULT ''\
             ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (tenant_id, path)",
            self.hot_dimensions_table()
        );
        self.clickhouse_exec(&hot_dimensions).await?;
        for column in [
            "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS lookup_enabled UInt8 DEFAULT 1",
            "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS aggregate_enabled UInt8 DEFAULT 1",
        ] {
            self.clickhouse_exec(&column.replace("{table}", &self.hot_dimensions_table()))
                .await?;
        }
        Ok(())
    }

    async fn clickhouse_query(&self, query: &str) -> Result<String, FacetError> {
        self.clickhouse_request(query, true).await
    }

    async fn clickhouse_exec(&self, query: &str) -> Result<(), FacetError> {
        self.clickhouse_request(query, false).await?;
        Ok(())
    }

    async fn clickhouse_exec_sync_mutation(&self, query: &str) -> Result<(), FacetError> {
        self.clickhouse_request_with_params(query, false, &[("mutations_sync", "2")])
            .await?;
        Ok(())
    }

    async fn clickhouse_request(&self, query: &str, json: bool) -> Result<String, FacetError> {
        self.clickhouse_request_with_params(query, json, &[]).await
    }

    async fn clickhouse_request_with_params(
        &self,
        query: &str,
        json: bool,
        params: &[(&str, &str)],
    ) -> Result<String, FacetError> {
        let url = self
            .cfg
            .clickhouse_url
            .as_deref()
            .ok_or(FacetError::ClickHouseNotConfigured)?;
        let mut request = self
            .http
            .post(url)
            .query(&[
                ("database", self.cfg.clickhouse_database.as_str()),
                ("type_json_skip_duplicated_paths", "1"),
            ])
            .query(params)
            .body(if json {
                format!("{query} FORMAT JSON")
            } else {
                query.to_string()
            });

        if let Some(user) = self.cfg.clickhouse_user.as_deref() {
            request = request.basic_auth(user, self.cfg.clickhouse_password.as_deref());
        }

        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(FacetError::ClickHouseResponse { status, body });
        }
        Ok(body)
    }

    fn events_table(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.cfg.clickhouse_database),
            quote_identifier(&self.cfg.clickhouse_table)
        )
    }

    fn facets_table(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.cfg.clickhouse_database),
            quote_identifier(&self.cfg.clickhouse_facets_table)
        )
    }

    fn event_index_table(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.cfg.clickhouse_database),
            quote_identifier(&self.cfg.clickhouse_event_index_table)
        )
    }

    fn field_values_table(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.cfg.clickhouse_database),
            quote_identifier(&self.cfg.clickhouse_field_values_table)
        )
    }

    fn hot_dimensions_table(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.cfg.clickhouse_database),
            quote_identifier(&self.cfg.clickhouse_hot_dimensions_table)
        )
    }
}

fn builtin_dimensions() -> Vec<HotDimension> {
    [
        ("trace_id", "string", "traceId", true, false),
        ("span_id", "string", "spanId", true, false),
        ("parent_span_id", "string", "parentSpanId", true, false),
        ("event_id", "string", "eventId", true, false),
        ("request_id", "string", "requestId", true, false),
        (
            "tenant_id",
            "low_cardinality_string",
            "tenant_id",
            true,
            true,
        ),
        ("service", "low_cardinality_string", "service", true, true),
        (
            "environment",
            "low_cardinality_string",
            "environment",
            true,
            true,
        ),
        (
            "event_type",
            "low_cardinality_string",
            "event_type",
            true,
            true,
        ),
        ("signal", "low_cardinality_string", "signal", true, true),
        ("name", "low_cardinality_string", "name", true, true),
        ("user_id", "string", "user_id", true, false),
        ("session_id", "string", "session_id", true, false),
        ("account_id", "string", "account_id", true, false),
        (
            "http.route",
            "low_cardinality_string",
            "http.route",
            true,
            true,
        ),
        (
            "http.method",
            "low_cardinality_string",
            "http.method",
            true,
            true,
        ),
        (
            "http.status_code",
            "integer",
            "http.status_code",
            true,
            true,
        ),
        (
            "severity_text",
            "low_cardinality_string",
            "severity_text",
            true,
            true,
        ),
        (
            "metric_name",
            "low_cardinality_string",
            "metric_name",
            true,
            true,
        ),
    ]
    .into_iter()
    .map(
        |(path, value_type, display_name, lookup_enabled, aggregate_enabled)| HotDimension {
            path: path.to_string(),
            value_type: value_type.to_string(),
            status: "active".to_string(),
            lookup_enabled,
            aggregate_enabled,
            display_name: display_name.to_string(),
            source: "builtin".to_string(),
            removable: false,
        },
    )
    .collect()
}

fn default_true() -> bool {
    true
}

fn validate_path(path: &str) -> Result<String, FacetError> {
    let path = path.trim().strip_prefix("data.").unwrap_or(path.trim());
    if path.is_empty()
        || path.len() > 256
        || path.starts_with('.')
        || path.ends_with('.')
        || path.contains("..")
        || !path
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' || ch == '-')
    {
        return Err(FacetError::InvalidPath);
    }
    Ok(path.to_string())
}

fn validate_job_id(job_id: &str) -> Result<String, FacetError> {
    let job_id = job_id.trim();
    if job_id.is_empty()
        || job_id.len() > 128
        || !job_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        return Err(FacetError::InvalidPath);
    }
    Ok(job_id.to_string())
}

fn backfill_response_from_row(
    row: (String, String, String, i64, i64, i64, i64, i64, String),
) -> FacetBackfillResponse {
    let (
        job_id,
        path,
        status,
        total_chunks,
        completed_chunks,
        failed_chunks,
        indexed_events,
        values,
        error,
    ) = row;
    FacetBackfillResponse {
        job_id,
        path,
        status,
        total_chunks: nonnegative_u64(total_chunks),
        completed_chunks: nonnegative_u64(completed_chunks),
        failed_chunks: nonnegative_u64(failed_chunks),
        indexed_events: nonnegative_u64(indexed_events),
        values: nonnegative_u64(values),
        error,
    }
}

fn nonnegative_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn backfill_chunks(min_ms: i64, max_ms: i64) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
    if min_ms <= 0 || max_ms < min_ms {
        return Vec::new();
    }
    let chunk_ms = 60 * 60 * 1_000;
    let mut chunks = Vec::new();
    let mut start_ms = min_ms;
    let exclusive_max_ms = max_ms.saturating_add(1);
    while start_ms < exclusive_max_ms {
        let end_ms = start_ms.saturating_add(chunk_ms).min(exclusive_max_ms);
        let Some(start) = DateTime::<Utc>::from_timestamp_millis(start_ms) else {
            break;
        };
        let Some(end) = DateTime::<Utc>::from_timestamp_millis(end_ms) else {
            break;
        };
        chunks.push((start, end));
        start_ms = end_ms;
    }
    chunks
}

fn backfill_job_id(organization_id: &str, path: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let timestamp = chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp_micros() * 1_000);
    let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "bf_{:x}_{:x}_{:x}",
        timestamp,
        std::process::id(),
        sequence ^ stable_path_hash(&format!("{organization_id}:{path}"))
    )
}

fn backfill_worker_id() -> String {
    format!(
        "server-{}-{}",
        std::process::id(),
        stable_path_hash("worker")
    )
}

fn clickhouse_datetime_literal(value: DateTime<Utc>) -> String {
    quote_literal(&value.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
}

fn clickhouse_minute_bucket_bounds(
    start: DateTime<Utc>,
    end_exclusive: DateTime<Utc>,
) -> (String, String) {
    const MINUTE_MS: i64 = 60_000;
    let start_ms = start.timestamp_millis();
    let last_included_ms = end_exclusive.timestamp_millis().saturating_sub(1);
    let bucket_start_ms = start_ms - start_ms.rem_euclid(MINUTE_MS);
    let bucket_end_ms = last_included_ms - last_included_ms.rem_euclid(MINUTE_MS) + MINUTE_MS;
    (
        clickhouse_datetime_literal(
            DateTime::<Utc>::from_timestamp_millis(bucket_start_ms).unwrap_or(start),
        ),
        clickhouse_datetime_literal(
            DateTime::<Utc>::from_timestamp_millis(bucket_end_ms).unwrap_or(end_exclusive),
        ),
    )
}

fn truncate_error(error: &str) -> String {
    error.chars().take(4096).collect()
}

fn stable_path_hash(path: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in path.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn validate_value_type(value_type: &str) -> Result<&'static str, FacetError> {
    match value_type {
        "string" => Ok("string"),
        "low_cardinality_string" => Ok("low_cardinality_string"),
        "float" | "number" => Ok("float"),
        "integer" => Ok("integer"),
        "unsigned" => Ok("unsigned"),
        "bool" => Ok("bool"),
        "datetime" => Ok("datetime"),
        _ => Err(FacetError::InvalidValueType),
    }
}

fn event_index_value_type(value_type: &str) -> Result<&'static str, FacetError> {
    match validate_value_type(value_type)? {
        "string" | "low_cardinality_string" | "datetime" => Ok("string"),
        "float" | "integer" | "unsigned" => Ok("number"),
        "bool" => Ok("bool"),
        _ => Err(FacetError::InvalidValueType),
    }
}

fn facet_value_expression(path: &str) -> String {
    format!("ifNull(toString(data.{}), '')", quote_identifier(path))
}

fn error_count_expression() -> &'static str {
    "lowerUTF8(ifNull(toString(data.is_error), '')) IN ('1', 'true') \
     OR lowerUTF8(ifNull(toString(data.span_status_code), '')) = 'error' \
     OR endsWith(lowerUTF8(ifNull(toString(data.event_type), '')), '_error')"
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}
