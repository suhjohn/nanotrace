use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::OnceCell;

use crate::config::Config;

#[derive(Clone)]
pub struct ReportStore {
    pg: Option<PgPool>,
    ready: Arc<OnceCell<()>>,
}

#[derive(Debug, Deserialize)]
pub struct CreateReportRequest {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub config: Value,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Serialize)]
pub struct ReportRecord {
    pub report_id: String,
    pub name: String,
    pub kind: String,
    pub config: Value,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: i64,
}

#[derive(Debug, Serialize)]
pub struct ReportListResponse {
    pub reports: Vec<ReportRecord>,
}

#[derive(Debug, thiserror::Error)]
pub enum ReportStoreError {
    #[error("Postgres is not configured")]
    PostgresNotConfigured,
    #[error("invalid report id")]
    InvalidId,
    #[error("invalid report name")]
    InvalidName,
    #[error("invalid report kind")]
    InvalidKind,
    #[error("invalid report config")]
    InvalidConfig,
    #[error("report not found")]
    NotFound,
    #[error("Postgres request failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("invalid report JSON: {0}")]
    Json(#[from] serde_json::Error),
}

impl ReportStore {
    pub async fn connect(cfg: Arc<Config>) -> Result<Self, ReportStoreError> {
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
            pg,
            ready: Arc::new(OnceCell::new()),
        })
    }

    pub async fn list(&self, organization_id: &str) -> Result<Vec<ReportRecord>, ReportStoreError> {
        let organization_id = validate_id(organization_id, ReportStoreError::InvalidId)?;
        self.ensure_table().await?;
        let rows = sqlx::query_as::<_, ReportRow>(
            "SELECT report_id, name, kind, config::text AS config, enabled, created_at, updated_at, version
             FROM nanotrace_reports
             WHERE organization_id = $1 AND deleted_at IS NULL
             ORDER BY updated_at DESC, report_id ASC",
        )
        .bind(organization_id)
        .fetch_all(self.pg()?)
        .await?;
        rows.into_iter().map(ReportRecord::try_from).collect()
    }

    pub async fn create(
        &self,
        organization_id: &str,
        request: CreateReportRequest,
    ) -> Result<ReportRecord, ReportStoreError> {
        let organization_id = validate_id(organization_id, ReportStoreError::InvalidId)?;
        let name = validate_name(&request.name)?;
        let kind = validate_kind(&request.kind)?;
        if !request.config.is_object() {
            return Err(ReportStoreError::InvalidConfig);
        }
        let version = Utc::now().timestamp_millis().max(0);
        let report_id = format!("rep_{}_{}", slug(name), version);
        let config =
            serde_json::to_string(&request.config).map_err(|_| ReportStoreError::InvalidConfig)?;
        self.ensure_table().await?;
        let row = sqlx::query_as::<_, ReportRow>(
            "INSERT INTO nanotrace_reports
                (organization_id, report_id, name, kind, config, enabled, created_at, updated_at, version)
             VALUES ($1, $2, $3, $4, $5::jsonb, $6, now(), now(), $7)
             RETURNING report_id, name, kind, config::text AS config, enabled, created_at, updated_at, version",
        )
        .bind(organization_id)
        .bind(report_id)
        .bind(name)
        .bind(kind)
        .bind(config)
        .bind(request.enabled)
        .bind(version)
        .fetch_one(self.pg()?)
        .await?;
        ReportRecord::try_from(row)
    }

    pub async fn delete(
        &self,
        organization_id: &str,
        report_id: &str,
    ) -> Result<ReportRecord, ReportStoreError> {
        let organization_id = validate_id(organization_id, ReportStoreError::InvalidId)?;
        let report_id = validate_id(report_id, ReportStoreError::InvalidId)?;
        self.ensure_table().await?;
        let row = sqlx::query_as::<_, ReportRow>(
            "UPDATE nanotrace_reports
             SET enabled = false, deleted_at = now(), updated_at = now()
             WHERE organization_id = $1 AND report_id = $2 AND deleted_at IS NULL
             RETURNING report_id, name, kind, config::text AS config, enabled, created_at, updated_at, version",
        )
        .bind(organization_id)
        .bind(report_id)
        .fetch_optional(self.pg()?)
        .await?;
        row.map(ReportRecord::try_from)
            .transpose()?
            .ok_or(ReportStoreError::NotFound)
    }

    async fn ensure_table(&self) -> Result<(), ReportStoreError> {
        self.ready
            .get_or_try_init(|| async {
                sqlx::query(
                    "CREATE TABLE IF NOT EXISTS nanotrace_reports (
                        organization_id text NOT NULL DEFAULT 'org_default',
                        report_id text NOT NULL,
                        name text NOT NULL,
                        kind text NOT NULL,
                        config jsonb NOT NULL DEFAULT '{}'::jsonb,
                        enabled boolean NOT NULL DEFAULT true,
                        created_at timestamptz NOT NULL DEFAULT now(),
                        updated_at timestamptz NOT NULL DEFAULT now(),
                        deleted_at timestamptz,
                        version bigint NOT NULL DEFAULT 0,
                        PRIMARY KEY (organization_id, report_id)
                    )",
                )
                .execute(self.pg()?)
                .await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS nanotrace_reports_org_idx
                     ON nanotrace_reports (organization_id, updated_at DESC)
                     WHERE deleted_at IS NULL",
                )
                .execute(self.pg()?)
                .await?;
                Ok::<(), ReportStoreError>(())
            })
            .await
            .copied()
    }

    fn pg(&self) -> Result<&PgPool, ReportStoreError> {
        self.pg
            .as_ref()
            .ok_or(ReportStoreError::PostgresNotConfigured)
    }
}

#[derive(Debug, sqlx::FromRow)]
struct ReportRow {
    report_id: String,
    name: String,
    kind: String,
    config: String,
    enabled: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    version: i64,
}

impl TryFrom<ReportRow> for ReportRecord {
    type Error = ReportStoreError;

    fn try_from(row: ReportRow) -> Result<Self, Self::Error> {
        Ok(Self {
            report_id: row.report_id,
            name: row.name,
            kind: row.kind,
            config: serde_json::from_str(&row.config)?,
            enabled: row.enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
            version: row.version,
        })
    }
}

fn default_enabled() -> bool {
    true
}

fn validate_name(value: &str) -> Result<&str, ReportStoreError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 160 {
        return Err(ReportStoreError::InvalidName);
    }
    Ok(value)
}

fn validate_kind(value: &str) -> Result<&str, ReportStoreError> {
    match value.trim() {
        "summary" | "sequence" | "cohort" | "retention" => Ok(value.trim()),
        _ => Err(ReportStoreError::InvalidKind),
    }
}

fn validate_id(value: &str, err: ReportStoreError) -> Result<&str, ReportStoreError> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 180
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(err);
    }
    Ok(value)
}

fn slug(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}
