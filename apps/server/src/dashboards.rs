use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::OnceCell;

use crate::config::Config;

#[derive(Clone)]
pub struct DashboardStore {
    pg: Option<PgPool>,
    ready: Arc<OnceCell<()>>,
}

#[derive(Debug, Deserialize)]
pub struct CreateVisualizationRequest {
    pub id: String,
    pub height: i32,
    #[serde(default, rename = "parameterBindings")]
    pub parameter_bindings: Vec<String>,
    #[serde(rename = "sourceCode")]
    pub source_code: String,
    pub title: String,
    pub width: i32,
    pub x: i32,
    pub y: i32,
}

#[derive(Debug, Deserialize)]
pub struct UpdateVisualizationRequest {
    pub height: i32,
    #[serde(default, rename = "parameterBindings")]
    pub parameter_bindings: Vec<String>,
    #[serde(rename = "sourceCode")]
    pub source_code: String,
    pub title: String,
    pub width: i32,
    pub x: i32,
    pub y: i32,
}

#[derive(Debug, Serialize)]
pub struct DashboardVisualization {
    #[serde(rename = "dashboardId")]
    pub dashboard_id: String,
    pub height: i32,
    pub id: String,
    #[serde(rename = "parameterBindings")]
    pub parameter_bindings: Vec<String>,
    #[serde(rename = "sourceCode")]
    pub source_code: String,
    pub title: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: DateTime<Utc>,
    pub width: i32,
    pub x: i32,
    pub y: i32,
}

#[derive(Debug, Serialize)]
pub struct DashboardVisualizationsResponse {
    pub visualizations: Vec<DashboardVisualization>,
}

#[derive(Debug, thiserror::Error)]
pub enum DashboardError {
    #[error("Postgres is not configured")]
    PostgresNotConfigured,
    #[error("invalid visualization id")]
    InvalidId,
    #[error("invalid dashboard id")]
    InvalidDashboardId,
    #[error("visualization title is required")]
    MissingTitle,
    #[error("visualization source code is required")]
    MissingSourceCode,
    #[error("invalid visualization dimensions")]
    InvalidDimensions,
    #[error("visualization not found")]
    NotFound,
    #[error("Postgres request failed: {0}")]
    Database(#[from] sqlx::Error),
}

impl DashboardStore {
    pub async fn connect(cfg: Arc<Config>) -> Result<Self, DashboardError> {
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

    pub async fn list(
        &self,
        organization_id: &str,
        dashboard_id: &str,
    ) -> Result<Vec<DashboardVisualization>, DashboardError> {
        let organization_id = validate_id(organization_id, DashboardError::InvalidDashboardId)?;
        let dashboard_id = validate_id(dashboard_id, DashboardError::InvalidDashboardId)?;
        self.ensure_table().await?;
        let rows = sqlx::query_as::<_, VisualizationRow>(
            "SELECT dashboard_id, id, title, source_code, parameter_bindings, x, y, width, height, updated_at
             FROM nanotrace_dashboard_visualizations
             WHERE organization_id = $1 AND dashboard_id = $2
             ORDER BY y ASC, x ASC, id ASC",
        )
        .bind(organization_id)
        .bind(dashboard_id)
        .fetch_all(self.pg()?)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn create(
        &self,
        organization_id: &str,
        dashboard_id: &str,
        request: CreateVisualizationRequest,
    ) -> Result<DashboardVisualization, DashboardError> {
        let organization_id = validate_id(organization_id, DashboardError::InvalidDashboardId)?;
        let dashboard_id = validate_id(dashboard_id, DashboardError::InvalidDashboardId)?;
        let id = validate_id(&request.id, DashboardError::InvalidId)?;
        validate_visualization(
            &request.title,
            &request.source_code,
            request.width,
            request.height,
        )?;
        let parameter_bindings = validate_parameter_bindings(&request.parameter_bindings);
        self.ensure_table().await?;
        let row = sqlx::query_as::<_, VisualizationRow>(
            "INSERT INTO nanotrace_dashboard_visualizations
                (organization_id, dashboard_id, id, title, source_code, parameter_bindings, x, y, width, height, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
             ON CONFLICT (organization_id, dashboard_id, id) DO UPDATE
             SET title = EXCLUDED.title,
                 source_code = EXCLUDED.source_code,
                 parameter_bindings = EXCLUDED.parameter_bindings,
                 x = EXCLUDED.x,
                 y = EXCLUDED.y,
                 width = EXCLUDED.width,
                 height = EXCLUDED.height,
                 updated_at = now()
             RETURNING dashboard_id, id, title, source_code, parameter_bindings, x, y, width, height, updated_at",
        )
        .bind(organization_id)
        .bind(dashboard_id)
        .bind(id)
        .bind(request.title.trim())
        .bind(request.source_code)
        .bind(parameter_bindings)
        .bind(request.x.max(0))
        .bind(request.y.max(0))
        .bind(request.width)
        .bind(request.height)
        .fetch_one(self.pg()?)
        .await?;
        Ok(row.into())
    }

    pub async fn update(
        &self,
        organization_id: &str,
        dashboard_id: &str,
        id: &str,
        request: UpdateVisualizationRequest,
    ) -> Result<DashboardVisualization, DashboardError> {
        let organization_id = validate_id(organization_id, DashboardError::InvalidDashboardId)?;
        let dashboard_id = validate_id(dashboard_id, DashboardError::InvalidDashboardId)?;
        let id = validate_id(id, DashboardError::InvalidId)?;
        validate_visualization(
            &request.title,
            &request.source_code,
            request.width,
            request.height,
        )?;
        let parameter_bindings = validate_parameter_bindings(&request.parameter_bindings);
        self.ensure_table().await?;
        let row = sqlx::query_as::<_, VisualizationRow>(
            "UPDATE nanotrace_dashboard_visualizations
             SET title = $4,
                 source_code = $5,
                 parameter_bindings = $6,
                 x = $7,
                 y = $8,
                 width = $9,
                 height = $10,
                 updated_at = now()
             WHERE organization_id = $1 AND dashboard_id = $2 AND id = $3
             RETURNING dashboard_id, id, title, source_code, parameter_bindings, x, y, width, height, updated_at",
        )
        .bind(organization_id)
        .bind(dashboard_id)
        .bind(id)
        .bind(request.title.trim())
        .bind(request.source_code)
        .bind(parameter_bindings)
        .bind(request.x.max(0))
        .bind(request.y.max(0))
        .bind(request.width)
        .bind(request.height)
        .fetch_one(self.pg()?)
        .await?;
        Ok(row.into())
    }

    pub async fn clear(
        &self,
        organization_id: &str,
        dashboard_id: &str,
    ) -> Result<(), DashboardError> {
        let organization_id = validate_id(organization_id, DashboardError::InvalidDashboardId)?;
        let dashboard_id = validate_id(dashboard_id, DashboardError::InvalidDashboardId)?;
        self.ensure_table().await?;
        sqlx::query("DELETE FROM nanotrace_dashboard_visualizations WHERE organization_id = $1 AND dashboard_id = $2")
            .bind(organization_id)
            .bind(dashboard_id)
            .execute(self.pg()?)
            .await?;
        Ok(())
    }

    pub async fn delete(
        &self,
        organization_id: &str,
        dashboard_id: &str,
        id: &str,
    ) -> Result<DashboardVisualization, DashboardError> {
        let organization_id = validate_id(organization_id, DashboardError::InvalidDashboardId)?;
        let dashboard_id = validate_id(dashboard_id, DashboardError::InvalidDashboardId)?;
        let id = validate_id(id, DashboardError::InvalidId)?;
        self.ensure_table().await?;
        let row = sqlx::query_as::<_, VisualizationRow>(
            "DELETE FROM nanotrace_dashboard_visualizations
             WHERE organization_id = $1 AND dashboard_id = $2 AND id = $3
             RETURNING dashboard_id, id, title, source_code, parameter_bindings, x, y, width, height, updated_at",
        )
        .bind(organization_id)
        .bind(dashboard_id)
        .bind(id)
        .fetch_optional(self.pg()?)
        .await?;
        row.map(Into::into).ok_or(DashboardError::NotFound)
    }

    async fn ensure_table(&self) -> Result<(), DashboardError> {
        self.ready
            .get_or_try_init(|| async {
                sqlx::query(
                    "CREATE TABLE IF NOT EXISTS nanotrace_dashboard_visualizations (
                        organization_id text NOT NULL DEFAULT 'org_default',
                        dashboard_id text NOT NULL,
                        id text NOT NULL,
                        title text NOT NULL,
                        source_code text NOT NULL,
                        parameter_bindings text[] NOT NULL DEFAULT '{}',
                        x integer NOT NULL,
                        y integer NOT NULL,
                        width integer NOT NULL,
                        height integer NOT NULL,
                        updated_at timestamptz NOT NULL DEFAULT now(),
                        PRIMARY KEY (organization_id, dashboard_id, id)
                    )",
                )
                .execute(self.pg()?)
                .await?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS nanotrace_dashboard_visualizations_dashboard_idx
                     ON nanotrace_dashboard_visualizations (organization_id, dashboard_id, y, x)",
                )
                .execute(self.pg()?)
                .await?;
                sqlx::query(
                    "CREATE UNIQUE INDEX IF NOT EXISTS nanotrace_dashboard_visualizations_org_key
                     ON nanotrace_dashboard_visualizations (organization_id, dashboard_id, id)",
                )
                .execute(self.pg()?)
                .await?;
                Ok::<(), DashboardError>(())
            })
            .await
            .copied()
    }

    fn pg(&self) -> Result<&PgPool, DashboardError> {
        self.pg
            .as_ref()
            .ok_or(DashboardError::PostgresNotConfigured)
    }
}

#[derive(Debug, sqlx::FromRow)]
struct VisualizationRow {
    dashboard_id: String,
    height: i32,
    id: String,
    source_code: String,
    parameter_bindings: Vec<String>,
    title: String,
    updated_at: DateTime<Utc>,
    width: i32,
    x: i32,
    y: i32,
}

impl From<VisualizationRow> for DashboardVisualization {
    fn from(row: VisualizationRow) -> Self {
        Self {
            dashboard_id: row.dashboard_id,
            height: row.height,
            id: row.id,
            parameter_bindings: row.parameter_bindings,
            source_code: row.source_code,
            title: row.title,
            updated_at: row.updated_at,
            width: row.width,
            x: row.x,
            y: row.y,
        }
    }
}

fn validate_visualization(
    title: &str,
    source_code: &str,
    width: i32,
    height: i32,
) -> Result<(), DashboardError> {
    if title.trim().is_empty() {
        return Err(DashboardError::MissingTitle);
    }
    if source_code.trim().is_empty() {
        return Err(DashboardError::MissingSourceCode);
    }
    if !(1..=500).contains(&width) || !(1..=1000).contains(&height) {
        return Err(DashboardError::InvalidDimensions);
    }
    Ok(())
}

fn validate_parameter_bindings(values: &[String]) -> Vec<String> {
    let mut bindings = Vec::new();
    for value in values {
        let normalized = match value.as_str() {
            "timeRange" | "filter" | "groupBy" => value.as_str(),
            _ => continue,
        };
        if !bindings.iter().any(|existing| existing == normalized) {
            bindings.push(normalized.to_owned());
        }
    }
    bindings
}

fn validate_id(value: &str, err: DashboardError) -> Result<&str, DashboardError> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 120
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(err);
    }
    Ok(value)
}
