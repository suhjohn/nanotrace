use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use http::{
    HeaderMap, HeaderValue, StatusCode,
    header::{AUTHORIZATION, COOKIE, SET_COOKIE},
};
use rand::RngCore;
use regex::Regex;
use serde::Serialize;
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction, postgres::PgPoolOptions};
use thiserror::Error;

pub const DEFAULT_ORGANIZATION_ID: &str = "org_default";

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub postgres_url: Option<String>,
    pub bootstrap_api_key: Option<String>,
    pub public_base_url: Option<String>,
    pub session_cookie_name: String,
    pub session_ttl: Duration,
    pub session_secure: bool,
    pub magic_link_ttl: Duration,
    pub allowed_emails: Vec<String>,
    pub admin_emails: Vec<String>,
}

impl AuthConfig {
    pub fn enabled(&self) -> bool {
        self.postgres_url.is_some()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthIdentity {
    pub auth_type: AuthType,
    pub subject: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub role: AuthRole,
    pub tenant_id: String,
    pub organization_id: String,
    pub organization_name: String,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthType {
    ApiKey,
    Session,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthRole {
    Viewer,
    Admin,
    Service,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiKeyRecord {
    pub id: i64,
    pub organization_id: String,
    pub name: String,
    pub prefix: String,
    pub role: AuthRole,
    pub scopes: Vec<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreatedApiKey {
    pub key: String,
    pub api_key: ApiKeyRecord,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrganizationRecord {
    pub id: String,
    pub slug: String,
    pub name: String,
    pub plan: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrganizationDataPlaneRecord {
    pub organization_id: String,
    pub mode: String,
    pub provider: String,
    pub region: String,
    pub public_base_url: String,
    pub ingest_url: String,
    pub query_url: String,
    pub internal_secret_ref: String,
    pub s3_bucket: String,
    pub processor_prefix: String,
    pub clickhouse_mode: String,
    pub clickhouse_provider: String,
    pub clickhouse_region: String,
    pub clickhouse_service_id: String,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub kms_key_arn: String,
    pub status: String,
    pub status_message: String,
    pub last_provisioning_job_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DataPlaneProvisionJobRecord {
    pub id: String,
    pub organization_id: String,
    pub kind: String,
    pub status: String,
    pub provider: String,
    pub region: String,
    pub clickhouse_mode: String,
    pub clickhouse_region: String,
    pub request: JsonValue,
    pub result: Option<JsonValue>,
    pub error: Option<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrganizationInviteRecord {
    pub id: i64,
    pub organization_id: String,
    pub email: String,
    pub role: AuthRole,
    pub invited_by: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub accepted_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreatedOrganizationInvite {
    pub invite: OrganizationInviteRecord,
    #[serde(skip_serializing)]
    pub token: String,
}

#[derive(Debug, Clone)]
pub struct CreateOrganizationInput {
    pub slug: String,
    pub name: String,
    pub plan: String,
}

#[derive(Debug, Clone)]
pub struct UpsertOrganizationDataPlaneInput {
    pub mode: String,
    pub provider: String,
    pub region: String,
    pub public_base_url: String,
    pub ingest_url: String,
    pub query_url: String,
    pub internal_secret_ref: String,
    pub s3_bucket: String,
    pub processor_prefix: String,
    pub clickhouse_mode: String,
    pub clickhouse_provider: String,
    pub clickhouse_region: String,
    pub clickhouse_service_id: String,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub kms_key_arn: String,
    pub status: String,
    pub status_message: String,
    pub last_provisioning_job_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CreateDataPlaneProvisionJobInput {
    pub provider: String,
    pub region: String,
    pub clickhouse_mode: String,
    pub clickhouse_region: String,
}

#[derive(Debug, Clone)]
pub struct CompleteDataPlaneProvisionJobInput {
    pub status: String,
    pub result: Option<JsonValue>,
    pub error: Option<String>,
    pub data_plane: Option<UpsertOrganizationDataPlaneInput>,
}

#[derive(Debug, Clone)]
struct ValidatedOrganizationDataPlaneInput {
    mode: String,
    provider: String,
    region: String,
    public_base_url: String,
    ingest_url: String,
    query_url: String,
    internal_secret_ref: String,
    s3_bucket: String,
    processor_prefix: String,
    clickhouse_mode: String,
    clickhouse_provider: String,
    clickhouse_region: String,
    clickhouse_service_id: String,
    clickhouse_url: String,
    clickhouse_database: String,
    kms_key_arn: String,
    status: String,
    status_message: String,
    last_provisioning_job_id: Option<String>,
}

#[derive(Clone)]
pub struct AuthStore {
    cfg: AuthConfig,
    pool: PgPool,
    api_key_cache: Arc<RwLock<ApiKeyCache>>,
}

#[derive(Debug, Default)]
struct ApiKeyCache {
    loaded_at: Option<Instant>,
    keys: HashMap<String, CachedApiKey>,
}

#[derive(Debug, Clone)]
struct CachedApiKey {
    name: String,
    role: String,
    scopes: Vec<String>,
    organization_id: String,
    organization_name: String,
}

impl AuthStore {
    pub async fn connect(cfg: AuthConfig) -> Result<Option<Self>, AuthError> {
        let Some(postgres_url) = cfg.postgres_url.clone() else {
            return Ok(None);
        };
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(&postgres_url)
            .await?;
        let store = Self {
            cfg,
            pool,
            api_key_cache: Arc::new(RwLock::new(ApiKeyCache::default())),
        };
        store.ensure_schema().await?;
        store.ensure_bootstrap_api_key().await?;
        Ok(Some(store))
    }

    pub async fn start_login(
        &self,
        email: &str,
        return_to: Option<&str>,
    ) -> Result<LoginStart, AuthError> {
        let public_base_url = required_cfg(
            self.cfg.public_base_url.as_deref(),
            "NANOTRACE_PUBLIC_BASE_URL",
        )?;
        let email = normalize_email(email)?;
        self.ensure_email_allowed(&email)?;
        let token = random_token();
        let token_hash = token_hash(&token);
        let expires_at = Utc::now()
            + chrono::Duration::from_std(self.cfg.magic_link_ttl)
                .unwrap_or_else(|_| chrono::Duration::minutes(10));
        let return_to = safe_return_to(return_to);
        sqlx::query(
            "INSERT INTO nanotrace_magic_links (token_hash, email, return_to, expires_at)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&token_hash)
        .bind(&email)
        .bind(&return_to)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;

        Ok(LoginStart {
            email,
            login_url: format!(
                "{}/auth/callback?token={}",
                public_base_url.trim_end_matches('/'),
                token
            ),
            expires_at,
        })
    }

    pub async fn complete_login(&self, token: &str) -> Result<LoginComplete, AuthError> {
        let token = token.trim();
        if token.is_empty() {
            return Err(AuthError::InvalidLoginToken);
        }
        let token_hash = token_hash(token);
        let row = sqlx::query_as::<_, (String, String)>(
            "DELETE FROM nanotrace_magic_links
             WHERE token_hash = $1 AND expires_at > now()
             RETURNING email, return_to",
        )
        .bind(&token_hash)
        .fetch_optional(&self.pool)
        .await?;
        let Some((email, return_to)) = row else {
            return Err(AuthError::InvalidLoginToken);
        };
        self.ensure_email_allowed(&email)?;
        let role = if self.is_admin_email(&email) {
            AuthRole::Admin
        } else {
            AuthRole::Viewer
        };
        let subject = format!("email:{email}");
        let session_token = self.create_session(&subject, &email, None, role).await?;
        Ok(LoginComplete {
            return_to,
            session_cookie: self.cookie_header(
                &self.cfg.session_cookie_name,
                &session_token,
                self.cfg.session_ttl.as_secs(),
                true,
            ),
        })
    }

    pub async fn logout(&self, headers: &HeaderMap) -> Result<String, AuthError> {
        if let Some(token) = read_cookie(headers, &self.cfg.session_cookie_name) {
            let token_hash = token_hash(&token);
            sqlx::query("DELETE FROM nanotrace_auth_sessions WHERE token_hash = $1")
                .bind(token_hash)
                .execute(&self.pool)
                .await?;
        }
        Ok(self.expire_cookie_header(&self.cfg.session_cookie_name))
    }

    pub async fn validate_session(&self, headers: &HeaderMap) -> Result<AuthIdentity, AuthError> {
        let token =
            read_cookie(headers, &self.cfg.session_cookie_name).ok_or(AuthError::Unauthorized)?;
        let token_hash = token_hash(&token);
        let row = sqlx::query_as::<_, (String, String, Option<String>, String)>(
            "UPDATE nanotrace_auth_sessions
             SET last_seen_at = now()
             WHERE token_hash = $1 AND expires_at > now()
             RETURNING subject, email, name, role",
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await?;
        let Some((subject, email, name, role)) = row else {
            return Err(AuthError::Unauthorized);
        };
        let requested_organization_id =
            read_organization_id(headers).unwrap_or(DEFAULT_ORGANIZATION_ID);
        let organization = self
            .organization_for_subject(&subject, requested_organization_id)
            .await?;
        let role = parse_role(&role);
        Ok(AuthIdentity {
            auth_type: AuthType::Session,
            subject,
            email: Some(email),
            name,
            role,
            tenant_id: organization.organization_id.clone(),
            organization_id: organization.organization_id,
            organization_name: organization.organization_name,
            scopes: default_scopes(role),
        })
    }

    pub async fn validate_api_key(&self, headers: &HeaderMap) -> Result<AuthIdentity, AuthError> {
        let token = read_api_key(headers).ok_or(AuthError::Unauthorized)?;
        let key_hash = token_hash(&token);
        let Some(row) = self.cached_api_key(&key_hash).await? else {
            return Err(AuthError::Unauthorized);
        };
        Ok(AuthIdentity {
            auth_type: AuthType::ApiKey,
            subject: format!("api_key:{}", row.name),
            email: None,
            name: Some(row.name),
            role: parse_role(&row.role),
            tenant_id: row.organization_id.clone(),
            organization_id: row.organization_id,
            organization_name: row.organization_name,
            scopes: normalize_scopes(&row.scopes, parse_role(&row.role)),
        })
    }

    pub async fn list_organizations(
        &self,
        organization_id: &str,
        subject: &str,
    ) -> Result<Vec<OrganizationRecord>, AuthError> {
        if is_api_key_subject(subject) {
            let rows = sqlx::query_as::<_, OrganizationRow>(
                "SELECT o.id, o.slug, o.name, o.plan, o.created_at, o.updated_at
                 FROM nanotrace_organizations o
                 WHERE o.id = $1
                 ORDER BY o.created_at ASC, o.id ASC",
            )
            .bind(organization_id)
            .fetch_all(&self.pool)
            .await?;
            return Ok(rows.into_iter().map(OrganizationRow::into_record).collect());
        }
        let rows = sqlx::query_as::<_, OrganizationRow>(
            "SELECT o.id, o.slug, o.name, o.plan, o.created_at, o.updated_at
             FROM nanotrace_organizations o
             INNER JOIN nanotrace_organization_members m ON m.organization_id = o.id
             WHERE m.subject = $1
             ORDER BY o.created_at ASC, o.id ASC",
        )
        .bind(subject)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(OrganizationRow::into_record).collect())
    }

    pub async fn create_organization(
        &self,
        subject: &str,
        input: CreateOrganizationInput,
    ) -> Result<OrganizationRecord, AuthError> {
        let slug = validate_slug(&input.slug)?;
        let name = validate_name(&input.name, "organization name")?;
        let plan = validate_token_or_default(&input.plan, "developer");
        let suffix = short_token();
        let organization_id = format!("org_{}_{}", slug.replace('-', "_"), suffix);

        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO nanotrace_organizations (id, slug, name, plan)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&organization_id)
        .bind(slug)
        .bind(name)
        .bind(&plan)
        .execute(&mut *tx)
        .await?;
        if !is_api_key_subject(subject) {
            sqlx::query(
                "INSERT INTO nanotrace_organization_members (organization_id, subject, role)
                 VALUES ($1, $2, 'admin')",
            )
            .bind(&organization_id)
            .bind(subject)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.get_organization(&organization_id, subject).await
    }

    pub async fn get_organization(
        &self,
        organization_id: &str,
        subject: &str,
    ) -> Result<OrganizationRecord, AuthError> {
        self.ensure_org_member(organization_id, subject).await?;
        let row = sqlx::query_as::<_, OrganizationRow>(
            "SELECT o.id, o.slug, o.name, o.plan, o.created_at, o.updated_at
             FROM nanotrace_organizations o
             WHERE o.id = $1",
        )
        .bind(organization_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(OrganizationRow::into_record)
            .ok_or(AuthError::NotFound)
    }

    pub async fn get_organization_data_plane(
        &self,
        organization_id: &str,
        subject: &str,
    ) -> Result<OrganizationDataPlaneRecord, AuthError> {
        self.ensure_org_member(organization_id, subject).await?;
        let row = sqlx::query_as::<_, OrganizationDataPlaneRow>(
            "SELECT organization_id, mode, provider, region, public_base_url, ingest_url, query_url,
                    internal_secret_ref, s3_bucket, processor_prefix, clickhouse_mode,
                    clickhouse_provider, clickhouse_region, clickhouse_service_id, clickhouse_url,
                    clickhouse_database, kms_key_arn, status, status_message,
                    last_provisioning_job_id, created_at, updated_at
             FROM nanotrace_organization_data_planes
             WHERE organization_id = $1",
        )
        .bind(organization_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(OrganizationDataPlaneRow::into_record)
            .ok_or(AuthError::NotFound)
    }

    pub async fn upsert_organization_data_plane(
        &self,
        organization_id: &str,
        subject: &str,
        input: UpsertOrganizationDataPlaneInput,
    ) -> Result<OrganizationDataPlaneRecord, AuthError> {
        self.ensure_org_admin(organization_id, subject).await?;
        let input = validate_data_plane_input(input)?;
        let mut tx = self.pool.begin().await?;
        let row = upsert_organization_data_plane_tx(&mut tx, organization_id, input).await?;
        tx.commit().await?;
        Ok(row.into_record())
    }

    pub async fn claim_next_data_plane_provision_job(
        &self,
        worker_id: &str,
    ) -> Result<Option<DataPlaneProvisionJobRecord>, AuthError> {
        let worker_id = validate_name(worker_id, "worker_id")?;
        let row = sqlx::query_as::<_, DataPlaneProvisionJobRow>(
            "WITH next_job AS (
                SELECT id
                FROM nanotrace_data_plane_jobs
                WHERE status = 'queued'
                ORDER BY created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
             )
             UPDATE nanotrace_data_plane_jobs AS job
             SET status = 'running',
                 started_at = COALESCE(job.started_at, now()),
                 updated_at = now(),
                 result_json = COALESCE(job.result_json, '{}'::jsonb)
                     || jsonb_build_object('worker_id', $1::text)
             FROM next_job
             WHERE job.id = next_job.id
             RETURNING job.id, job.organization_id, job.kind, job.status, job.provider,
                       job.region, job.clickhouse_mode, job.clickhouse_region,
                       job.request_json, job.result_json, job.error, job.created_by,
                       job.created_at, job.updated_at, job.started_at, job.finished_at",
        )
        .bind(worker_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(DataPlaneProvisionJobRow::into_record))
    }

    pub async fn complete_data_plane_provision_job(
        &self,
        job_id: &str,
        input: CompleteDataPlaneProvisionJobInput,
    ) -> Result<DataPlaneProvisionJobRecord, AuthError> {
        let job_id = validate_name(job_id, "job_id")?;
        let status = validate_token_or_default(&input.status, "succeeded");
        if status != "succeeded" && status != "failed" {
            return Err(AuthError::InvalidInput(
                "provision job status must be succeeded or failed".to_string(),
            ));
        }
        let mut error = input
            .error
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        if status == "failed" && error.is_none() {
            error = Some("Data-plane provisioning failed.".to_string());
        }

        let mut tx = self.pool.begin().await?;
        let job = sqlx::query_as::<_, DataPlaneProvisionJobRow>(
            "SELECT id, organization_id, kind, status, provider, region, clickhouse_mode,
                    clickhouse_region, request_json, result_json, error, created_by,
                    created_at, updated_at, started_at, finished_at
             FROM nanotrace_data_plane_jobs
             WHERE id = $1
             FOR UPDATE",
        )
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AuthError::NotFound)?;

        let mut result = input.result.unwrap_or_else(|| json!({}));
        if status == "succeeded" {
            let Some(mut data_plane) = input.data_plane else {
                return Err(AuthError::InvalidInput(
                    "data_plane is required when provision job succeeds".to_string(),
                ));
            };
            if data_plane.status.trim().is_empty() {
                data_plane.status = "active".to_string();
            }
            if data_plane.status_message.trim().is_empty() {
                data_plane.status_message = "Data plane provisioned.".to_string();
            }
            data_plane.last_provisioning_job_id = Some(job.id.clone());
            let data_plane = upsert_organization_data_plane_tx(
                &mut tx,
                &job.organization_id,
                validate_data_plane_input(data_plane)?,
            )
            .await?
            .into_record();
            let data_plane_json = serde_json::to_value(data_plane)
                .map_err(|err| AuthError::InvalidInput(err.to_string()))?;
            result = merge_json_object(result, "data_plane", data_plane_json);
        } else if let Some(message) = error.as_deref() {
            sqlx::query(
                "UPDATE nanotrace_organization_data_planes
                 SET status = 'failed',
                     status_message = $1,
                     updated_at = now()
                 WHERE organization_id = $2
                   AND last_provisioning_job_id = $3",
            )
            .bind(message)
            .bind(&job.organization_id)
            .bind(&job.id)
            .execute(&mut *tx)
            .await?;
        }

        let row = sqlx::query_as::<_, DataPlaneProvisionJobRow>(
            "UPDATE nanotrace_data_plane_jobs
             SET status = $2,
                 result_json = $3,
                 error = $4,
                 started_at = COALESCE(started_at, now()),
                 finished_at = now(),
                 updated_at = now()
             WHERE id = $1
             RETURNING id, organization_id, kind, status, provider, region, clickhouse_mode,
                       clickhouse_region, request_json, result_json, error, created_by,
                       created_at, updated_at, started_at, finished_at",
        )
        .bind(&job.id)
        .bind(status)
        .bind(result)
        .bind(error)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(row.into_record())
    }

    pub async fn create_data_plane_provision_job(
        &self,
        organization_id: &str,
        subject: &str,
        input: CreateDataPlaneProvisionJobInput,
    ) -> Result<DataPlaneProvisionJobRecord, AuthError> {
        self.ensure_org_admin(organization_id, subject).await?;
        let provider = validate_token_or_default(&input.provider, "aws");
        let region = validate_token_or_default(&input.region, "us-west-2");
        let clickhouse_mode = validate_token_or_default(&input.clickhouse_mode, "shared-service");
        let clickhouse_region = validate_token_or_default(&input.clickhouse_region, &region);
        let id = format!("dpjob_{}", short_token());
        let request = json!({
            "provider": provider,
            "region": region,
            "clickhouse_mode": clickhouse_mode,
            "clickhouse_region": clickhouse_region,
        });
        let row = sqlx::query_as::<_, DataPlaneProvisionJobRow>(
            "INSERT INTO nanotrace_data_plane_jobs
                (id, organization_id, kind, status, provider, region, clickhouse_mode,
                 clickhouse_region, request_json, created_by)
             VALUES ($1, $2, 'provision', 'queued', $3, $4, $5, $6, $7, $8)
             RETURNING id, organization_id, kind, status, provider, region, clickhouse_mode,
                       clickhouse_region, request_json, result_json, error, created_by,
                       created_at, updated_at, started_at, finished_at",
        )
        .bind(&id)
        .bind(organization_id)
        .bind(request["provider"].as_str().unwrap_or("aws"))
        .bind(request["region"].as_str().unwrap_or("us-west-2"))
        .bind(
            request["clickhouse_mode"]
                .as_str()
                .unwrap_or("shared-service"),
        )
        .bind(request["clickhouse_region"].as_str().unwrap_or("us-west-2"))
        .bind(&request)
        .bind(subject)
        .fetch_one(&self.pool)
        .await?;
        self.mark_data_plane_provision_queued(organization_id, &row)
            .await?;
        Ok(row.into_record())
    }

    pub async fn list_data_plane_jobs(
        &self,
        organization_id: &str,
        subject: &str,
    ) -> Result<Vec<DataPlaneProvisionJobRecord>, AuthError> {
        self.ensure_org_admin(organization_id, subject).await?;
        let rows = sqlx::query_as::<_, DataPlaneProvisionJobRow>(
            "SELECT id, organization_id, kind, status, provider, region, clickhouse_mode,
                    clickhouse_region, request_json, result_json, error, created_by,
                    created_at, updated_at, started_at, finished_at
             FROM nanotrace_data_plane_jobs
             WHERE organization_id = $1
             ORDER BY created_at DESC, id DESC",
        )
        .bind(organization_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(DataPlaneProvisionJobRow::into_record)
            .collect())
    }

    pub async fn get_data_plane_job(
        &self,
        organization_id: &str,
        subject: &str,
        job_id: &str,
    ) -> Result<DataPlaneProvisionJobRecord, AuthError> {
        self.ensure_org_admin(organization_id, subject).await?;
        let row = sqlx::query_as::<_, DataPlaneProvisionJobRow>(
            "SELECT id, organization_id, kind, status, provider, region, clickhouse_mode,
                    clickhouse_region, request_json, result_json, error, created_by,
                    created_at, updated_at, started_at, finished_at
             FROM nanotrace_data_plane_jobs
             WHERE organization_id = $1 AND id = $2",
        )
        .bind(organization_id)
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(DataPlaneProvisionJobRow::into_record)
            .ok_or(AuthError::NotFound)
    }

    async fn mark_data_plane_provision_queued(
        &self,
        organization_id: &str,
        job: &DataPlaneProvisionJobRow,
    ) -> Result<(), AuthError> {
        sqlx::query(
            "INSERT INTO nanotrace_organization_data_planes
                (organization_id, mode, provider, region, public_base_url, ingest_url, query_url,
                 internal_secret_ref, s3_bucket, processor_prefix, clickhouse_mode,
                 clickhouse_provider, clickhouse_region, clickhouse_service_id, clickhouse_url,
                 clickhouse_database, kms_key_arn, status, status_message, last_provisioning_job_id)
             VALUES ($1, 'dedicated', $2, $3, '', '', '', '', '', '', $4, $2, $5, '', '', 'observatory',
                     '', 'queued', 'Provisioning job queued.', $6)
             ON CONFLICT (organization_id) DO UPDATE SET
                 mode = 'dedicated',
                 provider = EXCLUDED.provider,
                 region = EXCLUDED.region,
                 clickhouse_mode = EXCLUDED.clickhouse_mode,
                 clickhouse_provider = EXCLUDED.clickhouse_provider,
                 clickhouse_region = EXCLUDED.clickhouse_region,
                 status = EXCLUDED.status,
                 status_message = EXCLUDED.status_message,
                 last_provisioning_job_id = EXCLUDED.last_provisioning_job_id,
                 updated_at = now()",
        )
        .bind(organization_id)
        .bind(&job.provider)
        .bind(&job.region)
        .bind(&job.clickhouse_mode)
        .bind(&job.clickhouse_region)
        .bind(&job.id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn create_api_key(
        &self,
        organization_id: &str,
        name: &str,
        role: AuthRole,
        scopes: &[String],
        created_by: &str,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<CreatedApiKey, AuthError> {
        let name = name.trim();
        if name.is_empty() {
            return Err(AuthError::InvalidInput(
                "API key name is required".to_string(),
            ));
        }
        let key = format!("ntak_{}", random_token());
        let prefix: String = key.chars().take(16).collect();
        let key_hash = token_hash(&key);
        let scopes = normalize_scopes(scopes, role);
        let row = sqlx::query_as::<_, ApiKeyRow>(
            "INSERT INTO nanotrace_api_keys
                (organization_id, key_hash, prefix, name, role, scopes, created_by, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             RETURNING id, organization_id, name, prefix, role, scopes,
                       created_by, created_at, last_used_at, expires_at, revoked_at",
        )
        .bind(organization_id)
        .bind(key_hash)
        .bind(prefix)
        .bind(name)
        .bind(role_name(role))
        .bind(scopes)
        .bind(created_by)
        .bind(expires_at)
        .fetch_one(&self.pool)
        .await?;
        self.invalidate_api_key_cache();
        Ok(CreatedApiKey {
            key,
            api_key: row.into_record(),
        })
    }

    pub async fn list_api_keys(
        &self,
        organization_id: &str,
    ) -> Result<Vec<ApiKeyRecord>, AuthError> {
        let rows = sqlx::query_as::<_, ApiKeyRow>(
            "SELECT id, organization_id, name, prefix, role, scopes,
                    created_by, created_at, last_used_at, expires_at, revoked_at
             FROM nanotrace_api_keys
             WHERE organization_id = $1
             ORDER BY created_at DESC, id DESC",
        )
        .bind(organization_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(ApiKeyRow::into_record).collect())
    }

    pub async fn revoke_api_key(
        &self,
        organization_id: &str,
        id: i64,
    ) -> Result<ApiKeyRecord, AuthError> {
        let row = sqlx::query_as::<_, ApiKeyRow>(
            "UPDATE nanotrace_api_keys
             SET revoked_at = COALESCE(revoked_at, now())
             WHERE id = $1 AND organization_id = $2
             RETURNING id, organization_id, name, prefix, role, scopes,
                       created_by, created_at, last_used_at, expires_at, revoked_at",
        )
        .bind(id)
        .bind(organization_id)
        .fetch_optional(&self.pool)
        .await?;
        self.invalidate_api_key_cache();
        row.map(ApiKeyRow::into_record).ok_or(AuthError::NotFound)
    }

    pub async fn list_organization_invites(
        &self,
        organization_id: &str,
        subject: &str,
    ) -> Result<Vec<OrganizationInviteRecord>, AuthError> {
        self.ensure_org_admin(organization_id, subject).await?;
        let rows = sqlx::query_as::<_, OrganizationInviteRow>(
            "SELECT id, organization_id, email, role, invited_by, created_at, expires_at, accepted_at, revoked_at
             FROM nanotrace_organization_invites
             WHERE organization_id = $1
             ORDER BY created_at DESC, id DESC",
        )
        .bind(organization_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(OrganizationInviteRow::into_record)
            .collect())
    }

    pub async fn create_organization_invite(
        &self,
        organization_id: &str,
        subject: &str,
        email: &str,
        role: AuthRole,
    ) -> Result<CreatedOrganizationInvite, AuthError> {
        self.ensure_org_admin(organization_id, subject).await?;
        let email = normalize_email(email)?;
        self.ensure_email_allowed(&email)?;
        let role = invite_role(role)?;
        let token = random_token();
        let token_hash = token_hash(&token);
        let expires_at = Utc::now() + chrono::Duration::days(7);
        let row = sqlx::query_as::<_, OrganizationInviteRow>(
            "INSERT INTO nanotrace_organization_invites
                (organization_id, email, role, token_hash, invited_by, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6)
             RETURNING id, organization_id, email, role, invited_by, created_at, expires_at, accepted_at, revoked_at",
        )
        .bind(organization_id)
        .bind(email)
        .bind(role_name(role))
        .bind(token_hash)
        .bind(subject)
        .bind(expires_at)
        .fetch_one(&self.pool)
        .await?;
        Ok(CreatedOrganizationInvite {
            invite: row.into_record(),
            token,
        })
    }

    pub async fn revoke_organization_invite(
        &self,
        organization_id: &str,
        invite_id: i64,
        subject: &str,
    ) -> Result<OrganizationInviteRecord, AuthError> {
        self.ensure_org_admin(organization_id, subject).await?;
        let row = sqlx::query_as::<_, OrganizationInviteRow>(
            "UPDATE nanotrace_organization_invites
             SET revoked_at = COALESCE(revoked_at, now())
             WHERE id = $1 AND organization_id = $2
             RETURNING id, organization_id, email, role, invited_by, created_at, expires_at, accepted_at, revoked_at",
        )
        .bind(invite_id)
        .bind(organization_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(OrganizationInviteRow::into_record)
            .ok_or(AuthError::NotFound)
    }

    pub async fn accept_organization_invite(
        &self,
        token: &str,
        subject: &str,
        email: &str,
    ) -> Result<OrganizationRecord, AuthError> {
        let token = token.trim();
        if token.is_empty() {
            return Err(AuthError::InvalidInviteToken);
        }
        let email = normalize_email(email)?;
        let token_hash = token_hash(token);
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query_as::<_, OrganizationInviteRow>(
            "UPDATE nanotrace_organization_invites
             SET accepted_at = now()
             WHERE token_hash = $1
               AND email = $2
               AND expires_at > now()
               AND accepted_at IS NULL
               AND revoked_at IS NULL
             RETURNING id, organization_id, email, role, invited_by, created_at, expires_at, accepted_at, revoked_at",
        )
        .bind(token_hash)
        .bind(&email)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(invite) = row else {
            return Err(AuthError::InvalidInviteToken);
        };
        sqlx::query(
            "INSERT INTO nanotrace_organization_members (organization_id, subject, role)
             VALUES ($1, $2, $3)
             ON CONFLICT (organization_id, subject)
             DO UPDATE SET role = EXCLUDED.role",
        )
        .bind(&invite.organization_id)
        .bind(subject)
        .bind(&invite.role)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        self.get_organization(&invite.organization_id, subject)
            .await
    }

    async fn ensure_schema(&self) -> Result<(), AuthError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nanotrace_organizations (
                id text PRIMARY KEY,
                slug text NOT NULL UNIQUE,
                name text NOT NULL,
                plan text NOT NULL DEFAULT 'developer',
                created_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now()
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nanotrace_organization_data_planes (
                organization_id text PRIMARY KEY REFERENCES nanotrace_organizations(id) ON DELETE CASCADE,
                mode text NOT NULL DEFAULT 'dedicated',
                provider text NOT NULL DEFAULT 'aws',
                region text NOT NULL DEFAULT 'us-west-2',
                public_base_url text NOT NULL,
                ingest_url text NOT NULL,
                query_url text NOT NULL,
                internal_secret_ref text NOT NULL,
                s3_bucket text NOT NULL,
                processor_prefix text NOT NULL,
                clickhouse_mode text NOT NULL DEFAULT 'dedicated-service',
                clickhouse_provider text NOT NULL DEFAULT 'aws',
                clickhouse_region text NOT NULL DEFAULT 'us-west-2',
                clickhouse_service_id text NOT NULL DEFAULT '',
                clickhouse_url text NOT NULL,
                clickhouse_database text NOT NULL DEFAULT 'observatory',
                kms_key_arn text NOT NULL DEFAULT '',
                status text NOT NULL DEFAULT 'provisioning',
                status_message text NOT NULL DEFAULT '',
                last_provisioning_job_id text,
                created_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now()
            )",
        )
        .execute(&self.pool)
        .await?;
        for statement in [
            "ALTER TABLE nanotrace_organization_data_planes ADD COLUMN IF NOT EXISTS mode text NOT NULL DEFAULT 'dedicated'",
            "ALTER TABLE nanotrace_organization_data_planes ADD COLUMN IF NOT EXISTS provider text NOT NULL DEFAULT 'aws'",
            "ALTER TABLE nanotrace_organization_data_planes ADD COLUMN IF NOT EXISTS region text NOT NULL DEFAULT 'us-west-2'",
            "ALTER TABLE nanotrace_organization_data_planes ADD COLUMN IF NOT EXISTS clickhouse_mode text NOT NULL DEFAULT 'dedicated-service'",
            "ALTER TABLE nanotrace_organization_data_planes ADD COLUMN IF NOT EXISTS clickhouse_provider text NOT NULL DEFAULT 'aws'",
            "ALTER TABLE nanotrace_organization_data_planes ADD COLUMN IF NOT EXISTS clickhouse_region text NOT NULL DEFAULT 'us-west-2'",
            "ALTER TABLE nanotrace_organization_data_planes ADD COLUMN IF NOT EXISTS clickhouse_service_id text NOT NULL DEFAULT ''",
            "ALTER TABLE nanotrace_organization_data_planes ADD COLUMN IF NOT EXISTS kms_key_arn text NOT NULL DEFAULT ''",
            "ALTER TABLE nanotrace_organization_data_planes ADD COLUMN IF NOT EXISTS status_message text NOT NULL DEFAULT ''",
            "ALTER TABLE nanotrace_organization_data_planes ADD COLUMN IF NOT EXISTS last_provisioning_job_id text",
        ] {
            sqlx::query(statement).execute(&self.pool).await?;
        }
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nanotrace_data_plane_jobs (
                id text PRIMARY KEY,
                organization_id text NOT NULL REFERENCES nanotrace_organizations(id) ON DELETE CASCADE,
                kind text NOT NULL,
                status text NOT NULL,
                provider text NOT NULL DEFAULT 'aws',
                region text NOT NULL DEFAULT 'us-west-2',
                clickhouse_mode text NOT NULL DEFAULT 'shared-service',
                clickhouse_region text NOT NULL DEFAULT 'us-west-2',
                request_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                result_json jsonb,
                error text,
                created_by text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now(),
                started_at timestamptz,
                finished_at timestamptz
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS nanotrace_data_plane_jobs_org_idx
             ON nanotrace_data_plane_jobs (organization_id, created_at DESC)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nanotrace_auth_users (
                subject text PRIMARY KEY,
                email text NOT NULL,
                name text,
                role text NOT NULL DEFAULT 'viewer',
                created_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now()
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nanotrace_organization_members (
                organization_id text NOT NULL REFERENCES nanotrace_organizations(id) ON DELETE CASCADE,
                subject text NOT NULL REFERENCES nanotrace_auth_users(subject) ON DELETE CASCADE,
                role text NOT NULL DEFAULT 'viewer',
                created_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (organization_id, subject)
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nanotrace_organization_invites (
                id bigserial PRIMARY KEY,
                organization_id text NOT NULL REFERENCES nanotrace_organizations(id) ON DELETE CASCADE,
                email text NOT NULL,
                role text NOT NULL DEFAULT 'viewer',
                token_hash text NOT NULL UNIQUE,
                invited_by text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now(),
                expires_at timestamptz NOT NULL,
                accepted_at timestamptz,
                revoked_at timestamptz
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS nanotrace_organization_invites_active_idx
             ON nanotrace_organization_invites (organization_id, email)
             WHERE accepted_at IS NULL AND revoked_at IS NULL",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nanotrace_auth_sessions (
                token_hash text PRIMARY KEY,
                subject text NOT NULL REFERENCES nanotrace_auth_users(subject) ON DELETE CASCADE,
                email text NOT NULL,
                name text,
                role text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now(),
                last_seen_at timestamptz NOT NULL DEFAULT now(),
                expires_at timestamptz NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS nanotrace_auth_sessions_expires_at_idx
             ON nanotrace_auth_sessions (expires_at)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nanotrace_magic_links (
                token_hash text PRIMARY KEY,
                email text NOT NULL,
                return_to text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now(),
                expires_at timestamptz NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS nanotrace_magic_links_expires_at_idx
             ON nanotrace_magic_links (expires_at)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nanotrace_api_keys (
                id bigserial PRIMARY KEY,
                organization_id text NOT NULL DEFAULT 'org_default',
                key_hash text NOT NULL UNIQUE,
                prefix text NOT NULL,
                name text NOT NULL,
                role text NOT NULL DEFAULT 'service',
                scopes text[] NOT NULL DEFAULT '{}',
                created_by text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now(),
                last_used_at timestamptz,
                expires_at timestamptz,
                revoked_at timestamptz
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS nanotrace_api_keys_active_idx
             ON nanotrace_api_keys (key_hash)
             WHERE revoked_at IS NULL",
        )
        .execute(&self.pool)
        .await?;
        self.ensure_default_control_plane().await?;
        Ok(())
    }

    async fn cached_api_key(&self, key_hash: &str) -> Result<Option<CachedApiKey>, AuthError> {
        if let Some(cached) = self.cached_api_key_if_fresh(key_hash)? {
            return Ok(cached);
        }

        self.refresh_api_key_cache().await?;
        Ok(self.cached_api_key_if_fresh(key_hash)?.flatten())
    }

    fn cached_api_key_if_fresh(
        &self,
        key_hash: &str,
    ) -> Result<Option<Option<CachedApiKey>>, AuthError> {
        let cache = self
            .api_key_cache
            .read()
            .map_err(|_| AuthError::InvalidInput("API key cache lock poisoned".to_string()))?;
        let Some(loaded_at) = cache.loaded_at else {
            return Ok(None);
        };
        if loaded_at.elapsed() > Duration::from_secs(5) {
            return Ok(None);
        }
        Ok(Some(cache.keys.get(key_hash).cloned()))
    }

    async fn refresh_api_key_cache(&self) -> Result<(), AuthError> {
        let rows = sqlx::query_as::<_, CachedApiKeyRow>(
            "SELECT k.key_hash, k.name, k.role, k.scopes, k.organization_id, o.name AS organization_name
             FROM nanotrace_api_keys AS k
             INNER JOIN nanotrace_organizations AS o ON k.organization_id = o.id
             WHERE k.revoked_at IS NULL
               AND (k.expires_at IS NULL OR k.expires_at > now())",
        )
        .fetch_all(&self.pool)
        .await?;
        let keys = rows
            .into_iter()
            .map(|row| {
                (
                    row.key_hash,
                    CachedApiKey {
                        name: row.name,
                        role: row.role,
                        scopes: row.scopes,
                        organization_id: row.organization_id,
                        organization_name: row.organization_name,
                    },
                )
            })
            .collect();
        let mut cache = self
            .api_key_cache
            .write()
            .map_err(|_| AuthError::InvalidInput("API key cache lock poisoned".to_string()))?;
        cache.loaded_at = Some(Instant::now());
        cache.keys = keys;
        Ok(())
    }

    fn invalidate_api_key_cache(&self) {
        if let Ok(mut cache) = self.api_key_cache.write() {
            cache.loaded_at = None;
            cache.keys.clear();
        }
    }

    async fn ensure_default_control_plane(&self) -> Result<(), AuthError> {
        sqlx::query(
            "INSERT INTO nanotrace_organizations (id, slug, name)
             VALUES ($1, 'default', 'Default')
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(DEFAULT_ORGANIZATION_ID)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "INSERT INTO nanotrace_organization_members (organization_id, subject, role)
             SELECT $1, subject, 'admin'
             FROM nanotrace_auth_users
             WHERE role = 'admin'
             ON CONFLICT (organization_id, subject) DO NOTHING",
        )
        .bind(DEFAULT_ORGANIZATION_ID)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn ensure_bootstrap_api_key(&self) -> Result<(), AuthError> {
        let Some(key) = self
            .cfg
            .bootstrap_api_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(());
        };
        let prefix: String = key.chars().take(16).collect();
        let key_hash = token_hash(key);
        sqlx::query(
            "INSERT INTO nanotrace_api_keys
                (organization_id, key_hash, prefix, name, role, scopes, created_by)
             VALUES ($1, $2, $3, 'bootstrap', 'admin', $4, 'pulumi')
             ON CONFLICT (key_hash)
             DO UPDATE SET revoked_at = NULL,
                           organization_id = EXCLUDED.organization_id,
                           role = 'admin',
                           scopes = EXCLUDED.scopes",
        )
        .bind(DEFAULT_ORGANIZATION_ID)
        .bind(key_hash)
        .bind(prefix)
        .bind(default_scopes(AuthRole::Admin))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn organization_for_subject(
        &self,
        subject: &str,
        organization_id: &str,
    ) -> Result<OrganizationIdentityRow, AuthError> {
        let row = sqlx::query_as::<_, OrganizationIdentityRow>(
            "SELECT o.id AS organization_id, o.name AS organization_name
             FROM nanotrace_organizations o
             INNER JOIN nanotrace_organization_members m ON m.organization_id = o.id
             WHERE o.id = $1 AND m.subject = $2",
        )
        .bind(organization_id)
        .bind(subject)
        .fetch_optional(&self.pool)
        .await?;
        row.ok_or(AuthError::Forbidden)
    }

    async fn ensure_org_member(
        &self,
        organization_id: &str,
        subject: &str,
    ) -> Result<(), AuthError> {
        if is_api_key_subject(subject) {
            return Ok(());
        }
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1 FROM nanotrace_organization_members
                WHERE organization_id = $1 AND subject = $2
             )",
        )
        .bind(organization_id)
        .bind(subject)
        .fetch_one(&self.pool)
        .await?;
        if exists {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }

    async fn ensure_org_admin(
        &self,
        organization_id: &str,
        subject: &str,
    ) -> Result<(), AuthError> {
        if is_api_key_subject(subject) {
            return Ok(());
        }
        let role = sqlx::query_scalar::<_, String>(
            "SELECT role FROM nanotrace_organization_members
             WHERE organization_id = $1 AND subject = $2",
        )
        .bind(organization_id)
        .bind(subject)
        .fetch_optional(&self.pool)
        .await?;
        if role.as_deref() == Some("admin") {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }

    async fn create_session(
        &self,
        subject: &str,
        email: &str,
        name: Option<&str>,
        role: AuthRole,
    ) -> Result<String, AuthError> {
        let role = role_name(role);
        sqlx::query(
            "INSERT INTO nanotrace_auth_users (subject, email, name, role, updated_at)
             VALUES ($1, $2, $3, $4, now())
             ON CONFLICT (subject)
             DO UPDATE SET email = EXCLUDED.email, name = EXCLUDED.name, role = EXCLUDED.role, updated_at = now()",
        )
        .bind(subject)
        .bind(email)
        .bind(name)
        .bind(role)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "INSERT INTO nanotrace_organization_members (organization_id, subject, role)
             VALUES ($1, $2, $3)
             ON CONFLICT (organization_id, subject)
             DO UPDATE SET role = EXCLUDED.role",
        )
        .bind(DEFAULT_ORGANIZATION_ID)
        .bind(subject)
        .bind(role)
        .execute(&self.pool)
        .await?;

        let token = random_token();
        let token_hash = token_hash(&token);
        let expires_at = Utc::now()
            + chrono::Duration::from_std(self.cfg.session_ttl)
                .unwrap_or_else(|_| chrono::Duration::days(7));
        sqlx::query(
            "INSERT INTO nanotrace_auth_sessions (token_hash, subject, email, name, role, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(token_hash)
        .bind(subject)
        .bind(email)
        .bind(name)
        .bind(role)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;
        Ok(token)
    }

    fn ensure_email_allowed(&self, email: &str) -> Result<(), AuthError> {
        if self.cfg.allowed_emails.is_empty() {
            return Ok(());
        }
        if self
            .cfg
            .allowed_emails
            .iter()
            .any(|pattern| email_matches_pattern(email, pattern))
        {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }

    fn is_admin_email(&self, email: &str) -> bool {
        self.cfg
            .admin_emails
            .iter()
            .any(|admin| admin.eq_ignore_ascii_case(email))
    }

    fn cookie_header(&self, name: &str, value: &str, max_age_secs: u64, http_only: bool) -> String {
        let mut parts = vec![
            format!("{name}={value}"),
            "Path=/".to_string(),
            format!("Max-Age={max_age_secs}"),
            "SameSite=Lax".to_string(),
        ];
        if http_only {
            parts.push("HttpOnly".to_string());
        }
        if self.cfg.session_secure {
            parts.push("Secure".to_string());
        }
        parts.join("; ")
    }

    fn expire_cookie_header(&self, name: &str) -> String {
        let mut parts = vec![
            format!("{name}="),
            "Path=/".to_string(),
            "Max-Age=0".to_string(),
            "SameSite=Lax".to_string(),
            "HttpOnly".to_string(),
        ];
        if self.cfg.session_secure {
            parts.push("Secure".to_string());
        }
        parts.join("; ")
    }
}

fn validate_data_plane_input(
    input: UpsertOrganizationDataPlaneInput,
) -> Result<ValidatedOrganizationDataPlaneInput, AuthError> {
    let mode = validate_token_or_default(&input.mode, "dedicated");
    let provider = validate_token_or_default(&input.provider, "aws");
    let region = validate_token_or_default(&input.region, "us-west-2");
    let public_base_url = validate_url(&input.public_base_url, "public_base_url")?;
    let ingest_url = validate_url(&input.ingest_url, "ingest_url")?;
    let query_url = validate_url(&input.query_url, "query_url")?;
    let internal_secret_ref = validate_name(&input.internal_secret_ref, "internal_secret_ref")?;
    let s3_bucket = validate_name(&input.s3_bucket, "s3_bucket")?;
    let processor_prefix = validate_name(&input.processor_prefix, "processor_prefix")?;
    let clickhouse_mode = validate_token_or_default(&input.clickhouse_mode, "dedicated-service");
    let clickhouse_provider = validate_token_or_default(&input.clickhouse_provider, &provider);
    let clickhouse_region = validate_token_or_default(&input.clickhouse_region, &region);
    let clickhouse_service_id = input.clickhouse_service_id.trim();
    let clickhouse_url = validate_url(&input.clickhouse_url, "clickhouse_url")?;
    let clickhouse_database = validate_token_or_default(&input.clickhouse_database, "observatory");
    let kms_key_arn = input.kms_key_arn.trim();
    let status = validate_token_or_default(&input.status, "provisioning");
    let status_message = input.status_message.trim();

    Ok(ValidatedOrganizationDataPlaneInput {
        mode,
        provider,
        region,
        public_base_url,
        ingest_url,
        query_url,
        internal_secret_ref: internal_secret_ref.to_string(),
        s3_bucket: s3_bucket.to_string(),
        processor_prefix: processor_prefix.to_string(),
        clickhouse_mode,
        clickhouse_provider,
        clickhouse_region,
        clickhouse_service_id: clickhouse_service_id.to_string(),
        clickhouse_url,
        clickhouse_database,
        kms_key_arn: kms_key_arn.to_string(),
        status,
        status_message: status_message.to_string(),
        last_provisioning_job_id: input.last_provisioning_job_id,
    })
}

async fn upsert_organization_data_plane_tx(
    tx: &mut Transaction<'_, Postgres>,
    organization_id: &str,
    input: ValidatedOrganizationDataPlaneInput,
) -> Result<OrganizationDataPlaneRow, AuthError> {
    let row = sqlx::query_as::<_, OrganizationDataPlaneRow>(
        "INSERT INTO nanotrace_organization_data_planes
            (organization_id, mode, provider, region, public_base_url, ingest_url, query_url,
             internal_secret_ref, s3_bucket, processor_prefix, clickhouse_mode,
             clickhouse_provider, clickhouse_region, clickhouse_service_id, clickhouse_url,
             clickhouse_database, kms_key_arn, status, status_message, last_provisioning_job_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                 $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
         ON CONFLICT (organization_id) DO UPDATE SET
             mode = EXCLUDED.mode,
             provider = EXCLUDED.provider,
             region = EXCLUDED.region,
             public_base_url = EXCLUDED.public_base_url,
             ingest_url = EXCLUDED.ingest_url,
             query_url = EXCLUDED.query_url,
             internal_secret_ref = EXCLUDED.internal_secret_ref,
             s3_bucket = EXCLUDED.s3_bucket,
             processor_prefix = EXCLUDED.processor_prefix,
             clickhouse_mode = EXCLUDED.clickhouse_mode,
             clickhouse_provider = EXCLUDED.clickhouse_provider,
             clickhouse_region = EXCLUDED.clickhouse_region,
             clickhouse_service_id = EXCLUDED.clickhouse_service_id,
             clickhouse_url = EXCLUDED.clickhouse_url,
             clickhouse_database = EXCLUDED.clickhouse_database,
             kms_key_arn = EXCLUDED.kms_key_arn,
             status = EXCLUDED.status,
             status_message = EXCLUDED.status_message,
             last_provisioning_job_id = EXCLUDED.last_provisioning_job_id,
             updated_at = now()
         RETURNING organization_id, mode, provider, region, public_base_url, ingest_url, query_url,
                   internal_secret_ref, s3_bucket, processor_prefix, clickhouse_mode,
                   clickhouse_provider, clickhouse_region, clickhouse_service_id, clickhouse_url,
                   clickhouse_database, kms_key_arn, status, status_message,
                   last_provisioning_job_id, created_at, updated_at",
    )
    .bind(organization_id)
    .bind(input.mode)
    .bind(input.provider)
    .bind(input.region)
    .bind(input.public_base_url)
    .bind(input.ingest_url)
    .bind(input.query_url)
    .bind(input.internal_secret_ref)
    .bind(input.s3_bucket)
    .bind(input.processor_prefix)
    .bind(input.clickhouse_mode)
    .bind(input.clickhouse_provider)
    .bind(input.clickhouse_region)
    .bind(input.clickhouse_service_id)
    .bind(input.clickhouse_url)
    .bind(input.clickhouse_database)
    .bind(input.kms_key_arn)
    .bind(input.status)
    .bind(input.status_message)
    .bind(input.last_provisioning_job_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(row)
}

fn merge_json_object(value: JsonValue, key: &str, item: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(mut object) => {
            object.insert(key.to_string(), item);
            JsonValue::Object(object)
        }
        other => json!({
            "value": other,
            key: item,
        }),
    }
}

pub struct LoginStart {
    pub email: String,
    pub login_url: String,
    pub expires_at: DateTime<Utc>,
}

pub struct LoginComplete {
    pub return_to: String,
    pub session_cookie: String,
}

pub async fn authorize_headers(
    headers: &HeaderMap,
    auth: Option<&AuthStore>,
) -> Result<AuthIdentity, AuthError> {
    if let Some(auth) = auth {
        if read_api_key(headers).is_some() {
            return auth.validate_api_key(headers).await;
        }
        return auth.validate_session(headers).await;
    }
    Err(AuthError::Unauthorized)
}

pub fn set_cookie_headers(
    values: impl IntoIterator<Item = String>,
) -> Result<HeaderMap, AuthError> {
    let mut headers = HeaderMap::new();
    for value in values {
        headers.append(
            SET_COOKIE,
            HeaderValue::from_str(&value)
                .map_err(|err| AuthError::Cookie(format!("invalid Set-Cookie header: {err}")))?,
        );
    }
    Ok(headers)
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    #[error("invalid or expired login link")]
    InvalidLoginToken,
    #[error("invalid or expired invite link")]
    InvalidInviteToken,
    #[error("not found")]
    NotFound,
    #[error("{0}")]
    InvalidInput(String),
    #[error("{0} is required")]
    MissingConfig(&'static str),
    #[error("cookie error: {0}")]
    Cookie(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl AuthError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::Unauthorized | Self::InvalidLoginToken | Self::InvalidInviteToken => {
                StatusCode::UNAUTHORIZED
            }
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::InvalidInput(_) => StatusCode::BAD_REQUEST,
            Self::MissingConfig(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Cookie(_) => StatusCode::BAD_REQUEST,
            Self::Database(_) => StatusCode::BAD_GATEWAY,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct ApiKeyRow {
    id: i64,
    organization_id: String,
    name: String,
    prefix: String,
    role: String,
    scopes: Vec<String>,
    created_by: String,
    created_at: DateTime<Utc>,
    last_used_at: Option<DateTime<Utc>>,
    expires_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
}

impl ApiKeyRow {
    fn into_record(self) -> ApiKeyRecord {
        ApiKeyRecord {
            id: self.id,
            organization_id: self.organization_id.clone(),
            name: self.name,
            prefix: self.prefix,
            role: parse_role(&self.role),
            scopes: self.scopes,
            created_by: self.created_by,
            created_at: self.created_at,
            last_used_at: self.last_used_at,
            expires_at: self.expires_at,
            revoked_at: self.revoked_at,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct CachedApiKeyRow {
    key_hash: String,
    name: String,
    role: String,
    scopes: Vec<String>,
    organization_id: String,
    organization_name: String,
}

#[derive(Debug, sqlx::FromRow)]
struct OrganizationInviteRow {
    id: i64,
    organization_id: String,
    email: String,
    role: String,
    invited_by: String,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    accepted_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
}

impl OrganizationInviteRow {
    fn into_record(self) -> OrganizationInviteRecord {
        OrganizationInviteRecord {
            id: self.id,
            organization_id: self.organization_id,
            email: self.email,
            role: parse_role(&self.role),
            invited_by: self.invited_by,
            created_at: self.created_at,
            expires_at: self.expires_at,
            accepted_at: self.accepted_at,
            revoked_at: self.revoked_at,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct OrganizationIdentityRow {
    organization_id: String,
    organization_name: String,
}

#[derive(Debug, sqlx::FromRow)]
struct OrganizationRow {
    id: String,
    slug: String,
    name: String,
    plan: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl OrganizationRow {
    fn into_record(self) -> OrganizationRecord {
        OrganizationRecord {
            id: self.id,
            slug: self.slug,
            name: self.name,
            plan: self.plan,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct OrganizationDataPlaneRow {
    organization_id: String,
    mode: String,
    provider: String,
    region: String,
    public_base_url: String,
    ingest_url: String,
    query_url: String,
    internal_secret_ref: String,
    s3_bucket: String,
    processor_prefix: String,
    clickhouse_mode: String,
    clickhouse_provider: String,
    clickhouse_region: String,
    clickhouse_service_id: String,
    clickhouse_url: String,
    clickhouse_database: String,
    kms_key_arn: String,
    status: String,
    status_message: String,
    last_provisioning_job_id: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl OrganizationDataPlaneRow {
    fn into_record(self) -> OrganizationDataPlaneRecord {
        OrganizationDataPlaneRecord {
            organization_id: self.organization_id,
            mode: self.mode,
            provider: self.provider,
            region: self.region,
            public_base_url: self.public_base_url,
            ingest_url: self.ingest_url,
            query_url: self.query_url,
            internal_secret_ref: self.internal_secret_ref,
            s3_bucket: self.s3_bucket,
            processor_prefix: self.processor_prefix,
            clickhouse_mode: self.clickhouse_mode,
            clickhouse_provider: self.clickhouse_provider,
            clickhouse_region: self.clickhouse_region,
            clickhouse_service_id: self.clickhouse_service_id,
            clickhouse_url: self.clickhouse_url,
            clickhouse_database: self.clickhouse_database,
            kms_key_arn: self.kms_key_arn,
            status: self.status,
            status_message: self.status_message,
            last_provisioning_job_id: self.last_provisioning_job_id,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct DataPlaneProvisionJobRow {
    id: String,
    organization_id: String,
    kind: String,
    status: String,
    provider: String,
    region: String,
    clickhouse_mode: String,
    clickhouse_region: String,
    request_json: JsonValue,
    result_json: Option<JsonValue>,
    error: Option<String>,
    created_by: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

impl DataPlaneProvisionJobRow {
    fn into_record(self) -> DataPlaneProvisionJobRecord {
        DataPlaneProvisionJobRecord {
            id: self.id,
            organization_id: self.organization_id,
            kind: self.kind,
            status: self.status,
            provider: self.provider,
            region: self.region,
            clickhouse_mode: self.clickhouse_mode,
            clickhouse_region: self.clickhouse_region,
            request: self.request_json,
            result: self.result_json,
            error: self.error,
            created_by: self.created_by,
            created_at: self.created_at,
            updated_at: self.updated_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
        }
    }
}

fn read_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    let cookie = headers.get(COOKIE)?.to_str().ok()?;
    for part in cookie.split(';') {
        let part = part.trim();
        let (key, value) = part.split_once('=')?;
        if key == name {
            return Some(value.to_string());
        }
    }
    None
}

fn read_organization_id(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("x-nanotrace-organization-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn read_api_key(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(value.to_string());
    }
    headers
        .get("x-nanotrace-api-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn is_api_key_subject(subject: &str) -> bool {
    subject.starts_with("api_key:")
}

fn normalize_email(email: &str) -> Result<String, AuthError> {
    let email = email.trim().to_ascii_lowercase();
    if email.is_empty()
        || email.len() > 320
        || email.contains(char::is_whitespace)
        || !email.contains('@')
    {
        return Err(AuthError::InvalidInput(
            "valid email is required".to_string(),
        ));
    }
    Ok(email)
}

fn random_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn short_token() -> String {
    let mut bytes = [0_u8; 6];
    rand::rng().fill_bytes(&mut bytes);
    hex_lower(&bytes)
}

fn token_hash(token: &str) -> String {
    hex_lower(Sha256::digest(token.as_bytes()).as_slice())
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

fn required_cfg<'a>(value: Option<&'a str>, key: &'static str) -> Result<&'a str, AuthError> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or(AuthError::MissingConfig(key))
}

fn safe_return_to(value: Option<&str>) -> String {
    let value = value.unwrap_or("/").trim();
    if value.starts_with('/') && !value.starts_with("//") {
        value.to_string()
    } else {
        "/".to_string()
    }
}

fn role_name(role: AuthRole) -> &'static str {
    match role {
        AuthRole::Viewer => "viewer",
        AuthRole::Admin => "admin",
        AuthRole::Service => "service",
    }
}

fn invite_role(role: AuthRole) -> Result<AuthRole, AuthError> {
    match role {
        AuthRole::Viewer | AuthRole::Admin => Ok(role),
        AuthRole::Service => Err(AuthError::InvalidInput(
            "organization invites support viewer or admin roles".to_string(),
        )),
    }
}

fn default_scopes(role: AuthRole) -> Vec<String> {
    match role {
        AuthRole::Admin => vec![
            "ingest:write".to_string(),
            "query:read".to_string(),
            "dashboards:write".to_string(),
            "api_keys:write".to_string(),
            "facets:write".to_string(),
            "processors:write".to_string(),
            "organizations:write".to_string(),
        ],
        AuthRole::Service => vec!["ingest:write".to_string(), "query:read".to_string()],
        AuthRole::Viewer => vec!["query:read".to_string()],
    }
}

fn normalize_scopes(scopes: &[String], role: AuthRole) -> Vec<String> {
    let mut normalized = Vec::new();
    let values: Vec<String> = if scopes.is_empty() {
        default_scopes(role)
    } else {
        scopes.to_vec()
    };
    for scope in values {
        let scope = scope.trim();
        if scope.is_empty() || normalized.iter().any(|existing| existing == scope) {
            continue;
        }
        normalized.push(scope.to_string());
    }
    normalized
}

fn validate_slug(value: &str) -> Result<&str, AuthError> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 48
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-'))
        || value.starts_with('-')
        || value.ends_with('-')
    {
        return Err(AuthError::InvalidInput(
            "organization slug must be lowercase letters, numbers, and dashes".to_string(),
        ));
    }
    Ok(value)
}

fn validate_name<'a>(value: &'a str, label: &str) -> Result<&'a str, AuthError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 120 {
        return Err(AuthError::InvalidInput(format!("{label} is required")));
    }
    Ok(value)
}

fn validate_url(value: &str, label: &str) -> Result<String, AuthError> {
    let value = value.trim().trim_end_matches('/');
    if value.is_empty()
        || value.len() > 2048
        || value.contains(char::is_whitespace)
        || !(value.starts_with("https://") || value.starts_with("http://"))
    {
        return Err(AuthError::InvalidInput(format!(
            "{label} must be an HTTP(S) URL"
        )));
    }
    Ok(value.to_string())
}

fn validate_token_or_default(value: &str, default: &str) -> String {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        default.to_string()
    } else {
        value.to_string()
    }
}

fn parse_role(value: &str) -> AuthRole {
    match value {
        "admin" => AuthRole::Admin,
        "service" => AuthRole::Service,
        "viewer" => AuthRole::Viewer,
        _ => AuthRole::Viewer,
    }
}

fn email_matches_pattern(email: &str, pattern: &str) -> bool {
    let pattern = pattern.trim();
    if pattern.is_empty() {
        return false;
    }
    if let Some(regex) = pattern
        .strip_prefix('/')
        .and_then(|value| value.strip_suffix('/'))
    {
        return Regex::new(regex)
            .map(|regex| regex.is_match(email))
            .unwrap_or(false);
    }
    if let Some(domain) = pattern.strip_prefix("*@") {
        return email
            .rsplit_once('@')
            .map(|(_, email_domain)| email_domain.eq_ignore_ascii_case(domain))
            .unwrap_or(false);
    }
    email.eq_ignore_ascii_case(pattern)
}

pub fn is_admin(identity: &AuthIdentity) -> bool {
    matches!(identity.role, AuthRole::Admin)
}

pub fn is_service_or_admin(identity: &AuthIdentity) -> bool {
    matches!(identity.role, AuthRole::Admin | AuthRole::Service)
}

pub fn has_scope(identity: &AuthIdentity, scope: &str) -> bool {
    is_admin(identity) || identity.scopes.iter().any(|candidate| candidate == scope)
}
