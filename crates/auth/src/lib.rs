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
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, postgres::PgPoolOptions};
use thiserror::Error;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub postgres_url: Option<String>,
    pub public_base_url: Option<String>,
    pub api_key_cache_refresh_interval: Duration,
    pub session_cookie_name: String,
    pub session_same_site: String,
    pub session_ttl: Duration,
    pub session_secure: bool,
    pub magic_link_ttl: Duration,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organizations: Option<Vec<OrganizationMembershipSummary>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_invitations: Option<Vec<PendingInvitationSummary>>,
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
pub struct OrganizationMembershipSummary {
    pub organization_id: String,
    pub organization_name: String,
    pub slug: String,
    pub role: AuthRole,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrganizationRecord {
    pub id: String,
    pub slug: String,
    pub name: String,
    pub plan: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub archived_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrganizationListResponse {
    pub active_organization_id: Option<String>,
    pub organizations: Vec<OrganizationMembershipSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrganizationMemberRecord {
    pub organization_id: String,
    pub subject: String,
    pub email: String,
    pub name: Option<String>,
    pub role: AuthRole,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrganizationInvitationRecord {
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
pub struct PendingInvitationSummary {
    pub id: i64,
    pub organization_id: String,
    pub organization_name: String,
    pub email: String,
    pub role: AuthRole,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct CreatedOrganizationInvitation {
    pub token: Option<String>,
    pub invitation: OrganizationInvitationRecord,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccountAuditEvent {
    pub id: i64,
    pub event_type: String,
    pub actor_subject: String,
    pub actor_auth_type: String,
    pub organization_id: Option<String>,
    pub target_subject: Option<String>,
    pub target_email: Option<String>,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct AuthStore {
    cfg: AuthConfig,
    pool: PgPool,
    api_key_cache: Arc<RwLock<ApiKeyCache>>,
}

#[derive(Debug, Clone)]
pub struct ApiKeyCacheStats {
    pub loaded: bool,
    pub entries: usize,
    pub age: Option<Duration>,
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
        store.run_migrations().await?;
        store.cleanup_expired_invitations().await?;
        store.cleanup_expired_oauth_states().await?;
        store.refresh_api_key_cache().await?;
        store.spawn_api_key_cache_refresher();
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

    pub async fn complete_login(
        &self,
        token: &str,
        headers: Option<&HeaderMap>,
    ) -> Result<LoginComplete, AuthError> {
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
        self.finish_login(&email, None, return_to, headers, "magic_link")
            .await
    }

    pub async fn start_oauth_login(
        &self,
        provider: &str,
        return_to: Option<&str>,
    ) -> Result<OAuthStart, AuthError> {
        let provider = normalized_provider(provider)?;
        let state = random_token();
        let state_hash = token_hash(&state);
        let expires_at = Utc::now()
            + chrono::Duration::from_std(self.cfg.magic_link_ttl)
                .unwrap_or_else(|_| chrono::Duration::minutes(10));
        let return_to = safe_return_to(return_to);
        self.cleanup_expired_oauth_states().await?;
        sqlx::query(
            "INSERT INTO nanotrace_oauth_states (token_hash, provider, return_to, expires_at)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&state_hash)
        .bind(&provider)
        .bind(&return_to)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;

        Ok(OAuthStart {
            provider,
            state,
            return_to,
            expires_at,
        })
    }

    pub async fn consume_oauth_state(
        &self,
        provider: &str,
        state: &str,
    ) -> Result<String, AuthError> {
        let provider = normalized_provider(provider)?;
        let state = state.trim();
        if state.is_empty() {
            return Err(AuthError::InvalidLoginToken);
        }
        let row = sqlx::query_as::<_, (String,)>(
            "DELETE FROM nanotrace_oauth_states
             WHERE token_hash = $1 AND provider = $2 AND expires_at > now()
             RETURNING return_to",
        )
        .bind(token_hash(state))
        .bind(provider)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|(return_to,)| return_to)
            .ok_or(AuthError::InvalidLoginToken)
    }

    pub async fn complete_external_login(
        &self,
        email: &str,
        name: Option<&str>,
        return_to: &str,
        headers: Option<&HeaderMap>,
        source: &str,
    ) -> Result<LoginComplete, AuthError> {
        self.finish_login(
            email,
            name,
            safe_return_to(Some(return_to)),
            headers,
            source,
        )
        .await
    }

    async fn finish_login(
        &self,
        email: &str,
        name: Option<&str>,
        return_to: String,
        headers: Option<&HeaderMap>,
        source: &str,
    ) -> Result<LoginComplete, AuthError> {
        let email = normalize_email(email)?;
        let name = name
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let subject = format!("email:{email}");
        self.upsert_user(&subject, &email, name.as_deref(), AuthRole::Viewer)
            .await?;
        let memberships = self.memberships_for_subject(&subject).await?;
        let pending_invites = self.pending_invitations_for_email(&email).await?;
        let previous_active_organization_id = match headers {
            Some(headers) => {
                self.previous_active_organization_id(headers, &subject, &memberships)
                    .await?
            }
            None => None,
        };
        let mut created_organization_id = None;
        let active_organization_id = if previous_active_organization_id.is_some() {
            previous_active_organization_id
        } else if let Some(membership) = memberships.first() {
            Some(membership.organization_id.clone())
        } else if pending_invites.is_empty() {
            let organization = self
                .create_organization_for_subject(&subject, personal_organization_name(&email), None)
                .await?;
            self.record_account_audit_event(
                "organization.created",
                &subject,
                "login",
                Some(&organization.id),
                None,
                Some(&email),
                serde_json::json!({ "source": source, "reason": "first_login", "slug": &organization.slug }),
            )
            .await?;
            tracing::info!(
                actor_subject = %subject,
                organization_id = %organization.id,
                "organization created during first login"
            );
            created_organization_id = Some(organization.id.clone());
            Some(organization.id)
        } else {
            None
        };
        let session_token = self
            .create_session(
                &subject,
                &email,
                name.as_deref(),
                AuthRole::Viewer,
                active_organization_id.as_deref(),
            )
            .await?;
        Ok(LoginComplete {
            return_to,
            session_cookie: self.cookie_header(
                &self.cfg.session_cookie_name,
                &session_token,
                self.cfg.session_ttl.as_secs(),
                true,
            ),
            active_organization_id,
            created_organization_id,
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
        let session_hash = token_hash(&token);
        let row = sqlx::query_as::<_, (String, String, Option<String>, String, Option<String>)>(
            "UPDATE nanotrace_auth_sessions
             SET last_seen_at = now()
             WHERE token_hash = $1 AND expires_at > now()
             RETURNING subject, email, name, role, active_organization_id",
        )
        .bind(&session_hash)
        .fetch_optional(&self.pool)
        .await?;
        let Some((subject, email, name, _session_role, active_organization_id)) = row else {
            return Err(AuthError::Unauthorized);
        };
        let memberships = self.memberships_for_subject(&subject).await?;
        let organization = self
            .resolve_active_membership(&subject, active_organization_id.as_deref(), &memberships)
            .await?;
        if active_organization_id.as_deref()
            != organization
                .as_ref()
                .map(|row| row.organization_id.as_str())
        {
            sqlx::query(
                "UPDATE nanotrace_auth_sessions
                 SET active_organization_id = $1
                 WHERE token_hash = $2",
            )
            .bind(
                organization
                    .as_ref()
                    .map(|row| row.organization_id.as_str()),
            )
            .bind(&session_hash)
            .execute(&self.pool)
            .await?;
        }
        let pending_invitations = self.pending_invitations_for_email(&email).await?;
        let role = organization
            .as_ref()
            .map(|row| parse_role(&row.role))
            .unwrap_or(AuthRole::Viewer);
        Ok(AuthIdentity {
            auth_type: AuthType::Session,
            subject,
            email: Some(email),
            name,
            role,
            tenant_id: organization
                .as_ref()
                .map(|row| row.organization_id.clone())
                .unwrap_or_default(),
            organization_id: organization
                .as_ref()
                .map(|row| row.organization_id.clone())
                .unwrap_or_default(),
            organization_name: organization
                .as_ref()
                .map(|row| row.organization_name.clone())
                .unwrap_or_default(),
            organizations: Some(memberships),
            pending_invitations: Some(pending_invitations),
            scopes: if organization.is_some() {
                default_scopes(role)
            } else {
                Vec::new()
            },
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
            organizations: None,
            pending_invitations: None,
            scopes: normalize_scopes(&row.scopes, parse_role(&row.role)),
        })
    }

    pub fn api_key_cache_stats(&self) -> ApiKeyCacheStats {
        let Ok(cache) = self.api_key_cache.read() else {
            return ApiKeyCacheStats {
                loaded: false,
                entries: 0,
                age: None,
            };
        };
        ApiKeyCacheStats {
            loaded: cache.loaded_at.is_some(),
            entries: cache.keys.len(),
            age: cache.loaded_at.map(|loaded_at| loaded_at.elapsed()),
        }
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
        let organization_exists = sqlx::query_as::<_, (String,)>(
            "SELECT id
             FROM nanotrace_organizations
             WHERE id = $1 AND archived_at IS NULL",
        )
        .bind(organization_id)
        .fetch_optional(&self.pool)
        .await?;
        if organization_exists.is_none() {
            return Err(AuthError::NotFound);
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
        self.refresh_api_key_cache().await?;
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

    pub async fn list_organization_ids(&self) -> Result<Vec<String>, AuthError> {
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT id
             FROM nanotrace_organizations
             WHERE archived_at IS NULL
             ORDER BY id ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
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
        let row = row.ok_or(AuthError::NotFound)?;
        self.refresh_api_key_cache().await?;
        Ok(row.into_record())
    }

    pub async fn list_session_organizations(
        &self,
        subject: &str,
        active_organization_id: Option<&str>,
    ) -> Result<OrganizationListResponse, AuthError> {
        let organizations = self.memberships_for_subject(subject).await?;
        let active_organization_id = active_organization_id
            .filter(|id| {
                organizations
                    .iter()
                    .any(|organization| organization.organization_id == *id)
            })
            .map(ToOwned::to_owned)
            .or_else(|| organizations.first().map(|row| row.organization_id.clone()));
        Ok(OrganizationListResponse {
            active_organization_id,
            organizations,
        })
    }

    pub async fn create_organization_for_subject(
        &self,
        subject: &str,
        name: String,
        requested_slug: Option<&str>,
    ) -> Result<OrganizationRecord, AuthError> {
        let name = normalized_organization_name(&name)?;
        let slug_base = match requested_slug {
            Some(slug) => normalized_slug(slug)?,
            None => slug_from_name(&name),
        };
        let mut slug = slug_base.clone();
        let mut suffix = 2_u32;
        loop {
            let id = format!("org_{}", random_token());
            let mut tx = self.pool.begin().await?;
            let inserted = sqlx::query_as::<_, OrganizationRecordRow>(
                "INSERT INTO nanotrace_organizations (id, slug, name)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (slug) DO NOTHING
                 RETURNING id, slug, name, plan, created_at, updated_at, archived_at",
            )
            .bind(&id)
            .bind(&slug)
            .bind(&name)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(row) = inserted {
                sqlx::query(
                    "INSERT INTO nanotrace_organization_members
                        (organization_id, subject, role, updated_at)
                     VALUES ($1, $2, 'admin', now())
                     ON CONFLICT (organization_id, subject)
                     DO UPDATE SET role = 'admin', updated_at = now()",
                )
                .bind(&row.id)
                .bind(subject)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                return Ok(row.into_record());
            }
            tx.rollback().await?;
            slug = format!("{slug_base}-{suffix}");
            suffix += 1;
            if suffix > 100 {
                return Err(AuthError::InvalidInput(
                    "could not generate a unique organization slug".to_string(),
                ));
            }
        }
    }

    pub async fn create_organization_for_session(
        &self,
        headers: &HeaderMap,
        subject: &str,
        name: String,
        requested_slug: Option<&str>,
    ) -> Result<OrganizationRecord, AuthError> {
        let token_hash = self.session_token_hash(headers)?;
        let name = normalized_organization_name(&name)?;
        let slug_base = match requested_slug {
            Some(slug) => normalized_slug(slug)?,
            None => slug_from_name(&name),
        };
        let mut slug = slug_base.clone();
        let mut suffix = 2_u32;
        loop {
            let id = format!("org_{}", random_token());
            let mut tx = self.pool.begin().await?;
            let (session_subject,) = sqlx::query_as::<_, (String,)>(
                "SELECT subject
                 FROM nanotrace_auth_sessions
                 WHERE token_hash = $1 AND expires_at > now()
                 FOR UPDATE",
            )
            .bind(&token_hash)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(AuthError::Unauthorized)?;
            if session_subject != subject {
                return Err(AuthError::Forbidden);
            }
            let inserted = sqlx::query_as::<_, OrganizationRecordRow>(
                "INSERT INTO nanotrace_organizations (id, slug, name)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (slug) DO NOTHING
                 RETURNING id, slug, name, plan, created_at, updated_at, archived_at",
            )
            .bind(&id)
            .bind(&slug)
            .bind(&name)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(row) = inserted {
                sqlx::query(
                    "INSERT INTO nanotrace_organization_members
                        (organization_id, subject, role, updated_at)
                     VALUES ($1, $2, 'admin', now())",
                )
                .bind(&row.id)
                .bind(subject)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "UPDATE nanotrace_auth_sessions
                     SET active_organization_id = $1, last_seen_at = now()
                     WHERE token_hash = $2",
                )
                .bind(&row.id)
                .bind(&token_hash)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                return Ok(row.into_record());
            }
            tx.rollback().await?;
            slug = format!("{slug_base}-{suffix}");
            suffix += 1;
            if suffix > 100 {
                return Err(AuthError::InvalidInput(
                    "could not generate a unique organization slug".to_string(),
                ));
            }
        }
    }

    pub async fn update_organization(
        &self,
        organization_id: &str,
        name: Option<String>,
        requested_slug: Option<&str>,
    ) -> Result<OrganizationRecord, AuthError> {
        let name = match name {
            Some(name) => Some(normalized_organization_name(&name)?),
            None => None,
        };
        let slug = match requested_slug {
            Some(slug) => Some(normalized_slug(slug)?),
            None => None,
        };
        if name.is_none() && slug.is_none() {
            return Err(AuthError::InvalidInput(
                "organization name or slug is required".to_string(),
            ));
        }
        if let Some(slug) = slug.as_deref() {
            let existing = sqlx::query_as::<_, (String,)>(
                "SELECT id FROM nanotrace_organizations WHERE slug = $1 AND id <> $2",
            )
            .bind(slug)
            .bind(organization_id)
            .fetch_optional(&self.pool)
            .await?;
            if existing.is_some() {
                return Err(AuthError::InvalidInput(
                    "organization slug is already in use".to_string(),
                ));
            }
        }
        let row = sqlx::query_as::<_, OrganizationRecordRow>(
            "UPDATE nanotrace_organizations
             SET name = COALESCE($2, name),
                 slug = COALESCE($3, slug),
                 updated_at = now()
             WHERE id = $1 AND archived_at IS NULL
             RETURNING id, slug, name, plan, created_at, updated_at, archived_at",
        )
        .bind(organization_id)
        .bind(name)
        .bind(slug)
        .fetch_optional(&self.pool)
        .await?;
        row.map(OrganizationRecordRow::into_record)
            .ok_or(AuthError::NotFound)
    }

    pub async fn archive_organization(
        &self,
        organization_id: &str,
    ) -> Result<OrganizationRecord, AuthError> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query_as::<_, OrganizationRecordRow>(
            "UPDATE nanotrace_organizations
             SET archived_at = COALESCE(archived_at, now()), updated_at = now()
             WHERE id = $1 AND archived_at IS NULL
             RETURNING id, slug, name, plan, created_at, updated_at, archived_at",
        )
        .bind(organization_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AuthError::NotFound)?;
        sqlx::query(
            "UPDATE nanotrace_api_keys
             SET revoked_at = COALESCE(revoked_at, now())
             WHERE organization_id = $1",
        )
        .bind(organization_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE nanotrace_auth_sessions
             SET active_organization_id = NULL
             WHERE active_organization_id = $1",
        )
        .bind(organization_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE nanotrace_organization_invitations
             SET revoked_at = COALESCE(revoked_at, now())
             WHERE organization_id = $1
               AND accepted_at IS NULL
               AND revoked_at IS NULL",
        )
        .bind(organization_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        self.refresh_api_key_cache().await?;
        Ok(row.into_record())
    }

    pub async fn switch_session_organization(
        &self,
        headers: &HeaderMap,
        organization_id: &str,
    ) -> Result<OrganizationMembershipSummary, AuthError> {
        let token_hash = self.session_token_hash(headers)?;
        let mut tx = self.pool.begin().await?;
        let (subject,) = sqlx::query_as::<_, (String,)>(
            "SELECT subject
             FROM nanotrace_auth_sessions
             WHERE token_hash = $1 AND expires_at > now()
             FOR UPDATE",
        )
        .bind(&token_hash)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AuthError::Unauthorized)?;
        let membership = sqlx::query_as::<_, OrganizationMembershipRow>(
            "SELECT o.id AS organization_id, o.name AS organization_name, o.slug,
                    m.role, m.created_at, m.updated_at
             FROM nanotrace_organization_members AS m
             INNER JOIN nanotrace_organizations AS o ON o.id = m.organization_id
             WHERE m.subject = $1
               AND m.organization_id = $2
               AND o.archived_at IS NULL",
        )
        .bind(&subject)
        .bind(organization_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AuthError::Forbidden)?
        .into_summary();
        sqlx::query(
            "UPDATE nanotrace_auth_sessions
             SET active_organization_id = $1, last_seen_at = now()
             WHERE token_hash = $2",
        )
        .bind(organization_id)
        .bind(token_hash)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(membership)
    }

    pub async fn list_organization_members(
        &self,
        organization_id: &str,
    ) -> Result<Vec<OrganizationMemberRecord>, AuthError> {
        let rows = sqlx::query_as::<_, OrganizationMemberRow>(
            "SELECT m.organization_id, m.subject, u.email, u.name, m.role,
                    m.created_at, m.updated_at
             FROM nanotrace_organization_members AS m
             INNER JOIN nanotrace_auth_users AS u ON u.subject = m.subject
             INNER JOIN nanotrace_organizations AS o ON o.id = m.organization_id
             WHERE m.organization_id = $1
               AND o.archived_at IS NULL
             ORDER BY m.created_at ASC, u.email ASC",
        )
        .bind(organization_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(OrganizationMemberRow::into_record)
            .collect())
    }

    pub async fn set_organization_member_role(
        &self,
        organization_id: &str,
        subject: &str,
        role: AuthRole,
    ) -> Result<OrganizationMemberRecord, AuthError> {
        if matches!(role, AuthRole::Service) {
            return Err(AuthError::InvalidInput(
                "organization member role must be admin or viewer".to_string(),
            ));
        }
        let mut tx = self.pool.begin().await?;
        let members = sqlx::query_as::<_, (String, String)>(
            "SELECT subject, role
             FROM nanotrace_organization_members
             WHERE organization_id = $1
             FOR UPDATE",
        )
        .bind(organization_id)
        .fetch_all(&mut *tx)
        .await?;
        let existing_role = members
            .iter()
            .find(|(member_subject, _)| member_subject == subject)
            .map(|(_, role)| parse_role(role))
            .ok_or(AuthError::NotFound)?;
        if matches!(existing_role, AuthRole::Admin) && !matches!(role, AuthRole::Admin) {
            let other_admin_exists = members.iter().any(|(member_subject, member_role)| {
                member_subject != subject && matches!(parse_role(member_role), AuthRole::Admin)
            });
            if !other_admin_exists {
                return Err(AuthError::InvalidInput(
                    "organization must have at least one admin".to_string(),
                ));
            }
        }
        let row = sqlx::query_as::<_, OrganizationMemberRow>(
            "UPDATE nanotrace_organization_members AS m
             SET role = $3, updated_at = now()
             FROM nanotrace_auth_users AS u
             WHERE m.organization_id = $1
               AND m.subject = $2
               AND u.subject = m.subject
             RETURNING m.organization_id, m.subject, u.email, u.name, m.role,
                       m.created_at, m.updated_at",
        )
        .bind(organization_id)
        .bind(subject)
        .bind(role_name(role))
        .fetch_optional(&mut *tx)
        .await?;
        let member = row
            .map(OrganizationMemberRow::into_record)
            .ok_or(AuthError::NotFound)?;
        tx.commit().await?;
        Ok(member)
    }

    pub async fn remove_organization_member(
        &self,
        organization_id: &str,
        subject: &str,
    ) -> Result<OrganizationMemberRecord, AuthError> {
        let mut tx = self.pool.begin().await?;
        let members = sqlx::query_as::<_, (String, String)>(
            "SELECT subject, role
             FROM nanotrace_organization_members
             WHERE organization_id = $1
             FOR UPDATE",
        )
        .bind(organization_id)
        .fetch_all(&mut *tx)
        .await?;
        let existing_role = members
            .iter()
            .find(|(member_subject, _)| member_subject == subject)
            .map(|(_, role)| parse_role(role))
            .ok_or(AuthError::NotFound)?;
        if matches!(existing_role, AuthRole::Admin) {
            let other_admin_exists = members.iter().any(|(member_subject, member_role)| {
                member_subject != subject && matches!(parse_role(member_role), AuthRole::Admin)
            });
            if !other_admin_exists {
                return Err(AuthError::InvalidInput(
                    "organization must have at least one admin".to_string(),
                ));
            }
        }
        let row = sqlx::query_as::<_, OrganizationMemberRow>(
            "DELETE FROM nanotrace_organization_members AS m
             USING nanotrace_auth_users AS u
             WHERE m.organization_id = $1
               AND m.subject = $2
               AND u.subject = m.subject
             RETURNING m.organization_id, m.subject, u.email, u.name, m.role,
                       m.created_at, m.updated_at",
        )
        .bind(organization_id)
        .bind(subject)
        .fetch_optional(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE nanotrace_auth_sessions
             SET active_organization_id = NULL
             WHERE subject = $1 AND active_organization_id = $2",
        )
        .bind(subject)
        .bind(organization_id)
        .execute(&mut *tx)
        .await?;
        let member = row
            .map(OrganizationMemberRow::into_record)
            .ok_or(AuthError::NotFound)?;
        tx.commit().await?;
        Ok(member)
    }

    pub async fn list_organization_invitations(
        &self,
        organization_id: &str,
    ) -> Result<Vec<OrganizationInvitationRecord>, AuthError> {
        let rows = sqlx::query_as::<_, OrganizationInvitationRow>(
            "SELECT id, organization_id, email, role, invited_by, created_at,
                    expires_at, accepted_at, revoked_at
             FROM nanotrace_organization_invitations AS i
             WHERE organization_id = $1
               AND EXISTS (
                   SELECT 1
                   FROM nanotrace_organizations AS o
                   WHERE o.id = i.organization_id
                     AND o.archived_at IS NULL
               )
             ORDER BY created_at DESC, id DESC",
        )
        .bind(organization_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(OrganizationInvitationRow::into_record)
            .collect())
    }

    pub async fn create_organization_invitation(
        &self,
        organization_id: &str,
        email: &str,
        role: AuthRole,
        invited_by: &str,
    ) -> Result<CreatedOrganizationInvitation, AuthError> {
        if matches!(role, AuthRole::Service) {
            return Err(AuthError::InvalidInput(
                "invitation role must be admin or viewer".to_string(),
            ));
        }
        let email = normalize_email(email)?;
        let token = random_token();
        let token_hash = token_hash(&token);
        let expires_at = Utc::now() + chrono::Duration::days(14);
        self.cleanup_expired_invitations().await?;
        let mut tx = self.pool.begin().await?;
        let organization_exists = sqlx::query_as::<_, (String,)>(
            "SELECT id
             FROM nanotrace_organizations
             WHERE id = $1 AND archived_at IS NULL",
        )
        .bind(organization_id)
        .fetch_optional(&mut *tx)
        .await?;
        if organization_exists.is_none() {
            return Err(AuthError::NotFound);
        }
        let existing = sqlx::query_as::<_, OrganizationInvitationRow>(
            "SELECT id, organization_id, email, role, invited_by, created_at,
                    expires_at, accepted_at, revoked_at
             FROM nanotrace_organization_invitations
             WHERE organization_id = $1
               AND email = $2
               AND expires_at > now()
               AND accepted_at IS NULL
               AND revoked_at IS NULL
             ORDER BY created_at DESC, id DESC
             LIMIT 1",
        )
        .bind(organization_id)
        .bind(&email)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(row) = existing {
            tx.commit().await?;
            return Ok(CreatedOrganizationInvitation {
                token: None,
                invitation: row.into_record(),
            });
        }
        let row = sqlx::query_as::<_, OrganizationInvitationRow>(
            "INSERT INTO nanotrace_organization_invitations
                (organization_id, email, role, token_hash, invited_by, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6)
             RETURNING id, organization_id, email, role, invited_by, created_at,
                       expires_at, accepted_at, revoked_at",
        )
        .bind(organization_id)
        .bind(email)
        .bind(role_name(role))
        .bind(token_hash)
        .bind(invited_by)
        .bind(expires_at)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(CreatedOrganizationInvitation {
            token: Some(token),
            invitation: row.into_record(),
        })
    }

    pub async fn resend_organization_invitation(
        &self,
        organization_id: &str,
        invitation_id: i64,
    ) -> Result<CreatedOrganizationInvitation, AuthError> {
        let token = random_token();
        let token_hash = token_hash(&token);
        let expires_at = Utc::now() + chrono::Duration::days(14);
        let row = sqlx::query_as::<_, OrganizationInvitationRow>(
            "UPDATE nanotrace_organization_invitations AS i
             SET token_hash = $3, expires_at = $4
             WHERE i.id = $1
               AND i.organization_id = $2
               AND i.expires_at > now()
               AND i.accepted_at IS NULL
               AND i.revoked_at IS NULL
               AND EXISTS (
                   SELECT 1
                   FROM nanotrace_organizations AS o
                   WHERE o.id = i.organization_id
                     AND o.archived_at IS NULL
               )
             RETURNING id, organization_id, email, role, invited_by, created_at,
                       expires_at, accepted_at, revoked_at",
        )
        .bind(invitation_id)
        .bind(organization_id)
        .bind(token_hash)
        .bind(expires_at)
        .fetch_optional(&self.pool)
        .await?;
        let row = row.ok_or(AuthError::NotFound)?;
        Ok(CreatedOrganizationInvitation {
            token: Some(token),
            invitation: row.into_record(),
        })
    }

    pub async fn revoke_organization_invitation(
        &self,
        organization_id: &str,
        invitation_id: i64,
    ) -> Result<OrganizationInvitationRecord, AuthError> {
        let row = sqlx::query_as::<_, OrganizationInvitationRow>(
            "UPDATE nanotrace_organization_invitations AS i
             SET revoked_at = COALESCE(revoked_at, now())
             WHERE id = $1
               AND organization_id = $2
               AND accepted_at IS NULL
               AND EXISTS (
                   SELECT 1
                   FROM nanotrace_organizations AS o
                   WHERE o.id = i.organization_id
                     AND o.archived_at IS NULL
               )
             RETURNING id, organization_id, email, role, invited_by, created_at,
                       expires_at, accepted_at, revoked_at",
        )
        .bind(invitation_id)
        .bind(organization_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(OrganizationInvitationRow::into_record)
            .ok_or(AuthError::NotFound)
    }

    pub async fn accept_organization_invitation(
        &self,
        headers: &HeaderMap,
        token: &str,
    ) -> Result<OrganizationMembershipSummary, AuthError> {
        let token = token.trim();
        if token.is_empty() {
            return Err(AuthError::InvalidInput(
                "invitation token is required".to_string(),
            ));
        }
        let session_hash = self.session_token_hash(headers)?;
        let mut tx = self.pool.begin().await?;
        let (subject, email) = sqlx::query_as::<_, (String, String)>(
            "SELECT subject, email
             FROM nanotrace_auth_sessions
             WHERE token_hash = $1 AND expires_at > now()
             FOR UPDATE",
        )
        .bind(&session_hash)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AuthError::Unauthorized)?;
        let email = normalize_email(&email)?;
        let invite_hash = token_hash(token);
        let row = sqlx::query_as::<_, (String, String)>(
            "UPDATE nanotrace_organization_invitations AS i
             SET accepted_at = now()
             WHERE token_hash = $1
               AND expires_at > now()
               AND accepted_at IS NULL
               AND revoked_at IS NULL
               AND email = $2
               AND EXISTS (
                   SELECT 1
                   FROM nanotrace_organizations AS o
                   WHERE o.id = i.organization_id
                     AND o.archived_at IS NULL
               )
             RETURNING organization_id, role",
        )
        .bind(invite_hash)
        .bind(&email)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AuthError::Forbidden)?;
        let (organization_id, role) = row;
        sqlx::query(
            "INSERT INTO nanotrace_organization_members
                (organization_id, subject, role, updated_at)
             VALUES ($1, $2, $3, now())
             ON CONFLICT (organization_id, subject)
             DO UPDATE SET role = EXCLUDED.role, updated_at = now()",
        )
        .bind(&organization_id)
        .bind(&subject)
        .bind(&role)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE nanotrace_auth_sessions
             SET active_organization_id = $1
             WHERE token_hash = $2",
        )
        .bind(&organization_id)
        .bind(session_hash)
        .execute(&mut *tx)
        .await?;
        let membership = sqlx::query_as::<_, OrganizationMembershipRow>(
            "SELECT o.id AS organization_id, o.name AS organization_name, o.slug,
                    m.role, m.created_at, m.updated_at
             FROM nanotrace_organization_members AS m
             INNER JOIN nanotrace_organizations AS o ON o.id = m.organization_id
             WHERE m.subject = $1
               AND m.organization_id = $2
               AND o.archived_at IS NULL",
        )
        .bind(&subject)
        .bind(&organization_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AuthError::NotFound)?
        .into_summary();
        tx.commit().await?;
        Ok(membership)
    }

    async fn run_migrations(&self) -> Result<(), AuthError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub async fn cleanup_expired_invitations(&self) -> Result<u64, AuthError> {
        let result = sqlx::query(
            "UPDATE nanotrace_organization_invitations
             SET revoked_at = now()
             WHERE expires_at <= now()
               AND accepted_at IS NULL
               AND revoked_at IS NULL",
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn cleanup_expired_oauth_states(&self) -> Result<u64, AuthError> {
        let result = sqlx::query("DELETE FROM nanotrace_oauth_states WHERE expires_at <= now()")
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    pub async fn record_account_audit_event(
        &self,
        event_type: &str,
        actor_subject: &str,
        actor_auth_type: &str,
        organization_id: Option<&str>,
        target_subject: Option<&str>,
        target_email: Option<&str>,
        metadata: serde_json::Value,
    ) -> Result<AccountAuditEvent, AuthError> {
        let row = sqlx::query_as::<_, AccountAuditEventRow>(
            "INSERT INTO nanotrace_account_audit_events
                (event_type, actor_subject, actor_auth_type, organization_id,
                 target_subject, target_email, metadata)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             RETURNING id, event_type, actor_subject, actor_auth_type, organization_id,
                       target_subject, target_email, metadata, created_at",
        )
        .bind(event_type)
        .bind(actor_subject)
        .bind(actor_auth_type)
        .bind(organization_id)
        .bind(target_subject)
        .bind(target_email)
        .bind(metadata)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.into_event())
    }

    async fn cached_api_key(&self, key_hash: &str) -> Result<Option<CachedApiKey>, AuthError> {
        if let Some(cached) = self.cached_api_key_if_loaded(key_hash)? {
            return Ok(cached);
        }

        self.refresh_api_key_cache().await?;
        Ok(self.cached_api_key_if_loaded(key_hash)?.flatten())
    }

    fn cached_api_key_if_loaded(
        &self,
        key_hash: &str,
    ) -> Result<Option<Option<CachedApiKey>>, AuthError> {
        let cache = self
            .api_key_cache
            .read()
            .map_err(|_| AuthError::InvalidInput("API key cache lock poisoned".to_string()))?;
        if cache.loaded_at.is_none() {
            return Ok(None);
        };
        Ok(Some(cache.keys.get(key_hash).cloned()))
    }

    async fn refresh_api_key_cache(&self) -> Result<(), AuthError> {
        let rows = sqlx::query_as::<_, CachedApiKeyRow>(
            "SELECT k.key_hash, k.name, k.role, k.scopes, k.organization_id, o.name AS organization_name
             FROM nanotrace_api_keys AS k
             INNER JOIN nanotrace_organizations AS o ON k.organization_id = o.id
             WHERE k.revoked_at IS NULL
               AND o.archived_at IS NULL
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

    fn spawn_api_key_cache_refresher(&self) {
        let interval = self.cfg.api_key_cache_refresh_interval;
        let store = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(err) = store.refresh_api_key_cache().await {
                    tracing::warn!(error = %err, "failed to refresh Nanotrace API key cache");
                }
            }
        });
    }

    async fn upsert_user(
        &self,
        subject: &str,
        email: &str,
        name: Option<&str>,
        role: AuthRole,
    ) -> Result<(), AuthError> {
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
        Ok(())
    }

    async fn create_session(
        &self,
        subject: &str,
        email: &str,
        name: Option<&str>,
        role: AuthRole,
        active_organization_id: Option<&str>,
    ) -> Result<String, AuthError> {
        let token = random_token();
        let token_hash = token_hash(&token);
        let expires_at = Utc::now()
            + chrono::Duration::from_std(self.cfg.session_ttl)
                .unwrap_or_else(|_| chrono::Duration::days(7));
        sqlx::query(
            "INSERT INTO nanotrace_auth_sessions
                (token_hash, subject, email, name, role, active_organization_id, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(token_hash)
        .bind(subject)
        .bind(email)
        .bind(name)
        .bind(role_name(role))
        .bind(active_organization_id)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;
        Ok(token)
    }

    async fn memberships_for_subject(
        &self,
        subject: &str,
    ) -> Result<Vec<OrganizationMembershipSummary>, AuthError> {
        let rows = sqlx::query_as::<_, OrganizationMembershipRow>(
            "SELECT o.id AS organization_id, o.name AS organization_name, o.slug,
                    m.role, m.created_at, m.updated_at
             FROM nanotrace_organization_members AS m
             INNER JOIN nanotrace_organizations AS o ON o.id = m.organization_id
             WHERE m.subject = $1
               AND o.archived_at IS NULL
             ORDER BY m.updated_at DESC, m.created_at DESC, o.name ASC",
        )
        .bind(subject)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(OrganizationMembershipRow::into_summary)
            .collect())
    }

    async fn resolve_active_membership(
        &self,
        _subject: &str,
        active_organization_id: Option<&str>,
        memberships: &[OrganizationMembershipSummary],
    ) -> Result<Option<OrganizationIdentityRow>, AuthError> {
        if let Some(active_organization_id) = active_organization_id
            && let Some(membership) = memberships
                .iter()
                .find(|membership| membership.organization_id == active_organization_id)
        {
            return Ok(Some(OrganizationIdentityRow {
                organization_id: membership.organization_id.clone(),
                organization_name: membership.organization_name.clone(),
                role: role_name(membership.role).to_string(),
            }));
        }
        if let Some(membership) = memberships.first() {
            return Ok(Some(OrganizationIdentityRow {
                organization_id: membership.organization_id.clone(),
                organization_name: membership.organization_name.clone(),
                role: role_name(membership.role).to_string(),
            }));
        }
        Ok(None)
    }

    async fn pending_invitations_for_email(
        &self,
        email: &str,
    ) -> Result<Vec<PendingInvitationSummary>, AuthError> {
        let email = normalize_email(email)?;
        let rows = sqlx::query_as::<_, PendingInvitationRow>(
            "SELECT i.id, i.organization_id, o.name AS organization_name,
                    i.email, i.role, i.expires_at
             FROM nanotrace_organization_invitations AS i
             INNER JOIN nanotrace_organizations AS o ON o.id = i.organization_id
             WHERE i.email = $1
               AND o.archived_at IS NULL
               AND i.expires_at > now()
               AND i.accepted_at IS NULL
               AND i.revoked_at IS NULL
             ORDER BY i.created_at DESC, i.id DESC",
        )
        .bind(email)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(PendingInvitationRow::into_summary)
            .collect())
    }

    async fn previous_active_organization_id(
        &self,
        headers: &HeaderMap,
        subject: &str,
        memberships: &[OrganizationMembershipSummary],
    ) -> Result<Option<String>, AuthError> {
        let Some(token) = read_cookie(headers, &self.cfg.session_cookie_name) else {
            return Ok(None);
        };
        let (session_subject, active_organization_id) =
            match sqlx::query_as::<_, (String, Option<String>)>(
                "SELECT subject, active_organization_id
                 FROM nanotrace_auth_sessions
                 WHERE token_hash = $1 AND expires_at > now()",
            )
            .bind(token_hash(&token))
            .fetch_optional(&self.pool)
            .await?
            {
                Some(row) => row,
                None => return Ok(None),
            };
        if session_subject != subject {
            return Ok(None);
        }
        Ok(active_organization_id.filter(|organization_id| {
            memberships
                .iter()
                .any(|membership| membership.organization_id == *organization_id)
        }))
    }

    async fn member_role(
        &self,
        organization_id: &str,
        subject: &str,
    ) -> Result<Option<AuthRole>, AuthError> {
        let row = sqlx::query_as::<_, (String,)>(
            "SELECT role
             FROM nanotrace_organization_members
             WHERE organization_id = $1
               AND subject = $2
               AND EXISTS (
                   SELECT 1
                   FROM nanotrace_organizations
                   WHERE id = $1 AND archived_at IS NULL
               )",
        )
        .bind(organization_id)
        .bind(subject)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(role,)| parse_role(&role)))
    }

    pub async fn is_organization_admin(
        &self,
        organization_id: &str,
        subject: &str,
    ) -> Result<bool, AuthError> {
        Ok(matches!(
            self.member_role(organization_id, subject).await?,
            Some(AuthRole::Admin)
        ))
    }

    fn session_token_hash(&self, headers: &HeaderMap) -> Result<String, AuthError> {
        let token =
            read_cookie(headers, &self.cfg.session_cookie_name).ok_or(AuthError::Unauthorized)?;
        Ok(token_hash(&token))
    }

    fn cookie_header(&self, name: &str, value: &str, max_age_secs: u64, http_only: bool) -> String {
        let mut parts = vec![
            format!("{name}={value}"),
            "Path=/".to_string(),
            format!("Max-Age={max_age_secs}"),
            format!("SameSite={}", self.cfg.session_same_site),
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
            format!("SameSite={}", self.cfg.session_same_site),
            "HttpOnly".to_string(),
        ];
        if self.cfg.session_secure {
            parts.push("Secure".to_string());
        }
        parts.join("; ")
    }
}

pub struct LoginStart {
    pub email: String,
    pub login_url: String,
    pub expires_at: DateTime<Utc>,
}

pub struct OAuthStart {
    pub provider: String,
    pub state: String,
    pub return_to: String,
    pub expires_at: DateTime<Utc>,
}

pub struct LoginComplete {
    pub return_to: String,
    pub session_cookie: String,
    pub active_organization_id: Option<String>,
    pub created_organization_id: Option<String>,
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
    #[error("not found")]
    NotFound,
    #[error("{0}")]
    InvalidInput(String),
    #[error("{0} is required")]
    MissingConfig(&'static str),
    #[error("cookie error: {0}")]
    Cookie(String),
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl AuthError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::Unauthorized | Self::InvalidLoginToken => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::InvalidInput(_) => StatusCode::BAD_REQUEST,
            Self::MissingConfig(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Cookie(_) => StatusCode::BAD_REQUEST,
            Self::Migration(_) => StatusCode::BAD_GATEWAY,
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
struct OrganizationIdentityRow {
    organization_id: String,
    organization_name: String,
    role: String,
}

#[derive(Debug, sqlx::FromRow)]
struct OrganizationMembershipRow {
    organization_id: String,
    organization_name: String,
    slug: String,
    role: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl OrganizationMembershipRow {
    fn into_summary(self) -> OrganizationMembershipSummary {
        OrganizationMembershipSummary {
            organization_id: self.organization_id,
            organization_name: self.organization_name,
            slug: self.slug,
            role: parse_role(&self.role),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct OrganizationRecordRow {
    id: String,
    slug: String,
    name: String,
    plan: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    archived_at: Option<DateTime<Utc>>,
}

impl OrganizationRecordRow {
    fn into_record(self) -> OrganizationRecord {
        OrganizationRecord {
            id: self.id,
            slug: self.slug,
            name: self.name,
            plan: self.plan,
            created_at: self.created_at,
            updated_at: self.updated_at,
            archived_at: self.archived_at,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct AccountAuditEventRow {
    id: i64,
    event_type: String,
    actor_subject: String,
    actor_auth_type: String,
    organization_id: Option<String>,
    target_subject: Option<String>,
    target_email: Option<String>,
    metadata: serde_json::Value,
    created_at: DateTime<Utc>,
}

impl AccountAuditEventRow {
    fn into_event(self) -> AccountAuditEvent {
        AccountAuditEvent {
            id: self.id,
            event_type: self.event_type,
            actor_subject: self.actor_subject,
            actor_auth_type: self.actor_auth_type,
            organization_id: self.organization_id,
            target_subject: self.target_subject,
            target_email: self.target_email,
            metadata: self.metadata,
            created_at: self.created_at,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct OrganizationMemberRow {
    organization_id: String,
    subject: String,
    email: String,
    name: Option<String>,
    role: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl OrganizationMemberRow {
    fn into_record(self) -> OrganizationMemberRecord {
        OrganizationMemberRecord {
            organization_id: self.organization_id,
            subject: self.subject,
            email: self.email,
            name: self.name,
            role: parse_role(&self.role),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct OrganizationInvitationRow {
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

impl OrganizationInvitationRow {
    fn into_record(self) -> OrganizationInvitationRecord {
        OrganizationInvitationRecord {
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
struct PendingInvitationRow {
    id: i64,
    organization_id: String,
    organization_name: String,
    email: String,
    role: String,
    expires_at: DateTime<Utc>,
}

impl PendingInvitationRow {
    fn into_summary(self) -> PendingInvitationSummary {
        PendingInvitationSummary {
            id: self.id,
            organization_id: self.organization_id,
            organization_name: self.organization_name,
            email: self.email,
            role: parse_role(&self.role),
            expires_at: self.expires_at,
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

fn normalized_organization_name(name: &str) -> Result<String, AuthError> {
    let name = name.trim();
    if name.is_empty() || name.len() > 120 {
        return Err(AuthError::InvalidInput(
            "organization name is required".to_string(),
        ));
    }
    Ok(name.to_string())
}

fn normalized_slug(slug: &str) -> Result<String, AuthError> {
    let slug = slug_from_name(slug);
    if slug.is_empty() || slug.len() > 80 {
        return Err(AuthError::InvalidInput(
            "organization slug is invalid".to_string(),
        ));
    }
    Ok(slug)
}

fn normalized_provider(provider: &str) -> Result<String, AuthError> {
    let provider = provider.trim().to_ascii_lowercase();
    if provider.is_empty()
        || provider.len() > 40
        || !provider
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-')
    {
        return Err(AuthError::InvalidInput(
            "oauth provider is invalid".to_string(),
        ));
    }
    Ok(provider)
}

fn slug_from_name(name: &str) -> String {
    let mut slug = String::new();
    let mut last_dash = false;
    for ch in name.trim().to_ascii_lowercase().chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            last_dash = false;
        } else if !last_dash && !slug.is_empty() {
            slug.push('-');
            last_dash = true;
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    if slug.is_empty() {
        "organization".to_string()
    } else {
        slug
    }
}

fn personal_organization_name(email: &str) -> String {
    let (local, domain) = email.split_once('@').unwrap_or((email, "personal"));
    let local = local
        .split(['.', '_', '-', '+'])
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let base = if local.is_empty() {
        domain.to_string()
    } else {
        local
    };
    format!("{base}'s organization")
}

fn random_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
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

fn default_scopes(role: AuthRole) -> Vec<String> {
    match role {
        AuthRole::Admin => vec![
            "ingest:write".to_string(),
            "query:read".to_string(),
            "definitions:write".to_string(),
            "api_keys:write".to_string(),
            "facets:write".to_string(),
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

fn parse_role(value: &str) -> AuthRole {
    match value {
        "admin" => AuthRole::Admin,
        "service" => AuthRole::Service,
        "viewer" => AuthRole::Viewer,
        _ => AuthRole::Viewer,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::COOKIE;
    use sqlx::postgres::PgPoolOptions;

    fn run_db_test<F>(test: F)
    where
        F: std::future::Future<Output = ()>,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(test);
    }

    async fn test_store() -> Option<AuthStore> {
        let postgres_url = std::env::var("NANOTRACE_AUTH_TEST_POSTGRES_URL").unwrap_or_else(|_| {
            "postgres://nanotrace:nanotrace@localhost:15432/nanotrace".to_string()
        });
        let pool = match PgPoolOptions::new()
            .max_connections(2)
            .connect(&postgres_url)
            .await
        {
            Ok(pool) => pool,
            Err(err) => {
                eprintln!("skipping auth DB test; Postgres unavailable: {err}");
                return None;
            }
        };
        let store = AuthStore {
            cfg: AuthConfig {
                postgres_url: Some(postgres_url),
                public_base_url: Some("http://localhost:18473".to_string()),
                api_key_cache_refresh_interval: Duration::from_secs(60),
                session_cookie_name: "nanotrace_session".to_string(),
                session_same_site: "Lax".to_string(),
                session_ttl: Duration::from_secs(3600),
                session_secure: false,
                magic_link_ttl: Duration::from_secs(600),
            },
            pool,
            api_key_cache: Arc::new(RwLock::new(ApiKeyCache::default())),
        };
        store.run_migrations().await.expect("auth migrations");
        Some(store)
    }

    fn cookie_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!("nanotrace_session={token}")).expect("cookie header"),
        );
        headers
    }

    fn login_token(login: &LoginStart) -> String {
        login
            .login_url
            .rsplit_once("token=")
            .map(|(_, token)| token.to_string())
            .expect("login token")
    }

    async fn create_test_user(store: &AuthStore, email: &str, role: AuthRole) -> String {
        let email = normalize_email(email).expect("test email");
        let subject = format!("email:{email}");
        store
            .upsert_user(&subject, &email, Some("Test User"), role)
            .await
            .expect("upsert user");
        subject
    }

    #[test]
    fn first_login_creates_personal_org_and_admin_membership() {
        run_db_test(async {
            let Some(store) = test_store().await else {
                return;
            };
            let email = format!("first-{}@example.com", random_token());
            let login = store
                .start_login(&email, Some("/"))
                .await
                .expect("start login");
            let complete = store
                .complete_login(&login_token(&login), None)
                .await
                .expect("complete login");
            let cookie = complete
                .session_cookie
                .split(';')
                .next()
                .and_then(|value| value.split_once('='))
                .map(|(_, token)| token.to_string())
                .expect("session token");
            let identity = store
                .validate_session(&cookie_headers(&cookie))
                .await
                .expect("validate session");
            assert!(!identity.organization_id.is_empty());
            assert_eq!(identity.role, AuthRole::Admin);
            assert_eq!(identity.organizations.as_ref().map(Vec::len), Some(1));
        });
    }

    #[test]
    fn oauth_state_is_consumed_once() {
        run_db_test(async {
            let Some(store) = test_store().await else {
                return;
            };
            let start = store
                .start_oauth_login("google", Some("/settings/api-keys"))
                .await
                .expect("start oauth login");
            assert_eq!(start.provider, "google");
            assert_eq!(start.return_to, "/settings/api-keys");
            let return_to = store
                .consume_oauth_state("google", &start.state)
                .await
                .expect("consume oauth state");
            assert_eq!(return_to, "/settings/api-keys");
            let second = store
                .consume_oauth_state("google", &start.state)
                .await
                .expect_err("oauth state should be one-time");
            assert!(matches!(second, AuthError::InvalidLoginToken));
        });
    }

    #[test]
    fn external_login_uses_same_session_and_onboarding_flow() {
        run_db_test(async {
            let Some(store) = test_store().await else {
                return;
            };
            let email = format!("google-{}@example.com", random_token()).to_ascii_lowercase();
            let complete = store
                .complete_external_login(
                    &email,
                    Some("Google User"),
                    "/schema",
                    None,
                    "google_oauth",
                )
                .await
                .expect("complete external login");
            assert_eq!(complete.return_to, "/schema");
            assert!(complete.created_organization_id.is_some());
            let cookie = complete
                .session_cookie
                .split(';')
                .next()
                .and_then(|value| value.split_once('='))
                .map(|(_, token)| token.to_string())
                .expect("session token");
            let identity = store
                .validate_session(&cookie_headers(&cookie))
                .await
                .expect("validate session");
            assert_eq!(identity.email.as_deref(), Some(email.as_str()));
            assert_eq!(identity.name.as_deref(), Some("Google User"));
            assert_eq!(identity.role, AuthRole::Admin);
        });
    }

    #[test]
    fn existing_member_login_preserves_valid_previous_active_org() {
        run_db_test(async {
            let Some(store) = test_store().await else {
                return;
            };
            let email = format!("existing-{}@example.com", random_token());
            let subject = create_test_user(&store, &email, AuthRole::Viewer).await;
            let org_a = store
                .create_organization_for_subject(&subject, format!("A {}", random_token()), None)
                .await
                .expect("org a");
            let org_b = store
                .create_organization_for_subject(&subject, format!("B {}", random_token()), None)
                .await
                .expect("org b");
            let old_token = store
                .create_session(
                    &subject,
                    &email,
                    Some("Test User"),
                    AuthRole::Viewer,
                    Some(&org_b.id),
                )
                .await
                .expect("old session");
            let login = store
                .start_login(&email, Some("/"))
                .await
                .expect("start login");
            let complete = store
                .complete_login(&login_token(&login), Some(&cookie_headers(&old_token)))
                .await
                .expect("complete login");
            let new_token = complete
                .session_cookie
                .split(';')
                .next()
                .and_then(|value| value.split_once('='))
                .map(|(_, token)| token.to_string())
                .expect("session token");
            let identity = store
                .validate_session(&cookie_headers(&new_token))
                .await
                .expect("validate session");
            assert_eq!(
                identity.organization_id, org_b.id,
                "expected previous active org; org_a={} org_b={}",
                org_a.id, org_b.id
            );
            assert_ne!(identity.organization_id, org_a.id);
        });
    }

    #[test]
    fn session_validation_uses_membership_role_not_user_role() {
        run_db_test(async {
            let Some(store) = test_store().await else {
                return;
            };
            let email = format!("role-{}@example.com", random_token());
            let subject = create_test_user(&store, &email, AuthRole::Admin).await;
            let org = store
                .create_organization_for_subject(&subject, format!("Role {}", random_token()), None)
                .await
                .expect("org");
            sqlx::query(
                "UPDATE nanotrace_organization_members
                 SET role = 'viewer', updated_at = now()
                 WHERE organization_id = $1 AND subject = $2",
            )
            .bind(&org.id)
            .bind(&subject)
            .execute(&store.pool)
            .await
            .expect("set viewer");
            let token = store
                .create_session(&subject, &email, None, AuthRole::Admin, Some(&org.id))
                .await
                .expect("session");
            let identity = store
                .validate_session(&cookie_headers(&token))
                .await
                .expect("validate session");
            assert_eq!(identity.role, AuthRole::Viewer);
            assert_eq!(identity.scopes, default_scopes(AuthRole::Viewer));
        });
    }

    #[test]
    fn switching_org_fails_without_membership() {
        run_db_test(async {
            let Some(store) = test_store().await else {
                return;
            };
            let email_a = format!("switch-a-{}@example.com", random_token());
            let email_b = format!("switch-b-{}@example.com", random_token());
            let subject_a = create_test_user(&store, &email_a, AuthRole::Viewer).await;
            let subject_b = create_test_user(&store, &email_b, AuthRole::Viewer).await;
            let org_a = store
                .create_organization_for_subject(
                    &subject_a,
                    format!("Switch A {}", random_token()),
                    None,
                )
                .await
                .expect("org a");
            let org_b = store
                .create_organization_for_subject(
                    &subject_b,
                    format!("Switch B {}", random_token()),
                    None,
                )
                .await
                .expect("org b");
            let token = store
                .create_session(
                    &subject_a,
                    &email_a,
                    None,
                    AuthRole::Viewer,
                    Some(&org_a.id),
                )
                .await
                .expect("session");
            let err = store
                .switch_session_organization(&cookie_headers(&token), &org_b.id)
                .await
                .expect_err("switch should fail");
            assert!(matches!(err, AuthError::Forbidden));
        });
    }

    #[test]
    fn last_admin_cannot_be_demoted_or_removed() {
        run_db_test(async {
            let Some(store) = test_store().await else {
                return;
            };
            let email = format!("admin-{}@example.com", random_token());
            let subject = create_test_user(&store, &email, AuthRole::Viewer).await;
            let org = store
                .create_organization_for_subject(
                    &subject,
                    format!("Admin {}", random_token()),
                    None,
                )
                .await
                .expect("org");
            let demote = store
                .set_organization_member_role(&org.id, &subject, AuthRole::Viewer)
                .await
                .expect_err("last admin demotion should fail");
            assert!(matches!(demote, AuthError::InvalidInput(_)));
            let remove = store
                .remove_organization_member(&org.id, &subject)
                .await
                .expect_err("last admin removal should fail");
            assert!(matches!(remove, AuthError::InvalidInput(_)));
        });
    }

    #[test]
    fn invite_token_accepts_once_and_only_for_matching_email() {
        run_db_test(async {
            let Some(store) = test_store().await else {
                return;
            };
            let inviter_email = format!("inviter-{}@example.com", random_token());
            let inviter = create_test_user(&store, &inviter_email, AuthRole::Viewer).await;
            let org = store
                .create_organization_for_subject(
                    &inviter,
                    format!("Invite {}", random_token()),
                    None,
                )
                .await
                .expect("org");
            let invitee_email = format!("invitee-{}@example.com", random_token());
            let invitee = create_test_user(&store, &invitee_email, AuthRole::Viewer).await;
            let wrong_email = format!("wrong-{}@example.com", random_token());
            let wrong_subject = create_test_user(&store, &wrong_email, AuthRole::Viewer).await;
            let wrong_token = store
                .create_session(&wrong_subject, &wrong_email, None, AuthRole::Viewer, None)
                .await
                .expect("wrong session");
            let wrong_invite = store
                .create_organization_invitation(&org.id, &invitee_email, AuthRole::Viewer, &inviter)
                .await
                .expect("wrong invite");
            let wrong = store
                .accept_organization_invitation(
                    &cookie_headers(&wrong_token),
                    wrong_invite.token.as_deref().expect("wrong invite token"),
                )
                .await
                .expect_err("wrong email should fail");
            assert!(matches!(wrong, AuthError::Forbidden));
            store
                .revoke_organization_invitation(&org.id, wrong_invite.invitation.id)
                .await
                .expect("revoke wrong-email probe invite");

            let invite = store
                .create_organization_invitation(&org.id, &invitee_email, AuthRole::Admin, &inviter)
                .await
                .expect("invite");
            let invitee_token = store
                .create_session(&invitee, &invitee_email, None, AuthRole::Viewer, None)
                .await
                .expect("invitee session");
            let membership = store
                .accept_organization_invitation(
                    &cookie_headers(&invitee_token),
                    invite.token.as_deref().expect("invite token"),
                )
                .await
                .expect("accept invite");
            assert_eq!(membership.organization_id, org.id);
            assert_eq!(membership.role, AuthRole::Admin);
            let second = store
                .accept_organization_invitation(
                    &cookie_headers(&invitee_token),
                    invite.token.as_deref().expect("invite token"),
                )
                .await
                .expect_err("second accept should fail");
            assert!(matches!(second, AuthError::Forbidden));
        });
    }

    #[test]
    fn duplicate_pending_invite_returns_existing_pending_invite() {
        run_db_test(async {
            let Some(store) = test_store().await else {
                return;
            };
            let inviter_email = format!("resend-inviter-{}@example.com", random_token());
            let inviter = create_test_user(&store, &inviter_email, AuthRole::Viewer).await;
            let org = store
                .create_organization_for_subject(
                    &inviter,
                    format!("Resend {}", random_token()),
                    None,
                )
                .await
                .expect("org");
            let invitee_email = format!("resend-invitee-{}@example.com", random_token());
            let invitee = create_test_user(&store, &invitee_email, AuthRole::Viewer).await;
            let first = store
                .create_organization_invitation(&org.id, &invitee_email, AuthRole::Viewer, &inviter)
                .await
                .expect("first invite");
            let second = store
                .create_organization_invitation(&org.id, &invitee_email, AuthRole::Admin, &inviter)
                .await
                .expect("second invite");
            assert!(first.token.is_some());
            assert!(second.token.is_none());
            assert_eq!(first.invitation.id, second.invitation.id);
            let invitations = store
                .list_organization_invitations(&org.id)
                .await
                .expect("list invitations");
            let active_record = invitations
                .iter()
                .find(|invite| invite.id == first.invitation.id)
                .expect("active invitation record");
            assert!(active_record.revoked_at.is_none());
            assert_eq!(active_record.role, AuthRole::Viewer);

            let invitee_token = store
                .create_session(&invitee, &invitee_email, None, AuthRole::Viewer, None)
                .await
                .expect("invitee session");
            let membership = store
                .accept_organization_invitation(
                    &cookie_headers(&invitee_token),
                    first.token.as_deref().expect("first token"),
                )
                .await
                .expect("accept original invite");
            assert_eq!(membership.organization_id, org.id);
            assert_eq!(membership.role, AuthRole::Viewer);
        });
    }

    #[test]
    fn organization_update_and_archive_disable_runtime_access() {
        run_db_test(async {
            let Some(store) = test_store().await else {
                return;
            };
            let email = format!("archive-{}@example.com", random_token());
            let subject = create_test_user(&store, &email, AuthRole::Viewer).await;
            let org = store
                .create_organization_for_subject(
                    &subject,
                    format!("Archive {}", random_token()),
                    None,
                )
                .await
                .expect("org");
            let updated = store
                .update_organization(
                    &org.id,
                    Some("Updated Archive Org".to_string()),
                    Some(&format!("updated-archive-{}", random_token())),
                )
                .await
                .expect("update org");
            assert_eq!(updated.name, "Updated Archive Org");
            assert!(updated.slug.starts_with("updated-archive-"));

            let created_key = store
                .create_api_key(&org.id, "archive-key", AuthRole::Admin, &[], &subject, None)
                .await
                .expect("api key");
            let token = store
                .create_session(&subject, &email, None, AuthRole::Viewer, Some(&org.id))
                .await
                .expect("session");
            let archived = store.archive_organization(&org.id).await.expect("archive");
            assert!(archived.archived_at.is_some());

            let identity = store
                .validate_session(&cookie_headers(&token))
                .await
                .expect("validate archived session");
            assert_eq!(identity.organization_id, "");
            assert!(identity.scopes.is_empty());
            assert!(
                store
                    .validate_api_key(&HeaderMap::from_iter([(
                        AUTHORIZATION,
                        HeaderValue::from_str(&format!("Bearer {}", created_key.key))
                            .expect("auth header"),
                    )]))
                    .await
                    .is_err()
            );
            let create_after_archive = store
                .create_api_key(
                    &org.id,
                    "after-archive",
                    AuthRole::Admin,
                    &[],
                    &subject,
                    None,
                )
                .await
                .expect_err("archived org should reject new api keys");
            assert!(matches!(create_after_archive, AuthError::NotFound));
        });
    }

    #[test]
    fn revoked_expired_and_resent_invites_have_deterministic_lifecycle() {
        run_db_test(async {
            let Some(store) = test_store().await else {
                return;
            };
            let inviter_email = format!("lifecycle-inviter-{}@example.com", random_token());
            let inviter = create_test_user(&store, &inviter_email, AuthRole::Viewer).await;
            let org = store
                .create_organization_for_subject(
                    &inviter,
                    format!("Invite Lifecycle {}", random_token()),
                    None,
                )
                .await
                .expect("org");
            let invitee_email = format!("lifecycle-invitee-{}@example.com", random_token());
            let invitee = create_test_user(&store, &invitee_email, AuthRole::Viewer).await;
            let invitee_token = store
                .create_session(&invitee, &invitee_email, None, AuthRole::Viewer, None)
                .await
                .expect("invitee session");

            let revoked = store
                .create_organization_invitation(&org.id, &invitee_email, AuthRole::Viewer, &inviter)
                .await
                .expect("revoked invite");
            store
                .revoke_organization_invitation(&org.id, revoked.invitation.id)
                .await
                .expect("revoke invite");
            let revoked_accept = store
                .accept_organization_invitation(
                    &cookie_headers(&invitee_token),
                    revoked.token.as_deref().expect("revoked token"),
                )
                .await
                .expect_err("revoked invite should fail");
            assert!(matches!(revoked_accept, AuthError::Forbidden));

            let expired = store
                .create_organization_invitation(&org.id, &invitee_email, AuthRole::Viewer, &inviter)
                .await
                .expect("expired invite");
            sqlx::query(
                "UPDATE nanotrace_organization_invitations
                 SET expires_at = now() - interval '1 second'
                 WHERE id = $1",
            )
            .bind(expired.invitation.id)
            .execute(&store.pool)
            .await
            .expect("expire invite");
            let expired_accept = store
                .accept_organization_invitation(
                    &cookie_headers(&invitee_token),
                    expired.token.as_deref().expect("expired token"),
                )
                .await
                .expect_err("expired invite should fail");
            assert!(matches!(expired_accept, AuthError::Forbidden));
            let cleaned = store
                .cleanup_expired_invitations()
                .await
                .expect("cleanup expired invitations");
            let (revoked_at,) = sqlx::query_as::<_, (Option<DateTime<Utc>>,)>(
                "SELECT revoked_at
                 FROM nanotrace_organization_invitations
                 WHERE id = $1",
            )
            .bind(expired.invitation.id)
            .fetch_one(&store.pool)
            .await
            .expect("expired invite row");
            assert!(
                cleaned >= 1 || revoked_at.is_some(),
                "expired invite should be cleaned or already revoked"
            );

            let resend = store
                .create_organization_invitation(&org.id, &invitee_email, AuthRole::Admin, &inviter)
                .await
                .expect("resend invite");
            let resent = store
                .resend_organization_invitation(&org.id, resend.invitation.id)
                .await
                .expect("resend active invite");
            let old = store
                .accept_organization_invitation(
                    &cookie_headers(&invitee_token),
                    resend.token.as_deref().expect("resend token"),
                )
                .await
                .expect_err("superseded resend token should fail");
            assert!(matches!(old, AuthError::Forbidden));
            let membership = store
                .accept_organization_invitation(
                    &cookie_headers(&invitee_token),
                    resent.token.as_deref().expect("resent token"),
                )
                .await
                .expect("accept resent invite");
            assert_eq!(membership.organization_id, org.id);
            assert_eq!(membership.role, AuthRole::Admin);
        });
    }

    #[test]
    fn account_audit_events_are_persisted() {
        run_db_test(async {
            let Some(store) = test_store().await else {
                return;
            };
            let actor = format!("email:audit-{}@example.com", random_token());
            let org_id = format!("org_{}", random_token());
            sqlx::query(
                "INSERT INTO nanotrace_organizations (id, slug, name)
                 VALUES ($1, $2, 'Audit Org')
                 ON CONFLICT (id) DO NOTHING",
            )
            .bind(&org_id)
            .bind(
                format!("audit-{}", random_token())
                    .chars()
                    .take(40)
                    .collect::<String>(),
            )
            .execute(&store.pool)
            .await
            .expect("insert audit org");
            let event = store
                .record_account_audit_event(
                    "organization.test",
                    &actor,
                    "session",
                    Some(&org_id),
                    Some("email:target@example.com"),
                    Some("target@example.com"),
                    serde_json::json!({ "ok": true }),
                )
                .await
                .expect("record audit");
            assert_eq!(event.event_type, "organization.test");
            assert_eq!(event.actor_subject, actor);
            assert_eq!(event.organization_id.as_deref(), Some(org_id.as_str()));
            assert_eq!(event.metadata["ok"], true);
        });
    }
}
