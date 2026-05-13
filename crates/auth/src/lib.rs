use std::time::Duration;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use http::{
    HeaderMap, HeaderValue, StatusCode,
    header::{AUTHORIZATION, COOKIE, SET_COOKIE},
};
use rand::RngCore;
use regex::Regex;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, postgres::PgPoolOptions};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub database_url: Option<String>,
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
        self.database_url.is_some()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthIdentity {
    pub auth_type: AuthType,
    pub subject: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub role: AuthRole,
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
    pub name: String,
    pub prefix: String,
    pub role: AuthRole,
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

#[derive(Clone)]
pub struct AuthStore {
    cfg: AuthConfig,
    pool: PgPool,
}

impl AuthStore {
    pub async fn connect(cfg: AuthConfig) -> Result<Option<Self>, AuthError> {
        let Some(database_url) = cfg.database_url.clone() else {
            return Ok(None);
        };
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(&database_url)
            .await?;
        let store = Self { cfg, pool };
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
        Ok(AuthIdentity {
            auth_type: AuthType::Session,
            subject,
            email: Some(email),
            name,
            role: parse_role(&role),
        })
    }

    pub async fn validate_api_key(&self, headers: &HeaderMap) -> Result<AuthIdentity, AuthError> {
        let token = read_api_key(headers).ok_or(AuthError::Unauthorized)?;
        let key_hash = token_hash(&token);
        let row = sqlx::query_as::<_, (String, String)>(
            "UPDATE nanotrace_api_keys
             SET last_used_at = now()
             WHERE key_hash = $1
               AND revoked_at IS NULL
               AND (expires_at IS NULL OR expires_at > now())
             RETURNING name, role",
        )
        .bind(key_hash)
        .fetch_optional(&self.pool)
        .await?;
        let Some((name, role)) = row else {
            return Err(AuthError::Unauthorized);
        };
        Ok(AuthIdentity {
            auth_type: AuthType::ApiKey,
            subject: format!("api_key:{name}"),
            email: None,
            name: Some(name),
            role: parse_role(&role),
        })
    }

    pub async fn create_api_key(
        &self,
        name: &str,
        role: AuthRole,
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
        let row = sqlx::query_as::<_, ApiKeyRow>(
            "INSERT INTO nanotrace_api_keys (key_hash, prefix, name, role, created_by, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6)
             RETURNING id, name, prefix, role, created_by, created_at, last_used_at, expires_at, revoked_at",
        )
        .bind(key_hash)
        .bind(prefix)
        .bind(name)
        .bind(role_name(role))
        .bind(created_by)
        .bind(expires_at)
        .fetch_one(&self.pool)
        .await?;
        Ok(CreatedApiKey {
            key,
            api_key: row.into_record(),
        })
    }

    pub async fn list_api_keys(&self) -> Result<Vec<ApiKeyRecord>, AuthError> {
        let rows = sqlx::query_as::<_, ApiKeyRow>(
            "SELECT id, name, prefix, role, created_by, created_at, last_used_at, expires_at, revoked_at
             FROM nanotrace_api_keys
             ORDER BY created_at DESC, id DESC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(ApiKeyRow::into_record).collect())
    }

    pub async fn revoke_api_key(&self, id: i64) -> Result<ApiKeyRecord, AuthError> {
        let row = sqlx::query_as::<_, ApiKeyRow>(
            "UPDATE nanotrace_api_keys
             SET revoked_at = COALESCE(revoked_at, now())
             WHERE id = $1
             RETURNING id, name, prefix, role, created_by, created_at, last_used_at, expires_at, revoked_at",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(ApiKeyRow::into_record).ok_or(AuthError::NotFound)
    }

    async fn ensure_schema(&self) -> Result<(), AuthError> {
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
                key_hash text NOT NULL UNIQUE,
                prefix text NOT NULL,
                name text NOT NULL,
                role text NOT NULL DEFAULT 'service',
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
            "INSERT INTO nanotrace_api_keys (key_hash, prefix, name, role, created_by)
             VALUES ($1, $2, 'bootstrap', 'admin', 'pulumi')
             ON CONFLICT (key_hash)
             DO UPDATE SET revoked_at = NULL, role = 'admin'",
        )
        .bind(key_hash)
        .bind(prefix)
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
            Self::Unauthorized | Self::InvalidLoginToken => StatusCode::UNAUTHORIZED,
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
    name: String,
    prefix: String,
    role: String,
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
            name: self.name,
            prefix: self.prefix,
            role: parse_role(&self.role),
            created_by: self.created_by,
            created_at: self.created_at,
            last_used_at: self.last_used_at,
            expires_at: self.expires_at,
            revoked_at: self.revoked_at,
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
