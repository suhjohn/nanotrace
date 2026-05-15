use std::{env, time::Duration};

use nanotrace_auth::AuthConfig;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub s3_bucket: Option<String>,
    pub clickhouse_url: Option<String>,
    pub clickhouse_user: Option<String>,
    pub clickhouse_password: Option<String>,
    pub clickhouse_database: String,
    pub clickhouse_table: String,
    pub clickhouse_facets_table: String,
    pub clickhouse_event_index_table: String,
    pub clickhouse_hot_dimensions_table: String,
    pub clickhouse_max_result_rows: u64,
    pub clickhouse_max_execution_secs: u64,
    pub clickhouse_max_bytes_to_read: u64,
    pub max_request_bytes: usize,
    pub request_timeout: Duration,
    pub auth: AuthConfig,
    pub cors_allowed_origins: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{key} must be a valid {kind}: {value}")]
    Invalid {
        key: &'static str,
        kind: &'static str,
        value: String,
    },
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let clickhouse_database = env::var("CLICKHOUSE_DATABASE")
            .unwrap_or_else(|_| "observatory".to_string())
            .trim()
            .to_string();
        let clickhouse_table = env::var("CLICKHOUSE_TABLE")
            .unwrap_or_else(|_| "events".to_string())
            .trim()
            .to_string();
        let clickhouse_facets_table = env::var("CLICKHOUSE_FACETS_TABLE")
            .unwrap_or_else(|_| "event_facets".to_string())
            .trim()
            .to_string();
        let clickhouse_event_index_table = env::var("CLICKHOUSE_EVENT_INDEX_TABLE")
            .unwrap_or_else(|_| "event_facet_index".to_string())
            .trim()
            .to_string();
        let clickhouse_hot_dimensions_table = env::var("CLICKHOUSE_HOT_DIMENSIONS_TABLE")
            .unwrap_or_else(|_| "hot_dimensions".to_string())
            .trim()
            .to_string();
        ensure_identifier("CLICKHOUSE_DATABASE", &clickhouse_database)?;
        ensure_identifier("CLICKHOUSE_TABLE", &clickhouse_table)?;
        ensure_identifier("CLICKHOUSE_FACETS_TABLE", &clickhouse_facets_table)?;
        ensure_identifier(
            "CLICKHOUSE_EVENT_INDEX_TABLE",
            &clickhouse_event_index_table,
        )?;
        ensure_identifier(
            "CLICKHOUSE_HOT_DIMENSIONS_TABLE",
            &clickhouse_hot_dimensions_table,
        )?;

        let clickhouse_max_result_rows = parse_env("CLICKHOUSE_MAX_RESULT_ROWS", 100_000)?;
        let clickhouse_max_execution_secs = parse_env("CLICKHOUSE_MAX_EXECUTION_SECS", 30)?;
        let clickhouse_max_bytes_to_read =
            parse_env("CLICKHOUSE_MAX_BYTES_TO_READ", 1_000_000_000)?;
        ensure_nonzero("CLICKHOUSE_MAX_RESULT_ROWS", clickhouse_max_result_rows)?;
        ensure_nonzero(
            "CLICKHOUSE_MAX_EXECUTION_SECS",
            clickhouse_max_execution_secs,
        )?;
        ensure_nonzero("CLICKHOUSE_MAX_BYTES_TO_READ", clickhouse_max_bytes_to_read)?;

        let request_timeout_secs = parse_env("QUERY_REQUEST_TIMEOUT_SECS", 60)?;
        ensure_nonzero("QUERY_REQUEST_TIMEOUT_SECS", request_timeout_secs)?;
        let public_base_url = optional_string("NANOTRACE_PUBLIC_BASE_URL");
        let session_secure = optional_bool_env("NANOTRACE_SESSION_SECURE")?.unwrap_or_else(|| {
            public_base_url
                .as_deref()
                .is_some_and(|url| url.starts_with("https://"))
        });
        let session_ttl_secs: u64 = parse_env("NANOTRACE_SESSION_TTL_SECS", 7 * 24 * 60 * 60)?;
        ensure_nonzero("NANOTRACE_SESSION_TTL_SECS", session_ttl_secs)?;
        let magic_link_ttl_secs: u64 = parse_env("NANOTRACE_MAGIC_LINK_TTL_SECS", 10 * 60)?;
        ensure_nonzero("NANOTRACE_MAGIC_LINK_TTL_SECS", magic_link_ttl_secs)?;
        let auth = AuthConfig {
            postgres_url: optional_string("NANOTRACE_POSTGRES_URL"),
            bootstrap_api_key: optional_string("NANOTRACE_DEV_BOOTSTRAP_API_KEY"),
            public_base_url,
            session_cookie_name: env::var("NANOTRACE_SESSION_COOKIE")
                .unwrap_or_else(|_| "nanotrace_session".to_string())
                .trim()
                .to_string(),
            session_same_site: session_same_site_env()?,
            session_ttl: Duration::from_secs(session_ttl_secs),
            session_secure,
            magic_link_ttl: Duration::from_secs(magic_link_ttl_secs),
            allowed_emails: parse_list_env("NANOTRACE_ALLOWED_EMAILS"),
            admin_emails: parse_list_env("NANOTRACE_ADMIN_EMAILS"),
        };

        Ok(Self {
            port: parse_env("PORT", 18_473)?,
            s3_bucket: optional_string("NANOTRACE_S3_BUCKET")
                .or_else(|| optional_string("S3_BUCKET")),
            clickhouse_url: optional_string("CLICKHOUSE_URL"),
            clickhouse_user: optional_string("CLICKHOUSE_USER")
                .or_else(|| optional_string("CLICKHOUSE_USERNAME")),
            clickhouse_password: optional_string("CLICKHOUSE_PASSWORD"),
            clickhouse_database,
            clickhouse_table,
            clickhouse_facets_table,
            clickhouse_event_index_table,
            clickhouse_hot_dimensions_table,
            clickhouse_max_result_rows,
            clickhouse_max_execution_secs,
            clickhouse_max_bytes_to_read,
            max_request_bytes: parse_env("MAX_REQUEST_BYTES", 16 * 1024 * 1024)?,
            request_timeout: Duration::from_secs(request_timeout_secs),
            auth,
            cors_allowed_origins: parse_list_env("NANOTRACE_CORS_ALLOWED_ORIGINS"),
        })
    }
}

fn optional_string(key: &'static str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn session_same_site_env() -> Result<String, ConfigError> {
    let value = env::var("NANOTRACE_SESSION_SAME_SITE")
        .unwrap_or_else(|_| "Lax".to_string())
        .trim()
        .to_ascii_lowercase();
    match value.as_str() {
        "strict" => Ok("Strict".to_string()),
        "lax" => Ok("Lax".to_string()),
        "none" => Ok("None".to_string()),
        _ => Err(ConfigError::Invalid {
            key: "NANOTRACE_SESSION_SAME_SITE",
            kind: "SameSite value (Strict, Lax, or None)",
            value,
        }),
    }
}

fn parse_env<T>(key: &'static str, default: T) -> Result<T, ConfigError>
where
    T: std::str::FromStr,
{
    match env::var(key) {
        Ok(value) => value.parse().map_err(|_| ConfigError::Invalid {
            key,
            kind: std::any::type_name::<T>(),
            value,
        }),
        Err(_) => Ok(default),
    }
}

fn optional_bool_env(key: &'static str) -> Result<Option<bool>, ConfigError> {
    match env::var(key) {
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(Some(true)),
            "0" | "false" | "no" | "off" => Ok(Some(false)),
            _ => Err(ConfigError::Invalid {
                key,
                kind: "bool",
                value,
            }),
        },
        Err(_) => Ok(None),
    }
}

fn parse_list_env(key: &'static str) -> Vec<String> {
    env::var(key)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn ensure_nonzero<T>(key: &'static str, value: T) -> Result<(), ConfigError>
where
    T: Copy + PartialEq + From<u8> + ToString,
{
    if value == T::from(0) {
        Err(ConfigError::Invalid {
            key,
            kind: "non-zero value",
            value: value.to_string(),
        })
    } else {
        Ok(())
    }
}

fn ensure_identifier(key: &'static str, value: &str) -> Result<(), ConfigError> {
    let valid = !value.is_empty()
        && value.chars().enumerate().all(|(index, ch)| {
            ch == '_' || ch.is_ascii_alphanumeric() && (index > 0 || ch.is_ascii_alphabetic())
        });
    if valid {
        Ok(())
    } else {
        Err(ConfigError::Invalid {
            key,
            kind: "ClickHouse identifier",
            value: value.to_string(),
        })
    }
}
