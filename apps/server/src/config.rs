use std::{env, path::PathBuf, time::Duration};

use nanotrace_auth::AuthConfig;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub data_dir: PathBuf,
    pub ui_dir: Option<PathBuf>,
    pub s3_bucket: Option<String>,
    pub s3_prefix: String,
    pub clickhouse_url: Option<String>,
    pub clickhouse_user: Option<String>,
    pub clickhouse_password: Option<String>,
    pub clickhouse_database: String,
    pub clickhouse_table: String,
    pub clickhouse_max_result_rows: u64,
    pub clickhouse_max_execution_secs: u64,
    pub clickhouse_max_bytes_to_read: u64,
    pub max_request_bytes: usize,
    pub max_event_bytes: usize,
    pub rotate_bytes: u64,
    pub rotate_after: Duration,
    pub upload_poll_interval: Duration,
    pub done_retention: Option<Duration>,
    pub done_cleanup_interval: Duration,
    pub writer_lanes: usize,
    pub writer_queue_capacity: usize,
    pub writer_flush_interval: Duration,
    pub writer_flush_bytes: u64,
    pub compact_batch_receipts: bool,
    pub processor_poll_interval: Duration,
    pub processor_builder_cmd: String,
    pub processor_prefix: String,
    pub auth: AuthConfig,
    pub email_from: Option<String>,
    pub cors_allowed_origins: Vec<String>,
    pub app_base_url: Option<String>,
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
        let port = parse_env("PORT", 18_473)?;
        let data_dir = PathBuf::from(
            env::var("NANOTRACE_DATA_DIR").unwrap_or_else(|_| "/data/events".to_string()),
        );
        let ui_dir = optional_string("NANOTRACE_UI_DIR").map(PathBuf::from);
        let s3_bucket = env::var("NANOTRACE_S3_BUCKET")
            .or_else(|_| env::var("S3_BUCKET"))
            .ok()
            .filter(|value| !value.trim().is_empty());
        let s3_prefix = env::var("S3_PREFIX")
            .or_else(|_| env::var("NANOTRACE_OBJECT_PREFIX"))
            .unwrap_or_else(|_| "events".to_string())
            .trim_matches('/')
            .to_string();
        let clickhouse_url = optional_string("CLICKHOUSE_URL");
        let clickhouse_user =
            optional_string("CLICKHOUSE_USER").or_else(|| optional_string("CLICKHOUSE_USERNAME"));
        let clickhouse_password = optional_string("CLICKHOUSE_PASSWORD");
        let clickhouse_database = env::var("CLICKHOUSE_DATABASE")
            .unwrap_or_else(|_| "observatory".to_string())
            .trim()
            .to_string();
        let clickhouse_table = env::var("CLICKHOUSE_TABLE")
            .unwrap_or_else(|_| "events".to_string())
            .trim()
            .to_string();
        let clickhouse_max_result_rows = parse_env("CLICKHOUSE_MAX_RESULT_ROWS", 100_000)?;
        let clickhouse_max_execution_secs = parse_env("CLICKHOUSE_MAX_EXECUTION_SECS", 30)?;
        let clickhouse_max_bytes_to_read =
            parse_env("CLICKHOUSE_MAX_BYTES_TO_READ", 1_000_000_000)?;
        let max_request_bytes = parse_env("MAX_REQUEST_BYTES", 209_715_200)?;
        let max_event_bytes = parse_env("MAX_EVENT_BYTES", max_request_bytes)?;
        let rotate_bytes = parse_env("NANOTRACE_PART_MAX_BYTES", 64 * 1024 * 1024)?;
        let rotate_after_secs = parse_env("NANOTRACE_PART_MAX_AGE_SECS", 1)?;
        let upload_poll_ms = parse_env("UPLOAD_POLL_INTERVAL_MS", 500)?;
        let done_retention_mins = parse_env("NANOTRACE_DONE_RETENTION_MINS", 60)?;
        let done_cleanup_interval_secs = parse_env("NANOTRACE_DONE_CLEANUP_INTERVAL_SECS", 60)?;
        let writer_lanes: usize = parse_env("NANOTRACE_WRITER_LANES", 4)?;
        let writer_queue_capacity: usize = parse_env("NANOTRACE_WRITER_QUEUE_CAPACITY", 8192)?;
        let writer_flush_interval_ms: u64 = parse_env("NANOTRACE_WRITER_FLUSH_INTERVAL_MS", 10)?;
        let writer_flush_bytes: u64 = parse_env("NANOTRACE_WRITER_FLUSH_BYTES", 1024 * 1024)?;
        let compact_batch_receipts = parse_bool_env("NANOTRACE_COMPACT_BATCH_RECEIPTS", false)?;
        let processor_poll_interval_secs: u64 = parse_env("PROCESSOR_POLL_INTERVAL_SECS", 30)?;
        let processor_builder_cmd = env::var("PROCESSOR_BUILDER_CMD")
            .unwrap_or_else(|_| "python3 /usr/local/bin/modal_processor_builder.py".to_string())
            .trim()
            .to_string();
        let processor_prefix = env::var("PROCESSOR_PREFIX")
            .unwrap_or_else(|_| "processors".to_string())
            .trim()
            .trim_matches('/')
            .to_string();
        let public_base_url = optional_string("NANOTRACE_PUBLIC_BASE_URL");
        let app_base_url = optional_string("NANOTRACE_APP_BASE_URL");
        let session_secure = optional_bool_env("NANOTRACE_SESSION_SECURE")?.unwrap_or_else(|| {
            public_base_url
                .as_deref()
                .is_some_and(|url| url.starts_with("https://"))
        });
        let session_ttl_secs: u64 = parse_env("NANOTRACE_SESSION_TTL_SECS", 7 * 24 * 60 * 60)?;
        let magic_link_ttl_secs: u64 = parse_env("NANOTRACE_MAGIC_LINK_TTL_SECS", 10 * 60)?;
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
        let cors_allowed_origins = parse_list_env("NANOTRACE_CORS_ALLOWED_ORIGINS");
        ensure_nonzero("NANOTRACE_WRITER_LANES", writer_lanes)?;
        ensure_nonzero("NANOTRACE_WRITER_QUEUE_CAPACITY", writer_queue_capacity)?;
        ensure_nonzero(
            "NANOTRACE_WRITER_FLUSH_INTERVAL_MS",
            writer_flush_interval_ms,
        )?;
        ensure_nonzero("NANOTRACE_WRITER_FLUSH_BYTES", writer_flush_bytes)?;
        ensure_nonzero("PROCESSOR_POLL_INTERVAL_SECS", processor_poll_interval_secs)?;
        ensure_nonzero("NANOTRACE_SESSION_TTL_SECS", session_ttl_secs)?;
        ensure_nonzero("NANOTRACE_MAGIC_LINK_TTL_SECS", magic_link_ttl_secs)?;
        ensure_identifier("CLICKHOUSE_DATABASE", &clickhouse_database)?;
        ensure_identifier("CLICKHOUSE_TABLE", &clickhouse_table)?;
        ensure_nonzero("CLICKHOUSE_MAX_RESULT_ROWS", clickhouse_max_result_rows)?;
        ensure_nonzero(
            "CLICKHOUSE_MAX_EXECUTION_SECS",
            clickhouse_max_execution_secs,
        )?;
        ensure_nonzero("CLICKHOUSE_MAX_BYTES_TO_READ", clickhouse_max_bytes_to_read)?;

        Ok(Self {
            port,
            data_dir,
            ui_dir,
            s3_bucket,
            s3_prefix,
            clickhouse_url,
            clickhouse_user,
            clickhouse_password,
            clickhouse_database,
            clickhouse_table,
            clickhouse_max_result_rows,
            clickhouse_max_execution_secs,
            clickhouse_max_bytes_to_read,
            max_request_bytes,
            max_event_bytes,
            rotate_bytes,
            rotate_after: Duration::from_secs(rotate_after_secs),
            upload_poll_interval: Duration::from_millis(upload_poll_ms),
            done_retention: if done_retention_mins == 0 {
                None
            } else {
                Some(Duration::from_secs(done_retention_mins * 60))
            },
            done_cleanup_interval: Duration::from_secs(done_cleanup_interval_secs),
            writer_lanes,
            writer_queue_capacity,
            writer_flush_interval: Duration::from_millis(writer_flush_interval_ms),
            writer_flush_bytes,
            compact_batch_receipts,
            processor_poll_interval: Duration::from_secs(processor_poll_interval_secs),
            processor_builder_cmd,
            processor_prefix,
            auth,
            email_from: optional_string("NANOTRACE_EMAIL_FROM"),
            cors_allowed_origins,
            app_base_url,
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

fn parse_bool_env(key: &'static str, default: bool) -> Result<bool, ConfigError> {
    Ok(optional_bool_env(key)?.unwrap_or(default))
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
