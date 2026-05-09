use std::{env, time::Duration};

use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Config {
    pub secret_key: String,
    pub port: u16,
    pub s3_bucket: Option<String>,
    pub clickhouse_url: Option<String>,
    pub clickhouse_user: Option<String>,
    pub clickhouse_password: Option<String>,
    pub clickhouse_database: String,
    pub clickhouse_table: String,
    pub clickhouse_max_result_rows: u64,
    pub clickhouse_max_execution_secs: u64,
    pub clickhouse_max_bytes_to_read: u64,
    pub max_request_bytes: usize,
    pub request_timeout: Duration,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{0} is required")]
    Missing(&'static str),
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
        ensure_identifier("CLICKHOUSE_DATABASE", &clickhouse_database)?;
        ensure_identifier("CLICKHOUSE_TABLE", &clickhouse_table)?;

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

        Ok(Self {
            secret_key: required("SECRET_KEY")?,
            port: parse_env("PORT", 18_473)?,
            s3_bucket: optional_string("NANOTRACE_S3_BUCKET")
                .or_else(|| optional_string("S3_BUCKET")),
            clickhouse_url: optional_string("CLICKHOUSE_URL"),
            clickhouse_user: optional_string("CLICKHOUSE_USER")
                .or_else(|| optional_string("CLICKHOUSE_USERNAME")),
            clickhouse_password: optional_string("CLICKHOUSE_PASSWORD"),
            clickhouse_database,
            clickhouse_table,
            clickhouse_max_result_rows,
            clickhouse_max_execution_secs,
            clickhouse_max_bytes_to_read,
            max_request_bytes: parse_env("MAX_REQUEST_BYTES", 16 * 1024 * 1024)?,
            request_timeout: Duration::from_secs(request_timeout_secs),
        })
    }
}

fn optional_string(key: &'static str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn required(key: &'static str) -> Result<String, ConfigError> {
    env::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(ConfigError::Missing(key))
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
