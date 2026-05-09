use std::{env, path::PathBuf, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::{Client as SqsClient, types::Message};
use nanotrace_processor_runtime::{ProcessorRuntime, ProcessorSyncConfig};
use serde::Deserialize;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
struct Config {
    sqs_queue_url: String,
    clickhouse_url: String,
    clickhouse_user: Option<String>,
    clickhouse_password: Option<String>,
    clickhouse_database: String,
    clickhouse_table: String,
    poll_wait: u32,
    max_messages: i32,
    visibility_timeout: i32,
    request_timeout: Duration,
    processor_bucket: Option<String>,
    processor_poll_interval: Duration,
    processor_dir: PathBuf,
}

#[derive(Clone)]
struct Loader {
    cfg: Config,
    http: reqwest::Client,
    processors: ProcessorRuntime,
    s3: S3Client,
    sqs: SqsClient,
}

#[derive(Debug, Deserialize)]
struct S3Event {
    #[serde(rename = "Records")]
    records: Vec<S3Record>,
}

#[derive(Debug, Deserialize)]
struct S3Record {
    s3: S3Entity,
}

#[derive(Debug, Deserialize)]
struct S3Entity {
    bucket: S3Bucket,
    object: S3Object,
}

#[derive(Debug, Deserialize)]
struct S3Bucket {
    name: String,
}

#[derive(Debug, Deserialize)]
struct S3Object {
    key: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env()?;
    let aws_config = aws_config::load_from_env().await;
    let s3 = S3Client::new(&aws_config);
    let processors = match cfg.processor_bucket.clone() {
        Some(bucket) => ProcessorRuntime::start(
            s3.clone(),
            ProcessorSyncConfig {
                bucket,
                interval: cfg.processor_poll_interval,
                root: cfg.processor_dir.clone(),
                stage: "loader".to_string(),
            },
        ),
        None => ProcessorRuntime::identity(),
    };
    let loader = Loader {
        cfg,
        http: reqwest::Client::new(),
        processors,
        s3,
        sqs: SqsClient::new(&aws_config),
    };

    info!("nanotrace loader starting");
    loader.run().await
}

impl Config {
    fn from_env() -> Result<Self> {
        let clickhouse_database = env_or("CLICKHOUSE_DATABASE", "observatory");
        let clickhouse_table = env_or("CLICKHOUSE_TABLE", "events");
        validate_identifier("CLICKHOUSE_DATABASE", &clickhouse_database)?;
        validate_identifier("CLICKHOUSE_TABLE", &clickhouse_table)?;

        Ok(Self {
            sqs_queue_url: required("LOADER_SQS_QUEUE_URL")?,
            clickhouse_url: required("CLICKHOUSE_URL")?,
            clickhouse_user: optional("CLICKHOUSE_USER")
                .or_else(|| optional("CLICKHOUSE_USERNAME")),
            clickhouse_password: optional("CLICKHOUSE_PASSWORD"),
            clickhouse_database,
            clickhouse_table,
            poll_wait: parse_env("LOADER_POLL_WAIT_SECS", 20)?,
            max_messages: parse_env("LOADER_MAX_MESSAGES", 10)?,
            visibility_timeout: parse_env("LOADER_VISIBILITY_TIMEOUT_SECS", 300)?,
            request_timeout: Duration::from_secs(parse_env("LOADER_REQUEST_TIMEOUT_SECS", 60)?),
            processor_bucket: optional("PROCESSOR_S3_BUCKET")
                .or_else(|| optional("NANOTRACE_S3_BUCKET"))
                .or_else(|| optional("S3_BUCKET")),
            processor_poll_interval: Duration::from_secs(parse_env(
                "PROCESSOR_POLL_INTERVAL_SECS",
                30,
            )?),
            processor_dir: PathBuf::from(env_or("PROCESSOR_DIR", "/tmp/nanotrace-processors")),
        })
    }

    fn table_name(&self) -> String {
        format!("{}.{}", self.clickhouse_database, self.clickhouse_table)
    }
}

impl Loader {
    async fn run(&self) -> Result<()> {
        loop {
            tokio::select! {
                result = self.poll_once() => {
                    if let Err(err) = result {
                        error!(error = %err, "loader poll failed");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
                _ = shutdown_signal() => {
                    info!("shutdown signal received");
                    return Ok(());
                }
            }
        }
    }

    async fn poll_once(&self) -> Result<()> {
        let output = self
            .sqs
            .receive_message()
            .queue_url(&self.cfg.sqs_queue_url)
            .wait_time_seconds(self.cfg.poll_wait as i32)
            .max_number_of_messages(self.cfg.max_messages)
            .visibility_timeout(self.cfg.visibility_timeout)
            .send()
            .await
            .context("receive SQS messages")?;

        for message in output.messages() {
            if let Err(err) = self.process_message(message).await {
                error!(
                    message_id = message.message_id().unwrap_or_default(),
                    error = %err,
                    "failed to process SQS message"
                );
            }
        }

        Ok(())
    }

    async fn process_message(&self, message: &Message) -> Result<()> {
        let body = message
            .body()
            .ok_or_else(|| anyhow!("SQS message missing body"))?;
        let records = parse_s3_records(body)?;
        for (bucket, key) in records {
            self.process_object(&bucket, &key)
                .await
                .with_context(|| format!("process s3://{bucket}/{key}"))?;
        }

        let receipt = message
            .receipt_handle()
            .ok_or_else(|| anyhow!("SQS message missing receipt handle"))?;
        self.sqs
            .delete_message()
            .queue_url(&self.cfg.sqs_queue_url)
            .receipt_handle(receipt)
            .send()
            .await
            .context("delete SQS message")?;

        Ok(())
    }

    async fn process_object(&self, bucket: &str, key: &str) -> Result<()> {
        let output = self
            .s3
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .context("get S3 object")?;
        let bytes = output
            .body
            .collect()
            .await
            .context("read S3 object body")?
            .into_bytes();

        if bytes.is_empty() {
            warn!(bucket, key, "skipping empty event object");
            return Ok(());
        }

        let processed = self
            .processors
            .transform_ndjson(&bytes)
            .context("transform event object")?;

        let rows = count_ndjson_rows(&processed);
        if rows == 0 {
            warn!(
                bucket,
                key,
                bytes = processed.len(),
                "skipping object without complete rows"
            );
            return Ok(());
        }

        self.insert_clickhouse(&processed).await?;
        info!(
            bucket,
            key,
            rows,
            bytes = processed.len(),
            "loaded event object"
        );
        Ok(())
    }

    async fn insert_clickhouse(&self, body: &[u8]) -> Result<()> {
        let query = format!("INSERT INTO {} FORMAT JSONEachRow", self.cfg.table_name());
        let mut request = self
            .http
            .post(&self.cfg.clickhouse_url)
            .timeout(self.cfg.request_timeout)
            .query(&[
                ("database", self.cfg.clickhouse_database.as_str()),
                ("query", query.as_str()),
            ])
            .body(body.to_vec());

        if let Some(user) = self.cfg.clickhouse_user.as_deref() {
            request = request.basic_auth(user, self.cfg.clickhouse_password.as_deref());
        }

        let response = request.send().await.context("send ClickHouse insert")?;
        let status = response.status();
        let text = response.text().await.context("read ClickHouse response")?;
        if !status.is_success() {
            bail!("ClickHouse insert failed: {status} {text}");
        }

        Ok(())
    }
}

fn parse_s3_records(body: &str) -> Result<Vec<(String, String)>> {
    let event: S3Event = serde_json::from_str(body).context("parse S3 event")?;
    let mut records = Vec::with_capacity(event.records.len());
    for record in event.records {
        records.push((
            record.s3.bucket.name,
            urlencoding::decode(&record.s3.object.key.replace('+', " "))
                .context("decode S3 object key")?
                .into_owned(),
        ));
    }
    Ok(records)
}

fn count_ndjson_rows(bytes: &[u8]) -> usize {
    bytes.iter().filter(|byte| **byte == b'\n').count()
}

fn required(key: &'static str) -> Result<String> {
    optional(key).ok_or_else(|| anyhow!("{key} is required"))
}

fn optional(key: &'static str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_or(key: &'static str, fallback: &'static str) -> String {
    optional(key).unwrap_or_else(|| fallback.to_string())
}

fn parse_env<T>(key: &'static str, fallback: T) -> Result<T>
where
    T: std::str::FromStr,
{
    match optional(key) {
        Some(value) => value
            .parse()
            .map_err(|_| anyhow!("{key} must be a valid {}", std::any::type_name::<T>())),
        None => Ok(fallback),
    }
}

fn validate_identifier(key: &'static str, value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.chars().enumerate().all(|(index, ch)| {
            ch == '_' || ch.is_ascii_alphanumeric() && (index > 0 || ch.is_ascii_alphabetic())
        });
    if valid {
        Ok(())
    } else {
        bail!("{key} must be a simple ClickHouse identifier")
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::{count_ndjson_rows, parse_s3_records};

    #[test]
    fn parses_s3_event_records() {
        let records = parse_s3_records(
            r#"{
              "Records": [
                {
                  "s3": {
                    "bucket": {"name": "bucket"},
                    "object": {"key": "events/dt%3D2026-05-11/part+1%2B2.ndjson"}
                  }
                }
              ]
            }"#,
        )
        .expect("parse records");

        assert_eq!(
            records,
            vec![(
                "bucket".to_string(),
                "events/dt=2026-05-11/part 1+2.ndjson".to_string()
            )]
        );
    }

    #[test]
    fn counts_complete_ndjson_rows() {
        assert_eq!(count_ndjson_rows(b"{\"a\":1}\n{\"b\":2}\n"), 2);
        assert_eq!(count_ndjson_rows(b"{\"a\":1}"), 0);
    }
}
