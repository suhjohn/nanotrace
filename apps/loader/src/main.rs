use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::{Client as SqsClient, types::Message};
use nanotrace_processor_runtime::{ProcessorRuntime, ProcessorSyncConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
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
    clickhouse_facets_table: String,
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

#[derive(Debug, Serialize)]
struct FacetRow {
    bucket_time: String,
    key: String,
    value: String,
    value_type: String,
    count: u64,
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
        let clickhouse_facets_table = env_or("CLICKHOUSE_FACETS_TABLE", "event_facets");
        validate_identifier("CLICKHOUSE_DATABASE", &clickhouse_database)?;
        validate_identifier("CLICKHOUSE_TABLE", &clickhouse_table)?;
        validate_identifier("CLICKHOUSE_FACETS_TABLE", &clickhouse_facets_table)?;

        Ok(Self {
            sqs_queue_url: required("LOADER_SQS_QUEUE_URL")?,
            clickhouse_url: required("CLICKHOUSE_URL")?,
            clickhouse_user: optional("CLICKHOUSE_USER")
                .or_else(|| optional("CLICKHOUSE_USERNAME")),
            clickhouse_password: optional("CLICKHOUSE_PASSWORD"),
            clickhouse_database,
            clickhouse_table,
            clickhouse_facets_table,
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

    fn facets_table_name(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_facets_table
        )
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

        self.insert_clickhouse(&self.cfg.table_name(), &processed)
            .await?;
        let facets = facets_ndjson(&processed).context("generate event facets")?;
        if !facets.is_empty() {
            self.insert_clickhouse(&self.cfg.facets_table_name(), &facets)
                .await?;
        }
        info!(
            bucket,
            key,
            rows,
            bytes = processed.len(),
            facet_bytes = facets.len(),
            "loaded event object"
        );
        Ok(())
    }

    async fn insert_clickhouse(&self, table: &str, body: &[u8]) -> Result<()> {
        let query = format!("INSERT INTO {table} FORMAT JSONEachRow");
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

fn facets_ndjson(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut counts = BTreeMap::<(String, String, String, String), u64>::new();
    let mut out = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let row: Value = serde_json::from_slice(line).context("parse event row for facets")?;
        let Some(row) = row.as_object() else {
            continue;
        };
        for facet in facet_rows(row) {
            *counts
                .entry((facet.bucket_time, facet.key, facet.value, facet.value_type))
                .or_insert(0) += facet.count;
        }
    }
    for ((bucket_time, key, value, value_type), count) in counts {
        serde_json::to_writer(
            &mut out,
            &FacetRow {
                bucket_time,
                key,
                value,
                value_type,
                count,
            },
        )
        .context("serialize facet row")?;
        out.push(b'\n');
    }
    Ok(out)
}

fn facet_rows(row: &Map<String, Value>) -> Vec<FacetRow> {
    let timestamp = string_value(row.get("timestamp"));
    if timestamp.is_empty() {
        return Vec::new();
    }

    let data = row.get("data").and_then(Value::as_object);
    let event_type = data
        .and_then(|data| string_value(data.get("event_type")).into_non_empty())
        .unwrap_or_default();
    let context = FacetContext {
        bucket_time: minute_bucket(&timestamp).unwrap_or(timestamp),
        signal: signal_for_event_type(&event_type).to_string(),
    };

    let mut facets = Vec::new();
    let mut seen = BTreeSet::new();
    if let Some(data) = data {
        for (key, value) in data {
            collect_facets(&context, key, value, &mut seen, &mut facets);
        }
    }
    push_facet(
        &context,
        "signal",
        context.signal.clone(),
        "string",
        &mut seen,
        &mut facets,
    );
    facets
}

struct FacetContext {
    bucket_time: String,
    signal: String,
}

trait NonEmptyString {
    fn into_non_empty(self) -> Option<String>;
}

impl NonEmptyString for String {
    fn into_non_empty(self) -> Option<String> {
        if self.is_empty() { None } else { Some(self) }
    }
}

fn collect_facets(
    context: &FacetContext,
    path: &str,
    value: &Value,
    seen: &mut BTreeSet<String>,
    facets: &mut Vec<FacetRow>,
) {
    match value {
        Value::Null => {}
        Value::Bool(value) => push_facet(
            context,
            path,
            if *value { "true" } else { "false" }.to_string(),
            "bool",
            seen,
            facets,
        ),
        Value::Number(value) => {
            push_facet(context, path, value.to_string(), "number", seen, facets)
        }
        Value::String(value) => push_facet(context, path, value.clone(), "string", seen, facets),
        Value::Array(values) => {
            for value in values {
                collect_facets(context, path, value, seen, facets);
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                let nested = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                collect_facets(context, &nested, value, seen, facets);
            }
        }
    }
}

fn push_facet(
    context: &FacetContext,
    key: &str,
    value: String,
    value_type: &str,
    seen: &mut BTreeSet<String>,
    facets: &mut Vec<FacetRow>,
) {
    if key.is_empty() || value.is_empty() {
        return;
    }
    let dedupe_key = format!("{key}\u{0}{value_type}\u{0}{value}");
    if !seen.insert(dedupe_key) {
        return;
    }
    facets.push(FacetRow {
        bucket_time: context.bucket_time.clone(),
        key: key.to_string(),
        value,
        value_type: value_type.to_string(),
        count: 1,
    });
}

fn minute_bucket(timestamp: &str) -> Option<String> {
    let normalized = timestamp.trim().replace('T', " ");
    if normalized.len() < 16 {
        return None;
    }
    let prefix = &normalized[..16];
    if prefix.as_bytes().get(4) != Some(&b'-')
        || prefix.as_bytes().get(7) != Some(&b'-')
        || prefix.as_bytes().get(10) != Some(&b' ')
        || prefix.as_bytes().get(13) != Some(&b':')
    {
        return None;
    }
    Some(format!("{prefix}:00.000"))
}

fn string_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => {
            if *value {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        _ => String::new(),
    }
}

fn signal_for_event_type(event_type: &str) -> &'static str {
    match event_type {
        "span" | "span_start" | "span_end" => "trace",
        "metric" => "metric",
        "log" => "log",
        "analytics" | "track" | "page" | "screen" | "identify" | "group" | "alias" => "analytics",
        _ => "other",
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
    use super::{count_ndjson_rows, facets_ndjson, parse_s3_records};
    use serde_json::Value;

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

    #[test]
    fn generates_facets_for_nested_scalar_values() {
        let facets = facets_ndjson(
            br#"{"event_id":"evt_1","timestamp":"2026-05-12T00:00:00.000Z","source_file":"events/part.ndjson","source_offset":42,"data":{"tenant_id":"tenant-a","event_type":"span","trace_id":"trace-1","span_id":"span-1","name":"GET /users","http":{"status_code":200},"ok":true,"tags":["api","api","prod"],"ignored":null}}"#,
        )
        .expect("generate facets");
        let rows = String::from_utf8(facets).expect("utf8");
        let parsed: Vec<Value> = rows
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert!(parsed.iter().any(|row| row["key"] == "name"
            && row["value"] == "GET /users"
            && row["value_type"] == "string"));
        assert!(parsed.iter().any(|row| row["key"] == "http.status_code"
            && row["value"] == "200"
            && row["value_type"] == "number"));
        assert!(parsed.iter().any(|row| row["key"] == "ok"
            && row["value"] == "true"
            && row["value_type"] == "bool"));
        assert!(
            parsed
                .iter()
                .any(|row| row["key"] == "signal" && row["value"] == "trace")
        );
        assert_eq!(
            parsed
                .iter()
                .filter(|row| row["key"] == "tags" && row["value"] == "api")
                .count(),
            1
        );
        assert!(
            parsed
                .iter()
                .all(|row| row["bucket_time"] == "2026-05-12 00:00:00.000")
        );
        assert!(parsed.iter().all(|row| row["count"] == 1));
    }

    #[test]
    fn aggregates_facets_by_minute_key_and_value() {
        let facets = facets_ndjson(
            br#"{"event_id":"evt_1","timestamp":"2026-05-12T00:00:01.000Z","data":{"name":"GET /users"}}
{"event_id":"evt_2","timestamp":"2026-05-12T00:00:59.000Z","data":{"name":"GET /users"}}
"#,
        )
        .expect("generate facets");
        let rows = String::from_utf8(facets).expect("utf8");
        let parsed: Vec<Value> = rows
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert!(parsed.iter().any(|row| row["key"] == "name"
            && row["value"] == "GET /users"
            && row["bucket_time"] == "2026-05-12 00:00:00.000"
            && row["count"] == 2));
    }
}
