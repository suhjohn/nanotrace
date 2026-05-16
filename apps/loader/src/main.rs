use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::{Client as SqsClient, types::Message};
use nanotrace_processor_runtime::{ProcessorRuntime, ProcessorSyncConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::RwLock;
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
    clickhouse_event_index_table: String,
    clickhouse_field_values_table: String,
    clickhouse_hot_dimensions_table: String,
    hot_dimensions_refresh: Duration,
    poll_wait: u32,
    max_messages: i32,
    visibility_timeout: i32,
    request_timeout: Duration,
    processor_bucket: Option<String>,
    processor_prefix: String,
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
    field_capabilities: Arc<RwLock<FieldCapabilityCache>>,
}

#[derive(Debug, Clone)]
struct FieldCapabilityCache {
    capabilities: FieldCapabilities,
    refreshed_at: Option<Instant>,
}

#[derive(Debug, Clone)]
struct FieldCapabilities {
    lookup: Vec<String>,
    aggregate: Vec<String>,
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
    tenant_id: String,
    bucket_time: String,
    key: String,
    value: String,
    value_type: String,
    count: u64,
    error_count: u64,
}

#[derive(Debug, Serialize)]
struct EventFacetIndexRow {
    tenant_id: String,
    key: String,
    value: String,
    value_type: String,
    timestamp: String,
    bucket_time: String,
    event_id: String,
    event_type: String,
    signal: String,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    name: String,
    start_time: Option<String>,
    end_time: Option<String>,
    duration_ms: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct HotDimensionCapability {
    path: String,
    lookup_enabled: u8,
    aggregate_enabled: u8,
}

#[derive(Debug, Serialize)]
struct FieldValueRow {
    tenant_id: String,
    key: String,
    value: String,
    value_type: String,
    first_seen: String,
    last_seen: String,
}

#[derive(Debug, Deserialize)]
struct ClickHouseResponse<T> {
    data: Vec<T>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env()?;
    let aws_config = aws_config::load_from_env().await;
    let s3 = s3_client(&aws_config);
    let processors = match cfg.processor_bucket.clone() {
        Some(bucket) => ProcessorRuntime::start(
            s3.clone(),
            ProcessorSyncConfig {
                bucket,
                prefix: cfg.processor_prefix.clone(),
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
        field_capabilities: Arc::new(RwLock::new(FieldCapabilityCache {
            capabilities: builtin_field_capabilities(),
            refreshed_at: None,
        })),
    };

    info!("nanotrace loader starting");
    loader.run().await
}

fn s3_client(config: &aws_config::SdkConfig) -> S3Client {
    let mut builder = aws_sdk_s3::config::Builder::from(config);
    if env_bool("AWS_S3_FORCE_PATH_STYLE") || env_bool("AWS_S3_PATH_STYLE") {
        builder.set_force_path_style(Some(true));
    }
    S3Client::from_conf(builder.build())
}

fn env_bool(key: &str) -> bool {
    env::var(key)
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}

impl Config {
    fn from_env() -> Result<Self> {
        let clickhouse_database = env_or("CLICKHOUSE_DATABASE", "observatory");
        let clickhouse_table = env_or("CLICKHOUSE_TABLE", "events");
        let clickhouse_facets_table = env_or("CLICKHOUSE_FACETS_TABLE", "event_facets");
        let clickhouse_event_index_table =
            env_or("CLICKHOUSE_EVENT_INDEX_TABLE", "event_facet_index");
        let clickhouse_field_values_table = env_or("CLICKHOUSE_FIELD_VALUES_TABLE", "field_values");
        let clickhouse_hot_dimensions_table =
            env_or("CLICKHOUSE_HOT_DIMENSIONS_TABLE", "hot_dimensions");
        validate_identifier("CLICKHOUSE_DATABASE", &clickhouse_database)?;
        validate_identifier("CLICKHOUSE_TABLE", &clickhouse_table)?;
        validate_identifier("CLICKHOUSE_FACETS_TABLE", &clickhouse_facets_table)?;
        validate_identifier(
            "CLICKHOUSE_EVENT_INDEX_TABLE",
            &clickhouse_event_index_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_FIELD_VALUES_TABLE",
            &clickhouse_field_values_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_HOT_DIMENSIONS_TABLE",
            &clickhouse_hot_dimensions_table,
        )?;

        Ok(Self {
            sqs_queue_url: required("LOADER_SQS_QUEUE_URL")?,
            clickhouse_url: required("CLICKHOUSE_URL")?,
            clickhouse_user: optional("CLICKHOUSE_USER")
                .or_else(|| optional("CLICKHOUSE_USERNAME")),
            clickhouse_password: optional("CLICKHOUSE_PASSWORD"),
            clickhouse_database,
            clickhouse_table,
            clickhouse_facets_table,
            clickhouse_event_index_table,
            clickhouse_field_values_table,
            clickhouse_hot_dimensions_table,
            hot_dimensions_refresh: Duration::from_secs(parse_env(
                "LOADER_HOT_DIMENSIONS_REFRESH_SECS",
                30,
            )?),
            poll_wait: parse_env("LOADER_POLL_WAIT_SECS", 20)?,
            max_messages: parse_env("LOADER_MAX_MESSAGES", 10)?,
            visibility_timeout: parse_env("LOADER_VISIBILITY_TIMEOUT_SECS", 300)?,
            request_timeout: Duration::from_secs(parse_env("LOADER_REQUEST_TIMEOUT_SECS", 60)?),
            processor_bucket: optional("PROCESSOR_S3_BUCKET")
                .or_else(|| optional("NANOTRACE_S3_BUCKET"))
                .or_else(|| optional("S3_BUCKET")),
            processor_prefix: env_or("PROCESSOR_PREFIX", "processors")
                .trim_matches('/')
                .to_string(),
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

    fn event_index_table_name(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_event_index_table
        )
    }

    fn field_values_table_name(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_field_values_table
        )
    }

    fn hot_dimensions_table_name(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_hot_dimensions_table
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
        let capabilities = self.field_capabilities().await;
        let field_values = field_values_ndjson(&processed, &capabilities.lookup)
            .context("generate field values")?;
        if !field_values.is_empty() {
            self.insert_clickhouse(&self.cfg.field_values_table_name(), &field_values)
                .await?;
        }
        let facets =
            facets_ndjson(&processed, &capabilities.aggregate).context("generate event facets")?;
        if !facets.is_empty() {
            self.insert_clickhouse(&self.cfg.facets_table_name(), &facets)
                .await?;
        }
        let event_index = event_facet_index_ndjson(&processed, &capabilities.lookup)
            .context("generate event index")?;
        if !event_index.is_empty() {
            self.insert_clickhouse(&self.cfg.event_index_table_name(), &event_index)
                .await?;
        }
        info!(
            bucket,
            key,
            rows,
            bytes = processed.len(),
            field_value_bytes = field_values.len(),
            facet_bytes = facets.len(),
            event_index_bytes = event_index.len(),
            lookup_keys = capabilities.lookup.len(),
            aggregate_keys = capabilities.aggregate.len(),
            "loaded event object"
        );
        Ok(())
    }

    async fn field_capabilities(&self) -> FieldCapabilities {
        let now = Instant::now();
        {
            let cache = self.field_capabilities.read().await;
            if cache.refreshed_at.is_some_and(|refreshed| {
                now.duration_since(refreshed) < self.cfg.hot_dimensions_refresh
            }) {
                return cache.capabilities.clone();
            }
        }

        let mut cache = self.field_capabilities.write().await;
        if cache.refreshed_at.is_some_and(|refreshed| {
            now.duration_since(refreshed) < self.cfg.hot_dimensions_refresh
        }) {
            return cache.capabilities.clone();
        }

        match self.fetch_field_capabilities().await {
            Ok(capabilities) => {
                cache.capabilities = capabilities;
                cache.refreshed_at = Some(now);
            }
            Err(err) => {
                cache.capabilities = builtin_field_capabilities();
                cache.refreshed_at = Some(now);
                warn!(error = %err, "failed to refresh field capability registry; using builtin capabilities");
            }
        }
        cache.capabilities.clone()
    }

    async fn fetch_field_capabilities(&self) -> Result<FieldCapabilities> {
        let query = format!(
            "SELECT path, lookup_enabled, aggregate_enabled \
             FROM (SELECT path, status, source, lookup_enabled, aggregate_enabled, updated_at \
                   FROM {} ORDER BY updated_at DESC LIMIT 1 BY path) \
             WHERE status = 'active' AND source = 'user' \
             ORDER BY path ASC FORMAT JSON",
            self.cfg.hot_dimensions_table_name()
        );
        let response: ClickHouseResponse<HotDimensionCapability> =
            serde_json::from_str(&self.clickhouse_query(&query).await?)
                .context("parse hot dimension registry response")?;

        let mut capabilities = builtin_field_capabilities();
        let mut lookup_seen = capabilities.lookup.iter().cloned().collect::<BTreeSet<_>>();
        let mut aggregate_seen = capabilities
            .aggregate
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for row in response.data {
            let key = row.path.trim().to_string();
            if !is_valid_facet_path(&key) {
                continue;
            }
            if row.lookup_enabled > 0 && lookup_seen.insert(key.clone()) {
                capabilities.lookup.push(key.clone());
            }
            if row.aggregate_enabled > 0 && aggregate_seen.insert(key.clone()) {
                capabilities.aggregate.push(key);
            }
        }
        Ok(capabilities)
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
                ("date_time_input_format", "best_effort"),
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

    async fn clickhouse_query(&self, query: &str) -> Result<String> {
        let mut request = self
            .http
            .post(&self.cfg.clickhouse_url)
            .timeout(self.cfg.request_timeout)
            .query(&[("database", self.cfg.clickhouse_database.as_str())])
            .body(query.to_string());

        if let Some(user) = self.cfg.clickhouse_user.as_deref() {
            request = request.basic_auth(user, self.cfg.clickhouse_password.as_deref());
        }

        let response = request.send().await.context("send ClickHouse query")?;
        let status = response.status();
        let text = response.text().await.context("read ClickHouse response")?;
        if !status.is_success() {
            bail!("ClickHouse query failed: {status} {text}");
        }

        Ok(text)
    }
}

fn field_values_ndjson(bytes: &[u8], value_keys: &[String]) -> Result<Vec<u8>> {
    let mut values = BTreeMap::<(String, String, String, String), (String, String)>::new();
    let mut out = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let row: Value =
            serde_json::from_slice(line).context("parse event row for field values")?;
        let Some(row) = row.as_object() else {
            continue;
        };
        for value in field_value_rows(row, value_keys) {
            let entry = values
                .entry((value.tenant_id, value.key, value.value, value.value_type))
                .or_insert((value.first_seen.clone(), value.last_seen.clone()));
            if value.first_seen < entry.0 {
                entry.0 = value.first_seen;
            }
            if value.last_seen > entry.1 {
                entry.1 = value.last_seen;
            }
        }
    }
    for ((tenant_id, key, value, value_type), (first_seen, last_seen)) in values {
        serde_json::to_writer(
            &mut out,
            &FieldValueRow {
                tenant_id,
                key,
                value,
                value_type,
                first_seen,
                last_seen,
            },
        )
        .context("serialize field value row")?;
        out.push(b'\n');
    }
    Ok(out)
}

fn field_value_rows(row: &Map<String, Value>, value_keys: &[String]) -> Vec<FieldValueRow> {
    let timestamp = string_value(row.get("timestamp"));
    if timestamp.is_empty() {
        return Vec::new();
    }
    let Some(data) = row.get("data").and_then(Value::as_object) else {
        return Vec::new();
    };
    let context = EventIndexContext::from_row(row, data, timestamp.clone());
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    for key in value_keys {
        if let Some(value) = indexed_value_for_key(row, data, &context, key) {
            collect_field_values(&context, key, &value, &mut seen, &mut rows);
        }
    }
    rows
}

fn collect_field_values(
    context: &EventIndexContext,
    key: &str,
    value: &Value,
    seen: &mut BTreeSet<String>,
    rows: &mut Vec<FieldValueRow>,
) {
    match value {
        Value::Null => {}
        Value::Bool(value) => push_field_value_row(
            context,
            key,
            if *value { "true" } else { "false" }.to_string(),
            "bool",
            seen,
            rows,
        ),
        Value::Number(value) => {
            push_field_value_row(context, key, value.to_string(), "number", seen, rows)
        }
        Value::String(value) => {
            push_field_value_row(context, key, value.clone(), "string", seen, rows)
        }
        Value::Array(values) => {
            for value in values {
                collect_field_values(context, key, value, seen, rows);
            }
        }
        Value::Object(_) => {}
    }
}

fn push_field_value_row(
    context: &EventIndexContext,
    key: &str,
    value: String,
    value_type: &str,
    seen: &mut BTreeSet<String>,
    rows: &mut Vec<FieldValueRow>,
) {
    if key.is_empty() || value.is_empty() {
        return;
    }
    let dedupe_key = format!("{key}\u{0}{value_type}\u{0}{value}");
    if !seen.insert(dedupe_key) {
        return;
    }
    rows.push(FieldValueRow {
        tenant_id: context.tenant_id.clone(),
        key: key.to_string(),
        value,
        value_type: value_type.to_string(),
        first_seen: context.timestamp.clone(),
        last_seen: context.timestamp.clone(),
    });
}

fn facets_ndjson(bytes: &[u8], aggregate_keys: &[String]) -> Result<Vec<u8>> {
    let mut counts = BTreeMap::<(String, String, String, String, String), (u64, u64)>::new();
    let mut out = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let row: Value = serde_json::from_slice(line).context("parse event row for facets")?;
        let Some(row) = row.as_object() else {
            continue;
        };
        for facet in facet_rows(row, aggregate_keys) {
            let count = counts
                .entry((
                    facet.tenant_id,
                    facet.bucket_time,
                    facet.key,
                    facet.value,
                    facet.value_type,
                ))
                .or_insert((0, 0));
            count.0 += facet.count;
            count.1 += facet.error_count;
        }
    }
    for ((tenant_id, bucket_time, key, value, value_type), (count, error_count)) in counts {
        serde_json::to_writer(
            &mut out,
            &FacetRow {
                tenant_id,
                bucket_time,
                key,
                value,
                value_type,
                count,
                error_count,
            },
        )
        .context("serialize facet row")?;
        out.push(b'\n');
    }
    Ok(out)
}

fn facet_rows(row: &Map<String, Value>, aggregate_keys: &[String]) -> Vec<FacetRow> {
    let timestamp = string_value(row.get("timestamp"));
    if timestamp.is_empty() {
        return Vec::new();
    }

    let data = row.get("data").and_then(Value::as_object);
    let context = FacetContext {
        tenant_id: data
            .map(|data| string_value(data.get("tenant_id")))
            .unwrap_or_default(),
        bucket_time: minute_bucket(&timestamp).unwrap_or_else(|| timestamp.clone()),
        error_count: if is_error_row(row) { 1 } else { 0 },
    };

    let mut facets = Vec::new();
    let mut seen = BTreeSet::new();
    if let Some(data) = data {
        let index_context = EventIndexContext::from_row(row, data, timestamp);
        for key in aggregate_keys {
            if let Some(value) = indexed_value_for_key(row, data, &index_context, key) {
                collect_facets(&context, key, &value, &mut seen, &mut facets);
            }
        }
    }
    facets
}

fn event_facet_index_ndjson(bytes: &[u8], indexed_keys: &[String]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let row: Value = serde_json::from_slice(line).context("parse event row for index")?;
        let Some(row) = row.as_object() else {
            continue;
        };
        for index_row in event_facet_index_rows(row, indexed_keys) {
            serde_json::to_writer(&mut out, &index_row).context("serialize event index row")?;
            out.push(b'\n');
        }
    }
    Ok(out)
}

fn event_facet_index_rows(
    row: &Map<String, Value>,
    indexed_keys: &[String],
) -> Vec<EventFacetIndexRow> {
    let timestamp = string_value(row.get("timestamp"));
    if timestamp.is_empty() {
        return Vec::new();
    }
    let Some(data) = row.get("data").and_then(Value::as_object) else {
        return Vec::new();
    };

    let context = EventIndexContext::from_row(row, data, timestamp);

    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    for key in indexed_keys {
        if let Some(value) = indexed_value_for_key(row, data, &context, key) {
            collect_event_index_facets(&context, key, &value, &mut seen, &mut rows);
        }
    }
    rows
}

struct EventIndexContext {
    tenant_id: String,
    timestamp: String,
    bucket_time: String,
    event_id: String,
    event_type: String,
    signal: String,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    name: String,
    start_time: Option<String>,
    end_time: Option<String>,
    duration_ms: Option<f64>,
}

impl EventIndexContext {
    fn from_row(row: &Map<String, Value>, data: &Map<String, Value>, timestamp: String) -> Self {
        let event_id = string_value(row.get("event_id"));
        let event_type = string_value(data.get("event_type"));
        let signal = signal_for_event_type(&event_type).to_string();
        Self {
            tenant_id: string_value(data.get("tenant_id")),
            timestamp: timestamp.clone(),
            bucket_time: minute_bucket(&timestamp).unwrap_or(timestamp),
            event_id,
            event_type,
            signal,
            trace_id: string_value(data.get("trace_id")),
            span_id: string_value(data.get("span_id")),
            parent_span_id: string_value(data.get("parent_span_id")),
            name: string_value(data.get("name")),
            start_time: optional_time(data.get("start_time")),
            end_time: optional_time(data.get("end_time")),
            duration_ms: optional_number_value(data.get("duration_ms")),
        }
    }
}

fn builtin_field_capabilities() -> FieldCapabilities {
    let aggregate = [
        "tenant_id",
        "service",
        "environment",
        "event_type",
        "signal",
        "name",
        "http.route",
        "http.method",
        "http.status_code",
        "severity_text",
        "metric_name",
    ];
    let lookup_only = [
        "trace_id",
        "span_id",
        "parent_span_id",
        "event_id",
        "request_id",
        "user_id",
        "account_id",
        "session_id",
    ];
    let mut lookup = BTreeSet::new();
    for key in aggregate.into_iter().chain(lookup_only) {
        lookup.insert(key.to_string());
    }
    FieldCapabilities {
        lookup: lookup.into_iter().collect(),
        aggregate: aggregate.into_iter().map(str::to_string).collect(),
    }
}

fn indexed_value_for_key(
    row: &Map<String, Value>,
    data: &Map<String, Value>,
    context: &EventIndexContext,
    key: &str,
) -> Option<Value> {
    match key {
        "event_id" => Some(Value::String(context.event_id.clone())),
        "signal" => Some(Value::String(context.signal.clone())),
        _ => value_at_path(data, key)
            .cloned()
            .or_else(|| row.get(key).cloned()),
    }
}

fn collect_event_index_facets(
    context: &EventIndexContext,
    key: &str,
    value: &Value,
    seen: &mut BTreeSet<String>,
    rows: &mut Vec<EventFacetIndexRow>,
) {
    match value {
        Value::Null => {}
        Value::Bool(value) => push_event_index_row(
            context,
            key,
            if *value { "true" } else { "false" }.to_string(),
            "bool",
            seen,
            rows,
        ),
        Value::Number(value) => {
            push_event_index_row(context, key, value.to_string(), "number", seen, rows)
        }
        Value::String(value) => {
            push_event_index_row(context, key, value.clone(), "string", seen, rows)
        }
        Value::Array(values) => {
            for value in values {
                collect_event_index_facets(context, key, value, seen, rows);
            }
        }
        Value::Object(_) => {}
    }
}

fn push_event_index_row(
    context: &EventIndexContext,
    key: &str,
    value: String,
    value_type: &str,
    seen: &mut BTreeSet<String>,
    rows: &mut Vec<EventFacetIndexRow>,
) {
    if key.is_empty() || value.is_empty() {
        return;
    }
    let dedupe_key = format!("{key}\u{0}{value_type}\u{0}{value}");
    if !seen.insert(dedupe_key) {
        return;
    }
    rows.push(EventFacetIndexRow {
        tenant_id: context.tenant_id.clone(),
        key: key.to_string(),
        value,
        value_type: value_type.to_string(),
        timestamp: context.timestamp.clone(),
        bucket_time: context.bucket_time.clone(),
        event_id: context.event_id.clone(),
        event_type: context.event_type.clone(),
        signal: context.signal.clone(),
        trace_id: context.trace_id.clone(),
        span_id: context.span_id.clone(),
        parent_span_id: context.parent_span_id.clone(),
        name: context.name.clone(),
        start_time: context.start_time.clone(),
        end_time: context.end_time.clone(),
        duration_ms: context.duration_ms,
    });
}

struct FacetContext {
    tenant_id: String,
    bucket_time: String,
    error_count: u64,
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
        tenant_id: context.tenant_id.clone(),
        bucket_time: context.bucket_time.clone(),
        key: key.to_string(),
        value,
        value_type: value_type.to_string(),
        count: 1,
        error_count: context.error_count,
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

fn optional_time(value: Option<&Value>) -> Option<String> {
    string_value(value).into_non_empty()
}

fn number_value(value: Option<&Value>) -> f64 {
    match value {
        Some(Value::Number(value)) => value.as_f64().unwrap_or_default(),
        Some(Value::String(value)) => value.parse().unwrap_or_default(),
        _ => 0.0,
    }
}

fn optional_number_value(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Number(value)) => value.as_f64(),
        Some(Value::String(value)) => value.parse().ok(),
        _ => None,
    }
}

fn is_error_row(row: &Map<String, Value>) -> bool {
    let data = row.get("data").and_then(Value::as_object);
    boolish_value(data.and_then(|data| data.get("is_error")))
        || data
            .and_then(|data| string_value(data.get("span_status_code")).into_non_empty())
            .is_some_and(|value| value.eq_ignore_ascii_case("error"))
        || {
            let event_type = string_value(row.get("event_type"))
                .into_non_empty()
                .or_else(|| {
                    data.and_then(|data| string_value(data.get("event_type")).into_non_empty())
                })
                .unwrap_or_default();
            event_type.to_ascii_lowercase().ends_with("_error")
        }
}

fn boolish_value(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(value)) => *value,
        Some(Value::Number(value)) => value.as_u64().is_some_and(|value| value > 0),
        Some(Value::String(value)) => {
            matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes")
        }
        _ => false,
    }
}

fn value_at_path<'a>(data: &'a Map<String, Value>, path: &str) -> Option<&'a Value> {
    if let Some(value) = data.get(path) {
        return Some(value);
    }
    let mut current: Option<&'a Value> = None;
    for part in path.split('.') {
        let object: &'a Map<String, Value> = match current {
            Some(Value::Object(object)) => object,
            None => data,
            _ => return None,
        };
        current = object.get(part);
    }
    current
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
    let value: Value = serde_json::from_str(body).context("parse S3 event")?;
    if value
        .get("Event")
        .and_then(Value::as_str)
        .is_some_and(|event| event == "s3:TestEvent")
    {
        return Ok(Vec::new());
    }
    let event: S3Event = serde_json::from_value(value).context("parse S3 event")?;
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

fn is_valid_facet_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 256
        && !path.starts_with('.')
        && !path.ends_with('.')
        && !path.contains("..")
        && path
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' || ch == '-')
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
    use super::{count_ndjson_rows, event_facet_index_ndjson, facets_ndjson, parse_s3_records};
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
        let aggregate_keys = [
            "name".to_string(),
            "http.status_code".to_string(),
            "ok".to_string(),
            "tags".to_string(),
            "signal".to_string(),
        ];
        let facets = facets_ndjson(
            br#"{"event_id":"evt_1","timestamp":"2026-05-12T00:00:00.000Z","source_file":"events/part.ndjson","source_offset":42,"data":{"tenant_id":"tenant-a","event_type":"span","trace_id":"trace-1","span_id":"span-1","name":"GET /users","http":{"status_code":200},"ok":true,"tags":["api","api","prod"],"ignored":null}}"#,
            &aggregate_keys,
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
        assert!(parsed.iter().all(|row| row["error_count"] == 0));
    }

    #[test]
    fn aggregates_facets_by_minute_key_and_value() {
        let aggregate_keys = ["name".to_string()];
        let facets = facets_ndjson(
            br#"{"event_id":"evt_1","timestamp":"2026-05-12T00:00:01.000Z","data":{"name":"GET /users","is_error":true}}
{"event_id":"evt_2","timestamp":"2026-05-12T00:00:59.000Z","data":{"name":"GET /users"}}
"#,
            &aggregate_keys,
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
            && row["count"] == 2
            && row["error_count"] == 1));
    }

    #[test]
    fn generates_event_index_for_configured_hot_dimensions_only() {
        let index = event_facet_index_ndjson(
            br#"{"event_id":"evt_1","timestamp":"2026-05-12T00:00:00.000Z","data":{"tenant_id":"tenant-a","event_type":"span_start","trace_id":"trace-1","span_id":"span-1","parent_span_id":"root","name":"GET /users","user_id":"user-1","session_id":"session-1","http":{"route":"/users/:id","method":"GET"},"ignored":"cold"}}"#,
            &[
                "trace_id".to_string(),
                "session_id".to_string(),
                "http.route".to_string(),
                "signal".to_string(),
            ],
        )
        .expect("generate event index");
        let rows = String::from_utf8(index).expect("utf8");
        let parsed: Vec<Value> = rows
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert!(parsed.iter().any(|row| row["key"] == "trace_id"
            && row["value"] == "trace-1"
            && row["trace_id"] == "trace-1"
            && row["span_id"] == "span-1"));
        assert!(
            parsed
                .iter()
                .any(|row| row["key"] == "session_id" && row["value"] == "session-1")
        );
        assert!(
            parsed
                .iter()
                .any(|row| row["key"] == "http.route" && row["value"] == "/users/:id")
        );
        assert!(
            parsed
                .iter()
                .any(|row| row["key"] == "signal" && row["value"] == "trace")
        );
        assert!(!parsed.iter().any(|row| row["key"] == "ignored"));
        assert!(!parsed.iter().any(|row| row["key"] == "user_id"));
    }
}
