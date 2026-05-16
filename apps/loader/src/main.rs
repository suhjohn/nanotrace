use std::{collections::BTreeSet, env, path::PathBuf, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::{Client as SqsClient, types::Message};
use nanotrace_processor_runtime::{ProcessorRuntime, ProcessorSyncConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const DEFAULT_CLICKHOUSE_INSERT_MAX_ROWS: usize = 100_000;
const DEFAULT_CLICKHOUSE_INSERT_MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
struct Config {
    sqs_queue_url: String,
    clickhouse_url: String,
    clickhouse_user: Option<String>,
    clickhouse_password: Option<String>,
    clickhouse_database: String,
    clickhouse_table: String,
    clickhouse_field_index_table: String,
    clickhouse_span_fragments_table: String,
    clickhouse_event_measures_table: String,
    clickhouse_entity_state_updates_table: String,
    clickhouse_definitions_table: String,
    poll_wait: u32,
    max_messages: i32,
    visibility_timeout: i32,
    request_timeout: Duration,
    processor_bucket: Option<String>,
    processor_prefix: String,
    processor_poll_interval: Duration,
    processor_dir: PathBuf,
    clickhouse_insert_max_rows: usize,
    clickhouse_insert_max_bytes: usize,
}

#[derive(Clone)]
struct Loader {
    cfg: Config,
    http: reqwest::Client,
    processors: ProcessorRuntime,
    s3: S3Client,
    sqs: SqsClient,
}

#[derive(Debug, Clone)]
struct FieldCapabilities {
    lookup: Vec<String>,
    aggregate: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct ExtractionDefinitions {
    fields: Vec<FieldDefinition>,
    measures: Vec<MeasureDefinition>,
    states: Vec<StateDefinition>,
}

#[derive(Debug, Clone)]
struct FieldDefinition {
    tenant_id: String,
    definition_id: String,
    definition_version: u64,
    name: String,
    mode: String,
    path: String,
    value_type: String,
}

#[derive(Debug, Clone)]
struct MeasureDefinition {
    tenant_id: String,
    definition_id: String,
    definition_version: u64,
    name: String,
    path: String,
    unit: String,
    dimension: String,
}

#[derive(Debug, Clone)]
struct StateDefinition {
    tenant_id: String,
    definition_id: String,
    definition_version: u64,
    name: String,
    path: String,
    value_type: String,
    entity_type: String,
    entity_id_path: String,
}

#[derive(Debug, Deserialize)]
struct DefinitionRecord {
    tenant_id: String,
    definition_id: String,
    name: String,
    kind: String,
    mode: String,
    config: Value,
    version: u64,
}

#[derive(Debug, Deserialize)]
struct ClickHouseJson<T> {
    data: Vec<T>,
}

impl ExtractionDefinitions {
    fn from_records(records: Vec<DefinitionRecord>) -> Self {
        let mut fields = Vec::new();
        let mut measures = Vec::new();
        let mut states = Vec::new();
        for record in records {
            match record.kind.as_str() {
                "field" => {
                    let path = config_string(&record.config, "path");
                    if path.is_empty() {
                        continue;
                    }
                    let mode = match record.mode.as_str() {
                        "lookup" => "lookup",
                        _ => "facet",
                    };
                    fields.push(FieldDefinition {
                        tenant_id: record.tenant_id,
                        definition_id: record.definition_id,
                        definition_version: record.version,
                        name: record.name,
                        mode: mode.to_string(),
                        path,
                        value_type: config_string_default(&record.config, "value_type", "string"),
                    });
                }
                "measure" | "rollup" => {
                    let path = config_string(&record.config, "path");
                    if path.is_empty() {
                        continue;
                    }
                    measures.push(MeasureDefinition {
                        tenant_id: record.tenant_id,
                        definition_id: record.definition_id,
                        definition_version: record.version,
                        name: record.name,
                        path,
                        unit: config_string_default(&record.config, "unit", ""),
                        dimension: config_string_default(&record.config, "dimension", ""),
                    });
                }
                "state" => {
                    let path = config_string(&record.config, "path");
                    let entity_type = config_string(&record.config, "entity_type");
                    let entity_id_path = config_string(&record.config, "entity_id_path");
                    if path.is_empty() || entity_type.is_empty() || entity_id_path.is_empty() {
                        continue;
                    }
                    states.push(StateDefinition {
                        tenant_id: record.tenant_id,
                        definition_id: record.definition_id,
                        definition_version: record.version,
                        name: record.name,
                        path,
                        value_type: config_string_default(&record.config, "value_type", "string"),
                        entity_type,
                        entity_id_path,
                    });
                }
                _ => {}
            }
        }
        Self {
            fields,
            measures,
            states,
        }
    }
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

#[cfg(test)]
#[derive(Debug, Serialize)]
struct EventIndexRow {
    tenant_id: String,
    timestamp: String,
    bucket_time: String,
    event_id: String,
    event_type: String,
    signal: String,
    service: String,
    environment: String,
    name: String,
    title: String,
    is_error: u8,
    correlation_id: String,
    parent_id: String,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    start_time: Option<String>,
    end_time: Option<String>,
    duration_ms: Option<f64>,
    source_file: String,
    source_offset: u64,
    source_length: u32,
    data: Value,
}

#[derive(Debug, Serialize)]
struct FieldIndexRow {
    tenant_id: String,
    mode: String,
    field_name: String,
    value: String,
    value_type: String,
    timestamp: String,
    bucket_time: String,
    event_id: String,
    event_type: String,
    signal: String,
    is_error: u8,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    name: String,
    start_time: Option<String>,
    end_time: Option<String>,
    duration_ms: Option<f64>,
    definition_id: String,
    definition_version: u64,
}

#[derive(Debug, Serialize)]
struct EventMeasureRow {
    tenant_id: String,
    definition_id: String,
    definition_version: u64,
    measure_name: String,
    value: f64,
    unit: String,
    timestamp: String,
    bucket_time: String,
    bucket_seconds: u32,
    event_id: String,
    event_type: String,
    signal: String,
    dimension_name: String,
    dimension_value: String,
}

#[derive(Debug, Serialize)]
struct EntityStateUpdateRow {
    tenant_id: String,
    definition_id: String,
    definition_version: u64,
    entity_type: String,
    entity_id: String,
    state_name: String,
    value: String,
    value_type: String,
    timestamp: String,
    event_id: String,
    event_type: String,
    signal: String,
}

#[derive(Debug, Serialize)]
struct SpanFragmentRow {
    tenant_id: String,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    event_id: String,
    event_type: String,
    signal: String,
    service: String,
    environment: String,
    name: String,
    start_time: Option<String>,
    end_time: Option<String>,
    duration_ms: Option<f64>,
    is_error: u8,
    timestamp: String,
    source_file: String,
    source_offset: u64,
    source_length: u32,
    data: Value,
}

#[derive(Default)]
struct DerivedBuffers {
    rows: usize,
    field_index: Vec<u8>,
    span_fragments: Vec<u8>,
    event_measures: Vec<u8>,
    entity_state_updates: Vec<u8>,
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
        let clickhouse_field_index_table = env_or("CLICKHOUSE_FIELD_INDEX_TABLE", "field_index");
        let clickhouse_span_fragments_table =
            env_or("CLICKHOUSE_SPAN_FRAGMENTS_TABLE", "span_fragments");
        let clickhouse_event_measures_table =
            env_or("CLICKHOUSE_EVENT_MEASURES_TABLE", "event_measures");
        let clickhouse_entity_state_updates_table = env_or(
            "CLICKHOUSE_ENTITY_STATE_UPDATES_TABLE",
            "entity_state_updates",
        );
        let clickhouse_definitions_table = env_or("CLICKHOUSE_DEFINITIONS_TABLE", "definitions");
        validate_identifier("CLICKHOUSE_DATABASE", &clickhouse_database)?;
        validate_identifier("CLICKHOUSE_TABLE", &clickhouse_table)?;
        validate_identifier(
            "CLICKHOUSE_FIELD_INDEX_TABLE",
            &clickhouse_field_index_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_SPAN_FRAGMENTS_TABLE",
            &clickhouse_span_fragments_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_EVENT_MEASURES_TABLE",
            &clickhouse_event_measures_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_ENTITY_STATE_UPDATES_TABLE",
            &clickhouse_entity_state_updates_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_DEFINITIONS_TABLE",
            &clickhouse_definitions_table,
        )?;

        Ok(Self {
            sqs_queue_url: required("LOADER_SQS_QUEUE_URL")?,
            clickhouse_url: required("CLICKHOUSE_URL")?,
            clickhouse_user: optional("CLICKHOUSE_USER")
                .or_else(|| optional("CLICKHOUSE_USERNAME")),
            clickhouse_password: optional("CLICKHOUSE_PASSWORD"),
            clickhouse_database,
            clickhouse_table,
            clickhouse_field_index_table,
            clickhouse_span_fragments_table,
            clickhouse_event_measures_table,
            clickhouse_entity_state_updates_table,
            clickhouse_definitions_table,
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
            clickhouse_insert_max_rows: parse_env(
                "CLICKHOUSE_INSERT_MAX_ROWS",
                DEFAULT_CLICKHOUSE_INSERT_MAX_ROWS,
            )?,
            clickhouse_insert_max_bytes: parse_env(
                "CLICKHOUSE_INSERT_MAX_BYTES",
                DEFAULT_CLICKHOUSE_INSERT_MAX_BYTES,
            )?,
        })
    }

    fn table_name(&self) -> String {
        format!("{}.{}", self.clickhouse_database, self.clickhouse_table)
    }

    fn field_index_table_name(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_field_index_table
        )
    }

    fn span_fragments_table_name(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_span_fragments_table
        )
    }

    fn event_measures_table_name(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_event_measures_table
        )
    }

    fn entity_state_updates_table_name(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_entity_state_updates_table
        )
    }

    fn definitions_table_name(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_definitions_table
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

        if count_ndjson_rows(&processed) == 0 {
            warn!(
                bucket,
                key,
                bytes = processed.len(),
                "skipping object without complete rows"
            );
            return Ok(());
        }

        let capabilities = builtin_field_capabilities();
        let definitions = self.active_definitions().await.unwrap_or_else(|err| {
            warn!(error = %err, "failed to load dynamic definitions; continuing with built-ins");
            ExtractionDefinitions::default()
        });
        let derived = derived_buffers(&processed, &capabilities, &definitions)
            .context("derive event rows")?;
        let token_prefix = insert_token_prefix(bucket, key);

        self.insert_clickhouse(&self.cfg.table_name(), &processed, &token_prefix, false)
            .await?;

        let field_index_table = self.cfg.field_index_table_name();
        let span_fragments_table = self.cfg.span_fragments_table_name();
        let event_measures_table = self.cfg.event_measures_table_name();
        let entity_state_updates_table = self.cfg.entity_state_updates_table_name();
        let field_index_token_prefix = format!("{token_prefix}:field_index");
        let span_fragments_token_prefix = format!("{token_prefix}:span_fragments");
        let event_measures_token_prefix = format!("{token_prefix}:event_measures");
        let entity_state_updates_token_prefix = format!("{token_prefix}:entity_state_updates");

        tokio::try_join!(
            self.insert_clickhouse(
                &field_index_table,
                &derived.field_index,
                &field_index_token_prefix,
                true,
            ),
            self.insert_clickhouse(
                &span_fragments_table,
                &derived.span_fragments,
                &span_fragments_token_prefix,
                true,
            ),
            self.insert_clickhouse(
                &event_measures_table,
                &derived.event_measures,
                &event_measures_token_prefix,
                true,
            ),
            self.insert_clickhouse(
                &entity_state_updates_table,
                &derived.entity_state_updates,
                &entity_state_updates_token_prefix,
                true,
            ),
        )?;

        info!(
            bucket,
            key,
            rows = derived.rows,
            bytes = processed.len(),
            field_index_bytes = derived.field_index.len(),
            span_fragment_bytes = derived.span_fragments.len(),
            event_measure_bytes = derived.event_measures.len(),
            entity_state_update_bytes = derived.entity_state_updates.len(),
            lookup_keys = capabilities.lookup.len(),
            aggregate_keys = capabilities.aggregate.len(),
            dynamic_fields = definitions.fields.len(),
            dynamic_measures = definitions.measures.len(),
            dynamic_states = definitions.states.len(),
            "loaded event object"
        );
        Ok(())
    }

    async fn active_definitions(&self) -> Result<ExtractionDefinitions> {
        let query = format!(
            "SELECT tenant_id, definition_id, name, kind, mode, config, version FROM {} FINAL WHERE enabled = 1 AND isNull(deleted_at) AND kind IN ('field', 'measure', 'rollup', 'state') FORMAT JSON",
            self.cfg.definitions_table_name()
        );
        let body = self.query_clickhouse(&query).await?;
        let response: ClickHouseJson<DefinitionRecord> =
            serde_json::from_str(&body).context("parse definitions response")?;
        Ok(ExtractionDefinitions::from_records(response.data))
    }

    async fn query_clickhouse(&self, query: &str) -> Result<String> {
        let mut request = self
            .http
            .get(&self.cfg.clickhouse_url)
            .timeout(self.cfg.request_timeout)
            .query(&[
                ("database", self.cfg.clickhouse_database.as_str()),
                ("query", query),
                ("date_time_input_format", "best_effort"),
            ]);

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

    async fn insert_clickhouse(
        &self,
        table: &str,
        body: &[u8],
        token_prefix: &str,
        async_insert: bool,
    ) -> Result<()> {
        if body.is_empty() {
            return Ok(());
        }

        for (chunk_index, chunk) in ndjson_chunks(
            body,
            self.cfg.clickhouse_insert_max_rows,
            self.cfg.clickhouse_insert_max_bytes,
        )
        .into_iter()
        .enumerate()
        {
            self.insert_clickhouse_chunk(table, chunk, token_prefix, chunk_index, async_insert)
                .await?;
        }

        Ok(())
    }

    async fn insert_clickhouse_chunk(
        &self,
        table: &str,
        body: &[u8],
        token_prefix: &str,
        chunk_index: usize,
        async_insert: bool,
    ) -> Result<()> {
        let query = format!("INSERT INTO {table} FORMAT JSONEachRow");
        let mut request = self
            .http
            .post(&self.cfg.clickhouse_url)
            .timeout(self.cfg.request_timeout)
            .query(&[
                ("database", self.cfg.clickhouse_database.as_str()),
                ("query", query.as_str()),
                ("date_time_input_format", "best_effort"),
                ("type_json_skip_duplicated_paths", "1"),
                ("insert_deduplicate", "1"),
            ])
            .body(body.to_vec());

        let dedupe_token = insert_deduplication_token(token_prefix, table, chunk_index);
        request = request.query(&[("insert_deduplication_token", dedupe_token.as_str())]);
        if async_insert {
            request = request.query(&[
                ("async_insert", "1"),
                ("wait_for_async_insert", "1"),
                ("async_insert_busy_timeout_ms", "1000"),
            ]);
        }

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

fn derived_buffers(
    bytes: &[u8],
    capabilities: &FieldCapabilities,
    definitions: &ExtractionDefinitions,
) -> Result<DerivedBuffers> {
    let mut buffers = DerivedBuffers::default();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        buffers.rows += 1;
        let value: Value =
            serde_json::from_slice(line).context("parse event row for derivation")?;
        let Some(row) = value.as_object() else {
            continue;
        };

        for field_row in field_index_rows(row, capabilities, definitions) {
            serde_json::to_writer(&mut buffers.field_index, &field_row)
                .context("serialize field index row")?;
            buffers.field_index.push(b'\n');
        }

        if let Some(span_row) = span_fragment_row(row) {
            serde_json::to_writer(&mut buffers.span_fragments, &span_row)
                .context("serialize span fragment row")?;
            buffers.span_fragments.push(b'\n');
        }

        for measure in event_measure_rows(row, definitions) {
            serde_json::to_writer(&mut buffers.event_measures, &measure)
                .context("serialize event measure row")?;
            buffers.event_measures.push(b'\n');
        }

        for state_update in entity_state_update_rows(row, definitions) {
            serde_json::to_writer(&mut buffers.entity_state_updates, &state_update)
                .context("serialize entity state update row")?;
            buffers.entity_state_updates.push(b'\n');
        }
    }
    Ok(buffers)
}

#[cfg(test)]
fn event_index_ndjson(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let row: Value = serde_json::from_slice(line).context("parse event row for event index")?;
        let Some(row) = row.as_object() else {
            continue;
        };
        let Some(index_row) = event_index_row(row) else {
            continue;
        };
        serde_json::to_writer(&mut out, &index_row).context("serialize event index row")?;
        out.push(b'\n');
    }
    Ok(out)
}

fn span_fragment_row(row: &Map<String, Value>) -> Option<SpanFragmentRow> {
    let timestamp = string_value(row.get("timestamp"));
    if timestamp.is_empty() {
        return None;
    }
    let data = row.get("data").and_then(Value::as_object)?;
    let context = EventIndexContext::from_row(row, data, timestamp.clone());
    if context.trace_id.is_empty() || context.span_id.is_empty() {
        return None;
    }

    let event_type = context.event_type.as_str();
    let start_time = optional_time(data.get("start_time"))
        .or_else(|| optional_time(data.get("span_start_time")))
        .or_else(|| optional_time(data.get("startedAt")));
    let end_time = optional_time(data.get("end_time"))
        .or_else(|| optional_time(data.get("span_end_time")))
        .or_else(|| optional_time(data.get("endedAt")));
    let duration_ms = context
        .duration_ms
        .or_else(|| optional_number_value(data.get("durationMs")))
        .or_else(|| optional_number_value(data.get("elapsed_ms")))
        .or_else(|| optional_number_value(data.get("latency_ms")));

    let is_lifecycle_span = matches!(event_type, "span_start" | "span_end" | "span");
    let is_operation_span = context.signal == "trace"
        || (duration_ms.is_some()
            && !matches!(
                event_type,
                "log" | "metric" | "analytics" | "track" | "page" | "screen"
            ));
    if !is_lifecycle_span && !is_operation_span {
        return None;
    }

    let normalized_start_time = match event_type {
        "span_end" => start_time,
        _ => start_time.or_else(|| Some(timestamp.clone())),
    };
    let normalized_end_time = match event_type {
        "span_start" => end_time,
        _ => end_time.or_else(|| {
            if duration_ms.is_some() {
                None
            } else {
                Some(timestamp.clone())
            }
        }),
    };

    Some(SpanFragmentRow {
        tenant_id: context.tenant_id.clone(),
        trace_id: context.trace_id.clone(),
        span_id: context.span_id.clone(),
        parent_span_id: context.parent_span_id.clone(),
        event_id: context.event_id.clone(),
        event_type: context.event_type.clone(),
        signal: context.signal.clone(),
        service: string_value(data.get("service")),
        environment: string_value(data.get("environment")),
        name: context.name.clone(),
        start_time: normalized_start_time,
        end_time: normalized_end_time,
        duration_ms,
        is_error: u8::from(is_error_row(row)),
        timestamp,
        source_file: string_value(row.get("source_file")),
        source_offset: u64_value(row.get("source_offset")),
        source_length: u32_value(row.get("source_length")),
        data: Value::Object(data.clone()),
    })
}

#[cfg(test)]
fn event_index_row(row: &Map<String, Value>) -> Option<EventIndexRow> {
    let timestamp = string_value(row.get("timestamp"));
    if timestamp.is_empty() {
        return None;
    }
    let data = row.get("data").and_then(Value::as_object)?;
    let context = EventIndexContext::from_row(row, data, timestamp);
    let service = string_value(data.get("service"));
    let environment = string_value(data.get("environment"));
    let title = context
        .name
        .clone()
        .into_non_empty()
        .or_else(|| string_value(data.get("metric_name")).into_non_empty())
        .or_else(|| string_value(data.get("body")).into_non_empty())
        .unwrap_or_else(|| context.event_type.clone());
    let correlation_id = context
        .trace_id
        .clone()
        .into_non_empty()
        .or_else(|| string_value(data.get("request_id")).into_non_empty())
        .or_else(|| context.span_id.clone().into_non_empty())
        .or_else(|| string_value(data.get("session_id")).into_non_empty())
        .or_else(|| string_value(data.get("account_id")).into_non_empty())
        .or_else(|| string_value(data.get("user_id")).into_non_empty())
        .unwrap_or_default();
    let parent_id = context
        .parent_span_id
        .clone()
        .into_non_empty()
        .unwrap_or_default();

    Some(EventIndexRow {
        tenant_id: context.tenant_id.clone(),
        timestamp: context.timestamp.clone(),
        bucket_time: context.bucket_time.clone(),
        event_id: context.event_id.clone(),
        event_type: context.event_type.clone(),
        signal: context.signal.clone(),
        service,
        environment,
        name: context.name.clone(),
        title,
        is_error: u8::from(is_error_row(row)),
        correlation_id,
        parent_id,
        trace_id: context.trace_id.clone(),
        span_id: context.span_id.clone(),
        parent_span_id: context.parent_span_id.clone(),
        start_time: context.start_time.clone(),
        end_time: context.end_time.clone(),
        duration_ms: context.duration_ms,
        source_file: string_value(row.get("source_file")),
        source_offset: u64_value(row.get("source_offset")),
        source_length: u32_value(row.get("source_length")),
        data: Value::Object(data.clone()),
    })
}

#[cfg(test)]
fn field_index_ndjson(
    bytes: &[u8],
    capabilities: &FieldCapabilities,
    definitions: &ExtractionDefinitions,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let row: Value = serde_json::from_slice(line).context("parse event row for field index")?;
        let Some(row) = row.as_object() else {
            continue;
        };
        for field_row in field_index_rows(row, capabilities, definitions) {
            serde_json::to_writer(&mut out, &field_row).context("serialize field index row")?;
            out.push(b'\n');
        }
    }
    Ok(out)
}

fn field_index_rows(
    row: &Map<String, Value>,
    capabilities: &FieldCapabilities,
    definitions: &ExtractionDefinitions,
) -> Vec<FieldIndexRow> {
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
    let aggregate = capabilities
        .aggregate
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();

    for key in &capabilities.aggregate {
        if let Some(value) = indexed_value_for_key(row, data, &context, key) {
            collect_field_index_rows(
                &context,
                row,
                BuiltFieldIndex {
                    mode: "facet",
                    field_name: key,
                    value_type: None,
                    definition_id: "",
                    definition_version: 0,
                },
                &value,
                &mut seen,
                &mut rows,
            );
        }
    }
    for key in &capabilities.lookup {
        if aggregate.contains(key) {
            continue;
        }
        if let Some(value) = indexed_value_for_key(row, data, &context, key) {
            collect_field_index_rows(
                &context,
                row,
                BuiltFieldIndex {
                    mode: "lookup",
                    field_name: key,
                    value_type: None,
                    definition_id: "",
                    definition_version: 0,
                },
                &value,
                &mut seen,
                &mut rows,
            );
        }
    }
    for definition in &definitions.fields {
        if definition.tenant_id != context.tenant_id {
            continue;
        }
        if let Some(value) = value_at_path(data, &definition.path).cloned() {
            collect_field_index_rows(
                &context,
                row,
                BuiltFieldIndex {
                    mode: &definition.mode,
                    field_name: &definition.name,
                    value_type: Some(&definition.value_type),
                    definition_id: &definition.definition_id,
                    definition_version: definition.definition_version,
                },
                &value,
                &mut seen,
                &mut rows,
            );
        }
    }
    rows
}

#[derive(Clone, Copy)]
struct BuiltFieldIndex<'a> {
    mode: &'a str,
    field_name: &'a str,
    value_type: Option<&'a str>,
    definition_id: &'a str,
    definition_version: u64,
}

fn collect_field_index_rows(
    context: &EventIndexContext,
    row: &Map<String, Value>,
    field: BuiltFieldIndex<'_>,
    value: &Value,
    seen: &mut BTreeSet<String>,
    rows: &mut Vec<FieldIndexRow>,
) {
    match value {
        Value::Null => {}
        Value::Bool(value) => push_field_index_row(
            context,
            row,
            &field,
            if *value { "true" } else { "false" }.to_string(),
            field.value_type.unwrap_or("bool"),
            seen,
            rows,
        ),
        Value::Number(value) => push_field_index_row(
            context,
            row,
            &field,
            value.to_string(),
            field.value_type.unwrap_or("number"),
            seen,
            rows,
        ),
        Value::String(value) => push_field_index_row(
            context,
            row,
            &field,
            value.clone(),
            field.value_type.unwrap_or("string"),
            seen,
            rows,
        ),
        Value::Array(values) => {
            for value in values {
                collect_field_index_rows(
                    context,
                    row,
                    BuiltFieldIndex { ..field },
                    value,
                    seen,
                    rows,
                );
            }
        }
        Value::Object(_) => {}
    }
}

fn push_field_index_row(
    context: &EventIndexContext,
    row: &Map<String, Value>,
    field: &BuiltFieldIndex<'_>,
    value: String,
    value_type: &str,
    seen: &mut BTreeSet<String>,
    rows: &mut Vec<FieldIndexRow>,
) {
    if field.field_name.is_empty() || value.is_empty() {
        return;
    }
    let dedupe_key = format!(
        "{}\u{0}{}\u{0}{value_type}\u{0}{value}",
        field.mode, field.field_name
    );
    if !seen.insert(dedupe_key) {
        return;
    }
    rows.push(FieldIndexRow {
        tenant_id: context.tenant_id.clone(),
        mode: field.mode.to_string(),
        field_name: field.field_name.to_string(),
        value,
        value_type: value_type.to_string(),
        timestamp: context.timestamp.clone(),
        bucket_time: context.bucket_time.clone(),
        event_id: context.event_id.clone(),
        event_type: context.event_type.clone(),
        signal: context.signal.clone(),
        is_error: u8::from(is_error_row(row)),
        trace_id: context.trace_id.clone(),
        span_id: context.span_id.clone(),
        parent_span_id: context.parent_span_id.clone(),
        name: context.name.clone(),
        start_time: context.start_time.clone(),
        end_time: context.end_time.clone(),
        duration_ms: context.duration_ms,
        definition_id: field.definition_id.to_string(),
        definition_version: field.definition_version,
    });
}

#[cfg(test)]
fn event_measures_ndjson(bytes: &[u8], definitions: &ExtractionDefinitions) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let row: Value = serde_json::from_slice(line).context("parse event row for measures")?;
        let Some(row) = row.as_object() else {
            continue;
        };
        for measure in event_measure_rows(row, definitions) {
            serde_json::to_writer(&mut out, &measure).context("serialize event measure row")?;
            out.push(b'\n');
        }
    }
    Ok(out)
}

fn event_measure_rows(
    row: &Map<String, Value>,
    definitions: &ExtractionDefinitions,
) -> Vec<EventMeasureRow> {
    let timestamp = string_value(row.get("timestamp"));
    if timestamp.is_empty() {
        return Vec::new();
    }
    let Some(data) = row.get("data").and_then(Value::as_object) else {
        return Vec::new();
    };
    let context = EventIndexContext::from_row(row, data, timestamp);
    let service = string_value(data.get("service"));
    let metric_unit = string_value(data.get("metric_unit"));
    let mut rows = Vec::new();
    for (name, unit) in [
        ("duration_ms", "ms"),
        ("metric_value", metric_unit.as_str()),
        ("revenue", ""),
        ("price", ""),
        ("quantity", ""),
        ("count", ""),
        ("sum", ""),
        ("metric_count", ""),
        ("metric_sum", ""),
        ("metric_min", ""),
        ("metric_max", ""),
    ] {
        let Some(value) = optional_number_value(value_at_path(data, name)) else {
            continue;
        };
        rows.push(EventMeasureRow {
            tenant_id: context.tenant_id.clone(),
            definition_id: String::new(),
            definition_version: 0,
            measure_name: name.to_string(),
            value,
            unit: unit.to_string(),
            timestamp: context.timestamp.clone(),
            bucket_time: context.bucket_time.clone(),
            bucket_seconds: 300,
            event_id: context.event_id.clone(),
            event_type: context.event_type.clone(),
            signal: context.signal.clone(),
            dimension_name: if service.is_empty() {
                String::new()
            } else {
                "service".to_string()
            },
            dimension_value: service.clone(),
        });
    }
    for definition in &definitions.measures {
        if definition.tenant_id != context.tenant_id {
            continue;
        }
        let Some(value) = optional_number_value(value_at_path(data, &definition.path)) else {
            continue;
        };
        let dimension_value = if definition.dimension.is_empty() {
            String::new()
        } else {
            string_value(value_at_path(data, &definition.dimension))
        };
        rows.push(EventMeasureRow {
            tenant_id: context.tenant_id.clone(),
            definition_id: definition.definition_id.clone(),
            definition_version: definition.definition_version,
            measure_name: definition.name.clone(),
            value,
            unit: definition.unit.clone(),
            timestamp: context.timestamp.clone(),
            bucket_time: context.bucket_time.clone(),
            bucket_seconds: 300,
            event_id: context.event_id.clone(),
            event_type: context.event_type.clone(),
            signal: context.signal.clone(),
            dimension_name: definition.dimension.clone(),
            dimension_value,
        });
    }
    rows
}

#[cfg(test)]
fn entity_state_updates_ndjson(
    bytes: &[u8],
    definitions: &ExtractionDefinitions,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let row: Value =
            serde_json::from_slice(line).context("parse event row for entity state updates")?;
        let Some(row) = row.as_object() else {
            continue;
        };
        for state_update in entity_state_update_rows(row, definitions) {
            serde_json::to_writer(&mut out, &state_update)
                .context("serialize entity state update row")?;
            out.push(b'\n');
        }
    }
    Ok(out)
}

fn entity_state_update_rows(
    row: &Map<String, Value>,
    definitions: &ExtractionDefinitions,
) -> Vec<EntityStateUpdateRow> {
    let timestamp = string_value(row.get("timestamp"));
    if timestamp.is_empty() {
        return Vec::new();
    }
    let Some(data) = row.get("data").and_then(Value::as_object) else {
        return Vec::new();
    };
    let context = EventIndexContext::from_row(row, data, timestamp);
    let mut rows = Vec::new();
    for definition in &definitions.states {
        if definition.tenant_id != context.tenant_id {
            continue;
        }
        let entity_id = string_value(value_at_path(data, &definition.entity_id_path));
        let value = string_value(value_at_path(data, &definition.path));
        if entity_id.is_empty() || value.is_empty() {
            continue;
        }
        rows.push(EntityStateUpdateRow {
            tenant_id: context.tenant_id.clone(),
            definition_id: definition.definition_id.clone(),
            definition_version: definition.definition_version,
            entity_type: definition.entity_type.clone(),
            entity_id,
            state_name: definition.name.clone(),
            value,
            value_type: definition.value_type.clone(),
            timestamp: context.timestamp.clone(),
            event_id: context.event_id.clone(),
            event_type: context.event_type.clone(),
            signal: context.signal.clone(),
        });
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
        let signal = string_value(data.get("signal"))
            .into_non_empty()
            .unwrap_or_else(|| signal_for_event_type(&event_type).to_string());
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

trait NonEmptyString {
    fn into_non_empty(self) -> Option<String>;
}

impl NonEmptyString for String {
    fn into_non_empty(self) -> Option<String> {
        if self.is_empty() { None } else { Some(self) }
    }
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

fn config_string(config: &Value, key: &str) -> String {
    config
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn config_string_default(config: &Value, key: &str, fallback: &str) -> String {
    let value = config_string(config, key);
    if value.is_empty() {
        fallback.to_string()
    } else {
        value
    }
}

fn optional_time(value: Option<&Value>) -> Option<String> {
    string_value(value).into_non_empty()
}

fn u64_value(value: Option<&Value>) -> u64 {
    match value {
        Some(Value::Number(value)) => value.as_u64().unwrap_or_default(),
        Some(Value::String(value)) => value.parse().unwrap_or_default(),
        _ => 0,
    }
}

fn u32_value(value: Option<&Value>) -> u32 {
    u64_value(value).try_into().unwrap_or_default()
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

fn ndjson_chunks(bytes: &[u8], max_rows: usize, max_bytes: usize) -> Vec<&[u8]> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let max_rows = max_rows.max(1);
    let max_bytes = max_bytes.max(1);
    let mut chunks = Vec::new();
    let mut start = 0;
    let mut rows = 0;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        rows += 1;
        let end = index + 1;
        if rows >= max_rows || end - start >= max_bytes {
            chunks.push(&bytes[start..end]);
            start = end;
            rows = 0;
        }
    }
    if start < bytes.len() {
        chunks.push(&bytes[start..]);
    }
    chunks
}

fn insert_token_prefix(bucket: &str, key: &str) -> String {
    format!("s3://{bucket}/{key}")
}

fn insert_deduplication_token(prefix: &str, table: &str, chunk_index: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prefix.as_bytes());
    hasher.update(b"\0");
    hasher.update(table.as_bytes());
    hasher.update(b"\0");
    hasher.update(chunk_index.to_string().as_bytes());
    format!("{:x}", hasher.finalize())
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
    use super::{
        ExtractionDefinitions, FieldDefinition, MeasureDefinition, StateDefinition,
        builtin_field_capabilities, count_ndjson_rows, entity_state_updates_ndjson,
        event_index_ndjson, event_measures_ndjson, field_index_ndjson, parse_s3_records,
    };
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
    fn generates_event_index_with_raw_source_pointer() {
        let index = event_index_ndjson(
            br#"{"event_id":"evt_1","timestamp":"2026-05-12T00:00:00.000Z","source_file":"events/part.ndjson","source_offset":42,"source_length":321,"data":{"tenant_id":"tenant-a","event_type":"span_start","service":"api","environment":"prod","trace_id":"trace-1","span_id":"span-1","parent_span_id":"root","name":"GET /users","duration_ms":12.5}}"#,
        )
        .expect("generate event index");
        let row: Value =
            serde_json::from_slice(index.split(|byte| *byte == b'\n').next().unwrap()).unwrap();

        assert_eq!(row["event_id"], "evt_1");
        assert_eq!(row["service"], "api");
        assert_eq!(row["correlation_id"], "trace-1");
        assert_eq!(row["source_file"], "events/part.ndjson");
        assert_eq!(row["source_offset"], 42);
        assert_eq!(row["source_length"], 321);
    }

    #[test]
    fn generates_field_index_for_facet_and_lookup_modes() {
        let index = field_index_ndjson(
            br#"{"event_id":"evt_1","timestamp":"2026-05-12T00:00:00.000Z","data":{"tenant_id":"tenant-a","event_type":"span_start","service":"api","trace_id":"trace-1","span_id":"span-1","name":"GET /users","http":{"route":"/users/:id","method":"GET"}}}"#,
            &builtin_field_capabilities(),
            &ExtractionDefinitions::default(),
        )
        .expect("generate field index");
        let rows = String::from_utf8(index).expect("utf8");
        let parsed: Vec<Value> = rows
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert!(parsed.iter().any(|row| row["mode"] == "facet"
            && row["field_name"] == "service"
            && row["value"] == "api"));
        assert!(parsed.iter().any(|row| row["mode"] == "lookup"
            && row["field_name"] == "trace_id"
            && row["value"] == "trace-1"));
        assert!(!parsed.iter().any(|row| row["field_name"] == "ignored"));
    }

    #[test]
    fn applies_dynamic_field_and_measure_definitions() {
        let definitions = ExtractionDefinitions {
            fields: vec![FieldDefinition {
                tenant_id: "tenant-a".to_string(),
                definition_id: "def_plan".to_string(),
                definition_version: 7,
                name: "plan".to_string(),
                mode: "facet".to_string(),
                path: "account.plan".to_string(),
                value_type: "string".to_string(),
            }],
            measures: vec![MeasureDefinition {
                tenant_id: "tenant-a".to_string(),
                definition_id: "def_gpu".to_string(),
                definition_version: 8,
                name: "gpu_ms".to_string(),
                path: "gpu.ms".to_string(),
                unit: "ms".to_string(),
                dimension: "service".to_string(),
            }],
            states: vec![StateDefinition {
                tenant_id: "tenant-a".to_string(),
                definition_id: "def_account_plan".to_string(),
                definition_version: 9,
                name: "account.plan".to_string(),
                path: "account.plan".to_string(),
                value_type: "string".to_string(),
                entity_type: "account".to_string(),
                entity_id_path: "account.id".to_string(),
            }],
        };
        let event = br#"{"event_id":"evt_1","timestamp":"2026-05-12T00:00:00.000Z","data":{"tenant_id":"tenant-a","event_type":"inference_complete","service":"worker","account":{"id":"acct_1","plan":"pro"},"gpu":{"ms":42.5}}}"#;

        let fields =
            field_index_ndjson(event, &builtin_field_capabilities(), &definitions).unwrap();
        let field_rows: Vec<Value> = String::from_utf8(fields)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert!(
            field_rows
                .iter()
                .any(|row| row["definition_id"] == "def_plan"
                    && row["field_name"] == "plan"
                    && row["value"] == "pro")
        );

        let measures = event_measures_ndjson(event, &definitions).unwrap();
        let measure_rows: Vec<Value> = String::from_utf8(measures)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert!(
            measure_rows
                .iter()
                .any(|row| row["definition_id"] == "def_gpu"
                    && row["measure_name"] == "gpu_ms"
                    && row["value"] == 42.5
                    && row["dimension_value"] == "worker")
        );

        let state_updates = entity_state_updates_ndjson(event, &definitions).unwrap();
        let state_rows: Vec<Value> = String::from_utf8(state_updates)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert!(
            state_rows
                .iter()
                .any(|row| row["definition_id"] == "def_account_plan"
                    && row["entity_type"] == "account"
                    && row["entity_id"] == "acct_1"
                    && row["state_name"] == "account.plan"
                    && row["value"] == "pro")
        );
    }
}
