use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    env,
    time::Duration,
};

use chrono::Utc;
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    message::{Header, Headers, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
};
use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

pub const DEFAULT_INGEST_TOPIC: &str = "events.ingest.v1";
pub const DEFAULT_NORMALIZED_TOPIC: &str = "events.normalized.v1";
pub const DEFAULT_TABLEFLOW_TOPIC: &str = "events.tableflow.batches.v1";
pub const DEFAULT_INVALID_TOPIC: &str = "events.invalid.v1";

pub const HEADER_TENANT_ID: &str = "nanotrace-tenant-id";
pub const HEADER_ORGANIZATION_ID: &str = "nanotrace-organization-id";
pub const HEADER_RECEIVED_AT: &str = "nanotrace-received-at";
pub const HEADER_CONTENT_TYPE: &str = "content-type";
pub const HEADER_SCHEMA_VERSION: &str = "nanotrace-schema-version";
pub const DEFAULT_MAX_EVENT_KV_INDEX_ROWS: usize = 2048;
pub const DEFAULT_MAX_EVENT_KV_STRING_BYTES: usize = 1024;
pub const DEFAULT_MAX_EVENT_SEARCH_TEXT_BYTES: usize = 32 * 1024;
pub const DEFAULT_MAX_EVENT_SEARCH_TERM_ROWS: usize = 512;

#[derive(Clone)]
pub struct RawBatchProducer {
    producer: FutureProducer,
    topic: String,
    timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct RawBatchProducerConfig {
    pub brokers: String,
    pub topic: String,
    pub client_id: String,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProducedBatch {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] KafkaError),
    #[error("Kafka produce failed: {0}")]
    Produce(String),
}

impl RawBatchProducer {
    pub fn new(cfg: RawBatchProducerConfig) -> Result<Self, IngestError> {
        let mut client_cfg = ClientConfig::new();
        client_cfg
            .set("bootstrap.servers", &cfg.brokers)
            .set("client.id", &cfg.client_id)
            .set("message.timeout.ms", cfg.timeout.as_millis().to_string())
            .set("queue.buffering.max.ms", "10")
            .set("compression.type", "zstd");
        apply_kafka_env_config(&mut client_cfg);
        let producer = client_cfg.create()?;
        Ok(Self {
            producer,
            topic: cfg.topic,
            timeout: cfg.timeout,
        })
    }

    pub async fn produce_raw_batch(
        &self,
        tenant_id: &str,
        organization_id: &str,
        content_type: &str,
        body: &[u8],
    ) -> Result<ProducedBatch, IngestError> {
        let received_at = Utc::now().to_rfc3339();
        let key = partition_key(tenant_id, body);
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: HEADER_TENANT_ID,
                value: Some(tenant_id),
            })
            .insert(Header {
                key: HEADER_ORGANIZATION_ID,
                value: Some(organization_id),
            })
            .insert(Header {
                key: HEADER_RECEIVED_AT,
                value: Some(received_at.as_str()),
            })
            .insert(Header {
                key: HEADER_CONTENT_TYPE,
                value: Some(content_type),
            })
            .insert(Header {
                key: HEADER_SCHEMA_VERSION,
                value: Some("1"),
            });
        let record = FutureRecord::to(&self.topic)
            .key(&key)
            .payload(body)
            .headers(headers);
        let (partition, offset) = self
            .producer
            .send(record, self.timeout)
            .await
            .map_err(|(err, _)| IngestError::Produce(err.to_string()))?;
        Ok(ProducedBatch {
            topic: self.topic.clone(),
            partition,
            offset,
        })
    }
}

fn partition_key(tenant_id: &str, body: &[u8]) -> String {
    let mut hash: u64 = 14_695_981_039_346_656_037;
    for byte in body.iter().take(4096) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    format!("{tenant_id}:{:016x}", hash)
}

pub fn consumer(
    brokers: &str,
    group_id: &str,
    client_id: &str,
) -> Result<StreamConsumer, KafkaError> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("client.id", client_id)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "45000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");
    apply_kafka_env_config(&mut cfg);
    cfg.create()
}

pub fn producer(brokers: &str, client_id: &str) -> Result<FutureProducer, KafkaError> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers)
        .set("client.id", client_id)
        .set("message.timeout.ms", "30000")
        .set("queue.buffering.max.ms", "10")
        .set("compression.type", "zstd");
    apply_kafka_env_config(&mut cfg);
    cfg.create()
}

fn apply_kafka_env_config(cfg: &mut ClientConfig) {
    set_optional_kafka_config(
        cfg,
        "security.protocol",
        &[
            "NANOTRACE_KAFKA_SECURITY_PROTOCOL",
            "KAFKA_SECURITY_PROTOCOL",
        ],
    );
    set_optional_kafka_config(
        cfg,
        "sasl.mechanism",
        &["NANOTRACE_KAFKA_SASL_MECHANISM", "KAFKA_SASL_MECHANISM"],
    );
    set_optional_kafka_config(
        cfg,
        "sasl.username",
        &["NANOTRACE_KAFKA_SASL_USERNAME", "KAFKA_SASL_USERNAME"],
    );
    set_optional_kafka_config(
        cfg,
        "sasl.password",
        &["NANOTRACE_KAFKA_SASL_PASSWORD", "KAFKA_SASL_PASSWORD"],
    );
}

fn set_optional_kafka_config(cfg: &mut ClientConfig, key: &str, env_keys: &[&str]) {
    for env_key in env_keys {
        if let Ok(value) = env::var(env_key) {
            let value = value.trim();
            if !value.is_empty() {
                cfg.set(key, value);
                return;
            }
        }
    }
}

pub fn subscribe(consumer: &StreamConsumer, topic: &str) -> Result<(), KafkaError> {
    consumer.subscribe(&[topic])
}

pub fn header_value(message: &rdkafka::message::BorrowedMessage<'_>, key: &str) -> Option<String> {
    message.headers().and_then(|headers| {
        for index in 0..headers.count() {
            let header = headers.get(index);
            if header.key == key {
                return header.value.and_then(|value| {
                    std::str::from_utf8(value)
                        .ok()
                        .map(std::borrow::ToOwned::to_owned)
                });
            }
        }
        None
    })
}

#[derive(Debug, Default)]
pub struct NormalizedBatch {
    pub normalized: Vec<u8>,
    pub invalid: Vec<u8>,
    pub managed_definitions: Vec<ManagedDefinitionSpec>,
    pub valid_rows: usize,
    pub invalid_rows: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ManagedDefinitionSpec {
    pub definition_id: String,
    pub name: String,
    pub kind: String,
    pub mode: String,
    pub config: Value,
    pub capabilities: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventKvIndexRow {
    pub tenant_id: String,
    pub timestamp: String,
    pub event_id: String,
    pub event_type: String,
    pub signal: String,
    pub path: String,
    pub value_type: String,
    pub string_value: String,
    pub number_value: Option<f64>,
    pub bool_value: Option<u8>,
    pub scope_path: String,
    pub scope_index: i32,
    pub source_file: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventTextIndexRow {
    pub tenant_id: String,
    pub timestamp: String,
    pub event_id: String,
    pub event_type: String,
    pub signal: String,
    pub trace_id: String,
    pub span_id: String,
    pub text: String,
    pub source_file: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventSearchTermRow {
    pub tenant_id: String,
    pub timestamp: String,
    pub event_id: String,
    pub event_type: String,
    pub signal: String,
    pub term: String,
    pub path: String,
    pub weight: u16,
    pub source_file: String,
}

#[derive(Debug, Serialize)]
struct InvalidEventRow<'a> {
    tenant_id: &'a str,
    observed_at: &'a str,
    reason: String,
    raw: String,
}

pub fn normalize_json_batch(
    body: &[u8],
    tenant_id: &str,
    organization_id: &str,
    source_file: &str,
    received_at: &str,
    max_event_bytes: usize,
) -> NormalizedBatch {
    let parsed = serde_json::from_slice::<Value>(body);
    let values = match parsed {
        Ok(Value::Array(values)) if !values.is_empty() => values,
        Ok(Value::Array(_)) => {
            return invalid_batch(
                tenant_id,
                received_at,
                "batch must contain at least one event",
                body,
            );
        }
        Ok(value) => vec![value],
        Err(err) => {
            return invalid_batch(
                tenant_id,
                received_at,
                &format!("invalid JSON body: {err}"),
                body,
            );
        }
    };

    let mut batch = NormalizedBatch::default();
    let mut seen_definitions = HashSet::new();
    let mut source_offset = 0u64;
    for value in values {
        match normalize_event(
            value,
            tenant_id,
            organization_id,
            source_file,
            source_offset,
            max_event_bytes,
        ) {
            Ok((line, definitions)) => {
                source_offset += line.len() as u64;
                batch.valid_rows += 1;
                batch.normalized.extend_from_slice(&line);
                for definition in definitions {
                    if seen_definitions.insert(definition.definition_id.clone()) {
                        batch.managed_definitions.push(definition);
                    }
                }
            }
            Err((reason, raw)) => {
                batch.invalid_rows += 1;
                append_invalid(
                    &mut batch.invalid,
                    tenant_id,
                    received_at,
                    reason,
                    raw.as_bytes(),
                );
            }
        }
    }
    batch
}

fn invalid_batch(tenant_id: &str, observed_at: &str, reason: &str, raw: &[u8]) -> NormalizedBatch {
    let mut batch = NormalizedBatch {
        invalid_rows: 1,
        ..NormalizedBatch::default()
    };
    append_invalid(
        &mut batch.invalid,
        tenant_id,
        observed_at,
        reason.to_string(),
        raw,
    );
    batch
}

fn normalize_event(
    value: Value,
    tenant_id: &str,
    organization_id: &str,
    source_file: &str,
    source_offset: u64,
    max_event_bytes: usize,
) -> Result<(Vec<u8>, Vec<ManagedDefinitionSpec>), (String, String)> {
    let raw_for_error = value.to_string();
    let Some(object) = value.as_object() else {
        return Err(("event must be a JSON object".to_string(), raw_for_error));
    };
    let Some(event_id) = string_field(object, "event_id") else {
        return Err((
            "event_id must be a non-empty string".to_string(),
            raw_for_error,
        ));
    };
    let Some(timestamp) = string_field(object, "timestamp") else {
        return Err((
            "timestamp must be a non-empty string".to_string(),
            raw_for_error,
        ));
    };
    let observed_timestamp = optional_string_field(object, "observed_timestamp")
        .map(|value| Value::String(value.to_string()));
    let Some(Value::Object(data)) = object.get("data") else {
        return Err(("data must be a JSON object".to_string(), raw_for_error));
    };

    let mut data = data.clone();
    data.insert(
        "tenant_id".to_string(),
        Value::String(tenant_id.to_string()),
    );
    data.insert(
        "organization_id".to_string(),
        Value::String(organization_id.to_string()),
    );
    let managed_definitions = managed_definition_specs(&data);

    let mut row = Map::new();
    row.insert(
        "tenant_id".to_string(),
        Value::String(tenant_id.to_string()),
    );
    row.insert("event_id".to_string(), Value::String(event_id.to_string()));
    row.insert(
        "timestamp".to_string(),
        Value::String(timestamp.to_string()),
    );
    if let Some(observed_timestamp) = observed_timestamp {
        row.insert("observed_timestamp".to_string(), observed_timestamp);
    }
    row.insert(
        "source_file".to_string(),
        Value::String(source_file.to_string()),
    );
    row.insert("source_offset".to_string(), source_offset.into());
    row.insert("source_length".to_string(), 0.into());
    row.insert("data".to_string(), Value::Object(data));

    let line =
        serialize_with_source_length(&mut row).map_err(|err| (err, raw_for_error.clone()))?;
    if line.len() > max_event_bytes {
        return Err(("event is too large".to_string(), raw_for_error));
    }
    Ok((line, managed_definitions))
}

fn string_field<'a>(object: &'a Map<String, Value>, field: &str) -> Option<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn optional_string_field<'a>(object: &'a Map<String, Value>, field: &str) -> Option<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn serialize_with_source_length(row: &mut Map<String, Value>) -> Result<Vec<u8>, String> {
    loop {
        let mut line = serde_json::to_vec(row).map_err(|err| err.to_string())?;
        line.push(b'\n');
        let source_length: u32 = line
            .len()
            .try_into()
            .map_err(|_| "serialized event length exceeds UInt32".to_string())?;
        if row.get("source_length").and_then(Value::as_u64) == Some(u64::from(source_length)) {
            return Ok(line);
        }
        row.insert("source_length".to_string(), source_length.into());
    }
}

fn append_invalid(
    output: &mut Vec<u8>,
    tenant_id: &str,
    observed_at: &str,
    reason: String,
    raw: &[u8],
) {
    let row = InvalidEventRow {
        tenant_id,
        observed_at,
        reason,
        raw: String::from_utf8_lossy(raw).into_owned(),
    };
    if serde_json::to_writer(&mut *output, &row).is_ok() {
        output.push(b'\n');
    }
}

pub fn count_ndjson_rows(bytes: &[u8]) -> usize {
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .count()
}

pub fn clickhouse_auth(user: Option<&str>, password: Option<&str>) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    if let Some(user) = user {
        headers.insert("X-ClickHouse-User".to_string(), user.to_string());
    }
    if let Some(password) = password {
        headers.insert("X-ClickHouse-Key".to_string(), password.to_string());
    }
    headers
}

pub fn managed_definition_specs(data: &Map<String, Value>) -> Vec<ManagedDefinitionSpec> {
    let fields = scalar_fields(data);
    let dimensions = fields
        .iter()
        .filter(|field| is_dimension_field(&field.path))
        .cloned()
        .collect::<Vec<_>>();
    let event_type = string_value(data, "event_type").unwrap_or_default();
    let mut specs = Vec::new();

    if event_type == "metric"
        && let Some(metric_name) =
            string_value(data, "metric_name").filter(|value| !value.is_empty())
    {
        specs.push(metric_rollup_definition(&metric_name, &dimensions));
    }

    dedupe_specs(specs)
}

pub fn event_kv_index_ndjson(events_ndjson: &[u8]) -> anyhow::Result<Vec<u8>> {
    event_kv_index_ndjson_with_limits(
        events_ndjson,
        DEFAULT_MAX_EVENT_KV_INDEX_ROWS,
        DEFAULT_MAX_EVENT_KV_STRING_BYTES,
    )
}

pub fn event_kv_index_ndjson_with_limits(
    events_ndjson: &[u8],
    max_rows_per_event: usize,
    max_string_bytes: usize,
) -> anyhow::Result<Vec<u8>> {
    let mut output = Vec::new();
    for line in events_ndjson
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let event: Value = serde_json::from_slice(line)?;
        for row in event_kv_index_rows_with_limits(&event, max_rows_per_event, max_string_bytes) {
            serde_json::to_writer(&mut output, &row)?;
            output.push(b'\n');
        }
    }
    Ok(output)
}

pub fn event_kv_index_rows(event: &Value) -> Vec<EventKvIndexRow> {
    event_kv_index_rows_with_limits(
        event,
        DEFAULT_MAX_EVENT_KV_INDEX_ROWS,
        DEFAULT_MAX_EVENT_KV_STRING_BYTES,
    )
}

pub fn event_kv_index_rows_with_limits(
    event: &Value,
    max_rows_per_event: usize,
    max_string_bytes: usize,
) -> Vec<EventKvIndexRow> {
    let Some(event_object) = event.as_object() else {
        return Vec::new();
    };
    let Some(data) = event_object.get("data").and_then(Value::as_object) else {
        return Vec::new();
    };
    let tenant_id = string_value(event_object, "tenant_id")
        .or_else(|| string_value(data, "tenant_id"))
        .unwrap_or_default();
    let event_id = string_value(event_object, "event_id").unwrap_or_default();
    let timestamp = string_value(event_object, "timestamp").unwrap_or_default();
    if tenant_id.is_empty() || event_id.is_empty() || timestamp.is_empty() {
        return Vec::new();
    }

    let event_type = string_value(data, "event_type").unwrap_or_default();
    let signal = derived_signal(data, &event_type);
    let source_file = string_value(event_object, "source_file").unwrap_or_default();
    let context = KvIndexContext {
        tenant_id,
        timestamp,
        event_id,
        event_type,
        signal,
        source_file,
        max_rows_per_event,
        max_string_bytes,
    };
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    collect_kv_index_rows(
        &context,
        "",
        Value::Object(data.clone()),
        None,
        &mut seen,
        &mut rows,
    );
    rows
}

pub fn event_text_index_ndjson(events_ndjson: &[u8]) -> anyhow::Result<Vec<u8>> {
    event_text_index_ndjson_with_limits(events_ndjson, DEFAULT_MAX_EVENT_SEARCH_TEXT_BYTES)
}

pub fn event_text_index_ndjson_with_limits(
    events_ndjson: &[u8],
    max_text_bytes: usize,
) -> anyhow::Result<Vec<u8>> {
    let mut output = Vec::new();
    for line in events_ndjson
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let event: Value = serde_json::from_slice(line)?;
        for row in event_text_index_rows_with_limits(&event, max_text_bytes) {
            serde_json::to_writer(&mut output, &row)?;
            output.push(b'\n');
        }
    }
    Ok(output)
}

pub fn event_text_index_rows(event: &Value) -> Vec<EventTextIndexRow> {
    event_text_index_rows_with_limits(event, DEFAULT_MAX_EVENT_SEARCH_TEXT_BYTES)
}

pub fn event_search_term_rows(event: &Value) -> Vec<EventSearchTermRow> {
    event_search_term_rows_with_limits(event, DEFAULT_MAX_EVENT_SEARCH_TERM_ROWS)
}

pub fn event_search_term_rows_with_limits(
    event: &Value,
    max_rows_per_event: usize,
) -> Vec<EventSearchTermRow> {
    let Some(event_object) = event.as_object() else {
        return Vec::new();
    };
    let Some(data) = event_object.get("data").and_then(Value::as_object) else {
        return Vec::new();
    };
    let tenant_id = string_value(event_object, "tenant_id")
        .or_else(|| string_value(data, "tenant_id"))
        .unwrap_or_default();
    let event_id = string_value(event_object, "event_id").unwrap_or_default();
    let timestamp = string_value(event_object, "timestamp").unwrap_or_default();
    if tenant_id.is_empty() || event_id.is_empty() || timestamp.is_empty() {
        return Vec::new();
    }

    let event_type = string_value(data, "event_type").unwrap_or_default();
    let signal = derived_signal(data, &event_type);
    let source_file = string_value(event_object, "source_file").unwrap_or_default();
    let mut terms = BTreeMap::<(String, String), u16>::new();
    collect_search_terms("event_id", &Value::String(event_id.clone()), 4, &mut terms);
    collect_search_terms(
        "event_type",
        &Value::String(event_type.clone()),
        4,
        &mut terms,
    );
    collect_search_terms("signal", &Value::String(signal.clone()), 3, &mut terms);
    if let Some(trace_id) = string_value(data, "trace_id") {
        collect_search_terms("trace_id", &Value::String(trace_id), 3, &mut terms);
    }
    if let Some(span_id) = string_value(data, "span_id") {
        collect_search_terms("span_id", &Value::String(span_id), 3, &mut terms);
    }
    collect_search_terms("", &Value::Object(data.clone()), 1, &mut terms);

    terms
        .into_iter()
        .take(max_rows_per_event)
        .map(|((term, path), weight)| EventSearchTermRow {
            tenant_id: tenant_id.clone(),
            timestamp: timestamp.clone(),
            event_id: event_id.clone(),
            event_type: event_type.clone(),
            signal: signal.clone(),
            term,
            path,
            weight,
            source_file: source_file.clone(),
        })
        .collect()
}

pub fn event_text_index_rows_with_limits(
    event: &Value,
    max_text_bytes: usize,
) -> Vec<EventTextIndexRow> {
    let Some(event_object) = event.as_object() else {
        return Vec::new();
    };
    let Some(data) = event_object.get("data").and_then(Value::as_object) else {
        return Vec::new();
    };
    let tenant_id = string_value(event_object, "tenant_id")
        .or_else(|| string_value(data, "tenant_id"))
        .unwrap_or_default();
    let event_id = string_value(event_object, "event_id").unwrap_or_default();
    let timestamp = string_value(event_object, "timestamp").unwrap_or_default();
    if tenant_id.is_empty() || event_id.is_empty() || timestamp.is_empty() {
        return Vec::new();
    }

    let event_type = string_value(data, "event_type").unwrap_or_default();
    let signal = derived_signal(data, &event_type);
    let mut text = String::new();
    push_search_term(&mut text, "event_id", &event_id, max_text_bytes);
    push_search_term(&mut text, "event_type", &event_type, max_text_bytes);
    push_search_term(&mut text, "signal", &signal, max_text_bytes);
    if let Some(trace_id) = string_value(data, "trace_id") {
        push_search_term(&mut text, "trace_id", &trace_id, max_text_bytes);
    }
    if let Some(span_id) = string_value(data, "span_id") {
        push_search_term(&mut text, "span_id", &span_id, max_text_bytes);
    }
    collect_search_text("", &Value::Object(data.clone()), &mut text, max_text_bytes);

    vec![EventTextIndexRow {
        tenant_id,
        timestamp,
        event_id,
        event_type,
        signal,
        trace_id: string_value(data, "trace_id").unwrap_or_default(),
        span_id: string_value(data, "span_id").unwrap_or_default(),
        text,
        source_file: string_value(event_object, "source_file").unwrap_or_default(),
    }]
}

fn collect_search_terms(
    path: &str,
    value: &Value,
    base_weight: u16,
    terms: &mut BTreeMap<(String, String), u16>,
) {
    match value {
        Value::Null => push_search_tokens(path, "null", base_weight, terms),
        Value::Bool(value) => push_search_tokens(path, &value.to_string(), base_weight, terms),
        Value::Number(value) => push_search_tokens(path, &value.to_string(), base_weight, terms),
        Value::String(value) => {
            push_search_tokens(path, value, path_search_weight(path, base_weight), terms)
        }
        Value::Object(object) => {
            for (key, value) in object {
                let child_path = if path.is_empty() {
                    key.to_string()
                } else {
                    format!("{path}.{key}")
                };
                collect_search_terms(&child_path, value, base_weight, terms);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_search_terms(path, value, base_weight, terms);
            }
        }
    }
}

fn path_search_weight(path: &str, base_weight: u16) -> u16 {
    match path {
        "event_id" | "event_type" => base_weight.max(4),
        "message" | "name" | "error.message" | "exception.message" => base_weight.max(3),
        "trace_id" | "span_id" | "request_id" | "service" => base_weight.max(2),
        _ => base_weight,
    }
}

fn push_search_tokens(
    path: &str,
    value: &str,
    weight: u16,
    terms: &mut BTreeMap<(String, String), u16>,
) {
    let mut token = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            token.push(ch.to_ascii_lowercase());
            if token.len() >= 64 {
                push_search_token(path, &token, weight, terms);
                token.clear();
            }
        } else {
            push_search_token(path, &token, weight, terms);
            token.clear();
        }
    }
    push_search_token(path, &token, weight, terms);
}

fn push_search_token(
    path: &str,
    token: &str,
    weight: u16,
    terms: &mut BTreeMap<(String, String), u16>,
) {
    if token.len() < 2 {
        return;
    }
    let key = (token.to_string(), path.to_string());
    let entry = terms.entry(key).or_insert(0);
    *entry = entry.saturating_add(weight).min(255);
}

fn collect_search_text(path: &str, value: &Value, output: &mut String, max_text_bytes: usize) {
    if output.len() >= max_text_bytes {
        return;
    }
    match value {
        Value::Null => push_search_term(output, path, "null", max_text_bytes),
        Value::Bool(value) => push_search_term(output, path, &value.to_string(), max_text_bytes),
        Value::Number(value) => push_search_term(output, path, &value.to_string(), max_text_bytes),
        Value::String(value) => push_search_term(output, path, value, max_text_bytes),
        Value::Object(object) => {
            for (key, value) in object {
                let child_path = if path.is_empty() {
                    key.to_string()
                } else {
                    format!("{path}.{key}")
                };
                collect_search_text(&child_path, value, output, max_text_bytes);
                if output.len() >= max_text_bytes {
                    return;
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_search_text(path, value, output, max_text_bytes);
                if output.len() >= max_text_bytes {
                    return;
                }
            }
        }
    }
}

fn push_search_term(output: &mut String, path: &str, value: &str, max_text_bytes: usize) {
    if value.is_empty() || output.len() >= max_text_bytes {
        return;
    }
    if !output.is_empty() {
        push_search_fragment(output, " ", max_text_bytes);
    }
    if !path.is_empty() {
        push_search_fragment(output, path, max_text_bytes);
        push_search_fragment(output, ":", max_text_bytes);
    }
    push_search_fragment(output, value, max_text_bytes);
}

fn push_search_fragment(output: &mut String, fragment: &str, max_text_bytes: usize) {
    let remaining = max_text_bytes.saturating_sub(output.len());
    if remaining == 0 {
        return;
    }
    if fragment.len() <= remaining {
        output.push_str(fragment);
        return;
    }
    for ch in fragment.chars() {
        if output.len() + ch.len_utf8() > max_text_bytes {
            break;
        }
        output.push(ch);
    }
}

struct KvIndexContext {
    tenant_id: String,
    timestamp: String,
    event_id: String,
    event_type: String,
    signal: String,
    source_file: String,
    max_rows_per_event: usize,
    max_string_bytes: usize,
}

#[derive(Clone)]
struct KvScope {
    path: String,
    index: i32,
}

fn collect_kv_index_rows(
    context: &KvIndexContext,
    path: &str,
    value: Value,
    scope: Option<KvScope>,
    seen: &mut BTreeSet<String>,
    rows: &mut Vec<EventKvIndexRow>,
) {
    if rows.len() >= context.max_rows_per_event {
        return;
    }
    match value {
        Value::Null => push_kv_index_row(context, path, "null", "", None, None, scope, seen, rows),
        Value::Bool(value) => push_kv_index_row(
            context,
            path,
            "bool",
            "",
            None,
            Some(u8::from(value)),
            scope,
            seen,
            rows,
        ),
        Value::Number(value) => {
            let Some(value) = value.as_f64() else {
                return;
            };
            push_kv_index_row(
                context,
                path,
                "number",
                "",
                Some(value),
                None,
                scope,
                seen,
                rows,
            );
        }
        Value::String(value) => {
            if value.len() > context.max_string_bytes {
                return;
            }
            push_kv_index_row(
                context, path, "string", &value, None, None, scope, seen, rows,
            );
        }
        Value::Object(object) => {
            for (key, value) in object {
                let child_path = if path.is_empty() {
                    key
                } else {
                    format!("{path}.{key}")
                };
                collect_kv_index_rows(context, &child_path, value, scope.clone(), seen, rows);
                if rows.len() >= context.max_rows_per_event {
                    return;
                }
            }
        }
        Value::Array(values) => {
            for (index, value) in values.into_iter().enumerate() {
                if rows.len() >= context.max_rows_per_event {
                    return;
                }
                match value {
                    Value::Object(_) => {
                        let array_path = format!("{path}[]");
                        collect_kv_index_rows(
                            context,
                            &array_path,
                            value,
                            Some(KvScope {
                                path: path.to_string(),
                                index: index.try_into().unwrap_or(i32::MAX),
                            }),
                            seen,
                            rows,
                        );
                    }
                    Value::Array(_) => {
                        let array_path = format!("{path}[]");
                        collect_kv_index_rows(
                            context,
                            &array_path,
                            value,
                            scope.clone(),
                            seen,
                            rows,
                        );
                    }
                    scalar => {
                        collect_kv_index_rows(context, path, scalar, scope.clone(), seen, rows);
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn push_kv_index_row(
    context: &KvIndexContext,
    path: &str,
    value_type: &str,
    string_value: &str,
    number_value: Option<f64>,
    bool_value: Option<u8>,
    scope: Option<KvScope>,
    seen: &mut BTreeSet<String>,
    rows: &mut Vec<EventKvIndexRow>,
) {
    if path.is_empty() || rows.len() >= context.max_rows_per_event {
        return;
    }
    let scope_path = scope
        .as_ref()
        .map(|scope| scope.path.clone())
        .unwrap_or_default();
    let scope_index = scope.as_ref().map(|scope| scope.index).unwrap_or(-1);
    let value_key = match value_type {
        "number" => number_value
            .map(|value| value.to_string())
            .unwrap_or_default(),
        "bool" => bool_value
            .map(|value| value.to_string())
            .unwrap_or_default(),
        _ => string_value.to_string(),
    };
    let dedupe_key =
        format!("{path}\u{0}{value_type}\u{0}{value_key}\u{0}{scope_path}\u{0}{scope_index}");
    if !seen.insert(dedupe_key) {
        return;
    }
    rows.push(EventKvIndexRow {
        tenant_id: context.tenant_id.clone(),
        timestamp: context.timestamp.clone(),
        event_id: context.event_id.clone(),
        event_type: context.event_type.clone(),
        signal: context.signal.clone(),
        path: path.to_string(),
        value_type: value_type.to_string(),
        string_value: string_value.to_string(),
        number_value,
        bool_value,
        scope_path,
        scope_index,
        source_file: context.source_file.clone(),
    });
}

fn derived_signal(data: &Map<String, Value>, event_type: &str) -> String {
    if let Some(signal) = string_value(data, "signal").filter(|value| !value.is_empty()) {
        return signal;
    }
    match event_type {
        "span" | "span_start" | "span_end" => "trace",
        "metric" => "metric",
        "log" => "log",
        "analytics" | "track" | "page" | "screen" | "identify" | "group" | "alias" => "analytics",
        _ => "other",
    }
    .to_string()
}

#[derive(Debug, Clone)]
struct ScalarField {
    path: String,
}

fn scalar_fields(data: &Map<String, Value>) -> Vec<ScalarField> {
    let mut fields = Vec::new();
    collect_scalar_fields("", data, &mut fields);
    fields
}

fn collect_scalar_fields(prefix: &str, data: &Map<String, Value>, fields: &mut Vec<ScalarField>) {
    for (key, value) in data {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match value {
            Value::String(value) if !value.is_empty() && valid_path(&path) => {
                fields.push(ScalarField { path });
            }
            Value::Number(_) if valid_path(&path) => fields.push(ScalarField { path }),
            Value::Bool(_) if valid_path(&path) => fields.push(ScalarField { path }),
            Value::Object(object) if valid_path_prefix(&path) => {
                collect_scalar_fields(&path, object, fields);
            }
            _ => {}
        }
    }
}

fn metric_rollup_definition(
    metric_name: &str,
    dimensions: &[ScalarField],
) -> ManagedDefinitionSpec {
    let dimensions = dimensions
        .iter()
        .filter(|field| {
            !matches!(
                field.path.as_str(),
                "metric_name" | "metric_type" | "metric_unit"
            )
        })
        .map(|field| {
            serde_json::json!({
                "name": field.path,
                "value": { "path": field.path }
            })
        })
        .collect::<Vec<_>>();
    let config = serde_json::json!({
        "managed_by": "sdk",
        "definition_source": "sdk_metric",
        "metric_name": metric_name,
        "match": {
            "all": [
                { "path": "event_type", "op": "eq", "value": "metric" },
                { "path": "metric_name", "op": "eq", "value": metric_name },
                { "path": "metric_value", "op": "is_number" }
            ]
        },
        "outputs": [
            {
                "target": "metric_rollups",
                "metric_name": metric_name,
                "metric_kind": { "path": "metric_type", "default": "counter" },
                "value": { "path": "metric_value" },
                "unit": { "path": "metric_unit", "default": "" },
                "dimensions": dimensions,
                "bucket_seconds": 60
            }
        ]
    });
    managed_spec(
        &format!("metric_rollup.{metric_name}"),
        &format!("metric.{}", safe_name(metric_name)),
        "metric_rollup",
        "managed",
        config,
        serde_json::json!({
            "managed_by": "sdk",
            "aggregate": true,
            "metric_rollup": true,
            "managed": true,
            "sdk_surface": "metric"
        }),
    )
}

fn managed_spec(
    key: &str,
    name: &str,
    kind: &str,
    mode: &str,
    config: Value,
    capabilities: Value,
) -> ManagedDefinitionSpec {
    ManagedDefinitionSpec {
        definition_id: format!("sdk_managed_{}_{}", safe_name(kind), short_hash(key)),
        name: safe_name(name),
        kind: kind.to_string(),
        mode: mode.to_string(),
        config,
        capabilities,
    }
}

fn dedupe_specs(specs: Vec<ManagedDefinitionSpec>) -> Vec<ManagedDefinitionSpec> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for spec in specs {
        if seen.insert(spec.definition_id.clone()) {
            deduped.push(spec);
        }
    }
    deduped
}

fn string_value(data: &Map<String, Value>, key: &str) -> Option<String> {
    data.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn is_dimension_field(path: &str) -> bool {
    !matches!(
        path,
        "tenant_id"
            | "organization_id"
            | "event_type"
            | "signal"
            | "metric_value"
            | "duration_ms"
            | "start_time"
            | "end_time"
            | "span_status_message"
    )
}

fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 160
        && path.split('.').all(|part| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        })
}

fn valid_path_prefix(path: &str) -> bool {
    path.len() <= 160
        && path.split('.').all(|part| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        })
}

fn safe_name(value: &str) -> String {
    let mut output = value
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches(['_', '.', '-'])
        .to_string();
    if output.is_empty() {
        output = "auto".to_string();
    }
    if output.len() > 96 {
        output.truncate(96);
        while output.ends_with(['_', '.', '-']) {
            output.pop();
        }
    }
    output
}

fn short_hash(value: &str) -> String {
    hex_lower(&Sha256::digest(value.as_bytes())[..8])
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_batch_and_stamps_tenant() {
        let body = br#"[
            {"event_id":"evt_1","timestamp":"2026-06-04T00:00:00Z","data":{"tenant_id":"evil","event_type":"log"}},
            {"event_id":"evt_2","timestamp":"2026-06-04T00:00:01Z","data":{"event_type":"metric","metric_value":1}}
        ]"#;

        let batch = normalize_json_batch(
            body,
            "tenant-a",
            "org-a",
            "kafka://events.ingest.v1/0/12",
            "2026-06-04T00:00:02Z",
            usize::MAX,
        );

        assert_eq!(batch.valid_rows, 2);
        assert_eq!(batch.invalid_rows, 0);
        assert!(
            !batch
                .managed_definitions
                .iter()
                .any(|spec| spec.kind == "field" && spec.name == "tenant_id")
        );
        for line in batch.normalized.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let row: Value = serde_json::from_slice(line).expect("row is JSON");
            assert_eq!(row["tenant_id"], "tenant-a");
            assert_eq!(row["data"]["tenant_id"], "tenant-a");
            assert_eq!(row["data"]["organization_id"], "org-a");
            assert!(row["source_length"].as_u64().unwrap() > 0);
        }
    }

    #[test]
    fn sends_invalid_rows_to_invalid_output() {
        let batch = normalize_json_batch(
            br#"{"event_id":"","timestamp":"2026-06-04T00:00:00Z","data":{}}"#,
            "tenant-a",
            "org-a",
            "kafka://events.ingest.v1/0/13",
            "2026-06-04T00:00:02Z",
            usize::MAX,
        );

        assert_eq!(batch.valid_rows, 0);
        assert_eq!(batch.invalid_rows, 1);
        let row: Value = serde_json::from_slice(&batch.invalid).expect("invalid row is JSON");
        assert_eq!(row["tenant_id"], "tenant-a");
        assert!(row["reason"].as_str().unwrap().contains("event_id"));
    }

    #[test]
    fn metric_event_creates_managed_metric_rollup_and_dimensions() {
        let data = serde_json::json!({
            "tenant_id": "org",
            "event_type": "metric",
            "metric_name": "checkout.attempts",
            "metric_type": "counter",
            "metric_value": 1,
            "plan": "pro"
        });
        let specs = managed_definition_specs(data.as_object().unwrap());

        assert!(specs.iter().any(|spec| spec.kind == "metric_rollup"));
        assert!(
            !specs
                .iter()
                .any(|spec| spec.kind == "field" && spec.name == "plan"),
            "plain metric dimensions stay inside the metric definition and are not promoted fields"
        );
    }

    #[test]
    fn plain_numeric_event_does_not_infer_measure_or_rollups() {
        let data = serde_json::json!({
            "tenant_id": "org",
            "event_type": "track",
            "name": "Checkout Completed",
            "revenue": 99.0,
            "product_id": "sku_1",
            "plan": "pro"
        });
        let specs = managed_definition_specs(data.as_object().unwrap());

        assert!(specs.is_empty());
    }

    #[test]
    fn span_lifecycle_events_do_not_promote_trace_fields_or_duration() {
        let body = br#"[
            {"event_id":"span-start","timestamp":"2026-06-04T00:00:00Z","data":{"event_type":"span_start","trace_id":"trace-1","span_id":"span-1","parent_span_id":"root","name":"GET /v1/events","service":"api"}},
            {"event_id":"span-end","timestamp":"2026-06-04T00:00:01Z","data":{"event_type":"span_end","trace_id":"trace-1","span_id":"span-1","name":"GET /v1/events","service":"api","duration_ms":42.5,"span_status_code":"ok"}}
        ]"#;

        let batch = normalize_json_batch(
            body,
            "tenant-a",
            "org-a",
            "kafka://events.ingest.v1/0/14",
            "2026-06-04T00:00:02Z",
            usize::MAX,
        );

        assert_eq!(batch.valid_rows, 2);
        assert!(batch.managed_definitions.is_empty());
    }

    #[test]
    fn kv_index_flattens_nested_scalars_and_arrays() {
        let event = serde_json::json!({
            "tenant_id": "tenant-a",
            "event_id": "evt-1",
            "timestamp": "2026-06-04T00:00:00Z",
            "source_file": "kafka://events/0/1",
            "data": {
                "event_type": "track",
                "plan": "pro",
                "llm": {
                    "model": "gpt-4.1",
                    "usage": {
                        "prompt_tokens": 1200
                    }
                },
                "tags": ["checkout", "mobile"],
                "items": [
                    { "sku": "sku_1", "price": 10 },
                    { "sku": "sku_2", "price": 20 }
                ]
            }
        });

        let rows = event_kv_index_rows(&event);
        assert!(
            rows.iter()
                .any(|row| row.path == "plan" && row.string_value == "pro")
        );
        assert!(
            rows.iter().any(
                |row| row.path == "llm.usage.prompt_tokens" && row.number_value == Some(1200.0)
            )
        );
        assert!(
            rows.iter()
                .any(|row| row.path == "tags" && row.string_value == "checkout")
        );
        assert!(rows.iter().any(|row| row.path == "items[].sku"
            && row.string_value == "sku_1"
            && row.scope_path == "items"
            && row.scope_index == 0));
        assert!(rows.iter().any(|row| row.path == "items[].price"
            && row.number_value == Some(20.0)
            && row.scope_path == "items"
            && row.scope_index == 1));
    }

    #[test]
    fn kv_index_ndjson_serializes_all_rows() {
        let batch = normalize_json_batch(
            br#"{"event_id":"evt-1","timestamp":"2026-06-04T00:00:00Z","data":{"event_type":"track","plan":"pro","latency_ms":125}}"#,
            "tenant-a",
            "org-a",
            "kafka://events.ingest.v1/0/14",
            "2026-06-04T00:00:02Z",
            usize::MAX,
        );

        let index = event_kv_index_ndjson(&batch.normalized).unwrap();
        let rows = count_ndjson_rows(&index);
        assert!(
            rows >= 5,
            "expected event fields plus tenant/org rows, got {rows}"
        );
        let text = String::from_utf8(index).unwrap();
        assert!(text.contains("\"path\":\"plan\""));
        assert!(text.contains("\"number_value\":125.0"));
    }

    #[test]
    fn text_index_flattens_searchable_scalars() {
        let event = serde_json::json!({
            "tenant_id": "tenant-a",
            "event_id": "evt-1",
            "timestamp": "2026-06-04T00:00:00Z",
            "source_file": "kafka://events/0/1",
            "data": {
                "event_type": "log",
                "trace_id": "trace-1",
                "span_id": "span-1",
                "service": "api",
                "message": "checkout timeout",
                "tags": ["payments", "retry"]
            }
        });

        let rows = event_text_index_rows_with_limits(&event, 512);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tenant_id, "tenant-a");
        assert_eq!(rows[0].event_id, "evt-1");
        assert_eq!(rows[0].trace_id, "trace-1");
        assert!(rows[0].text.contains("message:checkout timeout"));
        assert!(rows[0].text.contains("tags:payments"));
        assert!(rows[0].text.len() <= 512);
    }

    #[test]
    fn search_terms_tokenize_and_weight_log_text() {
        let event = serde_json::json!({
            "tenant_id": "tenant-a",
            "event_id": "evt-1",
            "timestamp": "2026-06-04T00:00:00Z",
            "source_file": "kafka://events/0/1",
            "data": {
                "event_type": "log",
                "trace_id": "trace-1",
                "service": "api",
                "message": "Checkout timeout talking to Redis",
                "llm": { "model": "gpt-5.5" }
            }
        });

        let rows = event_search_term_rows_with_limits(&event, 128);

        assert!(
            rows.iter()
                .any(|row| row.term == "checkout" && row.path == "message")
        );
        assert!(
            rows.iter()
                .any(|row| row.term == "redis" && row.path == "message")
        );
        assert!(
            rows.iter()
                .any(|row| row.term == "gpt" && row.path == "llm.model")
        );
        let message_weight = rows
            .iter()
            .find(|row| row.term == "checkout" && row.path == "message")
            .expect("message token")
            .weight;
        let model_weight = rows
            .iter()
            .find(|row| row.term == "gpt" && row.path == "llm.model")
            .expect("model token")
            .weight;
        assert!(message_weight > model_weight);
    }
}
