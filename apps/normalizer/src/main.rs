use std::{
    collections::HashSet,
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use nanotrace_ingest::{
    DEFAULT_INGEST_TOPIC, DEFAULT_INVALID_TOPIC, DEFAULT_NORMALIZED_TOPIC, DEFAULT_TABLEFLOW_TOPIC,
    HEADER_ORGANIZATION_ID, HEADER_RECEIVED_AT, HEADER_TENANT_ID, ManagedDefinitionSpec, consumer,
    count_ndjson_rows, header_value, normalize_json_batch, producer, subscribe,
};
use rdkafka::{
    Message,
    consumer::{CommitMode, Consumer},
    producer::FutureRecord,
};
use reqwest::StatusCode;
use tokio::time::Instant;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Clone)]
struct Config {
    brokers: String,
    ingest_topic: String,
    normalized_topic: String,
    tableflow_topic: String,
    invalid_topic: String,
    group_id: String,
    client_id: String,
    max_event_bytes: usize,
    clickhouse_url: Option<String>,
    clickhouse_database: String,
    clickhouse_invalid_table: String,
    clickhouse_definitions_table: String,
    clickhouse_user: Option<String>,
    clickhouse_password: Option<String>,
    request_timeout: Duration,
    fail_after_clickhouse_insert: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env()?;
    let consumer =
        consumer(&cfg.brokers, &cfg.group_id, &cfg.client_id).context("create Kafka consumer")?;
    subscribe(&consumer, &cfg.ingest_topic).context("subscribe to ingest topic")?;
    let producer = producer(&cfg.brokers, &format!("{}-producer", cfg.client_id))
        .context("create Kafka producer")?;
    let http = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .build()
        .context("build HTTP client")?;
    let mut managed_definition_cache = HashSet::new();

    info!(
        brokers = cfg.brokers,
        ingest_topic = cfg.ingest_topic,
        normalized_topic = cfg.normalized_topic,
        tableflow_topic = cfg.tableflow_topic,
        invalid_topic = cfg.invalid_topic,
        clickhouse_enabled = cfg.clickhouse_url.is_some(),
        "nanotrace normalizer starting"
    );

    loop {
        tokio::select! {
            message = consumer.recv() => {
                match message {
                    Ok(message) => {
                        if let Err(err) = process_message(
                            &cfg,
                            &http,
                            &producer,
                            &message,
                            &mut managed_definition_cache,
                        ).await {
                            error!(error = %err, "failed to process ingest message");
                            if cfg.fail_after_clickhouse_insert {
                                return Err(err);
                            }
                        } else {
                            consumer.commit_message(&message, CommitMode::Sync)
                                .context("commit Kafka offset")?;
                        }
                    }
                    Err(err) => {
                        warn!(error = %err, "Kafka receive failed");
                    }
                }
            }
            _ = shutdown_signal() => {
                info!("shutdown signal received");
                return Ok(());
            }
        }
    }
}

async fn process_message(
    cfg: &Config,
    http: &reqwest::Client,
    producer: &rdkafka::producer::FutureProducer,
    message: &rdkafka::message::BorrowedMessage<'_>,
    managed_definition_cache: &mut HashSet<String>,
) -> Result<()> {
    let tenant_id = header_value(message, HEADER_TENANT_ID)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("missing {HEADER_TENANT_ID} header"))?;
    let organization_id = header_value(message, HEADER_ORGANIZATION_ID)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| tenant_id.clone());
    let received_at = header_value(message, HEADER_RECEIVED_AT)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(chrono_like_now);
    let payload = message.payload().unwrap_or_default();
    let source_file = format!(
        "kafka://{}/{}/{}",
        message.topic(),
        message.partition(),
        message.offset()
    );
    let token_prefix = format!(
        "normalizer:{}:{}:{}",
        message.topic(),
        message.partition(),
        message.offset()
    );
    let started_at = Instant::now();
    let batch = normalize_json_batch(
        payload,
        &tenant_id,
        &organization_id,
        &source_file,
        &received_at,
        cfg.max_event_bytes,
    );

    let mut tableflow_publish_ms = 0_u64;
    let mut invalid_insert_ms = 0_u64;
    let mut clickhouse_inserted = false;

    if !batch.normalized.is_empty() {
        let publish_started = Instant::now();
        let tableflow_payload = tableflow_batch_payload(
            &batch.normalized,
            message,
            &tenant_id,
            &organization_id,
            &received_at,
        )
        .context("build Tableflow batch payload")?;
        produce_bytes(
            producer,
            &cfg.tableflow_topic,
            &tenant_id,
            &tableflow_payload,
        )
        .await
        .context("produce Tableflow batch")?;
        tableflow_publish_ms = elapsed_ms(publish_started.elapsed());
    }
    if !batch.invalid.is_empty() {
        let invalid_dedupe_token = format!("{token_prefix}:invalid");
        let insert_started = Instant::now();
        insert_clickhouse(
            cfg,
            http,
            &cfg.clickhouse_invalid_table,
            &batch.invalid,
            Some(&invalid_dedupe_token),
        )
        .await?;
        invalid_insert_ms = elapsed_ms(insert_started.elapsed());
        clickhouse_inserted = cfg.clickhouse_url.is_some();
    }
    if cfg.fail_after_clickhouse_insert && clickhouse_inserted {
        bail!("injected failure after ClickHouse insert");
    }
    if !batch.normalized.is_empty() {
        produce_bytes(
            producer,
            &cfg.normalized_topic,
            &tenant_id,
            &batch.normalized,
        )
        .await
        .context("produce normalized batch")?;
    }
    if !batch.invalid.is_empty() {
        produce_bytes(producer, &cfg.invalid_topic, &tenant_id, &batch.invalid)
            .await
            .context("produce invalid batch")?;
    }
    ensure_managed_definitions(
        cfg,
        http,
        &tenant_id,
        &batch.managed_definitions,
        managed_definition_cache,
    )
    .await?;

    info!(
        topic = message.topic(),
        partition = message.partition(),
        offset = message.offset(),
        valid_rows = batch.valid_rows,
        invalid_rows = batch.invalid_rows,
        managed_definitions = batch.managed_definitions.len(),
        normalized_bytes = batch.normalized.len(),
        invalid_bytes = batch.invalid.len(),
        tableflow_topic = cfg.tableflow_topic,
        tableflow_publish_ms,
        invalid_insert_ms,
        elapsed_ms = started_at.elapsed().as_millis(),
        "normalized ingest message"
    );
    Ok(())
}

async fn ensure_managed_definitions(
    cfg: &Config,
    http: &reqwest::Client,
    tenant_id: &str,
    definitions: &[ManagedDefinitionSpec],
    cache: &mut HashSet<String>,
) -> Result<()> {
    if cfg.clickhouse_url.is_none() || definitions.is_empty() {
        return Ok(());
    }

    let mut pending = Vec::new();
    for definition in definitions {
        let key = format!("{tenant_id}\0{}", definition.definition_id);
        if cache.insert(key.clone()) {
            pending.push((key, definition));
        }
    }
    if pending.is_empty() {
        return Ok(());
    }

    let now = clickhouse_now();
    let version = unix_millis();
    let mut body = Vec::new();
    for (_, definition) in &pending {
        let row = serde_json::json!({
            "tenant_id": tenant_id,
            "definition_id": definition.definition_id,
            "name": definition.name,
            "kind": definition.kind,
            "mode": definition.mode,
            "enabled": 1,
            "config": definition.config,
            "capabilities": definition.capabilities,
            "created_at": now,
            "updated_at": now,
            "deleted_at": null,
            "version": version
        });
        serde_json::to_writer(&mut body, &row).context("serialize managed definition row")?;
        body.push(b'\n');
    }

    if let Err(err) =
        insert_clickhouse(cfg, http, &cfg.clickhouse_definitions_table, &body, None).await
    {
        for (key, _) in pending {
            cache.remove(&key);
        }
        return Err(err).context("insert managed definitions");
    }

    Ok(())
}

async fn produce_bytes(
    producer: &rdkafka::producer::FutureProducer,
    topic: &str,
    key: &str,
    bytes: &[u8],
) -> Result<()> {
    producer
        .send(
            FutureRecord::to(topic).key(key).payload(bytes),
            Duration::from_secs(30),
        )
        .await
        .map_err(|(err, _)| anyhow::anyhow!("Kafka produce failed: {err}"))?;
    Ok(())
}

async fn insert_clickhouse(
    cfg: &Config,
    http: &reqwest::Client,
    table: &str,
    body: &[u8],
    dedupe_token: Option<&str>,
) -> Result<()> {
    let Some(clickhouse_url) = cfg.clickhouse_url.as_ref() else {
        return Ok(());
    };
    if body.is_empty() {
        return Ok(());
    }
    let rows = count_ndjson_rows(body);
    if rows == 0 {
        return Ok(());
    }
    let full_table = format!("{}.{}", cfg.clickhouse_database, table);
    let query = format!("INSERT INTO {full_table} FORMAT JSONEachRow");
    let mut request = http
        .post(clickhouse_url)
        .query(&[
            ("database", cfg.clickhouse_database.as_str()),
            ("query", query.as_str()),
            ("date_time_input_format", "best_effort"),
            ("type_json_skip_duplicated_paths", "1"),
            ("insert_deduplicate", "1"),
        ])
        .body(body.to_vec());
    if let Some(dedupe_token) = dedupe_token {
        request = request.query(&[("insert_deduplication_token", dedupe_token)]);
    }
    if let Some(user) = cfg.clickhouse_user.as_deref() {
        request = request.basic_auth(user, cfg.clickhouse_password.as_deref());
    }
    let response = request.send().await.context("send ClickHouse insert")?;
    let status = response.status();
    let text = response.text().await.context("read ClickHouse response")?;
    if status != StatusCode::OK {
        bail!("ClickHouse insert into {full_table} failed: {status} {text}");
    }
    Ok(())
}

impl Config {
    fn from_env() -> Result<Self> {
        let brokers = required("NANOTRACE_KAFKA_BROKERS")?;
        Ok(Self {
            brokers,
            ingest_topic: env_or("NANOTRACE_KAFKA_INGEST_TOPIC", DEFAULT_INGEST_TOPIC),
            normalized_topic: env_or("NANOTRACE_KAFKA_NORMALIZED_TOPIC", DEFAULT_NORMALIZED_TOPIC),
            tableflow_topic: env_or("NANOTRACE_KAFKA_TABLEFLOW_TOPIC", DEFAULT_TABLEFLOW_TOPIC),
            invalid_topic: env_or("NANOTRACE_KAFKA_INVALID_TOPIC", DEFAULT_INVALID_TOPIC),
            group_id: env_or("NANOTRACE_NORMALIZER_GROUP_ID", "nanotrace-normalizer"),
            client_id: env_or("NANOTRACE_NORMALIZER_CLIENT_ID", "nanotrace-normalizer"),
            max_event_bytes: parse_env("MAX_EVENT_BYTES", 209_715_200)?,
            clickhouse_url: optional("CLICKHOUSE_URL"),
            clickhouse_database: env_or("CLICKHOUSE_DATABASE", "observatory"),
            clickhouse_invalid_table: env_or("CLICKHOUSE_INVALID_EVENTS_TABLE", "invalid_events"),
            clickhouse_definitions_table: env_or("CLICKHOUSE_DEFINITIONS_TABLE", "definitions"),
            clickhouse_user: optional("CLICKHOUSE_USER")
                .or_else(|| optional("CLICKHOUSE_USERNAME")),
            clickhouse_password: optional("CLICKHOUSE_PASSWORD"),
            request_timeout: Duration::from_secs(parse_env(
                "NANOTRACE_NORMALIZER_REQUEST_TIMEOUT_SECS",
                60_u64,
            )?),
            fail_after_clickhouse_insert: env_bool(
                "NANOTRACE_NORMALIZER_FAIL_AFTER_CLICKHOUSE_INSERT",
            ),
        })
    }
}

fn required(key: &str) -> Result<String> {
    optional(key).ok_or_else(|| anyhow::anyhow!("{key} is required"))
}

fn optional(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool_default(key: &str, fallback: bool) -> bool {
    optional(key)
        .map(|value| matches_bool(&value))
        .unwrap_or(fallback)
}

fn env_bool(key: &str) -> bool {
    env_bool_default(key, false)
}

fn matches_bool(value: &str) -> bool {
    matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes")
}

fn env_or(key: &str, fallback: &str) -> String {
    optional(key).unwrap_or_else(|| fallback.to_string())
}

fn parse_env<T>(key: &str, fallback: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match optional(key) {
        Some(value) => value
            .parse()
            .with_context(|| format!("{key} has invalid value {value:?}")),
        None => Ok(fallback),
    }
}

fn chrono_like_now() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn clickhouse_now() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn elapsed_ms(elapsed: Duration) -> u64 {
    elapsed.as_millis().try_into().unwrap_or(u64::MAX)
}

fn tableflow_batch_payload(
    normalized_ndjson: &[u8],
    message: &rdkafka::message::BorrowedMessage<'_>,
    tenant_id: &str,
    organization_id: &str,
    received_at: &str,
) -> Result<Vec<u8>> {
    let mut events = Vec::new();
    for line in normalized_ndjson
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let event = serde_json::from_slice::<serde_json::Value>(line)
            .context("parse normalized event row for Tableflow")?;
        events.push(event);
    }

    let row = serde_json::json!({
        "schema_version": 1,
        "batch_id": format!("{}:{}:{}", message.topic(), message.partition(), message.offset()),
        "tenant_id": tenant_id,
        "organization_id": organization_id,
        "received_at": received_at,
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "source_file": format!("kafka://{}/{}/{}", message.topic(), message.partition(), message.offset()),
        "event_count": events.len(),
        "events": events,
    });
    let mut output = serde_json::to_vec(&row).context("serialize Tableflow batch row")?;
    output.push(b'\n');
    Ok(output)
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
