use std::{
    collections::BTreeSet,
    env,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::{Client as SqsClient, types::Message};
use nanotrace_lakehouse::{LakehouseCommit, LakehouseConfig, LakehouseRestCatalogConfig};
use nanotrace_processor_runtime::{ProcessorRuntime, ProcessorSyncConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::{sync::RwLock, task::JoinSet};
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const DEFAULT_CLICKHOUSE_INSERT_MAX_ROWS: usize = 100_000;
const DEFAULT_CLICKHOUSE_INSERT_MAX_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_CLICKHOUSE_INSERT_CONCURRENCY: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DerivationMode {
    Raw,
    Promoted,
}

#[derive(Clone, Copy)]
struct DerivationPlan {
    promoted_fields: bool,
    promoted_measures: bool,
    promoted_states: bool,
}

impl DerivationMode {
    fn plan(self) -> DerivationPlan {
        match self {
            Self::Raw => DerivationPlan {
                promoted_fields: false,
                promoted_measures: false,
                promoted_states: false,
            },
            Self::Promoted => DerivationPlan {
                promoted_fields: true,
                promoted_measures: true,
                promoted_states: true,
            },
        }
    }

    fn needs_definitions(self) -> bool {
        matches!(self, Self::Promoted)
    }
}

#[derive(Clone)]
struct Config {
    sqs_queue_url: String,
    clickhouse_url: String,
    clickhouse_user: Option<String>,
    clickhouse_password: Option<String>,
    clickhouse_database: String,
    clickhouse_table: String,
    clickhouse_field_index_table: String,
    clickhouse_event_measures_table: String,
    clickhouse_entity_state_updates_table: String,
    clickhouse_definitions_table: String,
    poll_wait: u32,
    max_messages: i32,
    concurrency: usize,
    visibility_timeout: i32,
    request_timeout: Duration,
    definitions_refresh_interval: Duration,
    processor_bucket: Option<String>,
    processor_prefix: String,
    processor_poll_interval: Duration,
    processor_dir: PathBuf,
    postgres_url: Option<String>,
    ingest_ledger_enabled: bool,
    ingest_ledger_stale_after: Duration,
    lakehouse_enabled: bool,
    lakehouse_warehouse_dir: PathBuf,
    lakehouse_target_file_size_bytes: u64,
    lakehouse_min_snapshots_to_keep: u64,
    lakehouse_max_snapshot_age_ms: u64,
    lakehouse_metadata_previous_versions_max: u64,
    iceberg_rest_catalog: Option<LakehouseRestCatalogConfig>,
    derivation_mode: DerivationMode,
    clickhouse_insert_max_rows: usize,
    clickhouse_insert_max_bytes: usize,
    clickhouse_insert_concurrency: usize,
}

#[derive(Clone)]
struct Loader {
    cfg: Config,
    http: reqwest::Client,
    processors: ProcessorRuntime,
    s3: S3Client,
    sqs: SqsClient,
    ingest_ledger: Option<IngestLedger>,
    definitions_cache: Arc<RwLock<CachedDefinitions>>,
}

#[derive(Clone)]
struct IngestLedger {
    pg: PgPool,
    owner: String,
    stale_after: Duration,
}

#[derive(Clone, Default)]
struct CachedDefinitions {
    definitions: ExtractionDefinitions,
    fetched_at: Option<Instant>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ObjectRef {
    bucket: String,
    key: String,
}

#[derive(Debug)]
struct PreparedObject {
    object_ref: ObjectRef,
    rows: usize,
    raw: Vec<u8>,
    derived: DerivedBuffers,
    s3_ms: u64,
    transform_ms: u64,
    definitions_ms: u64,
    derive_ms: u64,
}

#[derive(Default)]
struct BatchBuffers {
    rows: usize,
    objects: usize,
    raw: Vec<u8>,
    field_index: Vec<u8>,
    event_measures: Vec<u8>,
    entity_state_updates: Vec<u8>,
    raw_bytes: usize,
    field_index_bytes: usize,
    event_measure_bytes: usize,
    entity_state_update_bytes: usize,
    s3_ms: u64,
    transform_ms: u64,
    definitions_ms: u64,
    derive_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LedgerAcquire {
    Acquired,
    AlreadyCompleted,
    InProgress,
}

impl BatchBuffers {
    fn from_prepared(prepared: &[PreparedObject]) -> Self {
        let mut batch = Self::default();
        for object in prepared {
            batch.objects += 1;
            batch.rows += object.rows;
            batch.raw_bytes += object.raw.len();
            batch.field_index_bytes += object.derived.field_index.len();
            batch.event_measure_bytes += object.derived.event_measures.len();
            batch.entity_state_update_bytes += object.derived.entity_state_updates.len();
            batch.s3_ms += object.s3_ms;
            batch.transform_ms += object.transform_ms;
            batch.definitions_ms += object.definitions_ms;
            batch.derive_ms += object.derive_ms;

            append_ndjson(&mut batch.raw, &object.raw);
            append_ndjson(&mut batch.field_index, &object.derived.field_index);
            append_ndjson(&mut batch.event_measures, &object.derived.event_measures);
            append_ndjson(
                &mut batch.entity_state_updates,
                &object.derived.entity_state_updates,
            );
        }
        batch
    }
}

fn append_ndjson(dst: &mut Vec<u8>, src: &[u8]) {
    if src.is_empty() {
        return;
    }
    dst.extend_from_slice(src);
    if !src.ends_with(b"\n") {
        dst.push(b'\n');
    }
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
struct LakehouseCommitRow<'a> {
    namespace: &'a str,
    table_name: &'a str,
    snapshot_id: &'a str,
    sequence_number: u64,
    committed_at_ms: u64,
    data_file: &'a str,
    data_files: &'a [String],
    record_count: u64,
    content_sha256: &'a str,
    metadata_location: &'a str,
    source_batch_id: &'a str,
    deduplicated: u8,
}

#[derive(Debug, Serialize)]
struct ServingWatermarkRow<'a> {
    serving_table: &'a str,
    source_namespace: &'a str,
    source_table: &'a str,
    source_snapshot_id: &'a str,
    source_sequence_number: u64,
    source_record_count: u64,
    status: &'a str,
    attributes: Value,
}

#[derive(Debug, Default)]
struct DerivedBuffers {
    rows: usize,
    field_index: Vec<u8>,
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
    let ingest_ledger = IngestLedger::from_config(&cfg).await?;
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
        ingest_ledger,
        definitions_cache: Arc::new(RwLock::new(CachedDefinitions::default())),
    };

    info!(
        derivation_mode = ?loader.cfg.derivation_mode,
        ingest_ledger_enabled = loader.ingest_ledger.is_some(),
        "nanotrace loader starting"
    );
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

fn env_bool_default(key: &str, fallback: bool) -> bool {
    env::var(key).ok().map_or(fallback, |value| {
        matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES")
    })
}

fn iceberg_rest_catalog_from_env() -> Option<LakehouseRestCatalogConfig> {
    let uri = optional("NANOTRACE_ICEBERG_REST_URI")?;
    let warehouse = env_or("NANOTRACE_ICEBERG_WAREHOUSE", "s3://nanotrace-lakehouse");
    let catalog_name = env_or("NANOTRACE_ICEBERG_CATALOG_NAME", "nanotrace");
    let mut properties = Map::new();
    if let Some(prefix) = optional("NANOTRACE_ICEBERG_REST_PREFIX") {
        properties.insert("prefix".to_string(), Value::String(prefix));
    }

    Some(LakehouseRestCatalogConfig {
        catalog_name,
        uri,
        warehouse,
        properties: properties
            .into_iter()
            .filter_map(|(key, value)| value.as_str().map(|value| (key, value.to_string())))
            .collect(),
    })
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

impl Config {
    fn from_env() -> Result<Self> {
        let clickhouse_database = env_or("CLICKHOUSE_DATABASE", "observatory");
        let clickhouse_table = env_or("CLICKHOUSE_TABLE", "events");
        let clickhouse_field_index_table = env_or("CLICKHOUSE_FIELD_INDEX_TABLE", "field_index");
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
            clickhouse_event_measures_table,
            clickhouse_entity_state_updates_table,
            clickhouse_definitions_table,
            poll_wait: parse_env("LOADER_POLL_WAIT_SECS", 20)?,
            max_messages: parse_env("LOADER_MAX_MESSAGES", 10)?,
            concurrency: parse_env("LOADER_CONCURRENCY", 4)?,
            visibility_timeout: parse_env("LOADER_VISIBILITY_TIMEOUT_SECS", 300)?,
            request_timeout: Duration::from_secs(parse_env("LOADER_REQUEST_TIMEOUT_SECS", 60)?),
            definitions_refresh_interval: Duration::from_secs(parse_env(
                "LOADER_DEFINITIONS_REFRESH_SECS",
                60,
            )?),
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
            postgres_url: optional("NANOTRACE_POSTGRES_URL"),
            ingest_ledger_enabled: env_bool_default("NANOTRACE_INGEST_LEDGER_ENABLED", true),
            ingest_ledger_stale_after: Duration::from_secs(parse_env(
                "NANOTRACE_INGEST_LEDGER_STALE_SECS",
                3600_u64,
            )?),
            lakehouse_enabled: env_bool("NANOTRACE_LAKEHOUSE_ENABLED"),
            lakehouse_warehouse_dir: PathBuf::from(env_or(
                "NANOTRACE_LAKEHOUSE_WAREHOUSE_DIR",
                "/data/lakehouse",
            )),
            lakehouse_target_file_size_bytes: parse_env(
                "NANOTRACE_ICEBERG_TARGET_FILE_SIZE_BYTES",
                512_u64 * 1024 * 1024,
            )?,
            lakehouse_min_snapshots_to_keep: parse_env(
                "NANOTRACE_ICEBERG_MIN_SNAPSHOTS_TO_KEEP",
                10_000_u64,
            )?,
            lakehouse_max_snapshot_age_ms: parse_env(
                "NANOTRACE_ICEBERG_MAX_SNAPSHOT_AGE_MS",
                7_u64 * 24 * 60 * 60 * 1000,
            )?,
            lakehouse_metadata_previous_versions_max: parse_env(
                "NANOTRACE_ICEBERG_METADATA_PREVIOUS_VERSIONS_MAX",
                100_u64,
            )?,
            iceberg_rest_catalog: iceberg_rest_catalog_from_env(),
            derivation_mode: parse_derivation_mode(
                &env_or("LOADER_DERIVATION_MODE", "raw")
                    .trim()
                    .to_ascii_lowercase(),
            )?,
            clickhouse_insert_max_rows: parse_env(
                "CLICKHOUSE_INSERT_MAX_ROWS",
                DEFAULT_CLICKHOUSE_INSERT_MAX_ROWS,
            )?,
            clickhouse_insert_max_bytes: parse_env(
                "CLICKHOUSE_INSERT_MAX_BYTES",
                DEFAULT_CLICKHOUSE_INSERT_MAX_BYTES,
            )?,
            clickhouse_insert_concurrency: parse_env(
                "CLICKHOUSE_INSERT_CONCURRENCY",
                DEFAULT_CLICKHOUSE_INSERT_CONCURRENCY,
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

fn parse_derivation_mode(value: &str) -> Result<DerivationMode> {
    match value {
        "raw" | "none" | "off" => Ok(DerivationMode::Raw),
        "promoted" | "schema" | "definitions" => Ok(DerivationMode::Promoted),
        _ => bail!("LOADER_DERIVATION_MODE must be raw or promoted; got {value}"),
    }
}

impl IngestLedger {
    async fn from_config(cfg: &Config) -> Result<Option<Self>> {
        if !cfg.ingest_ledger_enabled {
            return Ok(None);
        }
        let Some(postgres_url) = cfg.postgres_url.as_ref() else {
            return Ok(None);
        };
        let pg = PgPoolOptions::new()
            .max_connections(4)
            .connect(postgres_url)
            .await
            .context("connect ingest ledger postgres")?;
        let ledger = Self {
            pg,
            owner: ledger_owner(),
            stale_after: cfg.ingest_ledger_stale_after,
        };
        ledger.ensure_table().await?;
        Ok(Some(ledger))
    }

    async fn ensure_table(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nanotrace_ingest_batches (
                source_batch_id text PRIMARY KEY,
                status text NOT NULL,
                owner text NOT NULL,
                source_objects jsonb NOT NULL,
                rows bigint NOT NULL,
                attempts bigint NOT NULL DEFAULT 1,
                lakehouse_snapshot_id text,
                lakehouse_sequence_number bigint,
                acquired_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now(),
                completed_at timestamptz,
                error text
            )",
        )
        .execute(&self.pg)
        .await
        .context("create ingest ledger table")?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS nanotrace_ingest_batches_status_idx
             ON nanotrace_ingest_batches (status, updated_at)",
        )
        .execute(&self.pg)
        .await
        .context("create ingest ledger status index")?;
        Ok(())
    }

    async fn acquire(&self, source_batch_id: &str, objects: &[ObjectRef]) -> Result<LedgerAcquire> {
        let source_objects = source_objects_json(objects);
        let inserted: Option<String> = sqlx::query_scalar(
            "INSERT INTO nanotrace_ingest_batches
                (source_batch_id, status, owner, source_objects, rows)
             VALUES ($1, 'processing', $2, $3, $4)
             ON CONFLICT DO NOTHING
             RETURNING source_batch_id",
        )
        .bind(source_batch_id)
        .bind(&self.owner)
        .bind(source_objects)
        .bind(0_i64)
        .fetch_optional(&self.pg)
        .await
        .context("insert ingest ledger row")?;
        if inserted.is_some() {
            return Ok(LedgerAcquire::Acquired);
        }

        let status: Option<String> = sqlx::query_scalar(
            "SELECT status FROM nanotrace_ingest_batches WHERE source_batch_id = $1",
        )
        .bind(source_batch_id)
        .fetch_optional(&self.pg)
        .await
        .context("read ingest ledger status")?;
        if status.as_deref() == Some("completed") {
            return Ok(LedgerAcquire::AlreadyCompleted);
        }

        let stale_secs = i64::try_from(self.stale_after.as_secs()).unwrap_or(i64::MAX);
        let reclaimed: Option<String> = sqlx::query_scalar(
            "UPDATE nanotrace_ingest_batches
             SET status = 'processing',
                 owner = $2,
                 source_objects = $3,
                 attempts = attempts + 1,
                 updated_at = now(),
                 error = NULL
             WHERE source_batch_id = $1
               AND status <> 'completed'
               AND updated_at < now() - ($4::bigint * interval '1 second')
             RETURNING source_batch_id",
        )
        .bind(source_batch_id)
        .bind(&self.owner)
        .bind(source_objects_json(objects))
        .bind(stale_secs)
        .fetch_optional(&self.pg)
        .await
        .context("reclaim stale ingest ledger row")?;
        if reclaimed.is_some() {
            Ok(LedgerAcquire::Acquired)
        } else {
            Ok(LedgerAcquire::InProgress)
        }
    }

    async fn complete(
        &self,
        source_batch_id: &str,
        prepared: &[PreparedObject],
        rows: usize,
        commit: Option<&LakehouseCommit>,
    ) -> Result<()> {
        let snapshot_id = commit.map(|commit| commit.snapshot_id.as_str());
        let sequence_number =
            commit.map(|commit| i64::try_from(commit.sequence_number).unwrap_or(i64::MAX));
        let rows = i64::try_from(rows).unwrap_or(i64::MAX);
        sqlx::query(
            "UPDATE nanotrace_ingest_batches
             SET status = 'completed',
                 source_objects = $3,
                 rows = $4,
                 lakehouse_snapshot_id = $5,
                 lakehouse_sequence_number = $6,
                 updated_at = now(),
                 completed_at = now(),
                 error = NULL
             WHERE source_batch_id = $1 AND owner = $2",
        )
        .bind(source_batch_id)
        .bind(&self.owner)
        .bind(source_objects_json_from_prepared(prepared))
        .bind(rows)
        .bind(snapshot_id)
        .bind(sequence_number)
        .execute(&self.pg)
        .await
        .context("update ingest ledger completion")?;
        Ok(())
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

        let messages: Vec<Message> = output.messages().to_vec();
        if messages.is_empty() {
            return Ok(());
        }

        self.process_message_batch(messages).await
    }

    async fn process_message_batch(&self, messages: Vec<Message>) -> Result<()> {
        let mut objects = Vec::new();
        for message in &messages {
            let body = message
                .body()
                .ok_or_else(|| anyhow!("SQS message missing body"))?;
            objects.extend(
                parse_s3_records(body)?
                    .into_iter()
                    .map(|(bucket, key)| ObjectRef { bucket, key }),
            );
        }

        let objects = normalize_objects(objects);
        if objects.is_empty() {
            self.delete_messages(&messages).await?;
            return Ok(());
        }

        let token_prefix = batch_token_prefix(&objects);
        if let Some(ledger) = self.ingest_ledger.as_ref() {
            match ledger
                .acquire(&token_prefix, &objects)
                .await
                .context("acquire ingest ledger batch")?
            {
                LedgerAcquire::Acquired => {}
                LedgerAcquire::AlreadyCompleted => {
                    info!(
                        source_batch_id = token_prefix,
                        "skipping already completed ingest batch"
                    );
                    self.delete_messages(&messages).await?;
                    return Ok(());
                }
                LedgerAcquire::InProgress => {
                    info!(
                        source_batch_id = token_prefix,
                        "leaving ingest batch for active ledger owner"
                    );
                    return Ok(());
                }
            }
        }

        let prepared = self.prepare_batch(objects).await?;
        if prepared.is_empty() {
            if let Some(ledger) = self.ingest_ledger.as_ref() {
                ledger
                    .complete(&token_prefix, &prepared, 0, None)
                    .await
                    .context("complete empty ingest ledger batch")?;
            }
            self.delete_messages(&messages).await?;
            return Ok(());
        }

        let batch = BatchBuffers::from_prepared(&prepared);

        let lakehouse_commit_started = Instant::now();
        let lakehouse_commit = if self.cfg.lakehouse_enabled {
            let mut cfg = LakehouseConfig::events_table(self.cfg.lakehouse_warehouse_dir.clone())
                .with_write_target_file_size_bytes(self.cfg.lakehouse_target_file_size_bytes)
                .with_snapshot_retention(
                    self.cfg.lakehouse_min_snapshots_to_keep,
                    self.cfg.lakehouse_max_snapshot_age_ms,
                    self.cfg.lakehouse_metadata_previous_versions_max,
                );
            if let Some(rest) = self.cfg.iceberg_rest_catalog.clone() {
                cfg = cfg.with_rest_catalog(rest);
            }
            Some(
                tokio::task::spawn_blocking({
                    let raw = batch.raw.clone();
                    let source_batch_id = token_prefix.clone();
                    move || {
                        nanotrace_lakehouse::commit_events_ndjson_with_source(
                            &cfg,
                            &raw,
                            Some(&source_batch_id),
                        )
                    }
                })
                .await
                .context("join lakehouse commit task")?
                .context("commit events to lakehouse")?,
            )
        } else {
            None
        };
        let lakehouse_commit_ms = elapsed_ms(lakehouse_commit_started);

        let raw_insert_started = Instant::now();
        self.insert_clickhouse(&self.cfg.table_name(), &batch.raw, &token_prefix, true)
            .await?;
        let raw_insert_ms = elapsed_ms(raw_insert_started);

        let derived_insert_started = Instant::now();
        self.insert_derived_tables(&batch, &token_prefix).await?;
        let derived_insert_ms = elapsed_ms(derived_insert_started);

        if let Some(commit) = lakehouse_commit.as_ref() {
            self.insert_lakehouse_serving_metadata(commit, &batch, &token_prefix)
                .await?;
        }

        if let Some(ledger) = self.ingest_ledger.as_ref() {
            ledger
                .complete(
                    &token_prefix,
                    &prepared,
                    batch.rows,
                    lakehouse_commit.as_ref(),
                )
                .await
                .context("complete ingest ledger batch")?;
        }

        self.delete_messages(&messages).await?;

        info!(
            objects = batch.objects,
            rows = batch.rows,
            bytes = batch.raw_bytes,
            field_index_bytes = batch.field_index_bytes,
            event_measure_bytes = batch.event_measure_bytes,
            entity_state_update_bytes = batch.entity_state_update_bytes,
            s3_ms = batch.s3_ms,
            transform_ms = batch.transform_ms,
            definitions_ms = batch.definitions_ms,
            derive_ms = batch.derive_ms,
            lakehouse_enabled = self.cfg.lakehouse_enabled,
            lakehouse_snapshot_id = lakehouse_commit
                .as_ref()
                .map(|commit| commit.snapshot_id.as_str()),
            lakehouse_sequence_number = lakehouse_commit
                .as_ref()
                .map(|commit| commit.sequence_number),
            lakehouse_deduplicated = lakehouse_commit.as_ref().map(|commit| commit.deduplicated),
            lakehouse_commit_ms,
            raw_insert_ms,
            derived_insert_ms,
            "loaded event object batch"
        );

        Ok(())
    }

    async fn insert_derived_tables(&self, batch: &BatchBuffers, token_prefix: &str) -> Result<()> {
        let field_index_table = self.cfg.field_index_table_name();
        let event_measures_table = self.cfg.event_measures_table_name();
        let entity_state_updates_table = self.cfg.entity_state_updates_table_name();
        let field_index_token_prefix = format!("{token_prefix}:field_index");
        let event_measures_token_prefix = format!("{token_prefix}:event_measures");
        let entity_state_updates_token_prefix = format!("{token_prefix}:entity_state_updates");

        tokio::try_join!(
            self.insert_clickhouse(
                &field_index_table,
                &batch.field_index,
                &field_index_token_prefix,
                true,
            ),
            self.insert_clickhouse(
                &event_measures_table,
                &batch.event_measures,
                &event_measures_token_prefix,
                true,
            ),
            self.insert_clickhouse(
                &entity_state_updates_table,
                &batch.entity_state_updates,
                &entity_state_updates_token_prefix,
                true,
            ),
        )?;

        Ok(())
    }

    async fn insert_lakehouse_serving_metadata(
        &self,
        commit: &LakehouseCommit,
        batch: &BatchBuffers,
        token_prefix: &str,
    ) -> Result<()> {
        let commit_row = LakehouseCommitRow {
            namespace: &commit.namespace,
            table_name: &commit.table,
            snapshot_id: &commit.snapshot_id,
            sequence_number: commit.sequence_number,
            committed_at_ms: commit.committed_at_ms.try_into().unwrap_or(0),
            data_file: &commit.data_file,
            data_files: &commit.data_files,
            record_count: commit.record_count.try_into().unwrap_or(u64::MAX),
            content_sha256: &commit.content_sha256,
            metadata_location: &commit.metadata_location,
            source_batch_id: commit.source_batch_id.as_deref().unwrap_or(""),
            deduplicated: u8::from(commit.deduplicated),
        };
        let mut watermark_rows = vec![ServingWatermarkRow {
            serving_table: &self.cfg.clickhouse_table,
            source_namespace: &commit.namespace,
            source_table: &commit.table,
            source_snapshot_id: &commit.snapshot_id,
            source_sequence_number: commit.sequence_number,
            source_record_count: commit.record_count.try_into().unwrap_or(u64::MAX),
            status: "loaded",
            attributes: serde_json::json!({
                "data_file": &commit.data_file,
                "data_files": &commit.data_files,
                "content_sha256": &commit.content_sha256,
                "metadata_location": &commit.metadata_location,
                "source_batch_id": &commit.source_batch_id,
                "deduplicated": commit.deduplicated,
            }),
        }];

        let plan = self.cfg.derivation_mode.plan();
        if plan.promoted_fields {
            watermark_rows.push(ServingWatermarkRow {
                serving_table: &self.cfg.clickhouse_field_index_table,
                source_namespace: &commit.namespace,
                source_table: &commit.table,
                source_snapshot_id: &commit.snapshot_id,
                source_sequence_number: commit.sequence_number,
                source_record_count: commit.record_count.try_into().unwrap_or(u64::MAX),
                status: "loaded",
                attributes: serde_json::json!({
                    "metadata_location": &commit.metadata_location,
                    "source_batch_id": &commit.source_batch_id,
                    "field_index_rows": count_ndjson_rows(&batch.field_index),
                }),
            });
        }
        if plan.promoted_measures {
            watermark_rows.push(ServingWatermarkRow {
                serving_table: &self.cfg.clickhouse_event_measures_table,
                source_namespace: &commit.namespace,
                source_table: &commit.table,
                source_snapshot_id: &commit.snapshot_id,
                source_sequence_number: commit.sequence_number,
                source_record_count: commit.record_count.try_into().unwrap_or(u64::MAX),
                status: "loaded",
                attributes: serde_json::json!({
                    "metadata_location": &commit.metadata_location,
                    "source_batch_id": &commit.source_batch_id,
                    "event_measure_rows": count_ndjson_rows(&batch.event_measures),
                }),
            });
        }
        if plan.promoted_states {
            watermark_rows.push(ServingWatermarkRow {
                serving_table: &self.cfg.clickhouse_entity_state_updates_table,
                source_namespace: &commit.namespace,
                source_table: &commit.table,
                source_snapshot_id: &commit.snapshot_id,
                source_sequence_number: commit.sequence_number,
                source_record_count: commit.record_count.try_into().unwrap_or(u64::MAX),
                status: "loaded",
                attributes: serde_json::json!({
                    "metadata_location": &commit.metadata_location,
                    "source_batch_id": &commit.source_batch_id,
                    "entity_state_update_rows": count_ndjson_rows(&batch.entity_state_updates),
                }),
            });
        }

        let mut commit_body = Vec::new();
        serde_json::to_writer(&mut commit_body, &commit_row)
            .context("serialize lakehouse commit row")?;
        commit_body.push(b'\n');

        let mut watermark_body = Vec::new();
        for row in &watermark_rows {
            serde_json::to_writer(&mut watermark_body, row)
                .context("serialize serving watermark row")?;
            watermark_body.push(b'\n');
        }

        let lakehouse_commits_table = format!("{}.lakehouse_commits", self.cfg.clickhouse_database);
        let serving_watermarks_table =
            format!("{}.serving_watermarks", self.cfg.clickhouse_database);
        let lakehouse_commits_token = format!("{token_prefix}:lakehouse_commits");
        let serving_watermarks_token = format!("{token_prefix}:serving_watermarks");
        tokio::try_join!(
            self.insert_clickhouse(
                &lakehouse_commits_table,
                &commit_body,
                &lakehouse_commits_token,
                false,
            ),
            self.insert_clickhouse(
                &serving_watermarks_table,
                &watermark_body,
                &serving_watermarks_token,
                false,
            ),
        )?;
        Ok(())
    }

    async fn prepare_batch(&self, objects: Vec<ObjectRef>) -> Result<Vec<PreparedObject>> {
        let mut unique = BTreeSet::new();
        let mut to_prepare = Vec::new();
        for object_ref in objects {
            if !unique.insert((object_ref.bucket.clone(), object_ref.key.clone())) {
                continue;
            }
            to_prepare.push(object_ref);
        }

        let concurrency = self.cfg.concurrency.max(1);
        let mut prepared = Vec::new();
        for chunk in to_prepare.chunks(concurrency) {
            let mut tasks = JoinSet::new();
            for object in chunk.iter().cloned() {
                let loader = self.clone();
                tasks.spawn(async move { loader.prepare_object(object).await });
            }
            while let Some(result) = tasks.join_next().await {
                match result {
                    Ok(Ok(Some(object))) => prepared.push(object),
                    Ok(Ok(None)) => {}
                    Ok(Err(err)) => return Err(err),
                    Err(err) => return Err(anyhow!("loader task failed: {err}")),
                }
            }
        }

        prepared.sort_by(|left, right| {
            left.object_ref
                .bucket
                .cmp(&right.object_ref.bucket)
                .then_with(|| left.object_ref.key.cmp(&right.object_ref.key))
        });
        Ok(prepared)
    }

    async fn prepare_object(&self, object_ref: ObjectRef) -> Result<Option<PreparedObject>> {
        let bucket = object_ref.bucket.as_str();
        let key = object_ref.key.as_str();
        let s3_started = Instant::now();
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
        let s3_ms = elapsed_ms(s3_started);

        if bytes.is_empty() {
            warn!(bucket, key, "skipping empty event object");
            return Ok(None);
        }

        let transform_started = Instant::now();
        let processed = self
            .processors
            .transform_ndjson(&bytes)
            .context("transform event object")?;
        let transform_ms = elapsed_ms(transform_started);
        let rows = count_ndjson_rows(&processed);

        if rows == 0 {
            warn!(
                bucket,
                key,
                bytes = processed.len(),
                "skipping object without complete rows"
            );
            return Ok(None);
        }

        let derive_started = Instant::now();
        let plan = self.cfg.derivation_mode.plan();
        let definitions_started = Instant::now();
        let definitions = if self.cfg.derivation_mode.needs_definitions() {
            self.cached_definitions().await.unwrap_or_else(|err| {
                warn!(error = %err, "failed to load dynamic definitions; continuing without promoted derivation");
                ExtractionDefinitions::default()
            })
        } else {
            ExtractionDefinitions::default()
        };
        let definitions_ms = elapsed_ms(definitions_started);
        let derived =
            derived_buffers(&processed, plan, &definitions).context("derive event rows")?;
        let derive_ms = elapsed_ms(derive_started);

        Ok(Some(PreparedObject {
            object_ref,
            rows,
            raw: processed,
            derived,
            s3_ms,
            transform_ms,
            definitions_ms,
            derive_ms,
        }))
    }

    async fn delete_messages(&self, messages: &[Message]) -> Result<()> {
        for message in messages {
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
        }
        Ok(())
    }

    async fn cached_definitions(&self) -> Result<ExtractionDefinitions> {
        let now = Instant::now();
        {
            let cached = self.definitions_cache.read().await;
            if cached.fetched_at.is_some_and(|fetched_at| {
                now.duration_since(fetched_at) < self.cfg.definitions_refresh_interval
            }) {
                return Ok(cached.definitions.clone());
            }
        }

        let mut cached = self.definitions_cache.write().await;
        if cached.fetched_at.is_some_and(|fetched_at| {
            now.duration_since(fetched_at) < self.cfg.definitions_refresh_interval
        }) {
            return Ok(cached.definitions.clone());
        }

        let definitions = self.active_definitions().await?;
        cached.definitions = definitions.clone();
        cached.fetched_at = Some(Instant::now());
        Ok(definitions)
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

        let insert_concurrency = self.cfg.clickhouse_insert_concurrency.max(1);
        let mut tasks = JoinSet::new();
        for (chunk_index, chunk) in ndjson_chunks(
            body,
            self.cfg.clickhouse_insert_max_rows,
            self.cfg.clickhouse_insert_max_bytes,
        )
        .into_iter()
        .enumerate()
        {
            let loader = self.clone();
            let table = table.to_string();
            let token_prefix = token_prefix.to_string();
            let chunk = chunk.to_vec();
            tasks.spawn(async move {
                loader
                    .insert_clickhouse_chunk(
                        &table,
                        chunk,
                        &token_prefix,
                        chunk_index,
                        async_insert,
                    )
                    .await
            });

            if tasks.len() >= insert_concurrency {
                wait_for_insert_task(&mut tasks).await?;
            }
        }

        while !tasks.is_empty() {
            wait_for_insert_task(&mut tasks).await?;
        }

        Ok(())
    }

    async fn insert_clickhouse_chunk(
        &self,
        table: &str,
        body: Vec<u8>,
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
            .body(body);

        let dedupe_token = insert_deduplication_token(token_prefix, table, chunk_index);
        request = request.query(&[("insert_deduplication_token", dedupe_token.as_str())]);
        if async_insert {
            request = request.query(&[
                ("async_insert", "1"),
                ("wait_for_async_insert", "0"),
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

async fn wait_for_insert_task(tasks: &mut JoinSet<Result<()>>) -> Result<()> {
    match tasks.join_next().await {
        Some(Ok(result)) => result,
        Some(Err(err)) => Err(anyhow!("ClickHouse insert task failed: {err}")),
        None => Ok(()),
    }
}

fn derived_buffers(
    bytes: &[u8],
    plan: DerivationPlan,
    definitions: &ExtractionDefinitions,
) -> Result<DerivedBuffers> {
    let mut buffers = DerivedBuffers::default();
    if !plan.promoted_fields && !plan.promoted_measures && !plan.promoted_states {
        buffers.rows = count_ndjson_rows(bytes);
        return Ok(buffers);
    }

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

        if plan.promoted_fields {
            for field_row in field_index_rows(row, definitions) {
                serde_json::to_writer(&mut buffers.field_index, &field_row)
                    .context("serialize field index row")?;
                buffers.field_index.push(b'\n');
            }
        }

        if plan.promoted_measures {
            for measure in event_measure_rows(row, definitions) {
                serde_json::to_writer(&mut buffers.event_measures, &measure)
                    .context("serialize event measure row")?;
                buffers.event_measures.push(b'\n');
            }
        }

        if plan.promoted_states {
            for state_update in entity_state_update_rows(row, definitions) {
                serde_json::to_writer(&mut buffers.entity_state_updates, &state_update)
                    .context("serialize entity state update row")?;
                buffers.entity_state_updates.push(b'\n');
            }
        }
    }
    Ok(buffers)
}

#[cfg(test)]
fn field_index_ndjson(bytes: &[u8], definitions: &ExtractionDefinitions) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let row: Value = serde_json::from_slice(line).context("parse event row for field index")?;
        let Some(row) = row.as_object() else {
            continue;
        };
        for field_row in field_index_rows(row, definitions) {
            serde_json::to_writer(&mut out, &field_row).context("serialize field index row")?;
            out.push(b'\n');
        }
    }
    Ok(out)
}

fn field_index_rows(
    row: &Map<String, Value>,
    definitions: &ExtractionDefinitions,
) -> Vec<FieldIndexRow> {
    let timestamp = string_value(row.get("timestamp"));
    if timestamp.is_empty() {
        return Vec::new();
    }
    let Some(data) = row.get("data").and_then(Value::as_object) else {
        return Vec::new();
    };
    let context = EventContext::from_row(row, data, timestamp);
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
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
    context: &EventContext,
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
    context: &EventContext,
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
    let context = EventContext::from_row(row, data, timestamp);
    let mut rows = Vec::new();
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
    let context = EventContext::from_row(row, data, timestamp);
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

struct EventContext {
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

impl EventContext {
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

fn normalize_objects(objects: Vec<ObjectRef>) -> Vec<ObjectRef> {
    let mut unique = BTreeSet::new();
    let mut normalized = Vec::new();
    for object in objects {
        if unique.insert((object.bucket.clone(), object.key.clone())) {
            normalized.push(object);
        }
    }
    normalized.sort_by(|left, right| {
        left.bucket
            .cmp(&right.bucket)
            .then_with(|| left.key.cmp(&right.key))
    });
    normalized
}

fn batch_token_prefix(objects: &[ObjectRef]) -> String {
    let mut hasher = Sha256::new();
    for object in objects {
        hasher.update(object.bucket.as_bytes());
        hasher.update(b"\0");
        hasher.update(object.key.as_bytes());
        hasher.update(b"\0");
    }
    format!("s3-batch:{:x}", hasher.finalize())
}

fn source_objects_json(objects: &[ObjectRef]) -> Value {
    Value::Array(
        objects
            .iter()
            .map(|object| {
                serde_json::json!({
                    "bucket": object.bucket,
                    "key": object.key,
                })
            })
            .collect(),
    )
}

fn source_objects_json_from_prepared(prepared: &[PreparedObject]) -> Value {
    Value::Array(
        prepared
            .iter()
            .map(|object| {
                serde_json::json!({
                    "bucket": object.object_ref.bucket,
                    "key": object.object_ref.key,
                    "rows": object.rows,
                    "bytes": object.raw.len(),
                })
            })
            .collect(),
    )
}

fn ledger_owner() -> String {
    let host = env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string());
    format!("{host}:{}", std::process::id())
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
        DerivationMode, ExtractionDefinitions, FieldDefinition, MeasureDefinition, ObjectRef,
        StateDefinition, batch_token_prefix, count_ndjson_rows, derived_buffers,
        entity_state_updates_ndjson, event_measures_ndjson, field_index_ndjson, normalize_objects,
        parse_derivation_mode, parse_s3_records,
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
    fn normalizes_objects_before_batch_token() {
        let normalized = normalize_objects(vec![
            ObjectRef {
                bucket: "b".to_string(),
                key: "z".to_string(),
            },
            ObjectRef {
                bucket: "a".to_string(),
                key: "x".to_string(),
            },
            ObjectRef {
                bucket: "a".to_string(),
                key: "x".to_string(),
            },
        ]);

        assert_eq!(
            normalized,
            vec![
                ObjectRef {
                    bucket: "a".to_string(),
                    key: "x".to_string(),
                },
                ObjectRef {
                    bucket: "b".to_string(),
                    key: "z".to_string(),
                },
            ]
        );
        assert_eq!(
            batch_token_prefix(&normalized),
            batch_token_prefix(&normalize_objects(vec![
                ObjectRef {
                    bucket: "b".to_string(),
                    key: "z".to_string(),
                },
                ObjectRef {
                    bucket: "a".to_string(),
                    key: "x".to_string(),
                },
            ]))
        );
    }

    #[test]
    fn parses_derivation_mode_aliases() {
        assert_eq!(parse_derivation_mode("raw").unwrap(), DerivationMode::Raw);
        assert_eq!(
            parse_derivation_mode("promoted").unwrap(),
            DerivationMode::Promoted
        );
        assert!(parse_derivation_mode("everything").is_err());
    }

    #[test]
    fn raw_derivation_mode_does_not_build_secondary_buffers() {
        let buffers = derived_buffers(
            b"{\"event_id\":\"evt_1\",\"timestamp\":\"2026-05-12T00:00:00.000Z\",\"data\":{\"tenant_id\":\"tenant-a\",\"event_type\":\"span\",\"trace_id\":\"trace-1\",\"span_id\":\"span-1\",\"duration_ms\":12.5,\"revenue\":9.99}}\n",
            DerivationMode::Raw.plan(),
            &ExtractionDefinitions::default(),
        )
        .expect("derive raw mode");

        assert_eq!(buffers.rows, 1);
        assert!(buffers.field_index.is_empty());
        assert!(buffers.event_measures.is_empty());
        assert!(buffers.entity_state_updates.is_empty());
    }

    #[test]
    fn skips_field_index_without_schema_definitions() {
        let index = field_index_ndjson(
            br#"{"event_id":"evt_1","timestamp":"2026-05-12T00:00:00.000Z","data":{"tenant_id":"tenant-a","event_type":"span_start","service":"api","trace_id":"trace-1","span_id":"span-1","name":"GET /users","request_id":"req_1","http":{"route":"/users/:id","method":"GET"}}}"#,
            &ExtractionDefinitions::default(),
        )
        .expect("generate field index");
        assert!(index.is_empty());
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

        let fields = field_index_ndjson(event, &definitions).unwrap();
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
