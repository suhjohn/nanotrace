use std::{
    collections::{BTreeSet, HashMap},
    env, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use nanotrace_lakehouse::LakehouseCommit;
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, file::reader::ChunkReader};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
struct Config {
    warehouse_dir: PathBuf,
    namespace: String,
    source_table: String,
    clickhouse_url: String,
    clickhouse_user: Option<String>,
    clickhouse_password: Option<String>,
    clickhouse_database: String,
    clickhouse_events_table: String,
    clickhouse_field_index_table: String,
    clickhouse_event_measures_table: String,
    clickhouse_entity_state_updates_table: String,
    clickhouse_definitions_table: String,
    truncate_events: bool,
    rebuild_raw: bool,
    rebuild_derived: bool,
    incremental_materialize: bool,
    materialize_loop: bool,
    commit_source: CommitSource,
    materialize_poll_interval: Duration,
    allow_non_empty: bool,
    from_sequence: u64,
    max_rows_per_insert: usize,
    max_bytes_per_insert: usize,
    s3_max_file_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitSource {
    Local,
    ClickHouse,
}

#[derive(Clone)]
struct LakehouseReader {
    s3: S3Client,
    s3_max_file_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DataFileLocation {
    Local(PathBuf),
    S3 { bucket: String, key: String },
}

#[derive(Debug, Serialize)]
struct EventInsertRow {
    event_id: String,
    timestamp: String,
    observed_timestamp: String,
    ingested_timestamp: String,
    source_file: String,
    source_offset: u64,
    source_length: u32,
    data: Value,
}

#[derive(Debug, Deserialize)]
struct ClickHouseJson<T> {
    data: Vec<T>,
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

#[derive(Debug, Default)]
struct MaterializedCounts {
    field_index: usize,
    event_measures: usize,
    entity_state_updates: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MaterializeTargets {
    field_index: bool,
    event_measures: bool,
    entity_state_updates: bool,
}

#[derive(Debug, Clone, Copy)]
struct MaterializeOptions<'a> {
    file_index: usize,
    targets: MaterializeTargets,
    token_namespace: &'a str,
}

impl MaterializeTargets {
    fn all() -> Self {
        Self {
            field_index: true,
            event_measures: true,
            entity_state_updates: true,
        }
    }

    fn any(self) -> bool {
        self.field_index || self.event_measures || self.entity_state_updates
    }
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

#[derive(Debug, Deserialize)]
struct LakehouseCommitRecord {
    namespace: String,
    table_name: String,
    snapshot_id: String,
    sequence_number: u64,
    committed_at_ms: u64,
    data_file: String,
    #[serde(default)]
    data_files: Vec<String>,
    record_count: u64,
    content_sha256: String,
    #[serde(default)]
    metadata_location: String,
    #[serde(default)]
    source_batch_id: String,
    #[serde(default)]
    deduplicated: u8,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::from_env()?;
    let aws_config = aws_config::load_from_env().await;
    let lakehouse_reader = LakehouseReader {
        s3: s3_client(&aws_config),
        s3_max_file_bytes: cfg.s3_max_file_bytes,
    };
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .context("build HTTP client")?;

    if cfg.materialize_loop {
        run_materializer_loop(&client, &lakehouse_reader, &cfg).await?;
        return Ok(());
    }

    let commits = read_commit_records(&client, &cfg).await?;
    if commits.is_empty() {
        bail!("no Nanotrace lakehouse commit records found");
    }

    let definitions = if cfg.rebuild_derived {
        let definitions = active_definitions(&client, &cfg)
            .await
            .context("load active materialization definitions")?;
        println!(
            "materialization_definitions fields={} measures={} states={}",
            definitions.fields.len(),
            definitions.measures.len(),
            definitions.states.len()
        );
        Some(definitions)
    } else {
        None
    };

    if cfg.incremental_materialize {
        let definitions = definitions
            .as_ref()
            .context("incremental materialization requires NANOTRACE_REBUILD_DERIVED=true")?;
        let rows =
            run_incremental_materialize(&client, &lakehouse_reader, &cfg, &commits, definitions)
                .await?;
        println!("incremental_materialized_scanned_rows={rows}");
        return Ok(());
    }

    if cfg.truncate_events {
        clickhouse_query(
            &client,
            &cfg,
            &format!(
                "TRUNCATE TABLE {}.{}",
                quote_ident(&cfg.clickhouse_database)?,
                quote_ident(&cfg.clickhouse_events_table)?
            ),
        )
        .await
        .context("truncate ClickHouse events table")?;
    }
    if cfg.rebuild_raw && !cfg.truncate_events && !cfg.allow_non_empty {
        let existing = clickhouse_count(&client, &cfg, &cfg.qualified_events_table())
            .await
            .context("count existing ClickHouse events")?;
        if existing > 0 {
            bail!(
                "{} already contains {existing} rows; set NANOTRACE_REBUILD_TRUNCATE=true for a destructive refill, NANOTRACE_REBUILD_RAW=false for materialization only, or NANOTRACE_REBUILD_ALLOW_NON_EMPTY=true to override",
                cfg.qualified_events_table()
            );
        }
    }
    if cfg.rebuild_derived && !cfg.allow_non_empty {
        for table in [
            cfg.qualified_field_index_table(),
            cfg.qualified_event_measures_table(),
            cfg.qualified_entity_state_updates_table(),
        ] {
            let existing = clickhouse_count(&client, &cfg, &table)
                .await
                .with_context(|| format!("count existing rows in {table}"))?;
            if existing > 0 {
                bail!(
                    "{table} already contains {existing} rows; reset serving tables first or set NANOTRACE_REBUILD_ALLOW_NON_EMPTY=true to override"
                );
            }
        }
    }

    let mut total_rows = 0usize;
    for commit in commits {
        let rows = rebuild_commit(
            &client,
            &lakehouse_reader,
            &cfg,
            &commit,
            definitions.as_ref(),
        )
        .await
        .with_context(|| format!("rebuild lakehouse snapshot {}", commit.snapshot_id))?;
        total_rows += rows;
        println!(
            "rebuilt snapshot={} sequence={} rows={}",
            commit.snapshot_id, commit.sequence_number, rows
        );
    }

    println!("rebuilt total_rows={total_rows}");
    Ok(())
}

impl Config {
    fn from_env() -> Result<Self> {
        let clickhouse_database = env_or("CLICKHOUSE_DATABASE", "observatory");
        let clickhouse_events_table = env_or("CLICKHOUSE_TABLE", "events");
        let clickhouse_field_index_table = env_or("CLICKHOUSE_FIELD_INDEX_TABLE", "field_index");
        let clickhouse_event_measures_table =
            env_or("CLICKHOUSE_EVENT_MEASURES_TABLE", "event_measures");
        let clickhouse_entity_state_updates_table = env_or(
            "CLICKHOUSE_ENTITY_STATE_UPDATES_TABLE",
            "entity_state_updates",
        );
        let clickhouse_definitions_table = env_or("CLICKHOUSE_DEFINITIONS_TABLE", "definitions");
        validate_identifier("CLICKHOUSE_DATABASE", &clickhouse_database)?;
        validate_identifier("CLICKHOUSE_TABLE", &clickhouse_events_table)?;
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
            warehouse_dir: PathBuf::from(env_or(
                "NANOTRACE_LAKEHOUSE_WAREHOUSE_DIR",
                "/data/lakehouse",
            )),
            namespace: env_or("NANOTRACE_LAKEHOUSE_NAMESPACE", "nanotrace"),
            source_table: env_or("NANOTRACE_LAKEHOUSE_TABLE", "events"),
            clickhouse_url: env_or("CLICKHOUSE_URL", "http://clickhouse:8123"),
            clickhouse_user: optional("CLICKHOUSE_USER"),
            clickhouse_password: optional("CLICKHOUSE_PASSWORD"),
            clickhouse_database,
            clickhouse_events_table,
            clickhouse_field_index_table,
            clickhouse_event_measures_table,
            clickhouse_entity_state_updates_table,
            clickhouse_definitions_table,
            truncate_events: env_bool("NANOTRACE_REBUILD_TRUNCATE"),
            rebuild_raw: env_bool_default("NANOTRACE_REBUILD_RAW", true),
            rebuild_derived: env_bool_default("NANOTRACE_REBUILD_DERIVED", true),
            incremental_materialize: env_bool("NANOTRACE_MATERIALIZE_INCREMENTAL"),
            materialize_loop: env_bool("NANOTRACE_MATERIALIZE_LOOP"),
            commit_source: parse_commit_source(&env_or("NANOTRACE_REBUILD_COMMIT_SOURCE", "local"))
                .context("parse NANOTRACE_REBUILD_COMMIT_SOURCE")?,
            materialize_poll_interval: Duration::from_secs(optional_u64(
                "NANOTRACE_MATERIALIZE_POLL_SECS",
                5,
            )?),
            allow_non_empty: env_bool("NANOTRACE_REBUILD_ALLOW_NON_EMPTY"),
            from_sequence: optional("NANOTRACE_REBUILD_FROM_SEQUENCE")
                .as_deref()
                .unwrap_or("0")
                .parse()
                .context("parse NANOTRACE_REBUILD_FROM_SEQUENCE")?,
            max_rows_per_insert: optional_usize("CLICKHOUSE_INSERT_MAX_ROWS", 25_000)?,
            max_bytes_per_insert: optional_usize("CLICKHOUSE_INSERT_MAX_BYTES", 8 * 1024 * 1024)?,
            s3_max_file_bytes: optional_u64(
                "NANOTRACE_REBUILD_S3_MAX_FILE_BYTES",
                1024 * 1024 * 1024,
            )?,
        })
    }

    fn metadata_dir(&self) -> PathBuf {
        self.warehouse_dir
            .join(&self.namespace)
            .join(&self.source_table)
            .join("metadata")
    }

    fn qualified_events_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_events_table
        )
    }

    fn qualified_field_index_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_field_index_table
        )
    }

    fn qualified_event_measures_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_event_measures_table
        )
    }

    fn qualified_entity_state_updates_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_entity_state_updates_table
        )
    }

    fn qualified_definitions_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_definitions_table
        )
    }

    fn qualified_lakehouse_commits_table(&self) -> String {
        format!("{}.lakehouse_commits", self.clickhouse_database)
    }

    fn qualified_serving_watermarks_table(&self) -> String {
        format!("{}.serving_watermarks", self.clickhouse_database)
    }
}

fn s3_client(config: &aws_config::SdkConfig) -> S3Client {
    let mut builder = aws_sdk_s3::config::Builder::from(config);
    if env_bool("AWS_S3_FORCE_PATH_STYLE") || env_bool("AWS_S3_PATH_STYLE") {
        builder.set_force_path_style(Some(true));
    }
    S3Client::from_conf(builder.build())
}

fn parse_commit_source(value: &str) -> Result<CommitSource> {
    match value.trim().to_ascii_lowercase().as_str() {
        "local" | "files" | "filesystem" => Ok(CommitSource::Local),
        "clickhouse" | "metadata" | "shared" => Ok(CommitSource::ClickHouse),
        _ => bail!("commit source must be local or clickhouse"),
    }
}

async fn rebuild_commit(
    client: &Client,
    lakehouse_reader: &LakehouseReader,
    cfg: &Config,
    commit: &LakehouseCommit,
    definitions: Option<&ExtractionDefinitions>,
) -> Result<usize> {
    let files = if commit.data_files.is_empty() {
        vec![commit.data_file.clone()]
    } else {
        commit.data_files.clone()
    };

    let mut rebuilt_rows = 0usize;
    let mut materialized = MaterializedCounts::default();
    for (file_index, data_file) in files.iter().enumerate() {
        let rows = lakehouse_reader
            .read_event_rows(data_file)
            .await
            .with_context(|| format!("read lakehouse data file {data_file}"))?;
        if rows.is_empty() {
            continue;
        }
        if cfg.rebuild_raw {
            let body = rows_to_ndjson(&rows).context("serialize event insert rows")?;
            let token_prefix = format!(
                "lakehouse-rebuild:{}:{}:{}",
                commit.namespace, commit.snapshot_id, file_index
            );
            insert_clickhouse(
                client,
                cfg,
                &cfg.qualified_events_table(),
                &body,
                &token_prefix,
            )
            .await
            .context("insert rebuilt events")?;
        }
        rebuilt_rows += rows.len();

        if let Some(definitions) = definitions {
            let counts = materialize_rows(
                client,
                cfg,
                commit,
                &rows,
                definitions,
                MaterializeOptions {
                    file_index,
                    targets: MaterializeTargets::all(),
                    token_namespace: "lakehouse-materialize",
                },
            )
            .await?;
            materialized.field_index += counts.field_index;
            materialized.event_measures += counts.event_measures;
            materialized.entity_state_updates += counts.entity_state_updates;
        }
    }

    insert_commit_metadata(
        client,
        cfg,
        commit,
        rebuilt_rows,
        definitions.is_some(),
        &materialized,
    )
    .await
    .context("insert rebuild metadata")?;
    Ok(rebuilt_rows)
}

async fn run_incremental_materialize(
    client: &Client,
    lakehouse_reader: &LakehouseReader,
    cfg: &Config,
    commits: &[LakehouseCommit],
    definitions: &ExtractionDefinitions,
) -> Result<usize> {
    let mut watermarks = derived_watermark_sequences(client, cfg).await?;
    let mut scanned_rows = 0usize;
    for commit in commits {
        let targets = materialize_targets_for_commit(cfg, commit.sequence_number, &watermarks);
        if !targets.any() {
            continue;
        }

        let files = if commit.data_files.is_empty() {
            vec![commit.data_file.clone()]
        } else {
            commit.data_files.clone()
        };
        let mut commit_scanned_rows = 0usize;
        let mut materialized = MaterializedCounts::default();
        for (file_index, data_file) in files.iter().enumerate() {
            let rows = lakehouse_reader
                .read_event_rows(data_file)
                .await
                .with_context(|| format!("read lakehouse data file {data_file}"))?;
            scanned_rows += rows.len();
            commit_scanned_rows += rows.len();
            let counts = materialize_rows(
                client,
                cfg,
                commit,
                &rows,
                definitions,
                MaterializeOptions {
                    file_index,
                    targets,
                    token_namespace: "lakehouse-incremental-materialize",
                },
            )
            .await?;
            materialized.field_index += counts.field_index;
            materialized.event_measures += counts.event_measures;
            materialized.entity_state_updates += counts.entity_state_updates;
        }
        insert_incremental_materialization_metadata(client, cfg, commit, &materialized, targets)
            .await?;
        if targets.field_index {
            watermarks.insert(
                cfg.clickhouse_field_index_table.clone(),
                commit.sequence_number,
            );
        }
        if targets.event_measures {
            watermarks.insert(
                cfg.clickhouse_event_measures_table.clone(),
                commit.sequence_number,
            );
        }
        if targets.entity_state_updates {
            watermarks.insert(
                cfg.clickhouse_entity_state_updates_table.clone(),
                commit.sequence_number,
            );
        }
        println!(
            "materialized snapshot={} sequence={} scanned_rows={} field_index_rows={} event_measure_rows={} entity_state_update_rows={}",
            commit.snapshot_id,
            commit.sequence_number,
            commit_scanned_rows,
            materialized.field_index,
            materialized.event_measures,
            materialized.entity_state_updates
        );
    }
    Ok(scanned_rows)
}

async fn run_materializer_loop(
    client: &Client,
    lakehouse_reader: &LakehouseReader,
    cfg: &Config,
) -> Result<()> {
    println!(
        "lakehouse materializer loop starting namespace={} table={} poll_secs={}",
        cfg.namespace,
        cfg.source_table,
        cfg.materialize_poll_interval.as_secs()
    );

    loop {
        match run_materializer_pass(client, lakehouse_reader, cfg).await {
            Ok(rows) => {
                println!("lakehouse materializer pass complete scanned_rows={rows}");
            }
            Err(err) => {
                eprintln!("lakehouse materializer pass failed: {err:?}");
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(cfg.materialize_poll_interval) => {}
            _ = shutdown_signal() => {
                println!("lakehouse materializer loop stopping");
                return Ok(());
            }
        }
    }
}

async fn run_materializer_pass(
    client: &Client,
    lakehouse_reader: &LakehouseReader,
    cfg: &Config,
) -> Result<usize> {
    let commits = read_available_commit_records(client, cfg).await?;
    if commits.is_empty() {
        return Ok(0);
    }
    let definitions = active_definitions(client, cfg)
        .await
        .context("load active materialization definitions")?;
    println!(
        "materialization_definitions fields={} measures={} states={}",
        definitions.fields.len(),
        definitions.measures.len(),
        definitions.states.len()
    );
    run_incremental_materialize(client, lakehouse_reader, cfg, &commits, &definitions).await
}

fn materialize_targets_for_commit(
    cfg: &Config,
    sequence_number: u64,
    watermarks: &HashMap<String, u64>,
) -> MaterializeTargets {
    MaterializeTargets {
        field_index: watermarks
            .get(&cfg.clickhouse_field_index_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
        event_measures: watermarks
            .get(&cfg.clickhouse_event_measures_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
        entity_state_updates: watermarks
            .get(&cfg.clickhouse_entity_state_updates_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
    }
}

async fn materialize_rows(
    client: &Client,
    cfg: &Config,
    commit: &LakehouseCommit,
    rows: &[EventInsertRow],
    definitions: &ExtractionDefinitions,
    options: MaterializeOptions<'_>,
) -> Result<MaterializedCounts> {
    let mut field_index = Vec::new();
    let mut event_measures = Vec::new();
    let mut entity_state_updates = Vec::new();

    for row in rows {
        if options.targets.field_index {
            field_index.extend(field_index_rows(row, definitions));
        }
        if options.targets.event_measures {
            event_measures.extend(event_measure_rows(row, definitions));
        }
        if options.targets.entity_state_updates {
            entity_state_updates.extend(entity_state_update_rows(row, definitions));
        }
    }

    let counts = MaterializedCounts {
        field_index: field_index.len(),
        event_measures: event_measures.len(),
        entity_state_updates: entity_state_updates.len(),
    };
    let token_prefix = format!(
        "{}:{}:{}:{}",
        options.token_namespace, commit.namespace, commit.snapshot_id, options.file_index
    );

    if !field_index.is_empty() {
        let body = rows_to_ndjson(&field_index)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_field_index_table(),
            &body,
            &format!("{token_prefix}:field_index"),
        )
        .await
        .context("insert materialized field index rows")?;
    }
    if !event_measures.is_empty() {
        let body = rows_to_ndjson(&event_measures)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_event_measures_table(),
            &body,
            &format!("{token_prefix}:event_measures"),
        )
        .await
        .context("insert materialized event measure rows")?;
    }
    if !entity_state_updates.is_empty() {
        let body = rows_to_ndjson(&entity_state_updates)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_entity_state_updates_table(),
            &body,
            &format!("{token_prefix}:entity_state_updates"),
        )
        .await
        .context("insert materialized entity state rows")?;
    }

    Ok(counts)
}

async fn insert_commit_metadata(
    client: &Client,
    cfg: &Config,
    commit: &LakehouseCommit,
    rebuilt_rows: usize,
    materialized_enabled: bool,
    materialized: &MaterializedCounts,
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
    let watermark_row = ServingWatermarkRow {
        serving_table: &cfg.clickhouse_events_table,
        source_namespace: &commit.namespace,
        source_table: &commit.table,
        source_snapshot_id: &commit.snapshot_id,
        source_sequence_number: commit.sequence_number,
        source_record_count: commit.record_count.try_into().unwrap_or(u64::MAX),
        status: "rebuilt",
        attributes: serde_json::json!({
            "data_file": &commit.data_file,
            "data_files": &commit.data_files,
            "content_sha256": &commit.content_sha256,
            "metadata_location": &commit.metadata_location,
            "source_batch_id": &commit.source_batch_id,
            "rebuilt_rows": rebuilt_rows,
            "materialized_enabled": materialized_enabled,
            "field_index_rows": materialized.field_index,
            "event_measure_rows": materialized.event_measures,
            "entity_state_update_rows": materialized.entity_state_updates,
        }),
    };

    let commit_body = rows_to_ndjson(&[commit_row])?;
    let mut watermarks = vec![watermark_row];
    if materialized_enabled {
        watermarks.extend(materialized_watermarks(
            cfg,
            commit,
            materialized,
            MaterializeTargets::all(),
        ));
    }
    let watermark_body = rows_to_ndjson(&watermarks)?;
    let token_prefix = format!(
        "lakehouse-rebuild-metadata:{}:{}",
        commit.namespace, commit.snapshot_id
    );
    insert_clickhouse(
        client,
        cfg,
        &cfg.qualified_lakehouse_commits_table(),
        &commit_body,
        &format!("{token_prefix}:lakehouse_commits"),
    )
    .await?;
    insert_clickhouse(
        client,
        cfg,
        &cfg.qualified_serving_watermarks_table(),
        &watermark_body,
        &format!("{token_prefix}:serving_watermarks"),
    )
    .await?;
    Ok(())
}

async fn insert_incremental_materialization_metadata(
    client: &Client,
    cfg: &Config,
    commit: &LakehouseCommit,
    materialized: &MaterializedCounts,
    targets: MaterializeTargets,
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
    let commit_body = rows_to_ndjson(&[commit_row])?;
    let watermark_body =
        rows_to_ndjson(&materialized_watermarks(cfg, commit, materialized, targets))?;
    let token_prefix = format!(
        "lakehouse-incremental-materialize-metadata:{}:{}",
        commit.namespace, commit.snapshot_id
    );
    insert_clickhouse(
        client,
        cfg,
        &cfg.qualified_lakehouse_commits_table(),
        &commit_body,
        &format!("{token_prefix}:lakehouse_commits"),
    )
    .await?;
    insert_clickhouse(
        client,
        cfg,
        &cfg.qualified_serving_watermarks_table(),
        &watermark_body,
        &format!("{token_prefix}:serving_watermarks"),
    )
    .await?;
    Ok(())
}

fn materialized_watermarks<'a>(
    cfg: &'a Config,
    commit: &'a LakehouseCommit,
    materialized: &MaterializedCounts,
    targets: MaterializeTargets,
) -> Vec<ServingWatermarkRow<'a>> {
    [
        (
            cfg.clickhouse_field_index_table.as_str(),
            "field_index_rows",
            materialized.field_index,
            targets.field_index,
        ),
        (
            cfg.clickhouse_event_measures_table.as_str(),
            "event_measure_rows",
            materialized.event_measures,
            targets.event_measures,
        ),
        (
            cfg.clickhouse_entity_state_updates_table.as_str(),
            "entity_state_update_rows",
            materialized.entity_state_updates,
            targets.entity_state_updates,
        ),
    ]
    .into_iter()
    .filter(|(_, _, _, enabled)| *enabled)
    .map(|(serving_table, row_key, rows, _)| ServingWatermarkRow {
        serving_table,
        source_namespace: &commit.namespace,
        source_table: &commit.table,
        source_snapshot_id: &commit.snapshot_id,
        source_sequence_number: commit.sequence_number,
        source_record_count: commit.record_count.try_into().unwrap_or(u64::MAX),
        status: "materialized",
        attributes: serde_json::json!({
            "metadata_location": &commit.metadata_location,
            "source_batch_id": &commit.source_batch_id,
            row_key: rows,
        }),
    })
    .collect()
}

async fn read_commit_records(client: &Client, cfg: &Config) -> Result<Vec<LakehouseCommit>> {
    match cfg.commit_source {
        CommitSource::Local => {
            let metadata_dir = cfg.metadata_dir();
            read_commit_records_from_dir(cfg, &metadata_dir)
                .with_context(|| format!("read lakehouse metadata dir {}", metadata_dir.display()))
        }
        CommitSource::ClickHouse => read_clickhouse_commit_records(client, cfg).await,
    }
}

async fn read_available_commit_records(
    client: &Client,
    cfg: &Config,
) -> Result<Vec<LakehouseCommit>> {
    match cfg.commit_source {
        CommitSource::Local => {
            let metadata_dir = cfg.metadata_dir();
            if !metadata_dir.exists() {
                return Ok(Vec::new());
            }
            read_commit_records_from_dir(cfg, &metadata_dir)
                .with_context(|| format!("read lakehouse metadata dir {}", metadata_dir.display()))
        }
        CommitSource::ClickHouse => read_clickhouse_commit_records(client, cfg).await,
    }
}

fn read_commit_records_from_dir(cfg: &Config, metadata_dir: &Path) -> Result<Vec<LakehouseCommit>> {
    let mut commits = Vec::new();
    for entry in fs::read_dir(metadata_dir)? {
        let entry = entry.context("read lakehouse metadata entry")?;
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if !file_name.starts_with("snapshot-") || !file_name.ends_with(".nanotrace.json") {
            continue;
        }
        let bytes = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
        let commit: LakehouseCommit =
            serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
        if commit.sequence_number >= cfg.from_sequence {
            commits.push(commit);
        }
    }
    commits.sort_by_key(|commit| commit.sequence_number);
    Ok(commits)
}

async fn read_clickhouse_commit_records(
    client: &Client,
    cfg: &Config,
) -> Result<Vec<LakehouseCommit>> {
    let query = format!(
        "SELECT namespace, table_name, snapshot_id, sequence_number, committed_at_ms, data_file, data_files, record_count, content_sha256, metadata_location, source_batch_id, deduplicated FROM {} FINAL WHERE namespace = '{}' AND table_name = '{}' AND sequence_number >= {} ORDER BY sequence_number, snapshot_id FORMAT JSON",
        cfg.qualified_lakehouse_commits_table(),
        cfg.namespace.replace('\'', "''"),
        cfg.source_table.replace('\'', "''"),
        cfg.from_sequence
    );
    let body = clickhouse_query(client, cfg, &query).await?;
    let response: ClickHouseJson<LakehouseCommitRecord> =
        serde_json::from_str(&body).context("parse lakehouse commits response")?;
    Ok(response
        .data
        .into_iter()
        .map(commit_record_to_commit)
        .collect())
}

fn commit_record_to_commit(row: LakehouseCommitRecord) -> LakehouseCommit {
    let source_batch_id = if row.source_batch_id.is_empty() {
        None
    } else {
        Some(row.source_batch_id)
    };
    LakehouseCommit {
        namespace: row.namespace,
        table: row.table_name,
        snapshot_id: row.snapshot_id,
        sequence_number: row.sequence_number,
        committed_at_ms: row.committed_at_ms.try_into().unwrap_or(i64::MAX),
        data_file: row.data_file,
        data_files: row.data_files,
        record_count: row.record_count.try_into().unwrap_or(usize::MAX),
        content_sha256: row.content_sha256,
        metadata_location: row.metadata_location,
        source_batch_id,
        deduplicated: row.deduplicated != 0,
    }
}

async fn active_definitions(client: &Client, cfg: &Config) -> Result<ExtractionDefinitions> {
    let query = format!(
        "SELECT tenant_id, definition_id, name, kind, mode, config, version FROM {} FINAL WHERE enabled = 1 AND isNull(deleted_at) AND kind IN ('field', 'measure', 'rollup', 'state') FORMAT JSON",
        cfg.qualified_definitions_table()
    );
    let body = clickhouse_query(client, cfg, &query).await?;
    let response: ClickHouseJson<DefinitionRecord> =
        serde_json::from_str(&body).context("parse definitions response")?;
    Ok(ExtractionDefinitions::from_records(response.data))
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
                    fields.push(FieldDefinition {
                        tenant_id: record.tenant_id,
                        definition_id: record.definition_id,
                        definition_version: record.version,
                        name: record.name,
                        mode: if record.mode == "lookup" {
                            "lookup".to_string()
                        } else {
                            "facet".to_string()
                        },
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

fn field_index_rows(
    row: &EventInsertRow,
    definitions: &ExtractionDefinitions,
) -> Vec<FieldIndexRow> {
    let Some(data) = row.data.as_object() else {
        return Vec::new();
    };
    let context = EventContext::from_event(row, data);
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
    row: &EventInsertRow,
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
                collect_field_index_rows(context, row, field, value, seen, rows);
            }
        }
        Value::Object(_) => {}
    }
}

fn push_field_index_row(
    context: &EventContext,
    row: &EventInsertRow,
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

fn event_measure_rows(
    row: &EventInsertRow,
    definitions: &ExtractionDefinitions,
) -> Vec<EventMeasureRow> {
    let Some(data) = row.data.as_object() else {
        return Vec::new();
    };
    let context = EventContext::from_event(row, data);
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

fn entity_state_update_rows(
    row: &EventInsertRow,
    definitions: &ExtractionDefinitions,
) -> Vec<EntityStateUpdateRow> {
    let Some(data) = row.data.as_object() else {
        return Vec::new();
    };
    let context = EventContext::from_event(row, data);
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
    fn from_event(row: &EventInsertRow, data: &Map<String, Value>) -> Self {
        let event_type = string_value(data.get("event_type"));
        let signal = string_value(data.get("signal"))
            .into_non_empty()
            .unwrap_or_else(|| signal_for_event_type(&event_type).to_string());
        Self {
            tenant_id: string_value(data.get("tenant_id")),
            timestamp: row.timestamp.clone(),
            bucket_time: minute_bucket(&row.timestamp).unwrap_or_else(|| row.timestamp.clone()),
            event_id: row.event_id.clone(),
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

impl LakehouseReader {
    async fn read_event_rows(&self, location: &str) -> Result<Vec<EventInsertRow>> {
        match data_file_location(location)? {
            DataFileLocation::Local(path) => read_event_rows_from_local_parquet(&path)
                .with_context(|| format!("read local Parquet {}", path.display())),
            DataFileLocation::S3 { bucket, key } => {
                let bytes = self.read_s3_object(&bucket, &key).await?;
                read_event_rows_from_parquet_reader(bytes)
                    .with_context(|| format!("read s3://{bucket}/{key} Parquet"))
            }
        }
    }

    async fn read_s3_object(&self, bucket: &str, key: &str) -> Result<Bytes> {
        let head = self
            .s3
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("head s3://{bucket}/{key}"))?;
        if let Some(length) = head.content_length()
            && u64::try_from(length).unwrap_or(u64::MAX) > self.s3_max_file_bytes
        {
            bail!(
                "s3://{bucket}/{key} is {length} bytes, above NANOTRACE_REBUILD_S3_MAX_FILE_BYTES={}",
                self.s3_max_file_bytes
            );
        }
        let object = self
            .s3
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("get s3://{bucket}/{key}"))?;
        let bytes = object
            .body
            .collect()
            .await
            .with_context(|| format!("read s3://{bucket}/{key} body"))?
            .into_bytes();
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > self.s3_max_file_bytes {
            bail!(
                "s3://{bucket}/{key} body is {} bytes, above NANOTRACE_REBUILD_S3_MAX_FILE_BYTES={}",
                bytes.len(),
                self.s3_max_file_bytes
            );
        }
        Ok(bytes)
    }
}

fn read_event_rows_from_local_parquet(path: &Path) -> Result<Vec<EventInsertRow>> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    read_event_rows_from_parquet_reader(file)
}

fn read_event_rows_from_parquet_reader<R>(reader: R) -> Result<Vec<EventInsertRow>>
where
    R: ChunkReader + 'static,
{
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(reader).context("open Parquet reader")?;
    let mut reader = builder
        .build()
        .context("build Parquet record batch reader")?;
    let mut rows = Vec::new();
    for batch in &mut reader {
        let batch = batch.context("read Parquet record batch")?;
        rows.extend(record_batch_to_event_rows(&batch)?);
    }
    Ok(rows)
}

fn record_batch_to_event_rows(batch: &RecordBatch) -> Result<Vec<EventInsertRow>> {
    let event_id = string_column(batch, "event_id")?;
    let timestamp = timestamp_column(batch, "timestamp")?;
    let observed_timestamp = optional_timestamp_column(batch, "observed_timestamp")?;
    let ingested_timestamp = timestamp_column(batch, "ingested_timestamp")?;
    let source_file = string_column(batch, "source_file")?;
    let source_offset = int64_column(batch, "source_offset")?;
    let source_length = int64_column(batch, "source_length")?;
    let data = string_column(batch, "data")?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let timestamp_us = timestamp.value(row);
        let observed_us = if observed_timestamp.is_null(row) {
            timestamp_us
        } else {
            observed_timestamp.value(row)
        };
        let data_value = serde_json::from_str::<Value>(data.value(row))
            .with_context(|| format!("parse data JSON for row {row}"))?;
        rows.push(EventInsertRow {
            event_id: event_id.value(row).to_string(),
            timestamp: format_timestamp_us(timestamp_us)?,
            observed_timestamp: format_timestamp_us(observed_us)?,
            ingested_timestamp: format_timestamp_us(ingested_timestamp.value(row))?,
            source_file: source_file.value(row).to_string(),
            source_offset: source_offset.value(row).try_into().unwrap_or(0),
            source_length: source_length.value(row).try_into().unwrap_or(0),
            data: data_value,
        });
    }
    Ok(rows)
}

async fn clickhouse_query(client: &Client, cfg: &Config, query: &str) -> Result<String> {
    let mut request = client.get(&cfg.clickhouse_url).query(&[
        ("database", cfg.clickhouse_database.as_str()),
        ("query", query),
        ("date_time_input_format", "best_effort"),
    ]);
    if let Some(user) = cfg.clickhouse_user.as_deref() {
        request = request.basic_auth(user, cfg.clickhouse_password.as_deref());
    }
    let response = request.send().await.context("send ClickHouse query")?;
    let status = response.status();
    let text = response.text().await.context("read ClickHouse response")?;
    if !status.is_success() {
        bail!("ClickHouse query failed: {status} {text}");
    }
    Ok(text)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn clickhouse_count(client: &Client, cfg: &Config, table: &str) -> Result<u64> {
    let query = format!("SELECT count() FROM {table}");
    let body = clickhouse_query(client, cfg, &query).await?;
    body.trim()
        .parse()
        .with_context(|| format!("parse ClickHouse count response {body:?}"))
}

async fn derived_watermark_sequences(
    client: &Client,
    cfg: &Config,
) -> Result<HashMap<String, u64>> {
    let serving_tables = [
        cfg.clickhouse_field_index_table.as_str(),
        cfg.clickhouse_event_measures_table.as_str(),
        cfg.clickhouse_entity_state_updates_table.as_str(),
    ]
    .into_iter()
    .map(|table| format!("'{}'", table.replace('\'', "''")))
    .collect::<Vec<_>>()
    .join(",");
    let query = format!(
        "SELECT serving_table, max(source_sequence_number) AS source_sequence_number FROM {} WHERE source_namespace = '{}' AND source_table = '{}' AND serving_table IN ({serving_tables}) GROUP BY serving_table FORMAT JSON",
        cfg.qualified_serving_watermarks_table(),
        cfg.namespace.replace('\'', "''"),
        cfg.source_table.replace('\'', "''")
    );
    let body = clickhouse_query(client, cfg, &query).await?;
    let response: ClickHouseJson<ServingSequenceRow> =
        serde_json::from_str(&body).context("parse serving watermarks response")?;
    Ok(response
        .data
        .into_iter()
        .map(|row| (row.serving_table, row.source_sequence_number))
        .collect())
}

#[derive(Debug, Deserialize)]
struct ServingSequenceRow {
    serving_table: String,
    source_sequence_number: u64,
}

async fn insert_clickhouse(
    client: &Client,
    cfg: &Config,
    table: &str,
    body: &[u8],
    token_prefix: &str,
) -> Result<()> {
    if body.is_empty() {
        return Ok(());
    }

    for (chunk_index, chunk) in
        ndjson_chunks(body, cfg.max_rows_per_insert, cfg.max_bytes_per_insert)
            .into_iter()
            .enumerate()
    {
        let query = format!("INSERT INTO {table} FORMAT JSONEachRow");
        let dedupe_token = insert_deduplication_token(token_prefix, table, chunk_index);
        let mut request = client
            .post(&cfg.clickhouse_url)
            .query(&[
                ("database", cfg.clickhouse_database.as_str()),
                ("query", query.as_str()),
                ("date_time_input_format", "best_effort"),
                ("type_json_skip_duplicated_paths", "1"),
                ("insert_deduplicate", "1"),
                ("insert_deduplication_token", dedupe_token.as_str()),
            ])
            .body(chunk.to_vec());
        if let Some(user) = cfg.clickhouse_user.as_deref() {
            request = request.basic_auth(user, cfg.clickhouse_password.as_deref());
        }
        let response = request.send().await.context("send ClickHouse insert")?;
        let status = response.status();
        let text = response.text().await.context("read ClickHouse response")?;
        if !status.is_success() {
            bail!("ClickHouse insert failed: {status} {text}");
        }
    }

    Ok(())
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    let index = batch.schema().index_of(name)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("{name} column is not String"))
}

fn int64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    let index = batch.schema().index_of(name)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("{name} column is not Int64"))
}

fn timestamp_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a TimestampMicrosecondArray> {
    let index = batch.schema().index_of(name)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| anyhow!("{name} column is not TimestampMicrosecond"))
}

fn optional_timestamp_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a TimestampMicrosecondArray> {
    timestamp_column(batch, name)
}

fn format_timestamp_us(value: i64) -> Result<String> {
    let secs = value.div_euclid(1_000_000);
    let micros = value.rem_euclid(1_000_000) as u32;
    let dt = DateTime::<Utc>::from_timestamp(secs, micros * 1000)
        .ok_or_else(|| anyhow!("timestamp out of range: {value}"))?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
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

fn is_error_row(row: &EventInsertRow) -> bool {
    let data = row.data.as_object();
    boolish_value(data.and_then(|data| data.get("is_error")))
        || data
            .and_then(|data| string_value(data.get("span_status_code")).into_non_empty())
            .is_some_and(|value| value.eq_ignore_ascii_case("error"))
        || {
            let event_type = data
                .and_then(|data| string_value(data.get("event_type")).into_non_empty())
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

fn data_file_location(value: &str) -> Result<DataFileLocation> {
    if let Some(path) = value.strip_prefix("file://") {
        return Ok(DataFileLocation::Local(PathBuf::from(path)));
    }
    if let Some(rest) = value
        .strip_prefix("s3://")
        .or_else(|| value.strip_prefix("s3a://"))
    {
        let (bucket, key) = rest
            .split_once('/')
            .ok_or_else(|| anyhow!("S3 data file path must include bucket and key: {value}"))?;
        if bucket.is_empty() || key.is_empty() {
            bail!("S3 data file path must include bucket and key: {value}");
        }
        return Ok(DataFileLocation::S3 {
            bucket: bucket.to_string(),
            key: key.to_string(),
        });
    }
    let path = PathBuf::from(value);
    if path.is_absolute() {
        return Ok(DataFileLocation::Local(path));
    }
    bail!(
        "unsupported lakehouse data file path {value}; expected file://, absolute path, s3://, or s3a://"
    )
}

fn rows_to_ndjson<T: Serialize>(rows: &[T]) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut body, row).context("serialize JSONEachRow row")?;
        body.push(b'\n');
    }
    Ok(body)
}

fn ndjson_chunks(bytes: &[u8], max_rows: usize, max_bytes: usize) -> Vec<&[u8]> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let max_rows = max_rows.max(1);
    let max_bytes = max_bytes.max(1);
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut rows = 0usize;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        rows += 1;
        let end = index + 1;
        if rows >= max_rows || end.saturating_sub(start) >= max_bytes {
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

fn insert_deduplication_token(prefix: &str, table: &str, chunk_index: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prefix.as_bytes());
    hasher.update(b"\0");
    hasher.update(table.as_bytes());
    hasher.update(b"\0");
    hasher.update(chunk_index.to_string().as_bytes());
    format!("{:x}", hasher.finalize())
}

fn quote_ident(value: &str) -> Result<String> {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        Ok(format!("`{value}`"))
    } else {
        bail!("{value} is not a simple ClickHouse identifier")
    }
}

fn env_or(key: &'static str, fallback: &'static str) -> String {
    optional(key).unwrap_or_else(|| fallback.to_string())
}

fn optional(key: &'static str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool(key: &'static str) -> bool {
    env_bool_default(key, false)
}

fn env_bool_default(key: &'static str, fallback: bool) -> bool {
    optional(key)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(fallback)
}

fn optional_usize(key: &'static str, fallback: usize) -> Result<usize> {
    match optional(key) {
        Some(value) => value.parse().with_context(|| format!("parse {key}")),
        None => Ok(fallback),
    }
}

fn optional_u64(key: &'static str, fallback: u64) -> Result<u64> {
    match optional(key) {
        Some(value) => value.parse().with_context(|| format!("parse {key}")),
        None => Ok(fallback),
    }
}

fn validate_identifier(key: &'static str, value: &str) -> Result<()> {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        Ok(())
    } else {
        bail!("{key} must be a simple ClickHouse identifier")
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, time::Duration};

    use serde_json::json;

    use super::{
        CommitSource, Config, DataFileLocation, EventInsertRow, ExtractionDefinitions,
        FieldDefinition, LakehouseCommitRecord, MeasureDefinition, StateDefinition,
        commit_record_to_commit, data_file_location, entity_state_update_rows, event_measure_rows,
        field_index_rows, format_timestamp_us, materialize_targets_for_commit, ndjson_chunks,
    };

    #[test]
    fn formats_microsecond_timestamp_for_clickhouse() {
        assert_eq!(
            format_timestamp_us(1_779_120_000_123_456).expect("timestamp"),
            "2026-05-18T16:00:00.123Z"
        );
    }

    #[test]
    fn chunks_ndjson_on_row_boundaries() {
        let chunks = ndjson_chunks(b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n", 2, 1024);
        assert_eq!(
            chunks,
            vec![
                b"{\"a\":1}\n{\"a\":2}\n".as_slice(),
                b"{\"a\":3}\n".as_slice()
            ]
        );
    }

    #[test]
    fn materializes_promoted_rows_from_rebuilt_event() {
        let row = EventInsertRow {
            event_id: "evt_1".to_string(),
            timestamp: "2026-05-18T16:00:00.123Z".to_string(),
            observed_timestamp: "2026-05-18T16:00:00.123Z".to_string(),
            ingested_timestamp: "2026-05-18T16:00:01.000Z".to_string(),
            source_file: "events/part.ndjson".to_string(),
            source_offset: 0,
            source_length: 128,
            data: json!({
                "tenant_id": "org_1",
                "event_type": "track",
                "country": "US",
                "revenue": 42.5,
                "currency": "USD",
                "user_id": "user_1",
                "plan": "pro"
            }),
        };
        let definitions = ExtractionDefinitions {
            fields: vec![FieldDefinition {
                tenant_id: "org_1".to_string(),
                definition_id: "def_country".to_string(),
                definition_version: 7,
                name: "country".to_string(),
                mode: "facet".to_string(),
                path: "country".to_string(),
                value_type: "string".to_string(),
            }],
            measures: vec![MeasureDefinition {
                tenant_id: "org_1".to_string(),
                definition_id: "def_revenue".to_string(),
                definition_version: 8,
                name: "revenue".to_string(),
                path: "revenue".to_string(),
                unit: "usd".to_string(),
                dimension: "currency".to_string(),
            }],
            states: vec![StateDefinition {
                tenant_id: "org_1".to_string(),
                definition_id: "def_plan".to_string(),
                definition_version: 9,
                name: "plan".to_string(),
                path: "plan".to_string(),
                value_type: "string".to_string(),
                entity_type: "user".to_string(),
                entity_id_path: "user_id".to_string(),
            }],
        };

        let fields = field_index_rows(&row, &definitions);
        let measures = event_measure_rows(&row, &definitions);
        let states = entity_state_update_rows(&row, &definitions);

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].field_name, "country");
        assert_eq!(fields[0].value, "US");
        assert_eq!(measures.len(), 1);
        assert_eq!(measures[0].value, 42.5);
        assert_eq!(measures[0].dimension_value, "USD");
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].entity_id, "user_1");
        assert_eq!(states[0].value, "pro");
    }

    #[test]
    fn parses_lakehouse_data_file_locations() {
        assert_eq!(
            data_file_location("file:///tmp/events.parquet").expect("file uri"),
            DataFileLocation::Local("/tmp/events.parquet".into())
        );
        assert_eq!(
            data_file_location("/tmp/events.parquet").expect("absolute path"),
            DataFileLocation::Local("/tmp/events.parquet".into())
        );
        assert_eq!(
            data_file_location("s3://bucket/path/to/events.parquet").expect("s3 uri"),
            DataFileLocation::S3 {
                bucket: "bucket".to_string(),
                key: "path/to/events.parquet".to_string(),
            }
        );
        assert_eq!(
            data_file_location("s3a://bucket/path/to/events.parquet").expect("s3a uri"),
            DataFileLocation::S3 {
                bucket: "bucket".to_string(),
                key: "path/to/events.parquet".to_string(),
            }
        );
        assert!(data_file_location("relative/events.parquet").is_err());
    }

    #[test]
    fn converts_clickhouse_commit_records_with_multiple_data_files() {
        let commit = commit_record_to_commit(LakehouseCommitRecord {
            namespace: "nanotrace".to_string(),
            table_name: "events".to_string(),
            snapshot_id: "123".to_string(),
            sequence_number: 7,
            committed_at_ms: 1_779_120_000_000,
            data_file: "s3://bucket/events/part-1.parquet".to_string(),
            data_files: vec![
                "s3://bucket/events/part-1.parquet".to_string(),
                "s3://bucket/events/part-2.parquet".to_string(),
            ],
            record_count: 42,
            content_sha256: "a".repeat(64),
            metadata_location: "s3://bucket/events/metadata/v1.json".to_string(),
            source_batch_id: "batch-1".to_string(),
            deduplicated: 1,
        });

        assert_eq!(commit.data_files.len(), 2);
        assert_eq!(commit.data_files[1], "s3://bucket/events/part-2.parquet");
        assert_eq!(commit.source_batch_id.as_deref(), Some("batch-1"));
        assert!(commit.deduplicated);
    }

    #[test]
    fn plans_incremental_materialization_from_watermarks() {
        let cfg = test_config();
        let mut watermarks = HashMap::new();
        watermarks.insert("field_index".to_string(), 8);
        watermarks.insert("event_measures".to_string(), 9);
        watermarks.insert("entity_state_updates".to_string(), 7);

        let targets = materialize_targets_for_commit(&cfg, 9, &watermarks);

        assert!(targets.field_index);
        assert!(!targets.event_measures);
        assert!(targets.entity_state_updates);
        assert!(targets.any());
    }

    fn test_config() -> Config {
        Config {
            warehouse_dir: PathBuf::from("/tmp/lakehouse"),
            namespace: "nanotrace".to_string(),
            source_table: "events".to_string(),
            clickhouse_url: "http://localhost:8123".to_string(),
            clickhouse_user: None,
            clickhouse_password: None,
            clickhouse_database: "observatory".to_string(),
            clickhouse_events_table: "events".to_string(),
            clickhouse_field_index_table: "field_index".to_string(),
            clickhouse_event_measures_table: "event_measures".to_string(),
            clickhouse_entity_state_updates_table: "entity_state_updates".to_string(),
            clickhouse_definitions_table: "definitions".to_string(),
            truncate_events: false,
            rebuild_raw: true,
            rebuild_derived: true,
            incremental_materialize: false,
            materialize_loop: false,
            commit_source: CommitSource::Local,
            materialize_poll_interval: Duration::from_secs(5),
            allow_non_empty: false,
            from_sequence: 0,
            max_rows_per_insert: 25_000,
            max_bytes_per_insert: 8 * 1024 * 1024,
            s3_max_file_bytes: 1024 * 1024 * 1024,
        }
    }
}
