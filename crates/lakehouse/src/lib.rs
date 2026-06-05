use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::Schema as ArrowSchema;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use iceberg::{
    Catalog, CatalogBuilder, MetadataLocation, NamespaceIdent, TableCreation, TableIdent,
    TableUpdate,
    io::LocalFsStorageFactory,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder},
    spec::{
        DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestListWriter,
        ManifestWriterBuilder, NestedField, Operation, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, Summary, Type,
    },
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
        },
        partitioning::unpartitioned_writer::UnpartitionedWriter,
    },
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::{
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct LakehouseConfig {
    pub warehouse_dir: PathBuf,
    pub namespace: String,
    pub table: String,
    pub catalog: LakehouseCatalogConfig,
    pub write_target_file_size_bytes: u64,
    pub min_snapshots_to_keep: u64,
    pub max_snapshot_age_ms: u64,
    pub metadata_previous_versions_max: u64,
}

#[derive(Clone, Debug)]
pub enum LakehouseCatalogConfig {
    LocalFilesystem,
    Rest(LakehouseRestCatalogConfig),
}

#[derive(Clone, Debug)]
pub struct LakehouseRestCatalogConfig {
    pub catalog_name: String,
    pub uri: String,
    pub warehouse: String,
    pub properties: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct LakehouseCompactionOptions {
    pub small_file_bytes: u64,
    pub min_input_files: usize,
    pub target_file_size_bytes: u64,
}

impl Default for LakehouseCompactionOptions {
    fn default() -> Self {
        Self {
            small_file_bytes: 128_u64 * 1024 * 1024,
            min_input_files: 2,
            target_file_size_bytes: 512_u64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LakehouseCompactionResult {
    pub compacted: bool,
    pub input_file_count: usize,
    pub input_small_file_count: usize,
    pub input_record_count: usize,
    pub output_file_count: usize,
    pub output_record_count: usize,
    pub snapshot_id: Option<String>,
    pub sequence_number: Option<u64>,
    pub metadata_location: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LakehouseCommit {
    pub namespace: String,
    pub table: String,
    pub snapshot_id: String,
    pub sequence_number: u64,
    pub committed_at_ms: i64,
    pub data_file: String,
    #[serde(default)]
    pub data_files: Vec<String>,
    pub record_count: usize,
    pub content_sha256: String,
    #[serde(default)]
    pub metadata_location: String,
    #[serde(default)]
    pub source_batch_id: Option<String>,
    #[serde(default)]
    pub deduplicated: bool,
}

#[derive(Debug)]
struct EventRow {
    event_id: String,
    tenant_id: String,
    timestamp_us: i64,
    observed_timestamp_us: Option<i64>,
    ingested_timestamp_us: i64,
    event_type: String,
    signal: String,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    service: String,
    environment: String,
    name: String,
    duration_ms: Option<f64>,
    is_error: Option<i64>,
    source_file: String,
    source_offset: u64,
    source_length: u32,
    data: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct LocalCatalogPointer {
    namespace: String,
    table: String,
    metadata_location: String,
    updated_at_ms: i64,
}

enum LoadedCatalog {
    Local(Box<MemoryCatalog>),
    Rest(Box<RestCatalog>),
}

impl LoadedCatalog {
    fn as_catalog(&self) -> &dyn Catalog {
        match self {
            Self::Local(catalog) => catalog.as_ref(),
            Self::Rest(catalog) => catalog.as_ref(),
        }
    }
}

impl LakehouseConfig {
    pub fn events_table(warehouse_dir: impl Into<PathBuf>) -> Self {
        Self {
            warehouse_dir: warehouse_dir.into(),
            namespace: "nanotrace".to_string(),
            table: "events".to_string(),
            catalog: LakehouseCatalogConfig::LocalFilesystem,
            write_target_file_size_bytes: 512_u64 * 1024 * 1024,
            min_snapshots_to_keep: 10_000,
            max_snapshot_age_ms: 7 * 24 * 60 * 60 * 1000,
            metadata_previous_versions_max: 100,
        }
    }

    pub fn with_rest_catalog(mut self, rest: LakehouseRestCatalogConfig) -> Self {
        self.catalog = LakehouseCatalogConfig::Rest(rest);
        self
    }

    pub fn with_write_target_file_size_bytes(mut self, bytes: u64) -> Self {
        self.write_target_file_size_bytes = bytes;
        self
    }

    pub fn with_snapshot_retention(
        mut self,
        min_snapshots_to_keep: u64,
        max_snapshot_age_ms: u64,
        metadata_previous_versions_max: u64,
    ) -> Self {
        self.min_snapshots_to_keep = min_snapshots_to_keep;
        self.max_snapshot_age_ms = max_snapshot_age_ms;
        self.metadata_previous_versions_max = metadata_previous_versions_max;
        self
    }

    fn table_dir(&self) -> PathBuf {
        self.warehouse_dir.join(&self.namespace).join(&self.table)
    }

    fn metadata_dir(&self) -> PathBuf {
        self.table_dir().join("metadata")
    }

    fn catalog_pointer_path(&self) -> PathBuf {
        self.metadata_dir().join("_nanotrace_catalog_pointer.json")
    }
}

pub fn commit_events_ndjson(cfg: &LakehouseConfig, ndjson: &[u8]) -> Result<LakehouseCommit> {
    commit_events_ndjson_with_source(cfg, ndjson, None)
}

pub fn commit_events_ndjson_with_source(
    cfg: &LakehouseConfig,
    ndjson: &[u8],
    source_batch_id: Option<&str>,
) -> Result<LakehouseCommit> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build lakehouse commit runtime")?
        .block_on(commit_events_ndjson_iceberg(cfg, ndjson, source_batch_id))
}

pub async fn commit_events_ndjson_iceberg(
    cfg: &LakehouseConfig,
    ndjson: &[u8],
    source_batch_id: Option<&str>,
) -> Result<LakehouseCommit> {
    let rows = parse_rows(ndjson)?;
    if rows.is_empty() {
        bail!("cannot commit empty event batch");
    }

    fs::create_dir_all(cfg.metadata_dir()).context("create lakehouse metadata directory")?;

    let (catalog, table) = load_or_create_iceberg_table(cfg).await?;
    let metadata_location = table
        .metadata_location_result()
        .context("iceberg table missing metadata location")?
        .to_string();
    if let Some(source_batch_id) = source_batch_id
        && let Some(commit) =
            commit_from_existing_source_snapshot(cfg, &table, &metadata_location, source_batch_id)
    {
        return Ok(commit);
    }

    let batch = rows_to_record_batch(&rows)?;
    let data_files =
        write_iceberg_data_files(&table, batch, cfg.write_target_file_size_bytes).await?;
    let data_file_paths = data_files
        .iter()
        .map(|data_file| data_file.file_path().to_string())
        .collect::<Vec<_>>();
    let record_count: usize = data_files
        .iter()
        .map(|data_file| data_file.record_count() as usize)
        .sum();
    let content_sha256 = hex_sha256(data_file_paths.join("\n").as_bytes());

    let mut snapshot_properties = HashMap::new();
    snapshot_properties.insert(
        "nanotrace.record-count".to_string(),
        record_count.to_string(),
    );
    snapshot_properties.insert(
        "nanotrace.writer".to_string(),
        "nanotrace-lakehouse".to_string(),
    );
    if let Some(source_batch_id) = source_batch_id {
        snapshot_properties.insert(
            "nanotrace.source-batch-id".to_string(),
            source_batch_id.to_string(),
        );
    }
    snapshot_properties.insert(
        "nanotrace.content-sha256".to_string(),
        content_sha256.clone(),
    );
    snapshot_properties.insert(
        "nanotrace.data-files-json".to_string(),
        serde_json::to_string(&data_file_paths).context("serialize data file paths")?,
    );

    let tx = Transaction::new(&table);
    let action = tx
        .fast_append()
        .set_commit_uuid(Uuid::new_v4())
        .set_snapshot_properties(snapshot_properties)
        .add_data_files(data_files);
    let table = action
        .apply(tx)
        .context("stage iceberg append transaction")?
        .commit(catalog.as_catalog())
        .await
        .context("commit iceberg append transaction")?;

    let snapshot = table
        .metadata()
        .current_snapshot()
        .context("iceberg table missing current snapshot after append")?;
    let metadata_location = table
        .metadata_location_result()
        .context("iceberg table missing metadata location")?
        .to_string();
    if matches!(cfg.catalog, LakehouseCatalogConfig::LocalFilesystem) {
        write_local_catalog_pointer(cfg, &metadata_location)?;
    }

    let commit = LakehouseCommit {
        namespace: cfg.namespace.clone(),
        table: cfg.table.clone(),
        snapshot_id: snapshot.snapshot_id().to_string(),
        sequence_number: snapshot.sequence_number().try_into().unwrap_or(0),
        committed_at_ms: snapshot.timestamp_ms(),
        data_file: data_file_paths.first().cloned().unwrap_or_default(),
        data_files: data_file_paths,
        record_count,
        content_sha256,
        metadata_location,
        source_batch_id: source_batch_id.map(str::to_string),
        deduplicated: false,
    };

    let metadata_path = cfg.metadata_dir().join(format!(
        "snapshot-{sequence_number:020}.nanotrace.json",
        sequence_number = commit.sequence_number
    ));
    write_json_atomic(&metadata_path, &commit)
        .context("write nanotrace lakehouse commit record")?;
    write_json_atomic(&cfg.metadata_dir().join("current.json"), &commit)
        .context("write nanotrace lakehouse current commit record")?;

    Ok(commit)
}

pub async fn compact_events_iceberg(
    cfg: &LakehouseConfig,
    options: LakehouseCompactionOptions,
) -> Result<LakehouseCompactionResult> {
    if !matches!(cfg.catalog, LakehouseCatalogConfig::LocalFilesystem) {
        bail!(
            "native Rust Iceberg compaction currently supports the local filesystem catalog; REST/catalog deployments should use NANOTRACE_ICEBERG_MAINTENANCE_CMD"
        );
    }

    let (_catalog, table) = load_or_create_iceberg_table(cfg).await?;
    let Some(current_snapshot) = table.metadata().current_snapshot() else {
        return Ok(LakehouseCompactionResult {
            compacted: false,
            input_file_count: 0,
            input_small_file_count: 0,
            input_record_count: 0,
            output_file_count: 0,
            output_record_count: 0,
            snapshot_id: None,
            sequence_number: None,
            metadata_location: table.metadata_location().map(str::to_string),
            reason: Some("table has no current snapshot".to_string()),
        });
    };

    let active_files = active_data_files(&table).await?;
    let input_small_file_count = active_files
        .iter()
        .filter(|data_file| data_file.file_size_in_bytes() < options.small_file_bytes)
        .count();
    let input_record_count = active_files
        .iter()
        .map(|data_file| data_file.record_count() as usize)
        .sum::<usize>();
    if input_small_file_count < options.min_input_files {
        return Ok(LakehouseCompactionResult {
            compacted: false,
            input_file_count: active_files.len(),
            input_small_file_count,
            input_record_count,
            output_file_count: 0,
            output_record_count: 0,
            snapshot_id: Some(current_snapshot.snapshot_id().to_string()),
            sequence_number: Some(current_snapshot.sequence_number().try_into().unwrap_or(0)),
            metadata_location: table.metadata_location().map(str::to_string),
            reason: Some(format!(
                "small file count {input_small_file_count} is below min input files {}",
                options.min_input_files
            )),
        });
    }

    let batches = scan_current_table_batches(&table).await?;
    let output_files =
        write_iceberg_record_batches(&table, batches, options.target_file_size_bytes).await?;
    let output_file_count = output_files.len();
    let output_record_count = output_files
        .iter()
        .map(|data_file| data_file.record_count() as usize)
        .sum::<usize>();
    let snapshot_id = generate_unique_snapshot_id(&table);
    let sequence_number = table.metadata().next_sequence_number();
    let metadata_location = commit_local_replace_snapshot(&table, cfg, snapshot_id, output_files)
        .await
        .context("commit local Iceberg compaction replace snapshot")?;

    Ok(LakehouseCompactionResult {
        compacted: true,
        input_file_count: active_files.len(),
        input_small_file_count,
        input_record_count,
        output_file_count,
        output_record_count,
        snapshot_id: Some(snapshot_id.to_string()),
        sequence_number: Some(sequence_number.try_into().unwrap_or(0)),
        metadata_location: Some(metadata_location),
        reason: None,
    })
}

async fn active_data_files(table: &Table) -> Result<Vec<DataFile>> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(Vec::new());
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .context("load current Iceberg manifest list")?;
    let mut data_files = Vec::new();
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .context("load current Iceberg manifest")?;
        for entry in manifest.entries() {
            if entry.is_alive() {
                data_files.push(entry.data_file.clone());
            }
        }
    }
    Ok(data_files)
}

async fn scan_current_table_batches(table: &Table) -> Result<Vec<RecordBatch>> {
    let scan = table
        .scan()
        .select_all()
        .build()
        .context("build Iceberg compaction scan")?;
    let mut stream = scan
        .to_arrow()
        .await
        .context("open Iceberg compaction Arrow stream")?;
    let mut batches = Vec::new();
    while let Some(batch) = stream
        .try_next()
        .await
        .context("read Iceberg compaction Arrow batch")?
    {
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    Ok(batches)
}

async fn commit_local_replace_snapshot(
    table: &Table,
    cfg: &LakehouseConfig,
    snapshot_id: i64,
    data_files: Vec<DataFile>,
) -> Result<String> {
    if data_files.is_empty() {
        bail!("cannot commit compaction snapshot with no output data files");
    }

    let sequence_number = table.metadata().next_sequence_number();
    let parent_snapshot_id = table.metadata().current_snapshot_id();
    let manifest_list_path =
        write_compaction_manifest_list(table, snapshot_id, sequence_number, &data_files).await?;
    let output_file_paths = data_files
        .iter()
        .map(|data_file| data_file.file_path().to_string())
        .collect::<Vec<_>>();
    let record_count = data_files
        .iter()
        .map(|data_file| data_file.record_count())
        .sum::<u64>();
    let mut additional_properties = HashMap::new();
    additional_properties.insert(
        "nanotrace.writer".to_string(),
        "nanotrace-lakehouse".to_string(),
    );
    additional_properties.insert("nanotrace.operation".to_string(), "compaction".to_string());
    additional_properties.insert(
        "nanotrace.record-count".to_string(),
        record_count.to_string(),
    );
    additional_properties.insert(
        "nanotrace.content-sha256".to_string(),
        hex_sha256(output_file_paths.join("\n").as_bytes()),
    );
    additional_properties.insert(
        "nanotrace.data-files-json".to_string(),
        serde_json::to_string(&output_file_paths).context("serialize compacted data file paths")?,
    );

    let snapshot = Snapshot::builder()
        .with_manifest_list(manifest_list_path)
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent_snapshot_id)
        .with_sequence_number(sequence_number)
        .with_summary(Summary {
            operation: Operation::Replace,
            additional_properties,
        })
        .with_schema_id(table.metadata().current_schema_id())
        .with_timestamp_ms(now_ms())
        .build();
    let updates = vec![
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
    ];
    let current_metadata_location = table
        .metadata_location_result()
        .context("iceberg table missing metadata location before compaction commit")?
        .to_string();
    assert_local_catalog_pointer_matches(cfg, &current_metadata_location)
        .context("verify local Iceberg pointer before compaction commit")?;
    let new_metadata_location = MetadataLocation::from_str(&current_metadata_location)
        .context("parse current Iceberg metadata location")?
        .with_next_version()
        .to_string();
    let mut builder = table
        .metadata()
        .clone()
        .into_builder(Some(current_metadata_location.clone()));
    for update in updates {
        builder = update.apply(builder)?;
    }
    let metadata = builder
        .build()
        .context("build compacted Iceberg table metadata")?
        .metadata;
    metadata
        .write_to(table.file_io(), &new_metadata_location)
        .await
        .context("write compacted Iceberg metadata")?;
    assert_local_catalog_pointer_matches(cfg, &current_metadata_location)
        .context("verify local Iceberg pointer before publishing compaction commit")?;
    write_local_catalog_pointer(cfg, &new_metadata_location)?;
    Ok(new_metadata_location)
}

async fn write_compaction_manifest_list(
    table: &Table,
    snapshot_id: i64,
    sequence_number: i64,
    data_files: &[DataFile],
) -> Result<String> {
    let commit_uuid = Uuid::new_v4();
    let manifest_path = format!(
        "{}/metadata/{}-m0.{}",
        table.metadata().location(),
        commit_uuid,
        DataFileFormat::Avro
    );
    let mut manifest_writer = {
        let builder = ManifestWriterBuilder::new(
            table.file_io().new_output(manifest_path.clone())?,
            Some(snapshot_id),
            None,
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        );
        match table.metadata().format_version() {
            FormatVersion::V1 => builder.build_v1(),
            FormatVersion::V2 => builder.build_v2_data(),
            FormatVersion::V3 => builder.build_v3_data(),
        }
    };
    for data_file in data_files {
        manifest_writer
            .add_file(data_file.clone(), sequence_number)
            .context("add compacted data file to manifest")?;
    }
    let manifest_file = manifest_writer
        .write_manifest_file()
        .await
        .context("write compacted Iceberg manifest")?;

    let manifest_list_path = format!(
        "{}/metadata/snap-{}-0-{}.{}",
        table.metadata().location(),
        snapshot_id,
        commit_uuid,
        DataFileFormat::Avro
    );
    let mut manifest_list_writer = match table.metadata().format_version() {
        FormatVersion::V1 => ManifestListWriter::v1(
            table.file_io().new_output(manifest_list_path.clone())?,
            snapshot_id,
            table.metadata().current_snapshot_id(),
        ),
        FormatVersion::V2 => ManifestListWriter::v2(
            table.file_io().new_output(manifest_list_path.clone())?,
            snapshot_id,
            table.metadata().current_snapshot_id(),
            sequence_number,
        ),
        FormatVersion::V3 => ManifestListWriter::v3(
            table.file_io().new_output(manifest_list_path.clone())?,
            snapshot_id,
            table.metadata().current_snapshot_id(),
            sequence_number,
            Some(table.metadata().next_row_id()),
        ),
    };
    manifest_list_writer
        .add_manifests(vec![manifest_file].into_iter())
        .context("add compacted manifest to manifest list")?;
    manifest_list_writer
        .close()
        .await
        .context("write compacted Iceberg manifest list")?;
    Ok(manifest_list_path)
}

fn generate_unique_snapshot_id(table: &Table) -> i64 {
    let generate_random_id = || -> i64 {
        let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
        let snapshot_id = (lhs ^ rhs) as i64;
        if snapshot_id < 0 {
            -snapshot_id
        } else {
            snapshot_id
        }
    };
    let mut snapshot_id = generate_random_id();
    while table
        .metadata()
        .snapshots()
        .any(|snapshot| snapshot.snapshot_id() == snapshot_id)
    {
        snapshot_id = generate_random_id();
    }
    snapshot_id
}

fn assert_local_catalog_pointer_matches(cfg: &LakehouseConfig, expected: &str) -> Result<()> {
    let pointer =
        read_local_catalog_pointer(cfg)?.context("local Iceberg catalog pointer missing")?;
    if pointer.metadata_location != expected {
        bail!(
            "local Iceberg catalog pointer changed during compaction: expected {}, found {}",
            expected,
            pointer.metadata_location
        );
    }
    Ok(())
}

fn commit_from_existing_source_snapshot(
    cfg: &LakehouseConfig,
    table: &Table,
    metadata_location: &str,
    source_batch_id: &str,
) -> Option<LakehouseCommit> {
    table
        .metadata()
        .snapshots()
        .filter(|snapshot| {
            snapshot
                .summary()
                .additional_properties
                .get("nanotrace.source-batch-id")
                .is_some_and(|value| value == source_batch_id)
        })
        .max_by_key(|snapshot| snapshot.sequence_number())
        .map(|snapshot| {
            let properties = &snapshot.summary().additional_properties;
            let data_files = properties
                .get("nanotrace.data-files-json")
                .and_then(|value| serde_json::from_str::<Vec<String>>(value).ok())
                .unwrap_or_default();
            let record_count = properties
                .get("nanotrace.record-count")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            let content_sha256 = properties
                .get("nanotrace.content-sha256")
                .cloned()
                .unwrap_or_else(|| hex_sha256(data_files.join("\n").as_bytes()));
            LakehouseCommit {
                namespace: cfg.namespace.clone(),
                table: cfg.table.clone(),
                snapshot_id: snapshot.snapshot_id().to_string(),
                sequence_number: snapshot.sequence_number().try_into().unwrap_or(0),
                committed_at_ms: snapshot.timestamp_ms(),
                data_file: data_files.first().cloned().unwrap_or_default(),
                data_files,
                record_count,
                content_sha256,
                metadata_location: metadata_location.to_string(),
                source_batch_id: Some(source_batch_id.to_string()),
                deduplicated: true,
            }
        })
}

fn parse_rows(ndjson: &[u8]) -> Result<Vec<EventRow>> {
    let mut rows = Vec::new();
    for line in ndjson.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_slice(line).context("parse event row")?;
        let Some(row) = value.as_object() else {
            continue;
        };
        let data = row
            .get("data")
            .and_then(Value::as_object)
            .context("event row data must be an object")?;
        let timestamp_us = parse_timestamp(required_str(row.get("timestamp"), "timestamp")?)?;
        let observed_timestamp_us = row
            .get("observed_timestamp")
            .and_then(Value::as_str)
            .map(parse_timestamp)
            .transpose()?;
        rows.push(EventRow {
            event_id: required_str(row.get("event_id"), "event_id")?.to_string(),
            tenant_id: str_at(data, "tenant_id"),
            timestamp_us,
            observed_timestamp_us,
            ingested_timestamp_us: now_us(),
            event_type: str_at(data, "event_type"),
            signal: classify_signal(data),
            trace_id: str_at(data, "trace_id"),
            span_id: str_at(data, "span_id"),
            parent_span_id: str_at(data, "parent_span_id"),
            service: str_at(data, "service"),
            environment: str_at(data, "environment"),
            name: str_at(data, "name"),
            duration_ms: data.get("duration_ms").and_then(Value::as_f64),
            is_error: data.get("is_error").and_then(Value::as_i64),
            source_file: str_value(row.get("source_file")),
            source_offset: row
                .get("source_offset")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            source_length: row
                .get("source_length")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or(0),
            data: serde_json::to_string(data).context("serialize event data")?,
        });
    }
    Ok(rows)
}

pub fn canonical_iceberg_schema() -> Result<Schema> {
    fn field(
        id: i32,
        name: &str,
        ty: PrimitiveType,
        required: bool,
    ) -> iceberg::spec::NestedFieldRef {
        if required {
            NestedField::required(id, name, Type::Primitive(ty)).into()
        } else {
            NestedField::optional(id, name, Type::Primitive(ty)).into()
        }
    }

    Schema::builder()
        .with_schema_id(1)
        .with_identifier_field_ids([2])
        .with_fields(vec![
            field(1, "tenant_id", PrimitiveType::String, true),
            field(2, "event_id", PrimitiveType::String, true),
            field(3, "timestamp", PrimitiveType::Timestamptz, true),
            field(4, "observed_timestamp", PrimitiveType::Timestamptz, false),
            field(5, "ingested_timestamp", PrimitiveType::Timestamptz, true),
            field(6, "event_type", PrimitiveType::String, true),
            field(7, "signal", PrimitiveType::String, true),
            field(8, "trace_id", PrimitiveType::String, true),
            field(9, "span_id", PrimitiveType::String, true),
            field(10, "parent_span_id", PrimitiveType::String, true),
            field(11, "service", PrimitiveType::String, true),
            field(12, "environment", PrimitiveType::String, true),
            field(13, "name", PrimitiveType::String, true),
            field(14, "duration_ms", PrimitiveType::Double, false),
            field(15, "is_error", PrimitiveType::Long, false),
            field(16, "source_file", PrimitiveType::String, true),
            field(17, "source_offset", PrimitiveType::Long, true),
            field(18, "source_length", PrimitiveType::Long, true),
            field(19, "data", PrimitiveType::String, true),
        ])
        .build()
        .context("build canonical iceberg schema")
}

pub fn canonical_arrow_schema() -> Result<ArrowSchema> {
    iceberg::arrow::schema_to_arrow_schema(&canonical_iceberg_schema()?)
        .context("convert iceberg schema to arrow schema")
}

fn rows_to_record_batch(rows: &[EventRow]) -> Result<RecordBatch> {
    let schema = Arc::new(canonical_arrow_schema()?);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.tenant_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.event_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter().map(|row| row.timestamp_us).collect::<Vec<_>>(),
                )
                .with_timezone("+00:00"),
            ),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter()
                        .map(|row| row.observed_timestamp_us)
                        .collect::<Vec<_>>(),
                )
                .with_timezone("+00:00"),
            ),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter()
                        .map(|row| row.ingested_timestamp_us)
                        .collect::<Vec<_>>(),
                )
                .with_timezone("+00:00"),
            ),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.event_type.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.signal.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.trace_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.span_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.parent_span_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.service.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.environment.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.duration_ms).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.is_error).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.source_file.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| i64::try_from(row.source_offset).unwrap_or(i64::MAX))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| i64::from(row.source_length))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.data.as_str()).collect::<Vec<_>>(),
            )),
        ],
    )?;
    Ok(batch)
}

async fn load_or_create_iceberg_table(cfg: &LakehouseConfig) -> Result<(LoadedCatalog, Table)> {
    match &cfg.catalog {
        LakehouseCatalogConfig::LocalFilesystem => load_or_create_local_iceberg_table(cfg).await,
        LakehouseCatalogConfig::Rest(rest) => load_or_create_rest_iceberg_table(cfg, rest).await,
    }
}

async fn load_or_create_local_iceberg_table(
    cfg: &LakehouseConfig,
) -> Result<(LoadedCatalog, Table)> {
    fs::create_dir_all(&cfg.warehouse_dir).context("create iceberg warehouse directory")?;
    fs::create_dir_all(cfg.table_dir()).context("create iceberg table directory")?;
    fs::create_dir_all(cfg.metadata_dir()).context("create iceberg metadata directory")?;

    let warehouse_location = file_uri(&absolute_path(&cfg.warehouse_dir)?);
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "nanotrace-local",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_location)]),
        )
        .await
        .context("load local iceberg catalog")?;

    let namespace = NamespaceIdent::from_strs([cfg.namespace.as_str()])
        .context("build iceberg namespace identifier")?;
    if !catalog
        .namespace_exists(&namespace)
        .await
        .context("check iceberg namespace")?
    {
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .context("create iceberg namespace")?;
    }

    let table_ident = TableIdent::new(namespace, cfg.table.clone());
    if let Some(pointer) = read_local_catalog_pointer(cfg)? {
        let table = catalog
            .register_table(&table_ident, pointer.metadata_location)
            .await
            .context("register local iceberg table from pointer")?;
        let table = ensure_table_properties(&catalog, cfg, table)
            .await
            .context("ensure local iceberg table properties")?;
        return Ok((LoadedCatalog::Local(Box::new(catalog)), table));
    }

    let table = catalog
        .create_table(
            table_ident.namespace(),
            TableCreation::builder()
                .name(table_ident.name().to_string())
                .location(file_uri(&absolute_path(&cfg.table_dir())?))
                .schema(canonical_iceberg_schema()?)
                .format_version(FormatVersion::V2)
                .properties(table_properties(cfg))
                .build(),
        )
        .await
        .context("create local iceberg events table")?;
    let metadata_location = table
        .metadata_location_result()
        .context("iceberg table missing metadata location after create")?
        .to_string();
    write_local_catalog_pointer(cfg, &metadata_location)?;

    Ok((LoadedCatalog::Local(Box::new(catalog)), table))
}

async fn load_or_create_rest_iceberg_table(
    cfg: &LakehouseConfig,
    rest: &LakehouseRestCatalogConfig,
) -> Result<(LoadedCatalog, Table)> {
    let mut properties = rest.properties.clone();
    properties.insert(REST_CATALOG_PROP_URI.to_string(), rest.uri.clone());
    properties.insert(
        REST_CATALOG_PROP_WAREHOUSE.to_string(),
        rest.warehouse.clone(),
    );

    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(storage_factory_for_warehouse(&rest.warehouse))
        .load(rest.catalog_name.clone(), properties)
        .await
        .context("load iceberg REST catalog")?;

    let namespace = NamespaceIdent::from_strs([cfg.namespace.as_str()])
        .context("build iceberg namespace identifier")?;
    if !catalog
        .namespace_exists(&namespace)
        .await
        .context("check iceberg REST namespace")?
    {
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .context("create iceberg REST namespace")?;
    }

    let table_ident = TableIdent::new(namespace, cfg.table.clone());
    if catalog
        .table_exists(&table_ident)
        .await
        .context("check iceberg REST table")?
    {
        let table = catalog
            .load_table(&table_ident)
            .await
            .context("load iceberg REST table")?;
        let table = ensure_table_properties(&catalog, cfg, table)
            .await
            .context("ensure iceberg REST table properties")?;
        return Ok((LoadedCatalog::Rest(Box::new(catalog)), table));
    }

    let table = catalog
        .create_table(
            table_ident.namespace(),
            TableCreation::builder()
                .name(table_ident.name().to_string())
                .schema(canonical_iceberg_schema()?)
                .format_version(FormatVersion::V2)
                .properties(table_properties(cfg))
                .build(),
        )
        .await
        .context("create iceberg REST events table")?;

    Ok((LoadedCatalog::Rest(Box::new(catalog)), table))
}

async fn ensure_table_properties(
    catalog: &dyn Catalog,
    cfg: &LakehouseConfig,
    table: Table,
) -> Result<Table> {
    let desired = table_properties(cfg);
    let current = table.metadata().properties();
    let updates = desired
        .iter()
        .filter(|(key, value)| current.get(*key) != Some(*value))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    if updates.is_empty() {
        return Ok(table);
    }

    let tx = Transaction::new(&table);
    let mut action = tx.update_table_properties();
    for (key, value) in updates {
        action = action.set(key, value);
    }
    let table = action
        .apply(tx)
        .context("stage iceberg table property update")?
        .commit(catalog)
        .await
        .context("commit iceberg table property update")?;
    if matches!(cfg.catalog, LakehouseCatalogConfig::LocalFilesystem) {
        let metadata_location = table
            .metadata_location_result()
            .context("iceberg table missing metadata location after property update")?
            .to_string();
        write_local_catalog_pointer(cfg, &metadata_location)?;
    }
    Ok(table)
}

fn table_properties(cfg: &LakehouseConfig) -> HashMap<String, String> {
    HashMap::from([
        (
            "write.parquet.compression-codec".to_string(),
            "zstd".to_string(),
        ),
        (
            "write.target-file-size-bytes".to_string(),
            cfg.write_target_file_size_bytes.to_string(),
        ),
        (
            "write.metadata.previous-versions-max".to_string(),
            cfg.metadata_previous_versions_max.to_string(),
        ),
        (
            "write.metadata.delete-after-commit.enabled".to_string(),
            "true".to_string(),
        ),
        (
            "history.expire.min-snapshots-to-keep".to_string(),
            cfg.min_snapshots_to_keep.to_string(),
        ),
        (
            "history.expire.max-snapshot-age-ms".to_string(),
            cfg.max_snapshot_age_ms.to_string(),
        ),
        ("commit.retry.num-retries".to_string(), "8".to_string()),
        (
            "commit.retry.total-timeout-ms".to_string(),
            "120000".to_string(),
        ),
    ])
}

fn storage_factory_for_warehouse(warehouse: &str) -> Arc<dyn iceberg::io::StorageFactory> {
    if warehouse.starts_with("s3a://") {
        Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3a".to_string(),
            customized_credential_load: None,
        })
    } else if warehouse.starts_with("s3://") {
        Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        })
    } else {
        Arc::new(OpenDalStorageFactory::Fs)
    }
}

async fn write_iceberg_data_files(
    table: &Table,
    batch: RecordBatch,
    target_file_size_bytes: u64,
) -> Result<Vec<DataFile>> {
    write_iceberg_record_batches(table, vec![batch], target_file_size_bytes).await
}

async fn write_iceberg_record_batches(
    table: &Table,
    batches: Vec<RecordBatch>,
    target_file_size_bytes: u64,
) -> Result<Vec<DataFile>> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .context("create iceberg data location generator")?;
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("nanotrace-{}", Uuid::new_v4()),
        None,
        DataFileFormat::Parquet,
    );
    let writer_props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let parquet_writer_builder =
        ParquetWriterBuilder::new(writer_props, table.metadata().current_schema().clone());
    let target_file_size = usize::try_from(target_file_size_bytes)
        .unwrap_or(usize::MAX)
        .max(1);
    let rolling_writer_builder = RollingFileWriterBuilder::new(
        parquet_writer_builder,
        target_file_size,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut writer = UnpartitionedWriter::new(data_file_writer_builder);
    for batch in batches {
        for chunk in record_batch_writer_chunks(&batch, target_file_size) {
            writer
                .write(chunk)
                .await
                .context("write iceberg record batch")?;
        }
    }
    writer.close().await.context("close iceberg data writer")
}

fn record_batch_writer_chunks(batch: &RecordBatch, target_file_size: usize) -> Vec<RecordBatch> {
    let rows = batch.num_rows();
    if rows == 0 {
        return Vec::new();
    }
    let memory_size = batch.get_array_memory_size().max(rows);
    let estimated_row_size = memory_size.div_ceil(rows).max(1);
    let rows_per_chunk = (target_file_size.max(1) / estimated_row_size)
        .max(1)
        .min(rows);

    let mut chunks = Vec::with_capacity(rows.div_ceil(rows_per_chunk));
    let mut offset = 0;
    while offset < rows {
        let length = (rows - offset).min(rows_per_chunk);
        chunks.push(batch.slice(offset, length));
        offset += length;
    }
    chunks
}

fn read_local_catalog_pointer(cfg: &LakehouseConfig) -> Result<Option<LocalCatalogPointer>> {
    let path = cfg.catalog_pointer_path();
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path).context("read local iceberg catalog pointer")?;
    let pointer = serde_json::from_slice(&bytes).context("parse local iceberg catalog pointer")?;
    Ok(Some(pointer))
}

fn write_local_catalog_pointer(cfg: &LakehouseConfig, metadata_location: &str) -> Result<()> {
    let pointer = LocalCatalogPointer {
        namespace: cfg.namespace.clone(),
        table: cfg.table.clone(),
        metadata_location: metadata_location.to_string(),
        updated_at_ms: now_ms(),
    };
    write_json_atomic(&cfg.catalog_pointer_path(), &pointer)
        .context("write local iceberg catalog pointer")
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()
            .context("read current directory")?
            .join(path))
    }
}

fn file_uri(path: &Path) -> String {
    format!("file://{}", path.to_string_lossy())
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).context("serialize json")?;
    fs::write(&tmp, bytes).context("write temporary json")?;
    fs::rename(&tmp, path).context("publish json")?;
    Ok(())
}

fn required_str<'a>(value: Option<&'a Value>, key: &'static str) -> Result<&'a str> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .with_context(|| format!("{key} must be a non-empty string"))
}

fn str_at(data: &serde_json::Map<String, Value>, key: &str) -> String {
    str_value(data.get(key))
}

fn str_value(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn classify_signal(data: &serde_json::Map<String, Value>) -> String {
    let signal = str_at(data, "signal");
    if !signal.is_empty() {
        return signal;
    }
    match str_at(data, "event_type").as_str() {
        "span" | "span_start" | "span_end" => "trace".to_string(),
        "metric" => "metric".to_string(),
        "log" => "log".to_string(),
        "analytics" | "track" | "page" | "screen" | "identify" | "group" | "alias" => {
            "analytics".to_string()
        }
        _ => "other".to_string(),
    }
}

fn parse_timestamp(value: &str) -> Result<i64> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("invalid timestamp {value}"))?
        .with_timezone(&Utc)
        .timestamp_micros())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn now_us() -> i64 {
    now_ms().saturating_mul(1000)
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::{
        EventRow, LakehouseCompactionOptions, LakehouseConfig, active_data_files,
        commit_events_ndjson, commit_events_ndjson_with_source, compact_events_iceberg,
        load_or_create_iceberg_table, record_batch_writer_chunks, rows_to_record_batch,
        scan_current_table_batches, table_properties,
    };

    #[test]
    fn commits_event_batch_to_parquet_and_metadata() {
        let root =
            std::env::temp_dir().join(format!("nanotrace-lakehouse-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let cfg = LakehouseConfig::events_table(&root);
        let ndjson = br#"{"event_id":"evt_1","timestamp":"2026-05-18T00:00:00Z","source_file":"events/part-1.ndjson","source_offset":0,"source_length":120,"data":{"tenant_id":"org_1","event_type":"span","trace_id":"tr_1","span_id":"sp_1","service":"api","duration_ms":42.5,"is_error":0}}
{"event_id":"evt_2","timestamp":"2026-05-18T00:00:01Z","data":{"tenant_id":"org_1","event_type":"log","name":"hello"}}"#;

        let commit = commit_events_ndjson(&cfg, ndjson).expect("commit");

        assert_eq!(commit.namespace, "nanotrace");
        assert_eq!(commit.table, "events");
        assert_eq!(commit.record_count, 2);
        assert!(!commit.deduplicated);
        let data_file_path = commit
            .data_file
            .strip_prefix("file://")
            .unwrap_or(commit.data_file.as_str());
        assert!(std::path::Path::new(data_file_path).exists());
        assert!(root.join("nanotrace/events/metadata").read_dir().is_ok());
        assert!(root.join("nanotrace/events/metadata/current.json").exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn reuses_existing_snapshot_for_same_source_batch() {
        let root = std::env::temp_dir().join(format!(
            "nanotrace-lakehouse-idempotency-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let cfg = LakehouseConfig::events_table(&root);
        let ndjson = br#"{"event_id":"evt_1","timestamp":"2026-05-18T00:00:00Z","data":{"tenant_id":"org_1","event_type":"span","trace_id":"tr_1","span_id":"sp_1","service":"api"}}"#;

        let first =
            commit_events_ndjson_with_source(&cfg, ndjson, Some("s3-batch:test")).expect("first");
        let second =
            commit_events_ndjson_with_source(&cfg, ndjson, Some("s3-batch:test")).expect("second");

        assert!(!first.deduplicated);
        assert!(second.deduplicated);
        assert_eq!(second.snapshot_id, first.snapshot_id);
        assert_eq!(second.sequence_number, first.sequence_number);
        assert_eq!(second.record_count, first.record_count);
        let metadata_count = std::fs::read_dir(root.join("nanotrace/events/metadata"))
            .expect("metadata dir")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".metadata.json")
            })
            .count();
        assert_eq!(metadata_count, 2);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn compacts_local_iceberg_table_without_new_nanotrace_append_commit() {
        let root = std::env::temp_dir().join(format!(
            "nanotrace-lakehouse-compaction-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let cfg = LakehouseConfig::events_table(&root).with_write_target_file_size_bytes(1);
        let first = br#"{"event_id":"evt_1","timestamp":"2026-05-18T00:00:00Z","data":{"tenant_id":"org_1","event_type":"log","name":"one"}}"#;
        let second = br#"{"event_id":"evt_2","timestamp":"2026-05-18T00:00:01Z","data":{"tenant_id":"org_1","event_type":"log","name":"two"}}"#;
        commit_events_ndjson(&cfg, first).expect("commit first");
        commit_events_ndjson(&cfg, second).expect("commit second");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let result = runtime
            .block_on(compact_events_iceberg(
                &cfg,
                LakehouseCompactionOptions {
                    small_file_bytes: u64::MAX,
                    min_input_files: 2,
                    target_file_size_bytes: 128 * 1024 * 1024,
                },
            ))
            .expect("compact");

        assert!(result.compacted);
        assert_eq!(result.input_file_count, 2);
        assert_eq!(result.input_small_file_count, 2);
        assert_eq!(result.input_record_count, 2);
        assert_eq!(result.output_file_count, 1);
        assert_eq!(result.output_record_count, 2);

        let (_catalog, table) = runtime
            .block_on(load_or_create_iceberg_table(&cfg))
            .expect("reload table");
        let files = runtime
            .block_on(active_data_files(&table))
            .expect("active files");
        assert_eq!(files.len(), 1);
        let rows = runtime
            .block_on(scan_current_table_batches(&table))
            .expect("scan compacted table")
            .into_iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>();
        assert_eq!(rows, 2);

        let nanotrace_snapshots = std::fs::read_dir(root.join("nanotrace/events/metadata"))
            .expect("read metadata dir")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".nanotrace.json")
                    && entry.file_name().to_string_lossy().starts_with("snapshot-")
            })
            .count();
        assert_eq!(nanotrace_snapshots, 2);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn tiny_target_file_size_rolls_commit_into_multiple_data_files() {
        let root = std::env::temp_dir().join(format!(
            "nanotrace-lakehouse-rollover-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let cfg = LakehouseConfig::events_table(&root).with_write_target_file_size_bytes(1);
        let ndjson = br#"{"event_id":"evt_1","timestamp":"2026-05-18T00:00:00Z","data":{"tenant_id":"org_1","event_type":"span","payload":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}
{"event_id":"evt_2","timestamp":"2026-05-18T00:00:01Z","data":{"tenant_id":"org_1","event_type":"span","payload":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}"#;

        let commit = commit_events_ndjson(&cfg, ndjson).expect("commit");

        assert_eq!(commit.record_count, 2);
        assert!(commit.data_files.len() > 1, "{:?}", commit.data_files);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn chunks_large_record_batches_for_writer_rollover() {
        let rows = vec![
            EventRow {
                tenant_id: "org_1".to_string(),
                event_id: "evt_1".to_string(),
                timestamp_us: 1,
                observed_timestamp_us: Some(1),
                ingested_timestamp_us: 1,
                event_type: "span".to_string(),
                signal: "event".to_string(),
                trace_id: String::new(),
                span_id: String::new(),
                parent_span_id: String::new(),
                service: String::new(),
                environment: String::new(),
                name: String::new(),
                duration_ms: None,
                is_error: Some(0),
                source_file: String::new(),
                source_offset: 0,
                source_length: 0,
                data: "{\"large\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}".to_string(),
            },
            EventRow {
                tenant_id: "org_1".to_string(),
                event_id: "evt_2".to_string(),
                timestamp_us: 2,
                observed_timestamp_us: Some(2),
                ingested_timestamp_us: 2,
                event_type: "span".to_string(),
                signal: "event".to_string(),
                trace_id: String::new(),
                span_id: String::new(),
                parent_span_id: String::new(),
                service: String::new(),
                environment: String::new(),
                name: String::new(),
                duration_ms: None,
                is_error: Some(0),
                source_file: String::new(),
                source_offset: 0,
                source_length: 0,
                data: "{\"large\":\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"}".to_string(),
            },
        ];
        let batch = rows_to_record_batch(&rows).expect("record batch");

        let chunks = record_batch_writer_chunks(&batch, 1);

        assert_eq!(chunks.len(), 2);
        assert_eq!(
            chunks.iter().map(|chunk| chunk.num_rows()).sum::<usize>(),
            2
        );
    }

    #[test]
    fn table_properties_include_retention_and_write_sizing() {
        let cfg = LakehouseConfig::events_table("/tmp/lakehouse")
            .with_write_target_file_size_bytes(256 * 1024 * 1024)
            .with_snapshot_retention(123, 456_000, 12);

        let properties = table_properties(&cfg);

        assert_eq!(
            properties.get("write.parquet.compression-codec"),
            Some(&"zstd".to_string())
        );
        assert_eq!(
            properties.get("write.target-file-size-bytes"),
            Some(&"268435456".to_string())
        );
        assert_eq!(
            properties.get("history.expire.min-snapshots-to-keep"),
            Some(&"123".to_string())
        );
        assert_eq!(
            properties.get("history.expire.max-snapshot-age-ms"),
            Some(&"456000".to_string())
        );
        assert_eq!(
            properties.get("write.metadata.previous-versions-max"),
            Some(&"12".to_string())
        );
        assert_eq!(
            properties.get("write.metadata.delete-after-commit.enabled"),
            Some(&"true".to_string())
        );
    }
}
