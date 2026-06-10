use std::{
    collections::{BTreeSet, HashMap},
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_json::LineDelimitedWriter;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use datafusion::{datasource::file_format::options::ParquetReadOptions, prelude::SessionContext};
use nanotrace_ingest::{
    DEFAULT_TABLEFLOW_TOPIC, consumer, event_kv_index_rows, event_text_index_rows, subscribe,
};
use nanotrace_lakehouse::{
    LakehouseCommit, LakehouseCompactionOptions, LakehouseCompactionResult, LakehouseConfig,
    compact_events_iceberg,
};
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, file::reader::ChunkReader};
use rdkafka::{
    Message,
    consumer::{CommitMode, Consumer},
};
use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
struct Config {
    kafka_brokers: String,
    tableflow_topic: String,
    tableflow_materializer_group_id: String,
    tableflow_materializer_client_id: String,
    warehouse_dir: PathBuf,
    namespace: String,
    source_table: String,
    clickhouse_url: String,
    clickhouse_user: Option<String>,
    clickhouse_password: Option<String>,
    clickhouse_database: String,
    clickhouse_events_table: String,
    clickhouse_event_text_index_table: String,
    clickhouse_event_kv_index_table: String,
    clickhouse_field_index_table: String,
    clickhouse_event_measures_table: String,
    clickhouse_measure_cube_points_table: String,
    clickhouse_measure_cube_rollups_table: String,
    clickhouse_counter_rollups_table: String,
    clickhouse_gauge_rollups_table: String,
    clickhouse_histogram_rollups_table: String,
    clickhouse_entity_state_updates_table: String,
    clickhouse_entity_state_current_table: String,
    clickhouse_report_results_table: String,
    clickhouse_sequence_report_results_table: String,
    clickhouse_cohort_memberships_table: String,
    clickhouse_definitions_table: String,
    truncate_events: bool,
    rebuild_raw: bool,
    rebuild_derived: bool,
    incremental_materialize: bool,
    materialize_loop: bool,
    materialization_queue_executor: bool,
    tableflow_materializer_mode: TableflowMaterializerMode,
    materialization_queue_max_chunks: usize,
    materialization_queue_lease_secs: u64,
    materialization_queue_worker_id: String,
    lakehouse_maintenance: bool,
    lakehouse_maintenance_small_file_bytes: u64,
    lakehouse_native_compaction: bool,
    lakehouse_native_compaction_min_input_files: usize,
    lakehouse_native_compaction_target_file_size_bytes: u64,
    lakehouse_maintenance_cmd: Option<String>,
    lakehouse_query: bool,
    lakehouse_query_tenant_id: Option<String>,
    lakehouse_query_from: Option<String>,
    lakehouse_query_to: Option<String>,
    lakehouse_query_event_type: Option<String>,
    lakehouse_query_text: Option<String>,
    lakehouse_query_regex: Option<String>,
    lakehouse_query_sql: Option<String>,
    lakehouse_query_tables: Vec<LakehouseSqlTableSpec>,
    lakehouse_query_limit: usize,
    commit_source: CommitSource,
    materialize_poll_interval: Duration,
    allow_non_empty: bool,
    from_sequence: u64,
    max_rows_per_insert: usize,
    max_bytes_per_insert: usize,
    s3_max_file_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableflowMaterializerMode {
    Disabled,
    Loop,
    Once { idle_timeout: Duration },
}

impl TableflowMaterializerMode {
    fn is_enabled(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    fn is_once(self) -> bool {
        matches!(self, Self::Once { .. })
    }

    fn label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Loop => "loop",
            Self::Once { .. } => "once",
        }
    }
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
struct LakehouseSqlTableSpec {
    name: String,
    locations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DataFileLocation {
    Local(PathBuf),
    S3 { bucket: String, key: String },
}

#[derive(Debug, Deserialize, Serialize)]
struct EventInsertRow {
    event_id: String,
    timestamp: String,
    #[serde(default)]
    observed_timestamp: String,
    #[serde(default)]
    ingested_timestamp: String,
    source_file: String,
    source_offset: u64,
    source_length: u32,
    data: Value,
}

#[derive(Debug, Deserialize)]
struct TableflowBatchRecord {
    schema_version: u16,
    batch_id: String,
    tenant_id: String,
    organization_id: String,
    received_at: String,
    source_topic: String,
    source_partition: i32,
    source_offset: i64,
    source_file: String,
    event_count: usize,
    events: Vec<EventInsertRow>,
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
    fields: Vec<FieldRule>,
    measures: Vec<MeasureRule>,
    measure_cubes: Vec<MeasureCubeRule>,
    metric_rollups: Vec<MetricRollupRule>,
    states: Vec<StateRule>,
    reports: Vec<ReportRule>,
    trace_reports: Vec<TraceReportRule>,
    retentions: Vec<RetentionRule>,
    sequences: Vec<SequenceRule>,
    cohorts: Vec<CohortRule>,
}

#[derive(Debug, Clone)]
struct FieldRule {
    tenant_id: String,
    definition_id: String,
    definition_version: u64,
    matcher: Matcher,
    output: FieldOutput,
}

#[derive(Debug, Clone)]
struct FieldOutput {
    field_name: StringExpr,
    mode: String,
    value: ValueExpr,
    value_type: String,
}

#[derive(Debug, Clone)]
struct MeasureRule {
    tenant_id: String,
    definition_id: String,
    definition_version: u64,
    matcher: Matcher,
    output: MeasureOutput,
}

#[derive(Debug, Clone)]
struct MeasureOutput {
    measure_name: StringExpr,
    value: NumberExpr,
    unit: StringExpr,
    dimensions: Vec<DimensionOutput>,
    bucket_seconds: u32,
}

#[derive(Debug, Clone)]
struct MeasureCubeRule {
    tenant_id: String,
    definition_id: String,
    definition_version: u64,
    matcher: Matcher,
    output: MeasureCubeOutput,
}

#[derive(Debug, Clone)]
struct MeasureCubeOutput {
    measure_name: StringExpr,
    value: NumberExpr,
    unit: StringExpr,
    dimension_sets: Vec<DimensionSetOutput>,
    bucket_seconds: u32,
}

#[derive(Debug, Clone)]
struct DimensionOutput {
    name: String,
    value: StringExpr,
}

#[derive(Debug, Clone)]
struct DimensionSetOutput {
    id: String,
    dimensions: Vec<DimensionOutput>,
}

#[derive(Debug, Clone)]
struct MetricRollupRule {
    tenant_id: String,
    definition_id: String,
    definition_version: u64,
    matcher: Matcher,
    output: MetricRollupOutput,
}

#[derive(Debug, Clone)]
struct MetricRollupOutput {
    metric_name: StringExpr,
    metric_kind: StringExpr,
    value: NumberExpr,
    unit: StringExpr,
    dimensions: Vec<DimensionOutput>,
    bucket_seconds: u32,
}

#[derive(Debug, Clone)]
struct StateRule {
    tenant_id: String,
    definition_id: String,
    definition_version: u64,
    matcher: Matcher,
    output: StateOutput,
}

#[derive(Debug, Clone)]
struct StateOutput {
    entity_type: StringExpr,
    entity_id: StringExpr,
    state_name: StringExpr,
    value: StringExpr,
    value_type: String,
}

#[derive(Debug, Clone)]
struct ReportRule {
    tenant_id: String,
    definition_version: u64,
    matcher: Matcher,
    output: ReportOutput,
}

#[derive(Debug, Clone)]
struct ReportOutput {
    report_id: StringExpr,
    dimensions: Vec<DimensionOutput>,
    metrics: Vec<ReportMetricOutput>,
    bucket_seconds: u32,
}

#[derive(Debug, Clone)]
struct ReportMetricOutput {
    name: String,
    op: ReportMetricOp,
    value: Option<NumberExpr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReportMetricOp {
    Count,
    ErrorCount,
    Sum,
}

#[derive(Debug, Clone)]
struct TraceReportRule {
    tenant_id: String,
    definition_version: u64,
    matcher: Matcher,
    output: TraceReportOutput,
}

#[derive(Debug, Clone)]
struct TraceReportOutput {
    report_id: StringExpr,
    dimensions: Vec<DimensionOutput>,
    bucket_seconds: u32,
}

#[derive(Debug, Clone)]
struct RetentionRule {
    tenant_id: String,
    definition_version: u64,
    matcher: Matcher,
    output: RetentionOutput,
}

#[derive(Debug, Clone)]
struct RetentionOutput {
    report_id: StringExpr,
    cohort_id: StringExpr,
    entity_type: StringExpr,
    entity_id: StringExpr,
    dimensions: Vec<DimensionOutput>,
    retention_bucket_seconds: u32,
}

#[derive(Debug, Clone)]
struct SequenceRule {
    tenant_id: String,
    definition_version: u64,
    matcher: Matcher,
    output: SequenceOutput,
}

#[derive(Debug, Clone)]
struct SequenceOutput {
    report_id: StringExpr,
    entity_id: StringExpr,
    segment: Vec<DimensionOutput>,
    steps: Vec<SequenceStep>,
    bucket_seconds: u32,
}

#[derive(Debug, Clone)]
struct SequenceStep {
    name: String,
    matcher: Matcher,
}

#[derive(Debug, Clone)]
struct CohortRule {
    tenant_id: String,
    definition_version: u64,
    matcher: Matcher,
    output: CohortOutput,
}

#[derive(Debug, Clone)]
struct CohortOutput {
    cohort_id: StringExpr,
    entity_type: StringExpr,
    entity_id: StringExpr,
}

#[derive(Debug, Clone, Default)]
struct Matcher {
    predicates: Vec<Predicate>,
}

#[derive(Debug, Clone)]
struct Predicate {
    path: String,
    op: PredicateOp,
    value: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PredicateOp {
    Exists,
    Eq,
    Neq,
    IsNumber,
    In,
}

#[derive(Debug, Clone)]
enum StringExpr {
    Literal(String),
    Path { path: String, default: String },
}

#[derive(Debug, Clone)]
enum ValueExpr {
    Literal(Value),
    Path { path: String },
}

#[derive(Debug, Clone)]
enum NumberExpr {
    Literal(f64),
    Path { path: String },
}

#[derive(Debug, Default)]
struct MaterializedCounts {
    events: usize,
    event_text_index: usize,
    event_kv_index: usize,
    field_index: usize,
    event_measures: usize,
    measure_cube_points: usize,
    counter_rollups: usize,
    gauge_rollups: usize,
    histogram_rollups: usize,
    entity_state_updates: usize,
    entity_state_current: usize,
    report_results: usize,
    sequence_report_results: usize,
    cohort_memberships: usize,
    output_versions: Vec<MaterializedOutputVersion>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MaterializedOutputVersion {
    tenant_id: String,
    target_type: &'static str,
    target_id: String,
    target_version: u64,
    row_count: u64,
    source_start: String,
    source_end: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MaterializeTargets {
    events: bool,
    event_text_index: bool,
    event_kv_index: bool,
    field_index: bool,
    event_measures: bool,
    measure_cube_points: bool,
    counter_rollups: bool,
    gauge_rollups: bool,
    histogram_rollups: bool,
    entity_state_updates: bool,
    entity_state_current: bool,
    report_results: bool,
    sequence_report_results: bool,
    cohort_memberships: bool,
}

#[derive(Debug, Clone, Copy)]
struct MaterializeOptions<'a> {
    file_index: usize,
    targets: MaterializeTargets,
    token_namespace: &'a str,
}

impl MaterializeTargets {
    fn none() -> Self {
        Self {
            events: false,
            event_text_index: false,
            event_kv_index: false,
            field_index: false,
            event_measures: false,
            measure_cube_points: false,
            counter_rollups: false,
            gauge_rollups: false,
            histogram_rollups: false,
            entity_state_updates: false,
            entity_state_current: false,
            report_results: false,
            sequence_report_results: false,
            cohort_memberships: false,
        }
    }

    fn all() -> Self {
        Self {
            events: true,
            event_text_index: true,
            event_kv_index: true,
            field_index: true,
            event_measures: true,
            measure_cube_points: true,
            counter_rollups: true,
            gauge_rollups: true,
            histogram_rollups: true,
            entity_state_updates: true,
            entity_state_current: true,
            report_results: true,
            sequence_report_results: true,
            cohort_memberships: true,
        }
    }

    fn any(self) -> bool {
        self.events
            || self.event_text_index
            || self.event_kv_index
            || self.field_index
            || self.event_measures
            || self.measure_cube_points
            || self.counter_rollups
            || self.gauge_rollups
            || self.histogram_rollups
            || self.entity_state_updates
            || self.entity_state_current
            || self.report_results
            || self.sequence_report_results
            || self.cohort_memberships
    }
}

#[derive(Debug, Serialize)]
struct FieldIndexRow {
    tenant_id: String,
    project_id: String,
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
    project_id: String,
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
struct MeasureCubePointRow {
    tenant_id: String,
    project_id: String,
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
    dimension_set_id: String,
    dimension_names: Vec<String>,
    dimension_values: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CounterRollupRow {
    tenant_id: String,
    project_id: String,
    definition_id: String,
    definition_version: u64,
    metric_name: String,
    unit: String,
    bucket_time: String,
    bucket_seconds: u32,
    dimensions: Value,
    count: u64,
    sum: f64,
}

#[derive(Debug, Serialize)]
struct GaugeRollupRow {
    tenant_id: String,
    project_id: String,
    definition_id: String,
    definition_version: u64,
    metric_name: String,
    unit: String,
    bucket_time: String,
    bucket_seconds: u32,
    dimensions: Value,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    last: f64,
}

#[derive(Debug, Serialize)]
struct HistogramRollupRow {
    tenant_id: String,
    project_id: String,
    definition_id: String,
    definition_version: u64,
    metric_name: String,
    unit: String,
    bucket_time: String,
    bucket_seconds: u32,
    dimensions: Value,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

#[derive(Debug, Clone, Serialize)]
struct EntityStateUpdateRow {
    tenant_id: String,
    project_id: String,
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
struct ReportResultRow {
    tenant_id: String,
    project_id: String,
    report_id: String,
    report_version: u64,
    bucket_time: String,
    dimensions: Value,
    metrics: Value,
}

#[derive(Debug, Serialize)]
struct SequenceReportResultRow {
    tenant_id: String,
    project_id: String,
    report_id: String,
    report_version: u64,
    bucket_time: String,
    segment: Value,
    step_index: u16,
    step_name: String,
    entity_count: u64,
    conversion_count: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CohortMembershipRow {
    tenant_id: String,
    #[serde(default)]
    project_id: String,
    cohort_id: String,
    cohort_version: u64,
    entity_type: String,
    entity_id: String,
    first_seen: String,
    last_seen: String,
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

#[derive(Debug, Serialize)]
struct MaterializationVersionPublishRow<'a> {
    tenant_id: &'a str,
    target_type: &'a str,
    target_id: &'a str,
    target_version: u64,
    status: &'a str,
    active: u8,
    source_start: &'a str,
    source_end: &'a str,
    row_count: u64,
    chunk_count: u64,
    config_hash: u64,
    config: Value,
    stats: Value,
    completed_at: &'a str,
}

#[derive(Debug, Serialize)]
struct MaterializationWatermarkPublishRow<'a> {
    tenant_id: &'a str,
    target_type: &'a str,
    target_id: &'a str,
    target_version: u64,
    source_table: &'a str,
    low_watermark: &'a str,
    high_watermark: &'a str,
    status: &'a str,
    lag_ms: u64,
    attributes: Value,
}

#[derive(Debug, Serialize)]
struct MaterializationJobPublishRow<'a> {
    tenant_id: &'a str,
    job_id: String,
    job_kind: &'a str,
    status: &'a str,
    priority: u8,
    target_type: &'a str,
    target_table: &'a str,
    target_id: &'a str,
    target_version: u64,
    source_table: &'a str,
    source_start: &'a str,
    source_end: &'a str,
    chunk_seconds: u32,
    total_chunks: u64,
    completed_chunks: u64,
    failed_chunks: u64,
    rows_scanned: u64,
    rows_written: u64,
    bytes_scanned: u64,
    bytes_written: u64,
    lease_owner: &'a str,
    leased_until: Option<&'a str>,
    attempt: u32,
    max_attempts: u32,
    error: &'a str,
    config: Value,
    created_at: &'a str,
    updated_at: &'a str,
    completed_at: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct MaterializationChunkPublishRow<'a> {
    tenant_id: &'a str,
    job_id: String,
    chunk_id: String,
    chunk_index: u64,
    status: &'a str,
    target_type: &'a str,
    target_table: &'a str,
    target_id: &'a str,
    target_version: u64,
    source_table: &'a str,
    source_start: &'a str,
    source_end: &'a str,
    rows_scanned: u64,
    rows_written: u64,
    bytes_scanned: u64,
    bytes_written: u64,
    lease_owner: &'a str,
    leased_until: Option<&'a str>,
    attempt: u32,
    max_attempts: u32,
    error: &'a str,
    started_at: Option<&'a str>,
    updated_at: &'a str,
    completed_at: Option<&'a str>,
    attributes: Value,
}

#[derive(Debug, Deserialize)]
struct QueuedMaterializationChunk {
    tenant_id: String,
    job_id: String,
    chunk_id: String,
    chunk_index: u64,
    target_type: String,
    target_table: String,
    target_id: String,
    target_version: u64,
    source_table: String,
    source_start: String,
    source_end: String,
    attempt: u32,
    max_attempts: u32,
}

#[derive(Debug, Deserialize, Default)]
struct MaterializationJobProgressRow {
    completed_chunks: u64,
    failed_chunks: u64,
    rows_scanned: u64,
    rows_written: u64,
    total_chunks: u64,
}

#[derive(Debug, Serialize)]
struct PipelineMetricRow {
    tenant_id: String,
    component: &'static str,
    metric_name: &'static str,
    value: f64,
    unit: &'static str,
    attributes: Value,
}

#[derive(Debug, Default)]
struct LakehouseMaintenanceAudit {
    commit_count: usize,
    data_file_count: usize,
    object_store_data_file_count: usize,
    known_data_file_bytes: u64,
    small_data_file_count: usize,
    data_file_inspect_error_count: usize,
    first_sequence: u64,
    last_sequence: u64,
    native_compaction_ran: bool,
    native_compaction_success: bool,
    native_compaction_compacted: bool,
    native_compaction_input_file_count: usize,
    native_compaction_input_small_file_count: usize,
    native_compaction_input_record_count: usize,
    native_compaction_output_file_count: usize,
    native_compaction_output_record_count: usize,
    native_compaction_snapshot_id: String,
    native_compaction_sequence_number: u64,
    native_compaction_reason: String,
    external_command_ran: bool,
    external_command_success: bool,
    engine_maintenance_required: bool,
    engine_maintenance_reason: String,
}

#[derive(Debug)]
struct LakehouseQueryFilter {
    tenant_id: Option<String>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    event_type: Option<String>,
    text: Option<String>,
    regex: Option<Regex>,
    limit: usize,
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

    if cfg.lakehouse_maintenance {
        run_lakehouse_maintenance(&client, &lakehouse_reader, &cfg).await?;
        return Ok(());
    }

    if cfg.lakehouse_query {
        let rows = run_lakehouse_query(&client, &lakehouse_reader, &cfg).await?;
        eprintln!("lakehouse_query_rows={rows}");
        return Ok(());
    }

    if cfg.tableflow_materializer_mode.is_enabled() {
        run_tableflow_materializer(&client, &cfg).await?;
        return Ok(());
    }

    if cfg.materialize_loop {
        if cfg.materialization_queue_executor {
            run_materialization_queue_loop(&client, &lakehouse_reader, &cfg).await?;
            return Ok(());
        }
        run_materializer_loop(&client, &lakehouse_reader, &cfg).await?;
        return Ok(());
    }

    if cfg.materialization_queue_executor {
        let chunks = run_materialization_queue_pass(&client, &lakehouse_reader, &cfg).await?;
        println!("materialization_queue_executed_chunks={chunks}");
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
            "materialization_definitions fields={} measures={} measure_cubes={} states={} reports={} trace_reports={} retentions={} sequences={} cohorts={}",
            definitions.fields.len(),
            definitions.measures.len(),
            definitions.measure_cubes.len(),
            definitions.states.len(),
            definitions.reports.len(),
            definitions.trace_reports.len(),
            definitions.retentions.len(),
            definitions.sequences.len(),
            definitions.cohorts.len()
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
            cfg.qualified_event_text_index_table(),
            cfg.qualified_event_kv_index_table(),
            cfg.qualified_field_index_table(),
            cfg.qualified_event_measures_table(),
            cfg.qualified_measure_cube_points_table(),
            cfg.qualified_measure_cube_rollups_table(),
            cfg.qualified_counter_rollups_table(),
            cfg.qualified_gauge_rollups_table(),
            cfg.qualified_histogram_rollups_table(),
            cfg.qualified_entity_state_updates_table(),
            cfg.qualified_entity_state_current_table(),
            cfg.qualified_report_results_table(),
            cfg.qualified_sequence_report_results_table(),
            cfg.qualified_cohort_memberships_table(),
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
        let clickhouse_event_text_index_table =
            env_or("CLICKHOUSE_EVENT_TEXT_INDEX_TABLE", "event_text_index");
        let clickhouse_event_kv_index_table =
            env_or("CLICKHOUSE_EVENT_KV_INDEX_TABLE", "event_kv_index");
        let clickhouse_field_index_table = env_or("CLICKHOUSE_FIELD_INDEX_TABLE", "field_index");
        let clickhouse_event_measures_table =
            env_or("CLICKHOUSE_EVENT_MEASURES_TABLE", "event_measures");
        let clickhouse_measure_cube_points_table = env_or(
            "CLICKHOUSE_MEASURE_CUBE_POINTS_TABLE",
            "measure_cube_points",
        );
        let clickhouse_measure_cube_rollups_table = env_or(
            "CLICKHOUSE_MEASURE_CUBE_ROLLUPS_TABLE",
            "measure_cube_rollups",
        );
        let clickhouse_counter_rollups_table =
            env_or("CLICKHOUSE_COUNTER_ROLLUPS_TABLE", "counter_rollups");
        let clickhouse_gauge_rollups_table =
            env_or("CLICKHOUSE_GAUGE_ROLLUPS_TABLE", "gauge_rollups");
        let clickhouse_histogram_rollups_table =
            env_or("CLICKHOUSE_HISTOGRAM_ROLLUPS_TABLE", "histogram_rollups");
        let clickhouse_entity_state_updates_table = env_or(
            "CLICKHOUSE_ENTITY_STATE_UPDATES_TABLE",
            "entity_state_updates",
        );
        let clickhouse_entity_state_current_table = env_or(
            "CLICKHOUSE_ENTITY_STATE_CURRENT_TABLE",
            "entity_state_current",
        );
        let clickhouse_report_results_table =
            env_or("CLICKHOUSE_REPORT_RESULTS_TABLE", "report_results");
        let clickhouse_sequence_report_results_table = env_or(
            "CLICKHOUSE_SEQUENCE_REPORT_RESULTS_TABLE",
            "sequence_report_results",
        );
        let clickhouse_cohort_memberships_table =
            env_or("CLICKHOUSE_COHORT_MEMBERSHIPS_TABLE", "cohort_memberships");
        let clickhouse_definitions_table = env_or("CLICKHOUSE_DEFINITIONS_TABLE", "definitions");
        validate_identifier("CLICKHOUSE_DATABASE", &clickhouse_database)?;
        validate_identifier("CLICKHOUSE_TABLE", &clickhouse_events_table)?;
        validate_identifier(
            "CLICKHOUSE_EVENT_TEXT_INDEX_TABLE",
            &clickhouse_event_text_index_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_EVENT_KV_INDEX_TABLE",
            &clickhouse_event_kv_index_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_FIELD_INDEX_TABLE",
            &clickhouse_field_index_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_EVENT_MEASURES_TABLE",
            &clickhouse_event_measures_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_MEASURE_CUBE_POINTS_TABLE",
            &clickhouse_measure_cube_points_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_MEASURE_CUBE_ROLLUPS_TABLE",
            &clickhouse_measure_cube_rollups_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_COUNTER_ROLLUPS_TABLE",
            &clickhouse_counter_rollups_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_GAUGE_ROLLUPS_TABLE",
            &clickhouse_gauge_rollups_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_HISTOGRAM_ROLLUPS_TABLE",
            &clickhouse_histogram_rollups_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_ENTITY_STATE_UPDATES_TABLE",
            &clickhouse_entity_state_updates_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_ENTITY_STATE_CURRENT_TABLE",
            &clickhouse_entity_state_current_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_REPORT_RESULTS_TABLE",
            &clickhouse_report_results_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_SEQUENCE_REPORT_RESULTS_TABLE",
            &clickhouse_sequence_report_results_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_COHORT_MEMBERSHIPS_TABLE",
            &clickhouse_cohort_memberships_table,
        )?;
        validate_identifier(
            "CLICKHOUSE_DEFINITIONS_TABLE",
            &clickhouse_definitions_table,
        )?;

        Ok(Self {
            kafka_brokers: env_or("NANOTRACE_KAFKA_BROKERS", "redpanda:9092"),
            tableflow_topic: env_or("NANOTRACE_KAFKA_TABLEFLOW_TOPIC", DEFAULT_TABLEFLOW_TOPIC),
            tableflow_materializer_group_id: env_or(
                "NANOTRACE_TABLEFLOW_MATERIALIZER_GROUP_ID",
                "nanotrace-tableflow-materializer",
            ),
            tableflow_materializer_client_id: env_or(
                "NANOTRACE_TABLEFLOW_MATERIALIZER_CLIENT_ID",
                "nanotrace-tableflow-materializer",
            ),
            warehouse_dir: PathBuf::from(env_or(
                "NANOTRACE_LAKEHOUSE_WAREHOUSE_DIR",
                "/var/lib/nanotrace/lakehouse",
            )),
            namespace: env_or("NANOTRACE_LAKEHOUSE_NAMESPACE", "nanotrace"),
            source_table: env_or("NANOTRACE_LAKEHOUSE_TABLE", "events"),
            clickhouse_url: env_or("CLICKHOUSE_URL", "http://clickhouse:8123"),
            clickhouse_user: optional("CLICKHOUSE_USER"),
            clickhouse_password: optional("CLICKHOUSE_PASSWORD"),
            clickhouse_database,
            clickhouse_events_table,
            clickhouse_event_text_index_table,
            clickhouse_event_kv_index_table,
            clickhouse_field_index_table,
            clickhouse_event_measures_table,
            clickhouse_measure_cube_points_table,
            clickhouse_measure_cube_rollups_table,
            clickhouse_counter_rollups_table,
            clickhouse_gauge_rollups_table,
            clickhouse_histogram_rollups_table,
            clickhouse_entity_state_updates_table,
            clickhouse_entity_state_current_table,
            clickhouse_report_results_table,
            clickhouse_sequence_report_results_table,
            clickhouse_cohort_memberships_table,
            clickhouse_definitions_table,
            truncate_events: env_bool("NANOTRACE_REBUILD_TRUNCATE"),
            rebuild_raw: env_bool_default("NANOTRACE_REBUILD_RAW", true),
            rebuild_derived: env_bool_default("NANOTRACE_REBUILD_DERIVED", true),
            incremental_materialize: env_bool("NANOTRACE_MATERIALIZE_INCREMENTAL"),
            materialize_loop: env_bool("NANOTRACE_MATERIALIZE_LOOP"),
            materialization_queue_executor: env_bool("NANOTRACE_MATERIALIZATION_QUEUE_EXECUTOR"),
            tableflow_materializer_mode: tableflow_materializer_mode_from_env()?,
            materialization_queue_max_chunks: optional_usize(
                "NANOTRACE_MATERIALIZATION_QUEUE_MAX_CHUNKS",
                10,
            )?,
            materialization_queue_lease_secs: optional_u64(
                "NANOTRACE_MATERIALIZATION_QUEUE_LEASE_SECS",
                300,
            )?,
            materialization_queue_worker_id: env_or(
                "NANOTRACE_MATERIALIZATION_QUEUE_WORKER_ID",
                "lakehouse-rebuild",
            ),
            lakehouse_maintenance: env_bool("NANOTRACE_LAKEHOUSE_MAINTENANCE"),
            lakehouse_maintenance_small_file_bytes: optional_u64(
                "NANOTRACE_LAKEHOUSE_MAINTENANCE_SMALL_FILE_BYTES",
                128 * 1024 * 1024,
            )?,
            lakehouse_native_compaction: env_bool("NANOTRACE_LAKEHOUSE_NATIVE_COMPACTION"),
            lakehouse_native_compaction_min_input_files: optional_usize(
                "NANOTRACE_LAKEHOUSE_NATIVE_COMPACTION_MIN_INPUT_FILES",
                2,
            )?,
            lakehouse_native_compaction_target_file_size_bytes: optional_u64(
                "NANOTRACE_LAKEHOUSE_NATIVE_COMPACTION_TARGET_FILE_SIZE_BYTES",
                512 * 1024 * 1024,
            )?,
            lakehouse_maintenance_cmd: optional("NANOTRACE_ICEBERG_MAINTENANCE_CMD"),
            lakehouse_query: env_bool("NANOTRACE_LAKEHOUSE_QUERY"),
            lakehouse_query_tenant_id: optional("NANOTRACE_LAKEHOUSE_QUERY_TENANT_ID"),
            lakehouse_query_from: optional("NANOTRACE_LAKEHOUSE_QUERY_FROM"),
            lakehouse_query_to: optional("NANOTRACE_LAKEHOUSE_QUERY_TO"),
            lakehouse_query_event_type: optional("NANOTRACE_LAKEHOUSE_QUERY_EVENT_TYPE"),
            lakehouse_query_text: optional("NANOTRACE_LAKEHOUSE_QUERY_TEXT"),
            lakehouse_query_regex: optional("NANOTRACE_LAKEHOUSE_QUERY_REGEX"),
            lakehouse_query_sql: optional("NANOTRACE_LAKEHOUSE_QUERY_SQL"),
            lakehouse_query_tables: optional("NANOTRACE_LAKEHOUSE_QUERY_TABLES")
                .as_deref()
                .map(parse_lakehouse_sql_table_specs)
                .transpose()?
                .unwrap_or_default(),
            lakehouse_query_limit: optional_usize("NANOTRACE_LAKEHOUSE_QUERY_LIMIT", 1000)?,
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

    fn qualified_event_kv_index_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_event_kv_index_table
        )
    }

    fn qualified_event_text_index_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_event_text_index_table
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

    fn qualified_measure_cube_points_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_measure_cube_points_table
        )
    }

    fn qualified_measure_cube_rollups_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_measure_cube_rollups_table
        )
    }

    fn qualified_counter_rollups_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_counter_rollups_table
        )
    }

    fn qualified_gauge_rollups_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_gauge_rollups_table
        )
    }

    fn qualified_histogram_rollups_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_histogram_rollups_table
        )
    }

    fn qualified_entity_state_updates_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_entity_state_updates_table
        )
    }

    fn qualified_entity_state_current_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_entity_state_current_table
        )
    }

    fn qualified_report_results_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_report_results_table
        )
    }

    fn qualified_sequence_report_results_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_sequence_report_results_table
        )
    }

    fn qualified_cohort_memberships_table(&self) -> String {
        format!(
            "{}.{}",
            self.clickhouse_database, self.clickhouse_cohort_memberships_table
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

    fn qualified_materialization_jobs_table(&self) -> String {
        format!("{}.materialization_jobs", self.clickhouse_database)
    }

    fn qualified_materialization_chunks_table(&self) -> String {
        format!("{}.materialization_chunks", self.clickhouse_database)
    }

    fn qualified_materialization_versions_table(&self) -> String {
        format!("{}.materialization_versions", self.clickhouse_database)
    }

    fn qualified_materialization_watermarks_table(&self) -> String {
        format!("{}.materialization_watermarks", self.clickhouse_database)
    }

    fn qualified_pipeline_metrics_table(&self) -> String {
        format!("{}.pipeline_metrics", self.clickhouse_database)
    }
}

fn tableflow_materializer_mode_from_env() -> Result<TableflowMaterializerMode> {
    if env_bool("NANOTRACE_TABLEFLOW_MATERIALIZE_ONCE") {
        let idle_secs = optional_u64("NANOTRACE_TABLEFLOW_MATERIALIZE_IDLE_SECS", 5)?;
        return Ok(TableflowMaterializerMode::Once {
            idle_timeout: Duration::from_secs(idle_secs),
        });
    }
    if env_bool("NANOTRACE_TABLEFLOW_MATERIALIZE_LOOP") {
        return Ok(TableflowMaterializerMode::Loop);
    }
    Ok(TableflowMaterializerMode::Disabled)
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
            materialized.event_text_index += counts.event_text_index;
            materialized.event_kv_index += counts.event_kv_index;
            materialized.field_index += counts.field_index;
            materialized.event_measures += counts.event_measures;
            materialized.measure_cube_points += counts.measure_cube_points;
            materialized.counter_rollups += counts.counter_rollups;
            materialized.gauge_rollups += counts.gauge_rollups;
            materialized.histogram_rollups += counts.histogram_rollups;
            materialized.entity_state_updates += counts.entity_state_updates;
            materialized.entity_state_current += counts.entity_state_current;
            materialized.report_results += counts.report_results;
            materialized.sequence_report_results += counts.sequence_report_results;
            materialized.cohort_memberships += counts.cohort_memberships;
            materialized.output_versions.extend(counts.output_versions);
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
            materialized.event_kv_index += counts.event_kv_index;
            materialized.events += counts.events;
            materialized.event_text_index += counts.event_text_index;
            materialized.field_index += counts.field_index;
            materialized.event_measures += counts.event_measures;
            materialized.measure_cube_points += counts.measure_cube_points;
            materialized.counter_rollups += counts.counter_rollups;
            materialized.gauge_rollups += counts.gauge_rollups;
            materialized.histogram_rollups += counts.histogram_rollups;
            materialized.entity_state_updates += counts.entity_state_updates;
            materialized.entity_state_current += counts.entity_state_current;
            materialized.report_results += counts.report_results;
            materialized.sequence_report_results += counts.sequence_report_results;
            materialized.cohort_memberships += counts.cohort_memberships;
            materialized.output_versions.extend(counts.output_versions);
        }
        insert_incremental_materialization_metadata(client, cfg, commit, &materialized, targets)
            .await?;
        if targets.events {
            watermarks.insert(cfg.clickhouse_events_table.clone(), commit.sequence_number);
        }
        if targets.event_kv_index {
            watermarks.insert(
                cfg.clickhouse_event_kv_index_table.clone(),
                commit.sequence_number,
            );
        }
        if targets.event_text_index {
            watermarks.insert(
                cfg.clickhouse_event_text_index_table.clone(),
                commit.sequence_number,
            );
        }
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
        if targets.measure_cube_points {
            watermarks.insert(
                cfg.clickhouse_measure_cube_rollups_table.clone(),
                commit.sequence_number,
            );
        }
        if targets.counter_rollups {
            watermarks.insert(
                cfg.clickhouse_counter_rollups_table.clone(),
                commit.sequence_number,
            );
        }
        if targets.gauge_rollups {
            watermarks.insert(
                cfg.clickhouse_gauge_rollups_table.clone(),
                commit.sequence_number,
            );
        }
        if targets.histogram_rollups {
            watermarks.insert(
                cfg.clickhouse_histogram_rollups_table.clone(),
                commit.sequence_number,
            );
        }
        if targets.entity_state_updates {
            watermarks.insert(
                cfg.clickhouse_entity_state_updates_table.clone(),
                commit.sequence_number,
            );
        }
        if targets.entity_state_current {
            watermarks.insert(
                cfg.clickhouse_entity_state_current_table.clone(),
                commit.sequence_number,
            );
        }
        if targets.report_results {
            watermarks.insert(
                cfg.clickhouse_report_results_table.clone(),
                commit.sequence_number,
            );
        }
        if targets.sequence_report_results {
            watermarks.insert(
                cfg.clickhouse_sequence_report_results_table.clone(),
                commit.sequence_number,
            );
        }
        if targets.cohort_memberships {
            watermarks.insert(
                cfg.clickhouse_cohort_memberships_table.clone(),
                commit.sequence_number,
            );
        }
        println!(
            "materialized snapshot={} sequence={} scanned_rows={} event_rows={} event_text_index_rows={} event_kv_index_rows={} field_index_rows={} event_measure_rows={} measure_cube_point_rows={} counter_rollup_rows={} gauge_rollup_rows={} histogram_rollup_rows={} entity_state_update_rows={} entity_state_current_rows={} report_result_rows={} sequence_report_result_rows={} cohort_membership_rows={}",
            commit.snapshot_id,
            commit.sequence_number,
            commit_scanned_rows,
            materialized.events,
            materialized.event_text_index,
            materialized.event_kv_index,
            materialized.field_index,
            materialized.event_measures,
            materialized.measure_cube_points,
            materialized.counter_rollups,
            materialized.gauge_rollups,
            materialized.histogram_rollups,
            materialized.entity_state_updates,
            materialized.entity_state_current,
            materialized.report_results,
            materialized.sequence_report_results,
            materialized.cohort_memberships
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
        "materialization_definitions fields={} measures={} measure_cubes={} states={} reports={} trace_reports={} retentions={} sequences={} cohorts={}",
        definitions.fields.len(),
        definitions.measures.len(),
        definitions.measure_cubes.len(),
        definitions.states.len(),
        definitions.reports.len(),
        definitions.trace_reports.len(),
        definitions.retentions.len(),
        definitions.sequences.len(),
        definitions.cohorts.len()
    );
    run_incremental_materialize(client, lakehouse_reader, cfg, &commits, &definitions).await
}

async fn run_tableflow_materializer(client: &Client, cfg: &Config) -> Result<()> {
    let consumer = consumer(
        &cfg.kafka_brokers,
        &cfg.tableflow_materializer_group_id,
        &cfg.tableflow_materializer_client_id,
    )
    .context("create Tableflow materializer Kafka consumer")?;
    subscribe(&consumer, &cfg.tableflow_topic).context("subscribe to Tableflow topic")?;
    println!(
        "tableflow materializer starting topic={} group={} mode={}",
        cfg.tableflow_topic,
        cfg.tableflow_materializer_group_id,
        cfg.tableflow_materializer_mode.label()
    );

    loop {
        let message = match cfg.tableflow_materializer_mode {
            TableflowMaterializerMode::Disabled => return Ok(()),
            TableflowMaterializerMode::Once { idle_timeout } => {
                match tokio::time::timeout(idle_timeout, consumer.recv()).await {
                    Ok(result) => result,
                    Err(_) => {
                        println!("tableflow materializer idle; stopping");
                        return Ok(());
                    }
                }
            }
            TableflowMaterializerMode::Loop => {
                tokio::select! {
                    message = consumer.recv() => message,
                    _ = shutdown_signal() => {
                        println!("tableflow materializer stopping");
                        return Ok(());
                    }
                }
            }
        };

        match message {
            Ok(message) => {
                if let Err(err) = process_tableflow_message(client, cfg, &message).await {
                    eprintln!("tableflow materializer message failed: {err:?}");
                    if cfg.tableflow_materializer_mode.is_once() {
                        return Err(err);
                    }
                } else {
                    consumer
                        .commit_message(&message, CommitMode::Sync)
                        .context("commit Tableflow materializer Kafka offset")?;
                }
            }
            Err(err) => {
                eprintln!("tableflow materializer Kafka receive failed: {err}");
                if cfg.tableflow_materializer_mode.is_once() {
                    return Err(anyhow!(
                        "Tableflow materializer Kafka receive failed: {err}"
                    ));
                }
            }
        }
    }
}

async fn process_tableflow_message(
    client: &Client,
    cfg: &Config,
    message: &rdkafka::message::BorrowedMessage<'_>,
) -> Result<()> {
    let payload = message.payload().unwrap_or_default();
    if payload.is_empty() {
        return Ok(());
    }
    let mut batch: TableflowBatchRecord =
        serde_json::from_slice(payload).context("parse Tableflow batch payload")?;
    if batch.schema_version != 1 {
        bail!(
            "unsupported Tableflow batch schema_version={}",
            batch.schema_version
        );
    }
    if batch.event_count != batch.events.len() {
        bail!(
            "Tableflow batch {} event_count={} but events.len()={}",
            batch.batch_id,
            batch.event_count,
            batch.events.len()
        );
    }
    if batch.events.is_empty() {
        return Ok(());
    }
    for row in &mut batch.events {
        if row.observed_timestamp.trim().is_empty() {
            row.observed_timestamp.clone_from(&row.timestamp);
        }
        if row.ingested_timestamp.trim().is_empty() {
            row.ingested_timestamp.clone_from(&batch.received_at);
        }
    }

    let definitions = active_definitions(client, cfg)
        .await
        .context("load active materialization definitions")?;
    let commit = tableflow_batch_commit(&batch, payload);
    let counts = materialize_rows(
        client,
        cfg,
        &commit,
        &batch.events,
        &definitions,
        MaterializeOptions {
            file_index: 0,
            targets: MaterializeTargets::all(),
            token_namespace: "tableflow-materialize",
        },
    )
    .await
    .context("materialize Tableflow batch rows")?;
    insert_incremental_materialization_metadata(
        client,
        cfg,
        &commit,
        &counts,
        MaterializeTargets::all(),
    )
    .await
    .context("insert Tableflow materialization metadata")?;

    println!(
        "tableflow materialized batch={} tenant={} org={} received_at={} source={}/{}:{} rows={} event_rows={} event_kv_index_rows={} field_index_rows={} event_measure_rows={} report_result_rows={} sequence_report_result_rows={} cohort_membership_rows={}",
        batch.batch_id,
        batch.tenant_id,
        batch.organization_id,
        batch.received_at,
        batch.source_topic,
        batch.source_partition,
        batch.source_offset,
        batch.events.len(),
        counts.events,
        counts.event_kv_index,
        counts.field_index,
        counts.event_measures,
        counts.report_results,
        counts.sequence_report_results,
        counts.cohort_memberships
    );
    Ok(())
}

fn tableflow_batch_commit(batch: &TableflowBatchRecord, payload: &[u8]) -> LakehouseCommit {
    let sequence_number = tableflow_sequence_number(batch.source_partition, batch.source_offset);
    LakehouseCommit {
        namespace: "nanotrace".to_string(),
        table: "events".to_string(),
        snapshot_id: format!(
            "tableflow-{}-{}-{}",
            batch.source_topic, batch.source_partition, batch.source_offset
        ),
        sequence_number,
        committed_at_ms: chrono::Utc::now().timestamp_millis(),
        data_file: batch.source_file.clone(),
        data_files: vec![batch.source_file.clone()],
        record_count: batch.events.len(),
        content_sha256: hex_sha256(payload),
        metadata_location: format!(
            "kafka://{}/{}/{}",
            batch.source_topic, batch.source_partition, batch.source_offset
        ),
        source_batch_id: Some(batch.batch_id.clone()),
        deduplicated: false,
    }
}

fn tableflow_sequence_number(partition: i32, offset: i64) -> u64 {
    let partition = u64::try_from(partition.max(0)).unwrap_or(0);
    let offset = u64::try_from(offset.max(0)).unwrap_or(0);
    (partition << 48) | (offset & ((1_u64 << 48) - 1))
}

async fn run_lakehouse_maintenance(
    client: &Client,
    lakehouse_reader: &LakehouseReader,
    cfg: &Config,
) -> Result<()> {
    let commits = read_available_commit_records(client, cfg).await?;
    let mut audit = audit_lakehouse_maintenance(lakehouse_reader, cfg, &commits).await?;
    if cfg.lakehouse_native_compaction {
        audit.native_compaction_ran = true;
        match run_native_lakehouse_compaction(cfg).await {
            Ok(result) => {
                audit.native_compaction_success = true;
                apply_native_compaction_result(&mut audit, result);
            }
            Err(err) => {
                audit.native_compaction_success = false;
                audit.native_compaction_reason = format!("{err:#}");
                eprintln!("native Iceberg compaction failed: {err:#}");
            }
        }
    }
    if let Some(command) = cfg.lakehouse_maintenance_cmd.as_deref() {
        audit.external_command_ran = true;
        audit.external_command_success = run_external_iceberg_maintenance_command(cfg, command)?;
    }
    publish_lakehouse_maintenance_metrics(client, cfg, &audit).await?;
    println!(
        "lakehouse_maintenance commit_count={} data_file_count={} object_store_data_file_count={} known_data_file_bytes={} small_data_file_count={} data_file_inspect_error_count={} native_compaction_ran={} native_compaction_success={} native_compaction_compacted={} native_compaction_input_files={} native_compaction_output_files={} command_ran={} command_success={} engine_maintenance_required={}",
        audit.commit_count,
        audit.data_file_count,
        audit.object_store_data_file_count,
        audit.known_data_file_bytes,
        audit.small_data_file_count,
        audit.data_file_inspect_error_count,
        audit.native_compaction_ran,
        audit.native_compaction_success,
        audit.native_compaction_compacted,
        audit.native_compaction_input_file_count,
        audit.native_compaction_output_file_count,
        audit.external_command_ran,
        audit.external_command_success,
        audit.engine_maintenance_required
    );
    if audit.native_compaction_ran && !audit.native_compaction_success {
        bail!("native Iceberg compaction failed");
    }
    if audit.external_command_ran && !audit.external_command_success {
        bail!("external Iceberg maintenance command failed");
    }
    Ok(())
}

async fn run_lakehouse_query(
    client: &Client,
    lakehouse_reader: &LakehouseReader,
    cfg: &Config,
) -> Result<usize> {
    if cfg
        .lakehouse_query_sql
        .as_deref()
        .is_some_and(|sql| !sql.trim().is_empty())
    {
        return run_lakehouse_sql_query(client, lakehouse_reader, cfg).await;
    }

    let filter = LakehouseQueryFilter::from_config(cfg)?;
    let commits = read_available_commit_records(client, cfg).await?;
    let mut seen_data_files = BTreeSet::new();
    let mut emitted = 0usize;
    for commit in &commits {
        let data_files = commit_data_files(commit);
        for data_file in data_files {
            if !seen_data_files.insert(data_file.clone()) {
                continue;
            }
            let rows = lakehouse_reader
                .read_event_rows(&data_file)
                .await
                .with_context(|| format!("lakehouse query read {data_file}"))?;
            for row in rows {
                if !lakehouse_query_row_matches(&row, &filter) {
                    continue;
                }
                let output = serde_json::json!({
                    "event_id": row.event_id,
                    "timestamp": row.timestamp,
                    "observed_timestamp": row.observed_timestamp,
                    "ingested_timestamp": row.ingested_timestamp,
                    "source_file": row.source_file,
                    "source_offset": row.source_offset,
                    "source_length": row.source_length,
                    "source_namespace": commit.namespace,
                    "source_table": commit.table,
                    "source_snapshot_id": commit.snapshot_id,
                    "source_sequence_number": commit.sequence_number,
                    "source_data_file": data_file,
                    "data": row.data,
                });
                println!(
                    "{}",
                    serde_json::to_string(&output).context("serialize lakehouse query row")?
                );
                emitted += 1;
                if filter.limit > 0 && emitted >= filter.limit {
                    return Ok(emitted);
                }
            }
        }
    }
    Ok(emitted)
}

async fn run_lakehouse_sql_query(
    client: &Client,
    lakehouse_reader: &LakehouseReader,
    cfg: &Config,
) -> Result<usize> {
    let sql = cfg
        .lakehouse_query_sql
        .as_deref()
        .map(str::trim)
        .filter(|sql| !sql.is_empty())
        .context("NANOTRACE_LAKEHOUSE_QUERY_SQL is required for SQL lakehouse query mode")?;
    let commits = read_available_commit_records(client, cfg).await?;
    let event_locations = unique_commit_data_files(&commits);
    if event_locations.is_empty() {
        bail!("no committed lakehouse data files are available for SQL query");
    }

    let mut tables = vec![LakehouseSqlTableSpec {
        name: "events".to_string(),
        locations: event_locations,
    }];
    tables.extend(cfg.lakehouse_query_tables.iter().cloned());
    let stdout = std::io::stdout();
    run_lakehouse_sql_query_for_tables(
        lakehouse_reader,
        tables,
        sql,
        cfg.lakehouse_query_limit,
        stdout.lock(),
    )
    .await
}

async fn run_lakehouse_sql_query_for_tables<W: Write>(
    lakehouse_reader: &LakehouseReader,
    tables: Vec<LakehouseSqlTableSpec>,
    sql: &str,
    limit: usize,
    output: W,
) -> Result<usize> {
    let ctx = SessionContext::new();
    let mut tempdir = None::<tempfile::TempDir>;

    for table in tables {
        validate_lakehouse_sql_table_name(&table.name)?;
        if table.locations.is_empty() {
            bail!(
                "lakehouse SQL table {} has no Parquet locations",
                table.name
            );
        }
        let paths = lakehouse_reader
            .datafusion_local_paths(&table.locations, &mut tempdir)
            .await
            .with_context(|| format!("prepare lakehouse SQL table {}", table.name))?;
        let frame = ctx
            .read_parquet(paths, ParquetReadOptions::default())
            .await
            .with_context(|| format!("register Parquet inputs for {}", table.name))?;
        ctx.register_table(&table.name, frame.into_view())
            .with_context(|| format!("register lakehouse SQL table {}", table.name))?;
    }

    let batches = ctx
        .sql(sql)
        .await
        .context("plan lakehouse SQL query")?
        .collect()
        .await
        .context("execute lakehouse SQL query")?;
    write_record_batches_as_ndjson(batches, limit, output)
}

fn write_record_batches_as_ndjson<W: Write>(
    batches: Vec<RecordBatch>,
    limit: usize,
    output: W,
) -> Result<usize> {
    let mut writer = LineDelimitedWriter::new(output);
    let mut emitted = 0usize;
    for batch in batches {
        if limit > 0 && emitted >= limit {
            break;
        }
        let batch = if limit > 0 {
            let remaining = limit - emitted;
            if batch.num_rows() > remaining {
                batch.slice(0, remaining)
            } else {
                batch
            }
        } else {
            batch
        };
        emitted += batch.num_rows();
        writer.write(&batch).context("write lakehouse SQL result")?;
    }
    writer.finish().context("finish lakehouse SQL output")?;
    Ok(emitted)
}

impl LakehouseQueryFilter {
    fn from_config(cfg: &Config) -> Result<Self> {
        Ok(Self {
            tenant_id: cfg.lakehouse_query_tenant_id.clone(),
            from: cfg
                .lakehouse_query_from
                .as_deref()
                .map(parse_required_event_timestamp)
                .transpose()
                .context("parse NANOTRACE_LAKEHOUSE_QUERY_FROM")?,
            to: cfg
                .lakehouse_query_to
                .as_deref()
                .map(parse_required_event_timestamp)
                .transpose()
                .context("parse NANOTRACE_LAKEHOUSE_QUERY_TO")?,
            event_type: cfg.lakehouse_query_event_type.clone(),
            text: cfg
                .lakehouse_query_text
                .as_ref()
                .map(|value| value.to_ascii_lowercase()),
            regex: cfg
                .lakehouse_query_regex
                .as_deref()
                .map(Regex::new)
                .transpose()
                .context("compile NANOTRACE_LAKEHOUSE_QUERY_REGEX")?,
            limit: cfg.lakehouse_query_limit,
        })
    }
}

fn commit_data_files(commit: &LakehouseCommit) -> Vec<String> {
    if commit.data_files.is_empty() {
        if commit.data_file.is_empty() {
            Vec::new()
        } else {
            vec![commit.data_file.clone()]
        }
    } else {
        commit.data_files.clone()
    }
}

fn unique_commit_data_files(commits: &[LakehouseCommit]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut data_files = Vec::new();
    for commit in commits {
        for data_file in commit_data_files(commit) {
            if seen.insert(data_file.clone()) {
                data_files.push(data_file);
            }
        }
    }
    data_files
}

fn lakehouse_query_row_matches(row: &EventInsertRow, filter: &LakehouseQueryFilter) -> bool {
    let data = row.data.as_object();
    if let Some(tenant_id) = filter.tenant_id.as_deref()
        && data
            .map(|data| string_value(data.get("tenant_id")) != tenant_id)
            .unwrap_or(true)
    {
        return false;
    }
    if let Some(event_type) = filter.event_type.as_deref()
        && data
            .map(|data| string_value(data.get("event_type")) != event_type)
            .unwrap_or(true)
    {
        return false;
    }
    if filter.from.is_some() || filter.to.is_some() {
        let Some(timestamp) = parse_event_timestamp(&row.timestamp) else {
            return false;
        };
        if filter.from.as_ref().is_some_and(|from| timestamp < *from) {
            return false;
        }
        if filter.to.as_ref().is_some_and(|to| timestamp >= *to) {
            return false;
        }
    }
    let mut haystack = None::<String>;
    if let Some(needle) = filter.text.as_deref() {
        let haystack_value = lakehouse_query_haystack(row);
        let haystack_lower = haystack_value.to_ascii_lowercase();
        if !haystack_lower.contains(needle) {
            return false;
        }
        haystack = Some(haystack_value);
    }
    if let Some(regex) = filter.regex.as_ref() {
        let haystack_value = haystack.get_or_insert_with(|| lakehouse_query_haystack(row));
        if !regex.is_match(haystack_value) {
            return false;
        }
    }
    true
}

fn lakehouse_query_haystack(row: &EventInsertRow) -> String {
    let data = serde_json::to_string(&row.data).unwrap_or_default();
    if row.event_id.is_empty() {
        data
    } else if data.is_empty() {
        row.event_id.clone()
    } else {
        format!("{}\n{}", row.event_id, data)
    }
}

async fn audit_lakehouse_maintenance(
    lakehouse_reader: &LakehouseReader,
    cfg: &Config,
    commits: &[LakehouseCommit],
) -> Result<LakehouseMaintenanceAudit> {
    let mut audit = LakehouseMaintenanceAudit {
        commit_count: commits.len(),
        first_sequence: commits
            .iter()
            .map(|commit| commit.sequence_number)
            .min()
            .unwrap_or(0),
        last_sequence: commits
            .iter()
            .map(|commit| commit.sequence_number)
            .max()
            .unwrap_or(0),
        ..Default::default()
    };
    let mut data_files = BTreeSet::new();
    for commit in commits {
        if commit.data_files.is_empty() {
            if !commit.data_file.is_empty() {
                data_files.insert(commit.data_file.clone());
            }
        } else {
            data_files.extend(commit.data_files.iter().cloned());
        }
    }
    audit.data_file_count = data_files.len();
    for data_file in data_files {
        if is_object_store_data_file(&data_file) {
            audit.object_store_data_file_count += 1;
        }
        match lakehouse_reader.data_file_size(&data_file).await {
            Ok(Some(size)) => {
                audit.known_data_file_bytes = audit.known_data_file_bytes.saturating_add(size);
                if size < cfg.lakehouse_maintenance_small_file_bytes {
                    audit.small_data_file_count += 1;
                }
            }
            Ok(None) => {}
            Err(err) => {
                audit.data_file_inspect_error_count += 1;
                eprintln!("lakehouse maintenance could not inspect {data_file}: {err:#}");
            }
        }
    }
    if object_store_engine_maintenance_required(&audit, cfg) {
        audit.engine_maintenance_required = true;
        audit.engine_maintenance_reason = format!(
            "object-store Iceberg table has {} small files and no NANOTRACE_ICEBERG_MAINTENANCE_CMD",
            audit.small_data_file_count
        );
    }
    Ok(audit)
}

fn is_object_store_data_file(path: &str) -> bool {
    let lower = path.trim().to_ascii_lowercase();
    lower.starts_with("s3://")
        || lower.starts_with("s3a://")
        || lower.starts_with("gs://")
        || lower.starts_with("abfs://")
        || lower.starts_with("abfss://")
}

fn object_store_engine_maintenance_required(
    audit: &LakehouseMaintenanceAudit,
    cfg: &Config,
) -> bool {
    audit.object_store_data_file_count > 0
        && audit.small_data_file_count >= cfg.lakehouse_native_compaction_min_input_files
        && cfg.lakehouse_maintenance_cmd.is_none()
}

async fn run_native_lakehouse_compaction(cfg: &Config) -> Result<LakehouseCompactionResult> {
    let mut lakehouse_cfg = LakehouseConfig::events_table(cfg.warehouse_dir.clone())
        .with_write_target_file_size_bytes(cfg.lakehouse_native_compaction_target_file_size_bytes);
    lakehouse_cfg.namespace = cfg.namespace.clone();
    lakehouse_cfg.table = cfg.source_table.clone();
    compact_events_iceberg(
        &lakehouse_cfg,
        LakehouseCompactionOptions {
            small_file_bytes: cfg.lakehouse_maintenance_small_file_bytes,
            min_input_files: cfg.lakehouse_native_compaction_min_input_files,
            target_file_size_bytes: cfg.lakehouse_native_compaction_target_file_size_bytes,
        },
    )
    .await
}

fn apply_native_compaction_result(
    audit: &mut LakehouseMaintenanceAudit,
    result: LakehouseCompactionResult,
) {
    audit.native_compaction_compacted = result.compacted;
    audit.native_compaction_input_file_count = result.input_file_count;
    audit.native_compaction_input_small_file_count = result.input_small_file_count;
    audit.native_compaction_input_record_count = result.input_record_count;
    audit.native_compaction_output_file_count = result.output_file_count;
    audit.native_compaction_output_record_count = result.output_record_count;
    audit.native_compaction_snapshot_id = result.snapshot_id.unwrap_or_default();
    audit.native_compaction_sequence_number = result.sequence_number.unwrap_or(0);
    audit.native_compaction_reason = result.reason.unwrap_or_default();
}

fn run_external_iceberg_maintenance_command(cfg: &Config, command: &str) -> Result<bool> {
    let output = Command::new("sh")
        .arg("-c")
        .arg(command)
        .env("NANOTRACE_LAKEHOUSE_NAMESPACE", &cfg.namespace)
        .env("NANOTRACE_LAKEHOUSE_TABLE", &cfg.source_table)
        .env("NANOTRACE_LAKEHOUSE_WAREHOUSE_DIR", &cfg.warehouse_dir)
        .env(
            "NANOTRACE_LAKEHOUSE_MAINTENANCE_SMALL_FILE_BYTES",
            cfg.lakehouse_maintenance_small_file_bytes.to_string(),
        )
        .output()
        .context("run external Iceberg maintenance command")?;
    if !output.stdout.is_empty() {
        println!(
            "iceberg maintenance stdout:\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
    if !output.stderr.is_empty() {
        eprintln!(
            "iceberg maintenance stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(output.status.success())
}

async fn publish_lakehouse_maintenance_metrics(
    client: &Client,
    cfg: &Config,
    audit: &LakehouseMaintenanceAudit,
) -> Result<()> {
    let attributes = serde_json::json!({
        "namespace": &cfg.namespace,
        "table": &cfg.source_table,
        "first_sequence": audit.first_sequence,
        "last_sequence": audit.last_sequence,
        "small_file_threshold_bytes": cfg.lakehouse_maintenance_small_file_bytes,
        "native_compaction_configured": cfg.lakehouse_native_compaction,
        "native_compaction_ran": audit.native_compaction_ran,
        "native_compaction_success": audit.native_compaction_success,
        "native_compaction_compacted": audit.native_compaction_compacted,
        "native_compaction_min_input_files": cfg.lakehouse_native_compaction_min_input_files,
        "native_compaction_target_file_size_bytes": cfg.lakehouse_native_compaction_target_file_size_bytes,
        "native_compaction_snapshot_id": audit.native_compaction_snapshot_id,
        "native_compaction_sequence_number": audit.native_compaction_sequence_number,
        "native_compaction_reason": audit.native_compaction_reason,
        "external_command_configured": cfg.lakehouse_maintenance_cmd.is_some(),
        "external_command_ran": audit.external_command_ran,
        "external_command_success": audit.external_command_success,
        "object_store_data_file_count": audit.object_store_data_file_count,
        "engine_maintenance_required": audit.engine_maintenance_required,
        "engine_maintenance_reason": audit.engine_maintenance_reason,
    });
    let rows = vec![
        PipelineMetricRow {
            tenant_id: "system".to_string(),
            component: "lakehouse_maintenance",
            metric_name: "commit_count",
            value: audit.commit_count as f64,
            unit: "count",
            attributes: attributes.clone(),
        },
        PipelineMetricRow {
            tenant_id: "system".to_string(),
            component: "lakehouse_maintenance",
            metric_name: "data_file_count",
            value: audit.data_file_count as f64,
            unit: "count",
            attributes: attributes.clone(),
        },
        PipelineMetricRow {
            tenant_id: "system".to_string(),
            component: "lakehouse_maintenance",
            metric_name: "object_store_data_file_count",
            value: audit.object_store_data_file_count as f64,
            unit: "count",
            attributes: attributes.clone(),
        },
        PipelineMetricRow {
            tenant_id: "system".to_string(),
            component: "lakehouse_maintenance",
            metric_name: "known_data_file_bytes",
            value: audit.known_data_file_bytes as f64,
            unit: "bytes",
            attributes: attributes.clone(),
        },
        PipelineMetricRow {
            tenant_id: "system".to_string(),
            component: "lakehouse_maintenance",
            metric_name: "small_data_file_count",
            value: audit.small_data_file_count as f64,
            unit: "count",
            attributes: attributes.clone(),
        },
        PipelineMetricRow {
            tenant_id: "system".to_string(),
            component: "lakehouse_maintenance",
            metric_name: "data_file_inspect_error_count",
            value: audit.data_file_inspect_error_count as f64,
            unit: "count",
            attributes: attributes.clone(),
        },
        PipelineMetricRow {
            tenant_id: "system".to_string(),
            component: "lakehouse_maintenance",
            metric_name: "native_compaction_input_file_count",
            value: audit.native_compaction_input_file_count as f64,
            unit: "count",
            attributes: attributes.clone(),
        },
        PipelineMetricRow {
            tenant_id: "system".to_string(),
            component: "lakehouse_maintenance",
            metric_name: "native_compaction_input_small_file_count",
            value: audit.native_compaction_input_small_file_count as f64,
            unit: "count",
            attributes: attributes.clone(),
        },
        PipelineMetricRow {
            tenant_id: "system".to_string(),
            component: "lakehouse_maintenance",
            metric_name: "native_compaction_input_record_count",
            value: audit.native_compaction_input_record_count as f64,
            unit: "count",
            attributes: attributes.clone(),
        },
        PipelineMetricRow {
            tenant_id: "system".to_string(),
            component: "lakehouse_maintenance",
            metric_name: "native_compaction_output_file_count",
            value: audit.native_compaction_output_file_count as f64,
            unit: "count",
            attributes: attributes.clone(),
        },
        PipelineMetricRow {
            tenant_id: "system".to_string(),
            component: "lakehouse_maintenance",
            metric_name: "native_compaction_output_record_count",
            value: audit.native_compaction_output_record_count as f64,
            unit: "count",
            attributes: attributes.clone(),
        },
        PipelineMetricRow {
            tenant_id: "system".to_string(),
            component: "lakehouse_maintenance",
            metric_name: "engine_maintenance_required",
            value: if audit.engine_maintenance_required {
                1.0
            } else {
                0.0
            },
            unit: "boolean",
            attributes,
        },
    ];
    let dedupe_token = format!(
        "lakehouse-maintenance:pipeline_metrics:{}:{}:{}",
        cfg.namespace,
        cfg.source_table,
        Utc::now().timestamp_millis()
    );
    insert_clickhouse(
        client,
        cfg,
        &cfg.qualified_pipeline_metrics_table(),
        &rows_to_ndjson(&rows)?,
        &dedupe_token,
    )
    .await
}

async fn run_materialization_queue_loop(
    client: &Client,
    lakehouse_reader: &LakehouseReader,
    cfg: &Config,
) -> Result<()> {
    println!(
        "materialization queue executor starting namespace={} table={} poll_secs={} worker={}",
        cfg.namespace,
        cfg.source_table,
        cfg.materialize_poll_interval.as_secs(),
        cfg.materialization_queue_worker_id
    );

    loop {
        match run_materialization_queue_pass(client, lakehouse_reader, cfg).await {
            Ok(chunks) => {
                println!("materialization queue pass complete chunks={chunks}");
            }
            Err(err) => {
                eprintln!("materialization queue pass failed: {err:?}");
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(cfg.materialize_poll_interval) => {}
            _ = shutdown_signal() => {
                println!("materialization queue executor stopping");
                return Ok(());
            }
        }
    }
}

async fn run_materialization_queue_pass(
    client: &Client,
    lakehouse_reader: &LakehouseReader,
    cfg: &Config,
) -> Result<usize> {
    let chunks = pending_materialization_chunks(client, cfg).await?;
    if chunks.is_empty() {
        return Ok(0);
    }
    let commits = read_available_commit_records(client, cfg).await?;
    if commits.is_empty() {
        return Ok(0);
    }
    let definitions = active_definitions(client, cfg)
        .await
        .context("load active materialization definitions")?;

    let mut executed = 0usize;
    for chunk in chunks {
        claim_materialization_chunk(client, cfg, &chunk).await?;
        match execute_materialization_chunk(
            client,
            lakehouse_reader,
            cfg,
            &commits,
            &definitions,
            &chunk,
        )
        .await
        {
            Ok(counts) => {
                complete_materialization_chunk(client, cfg, &chunk, &counts).await?;
                publish_queued_materialization_version(client, cfg, &chunk, &counts).await?;
                refresh_materialization_job_progress(client, cfg, &chunk).await?;
                executed += 1;
            }
            Err(err) => {
                fail_materialization_chunk(client, cfg, &chunk, &err.to_string()).await?;
                refresh_materialization_job_progress(client, cfg, &chunk).await?;
                return Err(err).with_context(|| {
                    format!(
                        "execute materialization chunk {} for job {}",
                        chunk.chunk_id, chunk.job_id
                    )
                });
            }
        }
    }
    Ok(executed)
}

async fn execute_materialization_chunk(
    client: &Client,
    lakehouse_reader: &LakehouseReader,
    cfg: &Config,
    commits: &[LakehouseCommit],
    definitions: &ExtractionDefinitions,
    chunk: &QueuedMaterializationChunk,
) -> Result<MaterializedCounts> {
    let source_start = parse_event_timestamp(&chunk.source_start).with_context(|| {
        format!(
            "parse materialization chunk source_start {}",
            chunk.source_start
        )
    })?;
    let source_end = parse_event_timestamp(&chunk.source_end).with_context(|| {
        format!(
            "parse materialization chunk source_end {}",
            chunk.source_end
        )
    })?;
    if source_end <= source_start {
        bail!(
            "materialization chunk {} has empty or negative source window {}..{}",
            chunk.chunk_id,
            chunk.source_start,
            chunk.source_end
        );
    }
    let expected_target_table = materialization_target_table(&chunk.target_type);
    if chunk.target_table != expected_target_table {
        bail!(
            "materialization chunk {} target_table={} does not match target_type={} expected table {}",
            chunk.chunk_id,
            chunk.target_table,
            chunk.target_type,
            expected_target_table
        );
    }

    let definitions = definitions_for_queued_target(definitions, chunk)?;
    if !queued_definitions_have_target(&definitions, chunk.target_type.as_str()) {
        bail!(
            "no active literal {} definition found for target_id={} target_version={}",
            chunk.target_type,
            chunk.target_id,
            chunk.target_version
        );
    }
    let targets = materialize_targets_for_queued_target(chunk.target_type.as_str())?;
    let mut total = MaterializedCounts::default();

    for commit in commits {
        let files = if commit.data_files.is_empty() {
            vec![commit.data_file.clone()]
        } else {
            commit.data_files.clone()
        };
        for (file_index, data_file) in files.iter().enumerate() {
            let rows = lakehouse_reader
                .read_event_rows(data_file)
                .await
                .with_context(|| format!("read lakehouse data file {data_file}"))?;
            let rows = rows
                .into_iter()
                .filter(|row| event_row_in_window(row, source_start, source_end))
                .collect::<Vec<_>>();
            if rows.is_empty() {
                continue;
            }
            let token_namespace = format!("queued-materialize:{}:{}", chunk.job_id, chunk.chunk_id);
            let counts = materialize_rows(
                client,
                cfg,
                commit,
                &rows,
                &definitions,
                MaterializeOptions {
                    file_index,
                    targets,
                    token_namespace: &token_namespace,
                },
            )
            .await?;
            total.events += rows.len();
            total.report_results += counts.report_results;
            total.sequence_report_results += counts.sequence_report_results;
            total.cohort_memberships += counts.cohort_memberships;
            total.output_versions.extend(counts.output_versions);
        }
    }
    Ok(total)
}

async fn pending_materialization_chunks(
    client: &Client,
    cfg: &Config,
) -> Result<Vec<QueuedMaterializationChunk>> {
    let query = format!(
        "SELECT tenant_id, job_id, chunk_id, chunk_index, target_type, target_table, target_id, target_version, source_table, source_start, source_end, attempt, max_attempts FROM {} FINAL WHERE source_table = '{}' AND status IN ('pending', 'retry') AND attempt < max_attempts AND (isNull(leased_until) OR leased_until < now64(3)) ORDER BY source_start, job_id, chunk_index LIMIT {} FORMAT JSON",
        cfg.qualified_materialization_chunks_table(),
        sql_string(&cfg.source_table),
        cfg.materialization_queue_max_chunks
    );
    let body = clickhouse_query(client, cfg, &query).await?;
    let response: ClickHouseJson<QueuedMaterializationChunk> =
        serde_json::from_str(&body).context("parse pending materialization chunks response")?;
    Ok(response.data)
}

async fn claim_materialization_chunk(
    client: &Client,
    cfg: &Config,
    chunk: &QueuedMaterializationChunk,
) -> Result<()> {
    let lease_until = format!(
        "now64(3) + INTERVAL {} SECOND",
        cfg.materialization_queue_lease_secs
    );
    let predicate = materialization_chunk_predicate(chunk);
    let query = format!(
        "ALTER TABLE {} UPDATE status = 'running', lease_owner = '{}', leased_until = {lease_until}, started_at = ifNull(started_at, now64(3)), updated_at = now64(3), attempt = attempt + 1 WHERE {predicate} SETTINGS mutations_sync = 1",
        cfg.qualified_materialization_chunks_table(),
        sql_string(&cfg.materialization_queue_worker_id),
    );
    clickhouse_query(client, cfg, &query).await?;
    Ok(())
}

async fn complete_materialization_chunk(
    client: &Client,
    cfg: &Config,
    chunk: &QueuedMaterializationChunk,
    counts: &MaterializedCounts,
) -> Result<()> {
    let rows_written = queued_rows_written(chunk.target_type.as_str(), counts);
    let predicate = materialization_chunk_predicate(chunk);
    let query = format!(
        "ALTER TABLE {} UPDATE status = 'completed', rows_scanned = {}, rows_written = {}, lease_owner = '', leased_until = NULL, error = '', updated_at = now64(3), completed_at = now64(3) WHERE {predicate} SETTINGS mutations_sync = 1",
        cfg.qualified_materialization_chunks_table(),
        counts.events,
        rows_written,
    );
    clickhouse_query(client, cfg, &query).await?;
    Ok(())
}

async fn fail_materialization_chunk(
    client: &Client,
    cfg: &Config,
    chunk: &QueuedMaterializationChunk,
    error: &str,
) -> Result<()> {
    let status = if chunk.attempt + 1 >= chunk.max_attempts {
        "failed"
    } else {
        "retry"
    };
    let predicate = materialization_chunk_predicate(chunk);
    let query = format!(
        "ALTER TABLE {} UPDATE status = '{status}', lease_owner = '', leased_until = NULL, error = '{}', updated_at = now64(3) WHERE {predicate} SETTINGS mutations_sync = 1",
        cfg.qualified_materialization_chunks_table(),
        sql_string(error),
    );
    clickhouse_query(client, cfg, &query).await?;
    Ok(())
}

async fn refresh_materialization_job_progress(
    client: &Client,
    cfg: &Config,
    chunk: &QueuedMaterializationChunk,
) -> Result<()> {
    let progress = materialization_job_progress(client, cfg, chunk).await?;
    let status = if progress.failed_chunks > 0 {
        "failed"
    } else if progress.total_chunks > 0 && progress.completed_chunks >= progress.total_chunks {
        "completed"
    } else {
        "running"
    };
    let completed_at = if status == "completed" {
        "now64(3)"
    } else {
        "NULL"
    };
    let query = format!(
        "ALTER TABLE {} UPDATE status = '{status}', completed_chunks = {}, failed_chunks = {}, rows_scanned = {}, rows_written = {}, lease_owner = '{}', leased_until = NULL, updated_at = now64(3), completed_at = {completed_at} WHERE tenant_id = '{}' AND job_id = '{}' SETTINGS mutations_sync = 1",
        cfg.qualified_materialization_jobs_table(),
        progress.completed_chunks,
        progress.failed_chunks,
        progress.rows_scanned,
        progress.rows_written,
        sql_string(&cfg.materialization_queue_worker_id),
        sql_string(&chunk.tenant_id),
        sql_string(&chunk.job_id),
    );
    clickhouse_query(client, cfg, &query).await?;
    Ok(())
}

async fn materialization_job_progress(
    client: &Client,
    cfg: &Config,
    chunk: &QueuedMaterializationChunk,
) -> Result<MaterializationJobProgressRow> {
    let query = format!(
        "SELECT countIf(status = 'completed') AS completed_chunks, countIf(status = 'failed') AS failed_chunks, sum(rows_scanned) AS rows_scanned, sum(rows_written) AS rows_written, count() AS total_chunks FROM {} FINAL WHERE tenant_id = '{}' AND job_id = '{}' FORMAT JSON",
        cfg.qualified_materialization_chunks_table(),
        sql_string(&chunk.tenant_id),
        sql_string(&chunk.job_id),
    );
    let body = clickhouse_query(client, cfg, &query).await?;
    let response: ClickHouseJson<MaterializationJobProgressRow> =
        serde_json::from_str(&body).context("parse materialization job progress response")?;
    Ok(response.data.into_iter().next().unwrap_or_default())
}

async fn publish_queued_materialization_version(
    client: &Client,
    cfg: &Config,
    chunk: &QueuedMaterializationChunk,
    counts: &MaterializedCounts,
) -> Result<()> {
    let rows_written = queued_rows_written(chunk.target_type.as_str(), counts);
    let completed_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let target_type = queued_static_target_type(chunk.target_type.as_str())?;
    let version = MaterializedOutputVersion {
        tenant_id: chunk.tenant_id.clone(),
        target_type,
        target_id: chunk.target_id.clone(),
        target_version: chunk.target_version,
        row_count: rows_written,
        source_start: chunk.source_start.clone(),
        source_end: chunk.source_end.clone(),
    };
    let version_row = MaterializationVersionPublishRow {
        tenant_id: &version.tenant_id,
        target_type: version.target_type,
        target_id: &version.target_id,
        target_version: version.target_version,
        status: "completed",
        active: 1,
        source_start: &version.source_start,
        source_end: &version.source_end,
        row_count: version.row_count,
        chunk_count: 1,
        config_hash: 0,
        config: serde_json::json!({
            "job_id": &chunk.job_id,
            "chunk_id": &chunk.chunk_id,
            "chunk_index": chunk.chunk_index,
            "executor": "queued_materialization"
        }),
        stats: serde_json::json!({
            "rows_scanned": counts.events,
            "rows_written": rows_written
        }),
        completed_at: completed_at.as_str(),
    };
    let watermark_row = MaterializationWatermarkPublishRow {
        tenant_id: &version.tenant_id,
        target_type: version.target_type,
        target_id: &version.target_id,
        target_version: version.target_version,
        source_table: &chunk.source_table,
        low_watermark: &version.source_start,
        high_watermark: &version.source_end,
        status: "materialized",
        lag_ms: 0,
        attributes: serde_json::json!({
            "job_id": &chunk.job_id,
            "chunk_id": &chunk.chunk_id,
            "chunk_index": chunk.chunk_index,
            "rows_scanned": counts.events,
            "rows_written": rows_written,
            "executor": "queued_materialization"
        }),
    };
    let token_prefix = format!(
        "queued-materialize-metadata:{}:{}",
        chunk.job_id, chunk.chunk_id
    );
    insert_clickhouse(
        client,
        cfg,
        &cfg.qualified_materialization_versions_table(),
        &rows_to_ndjson(&[version_row])?,
        &format!("{token_prefix}:materialization_versions"),
    )
    .await?;
    insert_clickhouse(
        client,
        cfg,
        &cfg.qualified_materialization_watermarks_table(),
        &rows_to_ndjson(&[watermark_row])?,
        &format!("{token_prefix}:materialization_watermarks"),
    )
    .await?;
    Ok(())
}

fn materialize_targets_for_commit(
    cfg: &Config,
    sequence_number: u64,
    watermarks: &HashMap<String, u64>,
) -> MaterializeTargets {
    MaterializeTargets {
        events: watermarks
            .get(&cfg.clickhouse_events_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
        event_text_index: watermarks
            .get(&cfg.clickhouse_event_text_index_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
        event_kv_index: watermarks
            .get(&cfg.clickhouse_event_kv_index_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
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
        measure_cube_points: watermarks
            .get(&cfg.clickhouse_measure_cube_rollups_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
        counter_rollups: watermarks
            .get(&cfg.clickhouse_counter_rollups_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
        gauge_rollups: watermarks
            .get(&cfg.clickhouse_gauge_rollups_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
        histogram_rollups: watermarks
            .get(&cfg.clickhouse_histogram_rollups_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
        entity_state_updates: watermarks
            .get(&cfg.clickhouse_entity_state_updates_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
        entity_state_current: watermarks
            .get(&cfg.clickhouse_entity_state_current_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
        report_results: watermarks
            .get(&cfg.clickhouse_report_results_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
        sequence_report_results: watermarks
            .get(&cfg.clickhouse_sequence_report_results_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
        cohort_memberships: watermarks
            .get(&cfg.clickhouse_cohort_memberships_table)
            .copied()
            .unwrap_or(0)
            < sequence_number,
    }
}

fn materialize_targets_for_queued_target(target_type: &str) -> Result<MaterializeTargets> {
    let mut targets = MaterializeTargets::none();
    match target_type {
        "report" => targets.report_results = true,
        "sequence" => targets.sequence_report_results = true,
        "cohort" => targets.cohort_memberships = true,
        other => bail!(
            "queued materialization target_type must be report, sequence, or cohort; got {other}"
        ),
    }
    Ok(targets)
}

fn definitions_for_queued_target(
    definitions: &ExtractionDefinitions,
    chunk: &QueuedMaterializationChunk,
) -> Result<ExtractionDefinitions> {
    let mut filtered = ExtractionDefinitions::default();
    match chunk.target_type.as_str() {
        "report" => {
            filtered.reports = definitions
                .reports
                .iter()
                .filter(|rule| queued_report_rule_matches(rule, chunk))
                .cloned()
                .collect();
            filtered.trace_reports = definitions
                .trace_reports
                .iter()
                .filter(|rule| queued_trace_report_rule_matches(rule, chunk))
                .cloned()
                .collect();
            filtered.retentions = definitions
                .retentions
                .iter()
                .filter(|rule| queued_retention_rule_matches(rule, chunk))
                .cloned()
                .collect();
        }
        "sequence" => {
            filtered.sequences = definitions
                .sequences
                .iter()
                .filter(|rule| queued_sequence_rule_matches(rule, chunk))
                .cloned()
                .collect();
        }
        "cohort" => {
            filtered.cohorts = definitions
                .cohorts
                .iter()
                .filter(|rule| queued_cohort_rule_matches(rule, chunk))
                .cloned()
                .collect();
        }
        other => bail!(
            "queued materialization target_type must be report, sequence, or cohort; got {other}"
        ),
    }
    Ok(filtered)
}

fn queued_definitions_have_target(definitions: &ExtractionDefinitions, target_type: &str) -> bool {
    match target_type {
        "report" => {
            !definitions.reports.is_empty()
                || !definitions.trace_reports.is_empty()
                || !definitions.retentions.is_empty()
        }
        "sequence" => !definitions.sequences.is_empty(),
        "cohort" => !definitions.cohorts.is_empty(),
        _ => false,
    }
}

fn queued_report_rule_matches(rule: &&ReportRule, chunk: &QueuedMaterializationChunk) -> bool {
    rule.tenant_id == chunk.tenant_id
        && rule.definition_version == chunk.target_version
        && string_expr_literal_eq(&rule.output.report_id, &chunk.target_id)
}

fn queued_trace_report_rule_matches(
    rule: &&TraceReportRule,
    chunk: &QueuedMaterializationChunk,
) -> bool {
    rule.tenant_id == chunk.tenant_id
        && rule.definition_version == chunk.target_version
        && string_expr_literal_eq(&rule.output.report_id, &chunk.target_id)
}

fn queued_retention_rule_matches(
    rule: &&RetentionRule,
    chunk: &QueuedMaterializationChunk,
) -> bool {
    rule.tenant_id == chunk.tenant_id
        && rule.definition_version == chunk.target_version
        && string_expr_literal_eq(&rule.output.report_id, &chunk.target_id)
}

fn queued_sequence_rule_matches(rule: &&SequenceRule, chunk: &QueuedMaterializationChunk) -> bool {
    rule.tenant_id == chunk.tenant_id
        && rule.definition_version == chunk.target_version
        && string_expr_literal_eq(&rule.output.report_id, &chunk.target_id)
}

fn queued_cohort_rule_matches(rule: &&CohortRule, chunk: &QueuedMaterializationChunk) -> bool {
    rule.tenant_id == chunk.tenant_id
        && rule.definition_version == chunk.target_version
        && string_expr_literal_eq(&rule.output.cohort_id, &chunk.target_id)
}

fn string_expr_literal_eq(expr: &StringExpr, value: &str) -> bool {
    matches!(expr, StringExpr::Literal(literal) if literal == value)
}

fn event_row_in_window(row: &EventInsertRow, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
    parse_event_timestamp(&row.timestamp)
        .is_some_and(|timestamp| timestamp >= start && timestamp < end)
}

fn queued_rows_written(target_type: &str, counts: &MaterializedCounts) -> u64 {
    match target_type {
        "report" => counts.report_results.try_into().unwrap_or(u64::MAX),
        "sequence" => counts
            .sequence_report_results
            .try_into()
            .unwrap_or(u64::MAX),
        "cohort" => counts.cohort_memberships.try_into().unwrap_or(u64::MAX),
        _ => 0,
    }
}

fn queued_static_target_type(target_type: &str) -> Result<&'static str> {
    match target_type {
        "report" => Ok("report"),
        "sequence" => Ok("sequence"),
        "cohort" => Ok("cohort"),
        other => bail!(
            "queued materialization target_type must be report, sequence, or cohort; got {other}"
        ),
    }
}

fn materialization_chunk_predicate(chunk: &QueuedMaterializationChunk) -> String {
    format!(
        "tenant_id = '{}' AND job_id = '{}' AND chunk_id = '{}'",
        sql_string(&chunk.tenant_id),
        sql_string(&chunk.job_id),
        sql_string(&chunk.chunk_id)
    )
}

async fn materialize_rows(
    client: &Client,
    cfg: &Config,
    commit: &LakehouseCommit,
    rows: &[EventInsertRow],
    definitions: &ExtractionDefinitions,
    options: MaterializeOptions<'_>,
) -> Result<MaterializedCounts> {
    let mut event_text_index = Vec::new();
    let mut event_kv_index = Vec::new();
    let mut field_index = Vec::new();
    let mut event_measures = Vec::new();
    let mut measure_cube_points = Vec::new();
    let mut counter_rollups = Vec::new();
    let mut gauge_rollups = Vec::new();
    let mut histogram_rollups = Vec::new();
    let mut entity_state_updates = Vec::new();
    let mut entity_state_current = Vec::new();
    let mut report_results = Vec::new();
    let mut sequence_report_results = Vec::new();
    let mut cohort_memberships = Vec::new();

    for row in rows {
        if options.targets.event_text_index {
            let value = serde_json::to_value(row).context("serialize event row for text index")?;
            event_text_index.extend(event_text_index_rows(&value));
        }
        if options.targets.event_kv_index {
            let value = serde_json::to_value(row).context("serialize event row for KV index")?;
            event_kv_index.extend(event_kv_index_rows(&value));
        }
        if options.targets.field_index {
            field_index.extend(field_index_rows(row, definitions));
        }
        if options.targets.event_measures {
            event_measures.extend(event_measure_rows(row, definitions));
        }
        if options.targets.measure_cube_points {
            measure_cube_points.extend(measure_cube_point_rows(row, definitions));
        }
        let metric_rows = metric_rollup_rows(row, definitions);
        if options.targets.counter_rollups {
            counter_rollups.extend(metric_rows.counters);
        }
        if options.targets.gauge_rollups {
            gauge_rollups.extend(metric_rows.gauges);
        }
        if options.targets.histogram_rollups {
            histogram_rollups.extend(metric_rows.histograms);
        }
        if options.targets.entity_state_updates {
            entity_state_updates.extend(entity_state_update_rows(row, definitions));
        }
        if options.targets.entity_state_current {
            entity_state_current.extend(entity_state_update_rows(row, definitions));
        }
    }
    if options.targets.report_results {
        report_results.extend(report_result_rows(rows, definitions));
        report_results.extend(trace_report_result_rows(rows, definitions));
    }
    if options.targets.cohort_memberships {
        cohort_memberships.extend(cohort_membership_rows(rows, definitions));
    }
    if options.targets.report_results {
        let retention_memberships =
            retention_memberships_for_rows(client, cfg, rows, definitions, &cohort_memberships)
                .await?;
        report_results.extend(retention_report_result_rows(
            rows,
            definitions,
            &retention_memberships,
        ));
    }
    if options.targets.sequence_report_results {
        sequence_report_results.extend(sequence_report_result_rows(rows, definitions));
    }

    let counts = MaterializedCounts {
        events: if options.targets.events {
            rows.len()
        } else {
            0
        },
        event_text_index: event_text_index.len(),
        event_kv_index: event_kv_index.len(),
        field_index: field_index.len(),
        event_measures: event_measures.len(),
        measure_cube_points: measure_cube_points.len(),
        counter_rollups: counter_rollups.len(),
        gauge_rollups: gauge_rollups.len(),
        histogram_rollups: histogram_rollups.len(),
        entity_state_updates: entity_state_updates.len(),
        entity_state_current: entity_state_current.len(),
        report_results: report_results.len(),
        sequence_report_results: sequence_report_results.len(),
        cohort_memberships: cohort_memberships.len(),
        output_versions: materialized_output_versions(
            &report_results,
            &sequence_report_results,
            &cohort_memberships,
        ),
    };
    let token_prefix = format!(
        "{}:{}:{}:{}",
        options.token_namespace, commit.namespace, commit.snapshot_id, options.file_index
    );

    if options.targets.events && !rows.is_empty() {
        let body = rows_to_ndjson(rows)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_events_table(),
            &body,
            &format!("{token_prefix}:events"),
        )
        .await
        .context("insert materialized event rows")?;
    }
    if !event_text_index.is_empty() {
        let body = rows_to_ndjson(&event_text_index)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_event_text_index_table(),
            &body,
            &format!("{token_prefix}:event_text_index"),
        )
        .await
        .context("insert materialized event text index rows")?;
    }
    if !event_kv_index.is_empty() {
        let body = rows_to_ndjson(&event_kv_index)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_event_kv_index_table(),
            &body,
            &format!("{token_prefix}:event_kv_index"),
        )
        .await
        .context("insert materialized event KV index rows")?;
    }
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
    if !measure_cube_points.is_empty() {
        let body = rows_to_ndjson(&measure_cube_points)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_measure_cube_points_table(),
            &body,
            &format!("{token_prefix}:measure_cube_points"),
        )
        .await
        .context("insert materialized measure cube point rows")?;
    }
    if !counter_rollups.is_empty() {
        let body = rows_to_ndjson(&counter_rollups)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_counter_rollups_table(),
            &body,
            &format!("{token_prefix}:counter_rollups"),
        )
        .await
        .context("insert materialized counter rollup rows")?;
    }
    if !gauge_rollups.is_empty() {
        let body = rows_to_ndjson(&gauge_rollups)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_gauge_rollups_table(),
            &body,
            &format!("{token_prefix}:gauge_rollups"),
        )
        .await
        .context("insert materialized gauge rollup rows")?;
    }
    if !histogram_rollups.is_empty() {
        let body = rows_to_ndjson(&histogram_rollups)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_histogram_rollups_table(),
            &body,
            &format!("{token_prefix}:histogram_rollups"),
        )
        .await
        .context("insert materialized histogram rollup rows")?;
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
    if !entity_state_current.is_empty() {
        let body = rows_to_ndjson(&entity_state_current)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_entity_state_current_table(),
            &body,
            &format!("{token_prefix}:entity_state_current"),
        )
        .await
        .context("insert materialized current entity state rows")?;
    }
    if !report_results.is_empty() {
        let body = rows_to_ndjson(&report_results)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_report_results_table(),
            &body,
            &format!("{token_prefix}:report_results"),
        )
        .await
        .context("insert materialized report result rows")?;
    }
    if !sequence_report_results.is_empty() {
        let body = rows_to_ndjson(&sequence_report_results)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_sequence_report_results_table(),
            &body,
            &format!("{token_prefix}:sequence_report_results"),
        )
        .await
        .context("insert materialized sequence report result rows")?;
    }
    if !cohort_memberships.is_empty() {
        let body = rows_to_ndjson(&cohort_memberships)?;
        insert_clickhouse(
            client,
            cfg,
            &cfg.qualified_cohort_memberships_table(),
            &body,
            &format!("{token_prefix}:cohort_memberships"),
        )
        .await
        .context("insert materialized cohort membership rows")?;
    }

    Ok(counts)
}

fn materialized_output_versions(
    report_results: &[ReportResultRow],
    sequence_report_results: &[SequenceReportResultRow],
    cohort_memberships: &[CohortMembershipRow],
) -> Vec<MaterializedOutputVersion> {
    let mut versions =
        HashMap::<(String, &'static str, String, u64), MaterializedOutputVersion>::new();
    for row in report_results {
        push_materialized_output_version(
            &mut versions,
            &row.tenant_id,
            "report",
            &row.report_id,
            row.report_version,
            &row.bucket_time,
            &row.bucket_time,
        );
    }
    for row in sequence_report_results {
        push_materialized_output_version(
            &mut versions,
            &row.tenant_id,
            "sequence",
            &row.report_id,
            row.report_version,
            &row.bucket_time,
            &row.bucket_time,
        );
    }
    for row in cohort_memberships {
        push_materialized_output_version(
            &mut versions,
            &row.tenant_id,
            "cohort",
            &row.cohort_id,
            row.cohort_version,
            &row.first_seen,
            &row.last_seen,
        );
    }
    let mut values = versions.into_values().collect::<Vec<_>>();
    values.sort_by(|left, right| {
        left.tenant_id
            .cmp(&right.tenant_id)
            .then(left.target_type.cmp(right.target_type))
            .then(left.target_id.cmp(&right.target_id))
            .then(left.target_version.cmp(&right.target_version))
    });
    values
}

fn push_materialized_output_version(
    versions: &mut HashMap<(String, &'static str, String, u64), MaterializedOutputVersion>,
    tenant_id: &str,
    target_type: &'static str,
    target_id: &str,
    target_version: u64,
    source_start: &str,
    source_end: &str,
) {
    if tenant_id.is_empty()
        || target_id.is_empty()
        || source_start.is_empty()
        || source_end.is_empty()
    {
        return;
    }
    let key = (
        tenant_id.to_string(),
        target_type,
        target_id.to_string(),
        target_version,
    );
    versions
        .entry(key)
        .and_modify(|version| {
            version.row_count += 1;
            if source_start < version.source_start.as_str() {
                version.source_start = source_start.to_string();
            }
            if source_end > version.source_end.as_str() {
                version.source_end = source_end.to_string();
            }
        })
        .or_insert_with(|| MaterializedOutputVersion {
            tenant_id: tenant_id.to_string(),
            target_type,
            target_id: target_id.to_string(),
            target_version,
            row_count: 1,
            source_start: source_start.to_string(),
            source_end: source_end.to_string(),
        });
}

fn combined_materialized_output_versions(
    versions: &[MaterializedOutputVersion],
) -> Vec<MaterializedOutputVersion> {
    let mut combined =
        HashMap::<(String, &'static str, String, u64), MaterializedOutputVersion>::new();
    for version in versions {
        let key = (
            version.tenant_id.clone(),
            version.target_type,
            version.target_id.clone(),
            version.target_version,
        );
        combined
            .entry(key)
            .and_modify(|current| {
                current.row_count += version.row_count;
                if version.source_start < current.source_start {
                    current.source_start = version.source_start.clone();
                }
                if version.source_end > current.source_end {
                    current.source_end = version.source_end.clone();
                }
            })
            .or_insert_with(|| version.clone());
    }
    let mut values = combined.into_values().collect::<Vec<_>>();
    values.sort_by(|left, right| {
        left.tenant_id
            .cmp(&right.tenant_id)
            .then(left.target_type.cmp(right.target_type))
            .then(left.target_id.cmp(&right.target_id))
            .then(left.target_version.cmp(&right.target_version))
    });
    values
}

fn materialization_target_table(target_type: &str) -> &'static str {
    match target_type {
        "cohort" => "cohort_memberships",
        "sequence" => "sequence_report_results",
        _ => "report_results",
    }
}

fn materialization_job_id(commit: &LakehouseCommit, version: &MaterializedOutputVersion) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}",
        commit.namespace,
        commit.snapshot_id,
        version.tenant_id,
        version.target_type,
        version.target_id,
        version.target_version
    )
}

fn materialization_chunk_id(
    commit: &LakehouseCommit,
    version: &MaterializedOutputVersion,
) -> String {
    format!("{}:chunk:0", materialization_job_id(commit, version))
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
            "event_text_index_rows": materialized.event_text_index,
            "event_kv_index_rows": materialized.event_kv_index,
            "field_index_rows": materialized.field_index,
            "event_measure_rows": materialized.event_measures,
            "measure_cube_point_rows": materialized.measure_cube_points,
            "counter_rollup_rows": materialized.counter_rollups,
            "gauge_rollup_rows": materialized.gauge_rollups,
            "histogram_rollup_rows": materialized.histogram_rollups,
            "entity_state_update_rows": materialized.entity_state_updates,
            "entity_state_current_rows": materialized.entity_state_current,
            "report_result_rows": materialized.report_results,
            "sequence_report_result_rows": materialized.sequence_report_results,
            "cohort_membership_rows": materialized.cohort_memberships,
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
    insert_materialization_publication_metadata(client, cfg, commit, materialized, &token_prefix)
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
    insert_materialization_publication_metadata(client, cfg, commit, materialized, &token_prefix)
        .await?;
    Ok(())
}

async fn insert_materialization_publication_metadata(
    client: &Client,
    cfg: &Config,
    commit: &LakehouseCommit,
    materialized: &MaterializedCounts,
    token_prefix: &str,
) -> Result<()> {
    let versions = combined_materialized_output_versions(&materialized.output_versions);
    if versions.is_empty() {
        return Ok(());
    }

    let completed_at = format_timestamp_ms(commit.committed_at_ms)?;
    let job_rows = versions
        .iter()
        .map(|version| {
            let target_table = materialization_target_table(version.target_type);
            MaterializationJobPublishRow {
                tenant_id: &version.tenant_id,
                job_id: materialization_job_id(commit, version),
                job_kind: "lakehouse_commit",
                status: "completed",
                priority: 50,
                target_type: version.target_type,
                target_table,
                target_id: &version.target_id,
                target_version: version.target_version,
                source_table: &commit.table,
                source_start: &version.source_start,
                source_end: &version.source_end,
                chunk_seconds: 0,
                total_chunks: 1,
                completed_chunks: 1,
                failed_chunks: 0,
                rows_scanned: commit.record_count.try_into().unwrap_or(u64::MAX),
                rows_written: version.row_count,
                bytes_scanned: 0,
                bytes_written: 0,
                lease_owner: "lakehouse-rebuild",
                leased_until: None,
                attempt: 1,
                max_attempts: 1,
                error: "",
                config: serde_json::json!({
                    "source_namespace": &commit.namespace,
                    "source_snapshot_id": &commit.snapshot_id,
                    "source_sequence_number": commit.sequence_number,
                    "metadata_location": &commit.metadata_location,
                    "source_batch_id": &commit.source_batch_id,
                }),
                created_at: completed_at.as_str(),
                updated_at: completed_at.as_str(),
                completed_at: Some(completed_at.as_str()),
            }
        })
        .collect::<Vec<_>>();
    let chunk_rows = versions
        .iter()
        .map(|version| {
            let target_table = materialization_target_table(version.target_type);
            MaterializationChunkPublishRow {
                tenant_id: &version.tenant_id,
                job_id: materialization_job_id(commit, version),
                chunk_id: materialization_chunk_id(commit, version),
                chunk_index: 0,
                status: "completed",
                target_type: version.target_type,
                target_table,
                target_id: &version.target_id,
                target_version: version.target_version,
                source_table: &commit.table,
                source_start: &version.source_start,
                source_end: &version.source_end,
                rows_scanned: commit.record_count.try_into().unwrap_or(u64::MAX),
                rows_written: version.row_count,
                bytes_scanned: 0,
                bytes_written: 0,
                lease_owner: "lakehouse-rebuild",
                leased_until: None,
                attempt: 1,
                max_attempts: 1,
                error: "",
                started_at: Some(completed_at.as_str()),
                updated_at: completed_at.as_str(),
                completed_at: Some(completed_at.as_str()),
                attributes: serde_json::json!({
                    "source_namespace": &commit.namespace,
                    "source_snapshot_id": &commit.snapshot_id,
                    "source_sequence_number": commit.sequence_number,
                    "metadata_location": &commit.metadata_location,
                    "source_batch_id": &commit.source_batch_id,
                    "row_count": version.row_count,
                }),
            }
        })
        .collect::<Vec<_>>();
    let version_rows = versions
        .iter()
        .map(|version| MaterializationVersionPublishRow {
            tenant_id: &version.tenant_id,
            target_type: version.target_type,
            target_id: &version.target_id,
            target_version: version.target_version,
            status: "completed",
            active: 1,
            source_start: &version.source_start,
            source_end: &version.source_end,
            row_count: version.row_count,
            chunk_count: 1,
            config_hash: 0,
            config: Value::Object(Map::new()),
            stats: serde_json::json!({
                "source_namespace": &commit.namespace,
                "source_table": &commit.table,
                "source_snapshot_id": &commit.snapshot_id,
                "source_sequence_number": commit.sequence_number,
                "metadata_location": &commit.metadata_location,
                "source_batch_id": &commit.source_batch_id,
            }),
            completed_at: completed_at.as_str(),
        })
        .collect::<Vec<_>>();
    let watermark_rows = versions
        .iter()
        .map(|version| MaterializationWatermarkPublishRow {
            tenant_id: &version.tenant_id,
            target_type: version.target_type,
            target_id: &version.target_id,
            target_version: version.target_version,
            source_table: &commit.table,
            low_watermark: &version.source_start,
            high_watermark: &version.source_end,
            status: "materialized",
            lag_ms: 0,
            attributes: serde_json::json!({
                "source_namespace": &commit.namespace,
                "source_snapshot_id": &commit.snapshot_id,
                "source_sequence_number": commit.sequence_number,
                "row_count": version.row_count,
                "metadata_location": &commit.metadata_location,
                "source_batch_id": &commit.source_batch_id,
            }),
        })
        .collect::<Vec<_>>();

    let jobs_body = rows_to_ndjson(&job_rows)?;
    insert_clickhouse(
        client,
        cfg,
        &cfg.qualified_materialization_jobs_table(),
        &jobs_body,
        &format!("{token_prefix}:materialization_jobs"),
    )
    .await?;

    let chunks_body = rows_to_ndjson(&chunk_rows)?;
    insert_clickhouse(
        client,
        cfg,
        &cfg.qualified_materialization_chunks_table(),
        &chunks_body,
        &format!("{token_prefix}:materialization_chunks"),
    )
    .await?;

    let versions_body = rows_to_ndjson(&version_rows)?;
    insert_clickhouse(
        client,
        cfg,
        &cfg.qualified_materialization_versions_table(),
        &versions_body,
        &format!("{token_prefix}:materialization_versions"),
    )
    .await?;

    let watermarks_body = rows_to_ndjson(&watermark_rows)?;
    insert_clickhouse(
        client,
        cfg,
        &cfg.qualified_materialization_watermarks_table(),
        &watermarks_body,
        &format!("{token_prefix}:materialization_watermarks"),
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
            cfg.clickhouse_events_table.as_str(),
            "event_rows",
            materialized.events,
            targets.events,
        ),
        (
            cfg.clickhouse_event_text_index_table.as_str(),
            "event_text_index_rows",
            materialized.event_text_index,
            targets.event_text_index,
        ),
        (
            cfg.clickhouse_event_kv_index_table.as_str(),
            "event_kv_index_rows",
            materialized.event_kv_index,
            targets.event_kv_index,
        ),
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
            cfg.clickhouse_measure_cube_rollups_table.as_str(),
            "measure_cube_point_rows",
            materialized.measure_cube_points,
            targets.measure_cube_points,
        ),
        (
            cfg.clickhouse_counter_rollups_table.as_str(),
            "counter_rollup_rows",
            materialized.counter_rollups,
            targets.counter_rollups,
        ),
        (
            cfg.clickhouse_gauge_rollups_table.as_str(),
            "gauge_rollup_rows",
            materialized.gauge_rollups,
            targets.gauge_rollups,
        ),
        (
            cfg.clickhouse_histogram_rollups_table.as_str(),
            "histogram_rollup_rows",
            materialized.histogram_rollups,
            targets.histogram_rollups,
        ),
        (
            cfg.clickhouse_entity_state_updates_table.as_str(),
            "entity_state_update_rows",
            materialized.entity_state_updates,
            targets.entity_state_updates,
        ),
        (
            cfg.clickhouse_entity_state_current_table.as_str(),
            "entity_state_current_rows",
            materialized.entity_state_current,
            targets.entity_state_current,
        ),
        (
            cfg.clickhouse_report_results_table.as_str(),
            "report_result_rows",
            materialized.report_results,
            targets.report_results,
        ),
        (
            cfg.clickhouse_sequence_report_results_table.as_str(),
            "sequence_report_result_rows",
            materialized.sequence_report_results,
            targets.sequence_report_results,
        ),
        (
            cfg.clickhouse_cohort_memberships_table.as_str(),
            "cohort_membership_rows",
            materialized.cohort_memberships,
            targets.cohort_memberships,
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
        "SELECT tenant_id, definition_id, name, kind, mode, config, version FROM {} FINAL WHERE enabled = 1 AND isNull(deleted_at) AND kind IN ('field', 'measure', 'rollup', 'metric_rollup', 'state', 'report', 'sequence', 'cohort') FORMAT JSON",
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
        let mut measure_cubes = Vec::new();
        let mut metric_rollups = Vec::new();
        let mut states = Vec::new();
        let mut reports = Vec::new();
        let mut trace_reports = Vec::new();
        let mut retentions = Vec::new();
        let mut sequences = Vec::new();
        let mut cohorts = Vec::new();
        for record in records {
            match record.kind.as_str() {
                "field" => {
                    fields.extend(field_rules_from_record(&record));
                }
                "measure" | "rollup" => {
                    measures.extend(measure_rules_from_record(&record));
                    measure_cubes.extend(measure_cube_rules_from_record(&record));
                }
                "metric_rollup" => {
                    metric_rollups.extend(metric_rollup_rules_from_record(&record));
                }
                "state" => {
                    states.extend(state_rules_from_record(&record));
                }
                "report" => {
                    if record.mode == "trace_summary" {
                        trace_reports.extend(trace_report_rules_from_record(&record));
                    } else if record.mode == "retention" {
                        retentions.extend(retention_rules_from_record(&record));
                    } else {
                        reports.extend(report_rules_from_record(&record));
                    }
                }
                "sequence" => {
                    sequences.extend(sequence_rules_from_record(&record));
                }
                "cohort" => {
                    cohorts.extend(cohort_rules_from_record(&record));
                }
                _ => {}
            }
        }
        Self {
            fields,
            measures,
            measure_cubes,
            metric_rollups,
            states,
            reports,
            trace_reports,
            retentions,
            sequences,
            cohorts,
        }
    }
}

fn field_rules_from_record(record: &DefinitionRecord) -> Vec<FieldRule> {
    let matcher = matcher_from_config(&record.config);
    let mut rules = generalized_outputs(&record.config, "field_index")
        .filter_map(|output| {
            let field_name = string_expr_from_value(output.get("field_name"))
                .unwrap_or_else(|| StringExpr::Literal(record.name.clone()));
            let value = value_expr_from_value(output.get("value"))?;
            Some(FieldRule {
                tenant_id: record.tenant_id.clone(),
                definition_id: record.definition_id.clone(),
                definition_version: record.version,
                matcher: matcher.clone(),
                output: FieldOutput {
                    field_name,
                    mode: normalize_field_mode(
                        output
                            .get("mode")
                            .and_then(Value::as_str)
                            .unwrap_or(record.mode.as_str()),
                    ),
                    value,
                    value_type: output
                        .get("value_type")
                        .and_then(Value::as_str)
                        .unwrap_or("string")
                        .to_string(),
                },
            })
        })
        .collect::<Vec<_>>();
    if !rules.is_empty() {
        return rules;
    }

    let path = config_string(&record.config, "path");
    if path.is_empty() {
        return Vec::new();
    }
    rules.push(FieldRule {
        tenant_id: record.tenant_id.clone(),
        definition_id: record.definition_id.clone(),
        definition_version: record.version,
        matcher,
        output: FieldOutput {
            field_name: StringExpr::Literal(record.name.clone()),
            mode: normalize_field_mode(&record.mode),
            value: ValueExpr::Path { path },
            value_type: config_string_default(&record.config, "value_type", "string"),
        },
    });
    rules
}

fn measure_rules_from_record(record: &DefinitionRecord) -> Vec<MeasureRule> {
    let matcher = matcher_from_config(&record.config);
    let mut rules = generalized_outputs(&record.config, "event_measures")
        .filter_map(|output| {
            let measure_name = string_expr_from_value(output.get("measure_name"))
                .unwrap_or_else(|| StringExpr::Literal(record.name.clone()));
            let value = number_expr_from_value(output.get("value"))?;
            Some(MeasureRule {
                tenant_id: record.tenant_id.clone(),
                definition_id: record.definition_id.clone(),
                definition_version: record.version,
                matcher: matcher.clone(),
                output: MeasureOutput {
                    measure_name,
                    value,
                    unit: string_expr_from_value(output.get("unit"))
                        .unwrap_or_else(|| StringExpr::Literal(String::new())),
                    dimensions: dimensions_from_output(output),
                    bucket_seconds: output
                        .get("bucket_seconds")
                        .and_then(Value::as_u64)
                        .and_then(|value| value.try_into().ok())
                        .unwrap_or(300),
                },
            })
        })
        .collect::<Vec<_>>();
    if !rules.is_empty() {
        return rules;
    }

    let path = config_string(&record.config, "path");
    if path.is_empty() {
        return Vec::new();
    }
    let dimension = config_string_default(&record.config, "dimension", "");
    rules.push(MeasureRule {
        tenant_id: record.tenant_id.clone(),
        definition_id: record.definition_id.clone(),
        definition_version: record.version,
        matcher,
        output: MeasureOutput {
            measure_name: StringExpr::Literal(record.name.clone()),
            value: NumberExpr::Path { path },
            unit: StringExpr::Literal(config_string_default(&record.config, "unit", "")),
            dimensions: if dimension.is_empty() {
                Vec::new()
            } else {
                vec![DimensionOutput {
                    name: dimension.clone(),
                    value: StringExpr::Path {
                        path: dimension,
                        default: String::new(),
                    },
                }]
            },
            bucket_seconds: 300,
        },
    });
    rules
}

fn measure_cube_rules_from_record(record: &DefinitionRecord) -> Vec<MeasureCubeRule> {
    if record.kind != "measure" || record.mode != "cube" {
        return Vec::new();
    }
    let matcher = matcher_from_config(&record.config);
    generalized_outputs(&record.config, "measure_cube_rollups")
        .filter(|output| {
            output
                .get("target")
                .and_then(Value::as_str)
                .is_some_and(|target| target == "measure_cube_rollups")
        })
        .filter_map(|output| {
            let dimension_sets = dimension_sets_from_output(output);
            if dimension_sets.is_empty() {
                return None;
            }
            let measure_name = string_expr_from_value(output.get("measure_name"))
                .unwrap_or_else(|| StringExpr::Literal(record.name.clone()));
            let value = number_expr_from_value(output.get("value"))?;
            Some(MeasureCubeRule {
                tenant_id: record.tenant_id.clone(),
                definition_id: record.definition_id.clone(),
                definition_version: record.version,
                matcher: matcher.clone(),
                output: MeasureCubeOutput {
                    measure_name,
                    value,
                    unit: string_expr_from_value(output.get("unit"))
                        .unwrap_or_else(|| StringExpr::Literal(String::new())),
                    dimension_sets,
                    bucket_seconds: output
                        .get("bucket_seconds")
                        .and_then(Value::as_u64)
                        .and_then(|value| value.try_into().ok())
                        .unwrap_or(300),
                },
            })
        })
        .collect()
}

fn metric_rollup_rules_from_record(record: &DefinitionRecord) -> Vec<MetricRollupRule> {
    let matcher = matcher_from_config(&record.config);
    generalized_outputs(&record.config, "metric_rollups")
        .filter_map(|output| {
            let metric_name = string_expr_from_value(output.get("metric_name"))
                .or_else(|| string_expr_from_value(output.get("measure_name")))
                .unwrap_or_else(|| StringExpr::Literal(record.name.clone()));
            let metric_kind =
                string_expr_from_value(output.get("metric_kind")).unwrap_or_else(|| {
                    StringExpr::Path {
                        path: "metric_type".to_string(),
                        default: "counter".to_string(),
                    }
                });
            let value = number_expr_from_value(output.get("value"))?;
            Some(MetricRollupRule {
                tenant_id: record.tenant_id.clone(),
                definition_id: record.definition_id.clone(),
                definition_version: record.version,
                matcher: matcher.clone(),
                output: MetricRollupOutput {
                    metric_name,
                    metric_kind,
                    value,
                    unit: string_expr_from_value(output.get("unit"))
                        .unwrap_or_else(|| StringExpr::Literal(String::new())),
                    dimensions: dimensions_from_output(output),
                    bucket_seconds: output
                        .get("bucket_seconds")
                        .and_then(Value::as_u64)
                        .and_then(|value| value.try_into().ok())
                        .unwrap_or(60),
                },
            })
        })
        .collect()
}

fn state_rules_from_record(record: &DefinitionRecord) -> Vec<StateRule> {
    let matcher = matcher_from_config(&record.config);
    let mut rules = generalized_outputs(&record.config, "entity_state_updates")
        .filter_map(|output| {
            Some(StateRule {
                tenant_id: record.tenant_id.clone(),
                definition_id: record.definition_id.clone(),
                definition_version: record.version,
                matcher: matcher.clone(),
                output: StateOutput {
                    entity_type: string_expr_from_value(output.get("entity_type"))?,
                    entity_id: string_expr_from_value(output.get("entity_id"))?,
                    state_name: string_expr_from_value(output.get("state_name"))
                        .unwrap_or_else(|| StringExpr::Literal(record.name.clone())),
                    value: string_expr_from_value(output.get("value"))?,
                    value_type: output
                        .get("value_type")
                        .and_then(Value::as_str)
                        .unwrap_or("string")
                        .to_string(),
                },
            })
        })
        .collect::<Vec<_>>();
    if !rules.is_empty() {
        return rules;
    }

    let path = config_string(&record.config, "path");
    let entity_type = config_string(&record.config, "entity_type");
    let entity_id_path = config_string(&record.config, "entity_id_path");
    if path.is_empty() || entity_type.is_empty() || entity_id_path.is_empty() {
        return Vec::new();
    }
    rules.push(StateRule {
        tenant_id: record.tenant_id.clone(),
        definition_id: record.definition_id.clone(),
        definition_version: record.version,
        matcher,
        output: StateOutput {
            entity_type: StringExpr::Literal(entity_type),
            entity_id: StringExpr::Path {
                path: entity_id_path,
                default: String::new(),
            },
            state_name: StringExpr::Literal(record.name.clone()),
            value: StringExpr::Path {
                path,
                default: String::new(),
            },
            value_type: config_string_default(&record.config, "value_type", "string"),
        },
    });
    rules
}

fn report_rules_from_record(record: &DefinitionRecord) -> Vec<ReportRule> {
    let matcher = matcher_from_config(&record.config);
    generalized_outputs(&record.config, "report_results")
        .map(|output| ReportRule {
            tenant_id: record.tenant_id.clone(),
            definition_version: record.version,
            matcher: matcher.clone(),
            output: ReportOutput {
                report_id: string_expr_from_value(output.get("report_id"))
                    .unwrap_or_else(|| StringExpr::Literal(record.name.clone())),
                dimensions: dimensions_from_output(output),
                metrics: report_metrics_from_output(output),
                bucket_seconds: output
                    .get("bucket_seconds")
                    .and_then(Value::as_u64)
                    .and_then(|value| value.try_into().ok())
                    .unwrap_or(60),
            },
        })
        .collect()
}

fn trace_report_rules_from_record(record: &DefinitionRecord) -> Vec<TraceReportRule> {
    let matcher = matcher_from_config(&record.config);
    generalized_outputs(&record.config, "report_results")
        .map(|output| TraceReportRule {
            tenant_id: record.tenant_id.clone(),
            definition_version: record.version,
            matcher: matcher.clone(),
            output: TraceReportOutput {
                report_id: string_expr_from_value(output.get("report_id"))
                    .unwrap_or_else(|| StringExpr::Literal(record.name.clone())),
                dimensions: dimensions_from_output(output),
                bucket_seconds: output
                    .get("bucket_seconds")
                    .and_then(Value::as_u64)
                    .and_then(|value| value.try_into().ok())
                    .unwrap_or(60),
            },
        })
        .collect()
}

fn retention_rules_from_record(record: &DefinitionRecord) -> Vec<RetentionRule> {
    let matcher = matcher_from_config(&record.config);
    generalized_outputs(&record.config, "report_results")
        .filter_map(|output| {
            Some(RetentionRule {
                tenant_id: record.tenant_id.clone(),
                definition_version: record.version,
                matcher: matcher.clone(),
                output: RetentionOutput {
                    report_id: string_expr_from_value(output.get("report_id"))
                        .unwrap_or_else(|| StringExpr::Literal(record.name.clone())),
                    cohort_id: string_expr_from_value(output.get("cohort_id"))?,
                    entity_type: string_expr_from_value(output.get("entity_type"))?,
                    entity_id: string_expr_from_value(output.get("entity_id"))?,
                    dimensions: dimensions_from_output(output),
                    retention_bucket_seconds: output
                        .get("retention_bucket_seconds")
                        .or_else(|| output.get("bucket_seconds"))
                        .and_then(Value::as_u64)
                        .and_then(|value| value.try_into().ok())
                        .unwrap_or(86_400),
                },
            })
        })
        .collect()
}

fn cohort_rules_from_record(record: &DefinitionRecord) -> Vec<CohortRule> {
    let matcher = matcher_from_config(&record.config);
    generalized_outputs(&record.config, "cohort_memberships")
        .filter_map(|output| {
            Some(CohortRule {
                tenant_id: record.tenant_id.clone(),
                definition_version: record.version,
                matcher: matcher.clone(),
                output: CohortOutput {
                    cohort_id: string_expr_from_value(output.get("cohort_id"))
                        .unwrap_or_else(|| StringExpr::Literal(record.name.clone())),
                    entity_type: string_expr_from_value(output.get("entity_type"))?,
                    entity_id: string_expr_from_value(output.get("entity_id"))?,
                },
            })
        })
        .collect()
}

fn sequence_rules_from_record(record: &DefinitionRecord) -> Vec<SequenceRule> {
    let matcher = matcher_from_config(&record.config);
    generalized_outputs(&record.config, "sequence_report_results")
        .filter_map(|output| {
            let steps = sequence_steps_from_output(output);
            if steps.is_empty() {
                return None;
            }
            Some(SequenceRule {
                tenant_id: record.tenant_id.clone(),
                definition_version: record.version,
                matcher: matcher.clone(),
                output: SequenceOutput {
                    report_id: string_expr_from_value(output.get("report_id"))
                        .unwrap_or_else(|| StringExpr::Literal(record.name.clone())),
                    entity_id: string_expr_from_value(output.get("entity_id"))?,
                    segment: dimensions_from_output(output),
                    steps,
                    bucket_seconds: output
                        .get("bucket_seconds")
                        .and_then(Value::as_u64)
                        .and_then(|value| value.try_into().ok())
                        .unwrap_or(60),
                },
            })
        })
        .collect()
}

fn generalized_outputs<'a>(
    config: &'a Value,
    target: &'static str,
) -> impl Iterator<Item = &'a Map<String, Value>> {
    config
        .get("outputs")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|outputs| outputs.iter())
        .filter_map(Value::as_object)
        .filter(move |output| {
            output
                .get("target")
                .and_then(Value::as_str)
                .is_none_or(|value| value == target)
        })
}

fn matcher_from_config(config: &Value) -> Matcher {
    matcher_from_match_value(config.get("match"))
}

fn matcher_from_match_value(value: Option<&Value>) -> Matcher {
    let predicates = value
        .and_then(Value::as_object)
        .and_then(|matcher| matcher.get("all"))
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|predicates| predicates.iter())
        .filter_map(predicate_from_value)
        .collect();
    Matcher { predicates }
}

fn predicate_from_value(value: &Value) -> Option<Predicate> {
    let object = value.as_object()?;
    let path = object.get("path")?.as_str()?.trim().to_string();
    if path.is_empty() {
        return None;
    }
    let op = match object.get("op").and_then(Value::as_str).unwrap_or("eq") {
        "exists" => PredicateOp::Exists,
        "eq" => PredicateOp::Eq,
        "neq" => PredicateOp::Neq,
        "is_number" => PredicateOp::IsNumber,
        "in" => PredicateOp::In,
        _ => return None,
    };
    Some(Predicate {
        path,
        op,
        value: object.get("value").cloned(),
    })
}

fn string_expr_from_value(value: Option<&Value>) -> Option<StringExpr> {
    match value {
        Some(Value::String(value)) => Some(StringExpr::Literal(value.clone())),
        Some(Value::Number(value)) => Some(StringExpr::Literal(value.to_string())),
        Some(Value::Bool(value)) => Some(StringExpr::Literal(if *value {
            "true".to_string()
        } else {
            "false".to_string()
        })),
        Some(Value::Object(object)) => {
            let path = object.get("path")?.as_str()?.trim().to_string();
            if path.is_empty() {
                return None;
            }
            let default = object
                .get("default")
                .map(scalar_value_to_string)
                .unwrap_or_default();
            Some(StringExpr::Path { path, default })
        }
        _ => None,
    }
}

fn value_expr_from_value(value: Option<&Value>) -> Option<ValueExpr> {
    match value {
        Some(Value::Object(object)) => {
            let path = object.get("path")?.as_str()?.trim().to_string();
            if path.is_empty() {
                return None;
            }
            Some(ValueExpr::Path { path })
        }
        Some(value) => Some(ValueExpr::Literal(value.clone())),
        None => None,
    }
}

fn number_expr_from_value(value: Option<&Value>) -> Option<NumberExpr> {
    match value {
        Some(Value::Object(object)) => {
            let path = object.get("path")?.as_str()?.trim().to_string();
            if path.is_empty() {
                return None;
            }
            Some(NumberExpr::Path { path })
        }
        Some(Value::Number(value)) => value.as_f64().map(NumberExpr::Literal),
        Some(Value::String(value)) => value.parse().ok().map(NumberExpr::Literal),
        _ => None,
    }
}

fn dimensions_from_output(output: &Map<String, Value>) -> Vec<DimensionOutput> {
    output
        .get("dimensions")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|dimensions| dimensions.iter())
        .filter_map(|dimension| {
            let object = dimension.as_object()?;
            let name = object.get("name")?.as_str()?.trim().to_string();
            if name.is_empty() {
                return None;
            }
            let value = string_expr_from_value(object.get("value"))?;
            Some(DimensionOutput { name, value })
        })
        .collect()
}

fn dimension_sets_from_output(output: &Map<String, Value>) -> Vec<DimensionSetOutput> {
    output
        .get("dimension_sets")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|dimension_sets| dimension_sets.iter())
        .filter_map(|dimension_set| {
            let object = dimension_set.as_object()?;
            let id = object.get("id")?.as_str()?.trim().to_string();
            if id.is_empty() {
                return None;
            }
            let dimensions = object
                .get("dimensions")
                .and_then(Value::as_array)
                .into_iter()
                .flat_map(|dimensions| dimensions.iter())
                .filter_map(|dimension| {
                    let object = dimension.as_object()?;
                    let name = object.get("name")?.as_str()?.trim().to_string();
                    if name.is_empty() {
                        return None;
                    }
                    let value = string_expr_from_value(object.get("value"))?;
                    Some(DimensionOutput { name, value })
                })
                .collect::<Vec<_>>();
            if dimensions.is_empty() {
                return None;
            }
            Some(DimensionSetOutput { id, dimensions })
        })
        .collect()
}

fn report_metrics_from_output(output: &Map<String, Value>) -> Vec<ReportMetricOutput> {
    let metrics = output
        .get("metrics")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|metrics| metrics.iter())
        .filter_map(|metric| {
            let object = metric.as_object()?;
            let name = object.get("name")?.as_str()?.trim().to_string();
            if name.is_empty() {
                return None;
            }
            let op = match object.get("op").and_then(Value::as_str).unwrap_or("count") {
                "count" => ReportMetricOp::Count,
                "error_count" | "count_errors" => ReportMetricOp::ErrorCount,
                "sum" => ReportMetricOp::Sum,
                _ => return None,
            };
            Some(ReportMetricOutput {
                name,
                op,
                value: number_expr_from_value(object.get("value")),
            })
        })
        .collect::<Vec<_>>();
    if metrics.is_empty() {
        vec![
            ReportMetricOutput {
                name: "events".to_string(),
                op: ReportMetricOp::Count,
                value: None,
            },
            ReportMetricOutput {
                name: "errors".to_string(),
                op: ReportMetricOp::ErrorCount,
                value: None,
            },
        ]
    } else {
        metrics
    }
}

fn sequence_steps_from_output(output: &Map<String, Value>) -> Vec<SequenceStep> {
    output
        .get("steps")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|steps| steps.iter())
        .filter_map(|step| {
            let object = step.as_object()?;
            let name = object.get("name")?.as_str()?.trim().to_string();
            if name.is_empty() {
                return None;
            }
            Some(SequenceStep {
                name,
                matcher: matcher_from_match_value(object.get("match")),
            })
        })
        .collect()
}

fn normalize_field_mode(mode: &str) -> String {
    if mode == "lookup" {
        "lookup".to_string()
    } else {
        "facet".to_string()
    }
}

#[cfg(test)]
fn sdk_metric_managed_definition_record(tenant_id: &str) -> DefinitionRecord {
    DefinitionRecord {
        tenant_id: tenant_id.to_string(),
        definition_id: "sdk_metric_default_v1".to_string(),
        name: "sdk.metrics".to_string(),
        kind: "metric_rollup".to_string(),
        mode: "managed".to_string(),
        version: 1,
        config: serde_json::json!({
            "managed_by": "sdk",
            "sdk_surface": "metric",
            "match": {
                "all": [
                    { "path": "event_type", "op": "eq", "value": "metric" },
                    { "path": "metric_name", "op": "exists" },
                    { "path": "metric_value", "op": "is_number" }
                ]
            },
            "outputs": [
                {
                    "target": "metric_rollups",
                    "metric_name": { "path": "metric_name" },
                    "metric_kind": { "path": "metric_type", "default": "counter" },
                    "value": { "path": "metric_value" },
                    "unit": { "path": "metric_unit", "default": "" },
                    "dimensions": [
                        { "name": "service", "value": { "path": "service" } },
                        { "name": "environment", "value": { "path": "environment" } },
                        { "name": "signal", "value": { "path": "signal" } },
                        { "name": "metric_type", "value": { "path": "metric_type" } },
                        { "name": "llm.model", "value": { "path": "llm.model" } },
                        { "name": "llm.provider", "value": { "path": "llm.provider" } },
                        { "name": "loadtest_run_id", "value": { "path": "_loadtest.run_id" } }
                    ],
                    "bucket_seconds": 60
                }
            ]
        }),
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
    for rule in &definitions.fields {
        if rule.tenant_id != context.tenant_id || !rule.matcher.matches(data) {
            continue;
        }
        if let Some(value) = eval_value_expr(data, &rule.output.value) {
            let field_name = eval_string_expr(data, &rule.output.field_name);
            collect_field_index_rows(
                &context,
                row,
                BuiltFieldIndex {
                    mode: &rule.output.mode,
                    field_name: &field_name,
                    value_type: Some(&rule.output.value_type),
                    definition_id: &rule.definition_id,
                    definition_version: rule.definition_version,
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
        project_id: context.project_id.clone(),
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
    for rule in &definitions.measures {
        if rule.tenant_id != context.tenant_id || !rule.matcher.matches(data) {
            continue;
        }
        let Some(value) = eval_number_expr(data, &rule.output.value) else {
            continue;
        };
        let measure_name = eval_string_expr(data, &rule.output.measure_name);
        if measure_name.is_empty() {
            continue;
        }
        let unit = eval_string_expr(data, &rule.output.unit);
        let dimensions = non_empty_dimensions(data, &rule.output.dimensions);
        if dimensions.is_empty() {
            rows.push(EventMeasureRow {
                tenant_id: context.tenant_id.clone(),
                project_id: context.project_id.clone(),
                definition_id: rule.definition_id.clone(),
                definition_version: rule.definition_version,
                measure_name: measure_name.clone(),
                value,
                unit: unit.clone(),
                timestamp: context.timestamp.clone(),
                bucket_time: context.bucket_time.clone(),
                bucket_seconds: rule.output.bucket_seconds,
                event_id: context.event_id.clone(),
                event_type: context.event_type.clone(),
                signal: context.signal.clone(),
                dimension_name: String::new(),
                dimension_value: String::new(),
            });
            continue;
        }
        for (dimension_name, dimension_value) in dimensions {
            rows.push(EventMeasureRow {
                tenant_id: context.tenant_id.clone(),
                project_id: context.project_id.clone(),
                definition_id: rule.definition_id.clone(),
                definition_version: rule.definition_version,
                measure_name: measure_name.clone(),
                value,
                unit: unit.clone(),
                timestamp: context.timestamp.clone(),
                bucket_time: context.bucket_time.clone(),
                bucket_seconds: rule.output.bucket_seconds,
                event_id: context.event_id.clone(),
                event_type: context.event_type.clone(),
                signal: context.signal.clone(),
                dimension_name,
                dimension_value,
            });
        }
    }
    rows
}

fn measure_cube_point_rows(
    row: &EventInsertRow,
    definitions: &ExtractionDefinitions,
) -> Vec<MeasureCubePointRow> {
    let Some(data) = row.data.as_object() else {
        return Vec::new();
    };
    let context = EventContext::from_event(row, data);
    let mut rows = Vec::new();
    for rule in &definitions.measure_cubes {
        if rule.tenant_id != context.tenant_id || !rule.matcher.matches(data) {
            continue;
        }
        let Some(value) = eval_number_expr(data, &rule.output.value) else {
            continue;
        };
        let measure_name = eval_string_expr(data, &rule.output.measure_name);
        if measure_name.is_empty() {
            continue;
        }
        let unit = eval_string_expr(data, &rule.output.unit);
        for dimension_set in &rule.output.dimension_sets {
            let mut dimension_names = Vec::with_capacity(dimension_set.dimensions.len());
            let mut dimension_values = Vec::with_capacity(dimension_set.dimensions.len());
            let mut complete = true;
            for dimension in &dimension_set.dimensions {
                let value = eval_string_expr(data, &dimension.value);
                if value.is_empty() {
                    complete = false;
                    break;
                }
                dimension_names.push(dimension.name.clone());
                dimension_values.push(value);
            }
            if !complete || dimension_names.is_empty() {
                continue;
            }
            rows.push(MeasureCubePointRow {
                tenant_id: context.tenant_id.clone(),
                project_id: context.project_id.clone(),
                definition_id: rule.definition_id.clone(),
                definition_version: rule.definition_version,
                measure_name: measure_name.clone(),
                value,
                unit: unit.clone(),
                timestamp: context.timestamp.clone(),
                bucket_time: context.bucket_time.clone(),
                bucket_seconds: rule.output.bucket_seconds,
                event_id: context.event_id.clone(),
                event_type: context.event_type.clone(),
                signal: context.signal.clone(),
                dimension_set_id: dimension_set.id.clone(),
                dimension_names,
                dimension_values,
            });
        }
    }
    rows
}

#[derive(Default)]
struct MetricRollupRows {
    counters: Vec<CounterRollupRow>,
    gauges: Vec<GaugeRollupRow>,
    histograms: Vec<HistogramRollupRow>,
}

fn metric_rollup_rows(
    row: &EventInsertRow,
    definitions: &ExtractionDefinitions,
) -> MetricRollupRows {
    let Some(data) = row.data.as_object() else {
        return MetricRollupRows::default();
    };
    let context = EventContext::from_event(row, data);
    let mut rows = MetricRollupRows::default();
    for rule in &definitions.metric_rollups {
        if rule.tenant_id != context.tenant_id || !rule.matcher.matches(data) {
            continue;
        }
        let Some(value) = eval_number_expr(data, &rule.output.value) else {
            continue;
        };
        let metric_name = eval_string_expr(data, &rule.output.metric_name);
        if metric_name.is_empty() {
            continue;
        }
        let metric_kind = normalize_metric_kind(&eval_string_expr(data, &rule.output.metric_kind));
        let unit = eval_string_expr(data, &rule.output.unit);
        let dimensions = dimension_object(data, &rule.output.dimensions);
        match metric_kind.as_str() {
            "gauge" => rows.gauges.push(GaugeRollupRow {
                tenant_id: context.tenant_id.clone(),
                project_id: context.project_id.clone(),
                definition_id: rule.definition_id.clone(),
                definition_version: rule.definition_version,
                metric_name,
                unit,
                bucket_time: context.bucket_time.clone(),
                bucket_seconds: rule.output.bucket_seconds,
                dimensions,
                count: 1,
                sum: value,
                min: value,
                max: value,
                last: value,
            }),
            "histogram" | "timing" => rows.histograms.push(HistogramRollupRow {
                tenant_id: context.tenant_id.clone(),
                project_id: context.project_id.clone(),
                definition_id: rule.definition_id.clone(),
                definition_version: rule.definition_version,
                metric_name,
                unit,
                bucket_time: context.bucket_time.clone(),
                bucket_seconds: rule.output.bucket_seconds,
                dimensions,
                count: 1,
                sum: value,
                min: value,
                max: value,
            }),
            _ => rows.counters.push(CounterRollupRow {
                tenant_id: context.tenant_id.clone(),
                project_id: context.project_id.clone(),
                definition_id: rule.definition_id.clone(),
                definition_version: rule.definition_version,
                metric_name,
                unit,
                bucket_time: context.bucket_time.clone(),
                bucket_seconds: rule.output.bucket_seconds,
                dimensions,
                count: 1,
                sum: value,
            }),
        }
    }
    rows
}

fn normalize_metric_kind(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "gauge" => "gauge".to_string(),
        "histogram" => "histogram".to_string(),
        "timing" => "timing".to_string(),
        _ => "counter".to_string(),
    }
}

fn dimension_object(data: &Map<String, Value>, dimensions: &[DimensionOutput]) -> Value {
    let mut object = Map::new();
    for dimension in dimensions {
        let value = eval_string_expr(data, &dimension.value);
        if !value.is_empty() {
            object.insert(dimension.name.clone(), Value::String(value));
        }
    }
    Value::Object(object)
}

fn non_empty_dimensions(
    data: &Map<String, Value>,
    dimensions: &[DimensionOutput],
) -> Vec<(String, String)> {
    dimensions
        .iter()
        .filter_map(|dimension| {
            let value = eval_string_expr(data, &dimension.value);
            if value.is_empty() {
                None
            } else {
                Some((dimension.name.clone(), value))
            }
        })
        .collect()
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
    for rule in &definitions.states {
        if rule.tenant_id != context.tenant_id || !rule.matcher.matches(data) {
            continue;
        }
        let entity_type = eval_string_expr(data, &rule.output.entity_type);
        let entity_id = eval_string_expr(data, &rule.output.entity_id);
        let state_name = eval_string_expr(data, &rule.output.state_name);
        let value = eval_string_expr(data, &rule.output.value);
        if entity_type.is_empty()
            || entity_id.is_empty()
            || state_name.is_empty()
            || value.is_empty()
        {
            continue;
        }
        rows.push(EntityStateUpdateRow {
            tenant_id: context.tenant_id.clone(),
            project_id: context.project_id.clone(),
            definition_id: rule.definition_id.clone(),
            definition_version: rule.definition_version,
            entity_type,
            entity_id,
            state_name,
            value,
            value_type: rule.output.value_type.clone(),
            timestamp: context.timestamp.clone(),
            event_id: context.event_id.clone(),
            event_type: context.event_type.clone(),
            signal: context.signal.clone(),
        });
    }
    rows
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReportAggregateKey {
    tenant_id: String,
    project_id: String,
    report_id: String,
    report_version: u64,
    bucket_time: String,
    dimensions_json: String,
}

struct ReportAggregate {
    dimensions: Value,
    metrics: HashMap<String, f64>,
}

fn report_result_rows(
    rows: &[EventInsertRow],
    definitions: &ExtractionDefinitions,
) -> Vec<ReportResultRow> {
    let mut aggregates: HashMap<ReportAggregateKey, ReportAggregate> = HashMap::new();
    for row in rows {
        let Some(data) = row.data.as_object() else {
            continue;
        };
        let context = EventContext::from_event(row, data);
        for rule in &definitions.reports {
            if rule.tenant_id != context.tenant_id || !rule.matcher.matches(data) {
                continue;
            }
            let report_id = eval_string_expr(data, &rule.output.report_id);
            if report_id.is_empty() {
                continue;
            }
            let dimensions = dimension_object(data, &rule.output.dimensions);
            let dimensions_json =
                serde_json::to_string(&dimensions).unwrap_or_else(|_| "{}".to_string());
            let key = ReportAggregateKey {
                tenant_id: context.tenant_id.clone(),
                project_id: context.project_id.clone(),
                report_id,
                report_version: rule.definition_version,
                bucket_time: context.bucket_time.clone(),
                dimensions_json,
            };
            let aggregate = aggregates.entry(key).or_insert_with(|| ReportAggregate {
                dimensions,
                metrics: HashMap::new(),
            });
            for metric in &rule.output.metrics {
                let increment = match metric.op {
                    ReportMetricOp::Count => Some(1.0),
                    ReportMetricOp::ErrorCount => Some(if is_error_row(row) { 1.0 } else { 0.0 }),
                    ReportMetricOp::Sum => metric
                        .value
                        .as_ref()
                        .and_then(|expr| eval_number_expr(data, expr)),
                };
                if let Some(increment) = increment {
                    *aggregate.metrics.entry(metric.name.clone()).or_default() += increment;
                }
            }
            aggregate
                .metrics
                .entry("source_events".to_string())
                .and_modify(|value| *value += 1.0)
                .or_insert(1.0);
            aggregate
                .metrics
                .entry("bucket_seconds".to_string())
                .or_insert(f64::from(rule.output.bucket_seconds));
        }
    }

    let mut rows = aggregates
        .into_iter()
        .map(|(key, aggregate)| {
            let mut metrics = Map::new();
            for (name, value) in aggregate.metrics {
                metrics.insert(name, json_number(value));
            }
            ReportResultRow {
                tenant_id: key.tenant_id,
                project_id: key.project_id,
                report_id: key.report_id,
                report_version: key.report_version,
                bucket_time: key.bucket_time,
                dimensions: aggregate.dimensions,
                metrics: Value::Object(metrics),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.tenant_id
            .cmp(&right.tenant_id)
            .then(left.project_id.cmp(&right.project_id))
            .then(left.report_id.cmp(&right.report_id))
            .then(left.bucket_time.cmp(&right.bucket_time))
            .then(
                left.dimensions
                    .to_string()
                    .cmp(&right.dimensions.to_string()),
            )
    });
    rows
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TraceAggregateKey {
    tenant_id: String,
    project_id: String,
    report_id: String,
    report_version: u64,
    bucket_time: String,
    trace_id: String,
    bucket_seconds: u32,
}

struct TraceAggregate {
    dimensions: Map<String, Value>,
    event_count: u64,
    error_count: u64,
    started_at: Option<DateTime<Utc>>,
    ended_at: Option<DateTime<Utc>>,
}

fn trace_report_result_rows(
    rows: &[EventInsertRow],
    definitions: &ExtractionDefinitions,
) -> Vec<ReportResultRow> {
    let mut aggregates: HashMap<TraceAggregateKey, TraceAggregate> = HashMap::new();
    for row in rows {
        let Some(data) = row.data.as_object() else {
            continue;
        };
        let context = EventContext::from_event(row, data);
        if context.trace_id.is_empty() {
            continue;
        }
        for rule in &definitions.trace_reports {
            if rule.tenant_id != context.tenant_id || !rule.matcher.matches(data) {
                continue;
            }
            let report_id = eval_string_expr(data, &rule.output.report_id);
            if report_id.is_empty() {
                continue;
            }
            let key = TraceAggregateKey {
                tenant_id: context.tenant_id.clone(),
                project_id: context.project_id.clone(),
                report_id,
                report_version: rule.definition_version,
                bucket_time: context.bucket_time.clone(),
                trace_id: context.trace_id.clone(),
                bucket_seconds: rule.output.bucket_seconds,
            };
            let aggregate = aggregates.entry(key).or_insert_with(|| {
                let mut dimensions = dimension_object(data, &rule.output.dimensions)
                    .as_object()
                    .cloned()
                    .unwrap_or_default();
                dimensions.insert(
                    "trace_id".to_string(),
                    Value::String(context.trace_id.clone()),
                );
                TraceAggregate {
                    dimensions,
                    event_count: 0,
                    error_count: 0,
                    started_at: None,
                    ended_at: None,
                }
            });
            aggregate.event_count += 1;
            if is_error_row(row) {
                aggregate.error_count += 1;
            }
            if context.parent_span_id.is_empty() {
                if !context.name.is_empty() {
                    aggregate
                        .dimensions
                        .entry("root_name".to_string())
                        .or_insert_with(|| Value::String(context.name.clone()));
                }
                if let Some(service) = data.get("service").map(scalar_value_to_string)
                    && !service.is_empty()
                {
                    aggregate
                        .dimensions
                        .entry("root_service".to_string())
                        .or_insert(Value::String(service));
                }
            }
            let start = context
                .start_time
                .as_deref()
                .and_then(parse_event_timestamp)
                .or_else(|| parse_event_timestamp(&context.timestamp));
            let end = context
                .end_time
                .as_deref()
                .and_then(parse_event_timestamp)
                .or_else(|| parse_event_timestamp(&context.timestamp));
            if let Some(start) = start
                && aggregate.started_at.is_none_or(|current| start < current)
            {
                aggregate.started_at = Some(start);
            }
            if let Some(end) = end
                && aggregate.ended_at.is_none_or(|current| end > current)
            {
                aggregate.ended_at = Some(end);
            }
        }
    }

    let mut rows = aggregates
        .into_iter()
        .map(|(key, aggregate)| {
            let duration_ms = aggregate
                .started_at
                .zip(aggregate.ended_at)
                .map(|(start, end)| end.signed_duration_since(start).num_milliseconds().max(0))
                .unwrap_or_default();
            ReportResultRow {
                tenant_id: key.tenant_id,
                project_id: key.project_id,
                report_id: key.report_id,
                report_version: key.report_version,
                bucket_time: key.bucket_time,
                dimensions: Value::Object(aggregate.dimensions),
                metrics: serde_json::json!({
                    "duration_ms": duration_ms,
                    "event_count": aggregate.event_count,
                    "errors": aggregate.error_count,
                    "source_events": aggregate.event_count,
                    "bucket_seconds": key.bucket_seconds
                }),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.tenant_id
            .cmp(&right.tenant_id)
            .then(left.project_id.cmp(&right.project_id))
            .then(left.report_id.cmp(&right.report_id))
            .then(left.bucket_time.cmp(&right.bucket_time))
            .then(
                left.dimensions
                    .to_string()
                    .cmp(&right.dimensions.to_string()),
            )
    });
    rows
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SequenceEntityKey {
    tenant_id: String,
    project_id: String,
    report_id: String,
    report_version: u64,
    bucket_time: String,
    segment_json: String,
    entity_id: String,
}

#[derive(Debug, Clone)]
struct SequenceEventMatch {
    timestamp: String,
    step_index: usize,
    segment: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SequenceAggregateKey {
    tenant_id: String,
    project_id: String,
    report_id: String,
    report_version: u64,
    bucket_time: String,
    segment_json: String,
}

fn sequence_report_result_rows(
    rows: &[EventInsertRow],
    definitions: &ExtractionDefinitions,
) -> Vec<SequenceReportResultRow> {
    let mut entity_events: HashMap<SequenceEntityKey, Vec<SequenceEventMatch>> = HashMap::new();

    for row in rows {
        let Some(data) = row.data.as_object() else {
            continue;
        };
        let context = EventContext::from_event(row, data);
        for rule in &definitions.sequences {
            if rule.tenant_id != context.tenant_id || !rule.matcher.matches(data) {
                continue;
            }
            let _bucket_seconds = rule.output.bucket_seconds;
            let report_id = eval_string_expr(data, &rule.output.report_id);
            let entity_id = eval_string_expr(data, &rule.output.entity_id);
            if report_id.is_empty() || entity_id.is_empty() {
                continue;
            }
            let segment = dimension_object(data, &rule.output.segment);
            let segment_json = serde_json::to_string(&segment).unwrap_or_else(|_| "{}".to_string());
            for (step_index, step) in rule.output.steps.iter().enumerate() {
                if !step.matcher.matches(data) {
                    continue;
                }
                let key = SequenceEntityKey {
                    tenant_id: context.tenant_id.clone(),
                    project_id: context.project_id.clone(),
                    report_id: report_id.clone(),
                    report_version: rule.definition_version,
                    bucket_time: context.bucket_time.clone(),
                    segment_json: segment_json.clone(),
                    entity_id: entity_id.clone(),
                };
                entity_events
                    .entry(key)
                    .or_default()
                    .push(SequenceEventMatch {
                        timestamp: context.timestamp.clone(),
                        step_index,
                        segment: segment.clone(),
                    });
            }
        }
    }

    let mut aggregates: HashMap<SequenceAggregateKey, (Value, Vec<u64>)> = HashMap::new();
    let mut step_names: HashMap<(String, u64), Vec<String>> = HashMap::new();
    for rule in &definitions.sequences {
        step_names.insert(
            (
                eval_string_expr(&Map::new(), &rule.output.report_id),
                rule.definition_version,
            ),
            rule.output
                .steps
                .iter()
                .map(|step| step.name.clone())
                .collect(),
        );
    }

    for (key, mut matches) in entity_events {
        matches.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then(left.step_index.cmp(&right.step_index))
        });
        let mut reached = 0usize;
        for matched in &matches {
            if matched.step_index == reached {
                reached += 1;
            }
        }
        if reached == 0 {
            continue;
        }
        let aggregate_key = SequenceAggregateKey {
            tenant_id: key.tenant_id,
            project_id: key.project_id,
            report_id: key.report_id,
            report_version: key.report_version,
            bucket_time: key.bucket_time,
            segment_json: key.segment_json,
        };
        let segment = matches
            .first()
            .map(|matched| matched.segment.clone())
            .unwrap_or_else(|| Value::Object(Map::new()));
        let (_, counts) = aggregates
            .entry(aggregate_key)
            .or_insert_with(|| (segment, vec![0; reached]));
        if counts.len() < reached {
            counts.resize(reached, 0);
        }
        for count in counts.iter_mut().take(reached) {
            *count += 1;
        }
    }

    let mut rows = Vec::new();
    for (key, (segment, counts)) in aggregates {
        let names = step_names
            .get(&(key.report_id.clone(), key.report_version))
            .cloned()
            .unwrap_or_default();
        for (step_index, entity_count) in counts.iter().copied().enumerate() {
            let conversion_count = counts.get(step_index + 1).copied().unwrap_or(entity_count);
            rows.push(SequenceReportResultRow {
                tenant_id: key.tenant_id.clone(),
                project_id: key.project_id.clone(),
                report_id: key.report_id.clone(),
                report_version: key.report_version,
                bucket_time: key.bucket_time.clone(),
                segment: segment.clone(),
                step_index: step_index.try_into().unwrap_or(u16::MAX),
                step_name: names
                    .get(step_index)
                    .cloned()
                    .unwrap_or_else(|| format!("step_{step_index}")),
                entity_count,
                conversion_count,
            });
        }
    }
    rows.sort_by(|left, right| {
        left.tenant_id
            .cmp(&right.tenant_id)
            .then(left.project_id.cmp(&right.project_id))
            .then(left.report_id.cmp(&right.report_id))
            .then(left.bucket_time.cmp(&right.bucket_time))
            .then(left.step_index.cmp(&right.step_index))
    });
    rows
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RetentionMembershipKey {
    tenant_id: String,
    project_id: String,
    cohort_id: String,
    entity_type: String,
    entity_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RetentionAggregateKey {
    tenant_id: String,
    project_id: String,
    report_id: String,
    report_version: u64,
    bucket_time: String,
    dimensions_json: String,
}

struct RetentionAggregate {
    dimensions: Value,
    retained_entities: BTreeSet<String>,
    cohort_entities: BTreeSet<String>,
}

fn retention_report_result_rows(
    rows: &[EventInsertRow],
    definitions: &ExtractionDefinitions,
    current_memberships: &[CohortMembershipRow],
) -> Vec<ReportResultRow> {
    if definitions.retentions.is_empty() {
        return Vec::new();
    }
    let mut memberships = HashMap::new();
    let mut cohort_sizes: HashMap<(String, String, String, String), BTreeSet<String>> =
        HashMap::new();
    for membership in current_memberships {
        let key = RetentionMembershipKey {
            tenant_id: membership.tenant_id.clone(),
            project_id: membership.project_id.clone(),
            cohort_id: membership.cohort_id.clone(),
            entity_type: membership.entity_type.clone(),
            entity_id: membership.entity_id.clone(),
        };
        cohort_sizes
            .entry((
                membership.tenant_id.clone(),
                membership.project_id.clone(),
                membership.cohort_id.clone(),
                membership.entity_type.clone(),
            ))
            .or_default()
            .insert(membership.entity_id.clone());
        memberships.insert(key, membership.first_seen.clone());
    }

    let mut aggregates: HashMap<RetentionAggregateKey, RetentionAggregate> = HashMap::new();
    for row in rows {
        let Some(data) = row.data.as_object() else {
            continue;
        };
        let context = EventContext::from_event(row, data);
        let Some(activity_at) = parse_event_timestamp(&context.timestamp) else {
            continue;
        };
        for rule in &definitions.retentions {
            if rule.tenant_id != context.tenant_id || !rule.matcher.matches(data) {
                continue;
            }
            let report_id = eval_string_expr(data, &rule.output.report_id);
            let cohort_id = eval_string_expr(data, &rule.output.cohort_id);
            let entity_type = eval_string_expr(data, &rule.output.entity_type);
            let entity_id = eval_string_expr(data, &rule.output.entity_id);
            if report_id.is_empty()
                || cohort_id.is_empty()
                || entity_type.is_empty()
                || entity_id.is_empty()
            {
                continue;
            }
            let membership_key = RetentionMembershipKey {
                tenant_id: context.tenant_id.clone(),
                project_id: context.project_id.clone(),
                cohort_id: cohort_id.clone(),
                entity_type: entity_type.clone(),
                entity_id: entity_id.clone(),
            };
            let Some(first_seen) = memberships.get(&membership_key) else {
                continue;
            };
            let Some(joined_at) = parse_event_timestamp(first_seen) else {
                continue;
            };
            let elapsed = activity_at.signed_duration_since(joined_at).num_seconds();
            if elapsed < 0 {
                continue;
            }
            let retention_bucket = elapsed / i64::from(rule.output.retention_bucket_seconds.max(1));
            let mut dimensions = dimension_object(data, &rule.output.dimensions);
            if let Some(object) = dimensions.as_object_mut() {
                object.insert(
                    "retention_bucket".to_string(),
                    Value::String(retention_bucket.to_string()),
                );
            }
            let dimensions_json =
                serde_json::to_string(&dimensions).unwrap_or_else(|_| "{}".to_string());
            let aggregate_key = RetentionAggregateKey {
                tenant_id: context.tenant_id.clone(),
                project_id: context.project_id.clone(),
                report_id,
                report_version: rule.definition_version,
                bucket_time: context.bucket_time.clone(),
                dimensions_json,
            };
            let cohort_entities = cohort_sizes
                .get(&(
                    context.tenant_id.clone(),
                    context.project_id.clone(),
                    cohort_id.clone(),
                    entity_type.clone(),
                ))
                .cloned()
                .unwrap_or_default();
            let aggregate = aggregates
                .entry(aggregate_key)
                .or_insert_with(|| RetentionAggregate {
                    dimensions,
                    retained_entities: BTreeSet::new(),
                    cohort_entities,
                });
            aggregate.retained_entities.insert(entity_id);
        }
    }

    let mut rows = aggregates
        .into_iter()
        .map(|(key, aggregate)| {
            let retained_users = aggregate.retained_entities.len() as f64;
            let cohort_size = aggregate.cohort_entities.len() as f64;
            let retention_rate = if cohort_size > 0.0 {
                retained_users / cohort_size
            } else {
                0.0
            };
            ReportResultRow {
                tenant_id: key.tenant_id,
                project_id: key.project_id,
                report_id: key.report_id,
                report_version: key.report_version,
                bucket_time: key.bucket_time,
                dimensions: aggregate.dimensions,
                metrics: serde_json::json!({
                    "retained_users": retained_users as u64,
                    "cohort_size": cohort_size as u64,
                    "retention_rate": retention_rate,
                    "source_events": retained_users as u64
                }),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.tenant_id
            .cmp(&right.tenant_id)
            .then(left.project_id.cmp(&right.project_id))
            .then(left.report_id.cmp(&right.report_id))
            .then(left.bucket_time.cmp(&right.bucket_time))
            .then(
                left.dimensions
                    .to_string()
                    .cmp(&right.dimensions.to_string()),
            )
    });
    rows
}

async fn retention_memberships_for_rows(
    client: &Client,
    cfg: &Config,
    rows: &[EventInsertRow],
    definitions: &ExtractionDefinitions,
    current_memberships: &[CohortMembershipRow],
) -> Result<Vec<CohortMembershipRow>> {
    if definitions.retentions.is_empty() {
        return Ok(Vec::new());
    }

    let mut memberships = current_memberships.to_vec();
    let mut existing = BTreeSet::new();
    for membership in current_memberships {
        existing.insert((
            membership.tenant_id.clone(),
            membership.project_id.clone(),
            membership.cohort_id.clone(),
            membership.entity_type.clone(),
            membership.entity_id.clone(),
        ));
    }

    let mut wanted = BTreeSet::new();
    for row in rows {
        let Some(data) = row.data.as_object() else {
            continue;
        };
        let context = EventContext::from_event(row, data);
        for rule in &definitions.retentions {
            if rule.tenant_id != context.tenant_id || !rule.matcher.matches(data) {
                continue;
            }
            let cohort_id = eval_string_expr(data, &rule.output.cohort_id);
            let entity_type = eval_string_expr(data, &rule.output.entity_type);
            let entity_id = eval_string_expr(data, &rule.output.entity_id);
            if cohort_id.is_empty() || entity_type.is_empty() || entity_id.is_empty() {
                continue;
            }
            let key = (
                context.tenant_id.clone(),
                context.project_id.clone(),
                cohort_id,
                entity_type,
                entity_id,
            );
            if !existing.contains(&key) {
                wanted.insert(key);
            }
        }
    }

    if wanted.is_empty() {
        return Ok(memberships);
    }

    let clauses = wanted
        .into_iter()
        .map(|(tenant_id, project_id, cohort_id, entity_type, entity_id)| {
            format!(
                "(tenant_id = {} AND project_id = {} AND cohort_id = {} AND entity_type = {} AND entity_id = {})",
                quote_sql_string(&tenant_id),
                quote_sql_string(&project_id),
                quote_sql_string(&cohort_id),
                quote_sql_string(&entity_type),
                quote_sql_string(&entity_id)
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    let query = format!(
        "SELECT tenant_id, project_id, cohort_id, cohort_version, entity_type, entity_id, toString(first_seen) AS first_seen, toString(last_seen) AS last_seen FROM {} FINAL WHERE {} FORMAT JSON",
        cfg.qualified_cohort_memberships_table(),
        clauses
    );
    let body = clickhouse_query(client, cfg, &query)
        .await
        .context("query retention cohort memberships")?;
    let response: ClickHouseJson<CohortMembershipRow> =
        serde_json::from_str(&body).context("parse retention cohort memberships")?;
    memberships.extend(response.data);
    Ok(memberships)
}

fn json_number(value: f64) -> Value {
    if value.is_finite() && value.fract() == 0.0 && value >= 0.0 {
        return Value::Number(serde_json::Number::from(value as u64));
    }
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CohortMembershipKey {
    tenant_id: String,
    project_id: String,
    cohort_id: String,
    cohort_version: u64,
    entity_type: String,
    entity_id: String,
}

struct CohortMembershipAggregate {
    first_seen: String,
    last_seen: String,
}

fn cohort_membership_rows(
    rows: &[EventInsertRow],
    definitions: &ExtractionDefinitions,
) -> Vec<CohortMembershipRow> {
    let mut memberships: HashMap<CohortMembershipKey, CohortMembershipAggregate> = HashMap::new();
    for row in rows {
        let Some(data) = row.data.as_object() else {
            continue;
        };
        let context = EventContext::from_event(row, data);
        for rule in &definitions.cohorts {
            if rule.tenant_id != context.tenant_id || !rule.matcher.matches(data) {
                continue;
            }
            let cohort_id = eval_string_expr(data, &rule.output.cohort_id);
            let entity_type = eval_string_expr(data, &rule.output.entity_type);
            let entity_id = eval_string_expr(data, &rule.output.entity_id);
            if cohort_id.is_empty() || entity_type.is_empty() || entity_id.is_empty() {
                continue;
            }
            let key = CohortMembershipKey {
                tenant_id: context.tenant_id.clone(),
                project_id: context.project_id.clone(),
                cohort_id,
                cohort_version: rule.definition_version,
                entity_type,
                entity_id,
            };
            memberships
                .entry(key)
                .and_modify(|membership| {
                    if context.timestamp < membership.first_seen {
                        membership.first_seen = context.timestamp.clone();
                    }
                    if context.timestamp > membership.last_seen {
                        membership.last_seen = context.timestamp.clone();
                    }
                })
                .or_insert_with(|| CohortMembershipAggregate {
                    first_seen: context.timestamp.clone(),
                    last_seen: context.timestamp.clone(),
                });
        }
    }

    let mut rows = memberships
        .into_iter()
        .map(|(key, membership)| CohortMembershipRow {
            tenant_id: key.tenant_id,
            project_id: key.project_id,
            cohort_id: key.cohort_id,
            cohort_version: key.cohort_version,
            entity_type: key.entity_type,
            entity_id: key.entity_id,
            first_seen: membership.first_seen,
            last_seen: membership.last_seen,
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.tenant_id
            .cmp(&right.tenant_id)
            .then(left.project_id.cmp(&right.project_id))
            .then(left.cohort_id.cmp(&right.cohort_id))
            .then(left.entity_type.cmp(&right.entity_type))
            .then(left.entity_id.cmp(&right.entity_id))
    });
    rows
}

struct EventContext {
    tenant_id: String,
    project_id: String,
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
            project_id: string_value(data.get("project_id")),
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

    async fn datafusion_local_paths(
        &self,
        locations: &[String],
        tempdir: &mut Option<tempfile::TempDir>,
    ) -> Result<Vec<String>> {
        let mut paths = Vec::with_capacity(locations.len());
        for (index, location) in locations.iter().enumerate() {
            match data_file_location(location)? {
                DataFileLocation::Local(path) => {
                    paths.push(path.display().to_string());
                }
                DataFileLocation::S3 { bucket, key } => {
                    if tempdir.is_none() {
                        *tempdir = Some(tempfile::tempdir().context(
                            "create temporary directory for lakehouse SQL S3 Parquet inputs",
                        )?);
                    }
                    let root = tempdir
                        .as_ref()
                        .expect("lakehouse SQL temporary directory")
                        .path();
                    let file_name = format!(
                        "table-{}-{}.parquet",
                        index,
                        Sha256::digest(location.as_bytes())
                            .iter()
                            .take(8)
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>()
                    );
                    let path = root.join(file_name);
                    let bytes = self.read_s3_object(&bucket, &key).await?;
                    fs::write(&path, bytes)
                        .with_context(|| format!("write temporary {}", path.display()))?;
                    paths.push(path.display().to_string());
                }
            }
        }
        Ok(paths)
    }

    async fn data_file_size(&self, location: &str) -> Result<Option<u64>> {
        match data_file_location(location)? {
            DataFileLocation::Local(path) => match fs::metadata(&path) {
                Ok(metadata) => Ok(Some(metadata.len())),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(err) => {
                    Err(err).with_context(|| format!("stat local Parquet {}", path.display()))
                }
            },
            DataFileLocation::S3 { bucket, key } => {
                let head = self
                    .s3
                    .head_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .with_context(|| format!("head s3://{bucket}/{key}"))?;
                Ok(head
                    .content_length()
                    .and_then(|value| u64::try_from(value).ok()))
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
        cfg.clickhouse_events_table.as_str(),
        cfg.clickhouse_event_text_index_table.as_str(),
        cfg.clickhouse_event_kv_index_table.as_str(),
        cfg.clickhouse_field_index_table.as_str(),
        cfg.clickhouse_event_measures_table.as_str(),
        cfg.clickhouse_measure_cube_rollups_table.as_str(),
        cfg.clickhouse_counter_rollups_table.as_str(),
        cfg.clickhouse_gauge_rollups_table.as_str(),
        cfg.clickhouse_histogram_rollups_table.as_str(),
        cfg.clickhouse_entity_state_updates_table.as_str(),
        cfg.clickhouse_entity_state_current_table.as_str(),
        cfg.clickhouse_report_results_table.as_str(),
        cfg.clickhouse_sequence_report_results_table.as_str(),
        cfg.clickhouse_cohort_memberships_table.as_str(),
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

fn format_timestamp_ms(value: i64) -> Result<String> {
    let secs = value.div_euclid(1_000);
    let millis = value.rem_euclid(1_000) as u32;
    let dt = DateTime::<Utc>::from_timestamp(secs, millis * 1_000_000)
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

fn parse_event_timestamp(timestamp: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(timestamp)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S%.f")
                .map(|value| value.and_utc())
        })
        .ok()
}

fn parse_required_event_timestamp(timestamp: &str) -> Result<DateTime<Utc>> {
    parse_event_timestamp(timestamp).ok_or_else(|| anyhow!("invalid timestamp: {timestamp}"))
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

fn scalar_value_to_string(value: &Value) -> String {
    string_value(Some(value))
}

fn eval_string_expr(data: &Map<String, Value>, expr: &StringExpr) -> String {
    match expr {
        StringExpr::Literal(value) => value.clone(),
        StringExpr::Path { path, default } => {
            let value = string_value(value_at_path(data, path));
            if value.is_empty() {
                default.clone()
            } else {
                value
            }
        }
    }
}

fn eval_value_expr(data: &Map<String, Value>, expr: &ValueExpr) -> Option<Value> {
    match expr {
        ValueExpr::Literal(value) => Some(value.clone()),
        ValueExpr::Path { path } => value_at_path(data, path).cloned(),
    }
}

fn eval_number_expr(data: &Map<String, Value>, expr: &NumberExpr) -> Option<f64> {
    match expr {
        NumberExpr::Literal(value) => Some(*value),
        NumberExpr::Path { path } => optional_number_value(value_at_path(data, path)),
    }
}

impl Matcher {
    fn matches(&self, data: &Map<String, Value>) -> bool {
        self.predicates
            .iter()
            .all(|predicate| predicate.matches(data))
    }
}

impl Predicate {
    fn matches(&self, data: &Map<String, Value>) -> bool {
        let actual = value_at_path(data, &self.path);
        match self.op {
            PredicateOp::Exists => actual.is_some_and(|value| !value.is_null()),
            PredicateOp::Eq => actual
                .zip(self.value.as_ref())
                .is_some_and(|(actual, expected)| json_values_equal(actual, expected)),
            PredicateOp::Neq => actual
                .zip(self.value.as_ref())
                .is_some_and(|(actual, expected)| !json_values_equal(actual, expected)),
            PredicateOp::IsNumber => optional_number_value(actual).is_some(),
            PredicateOp::In => match self.value.as_ref() {
                Some(Value::Array(values)) => actual.is_some_and(|actual| {
                    values
                        .iter()
                        .any(|expected| json_values_equal(actual, expected))
                }),
                _ => false,
            },
        }
    }
}

fn json_values_equal(actual: &Value, expected: &Value) -> bool {
    actual == expected || string_value(Some(actual)) == string_value(Some(expected))
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

fn hex_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
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

fn sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
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

fn parse_lakehouse_sql_table_specs(value: &str) -> Result<Vec<LakehouseSqlTableSpec>> {
    let mut tables = Vec::new();
    for entry in value
        .split(';')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (name, locations) = entry.split_once('=').with_context(|| {
            format!("NANOTRACE_LAKEHOUSE_QUERY_TABLES entry must be name=path[,path]: {entry}")
        })?;
        let name = name.trim().to_string();
        validate_lakehouse_sql_table_name(&name)?;
        let locations = locations
            .split(',')
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if locations.is_empty() {
            bail!("lakehouse SQL table {name} must include at least one Parquet path");
        }
        tables.push(LakehouseSqlTableSpec { name, locations });
    }
    Ok(tables)
}

fn validate_lakehouse_sql_table_name(value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.chars().enumerate().all(|(index, ch)| {
            ch == '_' || ch.is_ascii_alphanumeric() && (index > 0 || ch.is_ascii_alphabetic())
        });
    if valid {
        Ok(())
    } else {
        bail!("lakehouse SQL table names must be simple identifiers")
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

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, time::Duration};

    use nanotrace_lakehouse::{LakehouseConfig, commit_events_ndjson_iceberg};
    use regex::Regex;
    use serde_json::json;

    use super::{
        CohortMembershipRow, CommitSource, Config, DataFileLocation, DefinitionRecord,
        EventInsertRow, ExtractionDefinitions, LakehouseCommitRecord, ReportResultRow,
        SequenceReportResultRow, TableflowMaterializerMode, cohort_membership_rows,
        combined_materialized_output_versions, commit_record_to_commit, data_file_location,
        entity_state_update_rows, event_measure_rows, field_index_rows, format_timestamp_us,
        materialization_chunk_id, materialization_job_id, materialization_target_table,
        materialize_targets_for_commit, materialized_output_versions, metric_rollup_rows,
        ndjson_chunks, queued_rows_written, report_result_rows, retention_report_result_rows,
        sdk_metric_managed_definition_record, sequence_report_result_rows,
        trace_report_result_rows,
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
    fn materialized_output_versions_aggregate_report_sequence_and_cohort_rows() {
        let reports = vec![
            ReportResultRow {
                tenant_id: "tenant-a".to_string(),
                project_id: "proj-a".to_string(),
                report_id: "checkout".to_string(),
                report_version: 7,
                bucket_time: "2026-06-04T00:00:00.000Z".to_string(),
                dimensions: json!({}),
                metrics: json!({"events": 1}),
            },
            ReportResultRow {
                tenant_id: "tenant-a".to_string(),
                project_id: "proj-a".to_string(),
                report_id: "checkout".to_string(),
                report_version: 7,
                bucket_time: "2026-06-04T00:01:00.000Z".to_string(),
                dimensions: json!({"plan": "pro"}),
                metrics: json!({"events": 1}),
            },
        ];
        let sequences = vec![SequenceReportResultRow {
            tenant_id: "tenant-a".to_string(),
            project_id: "proj-a".to_string(),
            report_id: "signup".to_string(),
            report_version: 3,
            bucket_time: "2026-06-04T00:00:00.000Z".to_string(),
            segment: json!({}),
            step_index: 0,
            step_name: "start".to_string(),
            entity_count: 10,
            conversion_count: 4,
        }];
        let cohorts = vec![CohortMembershipRow {
            tenant_id: "tenant-a".to_string(),
            project_id: "proj-a".to_string(),
            cohort_id: "june_signups".to_string(),
            cohort_version: 2,
            entity_type: "user".to_string(),
            entity_id: "user_1".to_string(),
            first_seen: "2026-06-01T00:00:00.000Z".to_string(),
            last_seen: "2026-06-04T00:00:00.000Z".to_string(),
        }];

        let versions = combined_materialized_output_versions(&materialized_output_versions(
            &reports, &sequences, &cohorts,
        ));

        let report = versions
            .iter()
            .find(|version| version.target_type == "report")
            .expect("report version");
        assert_eq!(report.target_id, "checkout");
        assert_eq!(report.target_version, 7);
        assert_eq!(report.row_count, 2);
        assert_eq!(report.source_start, "2026-06-04T00:00:00.000Z");
        assert_eq!(report.source_end, "2026-06-04T00:01:00.000Z");

        assert!(
            versions
                .iter()
                .any(|version| version.target_type == "sequence"
                    && version.target_id == "signup"
                    && version.target_version == 3)
        );
        assert!(
            versions
                .iter()
                .any(|version| version.target_type == "cohort"
                    && version.target_id == "june_signups"
                    && version.target_version == 2)
        );
    }

    #[test]
    fn materialization_control_plane_ids_are_stable_for_outputs() {
        let record = LakehouseCommitRecord {
            namespace: "nanotrace".to_string(),
            table_name: "events".to_string(),
            snapshot_id: "snapshot-9".to_string(),
            sequence_number: 42,
            committed_at_ms: 1_779_120_000_123,
            data_file: "file:///tmp/events.parquet".to_string(),
            data_files: vec!["file:///tmp/events.parquet".to_string()],
            record_count: 10,
            content_sha256: "abc".to_string(),
            metadata_location: "file:///tmp/metadata.json".to_string(),
            source_batch_id: "batch-1".to_string(),
            deduplicated: 0,
        };
        let commit = commit_record_to_commit(record);
        let version = super::MaterializedOutputVersion {
            tenant_id: "tenant-a".to_string(),
            target_type: "sequence",
            target_id: "signup".to_string(),
            target_version: 3,
            row_count: 12,
            source_start: "2026-06-04T00:00:00.000Z".to_string(),
            source_end: "2026-06-04T00:05:00.000Z".to_string(),
        };

        assert_eq!(
            materialization_job_id(&commit, &version),
            "nanotrace:snapshot-9:tenant-a:sequence:signup:3"
        );
        assert_eq!(
            materialization_chunk_id(&commit, &version),
            "nanotrace:snapshot-9:tenant-a:sequence:signup:3:chunk:0"
        );
        assert_eq!(materialization_target_table("report"), "report_results");
        assert_eq!(
            materialization_target_table("sequence"),
            "sequence_report_results"
        );
        assert_eq!(materialization_target_table("cohort"), "cohort_memberships");
    }

    #[test]
    fn queued_target_helpers_select_expected_tables_and_counts() {
        let report_targets =
            super::materialize_targets_for_queued_target("report").expect("report target");
        assert!(report_targets.report_results);
        assert!(!report_targets.sequence_report_results);
        assert!(!report_targets.cohort_memberships);

        let sequence_targets =
            super::materialize_targets_for_queued_target("sequence").expect("sequence target");
        assert!(sequence_targets.sequence_report_results);
        assert!(!sequence_targets.report_results);

        let counts = super::MaterializedCounts {
            report_results: 3,
            sequence_report_results: 4,
            cohort_memberships: 5,
            ..Default::default()
        };
        assert_eq!(queued_rows_written("report", &counts), 3);
        assert_eq!(queued_rows_written("sequence", &counts), 4);
        assert_eq!(queued_rows_written("cohort", &counts), 5);
    }

    #[test]
    fn queued_definition_filter_requires_literal_matching_target() {
        let definitions = ExtractionDefinitions {
            reports: vec![
                super::ReportRule {
                    tenant_id: "tenant-a".to_string(),
                    definition_version: 7,
                    matcher: super::Matcher::default(),
                    output: super::ReportOutput {
                        report_id: super::StringExpr::Literal("checkout".to_string()),
                        dimensions: Vec::new(),
                        metrics: Vec::new(),
                        bucket_seconds: 60,
                    },
                },
                super::ReportRule {
                    tenant_id: "tenant-a".to_string(),
                    definition_version: 7,
                    matcher: super::Matcher::default(),
                    output: super::ReportOutput {
                        report_id: super::StringExpr::Literal("other".to_string()),
                        dimensions: Vec::new(),
                        metrics: Vec::new(),
                        bucket_seconds: 60,
                    },
                },
            ],
            ..Default::default()
        };
        let chunk = super::QueuedMaterializationChunk {
            tenant_id: "tenant-a".to_string(),
            job_id: "job-1".to_string(),
            chunk_id: "chunk-1".to_string(),
            chunk_index: 0,
            target_type: "report".to_string(),
            target_table: "report_results".to_string(),
            target_id: "checkout".to_string(),
            target_version: 7,
            source_table: "events".to_string(),
            source_start: "2026-06-04T00:00:00.000Z".to_string(),
            source_end: "2026-06-05T00:00:00.000Z".to_string(),
            attempt: 0,
            max_attempts: 5,
        };

        let filtered =
            super::definitions_for_queued_target(&definitions, &chunk).expect("filter target");
        assert_eq!(filtered.reports.len(), 1);
        assert!(super::queued_definitions_have_target(&filtered, "report"));
    }

    #[tokio::test]
    async fn lakehouse_maintenance_audit_counts_local_small_files() {
        let dir = std::env::temp_dir().join(format!(
            "nanotrace-maintenance-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create test dir");
        let small = dir.join("small.parquet");
        let large = dir.join("large.parquet");
        std::fs::write(&small, b"small").expect("write small file");
        std::fs::write(&large, b"large-enough").expect("write large file");

        let s3_config = aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test",
            ))
            .build();
        let reader = super::LakehouseReader {
            s3: super::S3Client::from_conf(s3_config),
            s3_max_file_bytes: 1024 * 1024,
        };
        let mut cfg = test_config();
        cfg.lakehouse_maintenance_small_file_bytes = 10;
        let commits = vec![super::LakehouseCommit {
            namespace: "nanotrace".to_string(),
            table: "events".to_string(),
            snapshot_id: "snapshot-1".to_string(),
            sequence_number: 7,
            committed_at_ms: 0,
            data_file: String::new(),
            data_files: vec![
                small.display().to_string(),
                large.display().to_string(),
                dir.join("missing.parquet").display().to_string(),
            ],
            record_count: 2,
            content_sha256: "abc".to_string(),
            metadata_location: String::new(),
            source_batch_id: None,
            deduplicated: false,
        }];

        let audit = super::audit_lakehouse_maintenance(&reader, &cfg, &commits)
            .await
            .expect("audit maintenance");

        assert_eq!(audit.commit_count, 1);
        assert_eq!(audit.data_file_count, 3);
        assert_eq!(audit.object_store_data_file_count, 0);
        assert_eq!(audit.known_data_file_bytes, 17);
        assert_eq!(audit.small_data_file_count, 1);
        assert_eq!(audit.data_file_inspect_error_count, 0);
        assert!(!audit.engine_maintenance_required);
        assert_eq!(audit.first_sequence, 7);
        assert_eq!(audit.last_sequence, 7);

        std::fs::remove_dir_all(dir).expect("remove test dir");
    }

    #[test]
    fn object_store_data_file_detector_covers_common_iceberg_schemes() {
        assert!(super::is_object_store_data_file(
            "s3://bucket/table/data.parquet"
        ));
        assert!(super::is_object_store_data_file(
            "s3a://bucket/table/data.parquet"
        ));
        assert!(super::is_object_store_data_file(
            "gs://bucket/table/data.parquet"
        ));
        assert!(super::is_object_store_data_file(
            "abfs://container/table/data.parquet"
        ));
        assert!(super::is_object_store_data_file(
            "abfss://container/table/data.parquet"
        ));
        assert!(!super::is_object_store_data_file("/tmp/table/data.parquet"));
        assert!(!super::is_object_store_data_file(
            "file:///tmp/table/data.parquet"
        ));
    }

    #[test]
    fn lakehouse_maintenance_flags_object_store_pressure_without_engine_command() {
        let mut cfg = test_config();
        cfg.lakehouse_native_compaction_min_input_files = 2;
        cfg.lakehouse_maintenance_cmd = None;
        let audit = super::LakehouseMaintenanceAudit {
            object_store_data_file_count: 2,
            small_data_file_count: 2,
            ..Default::default()
        };

        assert!(super::object_store_engine_maintenance_required(
            &audit, &cfg
        ));
        cfg.lakehouse_maintenance_cmd = Some("spark-maintenance".to_string());
        assert!(!super::object_store_engine_maintenance_required(
            &audit, &cfg
        ));
    }

    #[test]
    fn lakehouse_query_filter_matches_tenant_time_type_and_text() {
        let row = fixture_row("llm_call.json");
        let filter = super::LakehouseQueryFilter {
            tenant_id: Some("fixture".to_string()),
            from: Some(
                super::parse_required_event_timestamp("2026-05-08T01:00:00.000Z").expect("from"),
            ),
            to: Some(
                super::parse_required_event_timestamp("2026-05-08T02:00:00.000Z").expect("to"),
            ),
            event_type: Some("llm.call".to_string()),
            text: Some("gpt-5.5".to_string()),
            regex: Some(Regex::new(r#""model":"gpt-5\.5""#).expect("regex")),
            limit: 100,
        };

        assert!(super::lakehouse_query_row_matches(&row, &filter));

        let wrong_tenant = super::LakehouseQueryFilter {
            tenant_id: Some("other".to_string()),
            ..filter
        };
        assert!(!super::lakehouse_query_row_matches(&row, &wrong_tenant));
    }

    #[test]
    fn lakehouse_query_filter_rejects_non_matching_regex() {
        let row = fixture_row("llm_call.json");
        let filter = super::LakehouseQueryFilter {
            tenant_id: None,
            from: None,
            to: None,
            event_type: None,
            text: None,
            regex: Some(Regex::new(r#"error_rate":0\.[5-9]"#).expect("regex")),
            limit: 100,
        };

        assert!(!super::lakehouse_query_row_matches(&row, &filter));
    }

    #[test]
    fn parses_lakehouse_sql_table_specs() {
        let specs = super::parse_lakehouse_sql_table_specs(
            "accounts=/tmp/accounts-1.parquet,/tmp/accounts-2.parquet; tickets=s3://bucket/tickets.parquet",
        )
        .expect("parse table specs");

        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "accounts");
        assert_eq!(
            specs[0].locations,
            vec![
                "/tmp/accounts-1.parquet".to_string(),
                "/tmp/accounts-2.parquet".to_string()
            ]
        );
        assert_eq!(specs[1].name, "tickets");
        assert!(super::parse_lakehouse_sql_table_specs("bad-name=/tmp/a.parquet").is_err());
        assert!(super::parse_lakehouse_sql_table_specs("missing_equals").is_err());
    }

    #[tokio::test]
    async fn lakehouse_sql_query_runs_join_over_committed_parquet() {
        let root = tempfile::tempdir().expect("tempdir");
        let cfg = LakehouseConfig::events_table(root.path());
        let commit = commit_events_ndjson_iceberg(
            &cfg,
            br#"{"event_id":"evt-a","timestamp":"2026-06-04T00:00:00Z","data":{"tenant_id":"tenant-a","event_type":"span","trace_id":"trace-1","span_id":"span-a","service":"api"}}
{"event_id":"evt-b","timestamp":"2026-06-04T00:00:01Z","data":{"tenant_id":"tenant-a","event_type":"span","trace_id":"trace-1","span_id":"span-b","service":"worker"}}
"#,
            None,
        )
        .await
        .expect("commit events");
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        let reader = super::LakehouseReader {
            s3: super::s3_client(&aws_config),
            s3_max_file_bytes: 1024 * 1024,
        };
        let mut output = Vec::new();

        let rows = super::run_lakehouse_sql_query_for_tables(
            &reader,
            vec![super::LakehouseSqlTableSpec {
                name: "events".to_string(),
                locations: super::commit_data_files(&commit),
            }],
            "SELECT a.event_id AS parent_event, b.event_id AS child_event FROM events a JOIN events b ON a.trace_id = b.trace_id WHERE a.event_id < b.event_id",
            10,
            &mut output,
        )
        .await
        .expect("run sql");

        assert_eq!(rows, 1);
        let output = String::from_utf8(output).expect("utf8");
        assert!(output.contains(r#""parent_event":"evt-a""#));
        assert!(output.contains(r#""child_event":"evt-b""#));
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
                "project_id": "proj_1",
                "event_type": "track",
                "country": "US",
                "revenue": 42.5,
                "currency": "USD",
                "user_id": "user_1",
                "plan": "pro"
            }),
        };
        let definitions = ExtractionDefinitions::from_records(vec![
            DefinitionRecord {
                tenant_id: "org_1".to_string(),
                definition_id: "def_country".to_string(),
                name: "country".to_string(),
                kind: "field".to_string(),
                mode: "facet".to_string(),
                config: json!({
                    "path": "country",
                    "value_type": "string"
                }),
                version: 7,
            },
            DefinitionRecord {
                tenant_id: "org_1".to_string(),
                definition_id: "def_revenue".to_string(),
                name: "revenue".to_string(),
                kind: "measure".to_string(),
                mode: String::new(),
                config: json!({
                    "path": "revenue",
                    "unit": "usd",
                    "dimension": "currency"
                }),
                version: 8,
            },
            DefinitionRecord {
                tenant_id: "org_1".to_string(),
                definition_id: "def_plan".to_string(),
                name: "plan".to_string(),
                kind: "state".to_string(),
                mode: String::new(),
                config: json!({
                    "path": "plan",
                    "value_type": "string",
                    "entity_type": "user",
                    "entity_id_path": "user_id"
                }),
                version: 9,
            },
        ]);

        let fields = field_index_rows(&row, &definitions);
        let measures = event_measure_rows(&row, &definitions);
        let states = entity_state_update_rows(&row, &definitions);

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].project_id, "proj_1");
        assert_eq!(fields[0].field_name, "country");
        assert_eq!(fields[0].value, "US");
        assert_eq!(measures.len(), 1);
        assert_eq!(measures[0].project_id, "proj_1");
        assert_eq!(measures[0].value, 42.5);
        assert_eq!(measures[0].dimension_value, "USD");
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].project_id, "proj_1");
        assert_eq!(states[0].entity_id, "user_1");
        assert_eq!(states[0].value, "pro");
    }

    #[test]
    fn sdk_metric_definition_materializes_metric_rollup_fixtures() {
        let definitions =
            ExtractionDefinitions::from_records(vec![sdk_metric_managed_definition_record(
                "fixture",
            )]);

        for (fixture, expected_name, expected_value, expected_unit, expected_kind) in [
            ("metric_counter.json", "llm.requests", 1.0, "1", "counter"),
            (
                "metric_gauge.json",
                "process.memory.usage",
                734003200.0,
                "By",
                "gauge",
            ),
            (
                "metric_histogram.json",
                "tool.duration",
                245.0,
                "ms",
                "histogram",
            ),
        ] {
            let row = fixture_row(fixture);
            let rollups = metric_rollup_rows(&row, &definitions);
            match expected_kind {
                "counter" => {
                    assert_eq!(rollups.counters.len(), 1);
                    let row = &rollups.counters[0];
                    assert_eq!(row.definition_id, "sdk_metric_default_v1");
                    assert_eq!(row.metric_name, expected_name);
                    assert_eq!(row.sum, expected_value);
                    assert_eq!(row.unit, expected_unit);
                    assert_eq!(row.bucket_seconds, 60);
                    assert!(row.dimensions.get("service").is_some());
                    assert!(row.dimensions.get("environment").is_some());
                    assert!(row.dimensions.get("signal").is_some());
                    assert!(row.dimensions.get("metric_type").is_some());
                }
                "gauge" => {
                    assert_eq!(rollups.gauges.len(), 1);
                    let row = &rollups.gauges[0];
                    assert_eq!(row.metric_name, expected_name);
                    assert_eq!(row.sum, expected_value);
                    assert_eq!(row.min, expected_value);
                    assert_eq!(row.max, expected_value);
                    assert_eq!(row.last, expected_value);
                    assert_eq!(row.unit, expected_unit);
                }
                "histogram" => {
                    assert_eq!(rollups.histograms.len(), 1);
                    let row = &rollups.histograms[0];
                    assert_eq!(row.metric_name, expected_name);
                    assert_eq!(row.sum, expected_value);
                    assert_eq!(row.min, expected_value);
                    assert_eq!(row.max, expected_value);
                    assert_eq!(row.unit, expected_unit);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn generalized_field_definition_materializes_nested_llm_paths() {
        let row = fixture_row("llm_call.json");
        let definitions = ExtractionDefinitions::from_records(vec![DefinitionRecord {
            tenant_id: "fixture".to_string(),
            definition_id: "def_llm_fields".to_string(),
            name: "llm fields".to_string(),
            kind: "field".to_string(),
            mode: "facet".to_string(),
            config: json!({
                "match": {
                    "all": [
                        { "path": "event_type", "op": "eq", "value": "llm.call" }
                    ]
                },
                "outputs": [
                    {
                        "target": "field_index",
                        "field_name": "llm.model",
                        "value": { "path": "llm.model" },
                        "value_type": "string",
                        "mode": "facet"
                    },
                    {
                        "target": "field_index",
                        "field_name": "llm.provider",
                        "value": { "path": "llm.provider" },
                        "value_type": "string",
                        "mode": "facet"
                    }
                ]
            }),
            version: 11,
        }]);

        let fields = field_index_rows(&row, &definitions);

        assert_eq!(fields.len(), 2);
        assert!(
            fields
                .iter()
                .any(|row| row.field_name == "llm.model" && row.value == "gpt-5.5")
        );
        assert!(
            fields
                .iter()
                .any(|row| row.field_name == "llm.provider" && row.value == "openai")
        );
        assert!(fields.iter().all(|row| row.definition_version == 11));
    }

    #[test]
    fn generalized_state_definition_materializes_nested_account_state() {
        let row = fixture_row("state_account_plan_changed.json");
        let definitions = ExtractionDefinitions::from_records(vec![DefinitionRecord {
            tenant_id: "fixture".to_string(),
            definition_id: "def_account_plan".to_string(),
            name: "account.plan".to_string(),
            kind: "state".to_string(),
            mode: String::new(),
            config: json!({
                "match": {
                    "all": [
                        { "path": "event_type", "op": "eq", "value": "account.plan_changed" }
                    ]
                },
                "outputs": [
                    {
                        "target": "entity_state_updates",
                        "entity_type": "account",
                        "entity_id": { "path": "account.id" },
                        "state_name": "account.plan",
                        "value": { "path": "account.plan" },
                        "value_type": "string"
                    }
                ]
            }),
            version: 12,
        }]);

        let states = entity_state_update_rows(&row, &definitions);

        assert_eq!(states.len(), 1);
        assert_eq!(states[0].entity_type, "account");
        assert_eq!(states[0].entity_id, "acct_fixture");
        assert_eq!(states[0].state_name, "account.plan");
        assert_eq!(states[0].value, "enterprise");
    }

    #[test]
    fn generalized_measure_definition_materializes_product_revenue() {
        let row = fixture_row("product_order_filled.json");
        let definitions = ExtractionDefinitions::from_records(vec![DefinitionRecord {
            tenant_id: "fixture".to_string(),
            definition_id: "def_product_revenue".to_string(),
            name: "revenue".to_string(),
            kind: "measure".to_string(),
            mode: String::new(),
            config: json!({
                "match": {
                    "all": [
                        { "path": "event_type", "op": "in", "value": ["order.filled", "checkout.completed"] }
                    ]
                },
                "outputs": [
                    {
                        "target": "event_measures",
                        "measure_name": "revenue",
                        "value": { "path": "revenue" },
                        "unit": "usd",
                        "dimensions": [
                            { "name": "account.plan", "value": { "path": "account.plan" } },
                            { "name": "order.symbol", "value": { "path": "order.symbol" } }
                        ],
                        "bucket_seconds": 300
                    }
                ]
            }),
            version: 13,
        }]);

        let measures = event_measure_rows(&row, &definitions);

        assert_eq!(measures.len(), 2);
        assert!(measures.iter().all(|row| row.measure_name == "revenue"));
        assert!(measures.iter().all(|row| row.value == 0.42));
        assert!(
            measures
                .iter()
                .any(|row| row.dimension_name == "account.plan" && row.dimension_value == "pro")
        );
        assert!(
            measures
                .iter()
                .any(|row| row.dimension_name == "order.symbol" && row.dimension_value == "NVDA")
        );
    }

    #[test]
    fn generalized_report_definition_materializes_summary_rows() {
        let rows = vec![
            fixture_row("metric_counter.json"),
            fixture_row("metric_gauge.json"),
            fixture_row("metric_histogram.json"),
        ];
        let definitions = ExtractionDefinitions::from_records(vec![DefinitionRecord {
            tenant_id: "fixture".to_string(),
            definition_id: "def_metric_report".to_string(),
            name: "metrics.by_type".to_string(),
            kind: "report".to_string(),
            mode: "summary".to_string(),
            config: json!({
                "match": {
                    "all": [
                        { "path": "event_type", "op": "eq", "value": "metric" }
                    ]
                },
                "outputs": [
                    {
                        "target": "report_results",
                        "report_id": "metrics_by_type",
                        "dimensions": [
                            { "name": "metric_type", "value": { "path": "metric_type" } }
                        ],
                        "metrics": [
                            { "name": "events", "op": "count" },
                            { "name": "value_sum", "op": "sum", "value": { "path": "metric_value" } }
                        ],
                        "bucket_seconds": 60
                    }
                ]
            }),
            version: 14,
        }]);

        let reports = report_result_rows(&rows, &definitions);

        assert_eq!(reports.len(), 3);
        assert!(reports.iter().all(|row| row.report_id == "metrics_by_type"));
        assert!(reports.iter().all(|row| row.report_version == 14));
        let counter = reports
            .iter()
            .find(|row| row.dimensions["metric_type"] == "counter")
            .expect("counter report row");
        assert_eq!(counter.metrics["events"], 1);
        assert_eq!(counter.metrics["value_sum"], 1);
    }

    #[test]
    fn generalized_trace_report_materializes_trace_summary_rows() {
        let rows = vec![fixture_row("span_start.json"), fixture_row("span_end.json")];
        let definitions = ExtractionDefinitions::from_records(vec![DefinitionRecord {
            tenant_id: "fixture".to_string(),
            definition_id: "def_trace_summary".to_string(),
            name: "top_slow_traces".to_string(),
            kind: "report".to_string(),
            mode: "trace_summary".to_string(),
            config: json!({
                "match": {
                    "all": [
                        { "path": "trace_id", "op": "exists" }
                    ]
                },
                "outputs": [
                    {
                        "target": "report_results",
                        "report_id": "top_slow_traces",
                        "dimensions": [
                            { "name": "service", "value": { "path": "service" } }
                        ]
                    }
                ]
            }),
            version: 18,
        }]);

        let reports = trace_report_result_rows(&rows, &definitions);

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].report_id, "top_slow_traces");
        assert_eq!(reports[0].report_version, 18);
        assert_eq!(
            reports[0].dimensions["trace_id"],
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(reports[0].dimensions["root_service"], "codex-orchestrator");
        assert_eq!(reports[0].dimensions["root_name"], "POST /v1/responses");
        assert_eq!(reports[0].metrics["duration_ms"], 231);
        assert_eq!(reports[0].metrics["event_count"], 2);
        assert_eq!(reports[0].metrics["errors"], 1);
    }

    #[test]
    fn generalized_cohort_definition_materializes_memberships() {
        let rows = vec![
            fixture_row("product_checkout_completed.json"),
            fixture_row("product_order_filled.json"),
            fixture_row("state_account_plan_changed.json"),
        ];
        let definitions = ExtractionDefinitions::from_records(vec![DefinitionRecord {
            tenant_id: "fixture".to_string(),
            definition_id: "def_pro_accounts".to_string(),
            name: "pro_accounts".to_string(),
            kind: "cohort".to_string(),
            mode: "membership".to_string(),
            config: json!({
                "match": {
                    "all": [
                        { "path": "account.plan", "op": "eq", "value": "pro" }
                    ]
                },
                "outputs": [
                    {
                        "target": "cohort_memberships",
                        "cohort_id": "pro_accounts",
                        "entity_type": "account",
                        "entity_id": { "path": "account.id" }
                    }
                ]
            }),
            version: 15,
        }]);

        let memberships = cohort_membership_rows(&rows, &definitions);

        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].cohort_id, "pro_accounts");
        assert_eq!(memberships[0].cohort_version, 15);
        assert_eq!(memberships[0].entity_type, "account");
        assert_eq!(memberships[0].entity_id, "acct_fixture");
        assert!(memberships[0].first_seen <= memberships[0].last_seen);
    }

    #[test]
    fn generalized_retention_report_materializes_from_cohort_memberships() {
        let rows = vec![
            fixture_row("metric_gauge.json"),
            fixture_row("metric_runtime.json"),
        ];
        let definitions = ExtractionDefinitions::from_records(vec![
            DefinitionRecord {
                tenant_id: "fixture".to_string(),
                definition_id: "def_metric_gauge_cohort".to_string(),
                name: "metric_gauge_names".to_string(),
                kind: "cohort".to_string(),
                mode: "membership".to_string(),
                config: json!({
                    "match": {
                        "all": [
                            { "path": "event_type", "op": "eq", "value": "metric" },
                            { "path": "metric_type", "op": "eq", "value": "gauge" }
                        ]
                    },
                    "outputs": [
                        {
                            "target": "cohort_memberships",
                            "cohort_id": "metric_gauge_names",
                            "entity_type": "metric_name",
                            "entity_id": { "path": "metric_name" }
                        }
                    ]
                }),
                version: 16,
            },
            DefinitionRecord {
                tenant_id: "fixture".to_string(),
                definition_id: "def_metric_retention".to_string(),
                name: "metric_gauge_retention".to_string(),
                kind: "report".to_string(),
                mode: "retention".to_string(),
                config: json!({
                    "match": {
                        "all": [
                            { "path": "event_type", "op": "eq", "value": "metric" },
                            { "path": "metric_type", "op": "eq", "value": "gauge" }
                        ]
                    },
                    "outputs": [
                        {
                            "target": "report_results",
                            "report_id": "metric_gauge_retention",
                            "cohort_id": "metric_gauge_names",
                            "entity_type": "metric_name",
                            "entity_id": { "path": "metric_name" },
                            "retention_bucket_seconds": 60
                        }
                    ]
                }),
                version: 17,
            },
        ]);
        let memberships = cohort_membership_rows(&rows, &definitions);
        let reports = retention_report_result_rows(&rows, &definitions, &memberships);

        assert_eq!(memberships.len(), 2);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].report_id, "metric_gauge_retention");
        assert_eq!(reports[0].report_version, 17);
        assert_eq!(reports[0].dimensions["retention_bucket"], "0");
        assert_eq!(reports[0].metrics["retained_users"], 2);
        assert_eq!(reports[0].metrics["cohort_size"], 2);
        assert_eq!(reports[0].metrics["retention_rate"], 1.0);
    }

    #[test]
    fn generalized_sequence_definition_materializes_funnel_steps() {
        let rows = vec![
            fixture_row("metric_counter.json"),
            fixture_row("metric_gauge.json"),
        ];
        let definitions = ExtractionDefinitions::from_records(vec![DefinitionRecord {
            tenant_id: "fixture".to_string(),
            definition_id: "def_metric_sequence".to_string(),
            name: "metrics.counter_to_gauge".to_string(),
            kind: "sequence".to_string(),
            mode: "funnel".to_string(),
            config: json!({
                "outputs": [
                    {
                        "target": "sequence_report_results",
                        "report_id": "metric_counter_to_gauge",
                        "entity_id": "fixture_run",
                        "segment": [
                            { "name": "service", "value": { "path": "service" } }
                        ],
                        "steps": [
                            {
                                "name": "counter",
                                "match": { "all": [
                                    { "path": "metric_type", "op": "eq", "value": "counter" }
                                ] }
                            },
                            {
                                "name": "gauge",
                                "match": { "all": [
                                    { "path": "metric_type", "op": "eq", "value": "gauge" }
                                ] }
                            }
                        ]
                    }
                ]
            }),
            version: 21,
        }]);

        let rows = sequence_report_result_rows(&rows, &definitions);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].report_id, "metric_counter_to_gauge");
        assert_eq!(rows[0].report_version, 21);
        assert_eq!(rows[0].step_index, 0);
        assert_eq!(rows[0].step_name, "counter");
        assert_eq!(rows[0].entity_count, 1);
        assert_eq!(rows[0].conversion_count, 1);
        assert_eq!(rows[1].step_index, 1);
        assert_eq!(rows[1].step_name, "gauge");
        assert_eq!(rows[1].entity_count, 1);
    }

    #[test]
    fn generalized_measure_definition_skips_match_and_type_failures() {
        let definitions =
            ExtractionDefinitions::from_records(vec![sdk_metric_managed_definition_record(
                "fixture",
            )]);
        let non_metric = fixture_row("llm_call.json");
        let rollups = metric_rollup_rows(&non_metric, &definitions);
        assert!(rollups.counters.is_empty());
        assert!(rollups.gauges.is_empty());
        assert!(rollups.histograms.is_empty());

        let mut bad_metric = fixture_row("metric_counter.json");
        bad_metric.data["metric_value"] = json!("not-a-number");
        let rollups = metric_rollup_rows(&bad_metric, &definitions);
        assert!(rollups.counters.is_empty());
        assert!(rollups.gauges.is_empty());
        assert!(rollups.histograms.is_empty());
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
        watermarks.insert("measure_cube_rollups".to_string(), 9);
        watermarks.insert("entity_state_updates".to_string(), 7);

        let targets = materialize_targets_for_commit(&cfg, 9, &watermarks);

        assert!(targets.events);
        assert!(targets.event_text_index);
        assert!(targets.event_kv_index);
        assert!(targets.field_index);
        assert!(!targets.event_measures);
        assert!(!targets.measure_cube_points);
        assert!(targets.entity_state_updates);
        assert!(targets.entity_state_current);
        assert!(targets.any());
    }

    fn fixture_row(file_name: &str) -> EventInsertRow {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/events")
            .join(file_name);
        let bytes = std::fs::read(&path).unwrap_or_else(|error| {
            panic!("read fixture {}: {error}", path.display());
        });
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_else(|error| {
            panic!("parse fixture {}: {error}", path.display());
        });
        EventInsertRow {
            event_id: value["event_id"].as_str().unwrap_or(file_name).to_string(),
            timestamp: value["timestamp"]
                .as_str()
                .unwrap_or("2026-05-08T01:23:45.123Z")
                .to_string(),
            observed_timestamp: value["observed_timestamp"]
                .as_str()
                .unwrap_or("2026-05-08T01:23:45.123Z")
                .to_string(),
            ingested_timestamp: value["observed_timestamp"]
                .as_str()
                .unwrap_or("2026-05-08T01:23:45.123Z")
                .to_string(),
            source_file: format!("fixtures/events/{file_name}"),
            source_offset: 0,
            source_length: u32::try_from(bytes.len()).unwrap_or(u32::MAX),
            data: value["data"].clone(),
        }
    }

    fn test_config() -> Config {
        Config {
            kafka_brokers: "localhost:9092".to_string(),
            tableflow_topic: "events.tableflow.batches.v1".to_string(),
            tableflow_materializer_group_id: "test-materializer".to_string(),
            tableflow_materializer_client_id: "test-materializer".to_string(),
            warehouse_dir: PathBuf::from("/tmp/lakehouse"),
            namespace: "nanotrace".to_string(),
            source_table: "events".to_string(),
            clickhouse_url: "http://localhost:8123".to_string(),
            clickhouse_user: None,
            clickhouse_password: None,
            clickhouse_database: "observatory".to_string(),
            clickhouse_events_table: "events".to_string(),
            clickhouse_event_text_index_table: "event_text_index".to_string(),
            clickhouse_event_kv_index_table: "event_kv_index".to_string(),
            clickhouse_field_index_table: "field_index".to_string(),
            clickhouse_event_measures_table: "event_measures".to_string(),
            clickhouse_measure_cube_points_table: "measure_cube_points".to_string(),
            clickhouse_measure_cube_rollups_table: "measure_cube_rollups".to_string(),
            clickhouse_counter_rollups_table: "counter_rollups".to_string(),
            clickhouse_gauge_rollups_table: "gauge_rollups".to_string(),
            clickhouse_histogram_rollups_table: "histogram_rollups".to_string(),
            clickhouse_entity_state_updates_table: "entity_state_updates".to_string(),
            clickhouse_entity_state_current_table: "entity_state_current".to_string(),
            clickhouse_report_results_table: "report_results".to_string(),
            clickhouse_sequence_report_results_table: "sequence_report_results".to_string(),
            clickhouse_cohort_memberships_table: "cohort_memberships".to_string(),
            clickhouse_definitions_table: "definitions".to_string(),
            truncate_events: false,
            rebuild_raw: true,
            rebuild_derived: true,
            incremental_materialize: false,
            materialize_loop: false,
            materialization_queue_executor: false,
            tableflow_materializer_mode: TableflowMaterializerMode::Disabled,
            materialization_queue_max_chunks: 10,
            materialization_queue_lease_secs: 300,
            materialization_queue_worker_id: "test-worker".to_string(),
            lakehouse_maintenance: false,
            lakehouse_maintenance_small_file_bytes: 128 * 1024 * 1024,
            lakehouse_native_compaction: false,
            lakehouse_native_compaction_min_input_files: 2,
            lakehouse_native_compaction_target_file_size_bytes: 512 * 1024 * 1024,
            lakehouse_maintenance_cmd: None,
            lakehouse_query: false,
            lakehouse_query_tenant_id: None,
            lakehouse_query_from: None,
            lakehouse_query_to: None,
            lakehouse_query_event_type: None,
            lakehouse_query_text: None,
            lakehouse_query_regex: None,
            lakehouse_query_sql: None,
            lakehouse_query_tables: Vec::new(),
            lakehouse_query_limit: 1000,
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
