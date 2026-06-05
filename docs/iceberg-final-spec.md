# Nanotrace Iceberg Final Design

This document describes the target Iceberg-native design and the current
implementation slice. Today, the local/default hot path is HTTP ingest to
Kafka/Redpanda, normalizer, Iceberg commit, and materializer-driven ClickHouse
serving catch-up. The implemented lakehouse tools are also available for
replay, rebuild, and materialization work.

## Goal

In the final design, every accepted event is committed to open, versioned
lakehouse storage. ClickHouse serves recent and materialized interactive views,
but ClickHouse is rebuildable from lakehouse snapshots and is not the source of
truth.

## Principles

- Iceberg/lakehouse storage is truth for normalized valid events.
- ClickHouse is speed.
- Materialization is the product engine.
- Raw JSON is preserved.
- Bounded scalar KVs are flattened into `event_kv_index` for universal
  filtering.
- Only SDK-typed or user-defined semantics are promoted, aggregated, or rolled
  up.
- Repeated broad queries must become promoted fields, reports, or offline jobs.

## Systems

- Ingest API: authenticates, stamps tenant identity, validates event envelopes,
  and writes raw requests to Kafka/Redpanda.
- Normalizer: consumes Kafka, validates and tenant-stamps events, commits valid
  rows to Iceberg, records invalid rows, and advances offsets after durable work
  succeeds.
- Serving materializer: tails committed lakehouse snapshots and writes
  ClickHouse `events`, `event_kv_index`, promoted indexes, measures, states,
  reports, sequences, and cohorts.
- Materializers: build promoted fields, measures, states, reports, cohorts,
  funnels, and trace summaries from completed source snapshots.
- Query router: routes logical product queries to ClickHouse serving tables,
  `event_kv_index`, lakehouse scans, materialized outputs, or
  offline/new-subsystem paths.
- Compactor: rewrites small files, expires snapshots, and cleans orphan files
  after commit verification.
- Materialization lineage: records input snapshot, definition version/config,
  job identity, and output snapshot for replayable materializations.

## Canonical Event Table

The canonical event table keeps the raw event payload and a stable flattened
vocabulary:

```text
tenant_id
event_id
timestamp
observed_timestamp
ingested_timestamp
event_type
signal
trace_id
span_id
parent_span_id
service
environment
name
duration_ms
is_error
source_file
source_offset
data
```

Arbitrary customer fields remain in `data` and bounded scalar values are also
flattened into `event_kv_index` for filtering. They are promoted into semantic
derived tables only when an SDK type or explicit user/admin definition supplies
that intent.

## Serving Metadata

Each lakehouse commit records:

```text
namespace
table
snapshot_id
sequence_number
committed_at_ms
data_file
record_count
content_sha256
```

Each ClickHouse serving table records:

```text
serving_table
source_namespace
source_table
source_snapshot_id
source_sequence_number
source_record_count
status
updated_at
attributes
```

This makes serving lag observable and ClickHouse rebuilds deterministic.

## Query Taxonomy

Group 1 runs immediately from serving tables:

```text
latest events
single event lookup
bounded trace/span lookup
global event density
tenant-wide error rate
core top fields
```

Group 2 requires promotion, materialization, or bounded `event_kv_index`
planning:

```text
arbitrary group-by
multi-field scalar filters
dimensioned alerts
numeric percentiles
revenue analytics
DAU/WAU/MAU
top-N entities
funnels
retention
entity state at time
experiment analysis
broad trace analytics
reusable dashboards
```

Group 3 requires a new subsystem or offline path:

```text
free-text search over all payloads
regex over arbitrary JSON
exact distinct over huge arbitrary windows
full historical entity replay on demand
cross-tenant global analytics
large joins with external datasets
millisecond streaming alerts
```

## Current Implementation Slice

The current implementation includes the first Iceberg vertical slice:

- `crates/lakehouse` writes canonical events to Apache Iceberg V2 table
  metadata and field-id Parquet data files through the official Rust Iceberg
  writer/transaction APIs.
- Local development uses a filesystem-backed Iceberg table plus a small
  Nanotrace catalog pointer so repeated normalizer batches append to the same
  table.
- Production deployments can set `NANOTRACE_ICEBERG_REST_URI` and
  `NANOTRACE_ICEBERG_WAREHOUSE` to commit through an Iceberg REST catalog and
  object-store-backed warehouse.
- `nanotrace-normalizer` commits normalized Kafka batches to the lakehouse
  before committing Kafka offsets. It records invalid rows and lakehouse commit
  metadata, but raw ClickHouse serving rows and serving indexes are loaded by
  the materializer.
- Normalizer commits include a deterministic `nanotrace.source-batch-id`
  snapshot property based on the Kafka topic/partition/offset. Redelivered
  batches reuse the existing Iceberg snapshot instead of appending duplicate
  data files.
- `observatory.lakehouse_commits` records committed lakehouse snapshots.
  Each row carries the canonical first data file, the full Iceberg data-file
  list, table metadata location, deterministic source batch id, and
  deduplication flag. ClickHouse-sourced materializers must replay the full
  data-file list, falling back to the single `data_file` only for older rows.
- `observatory.serving_watermarks` records which snapshot ClickHouse serving
  tables have loaded. The materializer writes watermarks for `events`,
  `event_text_index`, `event_kv_index`, and derived serving tables after those
  target tables catch up.
- `nanotrace-lakehouse-rebuild` reads Nanotrace commit records and committed
  Parquet files from the lakehouse, then reloads `observatory.events`,
  `observatory.event_text_index`, `observatory.event_kv_index`,
  active-definition materializations
  (`observatory.field_index`, `observatory.event_measures`, metric rollups,
  `observatory.entity_state_updates`, `observatory.entity_state_current`,
  reports, sequences, and cohorts),
  `observatory.lakehouse_commits`, and `observatory.serving_watermarks` with
  deterministic ClickHouse insert tokens. Rebuilds are guarded against
  accidental writes into non-empty serving tables; materialization can run
  independently with `NANOTRACE_REBUILD_RAW=false`. The rebuild path supports
  both local `file://` data files and remote `s3://`/`s3a://` Iceberg data files.
- Incremental materialization can run with
  `NANOTRACE_MATERIALIZE_INCREMENTAL=true NANOTRACE_REBUILD_RAW=false`. It reads
  serving-table watermarks, scans only snapshots that are ahead of at least one
  raw, generic, or promoted serving table, writes the missing rows, and advances
  only the watermarks for tables it actually caught up.
- The same binary can run as a continuous materializer with
  `NANOTRACE_MATERIALIZE_LOOP=true`. This is the serving-plane catch-up worker:
  it repeatedly reloads active definitions, reads Nanotrace lakehouse commit
  records, applies the incremental materialization planner, and sleeps for
  `NANOTRACE_MATERIALIZE_POLL_SECS` between passes. Local Docker Compose wires
  this as the `materializer` service. Materializers can use
  `NANOTRACE_REBUILD_COMMIT_SOURCE=clickhouse` to read shared
  `observatory.lakehouse_commits` rows instead of local sidecar commit files;
  this shared source preserves multi-file Iceberg snapshots.
- Lakehouse table creation and existing-table loads enforce table properties
  for ZSTD Parquet writes, target file sizing, metadata cleanup, and snapshot
  retention. The normalizer and rebuild tooling expose these as
  `NANOTRACE_ICEBERG_TARGET_FILE_SIZE_BYTES`,
  `NANOTRACE_ICEBERG_MIN_SNAPSHOTS_TO_KEEP`,
  `NANOTRACE_ICEBERG_MAX_SNAPSHOT_AGE_MS`, and
  `NANOTRACE_ICEBERG_METADATA_PREVIOUS_VERSIONS_MAX`.
- Query reads inspect their source tables and compare raw/promoted serving
  watermarks to the latest lakehouse commit. Stale reads are rejected by
  default, with an explicit `allow_stale_serving` override for diagnostic or
  operator workflows.
- Local Docker Compose enables the normalizer lakehouse writer by default and
  stores warehouse files in the `lakehouse-data` volume.

Lakehouse maintenance is exposed as an operator mode in
`nanotrace-lakehouse-rebuild`. It audits snapshot/data-file pressure, writes
`pipeline_metrics`, keeps table retention properties configured, and can run an
operator-provided native Iceberg maintenance command for compaction, snapshot
expiry, and orphan cleanup. The Rust code does not delete data files behind
Iceberg metadata.
