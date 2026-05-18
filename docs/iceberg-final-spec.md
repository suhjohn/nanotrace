# Nanotrace Iceberg Final Design

Nanotrace is an Iceberg-native event lake with ClickHouse hot serving indexes.

## Goal

Every accepted event is committed to open, versioned lakehouse storage. ClickHouse
serves recent and materialized interactive views, but ClickHouse is rebuildable
from lakehouse snapshots and is not the source of truth.

## Principles

- Iceberg/lakehouse storage is truth.
- ClickHouse is speed.
- Materialization is the product engine.
- Raw JSON is preserved.
- Only the stable Nanotrace vocabulary is flattened.
- Repeated broad queries must become promoted fields, reports, or offline jobs.

## Systems

- Ingest API: authenticates, stamps tenant identity, validates event envelopes,
  and writes durable landing parts.
- Lake writer: reads landing objects, writes canonical Parquet data files, and
  publishes snapshot metadata.
- ClickHouse sync: loads committed lakehouse snapshots into serving tables and
  records watermarks.
- Materializers: build promoted fields, measures, states, reports, cohorts,
  funnels, and trace summaries from completed source snapshots.
- Query router: routes logical product queries to ClickHouse serving tables,
  lakehouse scans, materialized outputs, or offline/new-subsystem paths.
- Compactor: rewrites small files, expires snapshots, cleans orphan files, and
  removes landing objects after commit verification.
- Processor lineage: records input snapshot, processor version/config, and
  output snapshot for replayable transforms.

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
source_length
data
```

Arbitrary customer fields remain in `data` until they are promoted into derived
tables.

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

Group 2 requires promotion or materialization:

```text
arbitrary group-by
multi-field filters
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
  Nanotrace catalog pointer so repeated loader batches append to the same table.
- Production deployments can set `NANOTRACE_ICEBERG_REST_URI` and
  `NANOTRACE_ICEBERG_WAREHOUSE` to commit through an Iceberg REST catalog and
  object-store-backed warehouse.
- `nanotrace-loader` can commit processed event batches to the lakehouse before
  inserting ClickHouse serving rows.
- Loader commits include a deterministic `nanotrace.source-batch-id` snapshot
  property based on the S3 object batch. Redelivered batches reuse the existing
  Iceberg snapshot instead of appending duplicate data files.
- `observatory.lakehouse_commits` records committed lakehouse snapshots.
  Each row carries the canonical first data file, the full Iceberg data-file
  list, table metadata location, deterministic source batch id, and
  deduplication flag. ClickHouse-sourced materializers must replay the full
  data-file list, falling back to the single `data_file` only for older rows.
- `observatory.serving_watermarks` records which snapshot ClickHouse serving
  tables have loaded.
- `nanotrace-lakehouse-rebuild` reads Nanotrace commit records and committed
  Parquet files from the lakehouse, then reloads `observatory.events`,
  active-definition materializations (`observatory.field_index`,
  `observatory.event_measures`, `observatory.entity_state_updates`),
  `observatory.lakehouse_commits`, and `observatory.serving_watermarks` with
  deterministic ClickHouse insert tokens. Rebuilds are guarded against
  accidental writes into non-empty serving tables; materialization can run
  independently with `NANOTRACE_REBUILD_RAW=false`. The rebuild path supports
  both local `file://` data files and remote `s3://`/`s3a://` Iceberg data files.
- Incremental materialization can run with
  `NANOTRACE_MATERIALIZE_INCREMENTAL=true NANOTRACE_REBUILD_RAW=false`. It reads
  derived-table watermarks, scans only snapshots that are ahead of at least one
  promoted serving table, writes the missing rows, and advances only the
  watermarks for tables it actually caught up.
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
  retention. The loader exposes these as
  `NANOTRACE_ICEBERG_TARGET_FILE_SIZE_BYTES`,
  `NANOTRACE_ICEBERG_MIN_SNAPSHOTS_TO_KEEP`,
  `NANOTRACE_ICEBERG_MAX_SNAPSHOT_AGE_MS`, and
  `NANOTRACE_ICEBERG_METADATA_PREVIOUS_VERSIONS_MAX`.
- Loader concurrency is guarded by a Postgres ingest ledger when
  `NANOTRACE_POSTGRES_URL` is present. The ledger table
  `nanotrace_ingest_batches` is keyed by deterministic S3 batch id, records the
  active owner and lakehouse snapshot, lets completed redeliveries be
  acknowledged before S3 download or rewriting, and allows stale in-progress
  rows to be reclaimed after `NANOTRACE_INGEST_LEDGER_STALE_SECS`.
- Query reads inspect their source tables and compare raw/promoted serving
  watermarks to the latest lakehouse commit. Stale reads are rejected by
  default, with an explicit `allow_stale_serving` override for diagnostic or
  operator workflows.
- Local Docker Compose enables the lakehouse writer by default and stores
  warehouse files in the `lakehouse-data` volume.

The next major milestone is a native file-compaction action when the Rust
Iceberg API exposes one.
