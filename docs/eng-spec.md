# Nanotrace Rust Server Engineering Spec

Last reviewed: 2026-05-11

Nanotrace is a high-throughput observability ingest system for teams that want
to own their telemetry pipeline. It accepts logs, traces, metrics, and product
events over HTTP, writes them durably, stores the raw stream in S3, and indexes
the result in ClickHouse.

The current landed architecture is:

```text
HTTP clients -> AWS load balancer -> Rust ingest servers -> durable local disk
-> S3 raw event log -> SQS -> Rust loader -> ClickHouse
```

The core idea is simple: acknowledge events after a durable local write, move
raw data to cheap object storage, and let ClickHouse handle fast reads.

## Why This Exists

Most observability ingest paths are either expensive, opaque, or too tightly
coupled to the query store. Nanotrace keeps the hot write path small and makes
the raw data easy to reason about.

- The request path does not wait on S3 or ClickHouse.
- Accepted events are written to local disk with `flush + sync_data`.
- S3 holds the raw immutable event log.
- ClickHouse is an index over that raw log, not the only copy.
- Every ClickHouse row points back to the exact S3 byte range for the original
  JSON line.
- Event-specific fields live in a ClickHouse `JSON` column, so adding new event
  properties does not require a schema migration.
- Optional processors can transform events before S3 upload or before
  ClickHouse load.

## What Is Implemented

The current Rust implementation provides three core pieces.

1. A Rust ingest API that accepts authenticated event writes.
2. A durable raw event pipeline backed by local EBS and S3.
3. A ClickHouse query index for fast analysis and retrieval.

It is not just an HTTP collector. It is a raw-log plus query-index system with
explicit durability boundaries.

## Current Scope

This spec describes the Rust server and the currently landed AWS/S3/SQS loader
path.

Earlier design sessions explored Kafka, Confluent Cloud, Parquet, and
ClickPipes. Those experiments were useful, but they are not the active product
path in this worktree. The current implementation is local durable NDJSON,
S3 upload, SQS notification, Rust loader, and ClickHouse `JSONEachRow` inserts.

## Design Problems

### Keep Writes Cheap

Many observability systems put a database, queue, or managed ingest service
directly on the request path. That raises cost and increases failure coupling.

Nanotrace acknowledges requests after local durable append. S3 upload and
ClickHouse insertion happen asynchronously.

### Keep Raw Events

Telemetry systems often optimize for speed and silently drop raw context.

Nanotrace treats S3 as the durable source of truth. ClickHouse is the query
index, not the only copy.

### Avoid Schema Churn

Logs, traces, metrics, and product analytics do not all fit the same fixed
columns.

Nanotrace keeps semantic fields inside a ClickHouse native `JSON` column with
typed hot paths. New event properties can be added without a migration for every
payload change.

### Make Debugging Possible

If a dashboard looks wrong, teams need to inspect exactly what was accepted.

Each ClickHouse row points back to `source_file`, `source_offset`, and
`source_length`, allowing exact S3 byte-range retrieval of the final uploaded
event row.

## Architecture

```text
Client SDKs / agents
  |
  | POST /events
  v
Rust ingest server
  |
  | durable append, flush, fsync
  v
Local EBS NDJSON parts
  |
  | background upload
  v
S3 raw event log
  |
  | object-created notification
  v
SQS queue
  |
  | loader workers
  v
ClickHouse observatory.events
```

The architecture intentionally separates write acceptance from analytical
availability:

- Accepted means safely written to local durable disk.
- Uploaded means raw data is safely in S3.
- Queryable means the loader has inserted the row into ClickHouse.

This separation makes failures easier to reason about. ClickHouse can be slow or
unavailable without forcing the ingest API to reject writes that have already
landed safely on disk.

## Ingest API

All protected endpoints use:

```http
Authorization: Bearer <SECRET_KEY>
```

Requests without the exact bearer token are rejected before the server parses or
writes event data.

### `POST /events`

The server accepts one event object or a non-empty array of event objects.

Required fields:

- `event_id`: producer-provided event identity
- `timestamp`: event time
- `data`: all semantic event properties

Optional field:

- `observed_timestamp`: producer or collector observation time

Example event:

```json
{
  "event_id": "evt_123",
  "timestamp": "2026-05-10T12:34:56.789Z",
  "observed_timestamp": "2026-05-10T12:34:56.900Z",
  "data": {
    "tenant_id": "acme",
    "service": "checkout-api",
    "event_type": "log",
    "severity_text": "ERROR",
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "message": "payment failed"
  }
}
```

Example response:

```json
{
  "event_id": "evt_123",
  "source_file": "events/dt=2026-05-10/hour=12/host=i-abc/lane=0/part-123.ndjson",
  "source_offset": 0,
  "source_length": 234
}
```

The receipt is an acceptance receipt for the local durable append. When no
`upload` processor is active, its source fields also match the final S3 object
byte range. When an `upload` processor transforms or drops rows, the uploader
restamps source fields in the final S3 object and ClickHouse will contain the
final byte-range pointers.

Batch requests return one receipt per accepted event by default. Deployments can
enable compact batch receipts with `NANOTRACE_COMPACT_BATCH_RECEIPTS=true` when
they prefer smaller response bodies over per-event receipt detail.

Processors do not run in the `POST /events` request path. A successful response
means the server accepted and durably appended the submitted event rows to the
local log. Upload and loader processors can later transform or drop rows in the
asynchronous pipeline.

### `GET /events/{event_id}`

Fetches the final S3 event JSON for a queryable event.

The server uses ClickHouse only to find the S3 pointer, then fetches the exact
byte range from S3 and verifies the returned line has the requested `event_id`.

This endpoint is useful for support, compliance review, data debugging, and
replay workflows. If an `upload` processor changed the row before S3 upload,
this endpoint returns that final uploaded row.

### `POST /query`

Runs constrained read-only ClickHouse queries through the server.

The server rejects mutating SQL, multi-statement SQL, explicit `FORMAT` clauses,
and non-scalar parameters. It also applies ClickHouse limits for rows, execution
time, and bytes read.

This provides a useful query surface without exposing unrestricted ClickHouse
access.

### Processors

Nanotrace supports dynamic processors for event transformation.

Processors can run at two stages:

- `upload`: after a local part is closed and before the S3 object is written.
- `loader`: after the S3 object is fetched and before rows are inserted into
  ClickHouse.

Typical use cases:

- Redact sensitive fields.
- Normalize customer-specific event shapes.
- Add derived fields.
- Enforce tenant-specific routing.
- Transform legacy payloads during migration.

Processor code, config, manifests, and artifacts are stored in S3 under
`processors/`.

#### `GET /processors`

Lists active processor manifests from `processors/index.json`.

Response shape:

```json
{
  "processors": [
    {
      "name": "redact-pii",
      "stages": ["upload", "loader"],
      "status": "ready",
      "config": null,
      "artifact_key": null,
      "artifact_sha256": null,
      "configs": {
        "upload": { "fields": ["email"] },
        "loader": {}
      },
      "artifacts": {
        "upload": {
          "key": "processors/redact-pii/upload/artifacts/aarch64-unknown-linux-gnu/libprocessor.so",
          "sha256": "..."
        }
      },
      "error": null,
      "updated_at": "2026-05-11T18:00:00+00:00"
    }
  ]
}
```

Deleted processors are filtered out of the list.

#### `PUT /processors/{name}`

Creates or replaces a processor definition and starts an asynchronous build.

Processor names must be 1-128 characters and may contain only ASCII letters,
digits, `_`, and `-`.

Request body:

```json
{
  "upload": {
    "code": "pub fn transform_event(event: serde_json::Value, config: &serde_json::Value) -> anyhow::Result<Option<serde_json::Value>> { Ok(Some(event)) }",
    "config": {}
  },
  "loader": {
    "code": "pub fn transform_event(event: serde_json::Value, config: &serde_json::Value) -> anyhow::Result<Option<serde_json::Value>> { Ok(Some(event)) }",
    "config": {}
  }
}
```

At least one of `upload` or `loader` is required. Each supplied stage must have
non-empty Rust source in `code`; `config` defaults to `{}`.

`PUT` does not synchronously compile or typecheck the Rust source before
returning. The API validates only the processor name, required stage presence,
non-empty source code, and S3 availability. Rust validity is checked by the
asynchronous builder after the source is stored.

The server writes:

```text
processors/{name}/{stage}/source.rs
processors/{name}/{stage}/config.json
processors/{name}/manifest.json
processors/index.json
```

The immediate response is a manifest with `status: "building"` and no artifacts
yet. The server then spawns `PROCESSOR_BUILDER_CMD` with:

```text
PROCESSOR_BUCKET=<S3 bucket>
PROCESSOR_NAME=<name>
```

The default builder is `python3 /usr/local/bin/modal_processor_builder.py`.
It compiles each stage as a Rust `cdylib`, uploads stage artifacts to S3, stores
their SHA-256 hashes, and updates the manifest to either `ready` or `failed`.
If the Rust source does not compile, does not define the required
`transform_event` function, or fails to link for `PROCESSOR_TARGET`, the builder
records `status: "failed"` and stores the compiler/build error in `error`.

Build-time and runtime happen in different places:

- Build time: the default builder runs the compile on Modal. It creates a Modal
  sandbox from a Rust image, writes the submitted source into that sandbox, runs
  `cargo build --release --target <PROCESSOR_TARGET>`, and uploads the compiled
  `.so` artifact back to S3.
- Runtime: the compiled artifact runs inside Nanotrace. `nanotrace-server` loads
  `upload` processors from S3 and executes them in the background uploader.
  `nanotrace-loader` loads `loader` processors from S3 and executes them before
  ClickHouse insertion.

So Modal is only the build environment for the default setup. The live ingest
and load path execute compiled processor artifacts inside the Nanotrace
server/loader processes.

Artifact layout:

```text
processors/{name}/{stage}/artifacts/{PROCESSOR_TARGET}/libprocessor.so
```

Default target:

```text
aarch64-unknown-linux-gnu
```

Build lifecycle:

```text
PUT request
  -> source/config uploaded
  -> manifest status=building
  -> name added to processors/index.json
  -> builder process starts
  -> manifest status=ready with artifacts
     or status=failed with error
  -> uploader/loader processor runtime picks it up on next sync interval
```

#### `DELETE /processors/{name}`

Removes the processor name from `processors/index.json` and writes a
`status: "deleted"` manifest. It does not eagerly delete old source or artifact
objects.

#### Processor Code Contract

Processor source supplies this Rust function:

```rust
pub fn transform_event(
    event: serde_json::Value,
    config: &serde_json::Value,
) -> anyhow::Result<Option<serde_json::Value>> {
    Ok(Some(event))
}
```

Return values:

- `Ok(Some(event))`: emit the transformed row.
- `Ok(None)`: drop the row.
- `Err(error)`: fail the processor call.

For backward compatibility, processors that return
`anyhow::Result<serde_json::Value>` are also accepted by the generated wrapper
and are treated as `Ok(Some(event))`.

Example drop:

```rust
pub fn transform_event(
    event: serde_json::Value,
    _config: &serde_json::Value,
) -> anyhow::Result<Option<serde_json::Value>> {
    if event["data"]["event_type"] == "debug" {
        return Ok(None);
    }

    Ok(Some(event))
}
```

#### Supported Processor Libraries

Processor code is compiled inside a generated Rust crate. The submitted `code`
is written to `src/user.rs`; users do not provide their own `Cargo.toml`.

Current generated dependencies:

```toml
[dependencies]
anyhow = "1"
serde_json = "1"
```

Supported today:

- Rust standard library.
- `serde_json` for reading and modifying JSON events.
- `anyhow` for returning errors from `transform_event`.
- Pure Rust helper code included directly inside the submitted source.

Not supported today:

- Supplying arbitrary Cargo dependencies in the API request.
- Network package resolution controlled by the submitted processor.
- Native system libraries beyond what the builder image already contains.
- Long-running services or background tasks inside processors.
- Async processor functions. The processor contract is synchronous.
- Processors that require filesystem state to be durable across calls.
- Processors that call back into Nanotrace APIs as part of the hot path.

This keeps processor builds deterministic and keeps runtime loading simple. If a
processor needs additional crates, the builder contract should be extended
explicitly rather than allowing arbitrary dependency injection by default.

The builder wraps that function in a stable C ABI:

```text
nanotrace_transform_v1(input, config, output, error) -> c_int
nanotrace_free_v1(owned_bytes)
```

The runtime verifies the downloaded `.so` against the manifest SHA-256 before
loading it.

Upload-stage processors receive the closed local NDJSON object before it is
uploaded to S3. They can keep rows, mutate fields, remove keys, add keys, or
drop rows. If an upload processor changes or drops rows, the uploader restamps
`source_file`, `source_offset`, and `source_length` so the final S3 object
remains byte-range addressable. If every row is dropped, no S3 object is
uploaded for that local part and the local part is marked `.done`.

Loader-stage processors run the same `transform_event` contract over each row
in the uploaded NDJSON object. They can keep, mutate, or drop rows before
ClickHouse insertion. If the processed object has zero complete rows, the
loader skips the ClickHouse insert.

#### Processor Operational Notes

- Processor sync is polling-based; new `ready` artifacts are not loaded
  instantly.
- If `processors/index.json` is missing or unreadable, the runtime falls back to
  identity processing.
- A processor failure on the upload path marks the local file `.failed`; the
  original local bytes are retained for inspection or retry.
- A processor failure on the loader path makes SQS retry the object later.
- A dropped upload row remains in the local accepted log until retention removes
  the `.done` file, but it is not uploaded to S3.
- A dropped loader row remains in the S3 raw log but is not inserted into
  ClickHouse.
- Processor code is powerful native code. It should be treated as trusted code
  and reviewed before enabling in production.

## Durability

Nanotrace has explicit durability stages.

### Request Durability

The server only responds after event bytes are appended to a local file and the
writer lane completes `flush + sync_data`.

This means a successful `POST /events` does not depend on S3, SQS, ClickHouse,
or the network path to those services.

### Raw Log Durability

Closed local files are uploaded to S3 as immutable NDJSON parts. If an `upload`
processor is configured, it runs during this background upload step; otherwise
the uploader streams the closed file to S3 unchanged. The server does not delete
local uploaded files immediately. A separate retention process removes `.done`
files only after the configured retention window.

### Query Durability

The loader consumes S3 notifications from SQS and inserts rows into ClickHouse.
ClickHouse is optimized for search and analytics, while S3 remains the raw
record.

## Local Files

Each writer lane owns one active local file. That design removes the single
global append lock that would otherwise limit write throughput.

File lifecycle:

```text
.tmp -> .ready -> .uploading -> .done
                         \
                          -> .failed
```

Meanings:

- `.tmp`: active or incomplete local file.
- `.ready`: complete immutable local file ready for upload.
- `.uploading`: file currently owned by the uploader.
- `.done`: upload succeeded.
- `.failed`: upload failed and the file is retained for inspection.

Startup recovery is conservative:

- `.uploading` files go back to `.ready`.
- `.tmp` files are truncated to the last complete NDJSON line and then marked
  `.ready`.
- `.done` and `.failed` files are preserved.

The practical benefit is predictable recovery after process restart or instance
failure.

## Raw Event Format

Every S3 object is uncompressed NDJSON. Each line is one event:

```json
{
  "event_id": "evt_123",
  "timestamp": "2026-05-10T12:34:56.789Z",
  "observed_timestamp": "2026-05-10T12:34:56.900Z",
  "source_file": "events/dt=2026-05-10/hour=12/host=i-abc/lane=0/part-123.ndjson",
  "source_offset": 0,
  "source_length": 234,
  "data": {
    "tenant_id": "acme",
    "event_type": "log"
  }
}
```

Uncompressed NDJSON is a deliberate choice. It allows exact byte-range fetches
from S3 using ClickHouse pointer columns.

## ClickHouse Data Model

Nanotrace creates an append-only `observatory.events` table by default.

Top-level columns are reserved for event identity, time, ingest metadata, and
raw-log pointers:

- `event_id`
- `timestamp`
- `observed_timestamp`
- `ingested_timestamp`
- `source_file`
- `source_offset`
- `source_length`
- `data`

Most event-specific fields live inside `data JSON(...)`.

Materialized fields are promoted only when they are needed for sorting,
filtering, or skip indexes:

- `tenant_id`
- `event_type`
- `trace_id`
- `span_id`
- `signal`

This gives the schema a practical balance:

- Flexible enough for changing telemetry payloads.
- Structured enough for fast tenant, event-type, trace, and span queries.
- Cheap enough to avoid exploding top-level column count.

The primary table does not dedupe. If a loader retry inserts the same S3 object
twice, ClickHouse may contain duplicate rows. Dedupe is handled by downstream
queries, derived tables, or future exactly-once ingestion improvements.

## Loader Behavior

The loader is a separate Rust binary included in the same Docker image.

For each SQS message:

1. Parse the S3 object notification.
2. Fetch the uploaded NDJSON object.
3. Run optional loader processors.
4. Insert rows into ClickHouse using `JSONEachRow`.
5. Delete the SQS message only after successful processing.

If ClickHouse is unavailable, the message remains in SQS and is retried after
visibility timeout. This favors durability over strict exactly-once semantics.

## Security

Current security controls:

- Bearer-token auth on protected HTTP endpoints.
- Requests without a valid token are rejected before parsing.
- AWS instance role is preferred over static AWS keys in production.
- S3 public access is blocked in the Pulumi stack.
- ClickHouse reads through `/query` are constrained to read-only SQL.
- Processor artifacts are SHA-256 verified before loading.

The current auth model is intentionally simple. Production deployments may still
want API gateway auth, per-tenant tokens, TLS termination policy, WAF rules, or
network-private ClickHouse connectivity.

## Deployment

The current Pulumi AWS deployment provisions:

- VPC and public subnets.
- Application Load Balancer.
- Auto Scaling Group of EC2 instances.
- gp3 EBS data volume for local durable parts.
- S3 bucket for raw events and processor artifacts.
- SQS queue for loader notifications.
- S3 object-created notification to SQS.
- ECR repository and Docker image build/push.
- IAM role for S3, SQS, and ECR access.
- ClickHouse schema application.

Each EC2 instance runs:

- `nanotrace-server`
- `nanotrace-loader`

The same Docker image contains both binaries. The server handles ingest and
reads; the loader handles S3-to-ClickHouse indexing.

## Operational Controls

Important server settings:

```text
SECRET_KEY
PORT
NANOTRACE_DATA_DIR
NANOTRACE_S3_BUCKET
S3_PREFIX
MAX_REQUEST_BYTES
MAX_EVENT_BYTES
NANOTRACE_PART_MAX_BYTES
NANOTRACE_PART_MAX_AGE_SECS
UPLOAD_POLL_INTERVAL_MS
NANOTRACE_DONE_RETENTION_MINS
NANOTRACE_DONE_CLEANUP_INTERVAL_SECS
NANOTRACE_WRITER_LANES
NANOTRACE_WRITER_QUEUE_CAPACITY
NANOTRACE_WRITER_FLUSH_INTERVAL_MS
NANOTRACE_WRITER_FLUSH_BYTES
NANOTRACE_COMPACT_BATCH_RECEIPTS
```

Important ClickHouse settings:

```text
CLICKHOUSE_URL
CLICKHOUSE_USER
CLICKHOUSE_PASSWORD
CLICKHOUSE_DATABASE
CLICKHOUSE_TABLE
CLICKHOUSE_MAX_RESULT_ROWS
CLICKHOUSE_MAX_EXECUTION_SECS
CLICKHOUSE_MAX_BYTES_TO_READ
```

Important loader settings:

```text
LOADER_SQS_QUEUE_URL
LOADER_POLL_WAIT_SECS
LOADER_MAX_MESSAGES
LOADER_VISIBILITY_TIMEOUT_SECS
LOADER_REQUEST_TIMEOUT_SECS
```

These controls let deployments tune request size, file rotation, local retention,
writer parallelism, flush latency, batch response size, and ClickHouse query
limits without changing code.

## Observability

`GET /metrics` exposes Prometheus text metrics for:

- configured writer lanes
- queued requests
- committed requests and events
- bytes written
- parse errors
- writer errors
- request body read time
- queue wait time
- serialization time
- file write time
- flush/sync time
- file rotation time
- end-to-end append time inside the server

These metrics make it clear whether pressure is coming from request bodies,
writer queues, serialization, disk writes, fsync, or rotation.

## Performance Notes

Load testing on one AWS node behind an ALB observed:

- Single-event requests: about 1.5k events/s under p95 <= 2s and error <= 1%.
- 10-event batches: about 8k events/s passed; roughly 10k target events/s failed
  by latency.
- 50-event batches: about 11k events/s passed.
- 100-event batches: about 12.4k achieved events/s passed.
- A 5.93M event run reached ClickHouse visibility with p95 downstream ingest
  lag around 71 seconds.

These are not formal benchmarks. They show that the architecture can absorb
large writes on a modest single-node deployment, and that batch ingest materially
improves event throughput.

## Tradeoffs

Nanotrace intentionally prioritizes durability, raw data ownership, and a simple
write path over exactly-once semantics.

Strengths:

- Raw events remain available even if ClickHouse is delayed or unavailable.
- The request path is short and robust.
- S3 storage provides a low-cost long-term event log.
- ClickHouse can be rebuilt from raw data if needed.
- Batch ingest supports high event throughput.

Known tradeoffs:

- ClickHouse visibility is asynchronous, not immediate.
- SQS retries can create duplicate ClickHouse rows.
- The current bearer-token model is simple and should be wrapped with stronger
  edge controls for production deployments.
- Raw NDJSON is intentionally uncompressed to support byte-range retrieval.
- Local EBS sizing and retention settings matter under sustained high load.

## Success Criteria

A deployment is behaving correctly when:

- `POST /events` returns receipts after local durable append.
- `.ready` files are uploaded to S3 and become `.done`.
- S3 object notifications reach SQS.
- Loader workers insert uploaded rows into ClickHouse.
- ClickHouse rows contain valid `source_file`, `source_offset`, and
  `source_length`.
- `GET /events/{event_id}` can retrieve the final uploaded event row from S3.
- `/metrics` shows stable queue depth, write errors, and append latency under
  expected load.

## Positioning

Nanotrace is best understood as an owned telemetry ingestion layer:

```text
Managed observability tools are the dashboard.
Nanotrace is the durable, controllable ingest and raw-data foundation.
```

It gives teams a way to keep raw telemetry in their own object storage, query it
with ClickHouse, and evolve processing logic without surrendering the ingest
path to a black-box vendor pipeline.
