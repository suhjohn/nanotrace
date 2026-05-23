# Nanotrace

A unified event table for business analytics, observability, and AI-agent
debugging.

Nanotrace is built around one simple idea: the easiest system to query is the
one where every meaningful fact lands in the same event model. Logs, spans,
metrics, product actions, account state changes, LLM calls, tool executions,
retrieval steps, evals, and safety events all become rows in one queryable
timeline.

That gives humans and AI agents a single way to ask questions across the full
state of the system:

```text
Which customers hit this error after upgrading plans?
What did the agent see, decide, retrieve, and call before this bad answer?
Which model/tool path correlates with slow checkouts or failed workflows?
What changed in account state before support tickets, latency, or churn spiked?
```

Instead of stitching together separate logging, tracing, metrics, product
analytics, warehouse, and agent-evaluation systems, Nanotrace stores operational
telemetry and business facts in the same lakehouse-backed event table. Common
fields make events easy to scan and join; raw JSON keeps domain-specific context
intact until a field is important enough to promote into a fast serving index.

## What Nanotrace Offers

- **One query surface for the whole system:** query product behavior,
  infrastructure signals, workflow state, and AI-agent execution through the
  same event model.
- **AI-debuggable context:** preserve the timeline an agent needs: prompt and
  model activity, tool calls, retrieval, decisions, traces, errors, customer
  state, and business outcomes.
- **Unified high-cardinality events:** accept structured JSON over HTTP, SDKs,
  or a local sidecar, with stable fields for time, tenant, signal, service,
  trace/span identity, name, duration, and error state.
- **Raw-first analytics:** keep arbitrary customer and application fields in
  `data` by default, so new questions do not require a schema migration or a new
  pipeline.
- **Promotion when queries repeat:** turn useful JSON paths into indexed fields,
  measures, state updates, rollups, reports, or dashboards when they need to be
  fast.
- **Open durable history:** commit accepted event batches to Apache
  Iceberg/Parquet, then use ClickHouse as a rebuildable serving layer for
  interactive queries.

## Why A Unified Table

Most analytics and observability stacks split reality across specialized
stores. Logs answer one class of question, traces another, metrics another, and
business analytics another. That separation is painful for people, and it is
especially painful for AI agents that need complete context to debug behavior.

Nanotrace treats everything as an event first. A checkout failure, a span end, a
tool call, a token-usage record, an account-plan update, and an evaluation score
can be filtered, grouped, and correlated with the same language. You can start
from a business symptom and drill into infrastructure, or start from an error
and climb back up to the customer, workflow, model, account, and outcome it
affected.

The core design is lakehouse-first. ClickHouse makes the UI fast, but Iceberg is
the durable record. If an index, rollup, or promoted table changes, Nanotrace can
replay committed snapshots and produce a new serving view.

## Data Path

The production data path is:

```text
HTTP clients
  -> ALB
  -> EC2/EBS ingest servers
  -> S3 landing objects
  -> SQS
  -> loader
  -> Iceberg/Parquet commit
  -> ClickHouse raw serving rows + commit metadata
  -> materializer
  -> promoted serving tables and rollups
```

The server validates event JSON, writes durable local NDJSON parts, and uploads
closed parts to S3. The loader consumes S3 notifications, commits the batch to
the Iceberg event table, records the lakehouse snapshot in ClickHouse, and then
loads the committed events into ClickHouse serving tables. A separate
materializer keeps derived serving tables caught up from the same lakehouse
commit stream.

## Repository Layout

```text
apps/server        Rust ingest server and raw S3 uploader
apps/loader        Rust S3/SQS to ClickHouse loader
apps/query         Rust stateless read/query service
apps/sidecar      UDP sidecar client that batches and forwards events
packages/sdk-js    TypeScript SDK for app instrumentation
tools/loadtest     Rust deploy-aware load testing tool
tools/lakehouse-rebuild  Lakehouse replay and materialization worker
deploy/clickhouse  ClickHouse schema
deploy/pulumi/nanotrace  Pulumi EC2/EBS/S3 infrastructure
scripts            E2E commands
```

## How It Works

Nanotrace has three durable layers:

- Landing: ingest servers write accepted batches to local disk first, then S3.
- Lakehouse: the loader writes canonical Parquet files and commits them through
  Apache Iceberg table metadata.
- Serving: ClickHouse stores raw rows, rollups, indexes, and watermarks for
  interactive reads.

The canonical event table keeps a stable flattened vocabulary for common fields:
tenant, event id, timestamps, signal, event type, trace/span ids, service,
environment, name, duration, error state, and the raw `data` JSON. Arbitrary
customer fields stay in `data` until they become useful enough to promote.

Each Iceberg commit is mirrored into `observatory.lakehouse_commits`. Each
ClickHouse serving table records the latest source snapshot it has consumed in
`observatory.serving_watermarks`. Query APIs compare those watermarks before
reading raw or promoted serving tables, so stale serving data is rejected by
default. Diagnostic callers can opt in with `allow_stale_serving: true`.

## Query and Index Model

ClickHouse is used as a serving/index engine on top of Iceberg, not as the
system of record. Query planning follows this ladder:

- `events`: raw serving rows. This is the correctness fallback for every query.
  Time-window and tenant predicates use the table order key for pruning.
- `event_density_1s`: global histogram rollup used when the UI has no selected
  group and no restrictive filter.
- `field_rollups`: always-on grouped histograms and top-value lists for the
  small core dimension set: `signal`, `event_type`, `name`, `service`,
  `environment`, and `is_error`.
- `field_values`: exact lookup index for common identifiers such as `trace_id`,
  `span_id`, `request_id`, `user_id`, `anonymous_id`, `account_id`,
  `session_id`, `group_id`, `organization_id`, `thread_id`, and
  `conversation_id`.
- `field_index`: definition-backed promoted-field index. Materializers populate
  this for fields that need guaranteed-fast filtering, grouping, or drilldown.

The UI filter DSL supports equality, inequality, negation, disjunction,
substring matching, and set membership:

```text
service=llm-gateway
http.route=/v1/responses OR http.route=/v1/chat/completions
NOT environment=dev
request_id IN [req_1, req_2, req_3]
message CONTAINS timeout
```

Simple indexed equality filters can use `field_index` semi-joins by `event_id`.
Advanced boolean filters, `CONTAINS`, and unpromoted JSON paths fall back to raw
`events` predicates so the result stays correct. Repeated broad queries should
become a promoted field, measure, report, cohort, or another materialized output
rather than repeatedly scanning arbitrary JSON.

The canonical serving terminology and enablement rules live in
`docs/design.html` under "Query and UI Behavior".

The API server generates its OpenAPI document from annotated Axum handlers and
`ToSchema` request/response types, then publishes it at `/openapi.json`.

## Toolchains

- Rust 1.85 or newer.
- Node 24 or newer.
- Docker and the AWS CLI for AWS deployment.
- Pulumi CLI for infrastructure deployment.

## Local Checks

```sh
cargo fmt
cargo clippy --all-targets --all-features
cargo test --all-features
```

## Local Rewrite

Bring up the local stack, drop/recreate the ClickHouse schema, and repopulate
it through the normal ingest path with the loadtester:

```sh
npm run dev:up:detached
npm run dev:rewrite:loadtest
```

`dev:rewrite:loadtest` runs the schema applicator with
`CLICKHOUSE_RESET_DATABASE=true`, then posts generated events to
`http://localhost:18473` and waits until the accepted `_loadtest.run_id` rows
are visible in local ClickHouse at `http://localhost:18123`. Override
`NANOTRACE_LOADTEST_TOTAL_EVENTS`, `NANOTRACE_LOADTEST_BATCH_SIZES`,
`NANOTRACE_LOADTEST_PROFILE`, or `NANOTRACE_LOADTEST_RUN_ID` to change the
rewrite size and shape.

Local Compose enables the lakehouse writer by default. The loader commits each
processed event batch to a local Apache Iceberg V2 table under
`/data/lakehouse/nanotrace/events`, then loads the same committed batch into
ClickHouse serving tables. ClickHouse stores commit and serving watermark rows
in `observatory.lakehouse_commits` and `observatory.serving_watermarks`.
After a ClickHouse reset, refill serving rows from the committed lakehouse files
with:

```sh
npm run lakehouse:rebuild:local
```

The rebuild command restores raw `events` and, when active definitions exist,
materializes promoted `field_index`, `event_measures`, and
`entity_state_updates` rows from the same lakehouse snapshots. It refuses to
rewrite raw or derived rows into non-empty ClickHouse serving tables unless
`NANOTRACE_REBUILD_TRUNCATE=true` or `NANOTRACE_REBUILD_ALLOW_NON_EMPTY=true`.
Set `NANOTRACE_REBUILD_RAW=false` to run materialization only, or
`NANOTRACE_REBUILD_DERIVED=false` to skip derived serving tables. Data files may
be local `file://` paths or remote `s3://`/`s3a://` paths; S3 rebuilds use the
standard AWS environment and cap each fetched Parquet file with
`NANOTRACE_REBUILD_S3_MAX_FILE_BYTES`.

For normal catch-up after new lakehouse commits, run the same command with
`NANOTRACE_MATERIALIZE_INCREMENTAL=true NANOTRACE_REBUILD_RAW=false`. That mode
uses `observatory.serving_watermarks` to materialize only promoted serving
tables that are behind the latest committed Iceberg sequence, so it can run
against non-empty derived tables without a destructive refill.

Local Compose also includes a long-running materializer service:

```sh
npm run lakehouse:materializer:local
```

The service runs `nanotrace-lakehouse-rebuild` with
`NANOTRACE_MATERIALIZE_LOOP=true`, reloads active definitions every poll, and
continuously advances promoted serving watermarks from committed lakehouse
snapshots. Set `NANOTRACE_MATERIALIZE_POLL_SECS` to tune its cadence. In
service mode it can use `NANOTRACE_REBUILD_COMMIT_SOURCE=clickhouse` so workers
read shared `observatory.lakehouse_commits` metadata instead of local sidecar
files. Commit metadata stores the full Iceberg data-file list for each
snapshot, so materializers replay every file when a high-throughput commit rolls
over into multiple Parquet parts.

Lakehouse tables are created and kept current with production-oriented Iceberg
properties for ZSTD Parquet writes, target file sizing, metadata cleanup, and
snapshot-retention policy. Tune these with
`NANOTRACE_ICEBERG_TARGET_FILE_SIZE_BYTES`,
`NANOTRACE_ICEBERG_MIN_SNAPSHOTS_TO_KEEP`,
`NANOTRACE_ICEBERG_MAX_SNAPSHOT_AGE_MS`, and
`NANOTRACE_ICEBERG_METADATA_PREVIOUS_VERSIONS_MAX`.

When `NANOTRACE_POSTGRES_URL` is configured, the loader also uses a Postgres
`nanotrace_ingest_batches` ledger keyed by the deterministic S3 batch token. A
concurrent loader that sees the same batch will either skip it if completed or
leave the SQS message for the active owner before downloading S3 objects; stale
processing rows can be reclaimed after `NANOTRACE_INGEST_LEDGER_STALE_SECS`.

Query APIs check `observatory.serving_watermarks` before reading raw or promoted
serving tables. Requests that intentionally accept stale serving data can set
`allow_stale_serving: true` in the `/v1/query` JSON body.

See [docs/iceberg-final-spec.md](docs/iceberg-final-spec.md) for the
Iceberg-native final design and current implementation slice.

## AWS Quickstart

Deploy commands read only the process environment. Inject variables before
running them with your shell, password manager, or secret manager, for example
`infisical run --`, `op run --`, or `set -a && source .env && set +a`.

```text
AWS_REGION=us-west-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
NANOTRACE_API_KEY=ntak_...
NANOTRACE_EMAIL_FROM=nanotrace@example.com
NANOTRACE_ALLOWED_EMAILS=alice@company.com,*@company.com,/^.+@engineering\\.company\\.com$/
NANOTRACE_ADMIN_EMAILS=alice@company.com
CLICKHOUSE_URL=https://...
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=...
CLICKHOUSE_DATABASE=observatory
```

Browser login uses one-time email links sent through AWS SES. The sender in
`NANOTRACE_EMAIL_FROM` must be a verified SES identity in the deployment
region; if the account is still in the SES sandbox, recipients must be verified
too.

Deploy the ingest service:

```sh
npm run deploy:up
# or
infisical run -- npm run deploy:up
# or
op run -- npm run deploy:up
```

Run the deploy-aware E2E after `deploy:up`. It reads Pulumi outputs, posts
through the ALB, waits for the
returned S3 object key, and verifies the uploaded NDJSON row:

```sh
npm run e2e:pulumi
```

Run the deploy-aware Rust load test to find the max sustainable request rate
for 1, 10, and 100 events per request. It reads the schema-shaped JSON fixtures
from `fixtures/events`, uses a 10% log / 90% rest event mix, and generates fresh
`event_id`, `timestamp`, and `_loadtest` metadata for each event:

```sh
npm run loadtest:pulumi
```

To originate load from Modal instead of the local machine, set
`NANOTRACE_INGEST_URL` and `NANOTRACE_API_KEY`, then run:

```sh
npm run loadtest:modal
```

The Modal wrapper starts `NANOTRACE_LOADTEST_GENERATORS` sandboxes, uploads this
repo, and runs the same Rust loadtester in each sandbox. Event sequences are
sharded with `NANOTRACE_LOADTEST_SEQUENCE_OFFSET` and
`NANOTRACE_LOADTEST_SEQUENCE_STRIDE` so generators can share one run id without
colliding event ids.

Useful knobs:

```sh
NANOTRACE_LOADTEST_BATCH_SIZES=1,10,100
NANOTRACE_LOADTEST_PROFILE=atlas
NANOTRACE_LOADTEST_TOTAL_EVENTS=1000
NANOTRACE_LOADTEST_STEP_SECONDS=30
NANOTRACE_LOADTEST_MAX_RPS=2000
NANOTRACE_LOADTEST_MAX_P95_MS=2000
NANOTRACE_LOADTEST_MAX_ERROR_RATE=0.01
NANOTRACE_LOADTEST_CLICKHOUSE_WAIT_MS=300000
NANOTRACE_LOADTEST_CLICKHOUSE_POLL_MS=5000
NANOTRACE_LOADTEST_GENERATORS=4
```

`NANOTRACE_LOADTEST_PROFILE` defaults to `codex`, which produces a Codex-like
mix of agent traces, LLM calls, tool calls, retrieval steps, runtime metrics,
processor activity, and realistic logs over a 60-day history. The Codex profile
emits correlated 24-event workflows rather than independent samples, so traces
look like actual request lifecycles: span start/end, agent planning, retrieval,
LLM requests with message and usage payloads, tool execution, evaluation,
safety, runtime metrics, and terminal logs. It can also be `atlas` for the
older Atlas Markets mix, `realistic` for generic mixed fixture replay,
`product` for product/state analytics, `agent` for deep agent traces, `trace`
for trace-shaped events, `processor` for pipeline events, `metrics` for pure
metric traffic, `logs` for pure log traffic, `llm` for LLM log traffic, or
`fixture` for mostly static fixture replay.
Synthetic non-fixture profiles generate timestamps across a fixed 60-day
history with weighted business-hour traffic and trace-local event spacing.
Set `NANOTRACE_LOADTEST_TOTAL_EVENTS` when you want a fixed number of generated
events instead of the normal timed RPS search.

If `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, and `CLICKHOUSE_PASSWORD` are set, the
load test waits for all accepted events for the run to become visible in
ClickHouse and reports visible event count plus ingest-lag percentiles. If
Pulumi secrets are locked, set `NANOTRACE_INGEST_URL` directly to avoid reading
Pulumi outputs.

The AWS deployment runs `nanotrace-loader` beside `nanotrace-server` on each EC2
instance. S3 sends object-created notifications to SQS; loader instances share
that queue, fetch raw NDJSON objects from S3, commit the batch to Iceberg, and
load the committed rows into ClickHouse serving tables. Processors are shared
for the deployment under the `processors` S3 prefix. The uploaded row fields
match the raw-first analytics schema in
[deploy/clickhouse/schema.sql](deploy/clickhouse/schema.sql).

Destroy AWS resources:

```sh
npm run deploy:destroy
```

Resource names use `nanotrace-<Pulumi stack name>` unless overridden with
Pulumi config `nanotrace:name` or `nanotrace:deploymentId`.

## UDP Client

Run `nanotrace-client` beside an application server when the application should
avoid blocking on the Nanotrace HTTP ingest path. The app sends event JSON to a
local UDP socket; the client batches events and forwards them to
`POST /v1/events` on the configured Nanotrace URL.

```sh
NANOTRACE_URL=http://nanotrace-prod-alb.example.com \
NANOTRACE_KEY=... \
cargo run -p nanotrace-client
```

Default UDP bind address is `127.0.0.1:4319`. See
[apps/sidecar/README.md](apps/sidecar/README.md) for batching and retry
settings.
