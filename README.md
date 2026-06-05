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
telemetry and business facts in the same Kafka-ingested, ClickHouse-served event
table. Common fields make events easy to scan and join; raw JSON keeps
domain-specific context intact. A generic KV index makes scalar payload fields
filterable immediately; explicit promoted definitions are still required for
fast grouping, aggregation, reports, and reusable dashboards.

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
- **Universal scalar filtering:** flatten scalar KVs into `event_kv_index` so
  exact and numeric filters over arbitrary payload paths work without promoting
  the field into a semantic definition.
- **Promotion when queries repeat:** turn useful JSON paths into indexed fields,
  measures, state updates, rollups, reports, or dashboards when they need to be
  grouped, aggregated, reused, or made predictably cheap.
- **Durable ingest path:** accept batches through HTTP, publish them to Kafka,
  normalize asynchronously, commit valid rows to Iceberg, and serve interactive
  queries from ClickHouse.

## Canonical Docs

The concise current-state architecture spec is
[docs/design.html](docs/design.html). It consolidates the ingest path, event
contract, serving tables, query planner, materialization model, operational
commands, validation coverage, and open boundaries. The markdown files under
`docs/` remain deeper references for specific design areas, with
[docs/invariants.md](docs/invariants.md) capturing the hard system contracts.

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

The core design is streaming-first. Kafka is the ingest buffer and replay point;
Iceberg is the durable lakehouse record for accepted normalized events; and
ClickHouse is the interactive serving layer.

## Data Path

The production data path is:

```text
HTTP clients
  -> ALB
  -> ingest server
  -> Kafka raw ingest topic
  -> normalizer
  -> Iceberg lakehouse commit
  -> alert worker for hot event-match alerts and webhook outbox
  -> serving materializer
  -> ClickHouse raw rows, event_text_index rows, event_search_terms rows, and event_kv_index rows
  -> promoted serving indexes, definitions, alert events, reports, sequences, and cohorts
```

The server authenticates the request and produces the raw request body to Kafka.
The normalizer consumes that topic, validates and tenant-stamps events, writes
valid rows to Iceberg, and records invalid rows separately. ClickHouse serving
rows are loaded from committed lakehouse snapshots by the materializer/rebuild
worker; the normalizer does not write raw serving rows or serving indexes.
Semantic definitions come from typed SDK-managed defaults or user/admin
definitions, not from observed arbitrary payload shape. The Kafka consumer
offset advances only after the durable ingest work for that message succeeds.
SDK metric defaults are tenant bootstrap data: the server seeds them
idempotently at startup for known organizations when ClickHouse is configured,
not through a public `/v1/definitions/sdk-defaults` endpoint.

## Repository Layout

```text
apps/server        Rust HTTP ingest API that produces raw batches to Kafka
apps/normalizer    Rust Kafka consumer that normalizes events into Iceberg
apps/alerts        Rust Kafka consumer/notifier for hot alert matches and webhooks
apps/query         Rust stateless read/query service
apps/sidecar      UDP sidecar client that batches and forwards events
packages/sdk-js    TypeScript SDK for app instrumentation
tools/loadtest     Rust deploy-aware load testing tool
tools/lakehouse-rebuild  Lakehouse replay and materialization worker
deploy/clickhouse  ClickHouse schema
deploy/pulumi/nanotrace  Pulumi EC2/S3/ECR/RDS/KMS infrastructure
scripts            E2E commands
```

## How It Works

Nanotrace has three operational layers:

- Ingest: HTTP servers publish raw accepted batches to Kafka.
- Normalize: the normalizer validates events, stamps tenancy, emits invalid
  rows, and commits valid rows to Iceberg.
- Serving: ClickHouse stores raw rows, rollups, indexes, definitions, reports,
  sequences, and cohorts for interactive reads. The materializer
  loads raw and derived serving rows from lakehouse commits.

The canonical event table keeps a stable flattened vocabulary for common fields:
tenant, event id, timestamps, signal, event type, trace/span ids, service,
environment, name, duration, error state, and the raw `data` JSON. Arbitrary
customer fields stay in `data` and are also flattened into `event_kv_index` for
exact/range filtering. Promotion is reserved for fields and measures that need
semantic materialization.

Kafka-sourced rows use `kafka://<topic>/<partition>/<offset>` source pointers.
Event reads are reconstructed from ClickHouse serving rows.

## Query and Index Model

ClickHouse is used as the serving/index engine. Query planning follows this
ladder:

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
- `event_text_index`: bounded per-event search documents for event text search.
- `event_search_terms`: token-level inverted index for ranked interactive
  search over event text and common identifiers. `/v1/query` `type=search`
  defaults to token ranking, can run prefix or bounded fuzzy term matching, can
  require all query terms, can return bounded snippets from `event_text_index`,
  and can run phrase searches through the text-document index.
- `event_kv_index`: generic inverted index for arbitrary scalar payload paths.
  This is the default substrate for equality, set membership, existence, and
  numeric range filters over unpromoted KVs, including nested paths and
  array-object correlation.
- `field_index`: definition-backed promoted-field index. Materializers or
  definition backfills populate this for fields that need guaranteed-fast
  filtering, grouping, or drilldown.

The UI filter DSL supports equality, inequality, negation, disjunction,
substring matching, and set membership:

```text
service=llm-gateway
http.route=/v1/responses OR http.route=/v1/chat/completions
NOT environment=dev
request_id IN [req_1, req_2, req_3]
message CONTAINS timeout
```

Unpromoted equality, existence, set, and numeric range filters use
`event_kv_index` semi-joins by `event_id`. Promoted field filters can use
`field_index` only when the planner has a freshness proof for the promoted
definition/version or the lakehouse serving watermark says the whole index is
current. Those generated promoted-index reads are constrained by
`definition_id` and `definition_version`, not just field name. Event text
filters use bounded `event_text_index` documents; ranked interactive search uses
`event_search_terms` through `/v1/query` with `type=search`, with prefix,
bounded fuzzy, phrase, and snippet support backed by `event_text_index` when
needed. Field-specific
substring predicates remain bounded fallback work. Repeated broad queries should
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

Local Compose runs Redpanda, Postgres, ClickHouse, Localstack, the ingest
server, the normalizer, the materializer, and the UI. The default path is the
same one used by the Kafka integration test: HTTP events are accepted by
`nanotrace-server`, produced to Redpanda, consumed by `nanotrace-normalizer`,
committed to the local Iceberg lakehouse Docker volume, then loaded into
ClickHouse by `nanotrace-lakehouse-rebuild` running in materializer-loop mode.

The lakehouse rebuild and materialization tools replay committed lakehouse
snapshots back into ClickHouse serving tables. To rebuild serving rows from the
current lakehouse source, run:

```sh
npm run lakehouse:rebuild:local
```

The rebuild command restores raw `events` and materializes `event_text_index`,
`event_search_terms`, `event_kv_index`, and other serving outputs. When active definitions exist, it
also materializes promoted `field_index`,
`event_measures`, metric rollups, `entity_state_updates`,
`entity_state_current`, reports, sequences, and cohorts from the same lakehouse
snapshots. It refuses to
rewrite raw or derived rows into non-empty ClickHouse serving tables unless
`NANOTRACE_REBUILD_TRUNCATE=true` or `NANOTRACE_REBUILD_ALLOW_NON_EMPTY=true`.
Set `NANOTRACE_REBUILD_RAW=false` to run materialization only, or
`NANOTRACE_REBUILD_DERIVED=false` to skip derived serving tables. Data files may
be local `file://` paths or remote `s3://`/`s3a://` paths; S3 rebuilds use the
standard AWS environment and cap each fetched Parquet file with
`NANOTRACE_REBUILD_S3_MAX_FILE_BYTES`.

For normal catch-up after lakehouse commits, run the same command with
`NANOTRACE_MATERIALIZE_INCREMENTAL=true NANOTRACE_REBUILD_RAW=false`.
For explicit historical report, sequence, or cohort backfills, create
tenant-scoped jobs with `POST /v1/definitions/{definition_id}/backfills`; the
queued materializer claims the generated `materialization_chunks` rows and
publishes versions and watermarks when chunks complete.
Field, measure/rollup, and state definitions use synchronous
`POST /v1/definitions/{definition_id}/backfill`. Definitions are append/versioned:
create inserts an active row, delete inserts a disabled/deleted row, and there
is no in-place update endpoint.
For offline historical export or audit scans, run `nanotrace-lakehouse-rebuild`
with `NANOTRACE_LAKEHOUSE_QUERY=true`. It reads committed lakehouse Parquet
files directly, supports tenant/time/event-type/text/regex filters, and emits
matching events as NDJSON without using ClickHouse serving tables. For
SQL-shaped historical analysis, set `NANOTRACE_LAKEHOUSE_QUERY_SQL`; the tool
registers committed event Parquet files as the `events` table and can register
additional Parquet inputs from `NANOTRACE_LAKEHOUSE_QUERY_TABLES` for offline
joins.

Lakehouse maintenance mode can audit commit/data-file/small-file pressure and
publish `pipeline_metrics`. For local filesystem catalogs, setting
`NANOTRACE_LAKEHOUSE_NATIVE_COMPACTION=true` rewrites the current Iceberg
snapshot into compacted Parquet files without writing a new Nanotrace append
commit record, so ClickHouse materialization does not replay compacted rows as
new data. REST/object-store deployments can still provide
`NANOTRACE_ICEBERG_MAINTENANCE_CMD` for engine-specific compaction, snapshot
expiry, and orphan cleanup.

Query paths check `observatory.serving_watermarks` before reading lakehouse-fed
serving tables and use `observatory.materialization_watermarks` for
definition-scoped backfills. Requests that intentionally accept stale serving
data can set `allow_stale_serving: true` in the `/v1/query` JSON body.
`POST /v1/query` is the user-facing structured query API and remains
tenant-scoped. Cross-tenant or SQL-shaped historical analysis should use
operator-controlled lakehouse export/query tooling, not a public server route.

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
NANOTRACE_KAFKA_BROKERS=...
NANOTRACE_KAFKA_SECURITY_PROTOCOL=SASL_SSL
NANOTRACE_KAFKA_SASL_MECHANISM=SCRAM-SHA-512
NANOTRACE_KAFKA_SASL_USERNAME=...
NANOTRACE_KAFKA_SASL_PASSWORD=...
NANOTRACE_ICEBERG_REST_URI=https://...
```

Pulumi derives `NANOTRACE_ICEBERG_WAREHOUSE` from the provisioned storage
bucket and the `iceberg/` prefix unless `nanotrace:icebergWarehouse` or
`NANOTRACE_ICEBERG_WAREHOUSE` is set explicitly. The deployed write path is
WarpStream-compatible Kafka to the normalizer, then Iceberg object storage for
accepted normalized events. A separate materializer container tails lakehouse
commits and keeps ClickHouse serving tables current.

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
through the ALB, waits for the event to appear in ClickHouse, verifies
`event_kv_index`, checks serving watermarks, and exercises the public query
path:

```sh
npm run e2e:pulumi
```

Run the deploy-aware Rust load test to find the max sustainable request rate
for configured batch sizes. It reads the schema-shaped JSON fixtures from
`fixtures/events`, uses a 10% log / 90% rest event mix, and generates fresh
`event_id`, `timestamp`, and `_loadtest` metadata for each event:

```sh
npm run loadtest:pulumi
```

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
materializer activity, and realistic logs over a 60-day history. The Codex profile
emits correlated 24-event workflows rather than independent samples, so traces
look like actual request lifecycles: span start/end, agent planning, retrieval,
LLM requests with message and usage payloads, tool execution, evaluation,
safety, runtime metrics, and terminal logs. It can also be `atlas` for the
older Atlas Markets mix, `realistic` for generic mixed fixture replay,
`product` for product/state analytics, `agent` for deep agent traces, `trace`
for trace-shaped events, `pipeline` for materializer/pipeline events, `metrics` for pure
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

The AWS deployment runs `nanotrace-server` and `nanotrace-normalizer` on ingest
EC2 instances. Set `nanotrace:kafkaBrokers` or `NANOTRACE_KAFKA_BROKERS` before
deploying; Pulumi passes the broker list and topic names into both containers.
Rows are served from ClickHouse using the schema in
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
