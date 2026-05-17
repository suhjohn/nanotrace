# Nanotrace

Nanotrace is a single-tenant observability ingest pipeline built around one
`/events` API, S3, SQS, a Rust loader, and ClickHouse.

The current AWS deployment path is:

```text
HTTP clients -> ALB -> EC2/EBS ingest servers -> S3 -> SQS -> loader -> ClickHouse
```

The server validates event JSON, writes durable local NDJSON parts, uploads
closed parts to S3, and a loader service consumes S3 notifications to insert
rows into ClickHouse.

## Layout

```text
apps/server        Rust ingest server and raw S3 uploader
apps/loader        Rust S3/SQS to ClickHouse loader
apps/query         Rust stateless read/query service
apps/sidecar      UDP sidecar client that batches and forwards events
packages/sdk-js    TypeScript SDK for app instrumentation
tools/loadtest     Rust deploy-aware load testing tool
deploy/clickhouse  ClickHouse schema
deploy/pulumi/nanotrace  Pulumi EC2/EBS/S3 infrastructure
scripts            E2E commands
```

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
that queue, fetch raw NDJSON objects from S3, and insert JSONEachRow batches into
`observatory.events`. Processors are shared for the deployment under the
`processors` S3 prefix. The uploaded row fields match
[deploy/clickhouse/schema.sql](/Users/johnsuh/nanotrace/deploy/clickhouse/schema.sql).

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
`POST /events` on the configured Nanotrace URL.

```sh
NANOTRACE_URL=http://nanotrace-prod-alb.example.com \
NANOTRACE_KEY=... \
cargo run -p nanotrace-client
```

Default UDP bind address is `127.0.0.1:4319`. See
[apps/sidecar/README.md](/Users/johnsuh/nanotrace/apps/sidecar/README.md) for
batching and retry settings.
