# Nanotrace

Nanotrace is a unified event table for business analytics, observability, and
AI-agent debugging.

For the current architecture, event contract, query model, and operational
details, see [docs/design.html](docs/design.html). Hard system contracts live in
[docs/invariants.md](docs/invariants.md).

Nanotrace stores logs, spans, metrics, product actions, account state changes,
LLM calls, tool executions, retrieval steps, evals, and safety events as rows in
one queryable timeline. The goal is to make cross-system questions boring:

```text
Which customers hit this error after upgrading plans?
What did the agent see, decide, retrieve, and call before this bad answer?
Which model/tool path correlates with slow checkouts or failed workflows?
```

Features that distinguish Nanotrace:

- **One event model** for product behavior, infrastructure signals, workflow
  state, and AI-agent execution.
- **Raw JSON first**, so new questions do not require schema migrations.
- **Common fields** for time, tenant, signal, service, trace/span identity,
  name, duration, and error state.
- **Generic scalar filtering** through `event_kv_index` for unpromoted payload
  paths.
- **Promotion when queries repeat** for indexed fields, measures, rollups,
  reports, sequences, cohorts, and dashboards.
- **Streaming ingest** through Kafka, normalization, WarpStream Tableflow-ready
  batches, Iceberg, and ClickHouse serving tables.

## Architecture

```text
HTTP clients / SDKs / sidecar
  -> ingest server
  -> Kafka raw ingest topic
  -> normalizer
  -> Kafka Tableflow topic
  -> WarpStream Tableflow / Iceberg
  -> ClickHouse serving tables
  -> query API, UI, alerts, reports, and materialized definitions
```

ClickHouse is the interactive serving layer. Raw `events` are the correctness
fallback; text, term, KV, field, rollup, report, sequence, cohort, and state
tables make repeated reads cheap.

## Local Development

Node commands should use Node `22.17.1` through NVM:

```sh
export NVM_DIR="$HOME/.nvm"
. "$NVM_DIR/nvm.sh"
nvm use 22.17.1
```

Start the local stack:

```sh
npm run dev:up:detached
npm run dev:seed-auth
```

Local services:

- UI: <http://localhost:41233>
- API: <http://localhost:18473>
- ClickHouse: <http://localhost:18123>
- Dev API key: `ntak_dev`

Reset and repopulate ClickHouse through the normal ingest path:

```sh
npm run dev:rewrite:loadtest
```

Stop or reset the stack:

```sh
npm run dev:down
npm run dev:reset
```

## Checks

```sh
cargo fmt
cargo clippy --all-targets --all-features
cargo test --all-features
npm run typecheck
```

Run the Kafka integration path:

```sh
npm run integration:kafka
```

## Repository Layout

```text
apps/server        Rust HTTP ingest, auth, and query API
apps/normalizer    Rust Kafka consumer that validates and normalizes events
apps/alerts        Rust alert matcher and webhook notifier
apps/query         Rust read/query service
apps/sidecar       UDP/HTTP sidecar for non-blocking event forwarding
apps/ui            Web UI
crates/*           Shared Rust crates
packages/sdk-js    TypeScript SDK
packages/sdk-py    Python SDK
tools/loadtest     Deploy-aware load tester
tools/lakehouse-rebuild  Local Tableflow materializer and replay worker
deploy/clickhouse  ClickHouse schema
deploy/pulumi      AWS infrastructure
scripts            Development, deploy, and E2E commands
```

## More Information

- [docs/design.html](docs/design.html): canonical system design.
- [docs/invariants.md](docs/invariants.md): hard correctness contracts.
- [apps/server/README.md](apps/server/README.md): ingest and query API notes.
- [apps/sidecar/README.md](apps/sidecar/README.md): UDP/HTTP sidecar.
- [packages/sdk-js/README.md](packages/sdk-js/README.md): TypeScript SDK.
- [packages/sdk-py/README.md](packages/sdk-py/README.md): Python SDK.
- [.env.example](.env.example): environment variables for local and deploy
  workflows.
