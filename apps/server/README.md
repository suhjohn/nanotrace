Build

`POST /v1/events` publishes accepted request bodies to Kafka. The server does
not parse event JSON, write local event files, upload raw parts to S3, commit
Iceberg snapshots, or call ClickHouse on the HTTP request path. The normalizer
owns those asynchronous ingest writes.

Event write path

```text
POST /v1/events
-> validate API key from Authorization: Bearer ntak_...
-> enforce ingest:write scope
-> read the request body
-> produce the raw body to NANOTRACE_KAFKA_INGEST_TOPIC
-> return 202 with Kafka topic/partition/offset
```

Requests without a valid service/admin API key are rejected before Kafka
produce. The normalizer owns JSON parsing, tenant stamping, invalid-event
handling, typed SDK-managed metric definitions, and publishing the
Tableflow-ready Kafka topic. WarpStream Tableflow owns Iceberg writes.

Kafka message metadata

The server sets these headers on every produced batch:

```text
nanotrace-tenant-id
nanotrace-organization-id
nanotrace-received-at
nanotrace-schema-version
content-type
```

Client event contract

Clients send one JSON event object or a non-empty JSON array of event objects:

```text
event_id: non-empty string
timestamp: non-empty string
data: JSON object
```

The normalizer stamps `tenant_id` and `organization_id` into `data`, serializes
valid rows to the Tableflow Kafka topic, and sends invalid rows to the invalid
topic/table. Its Kafka offset advances only after the durable publishes for that
message succeed.

Runtime configuration

Required ingest settings:

```text
NANOTRACE_KAFKA_BROKERS
```

Common optional settings:

```text
PORT
NANOTRACE_KAFKA_INGEST_TOPIC
NANOTRACE_KAFKA_CLIENT_ID
NANOTRACE_KAFKA_PRODUCE_TIMEOUT_MS
NANOTRACE_KAFKA_SECURITY_PROTOCOL
NANOTRACE_KAFKA_SASL_MECHANISM
NANOTRACE_KAFKA_SASL_USERNAME
NANOTRACE_KAFKA_SASL_PASSWORD
DATABASE_URL
NANOTRACE_PUBLIC_BASE_URL
NANOTRACE_APP_BASE_URL
NANOTRACE_EMAIL_FROM
NANOTRACE_MAGIC_LINK_TTL_SECS
NANOTRACE_API_KEY_CACHE_REFRESH_SECS
NANOTRACE_GOOGLE_OAUTH_CLIENT_ID
NANOTRACE_GOOGLE_OAUTH_CLIENT_SECRET
NANOTRACE_GOOGLE_OAUTH_REDIRECT_URI
NANOTRACE_CORS_ALLOWED_ORIGINS
CLICKHOUSE_URL
CLICKHOUSE_USER
CLICKHOUSE_PASSWORD
CLICKHOUSE_DATABASE
CLICKHOUSE_TABLE
CLICKHOUSE_MAX_RESULT_ROWS
CLICKHOUSE_MAX_EXECUTION_SECS
CLICKHOUSE_MAX_BYTES_TO_READ
MAX_REQUEST_BYTES
```

Read APIs

`POST /v1/query` is the user-facing query surface. It is intentionally typed:
the body must include a `type` field and the fields for that query type. The
server injects tenant scope, applies ClickHouse read limits, checks serving
freshness unless `allowStaleServing` is set, records query usage, and may return
planner recommendations under the `nanotrace` response metadata.

Supported query types:

```text
events  Explore raw events, groups, density, latest rows, summaries, and flamegraphs.
search  Search indexed event text and terms.
measure Read promoted measure cube rollups.
funnel  Read sequence report results.
cohort  Read cohort memberships.
report  Read report result rows.
state   Read current entity state.
alerts  Read alert events or alert notifications.
```

Examples:

```json
{
  "type": "events",
  "view": "events",
  "filter": {
    "createdAfter": "2026-06-06T00:00:00Z",
    "createdBefore": "2026-06-06T01:00:00Z",
    "facets": [
      { "path": "service", "operator": "eq", "value": "api" },
      { "path": "duration_ms", "operator": "gte", "value": "1000" }
    ],
    "text": "timeout"
  },
  "limit": 100,
  "sort": { "direction": "desc" }
}
```

```json
{
  "type": "search",
  "query": "checkout timeout",
  "mode": "token",
  "requireAllTerms": true,
  "includeSnippets": true,
  "from": "2026-06-06T00:00:00Z",
  "to": "2026-06-06T01:00:00Z",
  "limit": 50
}
```

```json
{
  "type": "measure",
  "measureName": "checkout.latency",
  "from": "2026-06-06T00:00:00Z",
  "to": "2026-06-06T01:00:00Z",
  "bucketSeconds": 60,
  "groupBy": ["service"]
}
```

`GET /v1/events/{event_id}` reconstructs the event from ClickHouse serving
rows.

Definitions and backfills

Definitions are append/versioned control-plane rows. Creating a definition
inserts a new active row, deleting a definition inserts a disabled/deleted
version, and there is no in-place update endpoint. The server seeds SDK metric
defaults internally at startup for known organizations when ClickHouse is
configured.

```http
GET    /v1/definitions
GET    /v1/definitions/{definition_id}
POST   /v1/definitions
DELETE /v1/definitions/{definition_id}
```

Synchronous backfills are for field, measure/rollup, and state definitions:

```http
POST /v1/definitions/{definition_id}/backfill
```

Reports, sequences, and cohorts are definition kinds. To process historical
data for one of those definitions, create a queued backfill:

```http
POST /v1/definitions/{definition_id}/backfills
GET  /v1/backfills
GET  /v1/backfills/{job_id}
```

Internally these rows are stored as materialization jobs/chunks, but the public
API uses "backfill" because it is the user action: apply this definition to a
historical window.

Health

`GET /healthz` is liveness. `GET /readyz` reports the configured Kafka ingest
topic. `GET /metrics` exposes ops-only Prometheus text metrics for the server
process, including request counts/latency, accepted ingest bytes/batches,
structured query counts/latency, created backfill jobs, uptime, and API key
cache state. It is intentionally not part of the generated OpenAPI user surface
and should be ingress-restricted in production.
