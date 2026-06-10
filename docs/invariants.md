# Nanotrace Invariants

This document defines the product and system invariants that code changes must
preserve. If implementation and this document disagree, fix the implementation
or update this document in the same change that changes the contract.

The sections are organized by ownership boundary. Each invariant should live in
one section only; cross-boundary flows can be referenced, but the owning
component's section states the contract.

## Identity, Auth, And Tenant Scope

- Every persisted event, control-plane row, and queryable serving row that can
  expose customer data must belong to exactly one tenant.
- `tenant_id` maps to the authenticated organization identity. Client-supplied
  event data may not choose or override the persisted tenant.
- API keys and sessions authenticate an identity before route handlers perform
  work. Route handlers must enforce the required scope for the operation:
  `ingest:write` for ingest, `query:read` for reads, `definitions:write` for
  definition mutations, and `api_keys:write` for API-key mutations.
- Definition and API-key mutations are admin operations in addition to requiring
  their write scopes.
- Normal user reads are tenant-scoped. They must not be able to read another
  tenant's event, definition, materialization, alert, query-usage, or API-key
  data.
- Cross-tenant analytics and raw lakehouse scans are operator-controlled paths,
  not public tenant query APIs.
- Postgres remains the source of truth for organizations, users, sessions,
  magic links, and API keys. In-process auth caches may accelerate validation,
  but they are not the durable catalog.

## Client And Sidecar Delivery

- SDK clients and the sidecar protect caller latency by batching and forwarding
  asynchronously. Client-side acceptance is not central persistence.
- With `NANOTRACE_CLIENT_SPOOL_DIR`, sidecar HTTP acceptance means the event was
  written to the local disk spool. Without that setting, acceptance means the
  event entered the in-memory queue.
- A sidecar `202` means the local sidecar accepted the event for asynchronous
  forwarding. It does not guarantee that the central ingest API, Kafka,
  Iceberg, or ClickHouse has accepted the event.
- UDP intake is best-effort and should avoid oversized datagrams. HTTP
  transports may post batches.
- Failed sidecar sends restore spool files where possible, retry according to
  configuration, then drop or quarantine only with observable metrics/logs.
- Drop counts, retry exhaustion, upstream send failures, spool replay, and spool
  errors must be observable before the sidecar is described as
  production-durable.

## Ingest API And Kafka Acceptance

- `/v1/events` authenticates `ingest:write` before accepting event data.
- The HTTP ingest server produces the raw request body to Kafka/Redpanda. It
  does not write raw events directly to ClickHouse or Iceberg.
- A central `/v1/events` `202` means the raw request body was accepted by the
  Kafka ingest buffer. It does not mean the rows are normalized, committed to
  Iceberg, materialized to ClickHouse, or query-visible.
- Kafka source pointers are the replay boundary for ingest, using
  `kafka://<topic>/<partition>/<offset>` semantics.
- The ingest API must stamp or preserve authenticated tenant context for the
  normalizer; downstream normalization must not infer tenancy from arbitrary
  payload fields.

## Normalization And Event Validity

- The normalizer is responsible for converting raw Kafka messages into
  validated, tenant-stamped Nanotrace events.
- Valid normalized rows are committed to the Iceberg lakehouse before the
  corresponding raw Kafka offset is committed.
- Invalid events are retained in `invalid_events` with tenant, timestamp,
  reason, and raw body context. Invalid handling for a message must complete
  before the raw Kafka offset advances.
- The normalizer emits normalized and invalid topics only after the relevant
  validation work for the batch succeeds.
- Normalizer replay is at-least-once. Consumers must treat
  `(tenant_id, event_id)` as the application identity, not as a physical
  exactly-once storage guarantee.
- Managed metric definitions discovered from metric events may be inserted by
  the normalizer, but arbitrary observed payload shape must not become promoted
  semantics without an SDK-managed or user/admin definition.

## Lakehouse Event Truth

- Iceberg/lakehouse storage is the durable normalized event record for valid
  accepted events. ClickHouse is a serving/index plane, not the source of truth.
- Lakehouse commits use deterministic `nanotrace.source-batch-id` values
  derived from Kafka source pointers so redelivery can return the existing
  snapshot instead of appending duplicate data files.
- `lakehouse_commits` records committed snapshots and must preserve enough
  metadata for replay: namespace, table, snapshot, sequence, record count,
  metadata location, source batch id, dedupe flag, and the full data-file list
  when snapshots contain multiple files.
- Materializers that read from ClickHouse commit metadata must replay the full
  data-file list. Falling back to a single `data_file` is only for older rows
  that lack multi-file metadata.
- Multi-host deployments require a shared Iceberg REST catalog and object-store
  warehouse. Local filesystem lakehouse storage is for local development or
  single-node operation.
- Lakehouse maintenance may compact or expire data only through Iceberg-aware
  metadata operations. Tools must not delete files behind Iceberg metadata.

## Serving Materialization And Watermarks

- ClickHouse serving rows are rebuildable from committed lakehouse snapshots and
  are written by `nanotrace-lakehouse-rebuild` in rebuild, incremental, loop, or
  queued-executor modes.
- Raw serving rows and generic indexes are materialized from lakehouse commits:
  `events`, `event_text_index`, and `event_kv_index`.
- Full rebuild raw-event insertion is controlled by `NANOTRACE_REBUILD_RAW`.
  Derived materialization through `MaterializeTargets::all()` must not imply a
  second raw `events` insert.
- Incremental materialization selects raw and derived targets from
  `serving_watermarks`. It writes only targets whose watermarks lag the source
  sequence and advances only watermarks for targets it actually loaded.
- ClickHouse inserts that can be replayed must use stable source snapshots and
  deduplication tokens or equivalent dedupe settings.
- `serving_watermarks` may advance for a serving table only after that table's
  writes for the source snapshot have succeeded.
- Any new serving table must define its source table, row identity, replay
  behavior, and watermark behavior before query planners may depend on it.

## Query Planning And Freshness

- User-facing reads go through structured tenant-scoped query APIs. There is no
  public unrestricted SQL route.
- Query execution must apply tenant scope from the authenticated identity,
  validate allowed source tables, enforce row/time/byte limits, and check
  freshness unless the caller explicitly allows stale serving data.
- SQL-shaped internal reads are allowed only for a bounded set of serving and
  control tables. If the server cannot identify and safely scope every table
  source, the query must fail closed.
- CTE names and table functions are not tenant-isolation boundaries. They are
  allowed only when they resolve to an allowed, scopeable table source.
- Event browsing and unsupported predicates fall back to `events`, but repeated
  broad scans should become promoted fields, measures, reports, sequences,
  cohorts, or offline lakehouse work.
- Event text filters and saved search use `event_text_index`; arbitrary scalar
  equality, existence, set, and numeric range predicates use `event_kv_index`
  when possible.
- If a promoted index is absent or stale, scalar filters must fall back to
  `event_kv_index` when possible. Raw `events.data` scans are reserved for
  unsupported predicates or explicitly bounded diagnostic paths.
- Structured latest-state reads use `entity_state_current`; as-of and
  historical state reads use `entity_state_updates`.
- Report, funnel, cohort, measure, state, search, and alert query types must
  route through their typed request shapes and serving substrates rather than
  adding ad hoc public query surfaces.
- Successful query execution should record `query_usage` with enough metadata to
  understand plan kind, source tables, filters, grouping, freshness, cost, and
  promotion recommendations.

## Definitions, Promotion, And Backfills

- Definitions are metadata. A definition row alone never proves that its serving
  rows are populated or fresh.
- Active definitions are read from `definitions` with `FINAL`, `enabled = 1`,
  and `deleted_at IS NULL`.
- Definition mutation is append/versioned. Create inserts a new active
  definition row, delete inserts a disabled/deleted version, and the public API
  does not provide in-place updates.
- Definition versions are part of serving correctness. Planner reads against
  definition-backed tables must constrain the relevant definition id and
  version when the table stores versioned output.
- `field_index` reads for promoted fields must constrain `definition_id` and
  `definition_version`; a matching `field_name` alone is not a freshness proof.
- Definition-backed serving tables may be used for performance only when the
  relevant definition/version has a materialization watermark covering the query
  window, a serving watermark proves the table is current for that definition,
  or the caller explicitly allows stale serving data.
- Synchronous definition backfill supports the definition kinds implemented by
  the server backfill path, currently field, measure/rollup, and state.
- Historical report, sequence, and cohort backfills are job-based. Creating a
  backfill inserts `materialization_jobs` and `materialization_chunks`; a queued
  executor claims chunks, writes outputs, and updates job/chunk state.
- SDK/default metric definitions are organization-local seed data. The server
  seeds them idempotently at startup for known organizations and after account
  API organization creation when ClickHouse is configured; they should not be
  exposed as a public product endpoint.
- Versioned outputs must publish enough lineage for typed reads to select a
  serving result: target type, target id, target version, source table/window,
  source snapshot or sequence, row counts, status, and completion time.
- `materialization_versions` and `materialization_watermarks` are the authority
  for versioned report, sequence, cohort, and similar materialized output
  freshness when active versions exist.
- Query-usage recommendations may suggest promotions, but they must not
  auto-create or auto-mutate definitions without an explicit product/user/admin
  action.
- Archived organizations must not resolve as active session memberships or API
  key identities. Archiving revokes organization API keys, revokes pending
  invitations, and clears active sessions for that organization.
- Account lifecycle mutations are auditable through
  `nanotrace_account_audit_events`, including actor subject/auth type, target
  organization, target subject/email, metadata, and timestamp.
- Account API failures are exposed as
  `nanotrace_account_api_failures_total` with route area and status-class
  labels.

## Alerting And Notifications

- Alert definitions live in `definitions` with kind `alert`; the alert worker
  loads enabled, non-deleted definitions before evaluating matches.
- Hot event-match alerts consume normalized Kafka events. They do not wait for
  lakehouse-to-ClickHouse serving materialization to catch up.
- Matched alerts are recorded in `alert_events` with tenant, alert definition,
  source event, severity, match data, and dedupe context.
- Webhook delivery work is recorded in `alert_notifications`. Notification
  state changes are append/update rows in ClickHouse and must preserve attempt,
  retry, delivery, failure, and last-error context.
- Alert dedupe is bounded by the implemented dedupe key and time bucket
  behavior. Horizontal scaling changes must revisit dedupe guarantees before
  claiming stronger semantics.
- Alert queries read typed alert serving tables through the `alerts` query type.
  Paging policy, escalation policy, and hard sub-millisecond standing-query
  guarantees are separate product contracts unless explicitly implemented.

## Operations, Maintenance, And Observability

- Serving freshness, materialization freshness, lakehouse commit lag, query
  behavior, sidecar drops, alert delivery, and maintenance pressure must be
  observable through logs, metrics, or serving/control tables.
- `pipeline_metrics` is the operational sink for lakehouse/materializer
  maintenance signals such as small-file pressure, commit pressure, compaction
  results, and engine-maintenance requirements.
- Local filesystem lakehouse catalogs may use first-party native compaction
  when enabled. REST/object-store deployments should use an operator-provided
  Iceberg engine maintenance command for compaction, snapshot expiry, and
  orphan cleanup.
- Maintenance modes must report pressure and required external work instead of
  silently hiding unmaintained lakehouse state.
- Any new query feature must define request shape, allowed tables, tenant
  scoping, freshness behavior, query-usage metadata, and UI/API surface before
  it is considered production-ready.
- Any new promoted output must define its definition parser, materializer rows,
  ClickHouse schema, watermark/version publishing, query reader, and operator
  controls before planners may route user reads to it by default.
