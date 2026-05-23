# Query, Index, and Evolution Review

This review compares the ClickHouse schema, HTML specs, server write path, server query path, and current UI behavior. The design direction is sound: Iceberg is the durable source of truth, ClickHouse is a rebuildable serving/index plane, and repeated expensive raw query shapes should evolve into definitions, measures, reports, cohorts, or sequence outputs.

Files inspected:

- `deploy/clickhouse/schema.sql`
- `docs/design.html`
- `docs/usecase.html`
- `apps/ui/index.html`
- `apps/server/src/http.rs`
- `apps/server/src/event_log.rs`
- `apps/server/src/read.rs`
- `apps/server/src/definitions.rs`
- `apps/loader/src/main.rs`
- `tools/lakehouse-rebuild/src/main.rs`
- `apps/ui/src/routes/index.tsx`

## Current Table Inventory

| Table | Populated by | Read by current code | Status |
| --- | --- | --- | --- |
| `events` | Loader raw insert from accepted batches after Iceberg commit; rebuild tool can replay Iceberg files | `/v1/events/query` for events, summaries with filters, raw groups, density fallback, flamegraph, event lookup; `/v1/query` sandbox | Core raw serving table and correctness fallback |
| `event_density_1s` | `mv_event_density_1s` from `events` | server density/summary when no group/filter; some old UI direct SQL helpers | Active always-on rollup |
| `field_rollups` | `mv_field_rollups` from `events` | server groups/latest/summary/density for core rollup fields | Active always-on grouped rollup |
| `field_values` | `mv_field_values` from `events` | server groups/latest and filter semi-joins for built-in lookup ids | Active exact lookup index |
| `flamegraph_rollups_1m` | `mv_flamegraph_rollups_1m` from `events` | sandbox allowlist only | Built and documented, but not consumed by the current UI/server flamegraph path |
| `definitions` | schema APIs and definition backfills | server loads promoted field catalog; definition backfill code reads/writes it | Active control table for field/measure/state definitions |
| `field_index` | definition backfill, loader promoted mode, lakehouse materializer/rebuild | server groups/latest/filter semi-joins for promoted fields; sandbox; older UI direct helpers | Active promoted field index |
| `event_measures` | definition backfill, loader promoted mode, lakehouse materializer/rebuild | sandbox and tests; source for `measure_rollups` MV | Active promoted numeric extraction substrate |
| `measure_rollups` | `mv_measure_rollups` from `event_measures` | sandbox and tests | Active once measures exist, but not wired into the main event UI planner |
| `entity_state_updates` | definition backfill, loader promoted mode, lakehouse materializer/rebuild | sandbox and tests | Active state substrate, not first-class in main UI |
| `cohort_memberships` | lakehouse materializer/rebuild | sandbox and tests | Active cohort membership serving table |
| `report_results` | lakehouse materializer/rebuild for summary, trace summary, and retention reports | sandbox and tests | Active report serving table; full published-version jobs still planned |
| `sequence_report_results` | lakehouse materializer/rebuild for sequence definitions | sandbox and tests | Active sequence/funnel serving table |
| `definition_stats` | definition backfill stats | sandbox | Active metadata, narrow use |
| `query_usage` | server records every `/v1/query` and `/v1/events/query` SQL execution | sandbox | Active operational telemetry |
| `materialization_jobs` | no report/cohort/sequence job executor found | sandbox | Planned broader control plane |
| `materialization_chunks` | no report/cohort/sequence job executor found | sandbox | Planned broader control plane |
| `materialization_versions` | no report/cohort/sequence publisher found | sandbox | Planned stable-version selector |
| `materialization_watermarks` | no report/cohort/sequence watermark writer found | sandbox | Planned broader freshness/lag table |
| `pipeline_metrics` | schema exists; writers are not part of query planner path | sandbox | Operational metrics table |
| `lakehouse_commits` | loader and rebuild/materializer metadata writes | freshness guard and materializer/rebuild | Active Iceberg commit index |
| `serving_watermarks` | loader and rebuild/materializer metadata writes | freshness guard and materializer/rebuild | Active for `events`, `field_index`, `event_measures`, `entity_state_updates` |

## What The Planner Does Today

The current `/v1/events/query` planner follows this ladder:

1. `group_options`: reads `definitions` plus hardcoded core/lookup fields.
2. `groups`: core field -> `field_rollups`; lookup id -> `field_values`; promoted field -> `field_index`; otherwise raw `events`.
3. `latest`: core field -> `field_rollups`; lookup/promoted -> `field_values`/`field_index`; otherwise raw `events`.
4. `summary`: no group/filter -> `event_density_1s`; selected core group with no extra filter -> `field_rollups`; otherwise raw `events` with an `EventPredicatePlan`.
5. `events`: raw `events`, with simple indexed equality filters converted into `field_values`/`field_index` event-id semi-joins where possible.
6. `density`: no group/filter -> `event_density_1s`; selected core group with no extra filter -> `field_rollups`; otherwise raw `events`.
7. `flamegraph`: raw `events` only, then the UI builds the span hierarchy client-side.
8. `event`: raw `events` by `event_id`; `/v1/events/:id` first tries S3 byte-range lookup from the source pointer and falls back to ClickHouse.

The filter model is mixed correctly for correctness: simple non-negated equality and `IN` predicates on indexed paths become semi-joins by `event_id`; negation, `OR`, `CONTAINS`, text search, and unpromoted paths stay as raw predicates. That keeps answers correct while still pruning the parts of the query that can be indexed.

## What The Write Path Does Today

The hot ingest path is intentionally narrow:

1. `/v1/events` authenticates `ingest:write`, reads the request body, and calls `EventLogWriter::append_bytes`.
2. `append_bytes` parses one event or an event array, stamps the authenticated tenant, and queues the batch onto a local writer lane.
3. Writer lanes append NDJSON parts locally and rotate them through the object-store handoff lifecycle.
4. `nanotrace-loader` consumes object notifications, optionally runs processors, and prepares a deterministic batch.
5. With lakehouse enabled, the loader commits canonical event rows to Iceberg before writing ClickHouse serving rows.
6. In default `LOADER_DERIVATION_MODE=raw`, the loader writes only `events`, `lakehouse_commits`, and `serving_watermarks`. Attached ClickHouse MVs populate `event_density_1s`, `field_rollups`, `field_values`, and `flamegraph_rollups_1m`.
7. In `promoted` mode, the loader can also write `field_index`, `event_measures`, and `entity_state_updates`, but the documented production model prefers the separate lakehouse materializer.
8. `nanotrace-lakehouse-rebuild` can rebuild raw rows from Iceberg data files and run the incremental materializer. It reads `lakehouse_commits`, compares `serving_watermarks`, materializes `field_index`, `event_measures`, metric rollups, `entity_state_updates`, summary/trace/retention `report_results`, `sequence_report_results`, and `cohort_memberships`, then advances watermarks per target table.

That means the correct default write model is not "make `events` wider." It is "keep baseline write fanout bounded, then add definition-backed or versioned serving outputs through explicit SDK-managed or user/admin definitions."

## Spec And Implementation Gaps

1. `flamegraph_rollups_1m` is mispositioned.
   The HTML design says core flamegraphs read `flamegraph_rollups_1m`, but the server and UI flamegraph path reads raw event metadata and builds spans client-side. The existing table is really a fixed hierarchy rollup, not the trace waterfall data the UI renders. Either rename/reframe it as a generic hierarchy rollup and build a route that consumes it, or remove it until there is a real serving path.

2. `query_usage` is operational telemetry only.
   It should help explain planner behavior and raw fallback, but it should not create, mutate, or recommend definitions in this phase.

3. Full published-version report jobs are still future work.
   Summary reports, sequence reports, retention reports, and cohort memberships are now materialized by the lakehouse materializer. Stable version publication still needs the broader job/version control plane.

4. The main UI planner does not use measure/state/report outputs.
   Measures and states can be materialized, and `measure_rollups` is populated by an MV, but the event explorer only plans event/group/density/flamegraph views. Dashboard/report surfaces need a separate planner that selects completed report/measure/state versions.

5. Freshness guards cover the current serving-watermark model.
   Read freshness checks guard `events`, `field_index`, `event_measures`, metric rollups, `entity_state_updates`, `report_results`, `sequence_report_results`, and `cohort_memberships`. Full published report versions still need `materialization_versions` and `materialization_watermarks`.

6. UI has obsolete direct-SQL helper paths.
   The active UI calls `/v1/events/query` for group options, groups, latest, summary, events, flamegraph, density, and event lookup. Older direct SQL helper functions for density/field-index paths remain in `apps/ui/src/routes/index.tsx` but are not called. They should be deleted or updated when the UI query model is cleaned up.

## What Tables Should Exist

Keep the current shape, but classify it more explicitly:

### Baseline Serving Pack

These are always-on because they cover the default event explorer at bounded write fanout:

- `events`
- `event_density_1s`
- `field_rollups`
- `field_values`
- `lakehouse_commits`
- `serving_watermarks`
- `pipeline_metrics`

### Definition-Backed Promoted Pack

These are the right substrate for user-specific repeated query paths:

- `definitions`
- `definition_stats`
- `field_index`
- `event_measures`
- `measure_rollups`
- `entity_state_updates`

### Versioned Materialized Output Pack

These should remain. Summary, trace summary, retention, sequence, and cohort outputs have incremental materializers; full published-version report jobs still need executors:

- `report_results`
- `sequence_report_results`
- `cohort_memberships`
- `materialization_jobs`
- `materialization_chunks`
- `materialization_versions`
- `materialization_watermarks`

### Query Telemetry Pack

This remains useful for observing planner behavior, but it is not part of automatic schema evolution in this phase:

- `query_usage`

## Target Query Ladder

The serving planner should make these choices in order:

1. Point event lookup: `events` and then S3 source byte range when raw bytes are needed.
2. No-group density/summary: `event_density_1s`.
3. Core group lists: `field_rollups`.
4. Core grouped histograms: `field_rollups`.
5. Core lookup ids: `field_values`.
6. Promoted exact filters/group-bys: `field_index`.
7. Promoted numeric aggregates: `measure_rollups`, or `event_measures` for event-level drilldown.
8. Entity as-of/current state: `entity_state_updates`, later snapshot tables if state volume requires it.
9. Alerts, reusable dashboard cards, top-N reports, active users, broad trace summaries: `report_results`.
10. Funnels/sequences: `sequence_report_results`.
11. Retention/cohort membership: `cohort_memberships` plus `report_results`.
12. Free text, regex, external joins, cross-tenant analytics, and millisecond streaming alerts: new subsystem or explicitly offline path.

Raw `events` remains the correctness fallback, but repeated broad raw scans should not remain a normal interactive path.

## Usage-Driven Evolution Loop

Future query telemetry can help humans understand where raw fallback is expensive, but it should not auto-create or auto-mutate definitions in the current design:

1. Observe every query with planner metadata.
   Record `surface`, `plan_kind`, `is_raw_fallback`, source tables, filter paths, group-by paths, text search usage, time range, result rows, read rows, read bytes, latency, and whether any indexed semi-joins were used.

2. Classify the query shape.
   Map it to one of: field facet, exact lookup, numeric measure, entity state, report, sequence/funnel, cohort/retention, search, external join, global/admin rollup, or unsupported.

3. Materialize explicitly.
   Create or approve a definition/report, enqueue jobs/chunks for historical backfill when needed, write deterministic outputs, and publish a completed version or advance a watermark.

4. Route after publication.
   The planner should prefer a serving table only when the relevant definition/report version is complete or caught up enough for the requested time window. Otherwise return a clear "materializing" state or use raw fallback only for bounded, user-accepted cases.

## Prioritized Work

1. Decide the fate of `flamegraph_rollups_1m`.
   Either consume it through a real hierarchy-rollup endpoint or rename it away from "flamegraph" so it does not imply it backs the current trace waterfall UI.

2. Add freshness/version selection for reports/cohorts/sequences.
   Use `materialization_versions` and `materialization_watermarks` for these outputs, not just `serving_watermarks`.

3. Clean up stale UI direct-SQL helpers.
   Keep the UI on the typed `/v1/events/query` route for explorer behavior, and move reusable dashboard/report cards to a report-specific API backed by `report_results`.

7. Keep the baseline pack deliberately small.
   Do not add every common SDK field to always-on MVs. Add fields to core rollups only when they are universal and bounded; everything else should be promoted through SDK-managed defaults or explicit user/admin definitions.
