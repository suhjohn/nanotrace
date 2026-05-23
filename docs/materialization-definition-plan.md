# Definition And Materialization Implementation Plan

This plan describes the next implementation path for definition-backed materialization. It is scoped to:

- SDK-shaped events becoming useful through managed definitions.
- User-created or user-updated definitions driving materialized ClickHouse serving rows.
- Iceberg remaining the durable source of truth.
- ClickHouse remaining a rebuildable serving and index plane.

This phase intentionally does not use `query_usage` to automatically create or mutate definitions. Query usage can remain operational telemetry, but it is not part of definition creation, recommendation, or enablement in this plan.

## Current System Baseline

The current write path is:

1. SDK or HTTP caller posts events to `/v1/events`.
2. The server validates and queues raw event batches.
3. The loader commits the raw rows to Iceberg and writes them to ClickHouse `observatory.events`.
4. ClickHouse materialized views populate the small always-on rollup/index pack, currently `event_density_1s`, `field_rollups`, and `field_values`.
5. `nanotrace-lakehouse-rebuild` can run an incremental materializer that reads `lakehouse_commits`, loads active `observatory.definitions`, reads committed Iceberg data files, and writes:
   - `field_index`
   - `event_measures`
   - `counter_rollups`
   - `gauge_rollups`
   - `histogram_rollups`
   - `entity_state_updates`
   - `report_results`
   - `sequence_report_results`
   - `cohort_memberships`
6. `mv_measure_rollups` rolls `event_measures` into `measure_rollups`.

The current definition model is path-based:

- `kind='field'` extracts a path into `field_index`.
- `kind='measure'` or `kind='rollup'` extracts one numeric path into `event_measures`.
- `kind='state'` extracts one entity state value into `entity_state_updates`.

That model works for simple explicit definitions, but it is too narrow for SDK-managed metrics because SDK metric functions produce dynamic measure names:

```json
{
  "event_type": "metric",
  "metric_name": "http.server.requests",
  "metric_type": "counter",
  "metric_value": 15,
  "metric_unit": "1",
  "service": "api",
  "environment": "prod"
}
```

The materializer should not need one separate definition per metric name before it can extract this event.

## Target Model

Definitions should become semantic extraction rules, not only static path extractors.

Each definition has:

- `tenant_id`
- `definition_id`
- `name`
- `kind`
- `mode`
- `enabled`
- `config`
- `capabilities`
- `version`

The current table can hold the richer config as JSON, so the first implementation does not require a schema migration for `observatory.definitions`. We should evolve how `config` is interpreted by the materializer.

### Definition Kinds

Keep the existing kinds and extend their config shape:

| Kind | Target table | Purpose |
| --- | --- | --- |
| `field` | `field_index` | Fast equality filters, facets, drilldown, and group lists for promoted fields. |
| `measure` | `event_measures` then `measure_rollups` | Numeric event-level extraction and aggregate rollups. |
| `state` | `entity_state_updates` | Entity state timelines, as-of reads, and current-state derivation. |
| `report` | `report_results` | Reusable summary, trace summary, and retention outputs for dashboard cards, alerts, broad summaries, and top-N reports. |
| `sequence` | `sequence_report_results` | Funnel and ordered-workflow step outputs. |
| `cohort` | `cohort_memberships` | Membership sets for cohorts, retention inputs, and cohort-scoped drilldowns. |

`field`, `measure`, `metric_rollup`, `state`, summary `report`, trace summary `report`, retention `report`, sequence, and cohort membership definitions are in scope for the first implementation pass.

## Config Shape

Definitions should support both the current compact format and a generalized format.

### Field Definition

Current format remains valid:

```json
{
  "path": "llm.model",
  "value_type": "string"
}
```

Generalized format:

```json
{
  "match": {
    "all": [
      { "path": "event_type", "op": "eq", "value": "span" }
    ]
  },
  "outputs": [
    {
      "target": "field_index",
      "field_name": "llm.model",
      "value": { "path": "llm.model" },
      "value_type": "string",
      "mode": "facet"
    }
  ]
}
```

Rules:

- `match` is optional. If absent, the output applies to any event with the source path.
- `outputs[].value.path` supports nested JSON paths such as `llm.model` and exact dotted keys such as `http.method`.
- Field values can be scalar or arrays. Arrays emit one index row per scalar element.
- Object values are ignored unless a future output type explicitly supports object flattening.

### Static Measure Definition

Current format remains valid:

```json
{
  "path": "duration_ms",
  "unit": "ms",
  "dimension": "service"
}
```

Generalized format:

```json
{
  "match": {
    "all": [
      { "path": "event_type", "op": "eq", "value": "span" }
    ]
  },
  "outputs": [
    {
      "target": "event_measures",
      "measure_name": "duration_ms",
      "value": { "path": "duration_ms" },
      "unit": "ms",
      "dimensions": [
        { "name": "service", "value": { "path": "service" } },
        { "name": "environment", "value": { "path": "environment" } }
      ],
      "bucket_seconds": 300
    }
  ]
}
```

### SDK Metric Definition

This is the important managed-definition case. One definition extracts many SDK metric names.

```json
{
  "managed_by": "sdk",
  "sdk_surface": "metric",
  "match": {
    "all": [
      { "path": "event_type", "op": "eq", "value": "metric" },
      { "path": "metric_name", "op": "exists" },
      { "path": "metric_value", "op": "is_number" }
    ]
  },
  "outputs": [
    {
      "target": "event_measures",
      "measure_name": { "path": "metric_name" },
      "value": { "path": "metric_value" },
      "unit": { "path": "metric_unit", "default": "" },
      "dimensions": [
        { "name": "service", "value": { "path": "service" } },
        { "name": "environment", "value": { "path": "environment" } },
        { "name": "signal", "value": { "path": "signal" } },
        { "name": "metric_type", "value": { "path": "metric_type" } },
        { "name": "llm.model", "value": { "path": "llm.model" } },
        { "name": "llm.provider", "value": { "path": "llm.provider" } }
      ],
      "bucket_seconds": 300
    }
  ]
}
```

Rules:

- `measure_name` can be a literal string or a path expression.
- `unit` can be a literal string or a path expression with a default.
- The materializer emits one `event_measures` row per non-empty dimension value.
- If no configured dimension value exists, it emits one row with empty `dimension_name` and `dimension_value`.
- This preserves the existing `event_measures` schema while supporting dynamic SDK metric names.

### State Definition

Current format remains valid:

```json
{
  "path": "plan",
  "entity_type": "account",
  "entity_id_path": "account_id",
  "value_type": "string"
}
```

Generalized format:

```json
{
  "match": {
    "all": [
      { "path": "event_type", "op": "eq", "value": "track" },
      { "path": "name", "op": "eq", "value": "Account Plan Changed" }
    ]
  },
  "outputs": [
    {
      "target": "entity_state_updates",
      "entity_type": "account",
      "entity_id": { "path": "account_id" },
      "state_name": "account.plan",
      "value": { "path": "plan" },
      "value_type": "string"
    }
  ]
}
```

## SDK Defaults

The SDK should continue to emit canonical event shapes. It should not synchronously create one definition per SDK call.

Instead, tenants should get a small set of managed definitions. These can be seeded when a tenant/workspace is created, when definitions are initialized, or by an idempotent startup/admin task.

Initial managed definitions:

| SDK surface | Event shape | Managed definition |
| --- | --- | --- |
| `counter`, `gauge`, `histogram`, `timing` | `event_type='metric'`, `metric_name`, `metric_value` | Dynamic SDK metric measure definition. |
| `httpServerRequest`, `httpClientRequest` | `event_type='span'`, HTTP fields, `duration_ms` | Optional field definitions for `http.method`, `http.route`, `http.status_code`; optional duration measure. |
| `recordSpan`, `span`, `startSpan` | `event_type='span'`, `duration_ms`, trace ids | Optional duration measure and trace-related field definitions. |
| `dbQuery` | `event_type='span'`, `db.system`, `db.operation`, `duration_ms` | Optional database fields and duration measure. |
| `rpcCall` | `event_type='span'`, `rpc.system`, `rpc.service`, `rpc.method`, `duration_ms` | Optional RPC fields and duration measure. |
| `messagePublish`, `messageConsume` | `event_type='span'`, messaging fields, `duration_ms` | Optional messaging fields and duration measure. |
| `track`, `revenue` | `event_type='track'`, `name`, optional `revenue` | Optional revenue measure if `revenue` is numeric. |
| `identify`, `group`, `alias` | identity/account ids | Field definitions for high-value identifiers only when needed. |
| `experimentViewed`, `featureFlagEvaluated` | experiment and flag fields | Optional field definitions for experiment/variant/flag. |

For the first implementation, create only the SDK metric managed definition by default. User/admin-created summary report definitions can materialize `report_results`, but the other SDK defaults should be explicit follow-up definitions because each adds write fanout.

## User Definitions

User-created definitions should be explicit and versioned.

When a user creates or updates a definition:

1. Validate the config.
2. Write a new `observatory.definitions` version.
3. Decide the materialization mode:
   - `forward_only`: start applying from the next Iceberg commit.
   - `backfill`: enqueue or run historical materialization from a chosen start time or sequence.
4. The materializer reads active definitions and writes derived serving rows.
5. Readers may use the serving table only when the relevant watermark or materialization version covers the requested data.

For this phase, use existing `serving_watermarks` for `field_index`, `event_measures`, metric rollups, `entity_state_updates`, summary, trace summary, and retention `report_results`, `sequence_report_results`, and `cohort_memberships`. Broader `materialization_jobs`, `materialization_chunks`, `materialization_versions`, and `materialization_watermarks` remain the right model for full published report versions and large backfills, but do not need to block this work.

## Materializer Changes

### Phase 1: Internal Rule Engine

Add a small extraction rule engine inside `tools/lakehouse-rebuild/src/main.rs` or a sibling module.

It should parse both legacy configs and generalized configs into internal structs:

```text
ExtractionDefinitions
  fields: Vec<FieldRule>
  measures: Vec<MeasureRule>
  states: Vec<StateRule>
```

Each rule should contain:

- tenant id
- definition id
- definition version
- optional matcher
- output expressions

Expression types:

```text
LiteralString
Path
PathWithDefault
```

Matcher operations for the first pass:

```text
exists
eq
neq
is_number
in
```

Do not add substring matching, regex, or full boolean query syntax to materializer definitions yet. The materializer should stay deterministic and cheap per row.

### Phase 2: Dynamic Measure Names

Extend `event_measure_rows` so `measure_name`, `unit`, and dimensions can come from expressions.

The output should still be rows in the existing `event_measures` table:

```text
tenant_id
definition_id
definition_version
measure_name
value
unit
timestamp
bucket_time
bucket_seconds
event_id
event_type
signal
dimension_name
dimension_value
```

If a measure has multiple dimensions, emit one row per dimension. This matches the existing single `dimension_name` / `dimension_value` schema.

### Phase 3: Managed Definition Seeding

Add an idempotent definition seeding path for SDK defaults.

Options:

1. Server startup/admin task inserts default definitions for a tenant.
2. Workspace creation inserts default definitions.
3. A separate CLI seeds or repairs managed definitions.

Preferred first implementation: a server/admin helper function plus a CLI or test-callable function. Avoid adding this to the hot event ingest path.

The seed operation should upsert a stable definition id, for example:

```text
sdk_metric_default_v1
```

It should only update the row when the managed config version changes.

### Phase 4: Query Planner Awareness

The existing planner already uses:

- `field_index` for promoted field filters/groups.
- `event_measures` and `measure_rollups` in sandbox/read-source allowlists and tests.

For this phase:

1. Keep the event explorer behavior unchanged except for better promoted-field support.
2. Add focused measure query endpoints only when a UI/reporting surface needs them.
3. Do not silently route arbitrary event explorer histograms through measures unless the semantics match exactly.

## ClickHouse Schema Changes

No required schema migration for the first pass.

Use the existing tables:

- `definitions`
- `field_index`
- `event_measures`
- `measure_rollups`
- `entity_state_updates`
- `serving_watermarks`

Possible later schema improvements:

1. Add `dimension_map` or repeated dimensions to `event_measures` if multi-dimensional rollups become important.
2. Add `definition_scope` or `managed_by` columns if querying JSON config becomes inconvenient.
3. Add dedicated current-state snapshots if `entity_state_updates` replay becomes expensive.
4. Add versioned sequence and full published report executor tables once those materializers exist.

Do not widen `events` for arbitrary SDK fields. The raw table should remain a correctness fallback, not the optimization layer.

## Fixture-Based Tests

Add fast tests that use `fixtures/events/*.json` directly.

These tests should not require ClickHouse, Iceberg, S3, or HTTP.

### Test Cases

1. `metric_counter.json`
   - Given SDK metric managed definition.
   - Expect at least one `event_measures` row.
   - `measure_name` comes from `metric_name`.
   - `value` comes from `metric_value`.
   - `unit` comes from `metric_unit` or default.

2. `metric_gauge.json`
   - Same as counter.
   - Ensure non-monotonic gauge still materializes as a measure.

3. `metric_histogram.json`
   - Same as counter.
   - Ensure histogram values enter `event_measures`; percentile behavior is handled later by `measure_rollups`.

4. `llm_call.json`
   - Given field definitions for `llm.model` and `llm.provider`.
   - Expect `field_index` rows with field names `llm.model` and `llm.provider`.
   - Confirm nested object paths and exact dotted keys both work.

5. `tool_call.json`
   - Given a field definition for tool name or tool status.
   - Expect `field_index` rows.

6. `state_account_plan_changed.json`
   - Given state definition for account plan.
   - Expect one `entity_state_updates` row with entity type `account`.

7. `product_order_filled.json`
   - Given revenue or order-value measure definition.
   - Expect `event_measures` rows when the numeric field exists.

### Assertions

The tests should assert exact derived rows, not only row counts:

```text
definition_id
definition_version
measure_name
value
dimension_name
dimension_value
field_name
field value
entity_type
entity_id
state_name
```

Also add negative tests:

- Missing `metric_value` does not emit a measure.
- Non-numeric `metric_value` does not emit a measure.
- Matcher mismatch does not emit output.
- Empty dimension value is skipped.
- Duplicate field array values emit only one index row per event/definition/value.

## Local Integration Tests

Add an integration harness that runs against local ClickHouse plus the existing server/loader/materializer stack.

Use a unique `run_id` and avoid truncating shared local tables by default.

The repository script for this is:

```sh
npm run materialization:loadtest
```

It seeds the SDK managed metric definition through `/v1/definitions/sdk-defaults`, creates run-scoped summary report, trace summary report, sequence, cohort, and retention definitions, runs the Rust loadtester with a bounded profile, then polls ClickHouse for raw event visibility, SDK-derived metric rollups, `report_results`, `sequence_report_results`, `cohort_memberships`, and serving watermarks. Defaults target the local dev stack; override `NANOTRACE_INGEST_URL`, `NANOTRACE_API_KEY`, `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `NANOTRACE_LOADTEST_PROFILE`, or `NANOTRACE_LOADTEST_TOTAL_EVENTS` as needed.

Flow:

1. Start local dependencies.
2. Apply `deploy/clickhouse/schema.sql`.
3. Insert or seed managed SDK definitions for tenant `loadtest`.
4. Send fixture events through `/v1/events`.
5. Run or wait for the loader.
6. Run one materializer pass or wait for the materializer loop.
7. Query ClickHouse and assert raw and derived tables.

Useful assertions:

```sql
SELECT count()
FROM observatory.events
WHERE tenant_id = 'loadtest'
  AND getSubcolumn(data, '_loadtest.run_id') = '<run_id>';
```

```sql
SELECT measure_name, count()
FROM observatory.event_measures
WHERE tenant_id = 'loadtest'
GROUP BY measure_name;
```

```sql
SELECT field_name, count()
FROM observatory.field_index
WHERE tenant_id = 'loadtest'
  AND field_name IN ('llm.model', 'llm.provider', 'http.method')
GROUP BY field_name;
```

```sql
SELECT measure_name, bucket_seconds, count()
FROM observatory.measure_rollups
WHERE tenant_id = 'loadtest'
GROUP BY measure_name, bucket_seconds;
```

```sql
SELECT serving_table, max(source_sequence_number)
FROM observatory.serving_watermarks
WHERE serving_table IN ('events', 'field_index', 'event_measures', 'entity_state_updates')
GROUP BY serving_table;
```

## Loadtester Validation

Use the existing loadtester after fixture-level tests pass.

### Deterministic Small Metric Run

```sh
NANOTRACE_LOADTEST_PROFILE=metrics \
NANOTRACE_LOADTEST_TOTAL_EVENTS=128 \
NANOTRACE_LOADTEST_BATCH_SIZES=128 \
NANOTRACE_LOADTEST_START_RPS=1 \
NANOTRACE_LOADTEST_MAX_RPS=1 \
cargo run -p nanotrace-loadtest
```

Expected:

- 128 raw events for the run id.
- Metric-derived `event_measures` rows exist.
- `measure_rollups` rows appear after the MV processes `event_measures`.
- `serving_watermarks` for `event_measures` catches up to the tested commit sequence.

### Broader SDK Shape Run

```sh
NANOTRACE_LOADTEST_PROFILE=codex \
NANOTRACE_LOADTEST_TOTAL_EVENTS=1024 \
NANOTRACE_LOADTEST_BATCH_SIZES=128 \
NANOTRACE_LOADTEST_START_RPS=10 \
NANOTRACE_LOADTEST_MAX_RPS=10 \
cargo run -p nanotrace-loadtest
```

Expected:

- Raw events exist for the run.
- Managed metric measures exist.
- Explicit field definitions materialize expected fields from LLM/tool/span fixtures.
- No materializer backlog remains after the loader catches up.

### Performance Observation

For each load run, capture:

- HTTP accepted events/sec.
- Loader lag from POST accepted time to ClickHouse raw `events`.
- Materializer lag from ClickHouse raw availability to derived rows.
- Event counts by target table.
- p50/p95/p99 materialization delay where timestamps allow it.

The immediate goal is correctness and bounded lag at small scale, not unlimited RPS.

## UI Validation

After materialization works:

1. Use the event explorer to group by a promoted field such as `llm.model` or `llm.provider`.
2. Confirm group options include promoted definitions.
3. Confirm group queries use `field_index` instead of raw `events`.
4. Confirm event drilldown still reads raw events.
5. Confirm filters combining optimized and raw predicates remain correct:
   - optimized equality predicates become semi-joins by `event_id`;
   - complex predicates remain raw filters.

Example:

```text
llm.model=openai/gpt-5.5 AND message CONTAINS timeout
```

Expected behavior:

- `llm.model=...` uses `field_index` to narrow event ids.
- `message CONTAINS timeout` remains a raw event predicate.
- Final result is correct because raw `events` remains the final filtering source.

## Implementation Order

1. Add generalized config structs and parser.
   - Preserve existing legacy configs.
   - Add unit tests for legacy compatibility.

2. Add matcher evaluation.
   - Implement `exists`, `eq`, `neq`, `is_number`, and `in`.
   - Keep matcher evaluation side-effect free and cheap.

3. Add expression evaluation.
   - Implement literal strings, path strings, path numbers, and defaults.
   - Reuse the existing `value_at_path` behavior for nested paths and exact dotted keys.

4. Extend measure extraction.
   - Support dynamic `measure_name`.
   - Support dynamic `unit`.
   - Support multiple configured dimensions by emitting one row per dimension.
   - Keep legacy single-dimension output working.

5. Extend field extraction.
   - Support generalized `outputs[]`.
   - Keep legacy `path` definitions working.

6. Extend state extraction.
   - Support generalized `outputs[]`.
   - Keep legacy `path`, `entity_type`, and `entity_id_path` definitions working.

7. Add SDK metric managed definition.
   - Stable id.
   - Idempotent seed behavior.
   - No hot-ingest mutation.

8. Add explicit report materialization.
   - Support generalized `outputs[]` targeting `report_results`.
   - Aggregate matched events by bucket and dimensions.
   - Support summary, trace summary, and retention report modes.
   - Validate with fixture JSONs and bounded loadtester runs.

9. Add fixture tests.
   - Cover metrics, LLM, tool, state, and product fixtures.
   - Add negative tests for mismatches and invalid values.

10. Add local integration test script or test target.
   - Seed definitions.
   - Ingest fixtures.
   - Run loader/materializer.
   - Assert ClickHouse tables.

11. Run bounded loadtester validation.
   - `metrics` profile first.
   - `codex` profile second.
   - Observe raw and derived lag.

12. Wire UI validation.
   - Confirm promoted group fields appear.
   - Confirm filters use semi-joins where possible.
   - Confirm raw fallback still preserves correctness.

## Non-Goals For This Phase

- No query-usage-driven automatic definition creation.
- No automatic per-event or per-SDK-call definition creation.
- No full published-version report executor beyond explicit summary, trace summary, sequence, retention, and cohort membership materialization.
- No full boolean expression language in definitions.
- No arbitrary object flattening.
- No widening of `events` for every SDK field.
- No UI dependency on `measure_rollups` until a concrete measure/report surface exists.

## Success Criteria

This phase is complete when:

1. Existing legacy field, measure, and state definitions still materialize correctly.
2. The SDK metric managed definition materializes metric fixtures into `counter_rollups`, `gauge_rollups`, and `histogram_rollups`.
3. Legacy `measure` definitions still populate `event_measures` and `measure_rollups`.
4. User-created generalized field definitions materialize nested paths such as `llm.model`.
5. User-created generalized state definitions materialize entity state updates.
6. User-created generalized report definitions materialize summary and trace summary rows into `report_results`.
7. User-created generalized sequence definitions materialize funnel rows into `sequence_report_results`.
8. User-created generalized cohort definitions materialize memberships into `cohort_memberships`.
9. User-created retention report definitions materialize retention rows into `report_results`.
10. Fixture tests pass without external services.
11. Local integration tests prove raw ingest to derived ClickHouse rows.
12. A bounded loadtester run shows the loader and materializer catch up with expected row counts.
13. The event explorer remains correct for mixed optimized and raw filters.
