# Database Research Review For Nanotrace Observability Storage

Date: 2026-05-20

Scope: recent database and observability storage work, roughly 2021-2026, applied to the Nanotrace design assumptions:

- ClickHouse is a serving/index engine.
- Iceberg is the durable source of truth.
- Ingested events are heterogeneous.
- Measures, dimensions, reports, indexes, and other derived outputs are configurable and expected to expand over time.

## Bottom Line

The high-level direction is right: keep a raw append-first lakehouse record, keep ClickHouse rebuildable, and use explicit serving projections for repeated query shapes. This matches the strongest ideas from recent work on ClickHouse, lakehouses, observability metrics/log stores, and semi-structured columnar analytics.

The main correction is write fanout discipline. Recent observability systems optimize first for append throughput, loose coordination, tenant isolation, immutable batches, and recent-query acceleration. Our definition-backed materializer fits that. Some always-on ClickHouse materialized views can still become the real ingestion limiter at 10k requests/sec and 100 events/request, or 1M events/sec, so the fixed `field_rollups`, `field_values`, and `flamegraph_rollups_1m` pack must stay small even when user definitions are async.

Recommended stance:

1. Keep `/v1/events` and the loader raw-first.
2. Keep Iceberg as the replayable source of truth.
3. Keep ClickHouse `events` as the correctness fallback and recent serving table.
4. Keep extra derived rows behind explicit SDK-managed or user/admin definitions.
5. Move from "many always-on MVs" toward a small baseline pack plus definition-driven materializers with freshness state.

## Papers And Systems Reviewed

### ClickHouse, Column Stores, And Incremental Views

- [ClickHouse - Lightning Fast Analytics for Everyone, PVLDB 2024](https://www.vldb.org/pvldb/vol17/p3731-schulze.pdf)
  - ClickHouse is designed for high ingestion, large analytical data, real-time latency, SQL, vectorized execution, pruning, MergeTree parts, and incremental materialized views.
  - The paper explicitly calls out recurring queries as an opportunity to adapt physical layout to workload.
  - Applicability: validates ClickHouse for serving, pruning, and rollups; does not imply every rollup should be synchronous with raw insert.

- [Data Formats in Analytical DBMSs: Performance Trade-offs and Future Directions, arXiv 2024 / VLDB Journal 2025](https://www.microsoft.com/en-us/research/publication/data-formats-in-analytical-dbmss-performance-trade-offs-and-future-directions/)
  - Arrow, Parquet, and ORC each expose tradeoffs for analytical DBMS use.
  - Applicability: reinforces that Parquet/Iceberg is excellent durable analytical storage, but a serving engine can still need its own native layout for low-latency queries.

- [Progressive Partitioning for Parallelized Query Execution in Google's Napa, PVLDB 2023](https://research.google/pubs/progressive-partitioning-for-parallelized-query-execution-in-googles-napa/)
  - Napa uses LSM-style real-time ingestion and adapts partitioning for diverse workloads, including full scans, range scans, and multi-key lookups.
  - Applicability: supports our query ladder idea. One write-time order cannot satisfy every query; serving plans need alternate indexes/materializations.

### Lakehouse And Object Storage

- [Analyzing and Comparing Lakehouse Storage Systems, CIDR 2023](https://www.vldb.org/cidrdb/papers/2023/p92-jain.pdf)
  - Lakehouses add transactions, indexing, and DBMS features over low-cost object storage; metadata management determines planning performance.
  - Applicability: validates storing commit metadata and watermarks rather than relying on object-store listing. Our `lakehouse_commits` and `serving_watermarks` are the right shape.

- [Petabyte-Scale Row-Level Operations in Data Lakehouses, PVLDB 2024](https://www.vldb.org/pvldb/vol17/p4159-okolnychyi.pdf)
  - Iceberg captures file metadata, supports ACID, schema evolution, file skipping, time travel, rollback, and hidden partitioning; modern row-level operations choose between eager materialization and lazy delete/read-time strategies.
  - Applicability: reinforces that Iceberg should own durable table state and schema evolution. It also argues for versioned materialization decisions, not irreversible serving-only transformations.

- [Exploiting Cloud Object Storage for High-Performance Analytics, PVLDB 2023](https://www.vldb.org/pvldb/vol16/p2769-durner.pdf)
  - Object stores can serve analytics efficiently, but high throughput needs high request concurrency and careful CPU/network integration.
  - Applicability: long term, production Iceberg should use object storage plus a proper catalog. Current local EBS lakehouse storage is okay as a deploy step, but it is not the full elastic lakehouse design.

### Observability Metrics And Logs

- [Mach: A Pluggable Metrics Storage Engine for the Age of Observability, CIDR 2022](https://www.vldb.org/cidrdb/papers/2022/p12-solleza.pdf)
  - Metrics are labels + timestamp + one or more numeric values.
  - Observability metrics are fresh-biased: in Slack's workload, most queries look at recent data.
  - Mach's lesson is loose coordination, append orientation, and exploiting the metrics data model.
  - Applicability: supports SDK metric definitions and `event_measures`/`measure_rollups`, but warns against high-coordination or high-fanout ingest work.

- [LogStore: A Cloud-Native and Multi-Tenant Log Database, SIGMOD 2021](https://users.cs.utah.edu/~lifeifei/papers/logstore-sigmod21.pdf)
  - LogStore targets tens of millions of log records/sec, PB retrieval, and hundreds of thousands of tenants.
  - It combines local write staging, background upload to object storage, tenant/time organization, and read-optimized column/index blocks.
  - Applicability: strongly supports our local staging -> object-store/lakehouse -> serving table path. It also supports tenant/time as the primary physical layout and discourages doing expensive object-storage work in the foreground.

- [Observability Query Language at Google, Google Research 2024](https://research.google/pubs/observability-query-language-at-google/)
  - Google frames observability querying across telemetry, real-time analytics, logs, and traces.
  - Applicability: supports a unified query surface over heterogeneous signals, but not a single physical table shape for every query. The query language can be unified while storage remains specialized by query class.

### Semi-Structured Analytics

- [JSON Tiles: Fast Analytics on Semi-Structured Data, SIGMOD 2021](https://portal.fis.tum.de/en/publications/json-tiles-fast-analytics-on-semi-structured-data/)
  - Flexibility drives JSON adoption, but lack of fixed schema slows analytics.
  - JSON Tiles automatically detects important keys and extracts them while preserving heterogeneity.
  - Applicability: validates our raw JSON + promoted fields pattern. It also implies query usage or explicit definitions should discover important paths and make them columnar/indexed.

- [Columnar Formats for Schemaless LSM-based Document Stores, PVLDB 2022](https://www.vldb.org/pvldb/vol15/p2085-alkowaileet.pdf)
  - Schemaless document stores lose analytical speed when they cannot use column-major layout; columnar extraction can improve query time by orders of magnitude with limited ingest impact.
  - Applicability: validates storing heterogeneous raw events while extracting only selected paths into columnar serving structures.

## First-Principles Model

Observability data has four hard properties:

1. It is append-heavy.
2. It is heterogeneous.
3. Most interactive reads are recent and tenant-scoped.
4. Repeated dashboards/alerts/reports become predictable even when ad hoc exploration starts unpredictable.

Therefore the system should split storage into three layers:

| Layer | Responsibility | Nanotrace tables |
| --- | --- | --- |
| Durable raw lake | Accepted facts, replay, schema evolution, long retention | Iceberg `nanotrace.events`, Parquet files, commit metadata |
| Raw serving fallback | Recent browsing, point lookup, bounded ad hoc filters | ClickHouse `observatory.events` |
| Derived serving models | Repeated filters, facets, measures, reports, states, cohorts, sequences | `field_index`, `event_measures`, `measure_rollups`, `report_results`, `sequence_report_results`, `cohort_memberships`, `entity_state_updates` |

This is better than one huge wide events table because:

- Heterogeneous events do not share a stable column set.
- Promoting every path makes ingestion scale with payload width instead of event count.
- Query acceleration differs by use case: exact id lookup, low-cardinality facet, percentile measure, top-N report, cohort, and sequence are different physical problems.
- Iceberg gives replayability, so ClickHouse projections can be rebuilt, changed, or dropped.

## Assessment Of Current Nanotrace Design

### What Looks Correct

1. `events` is raw and rebuildable.
   - The ClickHouse table is ordered by `(tenant_id, timestamp, event_type, event_id)`, which matches tenant/time-pruned observability queries.
   - `event_id`, `trace_id`, and `span_id` bloom indexes match point and bounded trace lookups.

2. Iceberg is the right system of record.
   - `lakehouse_commits` records snapshot/file metadata and `serving_watermarks` records ClickHouse freshness.
   - This follows the lakehouse papers' emphasis on metadata-driven planning and snapshot correctness.

3. Heterogeneous payloads belong in JSON plus selected subcolumns.
   - The `data JSON(...)` column with nullable hints matches the semi-structured papers: keep flexibility, extract hot keys.
   - The schema correctly avoids non-null defaults for sparse telemetry fields.

4. Definitions are the right abstraction.
   - `definitions.config` can express matchers and outputs.
   - The materializer converts definitions into `field_index`, `event_measures`, and `entity_state_updates`.
   - This matches the "schema-on-read first, materialize hot paths later" direction from JSON Tiles and schemaless columnar work.

5. Complex filters should be partially optimized, not guessed.
   - Exact indexed equality predicates can become semi-joins over `field_index` or `field_values`.
   - Negation, substring, broad OR, and unpromoted JSON paths should remain raw predicates for correctness.

### What Needs Guardrails

1. Always-on MV fanout may dominate ingestion.
   - The default field rollup MV `ARRAY JOIN`s built-in dimensions when present.
   - `mv_flamegraph_rollups_1m` emits multiple hierarchy rows across service/name, service/route/method, signal/service/name, environment/service/name, plan/service/name, country/service/name, llm model/provider/name, and tool/event/name.
   - At 1M events/sec, even a "small" default pack can mean tens of millions of transform candidates/sec before user definitions.
   - This conflicts with Mach and LogStore's append-first lesson.

2. `event_measures` is one-dimensional per row.
   - The current schema stores one `dimension_name`/`dimension_value` per measure row.
   - That supports "p95 latency by service" and "cost by model".
   - It does not directly support joint grouping like "p95 latency by service + route + status + environment" without either emitting one row per dimension and losing joint semantics, emitting combinations, or using a report-specific output.

3. `measure_rollups` has fixed aggregate states for every measure.
   - Every measure gets count, sum, min, max, avg, and TDigest quantiles.
   - That is good for latency/duration metrics, but wasteful for counters where sum/rate are enough and for gauges where latest/min/max may matter more than TDigest.

4. Current production lakehouse storage is not yet the full object-store lakehouse target.
   - The design doc says production warehouse is `/data/lakehouse` on EBS-backed ingest node.
   - The literature supports object-store-backed analytics, but only with proper concurrency, metadata handling, and file-size discipline.

5. Report/cohort/sequence tables exist before executors.
   - The schema direction is correct.
   - Product behavior should not claim those paths are optimized until materializers, version publication, and query routing exist.

## 10k Req/S, 100 Events/Req Ingestion Impact

That load is 1,000,000 events/sec.

Direct HTTP ingest impact from definitions should be near zero if we keep the hot path as currently intended:

1. `/v1/events` authenticates, parses/stamps, and appends raw batches.
2. The loader commits raw rows to Iceberg and writes ClickHouse `events`.
3. The generalized materializer reads committed snapshots asynchronously and writes derived tables.

System-level ingestion impact is not zero:

| Component | Impact at 1M events/sec |
| --- | --- |
| Server HTTP path | Body bytes, JSON parse/stamp, local writer lanes, request scheduling. Definitions should not run here. |
| Loader | Parquet file creation, Iceberg commit cadence, raw ClickHouse insert batches. |
| ClickHouse `events` MVs | Current always-on MVs run synchronously with inserted blocks and can become the first ClickHouse bottleneck. |
| Definition materializer | Async lag and derived table write load; should not block raw correctness but can fall behind. |
| Query freshness | Optimized reads may be stale if materializer lag grows; raw `events` remains correctness fallback if ClickHouse raw is caught up. |

For SDK metrics specifically:

- If 100% of events are metric events and the default SDK metric definition emits four dimensions, the materializer writes about 4M `event_measures` rows/sec.
- If six dimensions are present, it writes about 6M `event_measures` rows/sec.
- If only 1% of events are metrics, that becomes about 40k-60k `event_measures` rows/sec.
- If 10% are metrics, that becomes about 400k-600k `event_measures` rows/sec.

The conclusion is not "do not materialize metrics." It is:

- keep metric materialization async;
- keep per-definition derived rows explicit and asynchronous;
- let tenants disable or narrow dimensions;
- aggregate high-rate metrics earlier when event-level drilldown is not needed;
- separate raw ingestion health from optimized-query freshness.

## Recommended Architecture Changes

### 1. Reduce The Always-On Pack

Keep:

- `events`
- `event_density_1s`
- `field_values` for true point-lookup ids only
- `lakehouse_commits`
- `serving_watermarks`

Reconsider as default-on:

- broad `field_rollups`
- `flamegraph_rollups_1m`

These can still exist, but they should be treated as a "core explorer pack" with a measurable row/CPU budget, not as free schema. A safer path is:

- default-on for a minimal set: `signal`, `event_type`, `service`, `environment`, maybe `name`;
- definition-enabled for `http.route`, `llm.model`, `llm.provider`, `plan`, `country`, `user_id`, `account_id`, and other product-specific paths;
- report-enabled for hierarchies and flamegraph-like summaries.

### 2. Keep Definition Creation Explicit

Before enabling a definition, require a clear semantic reason for the serving table it writes. SDK-managed metric definitions are allowed because their event shape and rollup semantics are known. User/admin definitions are allowed because the user intentionally chose the serving model. Query usage alone does not create or mutate definitions in this phase.

### 3. Split Measure Outputs By Query Semantics

Keep `event_measures` for event-level numeric drilldown and simple one-dimensional rollups.

Add or reserve explicit output shapes for:

- `measure_rollup`: aggregate-only rollup, no event-level row retention.
- `measure_cube`: controlled joint dimensions with a `dimension_set_hash` and ordered dimension map.
- `histogram_rollup`: bucketed histograms when TDigest is too expensive or when merge behavior needs explicit control.
- `counter_rollup`: sum/rate-oriented counter states without TDigest.
- `gauge_rollup`: latest/min/max/avg states by bucket.

This avoids forcing all metrics into the same row-amplifying `event_measures` shape.

### 4. Keep Materialization Definition-Driven

Do not create a new definition per SDK function call.

Better long-term model:

- SDK emits canonical semantic events.
- Workspace/tenant seed creates a small set of managed definitions.
- User-created definitions add explicit outputs.
- Materializers read active definitions and committed Iceberg snapshots.
- Query planners route only when watermarks or materialization versions prove the output covers the requested window.

This keeps the ingest API stateless and avoids coordination on every new metric name.

### 5. Use Reports For Multi-Dimensional Product Questions

For questions in `docs/usecase.md` such as revenue by product/plan/campaign, DAU by plan, top accounts by revenue, funnels, and retention:

- do not rely on raw `events`;
- do not rely only on one-dimensional `event_measures`;
- create versioned reports/cohorts/sequences with explicit materializers.

This is exactly where `report_results`, `sequence_report_results`, `cohort_memberships`, `materialization_jobs`, `materialization_chunks`, and `materialization_versions` should become real executors.

### 6. Treat Text Search Separately

`message CONTAINS timeout` should remain a correctness fallback for small/bounded cases. If full-text search becomes a primary use case, it needs a text index/search subsystem or a dedicated ClickHouse text-index strategy, not `field_index`.

## Query Planner Implications

The planner should expose why each path was chosen:

| Query shape | Correct path |
| --- | --- |
| No filter density | `event_density_1s` |
| Recent events page | `events` |
| Exact trace/span/request lookup | `events` bloom or `field_values` |
| Exact promoted field filter | `field_index` semi-join |
| Mixed indexed and raw filter | indexed semi-join for candidate reduction + raw predicate for correctness |
| Broad group-by on promoted field | `field_index` or report/topk rollup |
| Numeric aggregate, one dimension | `measure_rollups` if fresh; otherwise `event_measures` or raw |
| Numeric aggregate, multiple dimensions | report/cube materialization |
| Funnel/retention/cohort | sequence/cohort/report materialization |
| Substring/free text | raw fallback or search subsystem |

The planner can record into `query_usage` for operational visibility:

- exact filter paths;
- raw fallback paths;
- indexed semi-joins used;
- source tables;
- read rows/bytes;
- latency;
- whether freshness was stale or complete.

This telemetry is not an automatic optimizer in the current plan.

## Concrete "As Is -> To Be"

### Metrics

As is:

```json
{
  "event_type": "metric",
  "metric_name": "http.server.requests",
  "metric_value": 15,
  "metric_type": "counter",
  "service": "api",
  "environment": "prod",
  "llm": { "model": "openai/gpt-5.5", "provider": "openai" }
}
```

Current default materialization:

- one `event_measures` row for `service=api`;
- one for `environment=prod`;
- one for `metric_type=counter`;
- one for `llm.model=openai/gpt-5.5`;
- one for `llm.provider=openai`;
- then `mv_measure_rollups` writes aggregate states.

To be:

- low-volume/default tenants: current event-level metric rows are fine;
- high-volume tenants: use aggregate-only `counter_rollup` for counters and `gauge_rollup` for gauges;
- only emit event-level `event_measures` when drilldown needs individual metric events.

### HTTP Route Explorer

As is:

- `http.method`, `http.route`, and status are candidates for always-on `field_rollups` only when their cardinality is bounded and normalized.

To be:

- `http.method` can stay default-on because cardinality is tiny.
- `http.status_code` can stay default-on if normalized.
- `http.route` should be default-on only if route cardinality is bounded and normalized; otherwise use an explicit managed HTTP definition.

### LLM Model/Provider

As is:

- `llm.model` and `llm.provider` are nested JSON paths referenced in MVs.

To be:

- keep them as nested `data.llm.model` and `data.llm.provider`;
- use definitions for field index and metric dimensions;
- avoid treating literal dotted top-level keys and nested paths as the same without explicit path resolution tests.

### Flamegraph Rollups

As is:

- `flamegraph_rollups_1m` is always-on but not clearly the same as trace flamegraph/waterfall behavior.

To be:

- rename/reframe as `hierarchy_rollups_1m`, or only build it for explicit report definitions;
- keep trace waterfall/flamegraph UI on trace/span event structure unless a dedicated trace materialization exists.

## Decision Checklist

For any new field, measure, or report, ask:

1. Is it needed for correctness or only speed?
2. Is it used by an interactive UI, alert, report, or API?
3. What raw query does it replace?
4. What is the expected match rate?
5. How many derived rows per matching event?
6. What is the cardinality?
7. Is the query one-dimensional, multi-dimensional, sequence-based, or text-based?
8. What freshness does the user expect?
9. Can the output be rebuilt from Iceberg?
10. What happens if the materializer is behind?

If the answer is "we do not know," keep raw fallback and do not add write fanout yet.

## Final Assessment

We are doing the most important architectural things correctly:

- raw-first append path;
- Iceberg source of truth;
- ClickHouse serving/index layer;
- JSON for heterogeneous payloads;
- definitions for explicit semantic extraction;
- materializer outside the HTTP hot path.

The next correctness/performance frontier is not a new database. It is controlling fanout:

- shrink always-on ClickHouse MVs;
- keep derived outputs behind explicit definitions;
- add aggregate-only measure outputs for high-rate SDK metrics;
- build full published-report materializers before promising stable versioned report serving;
- keep query routing freshness-aware.

This keeps the system aligned with the research: append cheaply, preserve raw truth, materialize only query shapes that have earned a serving model, and make every derived output rebuildable from the lakehouse.
