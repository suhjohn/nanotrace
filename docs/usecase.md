# Nanotrace Query Use Cases at 1M req/s / 100M events/s

Assume the target system receives 1M requests/sec and 100 events/request, or 100M events/sec. At that scale, raw scans are only acceptable for narrow, recent, or point-lookups. Any query that repeatedly scans a broad time range must become a schema definition, report, materialization job, or new subsystem.

The product should classify every question into one of three groups:

1. Group 1: works well out of the box.
2. Group 2: can be made fast with schema/report/materialization.
3. Group 3: needs a new subsystem, not just schema/report/materialization.

## Group 1: Works Well Out Of The Box

These use default tables and paths the system already maintains.

1. Default event volume over time.

Question: "How many events happened over time?"

Query shape: count and error count by time bucket.

Current path: `event_density_1s`.

Status: good. This is the default global histogram/read path.

2. Global error threshold alerts.

Question: "Alert if error rate is above 1% for 5 minutes."

Query shape: `sum(error_count) / sum(count)` over recent buckets.

Current path: `event_density_1s`.

Status: good for tenant-wide error-rate and error-count alerts.

3. Global traffic anomaly alerts.

Question: "Alert if event volume drops by 80% or spikes 3x."

Query shape: compare recent `event_density_1s` buckets against a prior baseline.

Current path: `event_density_1s`.

Status: good for tenant-wide volume anomalies.

4. Latest events / default logs.

Question: "Show me the latest events and histogram."

Query shape: recent event page plus density histogram.

Current path: `events` plus `event_density_1s`.

Status: good for recent default browsing.

5. Single event lookup.

Question: "Show event `evt_123`."

Query shape: point lookup by `event_id`.

Current path: `events` bloom index, optionally followed by S3 byte-range fetch using `source_file`, `source_offset`, and `source_length`.

Status: good.

6. Bounded trace/span lookup.

Question: "Show events for this known `trace_id` or `span_id`."

Query shape: exact id filter over a bounded result set.

Current path: materialized `trace_id` / `span_id` plus bloom indexes.

Status: good when bounded. This is not the same as broad trace analytics.

## Group 2: Optimizable With Schema / Report / Materialization

These are not fast by default at 1M req/s scale, but the system has a clear optimization path. The raw table remains the source of truth; schema definitions, materialization jobs, and report outputs create serving paths.

Control-plane lifecycle:

1. Use SDK-managed definitions and explicit user/admin definitions for fields, measures, reports, cohorts, funnels, or alerts.
2. Materialize current supported outputs from Iceberg commits into ClickHouse serving tables.
3. Create bounded, retryable backfill or refresh work in `materialization_jobs` and `materialization_chunks` for larger published-version outputs.
4. Publish completed target versions in `materialization_versions` where readers need stable report versions.
5. Track incremental lag with serving or materialization watermarks.

This document describes the serving targets and query shapes. The current materializer handles promoted fields, measures, metric rollups, entity states, summary reports, trace summary reports, sequence reports, retention reports, and cohort memberships. Full published-version report jobs are separate implementation work.

### 7. Group by arbitrary event field

Question examples:

1. "Group events by browser."
2. "Group purchases by plan."
3. "Group traffic by country, user group, campaign, or feature flag."

General query:

```sql
SELECT
  data.<field> AS value,
  count() AS events
FROM observatory.events
WHERE tenant_id = {tenant_id:String}
  AND timestamp >= {from:DateTime64(3, 'UTC')}
GROUP BY value
ORDER BY events DESC;
```

Why raw is bad at 1M req/s:

The query has to scan the time range, extract `data.<field>` from raw JSON/subcolumns, build a group state for every distinct value, and sort the result. Over large windows, that is too much work for an interactive UI.

What to do:

Create a `field` definition with `mode='facet'` for bounded-cardinality fields.

Example definition config:

```json
{
  "kind": "field",
  "mode": "facet",
  "name": "plan",
  "config": {
    "path": "plan",
    "value_type": "string"
  },
  "backfill": {
    "from": "2026-05-01T00:00:00Z",
    "to": "2026-05-17T00:00:00Z"
  }
}
```

Backfill/materialization:

The definition backfill inserts matching historical rows into `field_index` through `materialization_jobs` and `materialization_chunks`. Future rows are maintained by a watermark-driven materializer for that definition.

Optimized serving path:

Use `field_index` ordered by `(tenant_id, mode, field_name, value_hash, value, timestamp, event_id, definition_id)`.

Post-optimization query:

```sql
SELECT
  value,
  uniqCombined64(event_id) AS events
FROM observatory.field_index
WHERE tenant_id = {tenant_id:String}
  AND mode = 'facet'
  AND field_name = 'plan'
  AND timestamp >= {from:DateTime64(3, 'UTC')}
GROUP BY value
ORDER BY events DESC
LIMIT 100;
```

Why faster:

The optimized query reads a narrow promoted index for one field instead of scanning wide raw event payloads and extracting `data.plan` from every row. The sort key clusters by tenant, mode, field name, and value hash, so ClickHouse can prune directly to the promoted field's row set.

Operational caveats:

Facet mode is for bounded dimensions like `plan`, `country`, `browser`, `variant`, or `service`. High-cardinality identifiers like `request_id` or `session_id` should usually be `lookup`, not `facet`, because value lists and group-bys approach event count.

### 8. Multi-field filters

Question examples:

1. "Show pro users in the US on enterprise accounts."
2. "Show checkout events for mobile Safari users in Canada."
3. "Show LLM calls where model is gpt-4.1 and provider is OpenAI."

General query:

```sql
SELECT
  event_id,
  timestamp,
  data
FROM observatory.events
WHERE tenant_id = {tenant_id:String}
  AND timestamp >= {from:DateTime64(3, 'UTC')}
  AND data.plan = 'pro'
  AND data.country = 'US'
  AND data.account_tier = 'enterprise'
ORDER BY timestamp DESC
LIMIT 100;
```

Why raw is bad at 1M req/s:

Raw predicates are evaluated row-by-row against the `events` table. Even if each predicate is selective, ClickHouse must find candidate rows without a serving index for those JSON paths.

What to do:

Promote every frequently used filter field. Use `facet` for low/medium cardinality fields and `lookup` for high-cardinality exact filters.

Backfill/materialization:

Backfill each field definition into `field_index`. For future events, the materializer advances each definition watermark and writes one index row per promoted field value.

Optimized serving path:

Build candidate event sets from `field_index` and intersect them by `event_id`, or use semi-joins. Then fetch the final event page from either `field_index` metadata or `events`.

Post-optimization query:

```sql
SELECT
  i.event_id,
  i.timestamp,
  i.event_type,
  i.signal,
  i.trace_id,
  i.span_id,
  i.name
FROM observatory.field_index AS i
WHERE i.tenant_id = {tenant_id:String}
  AND i.field_name = 'plan'
  AND i.value = 'pro'
  AND i.timestamp >= {from:DateTime64(3, 'UTC')}
  AND i.event_id IN (
    SELECT event_id
    FROM observatory.field_index
    WHERE tenant_id = {tenant_id:String}
      AND field_name = 'country'
      AND value = 'US'
      AND timestamp >= {from:DateTime64(3, 'UTC')}
  )
  AND i.event_id IN (
    SELECT event_id
    FROM observatory.field_index
    WHERE tenant_id = {tenant_id:String}
      AND field_name = 'account_tier'
      AND value = 'enterprise'
      AND timestamp >= {from:DateTime64(3, 'UTC')}
  )
ORDER BY i.timestamp DESC
LIMIT 100;
```

Why faster:

Each predicate becomes an exact lookup in a narrow table keyed by `(tenant_id, field_name, value)`. The query intersects candidate event ids after pruning each field independently, instead of evaluating all JSON predicates against every raw event in the time window.

Operational caveats:

The selected field set matters. Promoting every possible dynamic path recreates the write fanout problem. Promotion should happen through explicit SDK-managed defaults or user/admin action.

### 9. Dimensioned alerts

Question examples:

1. "Alert if checkout error rate for enterprise users exceeds 2%."
2. "Alert if p95 latency for service=api and route=/checkout exceeds 2 seconds."
3. "Alert if model=gpt-4.1 errors spike for one customer."

General query:

```sql
SELECT
  toStartOfMinute(timestamp) AS bucket,
  count() AS events,
  countIf(ifNull(data.is_error, 0) != 0 OR endsWith(lower(event_type), '_error')) AS errors,
  errors / nullIf(events, 0) AS error_rate
FROM observatory.events
WHERE tenant_id = {tenant_id:String}
  AND timestamp >= now() - INTERVAL 5 MINUTE
  AND data.service = 'checkout'
  AND data.plan = 'enterprise'
GROUP BY bucket
HAVING error_rate >= 0.02
ORDER BY bucket DESC;
```

Why raw is bad at 1M req/s:

An alert runs repeatedly. A raw scan every few seconds or every minute over arbitrary dimensions turns into permanent background load.

What to do:

Promote alert dimensions such as `service`, `route`, `plan`, `account_id`, `model`, or `region`. For threshold metrics, create a report or alert rollup keyed by the dimensions needed by the rule.

Backfill/materialization:

Backfill fields for historical baseline comparisons. For forward-only threshold alerts, start materializing from creation time. A dedicated alert evaluator should read the promoted/report path, not raw events.

Optimized serving path:

For global alerts, use `event_density_1s`. For dimensioned alerts, use `field_index` plus a report result/rollup table. The current schema has `report_results`; a recurring evaluator/materializer should populate it for production-grade alerting.

Post-optimization query:

```sql
SELECT
  bucket_time,
  toFloat64(metrics.error_rate) AS error_rate,
  toUInt64(metrics.events) AS events,
  toUInt64(metrics.errors) AS errors
FROM observatory.report_results
WHERE tenant_id = {tenant_id:String}
  AND report_id = 'checkout_error_rate_by_plan'
  AND bucket_time >= now64(3) - INTERVAL 5 MINUTE
  AND dimensions.service = 'checkout'
  AND dimensions.plan = 'enterprise'
ORDER BY bucket_time DESC;
```

Why faster:

The alert evaluator reads a few precomputed bucket rows instead of recomputing error counts over raw events every evaluation interval. The expensive filter/group/count work happens once during report materialization, not every time the alert rule checks its threshold.

Operational caveats:

Near-real-time alert latency is bounded by the async ingest path: local file -> S3 -> SQS -> loader -> ClickHouse -> evaluator. If the product requires millisecond alerts before S3/ClickHouse visibility, that becomes Group 3.

### 10. Arbitrary raw JSON predicate alert

Question examples:

1. "Alert when `data.foo='bar' AND data.region='us-east'` crosses a threshold."
2. "Alert when a custom customer-defined field becomes `failed`."
3. "Alert when a new product property matches a specific value."

General query:

```sql
SELECT
  toStartOfMinute(timestamp) AS bucket,
  count() AS matching_events
FROM observatory.events
WHERE tenant_id = {tenant_id:String}
  AND timestamp >= now() - INTERVAL 1 MINUTE
  AND data.foo = 'bar'
  AND data.region = 'us-east'
GROUP BY bucket
HAVING matching_events >= {threshold:UInt64}
ORDER BY bucket DESC;
```

Why raw is bad at 1M req/s:

The predicate may be arbitrary, but once it becomes an alert it is evaluated repeatedly on a schedule. Re-running the predicate from raw `events` every minute creates persistent full-scan pressure.

What to do:

Convert the predicate fields into schema definitions. If the alert needs a historical baseline, backfill those fields. If the alert only needs future detection, materialize from the point the rule is created.

Backfill/materialization:

Create `field` definitions for all predicate paths. Use report/alert materialization for the threshold aggregation itself.

Optimized serving path:

Use `field_index` to find matching events and a report/alert result table for repeated threshold evaluation.

Post-optimization query:

```sql
SELECT
  bucket_time,
  toUInt64(metrics.matching_events) AS matching_events
FROM observatory.report_results
WHERE tenant_id = {tenant_id:String}
  AND report_id = 'foo_bar_region_us_east_alert'
  AND bucket_time >= now64(3) - INTERVAL 1 MINUTE
HAVING matching_events >= {threshold:UInt64}
ORDER BY bucket_time DESC;
```

Why faster:

The arbitrary predicate is paid for during field promotion and report materialization. Runtime alert checks read a tiny report result table keyed by report id and bucket time, instead of scanning the raw event stream every minute.

Operational caveats:

This category is Group 2, not Group 3. The requirement is that the user or system must promote the predicate before treating it as a reliable low-cost alert.

### 11. Numeric metrics and percentiles

Question examples:

1. "What is p95 latency by service?"
2. "What is p99 LLM call duration by model?"
3. "What is average tool execution time by tool name?"

General query:

```sql
SELECT
  service,
  quantileTDigest(0.95)(toFloat64(data.duration_ms)) AS p95_ms
FROM observatory.events
WHERE timestamp >= {from:DateTime64(3, 'UTC')}
GROUP BY service;
```

Why raw is bad at 1M req/s:

Percentiles require reading all numeric values in the window and building quantile states. Doing that from raw JSON for every dashboard refresh is expensive.

What to do:

Create a `measure` definition for the numeric path. If the percentile is grouped by a dimension, create a `rollup` definition with that dimension.

Example configs:

```json
{
  "kind": "measure",
  "mode": "measure",
  "name": "duration_ms",
  "config": {
    "path": "duration_ms",
    "unit": "ms"
  }
}
```

```json
{
  "kind": "rollup",
  "mode": "measure_rollup",
  "name": "duration_ms.by.service",
  "config": {
    "path": "duration_ms",
    "unit": "ms",
    "dimension": "service",
    "aggregates": ["count", "avg", "p50", "p95", "p99"]
  }
}
```

Backfill/materialization:

Backfill writes numeric rows into `event_measures`. `mv_measure_rollups` maintains aggregate states in `measure_rollups`.

Optimized serving path:

Read `measure_rollups` for repeated percentile dashboards. Use `event_measures` for narrower ad hoc measure queries.

Post-optimization query:

```sql
SELECT
  bucket_time,
  dimension_value AS service,
  quantilesTDigestMerge(0.5, 0.9, 0.95, 0.99)(quantiles_state)[3] AS p95_ms
FROM observatory.measure_rollups
WHERE tenant_id = {tenant_id:String}
  AND measure_name = 'duration_ms'
  AND dimension_name = 'service'
  AND bucket_time >= {from:DateTime64(3, 'UTC')}
GROUP BY bucket_time, service
ORDER BY bucket_time DESC, p95_ms DESC;
```

Why faster:

The raw query builds quantile states from every matching event. The optimized query merges already-built aggregate states from 5-minute buckets, so work scales with number of buckets and services rather than number of raw events.

Operational caveats:

The current `measure_rollups` bucket is 5 minutes. If the product needs 1-second or 1-hour measure buckets, add bucket-specific report/rollup materialization rather than recomputing from raw.

### 12. Revenue analytics

Question examples:

1. "Revenue by product over time."
2. "Revenue by plan and campaign."
3. "Average order value by cohort."

General query:

```sql
SELECT
  toStartOfDay(timestamp) AS day,
  data.product_id,
  sum(toFloat64(data.revenue)) AS revenue
FROM observatory.events
WHERE timestamp >= {from:DateTime64(3, 'UTC')}
GROUP BY day, data.product_id;
```

Why raw is bad at 1M req/s:

Revenue queries combine numeric aggregation, business dimensions, and long time windows. Raw scans become expensive and will compete with ingest.

What to do:

Promote `revenue` as a measure. Promote grouping dimensions such as `product_id`, `plan`, `utm_campaign`, `currency`, and `country`. Create summary reports for dashboards that are repeatedly viewed.

Backfill/materialization:

Backfill `event_measures` for revenue and `field_index` for dimensions. Materialize summary outputs into `report_results` for repeated breakdowns.

Optimized serving path:

Use `measure_rollups` for time-series revenue metrics and `report_results` for dashboard-ready business cuts.

Post-optimization query:

```sql
SELECT
  bucket_time,
  dimension_value AS product_id,
  sumMerge(sum_state) AS revenue
FROM observatory.measure_rollups
WHERE tenant_id = {tenant_id:String}
  AND measure_name = 'revenue'
  AND dimension_name = 'product_id'
  AND bucket_time >= {from:DateTime64(3, 'UTC')}
GROUP BY bucket_time, product_id
ORDER BY bucket_time DESC, revenue DESC;
```

Why faster:

The optimized path reads aggregated revenue states by product and bucket. It avoids scanning raw purchase events, parsing `data.revenue`, grouping by `data.product_id`, and summing from scratch for every dashboard load.

Operational caveats:

Currency normalization and refunds are semantic problems, not storage problems. If revenue can be negative, multi-currency, or corrected later, define that in the report semantics before treating rollups as authoritative finance numbers.

### 13. Active users/accounts/sessions

Question examples:

1. "What is DAU by plan?"
2. "How many active accounts did we have this week?"
3. "How many sessions used feature X?"

General query:

```sql
SELECT
  toStartOfDay(timestamp) AS day,
  uniqCombined64(data.user_id) AS active_users
FROM observatory.events
WHERE timestamp >= {from:DateTime64(3, 'UTC')}
GROUP BY day;
```

Why raw is bad at 1M req/s:

Distinct entity counts create large aggregation state. Adding filters and dimensions increases both scan cost and state size.

What to do:

Promote identity paths such as `user_id`, `account_id`, and `session_id`. Promote dimensions used to slice activity. For repeated DAU/WAU/MAU views, create report materialization.

Backfill/materialization:

Backfill identity fields into `field_index`. For repeated reporting, compute and write aggregate results into `report_results`.

Optimized serving path:

Use `field_index` for exact activity membership and `report_results` for dashboard-ready counts.

Post-optimization query:

```sql
SELECT
  bucket_time,
  toUInt64(metrics.active_users) AS active_users,
  dimensions.plan AS plan
FROM observatory.report_results
WHERE tenant_id = {tenant_id:String}
  AND report_id = 'daily_active_users_by_plan'
  AND bucket_time >= {from:DateTime64(3, 'UTC')}
ORDER BY bucket_time DESC, active_users DESC;
```

Why faster:

Distinct entity counting is expensive because it builds large per-query aggregation state. A report materialization computes the distinct set once per bucket/dimension and stores the result, so UI reads are proportional to rendered buckets rather than raw activity events.

Operational caveats:

Exact distinct counts over arbitrary filter combinations can still be expensive. For very large windows, prefer approximate sketches or precomputed report outputs.

### 14. Top-N entities

Question examples:

1. "Top accounts by event volume."
2. "Top customers by revenue."
3. "Top users by error count."

General query:

```sql
SELECT
  data.account_id,
  count() AS events
FROM observatory.events
WHERE timestamp >= {from:DateTime64(3, 'UTC')}
GROUP BY data.account_id
ORDER BY events DESC
LIMIT 100;
```

Why raw is bad at 1M req/s:

Entity identifiers are high-cardinality. Grouping them from raw data can approach one group per active entity and requires a large top-N sort.

What to do:

Promote the entity id as `lookup` or a carefully controlled facet. Promote measures needed for ranking. Create a report for each repeated leaderboard.

Backfill/materialization:

Backfill entity id rows into `field_index`; backfill measures if the rank is not event count. Materialize the leaderboard into `report_results`.

Optimized serving path:

Use `report_results` for top-N dashboards. Use `field_index` for drilling into a selected entity.

Post-optimization query:

```sql
SELECT
  bucket_time,
  dimensions.account_id AS account_id,
  toUInt64(metrics.events) AS events,
  toFloat64(metrics.revenue) AS revenue
FROM observatory.report_results
WHERE tenant_id = {tenant_id:String}
  AND report_id = 'top_accounts_by_revenue'
  AND bucket_time >= {from:DateTime64(3, 'UTC')}
ORDER BY bucket_time DESC, revenue DESC
LIMIT 100;
```

Why faster:

The report stores the ranked entity result set or pre-aggregated entity metrics. Reads avoid a high-cardinality raw `GROUP BY account_id` and top-N sort across the full event window.

Operational caveats:

Do not expose high-cardinality value lists naively. The UI should search exact entity ids or show a precomputed top-N, not paginate every possible id from `field_index`.

### 15. Funnel conversion

Question examples:

1. "Signup -> invite teammate -> checkout within 7 days."
2. "Page view -> add to cart -> purchase in one session."
3. "Agent plan -> tool call -> successful final answer."

General query:

```sql
SELECT
  data.user_id AS user_id,
  windowFunnel(7 * 24 * 3600)(
    timestamp,
    data.event_type = 'signup_completed',
    data.event_type = 'teammate_invited',
    data.event_type = 'checkout_completed'
  ) AS reached_step
FROM observatory.events
WHERE tenant_id = {tenant_id:String}
  AND timestamp >= {from:DateTime64(3, 'UTC')}
  AND data.user_id != ''
  AND data.event_type IN ('signup_completed', 'teammate_invited', 'checkout_completed')
GROUP BY user_id;
```

Why raw is bad at 1M req/s:

Funnels require ordering events per entity and matching sequences. Running that directly over raw events for each dashboard refresh is effectively a historical entity replay.

What to do:

Create a sequence report with:

1. Entity id path, such as `user_id`, `session_id`, `account_id`, or `trace_id`.
2. Step definitions, such as event type/name/path filters.
3. Window, such as `7d`.
4. Optional dimensions, such as plan or country.

Backfill/materialization:

Backfill the sequence report over historical raw events and write step counts into `sequence_report_results`.

Optimized serving path:

Read `sequence_report_results` by report id, bucket time, and step index.

Post-optimization query:

```sql
SELECT
  bucket_time,
  step_index,
  step_name,
  entity_count,
  conversion_count
FROM observatory.sequence_report_results
WHERE tenant_id = {tenant_id:String}
  AND report_id = 'signup_invite_checkout_7d'
  AND bucket_time >= {from:DateTime64(3, 'UTC')}
ORDER BY bucket_time ASC, step_index ASC;
```

Why faster:

The raw query orders and sequence-matches events per entity. The optimized query reads one row per report step and bucket after the materializer has already performed the per-entity sequence work.

Operational caveats:

The current materializer can populate `sequence_report_results` from explicit sequence definitions. Full published-version report jobs are still separate production hardening work.

### 16. Retention cohorts

Question examples:

1. "Users who joined in June and returned in week 1, 2, and 4."
2. "Accounts that installed integration X and were active after 30 days."
3. "Users in variant B retained better than variant A."

General query:

```sql
WITH cohort AS (
  SELECT
    data.user_id AS user_id,
    min(timestamp) AS joined_at
  FROM observatory.events
  WHERE tenant_id = {tenant_id:String}
    AND data.event_type = 'signup_completed'
    AND timestamp >= toDateTime64('2026-06-01 00:00:00', 3, 'UTC')
    AND timestamp < toDateTime64('2026-07-01 00:00:00', 3, 'UTC')
  GROUP BY user_id
)
SELECT
  dateDiff('week', cohort.joined_at, e.timestamp) AS retention_week,
  uniqCombined64(cohort.user_id) AS retained_users
FROM cohort
INNER JOIN observatory.events AS e
  ON e.tenant_id = {tenant_id:String}
 AND e.data.user_id = cohort.user_id
WHERE e.timestamp >= cohort.joined_at
  AND e.timestamp < cohort.joined_at + INTERVAL 8 WEEK
  AND e.data.event_type = 'app_opened'
GROUP BY retention_week
ORDER BY retention_week ASC;
```

Why raw is bad at 1M req/s:

Retention queries require entity membership, date bucketing, and repeated future-window scans. Recomputing membership and activity from raw events is too expensive.

What to do:

Define:

1. Entity id path.
2. Cohort entry condition, such as `joined_at`, `signup_completed`, or `first_purchase`.
3. Activity event condition.
4. Retention buckets, such as day 1, week 1, week 4.
5. Optional segmentation dimensions.

Backfill/materialization:

Backfill cohort membership into `cohort_memberships`. Materialize retention counts into `report_results`.

Optimized serving path:

Use `cohort_memberships` for membership and `report_results` for rendered retention tables/curves.

Post-optimization query:

```sql
SELECT
  bucket_time,
  dimensions.retention_week AS retention_week,
  toUInt64(metrics.retained_users) AS retained_users,
  toUInt64(metrics.cohort_size) AS cohort_size,
  toFloat64(metrics.retention_rate) AS retention_rate
FROM observatory.report_results
WHERE tenant_id = {tenant_id:String}
  AND report_id = 'june_signup_weekly_retention'
ORDER BY retention_week ASC;
```

Why faster:

The expensive work is split into cohort membership materialization and retention result materialization. Reads no longer rebuild the cohort or join future activity from raw events; they fetch precomputed retention buckets.

Operational caveats:

State corrections matter. If `joined_at` or account ownership can change, define whether retention uses first observed value, latest corrected value, or a separate source of truth.

### 17. Entity state at time

Question examples:

1. "What plan did this account have on May 1?"
2. "Which country was this user in when they converted?"
3. "What model/provider was active for this trace step?"

General query:

```sql
SELECT
  data.account_id AS account_id,
  argMax(data.plan, timestamp) AS plan_at_time
FROM observatory.events
WHERE tenant_id = {tenant_id:String}
  AND timestamp <= {as_of:DateTime64(3, 'UTC')}
  AND data.account_id != ''
  AND data.plan != ''
GROUP BY account_id;
```

Why raw is bad at 1M req/s:

Without state materialization, the system must replay all state-changing events for the entity or population before the query timestamp.

What to do:

Create `state` definitions with:

1. `entity_type`, such as `user`, `account`, `session`, `trace`, or `host`.
2. `entity_id_path`, such as `user_id`.
3. State value path, such as `plan`, `country`, `feature_flag`, or `environment`.
4. Value type.

Example:

```json
{
  "kind": "state",
  "mode": "state_transition",
  "name": "account.plan",
  "config": {
    "entity_type": "account",
    "entity_id_path": "account_id",
    "path": "plan",
    "value_type": "string"
  }
}
```

Backfill/materialization:

Backfill historical transitions into `entity_state_updates`. Future events are maintained by a watermark-driven state materializer.

Optimized serving path:

Query `entity_state_updates` for latest value by `(tenant_id, entity_type, entity_hash, state_name, timestamp)`.

Post-optimization query:

```sql
SELECT
  entity_id,
  argMax(value, timestamp) AS plan_at_time
FROM observatory.entity_state_updates
WHERE tenant_id = {tenant_id:String}
  AND entity_type = 'account'
  AND state_name = 'account.plan'
  AND timestamp <= {as_of:DateTime64(3, 'UTC')}
GROUP BY entity_id;
```

Why faster:

The optimized query scans only state transition rows for one entity type and state name. It does not replay every raw account event or parse unrelated payload fields.

Operational caveats:

`entity_state_updates` stores transitions/history. Very frequent state changes or large as-of joins may need snapshot tables later.

### 18. Experiment analysis

Question examples:

1. "Conversion by experiment variant."
2. "Revenue by variant."
3. "Error rate by feature flag."

General query:

```sql
SELECT
  data.experiment_id AS experiment_id,
  data.variant AS variant,
  uniqCombined64(data.user_id) AS exposed_users,
  uniqCombined64If(data.user_id, data.event_type = 'checkout_completed') AS converted_users,
  sumIf(toFloat64(data.revenue), data.event_type = 'checkout_completed') AS revenue
FROM observatory.events
WHERE tenant_id = {tenant_id:String}
  AND timestamp >= {from:DateTime64(3, 'UTC')}
  AND data.experiment_id = 'checkout_flow'
GROUP BY experiment_id, variant;
```

Why raw is bad at 1M req/s:

Experiment reads combine dimensions, measures, and sometimes sequence/funnel logic. Raw scans are too costly for repeated dashboard use.

What to do:

Promote experiment fields as facets. Promote relevant measures like `revenue`, `duration_ms`, or `conversion_value`. For conversion, define a sequence or summary report.

Backfill/materialization:

Backfill experiment fields into `field_index` and measures into `event_measures`. Materialize variant summaries into `report_results`.

Optimized serving path:

Use `field_index` for variant filtering/grouping, `measure_rollups` for numeric metrics, and `report_results` for the final experiment dashboard.

Post-optimization query:

```sql
SELECT
  dimensions.variant AS variant,
  toUInt64(metrics.exposed_users) AS exposed_users,
  toUInt64(metrics.converted_users) AS converted_users,
  toFloat64(metrics.revenue) AS revenue,
  toFloat64(metrics.conversion_rate) AS conversion_rate
FROM observatory.report_results
WHERE tenant_id = {tenant_id:String}
  AND report_id = 'checkout_flow_experiment'
  AND bucket_time >= {from:DateTime64(3, 'UTC')}
ORDER BY conversion_rate DESC;
```

Why faster:

Experiment dashboards often combine exposure membership, conversion events, and measures. The optimized report stores those semantics once per variant/bucket instead of recomputing distinct users, conversions, and revenue from raw events.

Operational caveats:

Attribution semantics need to be explicit. Decide whether variant assignment is read from each event, first assignment per user, latest assignment, or state at event time.

### 19. Broad trace summary analytics

Question examples:

1. "Top slow traces by service over 24h."
2. "Trace duration p95 by workflow."
3. "Failing traces by root span name."

General query:

```sql
SELECT
  trace_id,
  min(timestamp) AS started_at,
  max(timestamp) AS ended_at,
  dateDiff('millisecond', started_at, ended_at) AS duration_ms,
  count() AS event_count,
  countIf(ifNull(data.is_error, 0) != 0 OR endsWith(lower(event_type), '_error')) AS errors,
  anyIf(data.service, data.parent_span_id = '') AS root_service,
  anyIf(data.name, data.parent_span_id = '') AS root_name
FROM observatory.events
WHERE tenant_id = {tenant_id:String}
  AND timestamp >= {from:DateTime64(3, 'UTC')}
  AND trace_id != ''
GROUP BY trace_id
ORDER BY duration_ms DESC
LIMIT 100;
```

Why raw is bad at 1M req/s:

Trace analytics over broad windows requires grouping many events by `trace_id`, deriving start/end/root/error semantics, and ranking the resulting traces.

What to do:

Create a trace report model rather than reintroducing always-on trace serving tables for every event. The report should define:

1. Trace id path.
2. Span id and parent span id paths if flamegraph semantics are needed.
3. Duration/start/end semantics.
4. Root service/name semantics.
5. Dimensions to group by.

Backfill/materialization:

Create an explicit `report` definition with `mode = 'trace_summary'`. The materializer groups matched span-shaped events by tenant, bucket, report, version, and `trace_id`, then writes one per-trace summary row into `report_results`. The row dimensions include `trace_id` plus configured dimensions and best-effort root `service`/`name`; metrics include `duration_ms`, `event_count`, and `errors`.

Optimized serving path:

Use report materialization for broad trace lists and rankings. Use raw `events` lookup for opening one selected trace.

Post-optimization query:

```sql
SELECT
  bucket_time,
  dimensions.trace_id AS trace_id,
  dimensions.root_service AS root_service,
  dimensions.root_name AS root_name,
  toFloat64(metrics.duration_ms) AS duration_ms,
  toUInt64(metrics.errors) AS errors
FROM observatory.report_results
WHERE tenant_id = {tenant_id:String}
  AND report_id = 'top_slow_traces'
  AND bucket_time >= {from:DateTime64(3, 'UTC')}
ORDER BY duration_ms DESC
LIMIT 100;
```

Why faster:

Broad trace analytics require grouping raw events into traces before ranking. A trace report stores per-trace summaries, so the list query ranks summary rows rather than grouping millions or billions of raw span/log events by `trace_id`.

Operational caveats:

This is Group 2 because it is optimized with an explicit report model. Raw `events` remain the source of truth for opening a selected trace and for trace shapes that have not been defined as reports.

### 20. Reusable dashboards

Question examples:

1. "Show this same filtered metric every day."
2. "Maintain this team dashboard with 20 saved charts."
3. "Share a customer-specific analytics report."

General query:

```sql
SELECT
  toStartOfInterval(timestamp, INTERVAL 5 MINUTE) AS bucket,
  data.plan AS plan,
  data.country AS country,
  count() AS events,
  countIf(ifNull(data.is_error, 0) != 0 OR endsWith(lower(event_type), '_error')) AS errors,
  quantileTDigest(0.95)(toFloat64(data.duration_ms)) AS p95_ms
FROM observatory.events
WHERE tenant_id = {tenant_id:String}
  AND timestamp >= {from:DateTime64(3, 'UTC')}
  AND data.service = 'api'
GROUP BY bucket, plan, country
ORDER BY bucket DESC, events DESC;
```

Why raw is bad at 1M req/s:

Repeated raw dashboard queries create predictable load. The system should pay the compute cost once during backfill/materialization, not every page load.

What to do:

Save the query as a report. Promote required fields and measures. Choose bucket size and refresh cadence. Backfill historical windows once.

Backfill/materialization:

Write results into `report_results` or a more specialized table such as `sequence_report_results`.

Optimized serving path:

Dashboard reads should hit precomputed report tables and only query raw events for drill-down.

Post-optimization query:

```sql
SELECT
  bucket_time,
  dimensions.plan AS plan,
  dimensions.country AS country,
  toUInt64(metrics.events) AS events,
  toUInt64(metrics.errors) AS errors,
  toFloat64(metrics.p95_ms) AS p95_ms
FROM observatory.report_results
WHERE tenant_id = {tenant_id:String}
  AND report_id = 'api_health_by_plan_country'
  AND bucket_time >= {from:DateTime64(3, 'UTC')}
ORDER BY bucket_time DESC, events DESC;
```

Why faster:

The dashboard no longer executes a multi-dimensional raw aggregate on every page load. The materializer pays the scan/group/quantile cost once per refresh interval, and the UI reads compact result rows.

Operational caveats:

The current report API stores report metadata in Postgres. Materialization into ClickHouse result tables requires a report materialization executor. Treat that executor as required production infrastructure for this category.

## Group 3: Fundamentally Not Optimizable In The Current System

These need a new subsystem, not just schema/report/materialization.

21. Free-text search over payloads/log bodies.

Question: "Find events containing 'timeout connecting to redis'."

Query shape: substring/token search over body/message/full JSON.

Why not optimizable now: field promotion helps exact fields, not general text search.

Needed new subsystem: inverted index, ClickHouse text/token indexes, dedicated log-search table, or external search engine.

22. Regex over arbitrary JSON.

Question: "Find payloads matching this regex."

Query shape: `match(toJSONString(data), ...)`.

Why not optimizable now: arbitrary regex remains a payload scan even if some fields are promoted.

Needed new subsystem: search/indexing engine or constrained extracted-field model.

23. Exact distinct over huge arbitrary filtered windows.

Question: "Exact distinct users over 180 days for any filter combination."

Query shape: `uniqExact` over massive event sets.

Why not optimizable now: exactness plus arbitrary filters creates huge state. Existing report paths can optimize known dimensions, not every arbitrary exact set.

Needed new subsystem: sketch-based cubes, offline jobs, or constrained report definitions.

24. Full historical entity replay on demand.

Question: "Recompute every user's complete lifecycle from raw events now."

Query shape: sort and replay all events per entity.

Why not optimizable now: state definitions optimize selected state transitions, not arbitrary lifecycle replay.

Needed new subsystem: entity snapshot/event-sourcing worker or offline batch job.

25. Cross-tenant/global analytics.

Question: "Compare every tenant globally."

Query shape: aggregate without tenant pruning.

Why not optimizable now: system is designed tenant-first for isolation and pruning.

Needed new subsystem: explicit admin/global rollups with governance.

26. Arbitrary joins with external datasets.

Question: "Join all events to CRM/account records by email/domain."

Query shape: large join against external business data.

Why not optimizable now: no dimension-table ingestion/dictionary model yet.

Needed new subsystem: managed dimension tables, dictionaries, or enrichment pipeline.

27. Ultra-low-latency streaming alerts.

Question: "Alert within milliseconds of a matching event."

Query shape: streaming rule evaluation before object-store/loader delay.

Why not optimizable now: pipeline is durable async batch/object-store oriented.

Needed new subsystem: streaming rules engine or synchronous alert side path.

## Product Rule

Group 1 should run immediately.

Group 2 should create or select a definition/report, enqueue materialization work, and serve completed versions instead of silently running a large raw scan.

Group 3 should be presented as unsupported or offline/new-subsystem work, not as an interactive query.
