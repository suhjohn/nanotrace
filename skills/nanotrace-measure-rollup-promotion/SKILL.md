---
name: nanotrace-measure-rollup-promotion
description: Promote repeated Nanotrace time-bucketed numeric aggregate or percentile queries into measure_rollups.
---

# Nanotrace Measure Rollup Promotion

Use this skill when an agent observes repeated Nanotrace questions that ask for numeric metrics over time buckets.

## Trigger Scenarios

Trigger measure rollup promotion when users repeatedly ask for:

- Time series of counts, sums, averages, min/max, rates, or percentiles.
- Bucketed latency, duration, token, cost, error, retry, throughput, or queue metrics.
- The same numeric metric sliced by stable dimensions such as project, model, route, workspace, agent, tool, or status.
- Dashboards that repeatedly compute the same bucketed aggregates from raw events.
- Slow queries that scan large event ranges only to produce numeric time buckets.

Good examples:

- "p95 tool latency by hour for the last 30 days"
- "daily token usage by model"
- "error rate per route every 5 minutes"
- "average agent run duration grouped by workspace"

## Observation Signals

Look for these signals before promoting:

- Query shape has `GROUP BY time_bucket(...)` or an equivalent date truncation.
- Metric is numeric and aggregatable.
- Percentiles or histograms are recomputed frequently from raw spans/events.
- Time range varies, but bucket size and dimensions are stable.
- Product workflow needs trend charts, alert panels, or repeated operational monitoring.
- Raw event freshness is less important than predictable low-latency reads.

## Promotion Action

Promote through a numeric `measure` definition first. Use a `rollup` definition when the product needs explicit aggregate semantics beyond the default `measure_rollups` materialized view.

Config sketch:

```json
{
  "name": "duration_ms_by_service",
  "kind": "rollup",
  "mode": "measure_rollup",
  "config": {
    "path": "duration_ms",
    "dimension": "service",
    "bucket": "5m",
    "aggregates": ["count", "avg", "p50", "p95", "p99"]
  }
}
```

Choose the smallest dimension set that supports the repeated questions. Add percentile sketches or precomputed percentile fields only when percentile queries are frequent enough to justify storage and refresh cost.

## Preferred Serving Path

Serve trend charts and repeated aggregate queries from `measure_rollups` first.

Use raw events only for:

- Drill-down into individual traces or spans.
- One-off dimensions not present in the rollup.
- Fresh tail data inside the configured lateness window.
- Backfill validation and reconciliation.

When possible, merge fresh raw tail rows with persisted rollups so users get both speed and recent data.

## Caveats

- Do not use measure rollups for "latest value" or "state as of time" questions; use state promotion instead.
- Avoid high-cardinality dimensions unless the product scenario clearly needs them.
- Percentile rollups need consistent sketching or approximation semantics; do not mix incompatible percentile methods.
- Bucket size is part of the product contract. Changing it may require a new rollup or backfill.
- Include tenant and access-control dimensions needed to serve safely without raw-table joins.
