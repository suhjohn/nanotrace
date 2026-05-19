---
name: nanotrace-query-optimization-observer
description: Use when analyzing Nanotrace query_usage history to discover repeated expensive raw query patterns, classify them into promotion actions, and recommend or create definitions, reports, materialization jobs, or new-subsystem work to make future queries faster.
---

# Nanotrace Query Optimization Observer

## Goal

Turn observed query history into concrete optimization work.

This is the meta-level skill above the promotion skills. Start from `observatory.query_usage`, identify recurring expensive shapes, classify each shape with `nanotrace-promotion-classifier`, then route to the smallest specific promotion skill.

## Read Models To Inspect

Primary table:

- `observatory.query_usage`: query fingerprints, source tables, JSON/filter/group paths, result rows, read rows, read bytes, latency, status, and attributes.

Current recorder note: the schema has `surface`, `plan_kind`, `is_raw_fallback`, `json_paths`, `filter_paths`, and `group_by_paths`, but older writer paths may leave some of these at defaults. When they are empty, classify from `query_shape`, `source_tables`, and `attributes`, and consider adding recorder enrichment as part of the optimization.

Useful supporting tables:

- `observatory.definitions`: existing field, measure, rollup, and state definitions.
- `observatory.definition_stats`: cardinality, scan, storage, and recommendation metadata when populated.
- `observatory.optimization_recommendations`: proposed materialization records when populated.
- `observatory.serving_watermarks`: whether promoted read models are caught up.
- `observatory.pipeline_metrics`: whether materializers/loaders are lagging.

## Discovery Queries

Start with repeated expensive shapes:

```sql
SELECT
  query_hash,
  anyLast(query_shape) AS query_shape,
  count() AS runs,
  sum(read_bytes) AS read_bytes,
  sum(read_rows) AS read_rows,
  quantileTDigest(0.95)(elapsed_ms) AS p95_ms,
  arrayDistinct(arrayFlatten(groupArray(source_tables))) AS source_tables,
  max(observed_at) AS last_seen
FROM observatory.query_usage
WHERE observed_at >= now64(3) - INTERVAL 7 DAY
GROUP BY query_hash
HAVING runs >= 3 OR read_bytes > 1000000000 OR p95_ms > 1000
ORDER BY read_bytes DESC
LIMIT 50;
```

Then inspect one shape:

```sql
SELECT *
FROM observatory.query_usage
WHERE query_hash = {query_hash:UInt64}
ORDER BY observed_at DESC
LIMIT 20;
```

Check whether a relevant definition already exists:

```sql
SELECT name, kind, mode, config, updated_at
FROM observatory.definitions FINAL
WHERE tenant_id = {tenant_id:String}
  AND isNull(deleted_at)
ORDER BY updated_at DESC;
```

## Classification Workflow

For each candidate query shape:

1. Confirm it is repeated or expensive enough to optimize.
2. Identify the semantic shape:
   - Raw JSON group-by.
   - Raw JSON filter/intersection.
   - Exact high-cardinality lookup.
   - Numeric extraction or percentile.
   - Time-bucketed trend.
   - Scheduled alert predicate.
   - Dashboard/report summary.
   - Top-N entity ranking.
   - Funnel/sequence.
   - Cohort/retention.
   - Entity state as-of/latest.
   - Broad trace summary.
   - Unsupported boundary.
3. Check existing definitions and materialized serving paths before proposing new ones.
4. Estimate cardinality and rows written per event when the promotion adds fanout.
5. Choose the narrowest promotion.
6. Return a recommendation or implement the definition/materialization work if requested.

## Route To Specific Skills

- Raw low/medium-cardinality group/filter: `nanotrace-field-facet-promotion`.
- Raw exact high-cardinality lookup: `nanotrace-field-lookup-promotion`.
- Numeric extraction: `nanotrace-measure-promotion`.
- Repeated bucketed numeric aggregate: `nanotrace-measure-rollup-promotion`.
- Latest/as-of entity state: `nanotrace-state-promotion`.
- Repeated dashboard/summary: `nanotrace-report-promotion`.
- Scheduled threshold/predicate: `nanotrace-alert-rollup-promotion`.
- High-cardinality leaderboard: `nanotrace-topn-entity-promotion`.
- Ordered conversion path: `nanotrace-sequence-funnel-promotion`.
- Cohort/retention analysis: `nanotrace-cohort-retention-promotion`.
- Broad trace analytics: `nanotrace-trace-summary-promotion`.
- Unsupported/new-subsystem cases: `nanotrace-promotion-boundary`.
- High-cardinality guardrails: `nanotrace-high-cardinality-aggregation`.

## Recommendation Format

For each recommendation, include:

- Query hash and representative query shape.
- Evidence: runs, read bytes/rows, p95 latency, last seen, source tables.
- Why raw is expensive.
- Existing definitions that already cover or partially cover it.
- Proposed promotion action and target serving table.
- Definition/report config sketch.
- Backfill window and freshness requirement.
- Cardinality/storage risk.
- Whether current code can materialize it now or it needs a future executor.

## Implementation Rules

- Do not recommend a new definition if an existing caught-up serving path already answers the query.
- Do not promote one-off exploration.
- Prefer exact lookup over aggregation for high-cardinality IDs.
- Prefer report promotion for product semantics that combine multiple fields/measures/ratios.
- Clearly separate implemented paths (`field_index`, `event_measures`, `entity_state_updates`, `measure_rollups`) from planned paths (`report_results`, `sequence_report_results`, `cohort_memberships`) unless the executor exists in current code.
