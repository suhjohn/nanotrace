---
name: nanotrace-promotion-classifier
description: Use when observing Nanotrace query usage, dashboard behavior, alert rules, raw fallback scans, or product analytics requests to decide whether a query should run as-is, be promoted/materialized, or be treated as unsupported/new-subsystem work. Routes agents to the correct Nanotrace promotion action.
---

# Nanotrace Promotion Classifier

## Goal

Start from an observed query pattern or product request and choose the smallest maintained serving artifact that avoids repeated raw work.

Promotion is warranted when raw `observatory.events` scans become repeated, broad, scheduled, dashboard-backed, or user-facing enough that the system should pay the extraction/materialization cost once.

## Observe These Signals

Use `query_usage`, dashboard source, report specs, UI behavior, or user intent to identify:

- Repeated raw fallback against `observatory.events`.
- High scanned bytes/read rows or high latency.
- Repeated extraction of the same `data.<path>` JSON fields.
- Repeated dashboard widgets or saved charts.
- Scheduled alert/evaluator queries.
- Group-by over a non-core field.
- Multi-field filters over raw JSON paths.
- Numeric aggregation or percentile over raw JSON.
- Distinct entity counts.
- Per-entity ordering, funnel, retention, or state replay.
- High-cardinality `GROUP BY` over ids.

## Decision Map

- **No group, recent density:** use existing `event_density_1s`; no promotion.
- **Core field group/list/histogram:** use existing `field_topk_1m` or `field_density_1s`; no promotion unless missing from core.
- **Exact id lookup:** use existing `field_values` when available; otherwise promote as field lookup.
- **Low/medium-cardinality field group/filter:** promote as field facet.
- **High-cardinality exact filter:** promote as field lookup, not facet.
- **Numeric sum/avg/min/max/percentile over JSON:** promote as measure, then use measure rollups when repeated over time.
- **Repeated time-bucketed numeric chart:** promote as measure rollup.
- **Latest/as-of entity state:** promote as state.
- **Repeated multi-dimensional dashboard/summary:** promote as report.
- **Scheduled threshold query:** promote as alert rollup/report.
- **Top users/accounts/entities:** promote as top-N entity report.
- **Ordered conversion path:** promote as sequence/funnel report.
- **Retention/cohort activity:** promote as cohort/retention report.
- **Broad trace analytics:** promote as trace summary report.
- **Arbitrary regex, arbitrary exact distinct over huge windows, full lifecycle replay, global cross-tenant analytics, external joins, ultra-low-latency alerts:** do not pretend current promotion is enough; classify as boundary/new subsystem.

## Output Format

When making a recommendation, include:

- Observed pattern.
- Recommended promotion action.
- Required paths/measures/dimensions/entity ids.
- Target serving table(s).
- Backfill requirement.
- Cardinality risk.
- Freshness/version requirement.
- Explicit caveats or unsupported pieces.

## Rules

- Choose exact lookup before aggregation for high-cardinality identifiers.
- Choose field facet only for bounded dimensions.
- Choose report promotion when the query combines multiple measures, filters, ratios, or product semantics.
- Do not recommend raw scans for recurring dashboards or scheduled alerts.
- Do not claim report/cohort/sequence/trace executors exist unless current code proves the executor exists; the schema/design may exist before the worker does.
