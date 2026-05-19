---
name: nanotrace-report-promotion
description: Promote repeated Nanotrace dashboards and multi-dimensional summary queries into report_results.
---

# Nanotrace Report Promotion

Use this skill when an agent observes repeated Nanotrace dashboards or summary queries that combine multiple dimensions, filters, and metrics into a reusable result.

## Trigger Scenarios

Trigger report promotion when users repeatedly ask for:

- A dashboard page or saved report with the same metric set.
- Multi-dimensional summaries by workspace, project, model, route, user, status, or time period.
- Leaderboards, breakdown tables, cohort summaries, or operational scorecards.
- Queries that join several promoted or raw sources into one product-ready result.
- Scheduled summaries that are refreshed and read many times.

Good examples:

- "daily workspace cost and latency dashboard"
- "top routes by errors, p95 latency, and request volume"
- "model performance summary by project and week"
- "customer health report combining usage, failures, and latest status"

## Observation Signals

Look for these signals before promoting:

- Same dashboard panels or summary tables are regenerated for many viewers.
- Query shape has multiple grouping dimensions and several metrics.
- Result is consumed as a report, export, dashboard, or recurring review artifact.
- Filters are parameterized, but the result schema is stable.
- Request latency matters more than ad hoc flexibility.
- A query composes measure rollups, state projections, and raw joins into a reusable product view.

## Promotion Action

Promote into a report definition and materialized `report_results`. The current API stores report metadata; verify a report materialization executor exists before claiming the result table will be populated automatically.

Config sketch:

```json
{
  "name": "workspace_operational_summary",
  "kind": "summary",
  "config": {
    "grain": "day",
    "parameters": ["workspace_id", "date_range", "project_id"],
    "dimensions": ["project_id", "model", "route", "status"],
    "metrics": ["request_count", "error_rate", "avg_latency_ms", "p95_latency_ms", "total_tokens"],
    "refresh": { "mode": "scheduled_or_incremental", "cadence": "15m" }
  }
}
```

Shape `report_results` around the product artifact users actually consume. Prefer stable, documented columns over exposing internal query fragments.

## Preferred Serving Path

Serve repeated dashboards, summary tables, exports, and scheduled reports from `report_results`.

Use lower-level sources only for:

- Drill-down links from a report row.
- Ad hoc exploration outside the report schema.
- Rebuilding or validating report output.
- Debugging discrepancies between report values and raw data.

Reports may read from `measure_rollups` and state projections during refresh, then serve users from the persisted report result.

## Caveats

- Do not use report promotion for a single simple time-series metric; use measure rollup promotion.
- Do not use report promotion for latest entity state alone; use state promotion.
- Keep report parameters bounded so result cardinality stays predictable.
- Define freshness, refresh cadence, and backfill behavior explicitly.
- Treat report schema as a product contract; changing columns can break dashboards and exports.
