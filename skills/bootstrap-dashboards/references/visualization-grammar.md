# Visualization Grammar

Use this as a mapping from discovered data shapes to useful dashboard widgets.

## Time + Count

When data has a timestamp and events:

- Event volume over time.
- Error volume over time.
- Event rate by event type.
- Freshness/last seen KPI.

Best read models:

- `event_density_1s`
- `field_density_1s` when grouped by a core field
- `report_results`

## Dimension + Count

When a low/medium-cardinality dimension exists:

- Top values bar chart.
- Stacked time series by top values.
- Error rate by value.
- Table of value, events, errors, last seen.

Best read models:

- `field_topk_1m` for top values on core fields
- `field_density_1s` for grouped time series on core fields
- `field_index` for promoted facet fields
- `report_results`

Avoid default charts over high-cardinality IDs.

## Time + Numeric Measure

When a numeric measure exists:

- Average over time.
- Sum over time.
- p95/p99 over time for durations.
- Distribution or histogram.
- Top dimensions by measure contribution.

Best read models:

- `measure_rollups`
- `event_measures` for bounded exploratory ranges
- `report_results`

## Two Dimensions + Count Or Measure

When two useful dimensions exist:

- Heatmap.
- Matrix table.
- Stacked bar.
- Top-K grouped table.

Examples:

- service by status.
- model by provider.
- plan by region.
- route by status code.

Use only when the observed combination count is bounded. If the product of cardinalities is large, recommend a top-K filtered widget.

## Entity + Ordered Events

When events share an entity key and ordered steps:

- Funnel.
- Dropoff table.
- Step latency.
- Conversion by cohort/time.

Best read models:

- `sequence_report_results`
- `report_results`
- bounded raw/index query only for exploration

If a funnel would need to recompute months of entity sequences on every dashboard refresh, recommend a report instead.

## Entity + Repeated Activity Over Time

When entities recur across days/weeks:

- Retention curve.
- Active entities over time.
- Cohort table.
- Reactivation/churn list.

Best read models:

- `cohort_memberships`
- `report_results`

Avoid raw recomputation for production dashboards.

## State Transitions

When entity state changes exist:

- Transitions over time.
- From/to Sankey.
- Current or latest known distribution if supported.
- Time from state A to event B.
- State changes before/after important actions.

Best read models:

- `entity_state_updates`
- `report_results`

Long-range as-of joins should usually be materialized as reports.

## Trace / Span / Workflow

When trace/span structure exists:

- Trace list.
- Slowest traces.
- Error traces.
- Flamegraph/waterfall for selected trace.
- Span duration by service/name.
- Critical path summary.

Best read models:

- `field_values` for exact trace/span lookup
- `events` for selected trace event lanes and payload hydration
- `report_results` when broad trace reports have been materialized

Do not build broad trace lists by grouping raw events by trace ID. Use a materialized trace report when the product needs top slow/error trace lists over large windows.

## Logs / Free Text

When data is mostly text:

- Recent events table.
- Error samples.
- Top event types, services, severity, source.
- Drilldown detail view.

Do not build generic full-text dashboards unless a search/read model exists.

## Metrics

When metric-shaped events exist:

- Metric value over time.
- Top series by latest value.
- Saturation/queue/memory gauges.
- Rate of counters.

Best read models:

- `measure_rollups`
- `event_measures`
- `report_results`

Counters and gauges should not be visualized the same way. Identify the metric kind before choosing the chart.

## Pipeline / Materializer Data

When ingestion, materializer, backfill, or report-worker events exist:

- Rows scanned/written.
- Backfill progress.
- Processing lag.
- Error count by materializer or report worker.
- Materialization freshness.

Best read models:

- `pipeline_metrics`
- `event_measures`
- `event_density_1s`
- `field_topk_1m`
- `report_results`
