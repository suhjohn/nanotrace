# Discovery Workflow

Use this workflow when the data shape is unknown. The goal is to learn the dataset before designing dashboards.

## 1. Establish Context

Confirm or infer:

- API base URL.
- Tenant or organization context, if the API requires one.
- Time range to inspect.
- Whether this is a local/dev tenant or production.

Use a broad but bounded initial range. For production-like data, start with the last 24 hours and expand only when needed.

## 2. Inventory Read Models

Check row presence and time coverage for core read models. Use small aggregate queries through `/v1/query`.

Useful read models to probe:

- `observatory.events`
- `observatory.event_index`
- `observatory.field_index`
- `observatory.field_counts_5m`
- `observatory.event_rollups_5m`
- `observatory.event_measures`
- `observatory.measure_rollups`
- `observatory.spans`
- `observatory.trace_summaries`
- `observatory.entity_state_updates`
- `observatory.report_results`
- `observatory.sequence_report_results`
- `observatory.cohort_memberships`
- `observatory.pipeline_metrics`
- `observatory.definition_stats`

For each populated model, capture:

- Approximate row count for the inspected range.
- Earliest and latest timestamp.
- Last refreshed or updated timestamp if present.
- Primary grouping fields.

## 3. Sample Raw Events

Sample a small number of rows across the time range. Prefer `event_index` for event metadata and `events` only when raw JSON is required.

Sample by:

- Recent events.
- Top event types.
- Top services or sources.
- Error events.
- Events with numeric-looking values.
- Events with nested objects.
- Events with trace/span IDs.
- Events with state-transition-looking fields.

Do not rely on one event sample. Unknown datasets are often mixed.

## 4. Infer Field Paths

For sampled JSON payloads, build a rough path catalog:

- Path.
- Example values.
- Observed types.
- Null/missing frequency in the sample.
- Whether the value looks bounded or unbounded.
- Whether the path appears across many event types or only one.

Classify paths:

- **Dimension:** string/bool with repeated values, such as service, route, status, plan, region, environment.
- **Lookup:** ID-like value with high cardinality, such as request_id, user_id, trace_id, order_id.
- **Measure:** numeric value, such as duration_ms, cost_usd, tokens, revenue, count, bytes, score.
- **Timestamp:** timestamp-like string or number.
- **State:** status/plan/risk/lifecycle fields, especially with previous/current or from/to forms.
- **Free text:** messages, prompts, SQL, stack traces, user agent, full URL.

Free text should usually drive event inspection, not dashboard group-bys.

## 5. Cardinality Checks

Before proposing group-bys, estimate cardinality over a small bounded range.

Interpretation:

- 1-50 distinct values: strong dashboard dimension.
- 51-1,000 distinct values: usable with care, usually top-K only.
- 1,001+ distinct values: likely lookup/drilldown, not a default dashboard grouping.

High-cardinality fields can still be important, but they should usually power search, drilldown, or exact filters rather than top-level charts.

## 6. Numeric Checks

For candidate measures, inspect:

- Count of non-null values.
- Min/max.
- Avg.
- p50/p95/p99 when cheap.
- Units inferred from path names or values.
- Whether values are counters, gauges, durations, money, bytes, scores, or counts.

Prefer measures that appear frequently enough to support charts. Rare measures may be better as detail columns or alert tables.

## 7. Entity and Sequence Checks

Find entity keys:

- `user_id`
- `account_id`
- `organization_id`
- `session_id`
- `conversation_id`
- `order_id`
- `request_id`
- domain-specific IDs

Then ask:

- Are there multiple events per entity?
- Are there ordered event types per entity?
- Do events span days/weeks?
- Are there lifecycle or status transitions?

If yes, recommend lifecycle, funnel, cohort, or journey dashboards. If not, avoid pretending retention/funnel analysis is supported.

## 8. Trace/Workflow Checks

Trace-like data exists when rows contain several of:

- trace ID
- span ID
- parent span ID
- start time
- end time
- duration
- service/name/status

If trace summaries exist and are populated, use them for trace lists. Use spans for selected trace flamegraphs or waterfalls. If only raw trace-shaped events exist, recommend creating the span/trace read models before building production trace dashboards.

## 9. State Transition Checks

State-transition-like data exists when events contain:

- `previous_*` and current fields
- `old_*` and `new_*`
- `from` and `to`
- `before` and `after`
- event names containing `changed`, `updated`, `transitioned`, `upgraded`, `downgraded`, `churned`, or `activated`

If `entity_state_updates` is populated, use it. If not, recommend a state definition/backfill before building long-range lifecycle dashboards.

## 10. Report and Rollup Checks

Before proposing expensive dashboard cards, check whether compact outputs already exist:

- `report_results`
- `sequence_report_results`
- `cohort_memberships`
- `measure_rollups`
- `event_rollups_5m`
- `field_counts_5m`

If compact outputs are absent, recommend either:

- a short-range exploratory card, or
- creating/backfilling the needed definition, rollup, state extractor, cohort, or report.

## 11. Evidence Standard

Every dashboard recommendation should cite concrete evidence:

- table/read model rows exist
- time range
- relevant event types
- relevant fields
- relevant row counts or distinct counts
- example values when useful

Avoid recommendations based only on table names or intuition.
