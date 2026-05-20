# Output Format

Use this structure when presenting dashboard bootstrap results.

## Dataset Summary

Include:

- Inspected time range.
- Populated read models.
- Approximate row counts.
- Freshness.
- Dominant event types or signals.
- Any obvious gaps.

Example:

```md
The dataset covers 2026-03-16 to 2026-05-16. `event_density_1s`, `field_topk_1m`, `field_density_1s`, `event_measures`, and `measure_rollups` are populated. `report_results` is empty, so long-range funnel/revenue widgets should be materialized before becoming production dashboard cards.
```

## Discovered Data Model

Group findings by role:

```md
Entities:
- account_id
- user_id
- session_id
- trace_id

Dimensions:
- service
- environment
- event_type
- account.plan
- account.risk_tier

Measures:
- duration_ms
- cost_usd
- revenue
- total_tokens

State transitions:
- account.plan
- account.risk_tier
```

Only include fields supported by observed data.

## Recommended Dashboards

For each recommendation:

```md
### 1. Service Health

Confidence: high
Cost profile: cheap with `event_density_1s`, `field_topk_1m`, `field_density_1s`, and `measure_rollups`; moderate fallback with tightly bounded `events`

What it answers:
Shows whether services are healthy by volume, error rate, and latency.

Evidence:
- `service` has 7 observed values.
- `duration_ms` exists in `event_measures`.
- `event_density_1s` and `field_density_1s` have rows across the selected range.

Widgets:
- Events/errors over time by service.
- p95 duration by service.
- Top routes by error rate.
- Recent slow/error event samples.

Read models:
- `event_density_1s`
- `field_density_1s`
- `field_topk_1m`
- `measure_rollups`

Missing prerequisites:
- None, or list exact definitions/rollups needed.
```

## Visualization Plan

When asked to create dashboards or hand off to `create-nanotrace-visualization`, produce a concrete plan:

```md
Dashboard: LLM Operations

Widgets:
1. Total LLM cost
   Type: KPI
   Query source: `event_measures`
   Measure: `cost_usd`

2. Token usage over time
   Type: stacked time series
   Query source: `measure_rollups`
   Measures: `input_tokens`, `output_tokens`

3. Calls by model
   Type: horizontal bar
   Query source: `field_topk_1m`
   Dimension: `model`
```

## Missing Prerequisites

Be explicit when the data supports an idea but the cheap read model is missing:

```md
The dataset has checkout and order events, but no materialized sequence report. A production checkout funnel should be created as a report first:

- entity: user_id or account_id
- steps: checkout.started -> checkout.completed -> order.filled
- output: `sequence_report_results`
```

## Avoid

Do not output:

- Generic dashboard lists with no evidence.
- Queries that scan unbounded raw events.
- Group-bys over obvious high-cardinality IDs as default widgets.
- Assumptions that event names or field paths exist because they appeared in another tenant.
