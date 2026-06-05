---
name: bootstrap-nanotrace-dashboards
description: Research unknown Nanotrace data and bootstrap dashboard recommendations or dashboard visualization plans from first principles. Use when asked to inspect Nanotrace read models, understand unfamiliar event formats, infer entities/measures/dimensions/traces/state transitions/reports, or propose dashboards from whatever data is actually present.
---

# Bootstrap Nanotrace Dashboards

## Goal

Start from an unknown dataset and infer useful dashboards from evidence. Do not assume fixture names, product domains, event shapes, field paths, or report availability. Treat every tenant as a new data model until profiling proves otherwise.

This skill is for discovery and dashboard planning. Use `create-nanotrace-visualization` after this skill has produced a concrete visualization plan.

## Data Access

- Read data with `POST /v1/query`.
- Discover definitions with `GET /v1/definitions`.
- Discover report, sequence, and cohort specs as definition records from `GET /v1/definitions`.
- When running outside a dashboard iframe, use `NANOTRACE_API_KEY` as `Authorization: Bearer $NANOTRACE_API_KEY` if authentication is required.

The `observatory.*` names in this skill are Nanotrace read models.

## First-Principles Workflow

1. **Inventory available read models**
   - Check which read models contain rows.
   - Find min/max timestamps and recent freshness.
   - Do not assume a table is useful because it exists.

2. **Profile raw/event-shaped data**
   - Sample recent rows and older rows.
   - Infer common top-level keys and nested paths.
   - Identify likely timestamp fields, event name/type fields, IDs, dimensions, numeric measures, booleans, arrays, and nested objects.
   - Detect whether the data is log-like, metric-like, trace-like, product-event-like, state-transition-like, report-like, or mixed.

3. **Classify fields by role**
   - **Time axes:** event time, observed time, bucket time, start/end time.
   - **Entities:** user, account, org, session, request, trace, span, order, document, conversation, device, host, service.
   - **Dimensions:** low/medium-cardinality strings or booleans suitable for grouping.
   - **Lookup IDs:** high-cardinality identifiers suitable for exact drilldown.
   - **Measures:** numeric values suitable for sum/avg/p95/rate/distribution.
   - **State transitions:** fields with previous/current, old/new, from/to, before/after, status changed, plan changed, risk changed, lifecycle changed.
   - **Sequences:** entity-keyed ordered events that could support funnels or retention.
   - **Trace structure:** trace ID, span ID, parent span ID, start/end/duration, service/name/status.

4. **Measure quality and coverage**
   - Time coverage.
   - Row count.
   - Null rate.
   - Distinct count.
   - Example values.
   - Numeric min/max/avg/p95 when cheap.
   - Whether optimized read models or materialized reports already exist.

5. **Recommend dashboards**
   - Prefer dashboards that match demonstrated data, not desired data.
   - For each dashboard, list required evidence, read models, candidate queries, confidence, and missing prerequisites.
   - Prefer compact, operational dashboards over broad dashboards with many expensive raw scans.

6. **Choose cheap read paths**
   - Prefer materialized reports and rollups for long-range cards.
   - Prefer `event_density_1s` for global event volume/error trends.
   - Prefer `field_topk_1m` for top values and `field_density_1s` for grouped histograms on core dimensions.
   - Prefer `field_values` for exact identifier drilldowns and `field_index` for definition-backed promoted fields.
   - Use `events` for recent exploration, samples, payload hydration, and narrow correctness fallbacks.

## Discovery References

Load only what is needed:

- Unknown dataset profiling: [references/discovery-workflow.md](references/discovery-workflow.md)
- Visualization selection: [references/visualization-grammar.md](references/visualization-grammar.md)
- Output format for recommendations: [references/output-format.md](references/output-format.md)

## Dashboard Recommendation Rules

A dashboard recommendation must include:

- **What it answers**
- **Why the data supports it**
- **Read models to use**
- **Fields or paths involved**
- **Suggested visualizations**
- **Cost profile:** cheap, moderate, expensive, or requires materialization
- **Confidence:** high, medium, or low
- **Missing prerequisites**, if any

Never present a dashboard as production-ready if it depends on long-range raw scans. Say what definition, rollup, state extraction, cohort, or report should be created first.

## Generic Dashboard Families

Use these as patterns, not assumptions:

- **Overview:** volume, errors, freshness, top dimensions, recent events.
- **Operations:** service/route/job/materializer health, latency, error rates, throughput.
- **Entity lifecycle:** state transitions, churn, upgrades, risk changes, status changes.
- **Revenue/product:** conversion, orders, checkout, payment, usage, billing, value metrics.
- **Trace/workflow:** traces, spans, stages, critical path, flamegraph, waterfall.
- **AI/agent:** requests, model/provider, tokens, cost, tools, retrieval, safety, evals.
- **Quality:** failures, anomalies, policy outcomes, scores, rejected/blocked actions.
- **Pipeline:** ingestion, extraction, backfill, report materialization, lag.
- **Loadtest/dev:** fixture mix, generated profiles, run IDs, data diversity, row rates.

Only recommend a family when profiling finds matching evidence.

## Final Response Shape

For normal use, return:

1. Dataset summary.
2. Discovered entities, dimensions, measures, and time ranges.
3. Recommended dashboards ranked by usefulness and confidence.
4. For each dashboard, the concrete widgets to create.
5. Any missing definitions, rollups, reports, or state extractors.

Keep the answer grounded in observed fields and row counts. Avoid generic dashboard advice when specific evidence is available.
