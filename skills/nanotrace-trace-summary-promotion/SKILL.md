---
name: nanotrace-trace-summary-promotion
description: Promote broad Nanotrace trace analytics into a trace report that derives root span, duration, and error summaries for report-style serving.
---

# Nanotrace Trace Summary Promotion

Use this skill when a Nanotrace request asks for broad trace analytics that can be answered from one summarized row per trace.

## Trigger When Observed

- The query groups, filters, or trends whole traces rather than inspecting every span in detail.
- The user asks about latency, duration, root operation, error rate, status, endpoint, service, tenant, or trace volume.
- The same trace-level metrics appear in dashboard tiles, scheduled reports, or product analytics.
- The query scans many spans only to derive trace-level fields such as root span, total duration, or whether any span errored.
- The result can tolerate report freshness rather than requiring sub-second stream evaluation.

## Observation Signals

- Phrases like "slow traces", "trace duration", "error traces", "top endpoints", "p95 by service", "root span", "failed requests", or "trace volume by day".
- Aggregations over `trace_id` followed by rollups over time, service, route, tenant, environment, or status.
- Repeated derivation of trace start time, end time, root span attributes, duration, span count, and error flags.
- Dashboards that need fast filtering over high-volume trace history.

## Action Sketch

- Create a trace report that summarizes raw spans into trace-level rows.
- Derive root span fields, trace start/end time, duration, span count, service, operation, route, status, and error indicators.
- Write promoted outputs into `report_results` only after verifying a trace-summary materializer exists; otherwise include the worker in the promotion.
- Include the report definition, source time window, dimensions, and metric version in config.

Example config shape:

```yaml
promotion: trace_summary
source: spans
report_type: trace_report
result_table: report_results
trace_key: trace_id
derived_fields:
  - root_span_name
  - root_service
  - start_time
  - end_time
  - duration_ms
  - span_count
  - has_error
dimensions: [tenant_id, environment, service, route, status]
metrics: [trace_count, error_count, p50_duration_ms, p95_duration_ms, p99_duration_ms]
```

## Preferred Serving Path

- Serve dashboards, trend charts, and product analytics from `report_results` when the trace-summary materializer exists.
- Use trace ids from the promoted report for drilldown back to raw spans.
- Use raw span queries for individual trace debugging, detailed span waterfall inspection, or validation samples.

## Caveats

- Be precise about root-span selection when traces have missing parents, multiple roots, or instrumentation gaps.
- Do not present trace-summary reports as implemented if the current code only has schema/design placeholders.
- Preserve enough identifiers for drilldown; summaries should not trap users away from raw traces.
- Version derived-field logic when changing duration, error, or root-span rules.
- Do not use this promotion for arbitrary exact distinct over huge windows, lifecycle replay, external joins, or ultra-low-latency alerts.
