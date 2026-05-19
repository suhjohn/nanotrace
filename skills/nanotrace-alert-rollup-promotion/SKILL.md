---
name: nanotrace-alert-rollup-promotion
description: Use when a Nanotrace alert, monitor, scheduled predicate, or notification rule repeatedly scans raw events and should be promoted into an alert rollup or report-style materialization for cheaper serving.
---

# Nanotrace Alert Rollup Promotion

## Trigger This Skill When

Use this promotion when a scheduled alert predicate repeatedly asks the same bounded question over raw events.

Common scenarios:

- A cron, monitor, or alert evaluates every minute over the last N minutes.
- The predicate filters raw events by event type, status, severity, service, tenant, account, environment, or similar stable fields.
- The alert asks for counts, rates, thresholds, distinct-ish summaries, or grouped health signals.
- The query is product-critical enough that repeated raw scans create latency, cost, or reliability risk.
- The same predicate also powers dashboard status, incident banners, or notification previews.

## Observation Signals

Look for these signals in traces, product requests, logs, SQL, or dashboard configs:

- Repeated `events` scans with the same `WHERE` clause and sliding time window.
- Alert evaluation latency grows with raw event volume.
- Scheduled jobs fan out across tenants, projects, services, or definitions.
- The user asks for "alert when X happens more than Y times" or "notify if condition stays true."
- Raw query results are immediately reduced to a small result set such as count, pass/fail, latest bucket, or grouped thresholds.
- The same derived value is read by more than one consumer.

## Promotion Action

Promote the predicate into a persisted alert rollup or report-style result. If using `report_results`, first verify the current code has an executor that populates it for this definition; otherwise treat this as a planned serving path and create the missing worker/materializer explicitly.

Config sketch:

```json
{
  "kind": "alert_rollup",
  "name": "checkout_errors_5m",
  "source": "events",
  "filter": {
    "event": "checkout.failed",
    "env": "prod"
  },
  "window": "5m",
  "bucket": "1m",
  "aggregate": {
    "op": "count"
  },
  "threshold": {
    "op": ">",
    "value": 25
  },
  "result_table": "report_results"
}
```

Prefer explicit definition ownership, retention, and backfill boundaries. Keep the raw event query as the source of truth for rebuilds and audit drilldown.

## Preferred Serving Path

Serve alert evaluation and alert status views from the alert rollup path, or from `report_results` when a materializer exists for the definition.

Use raw events only for:

- Drilldown into matching examples.
- Backfills or rebuilds.
- One-off debugging outside the scheduled predicate.
- Newly created alerts before materialization has caught up.

## Caveats

- Do not promote ad hoc exploratory queries that are unlikely to repeat.
- Do not claim automatic `report_results` population unless the current worker path proves it.
- Avoid high-cardinality group-bys unless the alert definition explicitly needs them and has quota/retention limits.
- Preserve enough predicate metadata to explain why an alert fired.
- Be clear about late-arriving events, bucket finalization, and whether alert evaluation uses partial buckets.
- Promotion should reduce repeated scans, not hide missing event fields or ambiguous alert semantics.
