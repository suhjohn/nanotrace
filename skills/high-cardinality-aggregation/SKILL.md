---
name: nanotrace-high-cardinality-aggregation
description: Use when designing, reviewing, or implementing Nanotrace user/account/session/request-id search, timelines, metric dimensions, promoted definitions, measure rollups, or other high-cardinality aggregation behavior. Applies especially to fields like user_id, request_id, session_id, trace_id, span_id, account_id, organization_id, thread_id, and conversation_id.
---

# Nanotrace High-Cardinality Aggregation

## Core Rule

High-cardinality identifiers are safe as event attributes and exact lookup keys, but risky as always-on aggregate dimensions.

For fields like `user_id`, `request_id`, `session_id`, `trace_id`, `span_id`, `account_id`, `organization_id`, `thread_id`, and `conversation_id`:

- Keep them in raw `events.data`.
- Keep exact lookup paths for timelines and drilldowns.
- Do not casually add them to default global density, top-k, metric, or measure rollups.
- Treat full aggregation by these fields as an explicit product feature with retention, bucket, and cardinality expectations.

## Current Nanotrace Mechanisms

Use the existing read/write model deliberately:

- **Raw events:** canonical event payloads live in `observatory.events`.
- **Exact lookup:** `observatory.field_values` is appropriate for finding events by identifiers such as `user_id`.
- **Default facets:** `field_density_1s` and `field_topk_1m` are useful for low/medium-cardinality dimensions. High-cardinality identifiers here create write amplification and large rollups.
- **Definitions:** `observatory.definitions` controls explicit promoted fields, measures, rollups, and states.
- **Numeric aggregation:** `event_measures` and `measure_rollups` support declared measures with optional dimensions, including `dimension: "user_id"` when intentionally enabled.

## Answering Design Questions

When asked whether a high-cardinality use case is supported, distinguish these cases:

- **Exact timeline for one user/entity:** yes, use `field_values` to find matching events and hydrate/query raw events.
- **Aggregate for one selected user/entity:** yes, filter to that identifier first, then aggregate over a bounded time range.
- **Top users/accounts by a metric:** yes, but prefer bounded top-K, larger buckets, and retention limits.
- **Full group-by over all users/accounts:** possible, but should be explicit, estimated, and quota-controlled.
- **Metric tagged by raw user_id by default:** avoid unless allowlisted, sampled, temporary, or explicitly declared as user-scoped.

## Sparse Identifier Semantics

It is expected that only some events contain `user_id` or similar identifiers.

- Missing identifiers should not block ingestion.
- Lookup indexes should only contain rows when the identifier is present and non-empty.
- For measure definitions with `dimension: "user_id"`, decide whether missing users should be skipped or grouped under an empty/unknown bucket. Prefer skipping missing dimensions for "by user" rollups unless product requirements say otherwise.

## Recommended Pattern

For a user-scoped numeric aggregation, create a measure definition:

```json
{
  "name": "tokens_by_user",
  "kind": "measure",
  "config": {
    "path": "tokens.total",
    "unit": "tokens",
    "dimension": "user_id"
  },
  "backfill": {
    "from": "2026-05-01T00:00:00Z",
    "to": "2026-05-19T00:00:00Z"
  }
}
```

This is enough for backend materialization when the measured field is numeric, `user_id` exists on relevant events, historical data is backfilled, and the loader/materializer is running with definitions enabled. Product/UI work may still be needed to query and present `measure_rollups`.

## Guardrails To Prefer

When adding or reviewing high-cardinality aggregation:

- Estimate distinct values and rows written per event before enabling.
- Prefer exact lookup plus filtered aggregation over global group-by.
- Prefer 5m/1h buckets over 1s buckets for high-cardinality dimensions.
- Add retention limits for high-cardinality rollups.
- Use top-K or allowlists for broad "top users" workflows.
- Warn or block `user_id`, `request_id`, and `session_id` as metric dimensions unless the metric is explicitly user-scoped.
- Keep raw data recoverable so bad rollups can be dropped and rebuilt.

## Review Checklist

Before approving a schema, loader, SDK, or dashboard change:

- Does it make a high-cardinality field searchable, aggregatable, or both?
- Is the field in an exact lookup table, an aggregate rollup, or a promoted definition?
- Is the aggregation default/on for all tenants, or opt-in per tenant/definition?
- What is the expected cardinality per tenant and per time bucket?
- What happens when the identifier is missing?
- Is there a cheaper query path for the intended product experience?
- Is the change reversible without losing raw events?
