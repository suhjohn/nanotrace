---
name: nanotrace-sequence-funnel-promotion
description: Use when a Nanotrace query or product workflow needs ordered per-entity event sequences, funnels, conversions, drop-offs, or stage transitions and should be promoted into a sequence report materialization.
---

# Nanotrace Sequence Funnel Promotion

## Trigger This Skill When

Use this promotion for ordered event-sequence questions scoped to an entity such as user, account, session, trace, request, conversation, checkout, or workflow run.

Common scenarios:

- "Users who did A then B but not C."
- Checkout, onboarding, signup, or activation funnels.
- Per-session or per-account conversion steps.
- Ordered failure paths before an incident.
- Workflow stages where order, elapsed time, or drop-off matters.
- Repeated product views that rebuild the same sequence logic from raw events.

## Observation Signals

Look for these signals in traces, SQL, product specs, or dashboard behavior:

- Queries partition by entity and order events by timestamp.
- Logic uses `A before B`, `within N minutes`, `did not reach step`, or "first/last step."
- The result is a compact sequence, funnel count, conversion rate, drop-off table, or stage transition summary.
- Raw scans repeatedly hydrate many events only to reduce them into stage outcomes.
- Users want drilldown from funnel metrics into example entities.
- The product asks for historical trend lines of the same funnel definition.

## Promotion Action

Promote the ordered sequence definition into a sequence report materialization such as `sequence_report_results`. First verify the current code has an executor for this table and definition shape; if not, this promotion includes creating the missing materializer. Keep the sequence definition explicit: entity key, step filters, ordering timestamp, time bounds, allowed gaps, terminal states, and output metrics.

Config sketch:

```json
{
  "kind": "sequence_funnel",
  "name": "checkout_conversion",
  "source": "events",
  "entity": {
    "path": "session_id"
  },
  "order_by": "timestamp",
  "window": "24h",
  "steps": [
    { "name": "cart", "filter": { "event": "cart.created" } },
    { "name": "payment", "filter": { "event": "payment.submitted" } },
    { "name": "success", "filter": { "event": "checkout.completed" } }
  ],
  "constraints": {
    "max_gap": "30m",
    "ordered": true
  },
  "result_table": "sequence_report_results"
}
```

Use raw events as the rebuildable source. Store enough result metadata to explain entity membership, stage counts, elapsed times, and drop-offs.

## Preferred Serving Path

Serve funnel charts, conversion summaries, stage tables, and repeated sequence analyses from `sequence_report_results` when the sequence materializer exists.

Use raw events for:

- Entity-level drilldown and event timeline inspection.
- Validating a new sequence definition.
- Backfills and rebuilds.
- Rare exploratory sequence questions that are not yet repeated product behavior.

## Caveats

- Define entity identity carefully; sequence results are only as correct as the partition key.
- Specify ordering behavior for tied or late-arriving events.
- Decide whether repeated steps count once, many times, or by first occurrence.
- Keep time windows and gap constraints explicit to avoid accidental unbounded scans.
- Do not use generic `report_results` when ordered per-entity sequence semantics are required; prefer `sequence_report_results` or add a dedicated executor.
