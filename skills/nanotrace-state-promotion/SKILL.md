---
name: nanotrace-state-promotion
description: Promote Nanotrace as-of, latest, and current entity state questions into kind state backed by entity_state_updates.
---

# Nanotrace State Promotion

Use this skill when an agent observes Nanotrace questions about the current or as-of state of an entity rather than an aggregate over events.

## Trigger Scenarios

Trigger state promotion when users repeatedly ask:

- "What is the latest status for this entity?"
- "Which runs are currently active, stuck, failed, queued, or completed?"
- "What was the state as of a specific timestamp?"
- "Show the current owner, phase, model, revision, deployment, or health for each entity."
- "Find entities whose latest value matches this filter."

Good examples:

- "latest state for every active agent run"
- "which trace is currently blocked?"
- "state of workspace quota as of noon yesterday"
- "current model routing config by project"

## Observation Signals

Look for these signals before promoting:

- Query uses `ORDER BY updated_at DESC LIMIT 1`, `argMax`, window functions, or latest-row joins.
- Product screen lists entities with current status, phase, owner, or config.
- Users filter by latest value, not by all historical events.
- Repeated queries reconstruct entity state from event logs.
- As-of reads need historical correctness but not a full event replay at request time.
- The entity has a stable identifier and state transitions can be represented as updates.

## Promotion Action

Promote into a `kind: state` projection using `entity_state_updates`.

Config sketch:

```json
{
  "name": "agent_run.status",
  "kind": "state",
  "mode": "state_transition",
  "config": {
    "path": "status",
    "entity_type": "agent_run",
    "entity_id_path": "run_id",
    "value_type": "string"
  }
}
```

Store state updates as changes over time. Create one state definition per state path that needs latest/as-of serving, and keep enough state paths promoted to answer the repeated product questions without replaying raw events.

## Preferred Serving Path

Serve latest entity lists, as-of lookups, and latest-value filters from the state projection over `entity_state_updates`.

Use raw events only for:

- Explaining why a state changed.
- Rendering full timelines or audit trails.
- Recovering or validating state reconstruction.
- Querying fields that were not promoted into state.

For latest reads, use the serving path optimized for the current row per entity. For as-of reads, select the newest update at or before the requested timestamp.

## Caveats

- Do not use state promotion for repeated numeric time-bucket aggregates; use measure rollup promotion.
- Define event ordering carefully when multiple updates share a timestamp.
- Make state fields explicit; implicit replay logic tends to drift across callers.
- Preserve historical updates if users need as-of answers or audits.
- Include tenant and authorization keys directly in the state shape when they are required for serving.
