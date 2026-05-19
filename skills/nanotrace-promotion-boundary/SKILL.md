---
name: nanotrace-promotion-boundary
description: Recognize Nanotrace scenarios that current promotion paths cannot solve and route them to the right subsystem or product boundary.
---

# Nanotrace Promotion Boundary

Use this skill when a Nanotrace query looks tempting to promote but does not fit current implemented promotion paths or planned report-shaped serving models.

## Trigger When Observed

- The request requires arbitrary regex over raw JSON payloads or span attributes.
- The request asks for arbitrary exact distinct counts across huge time windows.
- The request needs full lifecycle replay, event sourcing, or reconstructing every state transition.
- The query crosses tenants, customers, or isolation boundaries for analytics.
- The answer depends on external joins with systems outside Nanotrace.
- The request needs ultra-low-latency streaming alerts rather than report-backed serving.

## Observation Signals

- Phrases like "regex anywhere in JSON", "exact unique over all history", "replay every change", "across all tenants", "join with Salesforce/Snowflake/Stripe", or "alert within milliseconds".
- Query shape has unbounded text search, schema-free JSON traversal, high-cardinality exact distinct, or historical reconstruction from raw events.
- The user expects transactional freshness, stream processing latency, or external system consistency.
- The proposed result cannot be reduced to reusable field, measure, rollup, state, cohort, sequence, top-N, alert, report, or trace-summary outputs.

## Boundary Decision

- Do not force the request into `field_index`, `event_measures`, `entity_state_updates`, `measure_rollups`, `cohort_memberships`, or `report_results` if the core requirement remains unsolved.
- Classify the missing capability before proposing a workaround.
- Offer a bounded approximation only when the product scenario can accept it explicitly.
- Prefer making the limitation visible in the query planner, dashboard setup, or agent response.

## Needed Subsystem

- Arbitrary regex over JSON: indexed search subsystem or constrained JSON extraction fields.
- Arbitrary exact distinct over huge windows: dedicated cardinality service, predeclared keys, or approximate sketches with documented error bounds.
- Full lifecycle replay: event-sourcing or replay subsystem with ordered events and versioned state transitions.
- Cross-tenant analytics: explicit multi-tenant analytics product with isolation, authorization, and governance controls.
- External joins: connector/warehouse integration with defined freshness, ownership, and failure semantics.
- Ultra-low-latency streaming alerts: streaming rules engine, not report-result serving.

## Preferred Serving Path

- Keep current promotions for reusable indexed fields, measures, rollups, entity state, and report-shaped analytics.
- Route unsupported scenarios to the owning subsystem when it exists.
- For exploratory work, use raw query paths with clear bounds on time range, tenant scope, and result size.
- For dashboards, block or label unsupported panels rather than presenting report-backed results that miss the requested semantics.

## Caveats

- "Can scan it once" is not the same as "can promote it." Promotion should create a reusable serving path.
- Approximate distinct, sampled search, or bounded replay may be acceptable only if the user-facing semantics say so.
- Cross-tenant analysis needs product and security review before implementation.
- Streaming alerts require latency and delivery guarantees that `report_results` cannot provide.
