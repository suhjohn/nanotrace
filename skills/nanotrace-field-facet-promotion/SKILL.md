---
name: nanotrace-field-facet-promotion
description: Use when repeated Nanotrace queries group by, filter by, or facet on a low/medium-cardinality raw JSON field that should be promoted as a field facet backed by field_index.
---

# Nanotrace Field Facet Promotion

## When To Trigger

Trigger this promotion when observed Nanotrace product usage repeatedly groups, filters, or facets over the same raw JSON field and the field behaves like a dimension rather than a unique identifier.

Good candidates include environment, service, region, model, status, route, event type, host class, feature flag, tenant tier, or bounded enum-like labels.

## Observation Signals

- Users repeatedly run `group by data.<path>` or facet/filter on the same JSON path.
- Dashboards need top values, breakdowns, or comparison by that field.
- The field has low/medium cardinality per tenant and time bucket.
- Values are stable enough to index and aggregate without exploding row counts.
- Query latency is dominated by scanning raw `events.data`.
- The product wants selectable filters or top-K facet UI over the field.

## Promotion Action

Create or update a promoted field definition with facet semantics:

```json
{
  "name": "service",
  "kind": "field",
  "mode": "facet",
  "config": {
    "path": "service",
    "value_type": "string"
  }
}
```

Use the exact `data` JSON path observed in events. The current API normalizes field definitions into `field_index`; keep names stable and product-facing when the field will appear in dashboards or query builders.

## Preferred Serving Path

Serve repeated filters, group-bys, and facet value lists from `field_index` and related facet/top-K materializations when available. Use raw `events.data` as the source of truth for hydration, validation, backfill, and fallback queries.

## Caveats

- Do not use facet promotion for high-cardinality identifiers such as `trace_id`, `span_id`, `request_id`, or `user_id`.
- Estimate distinct values per tenant and bucket before enabling default rollups.
- Normalize only when product semantics require it; avoid silently changing raw values.
- Decide how to handle missing, null, empty, or malformed values before backfill.
- Keep promotion reversible: raw events should remain sufficient to rebuild or drop the index.
- Prefer lookup promotion when the workflow is exact drilldown into one identifier rather than broad grouping.
