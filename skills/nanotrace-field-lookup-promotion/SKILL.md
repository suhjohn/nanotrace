---
name: nanotrace-field-lookup-promotion
description: Use when repeated Nanotrace queries perform exact drilldowns on high-cardinality identifiers from raw JSON fields that should be promoted as lookup fields backed by field_index or field_values.
---

# Nanotrace Field Lookup Promotion

## When To Trigger

Trigger this promotion when observed Nanotrace product usage repeatedly asks for exact matches on a high-cardinality identifier and the intended experience is drilldown, timeline lookup, or hydration of matching events.

Good candidates include `user_id`, `request_id`, `session_id`, `trace_id`, `span_id`, `account_id`, `organization_id`, `thread_id`, and `conversation_id`.

## Observation Signals

- Users search for one identifier value at a time.
- Support, debugging, or trace workflows need "show me events for this id."
- Queries are exact equality or small `IN` filters, not broad group-bys.
- The field has high cardinality or is near-unique per event.
- Raw JSON scans are too slow for interactive drilldown.
- Product UI expects an identifier search box, entity timeline, or deep link target.

## Promotion Action

Create or update a promoted field definition with lookup semantics:

```json
{
  "name": "request_id",
  "kind": "field",
  "mode": "lookup",
  "config": {
    "path": "request_id",
    "value_type": "string"
  }
}
```

Use existing `field_values` when the identifier is already one of the default exact lookup fields. Create a lookup field definition when the identifier is tenant-specific or absent from `field_values`; the current definition-backed serving path is `field_index` with lookup capabilities and no facet/top-K intent.

## Preferred Serving Path

Serve identifier searches from `field_values` or exact-match `field_index`, then hydrate matching rows from raw events. Keep aggregation bounded after filtering to the selected identifier and time range.

## Caveats

- Do not add high-cardinality identifiers to default facet density, top-K, or global group-by rollups.
- Do not treat lookup promotion as permission to expose broad "top users" or "group by request_id" workflows.
- Index only present, non-empty values unless product requirements define an explicit missing-value behavior.
- Consider retention and tenant quotas because identifiers can grow quickly.
- Validate path consistency across SDK versions before backfill.
- Prefer measure promotion when the repeated query extracts and aggregates a numeric value, even if the result can be filtered by an identifier.
