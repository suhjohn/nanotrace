---
name: nanotrace-measure-promotion
description: Use when repeated Nanotrace queries extract numeric values from raw JSON and aggregate them, requiring a measure definition backed by event_measures and measure_rollups.
---

# Nanotrace Measure Promotion

## When To Trigger

Trigger this promotion when observed Nanotrace product usage repeatedly extracts a numeric value from raw JSON and aggregates it over time, filters, or dimensions.

Good candidates include latency, duration, token counts, cost, bytes, retries, queue depth, cache hits, error counts, ratings, and other numeric event attributes.

## Observation Signals

- Users repeatedly compute `sum`, `avg`, `min`, `max`, `count`, percentile, or rate from `events.data`.
- Dashboards need time series, rollups, or comparisons for the same numeric JSON path.
- Query cost comes from parsing raw JSON for numeric extraction.
- Product semantics need units, aggregation defaults, or named metric display.
- The value is numeric or can be reliably coerced with clear invalid-value handling.
- The measure may need optional dimensions after extraction.

## Promotion Action

Create or update a promoted measure definition:

```json
{
  "name": "tokens_total",
  "kind": "measure",
  "config": {
    "path": "usage.total_tokens",
    "unit": "tokens"
  }
}
```

Add bounded dimensions only when they are part of the product query shape:

```json
{
  "name": "latency_by_service",
  "kind": "measure",
  "config": {
    "path": "duration_ms",
    "unit": "ms",
    "dimension": "service"
  }
}
```

## Preferred Serving Path

Write extracted event-level numeric values into `event_measures`, then serve repeated time-window aggregations from `measure_rollups`. Hydrate or audit against raw `events.data` when validating extraction logic, backfilling, or debugging discrepancies.

## Caveats

- Do not promote strings or mixed-type values unless coercion rules are explicit and tested.
- Decide how to handle missing, null, non-finite, negative, or malformed values.
- Keep dimensions bounded; high-cardinality dimensions need explicit product justification and retention controls.
- Choose units and aggregation semantics before exposing the measure in dashboards.
- Backfill carefully because historical JSON shape changes can alter extracted values.
- Prefer field facet promotion when the repeated query is categorical grouping/filtering, not numeric aggregation.
