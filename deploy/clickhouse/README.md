# ClickHouse Schema Notes

The events table stores heterogeneous telemetry in a native `JSON` column with
type hints for hot semantic paths. ClickHouse 25.3+ stores those hinted paths as
columnar subcolumns, so the default pattern is:

```sql
data JSON(
  duration_ms Nullable(Float64),
  http.method LowCardinality(Nullable(String)),
  max_dynamic_paths = 8192,
  max_dynamic_types = 8
)
```

Use `Nullable(T)` for paths that only exist on some event types. Without
`Nullable`, ClickHouse fills absent hinted paths with the type default, such as
`0` for numbers or an empty string for strings, which is wrong for sparse
telemetry aggregates.

## Promoted Columns

Promote a JSON path to a top-level column only when it needs one of these:

- Membership in `ORDER BY`
- A data-skipping index
- A dedicated codec
- Backward compatibility for an existing query surface

The base schema currently promotes only `tenant_id`, `event_type`, `trace_id`,
`span_id`, and derived `signal`. Everything else should be queried from `data`:

```sql
SELECT
  tenant_id,
  avg(data.duration_ms) AS avg_duration_ms
FROM observatory.events
WHERE signal = 'trace'
GROUP BY tenant_id;
```

For dotted paths, query builders can use `getSubcolumn`:

```sql
SELECT count()
FROM observatory.events
WHERE getSubcolumn(data, 'http.method') = 'POST';
```

## Adding Hot Paths

Add stable, frequently queried paths as JSON type hints first. Adding or
changing JSON type hints on large existing tables can rewrite data parts unless
lazy type hints are explicitly enabled, so schedule that migration deliberately.

Keep rare or exploratory paths in the dynamic portion of `data`.
