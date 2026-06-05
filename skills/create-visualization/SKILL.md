---
name: create-nanotrace-visualization
description: Create, review, or improve Nanotrace dashboard iframe visualizations stored as persisted React modules. Use when working on Nanotrace dashboard visualization sourceCode, chart/table/card modules, iframe rendering, dashboard parameter bindings, scroll behavior, duplicate titles, or dashboard visualization API records.
---

# Create Nanotrace Visualization

## Goal

Build visualization modules that feel like the body of a dashboard card, not a full standalone app. The Nanotrace host already provides card chrome: title, bindings, grid size, drag, resize, and edit controls.

## API Surface

Use the Nanotrace API surface:

- Dashboard iframe runtime: call `nanotrace.query(payload)` from visualization modules with the same structured payload accepted by `POST /v1/query`.
- Product read API: `POST /v1/query` with a structured `type` field for events, search, measures, funnels, cohorts, reports, state, and alerts.
- Event hydration API: `GET /v1/events/{event_id}` when an external workflow needs one raw event payload.
- Definition discovery API: `GET /v1/definitions` when an external workflow needs active fields, measures, rollups, states, reports, sequences, or cohorts.
- Dashboard visualization persistence is product-owned. Use the current app surface or local UI state for saved modules; do not assume a public dashboard-visualization HTTP route exists.

Authentication:

- Inside dashboard iframe modules, do not read environment variables. Use the injected `nanotrace` runtime object.
- Outside the iframe, use `NANOTRACE_API_KEY` as `Authorization: Bearer $NANOTRACE_API_KEY` when calling Nanotrace HTTP APIs from scripts or terminal commands.

The `observatory.*` names used below are physical read-model references for planning and debugging. Dashboard iframe modules should read them through structured `POST /v1/query` helpers, not by sending SQL payloads.

## Scale Assumption

Assume production read models are massive by default:

- Raw/per-event read models such as `events`, `field_index`, `field_values`, and `event_measures` can be extremely large even for a single tenant.
- A dashboard card is an interactive surface. It must not casually scan days or months of raw/high-cardinality event-level rows.
- Prefer precomputed or reduced read models first: `report_results`, `sequence_report_results`, `cohort_memberships`, `measure_rollups`, `event_density_1s`, `field_density_1s`, and `field_topk_1m`.
- If the dashboard needs a query that would scan raw or per-event read models over a large range, state the missing prerequisite instead: define/backfill the field, measure, rollup, state, cohort, or report first.

Concrete rule: a dashboard visualization may use raw/per-event read models for recent, bounded exploration, but durable dashboard widgets should use rollups, facet counts, state histories, or materialized report outputs.

## Workflow

1. Inspect the existing saved module before editing.
   - Use the dashboard visualization API or local UI state to read the visualization `title`, `parameterBindings`, layout, and `sourceCode`.

2. Discover what data is actually available in the target production tenant before choosing a query.
   - Use `GET /v1/definitions` to see promoted fields, measures, rollups, states, reports, sequences, and cohorts.
   - Use structured `POST /v1/query` reads to check whether relevant materialized results exist.
   - Treat definitions with kind `report`, `sequence`, or `cohort` as saved report specs.
   - Use internal ClickHouse access, not a public HTTP SQL API, if you are debugging low-level metadata such as `definition_stats` or `pipeline_metrics`.
   - Do not assume a read model contains useful rows just because it exists.

3. Decide what the host owns versus what the iframe owns.
   - Host owns card title, edit/resize/move controls, grid dimensions, and binding chips.
   - Iframe owns the chart, table, number, empty state, error state, and concise contextual subtitles.
   - Do not repeat the host title inside the iframe unless the inner title adds distinct information.

4. Keep the module small and predictable.
   - Export one default React component.
   - Use TanStack Query for async state and call `nanotrace.query(payload)` with a structured query payload inside `queryFn`.
   - Use dashboard params only when the visualization lists the matching binding.
   - Handle loading, empty, and error states.
   - Keep styles local, but follow the existing dark, dense, operational UI.

5. Verify visually.
   - Open the dashboard in the browser.
   - Check at least one normal desktop viewport and one narrower viewport or resized card.
   - Confirm content does not duplicate chrome, overflow awkwardly, or trap scroll.

## Design Rules

- Do not duplicate the card title. If the card title is `Recent events`, the iframe body can start directly with rows.
- Use subtitles only for changing context, such as `grouped by service`, selected range, or filter summary.
- Do not create extra cards inside the iframe. The host card is already the container.
- Prefer compact typography: 11-13px labels, 12-14px body text, larger numbers only for KPI cards.
- Use consistent colors: black or near-black backgrounds, neutral text, cyan/blue accents only for data marks.
- Keep padding proportional to the card size. Large cards can use 14-16px; dense tables can use 8-12px.
- Do not use marketing-style hero text, decorative gradients, or large explanatory copy.

## Layout Rules

- Root modules should usually use `height: '100%'`.
- For chart modules, use `display: 'grid'` with stable rows such as `auto 1fr`.
- For table/list modules, make the list region scrollable:

```js
rows: {
  minHeight: 0,
  overflowY: 'auto',
  overscrollBehavior: 'contain',
  scrollbarColor: '#737373 transparent',
  scrollbarWidth: 'thin'
}
```

- Avoid `overflow: 'hidden'` on a container that can contain more rows than fit.
- Avoid nested scroll containers. Prefer one obvious scroll area per visualization.
- Avoid fixed pixel heights inside the iframe unless the chart truly needs a stable plotting region.

## Query Rules

- Import TanStack Query from `https://esm.sh/@tanstack/react-query@5.100.10?deps=react@19.2.1`.
- Each standalone iframe module that uses `useQuery` must create a `QueryClient` and wrap its body in `QueryClientProvider`.
- Prefer the injected typed helpers over handwritten payloads: `nanotrace.events`, `nanotrace.measure`, `nanotrace.report`, `nanotrace.funnel`, `nanotrace.cohort`, `nanotrace.state`, and `nanotrace.alerts`.
- The typed helpers automatically apply bound dashboard params when relevant: `nanotrace.events` uses the card's time/filter/group bindings, and `nanotrace.measure` uses the card's time range plus bound group as a default `groupBy`.
- Use `params.query.timeRange`, `params.query.eventFilter`, and `params.query.groupBy` for query keys and labels. `params.sql` is legacy compatibility data; do not build dashboard SQL from it.
- Keep limits explicit for tabular widgets and event lists.
- Prefer query output names that map directly to rendered fields.
- Use structured `/v1/query` payloads through the typed helpers or `nanotrace.query(payload)` for dashboard reads. Do not call write/control-plane endpoints such as `/v1/definitions` from iframe visualizations.
- Treat report, sequence, and cohort definitions as saved specs. Dashboard widgets should usually read materialized output through `/v1/query`, not fetch control-plane specs at render time.
- Use `nanotrace.events` only when the visualization needs raw event rows or a tightly bounded exploratory summary.
- Use `nanotrace.measure` for numeric rollup charts backed by explicit measure definitions.
- Use `nanotrace.state` for latest/as-of entity state widgets.
- Use `nanotrace.alerts` for hot alert match history and webhook notification/outbox status.
- Use `nanotrace.report`, `nanotrace.funnel`, and `nanotrace.cohort` for saved reports that have been materialized by report workers.
- Dashboard SQL payloads are rejected by the runtime. If a desired widget cannot be expressed as a structured query, state the missing definition/report/read model instead of embedding SQL.

## Production Discovery

Before creating a dashboard for a real tenant, use structured `/v1/query` reads to learn what materialized outputs are populated, then select the cheapest correct model.

Active definitions:

```sql
SELECT
  definition_id,
  name,
  kind,
  mode,
  config,
  capabilities,
  updated_at
FROM observatory.definitions
WHERE enabled = 1
  AND isNull(deleted_at)
ORDER BY kind, name
LIMIT 200
```

Latest definition backfill/status:

```sql
SELECT
  definition_id,
  argMax(decision, measured_at) AS status,
  argMax(window_start, measured_at) AS from_time,
  argMax(window_end, measured_at) AS to_time,
  argMax(rows_matched, measured_at) AS rows_matched,
  argMax(distinct_values, measured_at) AS distinct_values,
  max(measured_at) AS updated_at
FROM observatory.definition_stats
GROUP BY definition_id
ORDER BY updated_at DESC
LIMIT 200
```

Available materialized reports:

```sql
SELECT
  report_id,
  min(bucket_time) AS first_bucket,
  max(bucket_time) AS last_bucket,
  max(refreshed_at) AS refreshed_at,
  count() AS rows
FROM observatory.report_results
GROUP BY report_id
ORDER BY refreshed_at DESC
LIMIT 100
```

Available funnels/sequences:

```sql
SELECT
  report_id,
  groupArrayDistinct(step_name) AS steps,
  min(bucket_time) AS first_bucket,
  max(bucket_time) AS last_bucket,
  max(refreshed_at) AS refreshed_at,
  count() AS rows
FROM observatory.sequence_report_results
GROUP BY report_id
ORDER BY refreshed_at DESC
LIMIT 100
```

Available cohorts:

```sql
SELECT
  cohort_id,
  entity_type,
  min(first_seen) AS first_seen,
  max(last_seen) AS last_seen,
  max(refreshed_at) AS refreshed_at,
  count() AS members
FROM observatory.cohort_memberships
GROUP BY cohort_id, entity_type
ORDER BY refreshed_at DESC
LIMIT 100
```

Available facet fields and top values:

```sql
SELECT
  field_name,
  value,
  sum(count) AS events,
  sum(error_count) AS errors,
  max(bucket_time) AS last_seen
FROM observatory.field_topk_1m
WHERE bucket_time >= now() - INTERVAL 24 HOUR
GROUP BY field_name, value
ORDER BY events DESC
LIMIT 100
```

Available numeric rollups:

```sql
SELECT
  definition_id,
  measure_name,
  dimension_name,
  bucket_seconds,
  min(bucket_time) AS first_bucket,
  max(bucket_time) AS last_bucket,
  sumMerge(count_state) AS observations
FROM observatory.measure_rollups
GROUP BY definition_id, measure_name, dimension_name, bucket_seconds
ORDER BY observations DESC
LIMIT 100
```

Available state histories:

```sql
SELECT
  definition_id,
  entity_type,
  state_name,
  value_type,
  min(timestamp) AS first_seen,
  max(timestamp) AS last_seen,
  count() AS transitions,
  uniqCombined64(entity_hash) AS entities
FROM observatory.entity_state_updates
GROUP BY definition_id, entity_type, state_name, value_type
ORDER BY transitions DESC
LIMIT 100
```

Pipeline freshness:

```sql
SELECT
  component,
  metric_name,
  anyLast(value) AS value,
  anyLast(unit) AS unit,
  max(timestamp) AS updated_at
FROM observatory.pipeline_metrics
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY component, metric_name
ORDER BY updated_at DESC
LIMIT 100
```

If discovery shows no materialized report, no rollup, and no relevant definition/backfill, do not silently build an expensive raw-events dashboard. State the missing prerequisite and either use a narrow raw query for exploration or create/backfill the needed schema/report first.

## Nanotrace Read Model Catalog

- `events`: canonical raw JSON event log plus source pointer fields. Use for raw payload inspection and tight exploratory queries.
- `event_density_1s`: global per-second event and error counts. Use for unfiltered volume/error trends.
- `field_density_1s`: per-second counts by always-on core fields. Use for grouped histograms.
- `field_topk_1m`: per-minute top-value counts by always-on core fields. Use for top values and facet panels.
- `field_values`: exact lookup index for common identifiers. Use for trace/request/user/account/session/thread/conversation drilldowns.
- `field_index`: definition-backed promoted fields. Use for exact promoted value-to-event drilldowns and promoted facet grouping.
- `definitions`: active schema definitions: `field`, `measure`, `rollup`, `state`.
- `definition_stats`: latest estimate/backfill evidence for definitions.
- `event_measures`: per-event numeric observations. Use for narrow ad hoc numeric aggregations.
- `measure_rollups`: precomputed aggregate states for numeric measures grouped by configured dimensions.
- `entity_state_updates`: longitudinal entity state changes, such as `account.plan` by `account_id`.
- `report_results`: generic materialized report output with JSON dimensions/metrics.
- `sequence_report_results`: materialized funnel/ordered-step outputs.
- `cohort_memberships`: materialized reusable entity sets.
- `pipeline_metrics`: operational freshness and pipeline-health metrics.

## Physical Read-Model Reference

These SQL snippets explain what each serving model contains. Do not paste them into dashboard iframe modules; the dashboard runtime rejects SQL payloads. Use the typed helper that maps to the same model, or create the missing definition/report first.

Global event count over time from `event_density_1s`:

```sql
SELECT
  toStartOfInterval(bucket_time, INTERVAL 5 MINUTE) AS bucket,
  sum(count) AS events,
  sum(error_count) AS errors
FROM observatory.event_density_1s
WHERE bucket_time >= {created_after:DateTime64(3, 'UTC')}
  AND bucket_time <= {created_before:DateTime64(3, 'UTC')}
GROUP BY bucket
ORDER BY bucket
```

Grouped event trend from `field_density_1s`:

```sql
SELECT
  toStartOfInterval(bucket_time, INTERVAL 5 MINUTE) AS bucket,
  value,
  sum(count) AS events,
  sum(error_count) AS errors,
  sum(duration_sum) / nullIf(sum(duration_count), 0) AS avg_duration_ms
FROM observatory.field_density_1s
WHERE field_name = 'service'
  AND bucket_time >= {created_after:DateTime64(3, 'UTC')}
  AND bucket_time <= {created_before:DateTime64(3, 'UTC')}
GROUP BY bucket, value
ORDER BY bucket, events DESC
```

Top facet values from `field_topk_1m`:

```sql
SELECT
  value,
  sum(count) AS events,
  sum(error_count) AS errors
FROM observatory.field_topk_1m
WHERE field_name = 'service'
  AND bucket_time >= {created_after:DateTime64(3, 'UTC')}
  AND bucket_time <= {created_before:DateTime64(3, 'UTC')}
GROUP BY value
ORDER BY events DESC
LIMIT 20
```

Exact identifier drilldown from `field_values` to event rows:

```sql
SELECT
  e.timestamp,
  ifNull(toString(e.data.name), '') AS name,
  e.event_type,
  e.signal,
  e.event_id,
  e.source_file,
  e.source_offset,
  e.source_length
FROM observatory.field_values f
INNER JOIN observatory.events e
  ON e.event_id = f.event_id
WHERE f.field_name = 'request_id'
  AND f.value = {request_id:String}
  AND timestamp >= {created_after:DateTime64(3, 'UTC')}
  AND timestamp <= {created_before:DateTime64(3, 'UTC')}
ORDER BY e.timestamp DESC
LIMIT 200
```

Short-window measure aggregation from `event_measures`:

```sql
SELECT
  toStartOfInterval(timestamp, INTERVAL 5 MINUTE) AS bucket,
  avg(value) AS avg_value,
  quantileTDigest(0.95)(value) AS p95
FROM observatory.event_measures
WHERE measure_name = 'duration_ms'
  AND timestamp >= {created_after:DateTime64(3, 'UTC')}
  AND timestamp <= {created_before:DateTime64(3, 'UTC')}
GROUP BY bucket
ORDER BY bucket
```

Precomputed rollup from `measure_rollups`:

```sql
SELECT
  bucket_time,
  dimension_value,
  sumMerge(count_state) AS count,
  avgMerge(avg_state) AS avg_value,
  quantilesTDigestMerge(0.5, 0.9, 0.95, 0.99)(quantiles_state)[3] AS p95
FROM observatory.measure_rollups
WHERE measure_name = 'duration_ms'
  AND dimension_name = 'service'
  AND bucket_time >= {created_after:DateTime64(3, 'UTC')}
  AND bucket_time <= {created_before:DateTime64(3, 'UTC')}
GROUP BY bucket_time, dimension_value
ORDER BY bucket_time, count DESC
```

State transitions from `entity_state_updates`:

```sql
SELECT
  toStartOfWeek(timestamp) AS week,
  value,
  count() AS transitions,
  uniqCombined64(entity_hash) AS entities
FROM observatory.entity_state_updates
WHERE state_name = 'account.plan'
  AND entity_type = 'account'
  AND timestamp >= {created_after:DateTime64(3, 'UTC')}
  AND timestamp <= {created_before:DateTime64(3, 'UTC')}
GROUP BY week, value
ORDER BY week, value
```

Recent event rows with raw source pointers:

```sql
SELECT
  timestamp,
  name,
  event_type,
  signal,
  event_id,
  source_file,
  source_offset,
  source_length
FROM observatory.events
WHERE timestamp >= {created_after:DateTime64(3, 'UTC')}
  AND timestamp <= {created_before:DateTime64(3, 'UTC')}
ORDER BY timestamp DESC
LIMIT 200
```

## Reports In Dashboards

Reports are named analytics specs. They are different from dashboard visualizations:

- A **report spec** says what should be computed, such as `weekly revenue by account.plan`, `signup -> checkout funnel`, or `30-day retention for activated users`.
- A **report worker** computes that spec and materializes compact result read models.
- A **dashboard visualization** renders those compact result rows through `/v1/query`.

When a widget is backed by a saved report, query by the stable `report_id` and expose only display concerns inside the iframe. Do not reimplement the report logic in every widget.

Generic report result example:

```js
const reportQuery = useQuery({
  queryKey: ['report-results', 'rep_weekly_revenue', params.query.timeRange],
  queryFn: () => nanotrace.report({
    reportId: 'rep_weekly_revenue',
    limit: 500
  })
});
```

Sequence/funnel report example:

```js
const funnelQuery = useQuery({
  queryKey: ['sequence-report', 'rep_activation_funnel', params.query.timeRange],
  queryFn: () => nanotrace.funnel({
    reportId: 'rep_activation_funnel'
  })
});
```

Cohort-backed widget example:

```js
const cohortQuery = useQuery({
  queryKey: ['cohort-members', 'cohort_power_users'],
  queryFn: () => nanotrace.cohort({
    cohortId: 'cohort_power_users',
    limit: 1000
  })
});
```

If a report has not been materialized yet, prefer a clear empty state such as `Report has no results yet` instead of falling back to an expensive raw recomputation inside the visualization.

## Read Model Selection

Use this order when choosing a read model:

1. Saved report already exists and has materialized output: use `report_results`, `sequence_report_results`, or `cohort_memberships`.
2. Numeric chart maps to a rollup definition: use `measure_rollups`.
3. Global event volume/errors: use `event_density_1s`.
4. Top values/facets on core fields: use `field_topk_1m`.
5. Grouped histograms on core fields: use `field_density_1s`.
6. Exact identifier drilldown: use `field_values`, then hydrate from `events` if payload is needed.
7. Promoted custom field drilldown/grouping: use `field_index`, then hydrate from `events` if payload is needed.
8. Raw bespoke calculation or event timeline/list: use `events`, with strict time predicates and limits.

## Charting Library Choice

- Default to **Chart.js** loaded from esm.sh for bar, line, area, pie, doughnut, radar, polar area, scatter, and bubble charts. It is the smallest, simplest option that fits the iframe sandbox.
- Use **ECharts** when Chart.js does not natively cover the chart type: sankey, treemap, sunburst, heatmap, geo/choropleth, network/graph, candlestick, gauge, parallel coordinates, calendar, funnel, boxplot.
- Do not introduce a third charting library. KPIs, plain tables, and lists should stay as hand-rolled React + CSS — do not pull in a chart library for them.
- For numeric or text-only modules (counts, recent events, lists), keep using plain React without any charting library.

### Chart.js usage rules

- Import the auto bundle: `import Chart from 'https://esm.sh/chart.js@4/auto'`.
- For time axes, import a date adapter: `import 'https://esm.sh/chartjs-adapter-date-fns@3'`.
- Wrap the `<canvas>` in a `position: relative; height: 100%; width: 100%` div and set `responsive: true, maintainAspectRatio: false`.
- Call `chart.destroy()` in the effect cleanup. Reusing a canvas without destroying first throws "Canvas is already in use".
- Match the dark UI: `ticks: { color: '#737373' }`, `grid: { color: '#1a1a1a' }`, legend labels `'#d4d4d4'`, accent `'#22d3ee'`.

### ECharts usage rules

- Import as `import * as echarts from 'https://esm.sh/echarts@5'`.
- Use `echarts.init(container, 'dark', { renderer: 'canvas' })`. Use `'svg'` only for small dense charts under ~2k points.
- Drive a `ResizeObserver` that calls `chart.resize()`.
- Call `chart.dispose()` in the effect cleanup.
- Memoize the `option` object and call `chart.setOption(option)` only when data changes.
- Set `backgroundColor: 'transparent'` so the chart inherits the card surface.

## Common Patterns

KPI card:

```js
return React.createElement('div', { style: styles.root },
  React.createElement('div', { style: styles.label }, params.timeRange ? 'Selected range' : 'All events'),
  React.createElement('div', { style: styles.value }, loading ? '...' : formatNumber(value))
);
```

List body without duplicated title:

```js
return React.createElement('div', { style: styles.root },
  React.createElement('div', { style: styles.rows },
    rows.map((row, index) => React.createElement('div', { key: index, style: styles.row }, ...))
  )
);
```

Chart with contextual subtitle:

```js
React.createElement('div', { style: styles.header },
  React.createElement('div', { style: styles.subtitle },
    params.query.groupBy ? 'Grouped by ' + params.query.groupBy : 'Total'
  )
)
```

Chart.js skeleton (default):

```js
import React, { useEffect, useRef } from 'https://esm.sh/react@19.2.1';
import { QueryClient, QueryClientProvider, useQuery } from 'https://esm.sh/@tanstack/react-query@5.100.10?deps=react@19.2.1';
import Chart from 'https://esm.sh/chart.js@4/auto';
import 'https://esm.sh/chartjs-adapter-date-fns@3';

const queryClient = new QueryClient({ defaultOptions: { queries: { refetchOnWindowFocus: false, staleTime: 3000 } } });
const EMPTY_ROWS = [];

export default function EventsOverTime(props) {
  return React.createElement(QueryClientProvider, { client: queryClient },
    React.createElement(EventsOverTimeBody, props)
  );
}

function EventsOverTimeBody({ nanotrace, params }) {
  const canvasRef = useRef(null);
  const chartRef = useRef(null);
  const eventsQuery = useQuery({
    queryKey: ['events-over-time', params.query.timeRange, params.query.eventFilter],
    queryFn: () => nanotrace.events({
      view: 'density',
      buckets: 120
    })
  });

  const rows = eventsQuery.data?.data || eventsQuery.data?.buckets || EMPTY_ROWS;

  useEffect(() => {
    if (!canvasRef.current || eventsQuery.isLoading || eventsQuery.error) return;
    chartRef.current?.destroy();
    chartRef.current = new Chart(canvasRef.current, {
      type: 'bar',
      data: { datasets: [{ label: 'Events', data: rows.map(r => ({ x: r.t, y: Number(r.c) })), backgroundColor: '#22d3ee' }] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: { type: 'time', ticks: { color: '#737373' }, grid: { color: '#1a1a1a' } },
          y: { ticks: { color: '#737373' }, grid: { color: '#1a1a1a' } }
        },
        plugins: { legend: { labels: { color: '#d4d4d4' } } }
      }
    });
    return () => chartRef.current?.destroy();
  }, [eventsQuery.error, eventsQuery.isLoading, rows]);

  if (eventsQuery.error) return React.createElement('pre', { style: { color: '#fecaca', padding: 12 } }, String(eventsQuery.error.message || eventsQuery.error));
  return React.createElement('div', { style: { position: 'relative', height: '100%', width: '100%' } },
    React.createElement('canvas', { ref: canvasRef })
  );
}
```

ECharts skeleton (fallback for advanced chart types):

```js
import React, { useEffect, useMemo, useRef } from 'https://esm.sh/react@19.2.1';
import { QueryClient, QueryClientProvider, useQuery } from 'https://esm.sh/@tanstack/react-query@5.100.10?deps=react@19.2.1';
import * as echarts from 'https://esm.sh/echarts@5';

const queryClient = new QueryClient({ defaultOptions: { queries: { refetchOnWindowFocus: false, staleTime: 3000 } } });
const EMPTY_ROWS = [];

export default function ServiceFlow(props) {
  return React.createElement(QueryClientProvider, { client: queryClient },
    React.createElement(ServiceFlowBody, props)
  );
}

function ServiceFlowBody({ nanotrace, params }) {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const flowQuery = useQuery({
    queryKey: ['service-flow', params.query.timeRange, params.query.groupBy],
    queryFn: () => nanotrace.report({ reportId: 'service_flow_summary', limit: 500 })
  });
  const rows = flowQuery.data?.rows || flowQuery.data?.data || EMPTY_ROWS;

  useEffect(() => {
    if (!containerRef.current) return;
    const chart = echarts.init(containerRef.current, 'dark', { renderer: 'canvas' });
    chartRef.current = chart;
    const observer = new ResizeObserver(() => chart.resize());
    observer.observe(containerRef.current);
    return () => { observer.disconnect(); chart.dispose(); };
  }, []);

  const option = useMemo(() => ({
    backgroundColor: 'transparent',
    series: [{ type: 'sankey', data: [], links: [] }]
  }), [rows]);

  useEffect(() => { if (chartRef.current && !flowQuery.isLoading && !flowQuery.error) chartRef.current.setOption(option); }, [flowQuery.error, flowQuery.isLoading, option]);

  return React.createElement('div', { ref: containerRef, style: { height: '100%', width: '100%' } });
}
```

## Review Checklist

- Host card title is not repeated inside the iframe.
- The visualization uses only bound params.
- Loading, empty, and error states are readable.
- Scroll is owned by one intentional region.
- Text truncates or wraps deliberately.
- Resizing the dashboard card does not break the visualization.
- Query parameters are not string-interpolated except for trusted SQL fragments supplied by the runtime.
- TanStack Query imports are pinned to `@tanstack/react-query@5.100.10` and `react@19.2.1`.
- Chart.js is used for standard chart types; ECharts is used only for chart types Chart.js does not natively support.
- Chart instances are destroyed (`chart.destroy()` / `chart.dispose()`) in the effect cleanup.
