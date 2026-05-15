---
name: create-nanotrace-visualization
description: Create, review, or improve Nanotrace dashboard iframe visualizations stored as persisted React modules. Use when working on Nanotrace dashboard visualization sourceCode, chart/table/card modules, iframe rendering, dashboard parameter bindings, scroll behavior, duplicate titles, or the Postgres-backed nanotrace_dashboard_visualizations records.
---

# Create Nanotrace Visualization

## Goal

Build visualization modules that feel like the body of a dashboard card, not a full standalone app. The Nanotrace host already provides card chrome: title, bindings, grid size, drag, resize, and edit controls.

## Workflow

1. Inspect the existing saved module before editing.
   - Use the dashboard API or local state to read the visualization `title`, `parameterBindings`, layout, and `sourceCode`.

2. Decide what the host owns versus what the iframe owns.
   - Host owns card title, edit/resize/move controls, grid dimensions, and binding chips.
   - Iframe owns the chart, table, number, empty state, error state, and concise contextual subtitles.
   - Do not repeat the host title inside the iframe unless the inner title adds distinct information.

3. Keep the module small and predictable.
   - Export one default React component.
   - Use TanStack Query for async state and call `nanotrace.query({ query, parameters })` inside `queryFn`.
   - Use dashboard params only when the visualization lists the matching binding.
   - Handle loading, empty, and error states.
   - Keep styles local, but follow the existing dark, dense, operational UI.

4. Verify visually.
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
- Always pass `parameters: params.sql.parameters` when using `params.sql.where`.
- Reuse `params.sql.where` for global time/filter clauses.
- If the module supports grouping, use `params.sql.groupByExpression` and `params.sql.groupByLabel`.
- Keep limits explicit for tables and event lists.
- Prefer query output names that map directly to rendered fields.

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
    params.sql.groupByLabel ? 'Grouped by ' + params.sql.groupByLabel : 'Total'
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
    queryKey: ['events-over-time', params.sql.where, params.sql.parameters],
    queryFn: () => nanotrace.query({
      parameters: params.sql.parameters,
      query: `SELECT toStartOfMinute(timestamp) AS t, count() AS c
              FROM observatory.events
              WHERE ${params.sql.where}
              GROUP BY t ORDER BY t`
    })
  });

  const rows = eventsQuery.data?.data || EMPTY_ROWS;

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
    queryKey: ['service-flow', params.sql.where, params.sql.parameters],
    queryFn: () => nanotrace.query({ parameters: params.sql.parameters, query: `...` })
  });
  const rows = flowQuery.data?.data || EMPTY_ROWS;

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
