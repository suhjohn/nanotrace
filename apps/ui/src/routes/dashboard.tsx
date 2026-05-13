import { createFileRoute } from '@tanstack/react-router'
import { useMutation } from '@tanstack/react-query'
import { BarChart3, Hash, LineChart, PanelLeftOpen, Play, Table2 } from 'lucide-react'
import { useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import { cn } from '../lib/cn'
import { useAppShell } from '../lib/app-shell'

export const Route = createFileRoute('/dashboard')({
  component: DashboardRoute
})

type ChartKind = 'table' | 'bar' | 'line' | 'number'

type QueryResponse = {
  data: Record<string, unknown>[]
  meta?: { name: string; type: string }[]
  rows: number
  statistics?: {
    bytes_read?: number
    elapsed?: number
    rows_read?: number
  }
}

const starterQuery = `SELECT
  toStartOfMinute(timestamp) AS minute,
  count() AS events
FROM observatory.events
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute`

const chartOptions: { icon: ReactNode; kind: ChartKind; label: string }[] = [
  { icon: <Table2 size={15} strokeWidth={1.8} />, kind: 'table', label: 'Table' },
  { icon: <BarChart3 size={15} strokeWidth={1.8} />, kind: 'bar', label: 'Bar' },
  { icon: <LineChart size={15} strokeWidth={1.8} />, kind: 'line', label: 'Line' },
  { icon: <Hash size={15} strokeWidth={1.8} />, kind: 'number', label: 'Number' }
]

function DashboardRoute() {
  const { setSidebarOpen, sidebarOpen } = useAppShell()
  const [query, setQuery] = useState(starterQuery)
  const [chartKind, setChartKind] = useState<ChartKind>('table')
  const queryMutation = useMutation({
    mutationFn: () => runQuery(query)
  })
  const result = queryMutation.data
  const columns = useMemo(() => resultColumns(result), [result])

  return (
    <main className="flex h-full min-h-0 min-w-0 flex-col overflow-hidden bg-black text-[13px] text-neutral-100">
      <header className="flex h-10 shrink-0 items-center justify-between gap-3 border-b border-neutral-800 bg-neutral-950 px-3">
        <div className="flex min-w-0 items-center gap-2">
          {!sidebarOpen ? (
            <button
              aria-label="Expand navigation"
              className="inline-flex h-7 w-7 shrink-0 items-center justify-center border border-neutral-800 bg-black text-neutral-400 hover:bg-white/[0.04] hover:text-white"
              title="Expand navigation"
              type="button"
              onClick={() => setSidebarOpen(true)}
            >
              <PanelLeftOpen size={15} strokeWidth={1.8} />
            </button>
          ) : null}
          <div className="truncate text-[13px] font-medium text-white">Dashboard</div>
        </div>
        <div className="flex items-center gap-1">
          {chartOptions.map(option => (
            <button
              key={option.kind}
              aria-label={option.label}
              className={cn(
                'inline-flex h-7 w-7 items-center justify-center border border-neutral-800 bg-black text-neutral-400 hover:bg-white/[0.04] hover:text-white',
                chartKind === option.kind && 'border-neutral-600 bg-neutral-800 text-white'
              )}
              title={option.label}
              type="button"
              onClick={() => setChartKind(option.kind)}
            >
              {option.icon}
            </button>
          ))}
          <button
            className="ml-2 inline-flex h-7 items-center gap-1.5 border border-neutral-700 bg-white px-2 text-[12px] font-medium text-black hover:bg-neutral-200 disabled:border-neutral-800 disabled:bg-neutral-900 disabled:text-neutral-600"
            disabled={!query.trim() || queryMutation.isPending}
            type="button"
            onClick={() => queryMutation.mutate()}
          >
            <Play size={13} strokeWidth={2} />
            Run
          </button>
        </div>
      </header>
      <section className="grid min-h-0 flex-1 grid-cols-[minmax(320px,420px)_minmax(0,1fr)] overflow-hidden">
        <aside className="flex min-h-0 min-w-0 flex-col border-r border-neutral-800 bg-neutral-950">
          <div className="border-b border-neutral-800 px-3 py-2">
            <div className="text-[11px] uppercase text-neutral-500">Question</div>
            <div className="mt-1 text-[12px] text-neutral-300">Write a ClickHouse query and choose a visualization.</div>
          </div>
          <textarea
            className="min-h-0 flex-1 resize-none bg-black p-3 font-mono text-[12px] leading-5 text-neutral-100 outline-none placeholder:text-neutral-700"
            spellCheck={false}
            value={query}
            onChange={event => setQuery(event.target.value)}
          />
          {queryMutation.error ? (
            <div className="border-t border-red-950 bg-red-950/30 px-3 py-2 text-[12px] text-red-200">
              {errorMessage(queryMutation.error)}
            </div>
          ) : null}
        </aside>
        <section className="flex min-h-0 min-w-0 flex-col overflow-hidden">
          <div className="flex h-9 shrink-0 items-center justify-between border-b border-neutral-800 px-3 text-[12px] text-neutral-500">
            <span>{result ? `${result.rows.toLocaleString()} rows` : 'No result yet'}</span>
            {result?.statistics?.elapsed ? <span>{(result.statistics.elapsed * 1000).toFixed(1)} ms</span> : null}
          </div>
          <div className="min-h-0 flex-1 overflow-auto p-3">
            {result ? (
              <Visualization chartKind={chartKind} columns={columns} result={result} />
            ) : (
              <div className="flex h-full items-center justify-center text-[12px] text-neutral-600">
                Run a query to render a visualization.
              </div>
            )}
          </div>
        </section>
      </section>
    </main>
  )
}

function Visualization({
  chartKind,
  columns,
  result
}: {
  chartKind: ChartKind
  columns: string[]
  result: QueryResponse
}) {
  if (chartKind === 'number') return <NumberView columns={columns} result={result} />
  if (chartKind === 'bar') return <BarView columns={columns} result={result} />
  if (chartKind === 'line') return <LineView columns={columns} result={result} />
  return <TableView columns={columns} rows={result.data} />
}

function NumberView({ columns, result }: { columns: string[]; result: QueryResponse }) {
  const numeric = numericColumns(columns, result.data)[0] ?? columns[0]
  const value = result.data[0]?.[numeric]
  return (
    <div className="flex h-full items-center justify-center">
      <div className="text-center">
        <div className="text-[11px] uppercase text-neutral-500">{numeric || 'Value'}</div>
        <div className="mt-2 text-5xl font-semibold text-white">{formatCell(value)}</div>
      </div>
    </div>
  )
}

function BarView({ columns, result }: { columns: string[]; result: QueryResponse }) {
  const xColumn = columns[0]
  const yColumn = numericColumns(columns, result.data)[0]
  if (!xColumn || !yColumn) return <EmptyChart message="Bar chart needs one label column and one numeric column." />
  const rows = result.data.slice(0, 80)
  const max = Math.max(...rows.map(row => numberValue(row[yColumn])), 0)
  return (
    <div className="grid content-start gap-1">
      {rows.map((row, index) => {
        const value = numberValue(row[yColumn])
        const width = max > 0 ? `${Math.max(2, (value / max) * 100)}%` : '2%'
        return (
          <div key={index} className="grid grid-cols-[160px_minmax(0,1fr)_72px] items-center gap-2 text-[12px]">
            <div className="truncate text-neutral-400">{formatCell(row[xColumn])}</div>
            <div className="h-5 bg-neutral-900">
              <div className="h-full bg-cyan-500/80" style={{ width }} />
            </div>
            <div className="text-right font-mono text-neutral-300">{formatNumber(value)}</div>
          </div>
        )
      })}
    </div>
  )
}

function LineView({ columns, result }: { columns: string[]; result: QueryResponse }) {
  const yColumn = numericColumns(columns, result.data)[0]
  if (!yColumn) return <EmptyChart message="Line chart needs a numeric column." />
  const rows = result.data.slice(0, 240)
  const values = rows.map(row => numberValue(row[yColumn]))
  const max = Math.max(...values, 0)
  const min = Math.min(...values, 0)
  const range = max - min || 1
  const points = values
    .map((value, index) => {
      const x = rows.length <= 1 ? 0 : (index / (rows.length - 1)) * 100
      const y = 100 - ((value - min) / range) * 100
      return `${x},${y}`
    })
    .join(' ')
  return (
    <div className="grid h-full min-h-[280px] grid-rows-[1fr_auto] gap-3">
      <svg className="h-full w-full overflow-visible" preserveAspectRatio="none" viewBox="0 0 100 100">
        <polyline fill="none" points={points} stroke="rgb(34 211 238)" strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
      </svg>
      <div className="flex justify-between font-mono text-[11px] text-neutral-500">
        <span>{formatNumber(min)}</span>
        <span>{yColumn}</span>
        <span>{formatNumber(max)}</span>
      </div>
    </div>
  )
}

function TableView({ columns, rows }: { columns: string[]; rows: Record<string, unknown>[] }) {
  return (
    <div className="overflow-auto border border-neutral-800">
      <table className="w-full border-collapse text-left text-[12px]">
        <thead className="sticky top-0 bg-neutral-950 text-neutral-400">
          <tr>
            {columns.map(column => (
              <th key={column} className="border-b border-neutral-800 px-2 py-1.5 font-medium">
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={index} className="border-b border-neutral-900 odd:bg-white/[0.015]">
              {columns.map(column => (
                <td key={column} className="max-w-[360px] truncate px-2 py-1.5 font-mono text-neutral-300">
                  {formatCell(row[column])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function EmptyChart({ message }: { message: string }) {
  return <div className="flex h-full items-center justify-center text-[12px] text-neutral-600">{message}</div>
}

async function runQuery(query: string): Promise<QueryResponse> {
  const response = await fetch('/query', {
    body: JSON.stringify({ query }),
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(text || response.statusText)
  }
  return (await response.json()) as QueryResponse
}

function queryHeaders() {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  const token = import.meta.env.VITE_NANOTRACE_API_KEY
  if (token) headers.Authorization = `Bearer ${token}`
  return headers
}

function resultColumns(result: QueryResponse | undefined) {
  if (!result) return []
  if (result.meta?.length) return result.meta.map(column => column.name)
  return Object.keys(result.data[0] ?? {})
}

function numericColumns(columns: string[], rows: Record<string, unknown>[]) {
  return columns.filter(column => rows.some(row => typeof row[column] === 'number'))
}

function numberValue(value: unknown) {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0
}

function formatNumber(value: number) {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value)
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return ''
  if (typeof value === 'number') return formatNumber(value)
  if (typeof value === 'string') return value
  return JSON.stringify(value)
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}
