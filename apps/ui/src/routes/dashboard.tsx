import { createFileRoute } from '@tanstack/react-router'
import { Check, Code2, Grip, PanelLeftOpen, Save, Trash2, X } from 'lucide-react'
import { useEffect, useMemo, useRef, useState } from 'react'
import { cn } from '../lib/cn'
import { useAppShell } from '../lib/app-shell'
import { nanotraceApiBaseUrl, queryHeaders } from '../lib/nanotrace-api'

export const Route = createFileRoute('/dashboard')({
  component: DashboardRoute
})

type DashboardVisualization = {
  dashboardId: string
  height: number
  id: string
  parameterBindings: DashboardParameterKey[]
  sourceCode: string
  title: string
  updatedAt: string
  width: number
  x: number
  y: number
}

type DragState =
  | {
      id: string
      kind: 'move' | 'resize'
      originHeight: number
      originWidth: number
      originX: number
      originY: number
      pointerX: number
      pointerY: number
    }
  | null

type QueryPayload = {
  parameters?: Record<string, unknown>
  query: string
}

type DashboardParameterKey = 'timeRange' | 'filter' | 'groupBy'

type TimeRangeKey = '15m' | '1h' | '6h' | '24h' | '7d' | 'custom'

type ResolvedTimeRange = {
  createdAfter?: string
  createdBefore?: string
  key: string
  lookbackMinutes?: number
}

type ParsedEventFilter = {
  createdAfter?: string
  createdBefore?: string
  facets?: ParsedFacetFilter[]
  text: string
}

type ParsedFacetFilter = {
  path: string
  value: string
}

type DashboardRuntimeParams = Partial<Record<DashboardParameterKey, unknown>> & {
  sql: {
    groupByExpression?: string
    groupByLabel?: string
    parameters: Record<string, unknown>
    where: string
  }
}

const dashboardId = 'local-dashboard'
const gridColumns = 96
const rowHeight = 12
const minBlockWidth = 24
const minBlockHeight = 16
const dashboardParameterKeys: DashboardParameterKey[] = ['timeRange', 'filter', 'groupBy']
const noGroupValue = '__nanotrace_no_group__'
const timeRangeOptions: { key: Exclude<TimeRangeKey, 'custom'>; label: string; minutes: number }[] = [
  { key: '15m', label: '15m', minutes: 15 },
  { key: '1h', label: '1h', minutes: 60 },
  { key: '6h', label: '6h', minutes: 6 * 60 },
  { key: '24h', label: '24h', minutes: 24 * 60 },
  { key: '7d', label: '7d', minutes: 7 * 24 * 60 }
]
const dashboardGroupOptions = [
  'service',
  'name',
  'environment',
  'llm.model',
  'http.route',
  'http.status_code',
  'severity_text',
  'traceId',
  'user_id',
  'account_id'
]

function DashboardRoute() {
  const observatoryUrl = nanotraceApiBaseUrl()
  const { setSidebarOpen, sidebarOpen } = useAppShell()
  const [visualizations, setVisualizations] = useState<DashboardVisualization[]>([])
  const [selectedId, setSelectedId] = useState('')
  const [draftTitle, setDraftTitle] = useState('')
  const [draftSource, setDraftSource] = useState('')
  const [draftBindings, setDraftBindings] = useState<DashboardParameterKey[]>([])
  const [editorOpen, setEditorOpen] = useState(false)
  const [timeRangeKey, setTimeRangeKey] = useState<TimeRangeKey>('24h')
  const [customRangeStart, setCustomRangeStart] = useState(() => formatDateTimeLocalInput(new Date(Date.now() - 60 * 60 * 1000)))
  const [customRangeEnd, setCustomRangeEnd] = useState(() => formatDateTimeLocalInput(new Date()))
  const [filterDraft, setFilterDraft] = useState('')
  const [eventFilterParams, setEventFilterParams] = useState<ParsedEventFilter>({ text: '' })
  const [groupBy, setGroupBy] = useState('service')
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [deleting, setDeleting] = useState(false)
  const [dragging, setDragging] = useState<DragState>(null)
  const viewportRef = useRef<HTMLDivElement | null>(null)
  const visualizationsRef = useRef<DashboardVisualization[]>([])
  const [viewportWidth, setViewportWidth] = useState(960)
  const selected = visualizations.find(item => item.id === selectedId) ?? null
  const selectedTimeRange = useMemo(
    () => resolveTimeRange({ customEnd: customRangeEnd, customStart: customRangeStart, key: timeRangeKey }),
    [customRangeEnd, customRangeStart, timeRangeKey]
  )
  const globalParams = useMemo(
    () => buildDashboardParams({
      eventFilter: eventFilterParams,
      groupBy: groupBy === noGroupValue ? '' : groupBy,
      timeRange: selectedTimeRange
    }),
    [eventFilterParams, groupBy, selectedTimeRange]
  )
  const canvasPixelWidth = Math.max(360, viewportWidth)
  const columnWidth = canvasPixelWidth / gridColumns
  const canvasSize = useMemo(() => {
    const maxBottom = Math.max(32, ...visualizations.map(item => item.y + item.height + 10))
    return { height: Math.max(1400, maxBottom * rowHeight), width: canvasPixelWidth }
  }, [canvasPixelWidth, visualizations])

  useEffect(() => {
    visualizationsRef.current = visualizations
  }, [visualizations])

  useEffect(() => {
    let cancelled = false
    dashboardApi.listVisualizations(observatoryUrl, dashboardId).then(items => {
      if (cancelled) return
      setVisualizations(items)
      setSelectedId(items[0]?.id ?? '')
      setLoading(false)
    })
    return () => {
      cancelled = true
    }
  }, [observatoryUrl])

  useEffect(() => {
    if (!selected) return
    setDraftTitle(selected.title)
    setDraftSource(selected.sourceCode)
    setDraftBindings(selected.parameterBindings)
  }, [selected])

  useEffect(() => {
    const element = viewportRef.current
    if (!element) return

    const updateWidth = () => setViewportWidth(element.clientWidth)
    updateWidth()
    const observer = new ResizeObserver(updateWidth)
    observer.observe(element)
    return () => observer.disconnect()
  }, [])

  useEffect(() => {
    function onMessage(event: MessageEvent) {
      const message = event.data as { blockId?: string; payload?: QueryPayload; requestId?: string; type?: string }
      if (message.type !== 'nanotrace-query' || !message.requestId || !message.blockId || !message.payload) return

      queryNanotrace(message.payload)
        .then(result => {
          event.source?.postMessage(
            { requestId: message.requestId, result, type: 'nanotrace-query-result' },
            { targetOrigin: '*' }
          )
        })
        .catch(error => {
          event.source?.postMessage(
            { error: errorMessage(error), requestId: message.requestId, type: 'nanotrace-query-result' },
            { targetOrigin: '*' }
          )
        })
    }

    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])

  useEffect(() => {
    if (!dragging) return
    const activeDrag = dragging

    function onPointerMove(event: PointerEvent) {
      const dx = Math.round((event.clientX - activeDrag.pointerX) / columnWidth)
      const dy = Math.round((event.clientY - activeDrag.pointerY) / rowHeight)
      setVisualizations(items =>
        items.map(item => {
          if (item.id !== activeDrag.id) return item
          if (activeDrag.kind === 'resize') {
            return {
              ...item,
              height: Math.max(minBlockHeight, activeDrag.originHeight + dy),
              width: clamp(activeDrag.originWidth + dx, minBlockWidth, gridColumns - item.x)
            }
          }
          return {
            ...item,
            x: clamp(activeDrag.originX + dx, 0, gridColumns - item.width),
            y: Math.max(0, activeDrag.originY + dy)
          }
        })
      )
    }

    function onPointerUp() {
      const item = visualizationsRef.current.find(candidate => candidate.id === activeDrag.id)
      setDragging(null)
      if (item) void persistVisualization(item)
    }

    document.body.style.cursor = activeDrag.kind === 'resize' ? 'nwse-resize' : 'grabbing'
    document.body.style.userSelect = 'none'
    window.addEventListener('pointermove', onPointerMove)
    window.addEventListener('pointerup', onPointerUp)
    return () => {
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
      window.removeEventListener('pointermove', onPointerMove)
      window.removeEventListener('pointerup', onPointerUp)
    }
  }, [columnWidth, dragging])

  async function saveSelected() {
    if (!selected) return
    setSaving(true)
    try {
      const updated = await dashboardApi.updateVisualization(observatoryUrl, {
        ...selected,
        parameterBindings: draftBindings,
        sourceCode: draftSource,
        title: draftTitle.trim() || 'Untitled visualization'
      })
      setVisualizations(items => items.map(item => item.id === updated.id ? updated : item))
    } finally {
      setSaving(false)
    }
  }

  async function persistVisualization(item: DashboardVisualization) {
    const updated = await dashboardApi.updateVisualization(observatoryUrl, item)
    setVisualizations(items => items.map(candidate => candidate.id === updated.id ? updated : candidate))
  }

  async function deleteSelected() {
    if (!selected || deleting) return
    const deletedId = selected.id
    setDeleting(true)
    try {
      await dashboardApi.deleteVisualization(observatoryUrl, selected)
      setVisualizations(items => {
        const remaining = items.filter(item => item.id !== deletedId)
        setSelectedId(remaining[0]?.id ?? '')
        if (remaining.length === 0) setEditorOpen(false)
        return remaining
      })
    } finally {
      setDeleting(false)
    }
  }

  function applyFilter() {
    setEventFilterParams(parseEventFilter(filterDraft))
  }

  function clearFilter() {
    setFilterDraft('')
    setEventFilterParams({ text: '' })
  }

  function selectTimeRange(key: TimeRangeKey) {
    setTimeRangeKey(key)
  }

  return (
    <main className="flex h-full min-h-0 min-w-0 flex-col overflow-hidden bg-black text-[13px] text-neutral-100">
      <header className="flex min-h-10 shrink-0 flex-wrap items-center gap-2 border-b border-neutral-800 bg-neutral-950 px-3 py-1.5">
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
        </div>
        <form
          className="flex min-w-[280px] flex-1 items-center gap-1.5"
          onSubmit={event => {
            event.preventDefault()
            applyFilter()
          }}
        >
          <input
            className="h-7 min-w-0 flex-1 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none placeholder:text-neutral-600 focus:border-neutral-600"
            value={filterDraft}
            onChange={event => setFilterDraft(event.target.value)}
            placeholder="filter events, e.g. name=llm service=api"
          />
          <button
            aria-label="Apply dashboard filter"
            className="inline-flex h-7 w-7 shrink-0 items-center justify-center border border-neutral-800 bg-black text-neutral-300 hover:bg-white/[0.04] hover:text-white"
            title="Apply filter"
            type="submit"
          >
            <Check size={13} strokeWidth={1.8} />
          </button>
          {hasAppliedEventFilter(eventFilterParams) || filterDraft ? (
            <button
              aria-label="Clear dashboard filter"
              className="inline-flex h-7 w-7 shrink-0 items-center justify-center border border-neutral-800 bg-black text-neutral-400 hover:bg-white/[0.04] hover:text-white"
              title="Clear filter"
              type="button"
              onClick={clearFilter}
            >
              <X size={13} strokeWidth={1.8} />
            </button>
          ) : null}
        </form>
        <label className="flex shrink-0 items-center gap-1.5">
          <span className="text-[10px] uppercase tracking-[0.08em] text-neutral-500">Group</span>
          <select
            aria-label="Dashboard group by"
            className="h-7 w-[132px] min-w-0 border border-neutral-800 bg-black px-1.5 text-[11px] text-white outline-none focus:border-neutral-600"
            value={groupBy || noGroupValue}
            onChange={event => setGroupBy(event.target.value)}
          >
            <option value={noGroupValue}>No grouping</option>
            {dashboardGroupOptions.map(option => (
              <option key={option} value={option}>
                {displayFacetPath(option)}
              </option>
            ))}
          </select>
        </label>
        <div className="flex shrink-0 items-center justify-end gap-1.5">
          <div className="flex overflow-hidden border border-neutral-800 bg-black">
            {timeRangeOptions.map(option => (
              <button
                key={option.key}
                className={cn(
                  'h-7 border-l border-neutral-800 px-1.5 text-[10px] text-neutral-400 first:border-l-0 hover:bg-white/[0.04] hover:text-white',
                  timeRangeKey === option.key && 'bg-neutral-800 text-white'
                )}
                type="button"
                onClick={() => selectTimeRange(option.key)}
              >
                {option.label}
              </button>
            ))}
            <button
              className={cn(
                'h-7 border-l border-neutral-800 px-1.5 text-[10px] text-neutral-400 hover:bg-white/[0.04] hover:text-white',
                timeRangeKey === 'custom' && 'bg-neutral-800 text-white'
              )}
              type="button"
              onClick={() => selectTimeRange('custom')}
            >
              Custom
            </button>
          </div>
          {timeRangeKey === 'custom' ? (
            <div className="flex items-center gap-1">
              <input
                aria-label="Custom range start"
                className="h-7 w-[140px] border border-neutral-800 bg-black px-1.5 text-[10px] text-white outline-none"
                type="datetime-local"
                value={customRangeStart}
                onChange={event => setCustomRangeStart(event.target.value)}
              />
              <span className="text-[11px] text-neutral-600">to</span>
              <input
                aria-label="Custom range end"
                className="h-7 w-[140px] border border-neutral-800 bg-black px-1.5 text-[10px] text-white outline-none"
                type="datetime-local"
                value={customRangeEnd}
                onChange={event => setCustomRangeEnd(event.target.value)}
              />
            </div>
          ) : null}
        </div>
      </header>
      <section className={cn(
        'grid min-h-0 flex-1 overflow-hidden',
        editorOpen ? 'grid-cols-[minmax(0,1fr)_minmax(320px,380px)]' : 'grid-cols-1'
      )}>
        <div
          ref={viewportRef}
          className="min-h-0 min-w-0 overflow-auto bg-black overscroll-contain"
        >
          <div
            className="relative"
            style={{
              backgroundColor: '#050505',
              backgroundImage: [
                'linear-gradient(rgba(255,255,255,0.11) 1px, transparent 1px)',
                'linear-gradient(90deg, rgba(255,255,255,0.11) 1px, transparent 1px)',
                'linear-gradient(rgba(255,255,255,0.18) 1px, transparent 1px)',
                'linear-gradient(90deg, rgba(255,255,255,0.18) 1px, transparent 1px)'
              ].join(', '),
              backgroundSize: [
                `${columnWidth}px ${rowHeight}px`,
                `${columnWidth}px ${rowHeight}px`,
                `${columnWidth * 4}px ${rowHeight * 4}px`,
                `${columnWidth * 4}px ${rowHeight * 4}px`
              ].join(', '),
              height: canvasSize.height,
              width: canvasSize.width
            }}
          >
            {loading ? (
              <div className="absolute left-6 top-6 text-[12px] text-neutral-500">Loading dashboard...</div>
            ) : null}
            {visualizations.map(item => (
              <VisualizationBlock
                key={item.id}
                selected={item.id === selectedId}
                visualization={item}
                columnWidth={columnWidth}
                params={pickDashboardParams(globalParams, item.parameterBindings)}
                onDragStart={(event, kind) => {
                  event.preventDefault()
                  setSelectedId(item.id)
                  setDragging({
                    id: item.id,
                    kind,
                    originHeight: item.height,
                    originWidth: item.width,
                    originX: item.x,
                    originY: item.y,
                    pointerX: event.clientX,
                    pointerY: event.clientY
                  })
                }}
                onSelect={() => setSelectedId(item.id)}
                onEdit={() => {
                  setSelectedId(item.id)
                  setEditorOpen(true)
                }}
              />
            ))}
          </div>
        </div>
        {editorOpen ? (
        <aside className="flex min-h-0 min-w-0 flex-col border-l border-neutral-800 bg-neutral-950">
          {selected ? (
            <>
              <div className="flex h-10 shrink-0 items-center justify-between border-b border-neutral-800 px-3">
                <div className="flex min-w-0 items-center gap-2">
                  <Code2 size={14} strokeWidth={1.8} />
                  <span className="truncate text-[12px] font-medium text-white">Visualization code</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <button
                    className="inline-flex h-7 items-center gap-1.5 border border-red-950 bg-black px-2 text-[12px] font-medium text-red-300 hover:bg-red-950/30 hover:text-red-200 disabled:text-neutral-700"
                    disabled={deleting}
                    type="button"
                    onClick={() => void deleteSelected()}
                  >
                    <Trash2 size={13} strokeWidth={1.8} />
                    {deleting ? 'Deleting' : 'Delete'}
                  </button>
                  <button
                    className="inline-flex h-7 items-center gap-1.5 border border-neutral-700 bg-white px-2 text-[12px] font-medium text-black hover:bg-neutral-200 disabled:bg-neutral-900 disabled:text-neutral-600"
                    disabled={saving}
                    type="button"
                    onClick={() => void saveSelected()}
                  >
                    <Save size={13} strokeWidth={2} />
                    {saving ? 'Saving' : 'Save'}
                  </button>
                  <button
                    aria-label="Close editor"
                    className="inline-flex h-7 w-7 items-center justify-center border border-neutral-800 bg-black text-neutral-400 hover:bg-white/[0.04] hover:text-white"
                    title="Close editor"
                    type="button"
                    onClick={() => setEditorOpen(false)}
                  >
                    <X size={13} strokeWidth={1.8} />
                  </button>
                </div>
              </div>
              <div className="grid shrink-0 gap-2 border-b border-neutral-800 p-3">
                <label className="grid gap-1">
                  <span className="text-[10px] uppercase tracking-[0.08em] text-neutral-500">Title</span>
                  <input
                    className="h-8 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none focus:border-neutral-600"
                    value={draftTitle}
                    onChange={event => setDraftTitle(event.target.value)}
                  />
                </label>
                <div className="grid grid-cols-4 gap-1.5 text-[11px] text-neutral-500">
                  <Metric label="X" value={selected.x} />
                  <Metric label="Y" value={selected.y} />
                  <Metric label="W" value={selected.width} />
                  <Metric label="H" value={selected.height} />
                </div>
                <div className="grid gap-1.5">
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-[10px] uppercase tracking-[0.08em] text-neutral-500">Uses dashboard params</span>
                    <ParameterChips bindings={draftBindings} />
                  </div>
                  <div className="grid grid-cols-3 gap-1.5">
                    {dashboardParameterKeys.map(key => (
                      <label
                        key={key}
                        className={cn(
                          'flex h-7 cursor-pointer items-center justify-center border px-1.5 text-[11px]',
                          draftBindings.includes(key)
                            ? 'border-neutral-600 bg-neutral-800 text-white'
                            : 'border-neutral-800 bg-black text-neutral-500 hover:text-neutral-300'
                        )}
                      >
                        <input
                          className="sr-only"
                          checked={draftBindings.includes(key)}
                          type="checkbox"
                          onChange={event => {
                            setDraftBindings(bindings =>
                              event.target.checked
                                ? normalizeParameterBindings([...bindings, key])
                                : bindings.filter(binding => binding !== key)
                            )
                          }}
                        />
                        {parameterLabel(key)}
                      </label>
                    ))}
                  </div>
                </div>
              </div>
              <textarea
                className="min-h-0 flex-1 resize-none bg-black p-3 font-mono text-[12px] leading-5 text-neutral-100 outline-none"
                spellCheck={false}
                value={draftSource}
                onChange={event => setDraftSource(event.target.value)}
              />
            </>
          ) : (
            <div className="flex h-full items-center justify-center px-8 text-center text-[12px] text-neutral-600">
              Select a visualization to edit its saved React module.
            </div>
          )}
        </aside>
        ) : null}
      </section>
    </main>
  )
}

function VisualizationBlock({
  columnWidth,
  onEdit,
  onDragStart,
  onSelect,
  params,
  selected,
  visualization
}: {
  columnWidth: number
  onEdit: () => void
  onDragStart: (event: React.PointerEvent, kind: 'move' | 'resize') => void
  onSelect: () => void
  params: DashboardRuntimeParams
  selected: boolean
  visualization: DashboardVisualization
}) {
  const bindings = visualization.parameterBindings
  return (
    <section
      className={cn(
        'absolute flex flex-col overflow-hidden border bg-neutral-950 shadow-2xl shadow-black/40',
        selected ? 'border-white/70' : 'border-neutral-800 hover:border-neutral-600'
      )}
      style={{
        height: visualization.height * rowHeight,
        left: visualization.x * columnWidth,
        top: visualization.y * rowHeight,
        width: visualization.width * columnWidth
      }}
      onPointerDown={onSelect}
    >
      <div
        className="flex h-8 shrink-0 cursor-grab items-center justify-between gap-2 border-b border-neutral-800 bg-black px-2 active:cursor-grabbing"
        onPointerDown={event => {
          event.stopPropagation()
          onDragStart(event, 'move')
        }}
      >
        <div className="flex min-w-0 items-center gap-1.5">
          <Grip size={13} strokeWidth={1.8} className="text-neutral-600" />
          <span className="truncate text-[12px] font-medium text-white">{visualization.title}</span>
          <ParameterChips bindings={bindings} compact />
        </div>
        <div className="flex shrink-0 items-center gap-1.5">
          <span className="font-mono text-[10px] text-neutral-600">
            {visualization.width}c x {visualization.height}r
          </span>
          <button
            aria-label={`Edit ${visualization.title}`}
            className="inline-flex h-5 w-5 items-center justify-center border border-neutral-800 bg-black text-neutral-500 hover:text-white"
            title="Edit code"
            type="button"
            onPointerDown={event => {
              event.stopPropagation()
            }}
            onClick={event => {
              event.stopPropagation()
              onEdit()
            }}
          >
            <Code2 size={11} strokeWidth={1.8} />
          </button>
        </div>
      </div>
      <div className="min-h-0 flex-1 bg-black">
        <VisualizationFrame params={params} visualization={visualization} />
      </div>
      <button
        aria-label={`Resize ${visualization.title}`}
        className="absolute bottom-0 right-0 h-5 w-5 cursor-nwse-resize text-neutral-600 hover:text-white"
        type="button"
        onPointerDown={event => {
          event.stopPropagation()
          onDragStart(event, 'resize')
        }}
      >
        <span className="absolute bottom-1 right-1 h-2.5 w-2.5 border-b border-r border-current" />
      </button>
    </section>
  )
}

function VisualizationFrame({ params, visualization }: { params: DashboardRuntimeParams; visualization: DashboardVisualization }) {
  return (
    <iframe
      className="h-full w-full border-0 bg-black"
      sandbox="allow-scripts"
      srcDoc={visualizationSrcDoc(visualization, params)}
      tabIndex={0}
      title={visualization.title}
    />
  )
}

function ParameterChips({ bindings, compact = false }: { bindings: DashboardParameterKey[]; compact?: boolean }) {
  const active = bindings.length > 0 ? bindings : []
  if (active.length === 0) {
    return (
      <span className={cn('inline-flex shrink-0 items-center border border-neutral-900 text-neutral-600', compact ? 'h-4 px-1 text-[9px]' : 'h-5 px-1.5 text-[10px]')}>
        static
      </span>
    )
  }
  return (
    <span className="flex min-w-0 shrink-0 items-center gap-1">
      {active.map(binding => (
        <span
          key={binding}
          className={cn('inline-flex shrink-0 items-center border border-neutral-800 bg-black text-neutral-400', compact ? 'h-4 px-1 text-[9px]' : 'h-5 px-1.5 text-[10px]')}
        >
          {parameterLabel(binding)}
        </span>
      ))}
    </span>
  )
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div className="border border-neutral-800 bg-black px-2 py-1">
      <div className="text-[9px] uppercase tracking-[0.08em] text-neutral-600">{label}</div>
      <div className="font-mono text-[12px] text-neutral-300">{value}</div>
    </div>
  )
}

function visualizationSrcDoc(visualization: DashboardVisualization, params: DashboardRuntimeParams) {
  const bootstrap = `
    const sourceCode = ${JSON.stringify(visualization.sourceCode)};
    const blockId = ${JSON.stringify(visualization.id)};
    const params = ${JSON.stringify(params)};
    const pending = new Map();
    window.addEventListener('message', event => {
      const message = event.data || {};
      if (message.type !== 'nanotrace-query-result' || !message.requestId) return;
      const callbacks = pending.get(message.requestId);
      if (!callbacks) return;
      pending.delete(message.requestId);
      if (message.error) callbacks.reject(new Error(message.error));
      else callbacks.resolve(message.result);
    });
    const nanotrace = {
      query(payload) {
        const requestId = crypto.randomUUID();
        parent.postMessage({ blockId, payload, requestId, type: 'nanotrace-query' }, '*');
        return new Promise((resolve, reject) => pending.set(requestId, { resolve, reject }));
      }
    };
    function nearestVerticalScroller(target) {
      const rootElement = document.getElementById('root');
      let element = target instanceof HTMLElement ? target : target?.parentElement;
      while (element && element !== document.documentElement) {
        const style = getComputedStyle(element);
        const canScroll = element.scrollHeight > element.clientHeight + 1;
        const allowsScroll = /auto|scroll|hidden|clip/.test(style.overflowY);
        if (canScroll && allowsScroll) return element;
        if (element === rootElement) break;
        element = element.parentElement;
      }
      const scrollingElement = document.scrollingElement || document.documentElement;
      return scrollingElement.scrollHeight > scrollingElement.clientHeight + 1 ? scrollingElement : null;
    }
    window.addEventListener('wheel', event => {
      if (event.defaultPrevented || Math.abs(event.deltaY) < Math.abs(event.deltaX)) return;
      const scroller = nearestVerticalScroller(event.target);
      if (!scroller) return;
      const maxScrollTop = scroller.scrollHeight - scroller.clientHeight;
      const nextScrollTop = Math.max(0, Math.min(maxScrollTop, scroller.scrollTop + event.deltaY));
      if (nextScrollTop === scroller.scrollTop) return;
      scroller.scrollTop = nextScrollTop;
      event.preventDefault();
      event.stopPropagation();
    }, { capture: true, passive: false });
    try {
      const blob = new Blob([sourceCode], { type: 'text/javascript' });
      const url = URL.createObjectURL(blob);
      const module = await import(url);
      URL.revokeObjectURL(url);
      const Component = module.default;
      if (typeof Component !== 'function') throw new Error('Visualization module must export a default React component.');
      const root = ReactDOM.createRoot(document.getElementById('root'));
      root.render(React.createElement(
        'div',
        { className: 'nanotrace-visualization-host' },
        React.createElement(Component, { nanotrace, params })
      ));
      const enableDynamicScroll = () => {
        const rootElement = document.getElementById('root');
        if (!rootElement) return;
        for (const element of rootElement.querySelectorAll('*')) {
          if (!(element instanceof HTMLElement)) continue;
          const style = getComputedStyle(element);
          if (
            (style.overflowY === 'hidden' || style.overflowY === 'clip') &&
            element.clientHeight > 0 &&
            element.scrollHeight > element.clientHeight + 8
          ) {
            element.style.overflowY = 'auto';
            element.style.overscrollBehavior = 'contain';
            element.style.scrollbarColor = '#737373 transparent';
            element.style.scrollbarWidth = 'thin';
            if (!element.hasAttribute('tabindex')) element.tabIndex = 0;
          }
        }
      };
      requestAnimationFrame(enableDynamicScroll);
      new ResizeObserver(enableDynamicScroll).observe(document.getElementById('root'));
      new MutationObserver(() => requestAnimationFrame(enableDynamicScroll)).observe(document.getElementById('root'), {
        childList: true,
        subtree: true
      });
    } catch (error) {
      document.getElementById('root').innerHTML = '<pre>' + String(error?.stack || error?.message || error).replace(/[&<>]/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[ch])) + '</pre>';
    }
  `

  return `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <style>
      html, body, #root { height: 100%; margin: 0; background: #050505; color: #f5f5f5; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
      body { overflow: auto; overscroll-behavior: contain; scrollbar-color: #737373 transparent; scrollbar-width: thin; }
      #root { min-height: 100%; overflow: visible; }
      .nanotrace-visualization-host { min-height: 100%; height: 100%; overflow: visible; }
      * { box-sizing: border-box; }
      pre { margin: 0; padding: 12px; white-space: pre-wrap; color: #fecaca; font: 12px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace; }
    </style>
  </head>
  <body>
    <div id="root"></div>
    <script type="module">
      import React from 'https://esm.sh/react@19.2.1';
      import ReactDOM from 'https://esm.sh/react-dom@19.2.1/client';
      ${bootstrap}
    </script>
  </body>
</html>`
}

async function queryNanotrace(payload: QueryPayload) {
  const response = await fetch('/v1/query', {
    body: JSON.stringify({
      parameters: payload.parameters ?? {},
      query: payload.query
    }),
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(text || response.statusText)
  }
  return response.json()
}

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max)
}

const dashboardApi = {
  async createVisualization(apiBaseUrl: string, input: DashboardVisualizationCreate) {
    const response = await fetch(dashboardVisualizationsUrl(apiBaseUrl, input.dashboardId), {
      body: JSON.stringify(input),
      credentials: 'include',
      headers: queryHeaders(),
      method: 'POST'
    })
    return responseVisualization(response)
  },

  async listVisualizations(apiBaseUrl: string, id: string) {
    const response = await fetch(dashboardVisualizationsUrl(apiBaseUrl, id), {
      credentials: 'include',
      headers: queryHeaders(),
      method: 'GET'
    })
    if (!response.ok) {
      const text = await response.text()
      throw new Error(text || response.statusText)
    }
    const body = (await response.json()) as { visualizations?: DashboardVisualization[] }
    const visualizations = Array.isArray(body.visualizations)
      ? body.visualizations.filter(isDashboardVisualization).map(normalizeDashboardVisualization)
      : []
    return visualizations
  },

  async updateVisualization(apiBaseUrl: string, item: DashboardVisualization) {
    const response = await fetch(`${dashboardVisualizationsUrl(apiBaseUrl, item.dashboardId)}/${encodeURIComponent(item.id)}`, {
      body: JSON.stringify(item),
      credentials: 'include',
      headers: queryHeaders(),
      method: 'PUT'
    })
    return responseVisualization(response)
  },

  async deleteVisualization(apiBaseUrl: string, item: DashboardVisualization) {
    const response = await fetch(`${dashboardVisualizationsUrl(apiBaseUrl, item.dashboardId)}/${encodeURIComponent(item.id)}`, {
      credentials: 'include',
      headers: queryHeaders(),
      method: 'DELETE'
    })
    return responseVisualization(response)
  }
}

type DashboardVisualizationCreate = Omit<DashboardVisualization, 'updatedAt'>

async function responseVisualization(response: Response) {
  if (!response.ok) {
    const text = await response.text()
    throw new Error(text || response.statusText)
  }
  const body = (await response.json()) as { visualization?: DashboardVisualization }
  if (!isDashboardVisualization(body.visualization)) {
    throw new Error('dashboard visualization response missing visualization')
  }
  return normalizeDashboardVisualization(body.visualization)
}

function dashboardVisualizationsUrl(apiBaseUrl: string, id: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  const path = `/dashboards/${encodeURIComponent(id)}/visualizations`
  return base ? `${base}${path}` : path
}

function isDashboardVisualization(value: unknown): value is DashboardVisualization {
  if (!value || typeof value !== 'object') return false
  const item = value as DashboardVisualization
  return (
    typeof item.dashboardId === 'string' &&
    typeof item.height === 'number' &&
    typeof item.id === 'string' &&
    (item.parameterBindings === undefined ||
      (Array.isArray(item.parameterBindings) && item.parameterBindings.every(isDashboardParameterKey))) &&
    typeof item.sourceCode === 'string' &&
    typeof item.title === 'string' &&
    typeof item.updatedAt === 'string' &&
    typeof item.width === 'number' &&
    typeof item.x === 'number' &&
    typeof item.y === 'number'
  )
}

function normalizeDashboardVisualization(item: DashboardVisualization): DashboardVisualization {
  return {
    ...item,
    parameterBindings: normalizeParameterBindings(item.parameterBindings ?? [])
  }
}

function normalizeParameterBindings(bindings: string[]): DashboardParameterKey[] {
  const normalized: DashboardParameterKey[] = []
  for (const binding of bindings) {
    if (!isDashboardParameterKey(binding) || normalized.includes(binding)) continue
    normalized.push(binding)
  }
  return normalized
}

function isDashboardParameterKey(value: unknown): value is DashboardParameterKey {
  return value === 'timeRange' || value === 'filter' || value === 'groupBy'
}

function parameterLabel(key: DashboardParameterKey) {
  switch (key) {
    case 'timeRange':
      return 'time'
    case 'filter':
      return 'filter'
    case 'groupBy':
      return 'group'
  }
}

function parameterUsageCounts(visualizations: DashboardVisualization[]) {
  const counts: Record<DashboardParameterKey, number> = { filter: 0, groupBy: 0, timeRange: 0 }
  for (const visualization of visualizations) {
    for (const binding of visualization.parameterBindings) {
      counts[binding] += 1
    }
  }
  return counts
}

function buildDashboardParams({
  eventFilter,
  groupBy,
  timeRange
}: {
  eventFilter: ParsedEventFilter
  groupBy: string
  timeRange: ResolvedTimeRange
}): DashboardRuntimeParams {
  return compileDashboardParams({ eventFilter, groupBy, timeRange })
}

function pickDashboardParams(global: DashboardRuntimeParams, bindings: DashboardParameterKey[]): DashboardRuntimeParams {
  return compileDashboardParams({
    eventFilter: bindings.includes('filter') ? global.filter as ParsedEventFilter | undefined : undefined,
    groupBy: bindings.includes('groupBy') ? global.groupBy as string | undefined : undefined,
    timeRange: bindings.includes('timeRange') ? global.timeRange as ResolvedTimeRange | undefined : undefined
  })
}

function compileDashboardParams({
  eventFilter,
  groupBy,
  timeRange
}: {
  eventFilter?: ParsedEventFilter
  groupBy?: string
  timeRange?: ResolvedTimeRange
}): DashboardRuntimeParams {
  const parameters: Record<string, unknown> = {}
  const where = ["ifNull(toString(getSubcolumn(data, '_loadtest.fixture')), '') = ''"]

  if (timeRange) {
    Object.assign(parameters, timeRangeParameters(timeRange))
    const timeClause = timeRangeWhereClause(timeRange)
    if (timeClause) where.push(timeClause)
  }

  if (eventFilter) {
    Object.assign(parameters, eventFilterParameters(eventFilter))
    for (const clause of eventFilterWhereClauses(eventFilter)) {
      where.push(clause)
    }
  }

  const groupByExpression = groupBy ? eventValueExpression(groupBy) : undefined
  const params: DashboardRuntimeParams = {
    sql: {
      ...(groupByExpression ? { groupByExpression, groupByLabel: displayFacetPath(groupBy!) } : {}),
      parameters,
      where: where.join(' AND ')
    }
  }
  if (timeRange) params.timeRange = timeRange
  if (eventFilter) params.filter = eventFilter
  if (groupBy) params.groupBy = groupBy
  return params
}

function resolveTimeRange({
  customEnd,
  customStart,
  key
}: {
  customEnd: string
  customStart: string
  key: TimeRangeKey
}): ResolvedTimeRange {
  if (key === 'custom') {
    const createdAfter = dateTimeLocalInputToIso(customStart)
    const createdBefore = dateTimeLocalInputToIso(customEnd)
    return { createdAfter, createdBefore, key: `custom:${createdAfter}:${createdBefore}` }
  }
  const option = timeRangeOptions.find(item => item.key === key) ?? timeRangeOptions.find(item => item.key === '24h')!
  const createdBefore = new Date()
  return {
    createdAfter: new Date(createdBefore.getTime() - option.minutes * 60 * 1000).toISOString(),
    createdBefore: createdBefore.toISOString(),
    key: option.key,
    lookbackMinutes: option.minutes
  }
}

function timeRangeWhereClause(range: ResolvedTimeRange, column = 'timestamp') {
  if (range.lookbackMinutes) {
    return `${column} >= now64(3) - toIntervalMinute({lookback_minutes:UInt64})`
  }
  return [
    range.createdAfter ? `${column} >= {created_after:DateTime64(3, 'UTC')}` : '',
    range.createdBefore ? `${column} <= {created_before:DateTime64(3, 'UTC')}` : ''
  ].filter(Boolean).join(' AND ')
}

function timeRangeParameters(range: ResolvedTimeRange): Record<string, unknown> {
  return {
    ...(range.lookbackMinutes ? { lookback_minutes: range.lookbackMinutes } : {}),
    ...(range.createdAfter ? { created_after: clickHouseDateTime64(range.createdAfter) } : {}),
    ...(range.createdBefore ? { created_before: clickHouseDateTime64(range.createdBefore) } : {})
  }
}

function eventFilterWhereClauses(filter: ParsedEventFilter) {
  return [
    filter.createdAfter ? 'timestamp >= {filter_created_after:DateTime64(3, \'UTC\')}' : '',
    filter.createdBefore ? 'timestamp <= {filter_created_before:DateTime64(3, \'UTC\')}' : '',
    ...(filter.facets ?? []).map((facet, index) => `${eventValueExpression(facet.path)} = {facet_filter_${index}_value:String}`),
    filter.text
      ? "(positionCaseInsensitive(toJSONString(data), {event_filter:String}) > 0 OR positionCaseInsensitive(event_id, {event_filter:String}) > 0)"
      : ''
  ].filter(Boolean)
}

function eventFilterParameters(filter: ParsedEventFilter): Record<string, unknown> {
  return {
    ...(filter.createdAfter ? { filter_created_after: clickHouseDateTime64(filter.createdAfter) } : {}),
    ...(filter.createdBefore ? { filter_created_before: clickHouseDateTime64(filter.createdBefore) } : {}),
    ...(filter.text ? { event_filter: filter.text } : {}),
    ...Object.fromEntries((filter.facets ?? []).map((facet, index) => [`facet_filter_${index}_value`, facet.value]))
  }
}

function eventValueExpression(path: string) {
  const column = promotedStringColumn(path)
  if (column) return `ifNull(toString(nullIf(${column}, '')), '')`
  return `ifNull(toString(${jsonFieldExpression(path)}), '')`
}

function promotedStringColumn(path: string) {
  switch (nanotracePath(path)) {
    case 'tenant_id':
      return 'tenant_id'
    case 'trace_id':
      return 'trace_id'
    case 'span_id':
      return 'span_id'
    case 'event_type':
      return 'event_type'
    case 'signal':
      return 'signal'
    default:
      return ''
  }
}

function jsonFieldExpression(path: string) {
  const normalized = nanotracePath(path)
  if (!/^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$/.test(normalized)) {
    throw new Error(`Unsupported field path: ${path}`)
  }
  return `data.${normalized}`
}

function nanotracePath(path: string) {
  switch (path) {
    case 'traceId':
      return 'trace_id'
    case 'spanId':
      return 'span_id'
    case 'parentSpanId':
      return 'parent_span_id'
    default:
      return normalizedPayloadPath(path)
  }
}

function displayFacetPath(path: string) {
  switch (path) {
    case 'trace_id':
      return 'traceId'
    case 'span_id':
      return 'spanId'
    case 'parent_span_id':
      return 'parentSpanId'
    default:
      return path
  }
}

function normalizedPayloadPath(path: string) {
  return path.trim().replace(/-/g, '.')
}

function parseEventFilter(value: string): ParsedEventFilter {
  const filter: ParsedEventFilter = { text: '' }
  const withoutTimestamps = value.replace(
    /(?:^|\s)(?:createdAt|timestamp)\s*(>=|>|<=|<)\s*("[^"]+"|'[^']+'|\S+)/gi,
    (match: string, operator: string, rawTimestamp: string) => {
      const timestamp = normalizeFilterTimestamp(rawTimestamp)
      if (!timestamp) return match
      if (operator === '>' || operator === '>=') filter.createdAfter = timestamp
      else filter.createdBefore = timestamp
      return ' '
    }
  )
  const facets: ParsedFacetFilter[] = []
  const text = withoutTimestamps.replace(
    /(?:^|\s)([A-Za-z_][A-Za-z0-9_.-]*)\s*=\s*("[^"]*"|'[^']*'|\S+)/g,
    (match: string, rawPath: string, rawValue: string) => {
      const path = normalizedPayloadPath(rawPath)
      const parsedValue = unquoteFilterValue(rawValue)
      if (!isSupportedFacetFilterPath(path) || !parsedValue) return match
      facets.push({ path: displayFacetPath(path), value: parsedValue })
      return ' '
    }
  )
  if (facets.length > 0) filter.facets = facets
  filter.text = trimBooleanOperators(text.trim().split(/\s+/).filter(Boolean)).join(' ')
  return filter
}

function isSupportedFacetFilterPath(path: string) {
  return /^[A-Za-z_][A-Za-z0-9_]*(?:[.-][A-Za-z0-9_]+)*$/.test(path)
}

function hasAppliedEventFilter(filter: ParsedEventFilter) {
  return filter.text !== '' || Boolean(filter.createdAfter) || Boolean(filter.createdBefore) || Boolean(filter.facets?.length)
}

function unquoteFilterValue(value: string) {
  return value.trim().replace(/^"([^"]*)"$/, '$1').replace(/^'([^']*)'$/, '$1')
}

function trimBooleanOperators(tokens: string[]) {
  while (tokens.length > 0 && /^and$/i.test(tokens[0]!)) tokens.shift()
  while (tokens.length > 0 && /^(and|or)$/i.test(tokens[tokens.length - 1] ?? '')) tokens.pop()
  return tokens.filter((token, index) => !/^(and|or)$/i.test(token) || !/^(and|or)$/i.test(tokens[index - 1] ?? ''))
}

function normalizeFilterTimestamp(value: string) {
  value = value.trim().replace(/^["']|["']$/g, '')
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return `${value}T00:00:00Z`
  const time = Date.parse(value)
  return Number.isFinite(time) ? new Date(time).toISOString() : ''
}

function dateTimeLocalInputToIso(value: string) {
  if (!value) return ''
  const date = new Date(value)
  return Number.isFinite(date.getTime()) ? date.toISOString() : ''
}

function formatDateTimeLocalInput(date: Date) {
  const pad = (value: number) => String(value).padStart(2, '0')
  return [
    date.getFullYear(),
    '-',
    pad(date.getMonth() + 1),
    '-',
    pad(date.getDate()),
    'T',
    pad(date.getHours()),
    ':',
    pad(date.getMinutes())
  ].join('')
}

function clickHouseDateTime64(value: string) {
  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) return value
  return [
    date.getUTCFullYear(),
    '-',
    pad2(date.getUTCMonth() + 1),
    '-',
    pad2(date.getUTCDate()),
    ' ',
    pad2(date.getUTCHours()),
    ':',
    pad2(date.getUTCMinutes()),
    ':',
    pad2(date.getUTCSeconds()),
    '.',
    String(date.getUTCMilliseconds()).padStart(3, '0')
  ].join('')
}

function pad2(value: number) {
  return String(value).padStart(2, '0')
}

function delay<T>(value: T) {
  return new Promise<T>(resolve => window.setTimeout(() => resolve(value), 80))
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : error ? String(error) : ''
}
