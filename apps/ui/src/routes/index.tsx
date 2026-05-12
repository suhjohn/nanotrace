import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { keepPreviousData, useInfiniteQuery, useQuery } from '@tanstack/react-query'
import type { InfiniteData } from '@tanstack/react-query'
import { useVirtualizer } from '@tanstack/react-virtual'
import { Columns3, PanelLeftClose, PanelLeftOpen, X } from 'lucide-react'
import { useEffect, useMemo, useRef, useState } from 'react'
import type { JsonObject, JsonValue } from '../lib/json'
import { clamp, useCookieState, useIndexedDbState } from '../lib/hooks'
import { cn } from '../lib/cn'

export const Route = createFileRoute('/')({
  validateSearch: parseObservatorySearch,
  component: IndexRoute
})

function IndexRoute() {
  const search = Route.useSearch()
  return <ObservatoryHome eventFilterSearchText={search.filter} selectedEventId={search.eventId ?? ''} />
}

type LogGroupSummary = {
  groupBy: string
  value: string
  fields?: JsonObject
  startedAt?: string
  endedAt?: string
  durationMs?: number
  count: number
  errorCount?: number
}

type TraceEvent = {
  id: string
  createdAt: string
  data: JsonObject
}

type TraceDetail = {
  fields: LogField[]
  group: LogGroupSummary
  events: TraceEvent[]
  relatedEvents: TraceEvent[]
}

type LogGroupDetail = {
  fields?: LogField[]
  group: LogGroupSummary
  logs: TraceEvent[]
}

type LogEventsPage = {
  anchorIndex?: number
  events: TraceEvent[]
  fields: LogField[]
  group?: LogGroupSummary
  nextCursor?: string
  prevCursor?: string
}

type EventPageParam = {
  after?: string
  around?: string
  before?: string
}

type LogEventPayload = {
  event: TraceEvent
}

type LogField = {
  count: number
  path: string
  types: string[]
}

type LogLatest = {
  lastCreatedAt: string
}

type LogSummary = {
  capped: boolean
  count: number
  limit: number
}

type DensityBucket = {
  count: number
  errorCount?: number
  start: string
}

type LogDensity = {
  bucketMs: number
  buckets: DensityBucket[]
  from: string
  to: string
}

type LogFlamegraph = Flamegraph & {
  capped?: boolean
  spanCount?: number
}

type GroupOption = {
  cardinality: number
  capped: boolean
  path: string
}

export type RouteSelection = {
  field: string
  value: string
}

export type ObservatorySearch = {
  eventId?: string
  filter?: string
}

type FlameKind = 'event' | 'run' | 'turn' | 'model' | 'tool'
type GraphMode = 'flamegraph' | 'histogram'
type TimeRangeKey = '15m' | '1h' | '6h' | '24h' | '7d' | 'custom'

type FlameSpan = {
  eventIds: string[]
  id: string
  label: string
  kind: FlameKind
  parentSpanId?: string
  startMs: number
  endMs: number
  lane: number
  payload: JsonObject
}

type Flamegraph = {
  eventCreatedAt: Record<string, string>
  eventSpanIds: Record<string, string>
  minStart: number
  maxEnd: number
  totalDuration: number
  rows: FlameSpan[][]
}

type JsonTreeNode =
  | {
      name: string
      type: 'null'
      value: null
    }
  | {
      name: string
      type: 'boolean'
      value: boolean
    }
  | {
      name: string
      type: 'number'
      value: number
    }
  | {
      name: string
      type: 'string'
      value: string
    }
  | {
      entries: JsonTreeNode[]
      name: string
      size: number
      type: 'object'
    }
  | {
      items: JsonTreeNode[]
      length: number
      name: string
      type: 'array'
    }

const panelClass =
  'flex min-h-0 flex-col bg-neutral-950'
const eventMarkerWidth = 5
const defaultEventColumns: string[] = ['timestamp', 'name', 'traceId', 'data']
const timeRangeOptions: { key: Exclude<TimeRangeKey, 'custom'>; label: string; minutes: number }[] = [
  { key: '15m', label: '15m', minutes: 15 },
  { key: '1h', label: '1h', minutes: 60 },
  { key: '6h', label: '6h', minutes: 6 * 60 },
  { key: '24h', label: '24h', minutes: 24 * 60 },
  { key: '7d', label: '7d', minutes: 7 * 24 * 60 }
]

export function parseObservatorySearch(search: Record<string, unknown>): ObservatorySearch {
  const parsed: ObservatorySearch = {}
  if (typeof search.eventId === 'string' && search.eventId) parsed.eventId = search.eventId
  if ('filter' in search && typeof search.filter === 'string') parsed.filter = search.filter
  return parsed
}

function parseStringArray(value: string) {
  const parsed = JSON.parse(value)
  return Array.isArray(parsed) ? parsed.filter((item): item is string => typeof item === 'string') : [...defaultEventColumns]
}

function parseTimeRangeKey(value: string): TimeRangeKey {
  return value === 'custom' || timeRangeOptions.some(option => option.key === value) ? value as TimeRangeKey : '24h'
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

function dateTimeLocalInputToIso(value: string) {
  const time = Date.parse(value)
  return Number.isFinite(time) ? new Date(time).toISOString() : undefined
}

function getJsonValueType(value: JsonValue) {
  if (value === null) return 'null'
  if (Array.isArray(value)) return 'array'
  return typeof value
}

export function ObservatoryHome({
  eventFilterSearchText,
  routeSelection,
  selectedEventId
}: {
  eventFilterSearchText?: string
  routeSelection?: RouteSelection
  selectedEventId: string
}) {
  const observatoryUrl = import.meta.env.VITE_NANOTRACE_URL || ''
  const navigate = useNavigate()
  const [runsWidth, setRunsWidth] = useCookieState({
    cookieName: 'observatory-ui-runs-width',
    initialValue: 320
  })
  const [runsOpen, setRunsOpen] = useCookieState({
    cookieName: 'observatory-ui-runs-open',
    initialValue: true
  })
  const [inspectorWidth, setInspectorWidth] = useCookieState({
    cookieName: 'observatory-ui-inspector-width',
    initialValue: 420
  })
  const [flamegraphHeight, setFlamegraphHeight] = useCookieState({
    cookieName: 'observatory-ui-flamegraph-height',
    initialValue: 260
  })
  const [selectedGraphMode, setSelectedGraphMode] = useCookieState<GraphMode>({
    cookieName: 'observatory-ui-graph-mode',
    initialValue: 'flamegraph'
  })
  const [timeRangeKey, setTimeRangeKey] = useCookieState<TimeRangeKey>({
    cookieName: 'observatory-ui-time-range',
    initialValue: '24h',
    parse: parseTimeRangeKey
  })
  const [customRangeStart, setCustomRangeStart] = useCookieState<string>({
    cookieName: 'observatory-ui-custom-range-start',
    initialValue: () => formatDateTimeLocalInput(new Date(Date.now() - 60 * 60 * 1000)),
    parse: String
  })
  const [customRangeEnd, setCustomRangeEnd] = useCookieState<string>({
    cookieName: 'observatory-ui-custom-range-end',
    initialValue: () => formatDateTimeLocalInput(new Date()),
    parse: String
  })
  const centerRef = useRef<HTMLElement | null>(null)
  const [dragging, setDragging] = useState<null | 'runs' | 'inspector' | 'flamegraph'>(null)
  const [manualGroupBy, setManualGroupBy] = useState('')
  const [highlightedEventIds, setHighlightedEventIds] = useState<string[]>([])
  const [selectedCanvasSpanId, setSelectedCanvasSpanId] = useState('')
  const [filter, setFilter] = useState('')
  const [eventFilterDraft, setEventFilterDraft] = useState('')
  const [eventFilterGroupKey, setEventFilterGroupKey] = useState('')
  const [eventFilterParams, setEventFilterParams] = useState<ParsedEventFilter>({ text: '' })
  const [eventAnchorOverride, setEventAnchorOverride] = useState<{ key: string; timestamp: string } | null>(null)
  const [inspectorQuery, setInspectorQuery] = useState('')

  const [selectedEventColumns, setSelectedEventColumns] = useIndexedDbState<string[]>({
    initialValue: defaultEventColumns,
    key: 'observatory-ui-event-columns',
    parse: parseStringArray
  })
  const filterTouchedRef = useRef(false)
  const seededLatestGroupKeyRef = useRef('')
  const previousGroupKeyRef = useRef('')
  const workspaceRef = useRef<HTMLElement | null>(null)
  const arrowKeyScopeRef = useRef<'events' | 'local'>('events')
  const groupOptionsQuery = useQuery({
    queryKey: ['logs', observatoryUrl, 'group-options'],
    queryFn: () => fetchGroupOptions({ apiBaseUrl: observatoryUrl, limit: 120 })
  })
  const groupOptions = groupOptionsQuery.data?.fields ?? []
  const defaultGroupBy = groupOptions.find(option => option.path === 'traceId')?.path || groupOptions[0]?.path || ''
  const manualGroupByValid = groupOptions.some(option => option.path === manualGroupBy)
  const groupBy = routeSelection?.field || (manualGroupByValid ? manualGroupBy : defaultGroupBy)
  const displayedGroupOptions = useMemo(
    () =>
      groupBy && !groupOptions.some(option => option.path === groupBy)
        ? [{ cardinality: 0, capped: false, path: groupBy }, ...groupOptions]
        : groupOptions,
    [groupBy, groupOptions]
  )
  const selectedGroupValue = routeSelection?.field === groupBy ? routeSelection.value : ''
  const selectedTimeRange = useMemo(
    () =>
      resolveTimeRange({
        customEnd: customRangeEnd,
        customStart: customRangeStart,
        key: timeRangeKey
      }),
    [customRangeEnd, customRangeStart, timeRangeKey]
  )
  const selectedGroupKey = groupBy && selectedGroupValue ? `${groupBy}\u0000${selectedGroupValue}\u0000${selectedTimeRange.key}` : ''
  const eventFilterReady = Boolean(selectedGroupKey) && eventFilterGroupKey === selectedGroupKey
  const hasEventQuery = eventFilterReady
  const groupsQuery = useQuery({
    enabled: groupOptions.length > 0 && Boolean(groupBy),
    queryKey: ['logs', observatoryUrl, 'groups', groupBy, selectedTimeRange.key],
    queryFn: () => fetchGroups({ apiBaseUrl: observatoryUrl, groupBy, limit: 120, timeRange: selectedTimeRange })
  })
  const traceList = groupsQuery.data?.groups ?? []
  const selectedGroupSummary =
    traceList.find(trace => trace.groupBy === groupBy && trace.value === selectedGroupValue) ?? null
  const needsLatest = Boolean(groupBy && selectedGroupValue && !selectedGroupSummary?.endedAt)
  const latestQuery = useQuery({
    enabled: needsLatest,
    queryKey: ['logs', observatoryUrl, 'latest', groupBy, selectedGroupValue],
    queryFn: () => fetchLatest({ apiBaseUrl: observatoryUrl, groupBy, selectedGroupValue }),
    retry: false
  })
  const latestCreatedAt = selectedGroupSummary?.endedAt || latestQuery.data?.lastCreatedAt
  const eventDataKey = [
    selectedGroupKey,
    eventFilterParams.text,
    eventFilterParams.createdAfter ?? '',
    eventFilterParams.createdBefore ?? ''
  ].join('\u0000')
  const eventAnchorTimestamp =
    eventAnchorOverride?.key === eventDataKey
      ? eventAnchorOverride.timestamp
      : eventFilterParams.createdBefore || latestCreatedAt || eventFilterParams.createdAfter || ''
  const summaryQuery = useQuery({
    enabled: Boolean(groupBy && selectedGroupValue && hasEventQuery),
    queryKey: ['logs', observatoryUrl, 'summary', groupBy, selectedGroupValue, eventFilterParams],
    queryFn: () =>
      fetchSummary({ apiBaseUrl: observatoryUrl, eventFilter: eventFilterParams, groupBy, selectedGroupValue }),
    retry: false
  })
  const flamegraphDisabledBySummary = Boolean(summaryQuery.data?.capped)
  const graphModeBeforeFlamegraph = flamegraphDisabledBySummary ? 'histogram' : selectedGraphMode
  const eventsQuery = useInfiniteQuery<LogEventsPage, Error, InfiniteData<LogEventsPage>, (string | ParsedEventFilter)[], EventPageParam>({
    enabled: Boolean(groupBy && selectedGroupValue && hasEventQuery),
    queryKey: ['logs', observatoryUrl, 'events', groupBy, selectedGroupValue, eventFilterParams, eventAnchorTimestamp],
    initialPageParam: { around: eventAnchorTimestamp } as EventPageParam,
    queryFn: ({ pageParam }) =>
      fetchEvents({
        apiBaseUrl: observatoryUrl,
        eventFilter: eventFilterParams,
        groupBy,
        limit: 500,
        pageParam,
        selectedGroupValue
      }),
    getNextPageParam: lastPage => lastPage.nextCursor ? { after: lastPage.nextCursor } : undefined,
    getPreviousPageParam: firstPage => firstPage.prevCursor ? { before: firstPage.prevCursor } : undefined,
    placeholderData: keepPreviousData,
    retry: false
  })
  const flamegraphQuery = useQuery({
    enabled: Boolean(groupBy && selectedGroupValue && hasEventQuery && summaryQuery.data && graphModeBeforeFlamegraph === 'flamegraph'),
    queryKey: ['logs', observatoryUrl, 'flamegraph', groupBy, selectedGroupValue, eventFilterParams],
    queryFn: () =>
      fetchFlamegraph({
        apiBaseUrl: observatoryUrl,
        eventFilter: eventFilterParams,
        groupBy,
        maxSpans: 200_000,
        selectedGroupValue
      }),
    retry: false
  })
  const flamegraphDisabled = flamegraphDisabledBySummary || Boolean(flamegraphQuery.data?.capped)
  const graphMode = flamegraphDisabled ? 'histogram' : selectedGraphMode
  const densityQuery = useQuery({
    enabled: Boolean(groupBy && selectedGroupValue && hasEventQuery && summaryQuery.data && graphMode === 'histogram'),
    queryKey: ['logs', observatoryUrl, 'density', groupBy, selectedGroupValue, eventFilterParams],
    queryFn: () =>
      fetchDensity({
        apiBaseUrl: observatoryUrl,
        buckets: 700,
        eventFilter: eventFilterParams,
        groupBy,
        selectedGroupValue
      }),
    retry: false
  })
  const eventPages = eventsQuery.data?.pages ?? []
  const allEvents = useMemo(
    () => eventPages.flatMap(page => page.events),
    [eventPages]
  )
  const traceDetail = eventPages[0]?.group
    ? {
        fields: mergeLogFields(eventPages.flatMap(page => page.fields)),
        group: eventPages[0].group,
        events: allEvents,
        relatedEvents: []
      }
    : null
  const flamegraph = flamegraphQuery.data ?? emptyFlamegraph
  const listError = errorMessage(groupOptionsQuery.error) || errorMessage(groupsQuery.error)
  const traceError =
    (!hasEventQuery && needsLatest ? errorMessage(latestQuery.error) : '') ||
    errorMessage(summaryQuery.error) ||
    errorMessage(eventsQuery.error) ||
    (graphMode === 'histogram' ? errorMessage(densityQuery.error) : errorMessage(flamegraphQuery.error))
  const loadingList = groupOptionsQuery.isPending || (Boolean(groupBy) && groupsQuery.isPending)
  const emptyObservatory = !loadingList && !listError && groupOptions.length === 0
  const emptyGroup = !loadingList && !listError && groupOptions.length > 0 && traceList.length === 0
  const waitingForLatest = Boolean(needsLatest && latestQuery.isPending && !hasEventQuery)
  const waitingForSummary = Boolean(selectedGroupValue && hasEventQuery && summaryQuery.isPending)
  const loadingGraph =
    graphMode === 'histogram'
      ? densityQuery.isPending || densityQuery.isFetching
      : flamegraphQuery.isPending || flamegraphQuery.isFetching
  const loadingDetail = Boolean(selectedGroupValue) && (waitingForLatest || waitingForSummary || loadingGraph)
  const loadingTableDetail = Boolean(selectedGroupValue) && (eventsQuery.isPending || eventsQuery.isFetching)
  const loadingAnchoredEvents = Boolean(
    eventAnchorOverride?.key === eventDataKey &&
    eventsQuery.isFetching &&
    !eventsQuery.isFetchingNextPage &&
    !eventsQuery.isFetchingPreviousPage
  )
  const draftEventFilterParams = useMemo(
    () => parseEventFilter({ referenceTimestamp: traceDetail?.group.startedAt ?? latestCreatedAt, value: eventFilterDraft }),
    [eventFilterDraft, latestCreatedAt, traceDetail?.group.startedAt]
  )
  const eventFilterDirty =
    draftEventFilterParams.text !== eventFilterParams.text ||
    draftEventFilterParams.createdAfter !== eventFilterParams.createdAfter ||
    draftEventFilterParams.createdBefore !== eventFilterParams.createdBefore
  const hasEventFilter =
    eventFilterParams.text !== '' ||
    Boolean(eventFilterParams.createdAfter) ||
    Boolean(eventFilterParams.createdBefore) ||
    eventFilterDraft !== ''

  function setFilterSearch(value: string) {
    void navigate({
      search: (current: ObservatorySearch) => ({
        ...current,
        filter: value
      })
    } as never)
  }

  function applyEventFilter() {
    filterTouchedRef.current = true
    const appliedFilter = eventFilterText(draftEventFilterParams)
    setEventFilterGroupKey(selectedGroupKey)
    setEventFilterDraft(appliedFilter)
    setEventFilterParams(draftEventFilterParams)
    setFilterSearch(appliedFilter)
  }

  function clearEventFilter() {
    filterTouchedRef.current = true
    setEventFilterGroupKey(selectedGroupKey)
    setEventFilterDraft('')
    setEventFilterParams({ text: '' })
    setFilterSearch('')
  }

  function applyTimeRange({ createdAfter, createdBefore }: { createdAfter: string; createdBefore: string }) {
    filterTouchedRef.current = true
    const nextFilter = {
      ...eventFilterParams,
      createdAfter,
      createdBefore
    }
    setEventFilterGroupKey(selectedGroupKey)
    setEventFilterDraft(eventFilterText(nextFilter))
    setEventFilterParams(nextFilter)
    setFilterSearch(eventFilterText(nextFilter))
  }

  useEffect(() => {
    if (previousGroupKeyRef.current === selectedGroupKey) {
      return
    }

    previousGroupKeyRef.current = selectedGroupKey
    seededLatestGroupKeyRef.current = ''
    filterTouchedRef.current = false
    setEventFilterGroupKey('')
    setEventFilterDraft('')
    setEventFilterParams({ text: '' })
  }, [selectedGroupKey])

  useEffect(() => {
    if (!selectedGroupKey || eventFilterSearchText === undefined) {
      return
    }

    filterTouchedRef.current = true
    seededLatestGroupKeyRef.current = selectedGroupKey
    setEventFilterGroupKey(selectedGroupKey)
    setEventFilterDraft(eventFilterSearchText)
    setEventFilterParams(parseEventFilter({
      referenceTimestamp: traceDetail?.group.startedAt ?? latestCreatedAt,
      value: eventFilterSearchText
    }))
  }, [eventFilterSearchText, latestCreatedAt, selectedGroupKey, traceDetail?.group.startedAt])

  useEffect(() => {
    if (flamegraphDisabled && selectedGraphMode !== 'histogram') {
      setSelectedGraphMode('histogram')
    }
  }, [flamegraphDisabled, selectedGraphMode, setSelectedGraphMode])

  useEffect(() => {
    const defaultFilter = defaultTimeRangeFilter({
      lastCreatedAt: latestCreatedAt,
      timeRange: selectedTimeRange
    })
    if (
      eventFilterSearchText !== undefined ||
      !selectedGroupKey ||
      !defaultFilter ||
      filterTouchedRef.current ||
      seededLatestGroupKeyRef.current === selectedGroupKey
    ) {
      return
    }

    seededLatestGroupKeyRef.current = selectedGroupKey
    const defaultFilterText = eventFilterText(defaultFilter)
    setEventFilterGroupKey(selectedGroupKey)
    setEventFilterDraft(defaultFilterText)
    setEventFilterParams(defaultFilter)
    setFilterSearch(defaultFilterText)
  }, [eventFilterSearchText, latestCreatedAt, selectedGroupKey, selectedTimeRange])

  const filteredTraces = useMemo(() => {
    const query = filter.trim().toLowerCase()
    if (!query) {
      return traceList
    }

    return traceList.filter(trace =>
      [trace.value, trace.groupBy, ...Object.values(trace.fields ?? {}).map(String)]
        .filter((value): value is string => Boolean(value))
        .some(value => value.toLowerCase().includes(query))
      )
  }, [filter, traceList])
  const emptyFilter = !loadingList && !listError && traceList.length > 0 && filteredTraces.length === 0

  const eventTableScrollKey = useMemo(
    () =>
      [
        'observatory-ui-events-scroll',
        `/${encodeURIComponent(groupBy)}/${encodeURIComponent(selectedGroupValue)}`,
        eventFilterParams.text,
        eventFilterParams.createdAfter ?? '',
        eventFilterParams.createdBefore ?? ''
      ].join('\u0000'),
    [eventFilterParams.createdAfter, eventFilterParams.createdBefore, eventFilterParams.text, groupBy, selectedGroupValue]
  )
  const selectedEventColumnsForTrace = useMemo(() => {
    if (!traceDetail) return selectedEventColumns
    const available = new Set(['timestamp', 'data', ...traceDetail.fields.map(field => field.path)])
    const kept = selectedEventColumns.filter(path => available.has(path))
    return kept.length > 0 ? kept : [...defaultEventColumns].filter(path => available.has(path))
  }, [selectedEventColumns, traceDetail])
  const selectedEvent = allEvents.find(event => event.id === selectedEventId) ?? null
  const pendingAnchoredEvent = Boolean(
    selectedEventId &&
    !selectedEvent &&
    eventAnchorOverride?.key === eventDataKey &&
    eventsQuery.isFetching
  )
  const eventPayloadQuery = useQuery({
    enabled: Boolean(selectedEventId && !pendingAnchoredEvent),
    queryKey: ['logs', observatoryUrl, 'event', selectedEventId],
    queryFn: () => fetchEvent({ apiBaseUrl: observatoryUrl, eventId: selectedEventId }),
    retry: false
  })
  const inspectedEvent = eventPayloadQuery.data?.event ?? (eventPayloadQuery.isFetching ? null : selectedEvent)
  const inspectorPayload = inspectedEvent
    ? {
        title: `${String(inspectedEvent.data.type || 'event')} event`,
        value: inspectedEvent as unknown as JsonValue
      }
    : null
  const inspectorFilter = inspectorQuery.trim().toLowerCase()
  const filteredPayloadNode = useMemo(
    () =>
      inspectorPayload
        ? buildFilteredJsonTree({
            filter: inspectorFilter,
            isRoot: true,
            name: inspectorPayload.title,
            value: inspectorPayload.value
          })
        : null,
    [inspectorFilter, inspectorPayload]
  )
  const hasInspectorFilter = inspectorFilter !== ''

  function setEventSearch(eventId: string) {
    void navigate({
      search: (current: ObservatorySearch) => ({
        ...current,
        eventId: eventId || undefined
      })
    } as never)
  }

  function selectEvent(event: TraceEvent) {
    setEventSearch(event.id)
    setHighlightedEventIds([event.id])
    setSelectedCanvasSpanId(flamegraph.eventSpanIds[event.id] ?? event.id)
  }

  function inspectSpan(span: FlameSpan) {
    const nextHighlightedEventIds = span.kind === 'event' ? [span.id] : span.eventIds
    const nextEventId = nextHighlightedEventIds[0] ?? ''
    setEventSearch(nextEventId)
    setHighlightedEventIds(nextHighlightedEventIds)
    setSelectedCanvasSpanId(span.id)
    const anchorTimestamp = flamegraph.eventCreatedAt[nextEventId] || (Number.isFinite(span.startMs) ? new Date(span.startMs).toISOString() : '')
    if (nextEventId && anchorTimestamp) {
      setEventAnchorOverride({ key: eventDataKey, timestamp: anchorTimestamp })
    } else {
      setEventAnchorOverride(null)
    }
  }

  useEffect(() => {
    if (!selectedEventId) {
      setHighlightedEventIds([])
      setSelectedCanvasSpanId('')
      return
    }

    if (!selectedEvent) {
      if (pendingAnchoredEvent) {
        return
      }
      setHighlightedEventIds([])
      if (eventPayloadQuery.error instanceof HTTPError && eventPayloadQuery.error.status === 404) {
        setEventSearch('')
      }
      return
    }

    setHighlightedEventIds([selectedEventId])
    setSelectedCanvasSpanId(flamegraph.eventSpanIds[selectedEventId] ?? selectedEventId)
  }, [eventPayloadQuery.error, flamegraph.eventSpanIds, pendingAnchoredEvent, selectedEvent, selectedEventId])

  useEffect(() => {
    document.body.style.cursor = dragging ? (dragging === 'flamegraph' ? 'row-resize' : 'col-resize') : ''
    document.body.style.userSelect = dragging ? 'none' : ''
    return () => {
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
    }
  }, [dragging])

  useEffect(() => {
    if (!selectedEventId) {
      return
    }

    const element = document.querySelector<HTMLElement>(`[data-trace-event-id="${CSS.escape(selectedEventId)}"]`)
    element?.scrollIntoView({
      block: 'nearest'
    })
  }, [selectedEventId])

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== 'ArrowDown' && event.key !== 'ArrowUp') {
        return
      }

      const target = event.target
      if (
        arrowKeyScopeRef.current === 'local' ||
        target instanceof HTMLInputElement ||
        target instanceof HTMLTextAreaElement ||
        target instanceof HTMLSelectElement ||
        (target instanceof HTMLElement &&
          (target.isContentEditable || Boolean(target.closest('[data-arrow-key-scope="local"]'))))
      ) {
        return
      }

      if (allEvents.length === 0) {
        return
      }

      event.preventDefault()
      const currentIndex = allEvents.findIndex(item => item.id === selectedEventId)
      const fallbackIndex = event.key === 'ArrowDown' ? 0 : allEvents.length - 1
      const nextIndex =
        currentIndex === -1
          ? fallbackIndex
          : clamp(
              currentIndex + (event.key === 'ArrowDown' ? 1 : -1),
              0,
              allEvents.length - 1
            )
      selectEvent(allEvents[nextIndex]!)
    }

    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [allEvents, selectedEventId, flamegraph.eventSpanIds])

  useEffect(() => {
    if (!dragging) {
      return
    }

    const onPointerMove = (event: PointerEvent) => {
      if (dragging === 'flamegraph') {
        const center = centerRef.current
        if (!center) {
          return
        }
        const bounds = center.getBoundingClientRect()
        const headerHeight = 42
        setFlamegraphHeight(clamp(Math.round(event.clientY - bounds.top - headerHeight), 80, bounds.height - headerHeight - 120))
        return
      }

      const workspace = workspaceRef.current
      if (!workspace) {
        return
      }

      const bounds = workspace.getBoundingClientRect()
      const width = bounds.width

      if (dragging === 'runs') {
        setRunsWidth(clamp(Math.round(event.clientX - bounds.left), 220, width - inspectorWidth - 260))
        return
      }

      setInspectorWidth(clamp(Math.round(bounds.right - event.clientX), 280, width - (runsOpen ? runsWidth : 0) - 260))
    }

    const onPointerUp = () => setDragging(null)
    window.addEventListener('pointermove', onPointerMove)
    window.addEventListener('pointerup', onPointerUp)
    return () => {
      window.removeEventListener('pointermove', onPointerMove)
      window.removeEventListener('pointerup', onPointerUp)
    }
  }, [dragging, flamegraphHeight, inspectorWidth, runsOpen, runsWidth, setFlamegraphHeight, setInspectorWidth, setRunsWidth])

  return (
    <main className="flex h-screen flex-col overflow-hidden bg-black text-[13px] text-neutral-100">
      <header className="flex h-10 shrink-0 items-center justify-between gap-3 border-b border-neutral-800 bg-neutral-950 px-3">
        <div className="min-w-0 truncate text-[13px] text-white">Nanotrace Observatory</div>
        <div className="flex shrink-0 items-center gap-2">
          <span className="text-[11px] uppercase tracking-[0.08em] text-neutral-500">Range</span>
          <div className="flex overflow-hidden border border-neutral-800 bg-black">
            {timeRangeOptions.map(option => (
              <button
                key={option.key}
                className={cn(
                  'h-7 border-l border-neutral-800 px-2 text-[11px] text-neutral-400 first:border-l-0 hover:bg-white/[0.04] hover:text-white',
                  timeRangeKey === option.key && 'bg-neutral-800 text-white'
                )}
                type="button"
                onClick={() => setTimeRangeKey(option.key)}
              >
                {option.label}
              </button>
            ))}
            <button
              className={cn(
                'h-7 border-l border-neutral-800 px-2 text-[11px] text-neutral-400 hover:bg-white/[0.04] hover:text-white',
                timeRangeKey === 'custom' && 'bg-neutral-800 text-white'
              )}
              type="button"
              onClick={() => setTimeRangeKey('custom')}
            >
              Custom
            </button>
          </div>
          {timeRangeKey === 'custom' ? (
            <div className="flex items-center gap-1">
              <input
                aria-label="Custom range start"
                className="h-7 w-[170px] border border-neutral-800 bg-black px-2 text-[11px] text-white outline-none"
                type="datetime-local"
                value={customRangeStart}
                onChange={event => {
                  setCustomRangeStart(event.target.value)
                  setTimeRangeKey('custom')
                }}
              />
              <span className="text-[11px] text-neutral-600">to</span>
              <input
                aria-label="Custom range end"
                className="h-7 w-[170px] border border-neutral-800 bg-black px-2 text-[11px] text-white outline-none"
                type="datetime-local"
                value={customRangeEnd}
                onChange={event => {
                  setCustomRangeEnd(event.target.value)
                  setTimeRangeKey('custom')
                }}
              />
            </div>
          ) : null}
        </div>
      </header>
      {listError ? (
        <div className="border-b border-neutral-800 bg-neutral-950 px-3 py-2 text-white">
          Trace list failed: {listError}
        </div>
      ) : null}
      {traceError ? (
        <div className="border-b border-neutral-800 bg-neutral-950 px-3 py-2 text-white">
          Trace detail failed: {traceError}
        </div>
      ) : null}

      <section ref={workspaceRef} className="flex min-h-0 flex-1 overflow-hidden">
        {runsOpen ? (
          <aside className={cn(panelClass, 'border-r border-neutral-800')} style={{ width: runsWidth, minWidth: runsWidth, maxWidth: runsWidth }}>
          <div className="flex items-center justify-between gap-2 border-b border-neutral-800 px-3 py-3">
            <div className="min-w-0">
              <h2 className="text-[15px]">Observatory</h2>
            </div>
            <button
              aria-label="Close observatory sidebar"
              className="inline-flex h-7 w-7 items-center justify-center bg-black text-neutral-300 hover:bg-white/[0.04] hover:text-white"
              title="Close observatory sidebar"
              type="button"
              onClick={() => setRunsOpen(false)}
            >
              <PanelLeftClose size={16} strokeWidth={1.8} />
            </button>
          </div>

          <div className="grid gap-1.5 border-b border-neutral-800 px-2 py-1.5">
            <div className="flex items-center gap-2">
              <span className="w-12 shrink-0 text-[11px] uppercase tracking-[0.08em] text-neutral-500">Group</span>
              <select
                className="h-7 min-w-0 flex-1 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none disabled:text-neutral-700"
                disabled={displayedGroupOptions.length === 0}
                value={groupBy}
                onChange={event => {
                  setManualGroupBy(event.target.value)
                  void navigate({ search: {}, to: '/' })
                }}
              >
                {displayedGroupOptions.map(option => (
                  <option key={option.path} value={option.path}>
                    {option.path}
                  </option>
                ))}
                {displayedGroupOptions.length === 0 ? <option value="">No groups</option> : null}
              </select>
            </div>
            {traceList.length > 0 || filter ? (
              <div className="flex items-center gap-2">
                <span className="w-12 shrink-0 text-[11px] uppercase tracking-[0.08em] text-neutral-500">Search</span>
                <input
                  className="h-7 min-w-0 flex-1 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none placeholder:text-neutral-600"
                  value={filter}
                  onChange={event => setFilter(event.target.value)}
                  placeholder="search groups..."
                />
              </div>
            ) : null}
          </div>

          <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-1.5 py-1.5">
            <div className="grid content-start gap-px">
              {filteredTraces.map(trace => (
                <button
                  key={`${trace.groupBy}:${trace.value}`}
                  className={cn(
                    'group relative w-full overflow-hidden rounded px-2.5 py-2 text-left text-inherit transition-colors',
                    trace.value === selectedGroupValue
                      ? 'bg-white/[0.07]'
                      : 'hover:bg-white/[0.04]'
                  )}
                  type="button"
                  onClick={() => {
                    void navigate({
                      to: '/$field/$value',
                      params: {
                        field: groupBy,
                        value: trace.value
                      },
                      search: {}
                    })
                  }}
                >
                  <div
                    className={cn(
                      'absolute inset-y-1 left-0 w-0.5 rounded-full transition-colors',
                      trace.value === selectedGroupValue ? 'bg-white' : 'bg-transparent'
                    )}
                  />
                  <div className="flex items-baseline justify-between gap-2 pl-1.5">
                    <span className="min-w-0 truncate font-mono text-[12px] text-white">
                      {trace.value}
                    </span>
                  </div>
                  <div className="mt-0.5 flex items-center gap-1.5 pl-1.5 text-[11px] text-neutral-500">
                    <span className="truncate">{formatDateTimeUs(trace.startedAt)}</span>
                    <span className="text-neutral-700">&middot;</span>
                    <span className="shrink-0">{formatDurationMs(trace.durationMs)}</span>
                  </div>
                  {trace.fields ? (
                    <div className="mt-0.5 truncate pl-1.5 text-[11px] text-neutral-600">
                      {previewGroupFields(trace.fields)}
                    </div>
                  ) : null}
                </button>
              ))}
              {loadingList ? (
                <div className="px-3 py-6 text-center text-[12px] text-neutral-600">
                  Loading groups...
                </div>
              ) : null}
              {emptyObservatory ? <EmptyState label="No observations yet." /> : null}
              {emptyGroup ? <EmptyState label="No groups found." /> : null}
              {emptyFilter ? <EmptyState label="No groups matched filter." /> : null}
            </div>
          </div>
          </aside>
        ) : null}

        {runsOpen ? <ResizeHandle onPointerDown={() => setDragging('runs')} /> : null}

        <section
          ref={centerRef}
          className={cn(panelClass, 'min-w-0 flex-1')}
          onFocusCapture={() => {
            arrowKeyScopeRef.current = 'events'
          }}
          onPointerDownCapture={() => {
            arrowKeyScopeRef.current = 'events'
          }}
        >
          <div className="flex items-center justify-between gap-2 border-b border-neutral-800 px-2 py-2">
            <div className="flex min-w-0 items-center gap-2">
              {!runsOpen && (
                <button
                  aria-label="Open traces sidebar"
                  className="inline-flex h-7 w-7 shrink-0 items-center justify-center bg-black text-neutral-300 hover:bg-white/[0.04] hover:text-white"
                  title="Open traces sidebar"
                  type="button"
                  onClick={() => setRunsOpen(true)}
                >
                  <PanelLeftOpen size={16} strokeWidth={1.8} />
                </button>
              )}
              <h2 className="truncate">{selectedGroupValue ? `${groupBy}=${selectedGroupValue}` : 'Select group'}</h2>
            </div>
          </div>

          {selectedGroupValue ? (
            <form
              className="flex min-w-0 items-center gap-2 overflow-hidden border-b border-neutral-800 px-2 py-1.5"
              onSubmit={event => {
                event.preventDefault()
                applyEventFilter()
              }}
            >
              <span className="shrink-0 text-[11px] uppercase tracking-[0.08em] text-neutral-500">Filter</span>
              <input
                className="h-7 min-w-0 flex-1 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none placeholder:text-neutral-600"
                value={eventFilterDraft}
                onChange={event => setEventFilterDraft(event.target.value)}
                placeholder="name=llm timestamp>=16:33:00 timestamp<16:34:00"
              />
              <button
                className="h-7 shrink-0 border border-neutral-700 px-2 text-[11px] text-neutral-200 hover:bg-white/[0.04] disabled:border-neutral-900 disabled:text-neutral-700"
                disabled={!eventFilterDirty}
                type="submit"
              >
                Apply
              </button>
              {hasEventFilter ? (
                <button
                  aria-label="Clear event filter"
                  className="inline-flex h-7 w-7 shrink-0 items-center justify-center text-neutral-400 hover:bg-white/[0.04] hover:text-white"
                  type="button"
                  onClick={clearEventFilter}
                >
                  <X size={13} strokeWidth={1.8} />
                </button>
              ) : null}
            </form>
          ) : null}

          <div className="overflow-hidden border-b border-neutral-800 bg-black" style={{ height: flamegraphHeight, minHeight: flamegraphHeight }}>
            {loadingDetail ? <EmptyState label="Loading trace detail." /> : null}
            {!loadingDetail && !selectedGroupValue ? <EmptyState label="Pick trace from left rail." /> : null}
            {!loadingDetail && selectedGroupValue && graphMode === 'histogram' && densityQuery.data ? (
              <DensityHistogramCanvas
                density={densityQuery.data}
                totalCount={summaryQuery.data?.count ?? 0}
                onSelectRange={applyTimeRange}
              />
            ) : null}
            {!loadingDetail && selectedGroupValue && graphMode === 'histogram' && !densityQuery.data ? (
              <EmptyState label="Loading density histogram." />
            ) : null}
            {!loadingDetail && selectedGroupValue && graphMode === 'flamegraph' && !traceDetail ? <EmptyState label="No events matched filter." /> : null}
            {!loadingDetail && graphMode === 'flamegraph' && traceDetail && flamegraph.rows.length === 0 ? (
              <EmptyState label="No bounded lifecycle spans in trace." />
            ) : null}
            {!loadingDetail && graphMode === 'flamegraph' && traceDetail && flamegraph.rows.length > 0 ? (
              <FlamegraphCanvas
                flamegraph={flamegraph}
                selectedCanvasSpanId={selectedCanvasSpanId}
                selectedEventId={selectedEventId}
                onInspect={inspectSpan}
              />
            ) : null}
          </div>

          <div
            aria-label="Resize flamegraph"
            className="group relative z-10 h-0 shrink-0 cursor-row-resize"
            onPointerDown={event => {
              event.preventDefault()
              setDragging('flamegraph')
            }}
            role="separator"
          >
            <div className="absolute inset-x-0 -top-[3px] h-[6px] group-hover:bg-white" />
          </div>

          {selectedGroupValue ? (
            <div className="flex items-center gap-2 border-b border-neutral-800 bg-neutral-950 px-2 py-1.5">
              <div className="inline-flex border border-neutral-800 bg-black">
                <button
                  className={cn(
                    'h-6 px-2 text-[11px] text-neutral-500 hover:text-white disabled:cursor-not-allowed disabled:text-neutral-700',
                    graphMode === 'flamegraph' && 'bg-neutral-800 text-white'
                  )}
                  disabled={flamegraphDisabled}
                  type="button"
                  onClick={() => setSelectedGraphMode('flamegraph')}
                >
                  Flamegraph
                </button>
                <button
                  className={cn(
                    'h-6 border-l border-neutral-800 px-2 text-[11px] text-neutral-500 hover:text-white',
                    graphMode === 'histogram' && 'bg-neutral-800 text-white'
                  )}
                  type="button"
                  onClick={() => setSelectedGraphMode('histogram')}
                >
                  Histogram
                </button>
              </div>
              {flamegraphDisabled ? (
                <span className="truncate text-[11px] text-neutral-600">Flamegraph disabled above 200k events.</span>
              ) : null}
            </div>
          ) : null}

          {traceDetail ? (
            <EventPanel
              anchorIndex={eventPages[0]?.anchorIndex ?? 0}
              events={allEvents}
              emptyLabel={hasAppliedEventFilter(eventFilterParams) ? 'No events matched filter.' : 'No events.'}
              fields={traceDetail.fields}
              hasMore={eventsQuery.hasNextPage}
              hasPrevious={eventsQuery.hasPreviousPage}
              highlightedEventIds={highlightedEventIds}
              loading={loadingTableDetail}
              loadingAnchor={loadingAnchoredEvents}
              loadingMore={eventsQuery.isFetchingNextPage}
              loadingPrevious={eventsQuery.isFetchingPreviousPage}
              scrollStateKey={eventTableScrollKey}
              selectedColumns={selectedEventColumnsForTrace}
              selectedEventAlign={eventAnchorOverride?.key === eventDataKey ? 'center' : 'auto'}
              selectedEventId={selectedEventId}
              onLoadMore={() => {
                void eventsQuery.fetchNextPage()
              }}
              onLoadPrevious={() => eventsQuery.fetchPreviousPage().then(() => undefined)}
              onInspect={selectEvent}
              onSetColumns={setSelectedEventColumns}
              onToggleColumn={path =>
                setSelectedEventColumns(current =>
                  current.includes(path) ? current.filter(value => value !== path) : [...current, path]
                )
              }
            />
          ) : (
            <div className="min-h-0 flex-1 bg-black" />
          )}
        </section>

        {selectedEventId ? (
          <>
            <ResizeHandle onPointerDown={() => setDragging('inspector')} />

            <aside
              className={cn(panelClass, 'border-l border-neutral-800')}
              data-arrow-key-scope="local"
              onFocusCapture={() => {
                arrowKeyScopeRef.current = 'local'
              }}
              onPointerDownCapture={() => {
                arrowKeyScopeRef.current = 'local'
              }}
              style={{ width: inspectorWidth, minWidth: inspectorWidth, maxWidth: inspectorWidth }}
            >
              <div className="flex items-center justify-between gap-2 border-b border-neutral-800 px-2 py-2">
                <div className="min-w-0">
                  <p className="text-[11px] uppercase tracking-[0.08em] text-neutral-500">Inspector</p>
                  <h2 className="truncate">{inspectorPayload?.title || 'Loading event'}</h2>
                </div>
                <div className="flex shrink-0 items-center gap-1">
                  {hasInspectorFilter ? (
                    <button
                      className="border border-neutral-800 px-2 py-1 text-neutral-300"
                      type="button"
                      onClick={() => setInspectorQuery('')}
                    >
                      Clear
                    </button>
                  ) : null}
                  <button
                    aria-label="Close inspector"
                    className="inline-flex h-7 w-7 items-center justify-center text-neutral-400 hover:bg-white/[0.04] hover:text-white"
                    type="button"
                    onClick={() => setEventSearch('')}
                  >
                    <X size={13} strokeWidth={1.8} />
                  </button>
                </div>
              </div>
              <div className="flex items-center gap-2 border-b border-neutral-800 px-2 py-1.5">
                <p className="text-[11px] uppercase tracking-[0.08em] text-neutral-500">Filter</p>
                <input
                  className="h-7 min-w-0 flex-1 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none placeholder:text-neutral-600"
                  value={inspectorQuery}
                  onChange={event => setInspectorQuery(event.target.value)}
                  placeholder="field name or value"
                />
              </div>
              <div className="min-h-0 flex-1 overflow-auto overscroll-contain bg-black p-2">
                {!inspectorPayload ? (
                  <EmptyState label="Loading event." />
                ) : hasInspectorFilter ? (
                  filteredPayloadNode ? (
                    <FilteredJsonTree node={filteredPayloadNode} />
                  ) : (
                    <div className="px-2 py-3 text-neutral-500">No keys matched filter.</div>
                  )
                ) : (
                  <JsonTree name={inspectorPayload.title} value={inspectorPayload.value} />
                )}
              </div>
            </aside>
          </>
        ) : null}
      </section>
    </main>
  )
}

function ResizeHandle({ onPointerDown }: { onPointerDown: () => void }) {
  return (
    <div
      aria-label="Resize panel"
      className="group relative z-10 w-0 shrink-0 cursor-col-resize"
      onPointerDown={event => {
        event.preventDefault()
        onPointerDown()
      }}
      role="separator"
    >
      <div className="absolute inset-y-0 -left-[3px] w-[6px] group-hover:bg-white" />
    </div>
  )
}

function normalizeTraceDetail(payload: LogGroupDetail): TraceDetail {
  const events = mergeSpanRecords(Array.isArray(payload.logs) ? payload.logs : [])
  return {
    fields: orderLogFields(inferLogFields(events)),
    group: payload.group,
    events,
    relatedEvents: []
  }
}

const emptyFlamegraph: Flamegraph = {
  eventCreatedAt: {},
  eventSpanIds: {},
  maxEnd: 0,
  minStart: 0,
  rows: [],
  totalDuration: 0
}

function mergeLogFields(fields: LogField[]) {
  const counts = new Map<string, { count: number; types: Set<string> }>()
  for (const field of fields) {
    const current = counts.get(field.path) ?? { count: 0, types: new Set<string>() }
    current.count = Math.max(current.count, field.count)
    for (const type of field.types) current.types.add(type)
    counts.set(field.path, current)
  }
  return orderLogFields(
    [...counts].map(([path, field]) => ({
      path,
      count: field.count,
      types: [...field.types].sort()
    }))
  )
}

function mergeSpanRecords(events: TraceEvent[]) {
  const spans = new Map<
    string,
    {
      end?: TraceEvent
      start?: TraceEvent
    }
  >()
  const passthrough: TraceEvent[] = []

  for (const event of events) {
    const type = stringField(event.data.type)
    const spanId = stringField(event.data.spanId)
    if (spanId && (type === 'span_start' || type === 'span_end')) {
      const span = spans.get(spanId) ?? {}
      spans.set(spanId, type === 'span_start' ? { ...span, start: event } : { ...span, end: event })
      continue
    }
    passthrough.push(event)
  }

  return [
    ...passthrough,
    ...[...spans.values()].map(mergedSpanRecord)
  ].sort((a, b) => traceTimeMs(a.createdAt) - traceTimeMs(b.createdAt))
}

function mergedSpanRecord({ end, start }: { end?: TraceEvent; start?: TraceEvent }): TraceEvent {
  const source = end ?? start!
  const startData = start?.data ?? {}
  const endData = end?.data ?? {}
  const startedAt = stringField(startData.startedAt) || stringField(endData.startedAt) || start?.createdAt || source.createdAt
  const endedAt = stringField(endData.endedAt)
  const durationMs = endedAt ? traceTimeMs(endedAt) - traceTimeMs(startedAt) : NaN
  return {
    ...source,
    createdAt: startedAt,
    data: {
      ...startData,
      ...endData,
      type: 'span',
      startedAt,
      ...(endedAt ? { endedAt } : { open: true }),
      ...(Number.isFinite(durationMs) && durationMs >= 0 ? { durationMs } : {})
    }
  }
}

function inferLogFields(events: TraceEvent[]) {
  const fields = new Map<string, { count: number; types: Set<string> }>()
  for (const event of events) {
    const seen = new Set<string>()
    collectLogFields({
      fields,
      seen,
      value: event.data
    })
  }
  return [...fields.entries()].map(([path, field]) => ({
    count: field.count,
    path,
    types: [...field.types].sort()
  }))
}

function collectLogFields({
  fields,
  path = '',
  seen,
  value
}: {
  fields: Map<string, { count: number; types: Set<string> }>
  path?: string
  seen: Set<string>
  value: JsonValue
}) {
  if (path && !seen.has(path)) {
    seen.add(path)
    const field = fields.get(path) ?? { count: 0, types: new Set<string>() }
    field.count += 1
    field.types.add(getJsonValueType(value))
    fields.set(path, field)
  }

  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return
  }
  for (const [key, child] of Object.entries(value)) {
    if (child === undefined) continue
    collectLogFields({
      fields,
      path: path ? `${path}.${key}` : key,
      seen,
      value: child
    })
  }
}

function orderLogFields(fields: LogField[]) {
  const common = new Map(defaultEventColumns.map((path, index) => [path, index]))
  return [...fields].sort((left, right) => {
    const leftCommon = common.get(left.path)
    const rightCommon = common.get(right.path)
    if (leftCommon !== undefined || rightCommon !== undefined) {
      return (leftCommon ?? Number.MAX_SAFE_INTEGER) - (rightCommon ?? Number.MAX_SAFE_INTEGER)
    }
    if (left.count !== right.count) {
      return right.count - left.count
    }
    return left.path.localeCompare(right.path)
  })
}

function previewGroupFields(fields: JsonObject | undefined) {
  if (!fields) {
    return 'no scalar fields'
  }
  return (
    Object.entries(fields)
      .filter(([, value]) => typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean')
      .slice(0, 2)
      .map(([key, value]) => `${key}=${String(value)}`)
      .join('  ') || 'no scalar fields'
  )
}

function EmptyState({ label }: { label: string }) {
  return <div className="px-3 py-6 text-center text-neutral-500">{label}</div>
}

function DensityHistogramCanvas({
  density,
  totalCount,
  onSelectRange
}: {
  density: LogDensity
  totalCount: number
  onSelectRange: (range: { createdAfter: string; createdBefore: string }) => void
}) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null)
  const rootRef = useRef<HTMLDivElement | null>(null)
  const [brush, setBrush] = useState<null | { fromX: number; toX: number }>(null)
  const fromMs = traceTimeMs(density.from)
  const toMs = traceTimeMs(density.to)
  const durationMs = Math.max(toMs - fromMs, 1)
  const maxCount = Math.max(...density.buckets.map(bucket => bucket.count), 1)

  useEffect(() => {
    const canvas = canvasRef.current
    const root = rootRef.current
    if (!canvas || !root) return

    const rect = root.getBoundingClientRect()
    const dpr = window.devicePixelRatio || 1
    canvas.width = Math.max(1, Math.floor(rect.width * dpr))
    canvas.height = Math.max(1, Math.floor(rect.height * dpr))

    const ctx = canvas.getContext('2d')
    if (!ctx) return

    ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    ctx.clearRect(0, 0, rect.width, rect.height)

    const padX = 18
    const padTop = 18
    const axisHeight = 26
    const plotHeight = Math.max(1, rect.height - padTop - axisHeight)
    const plotWidth = Math.max(1, rect.width - padX * 2)

    ctx.fillStyle = 'rgba(255,255,255,0.035)'
    for (let x = padX; x <= rect.width - padX; x += Math.max(80, plotWidth / 8)) {
      ctx.fillRect(Math.round(x), padTop, 1, plotHeight)
    }

    const bucketWidth = Math.max(1, (density.bucketMs / durationMs) * plotWidth)
    for (const bucket of density.buckets) {
      const start = traceTimeMs(bucket.start)
      if (!Number.isFinite(start)) continue
      const x = padX + ((start - fromMs) / durationMs) * plotWidth
      const height = Math.max(1, (bucket.count / maxCount) * plotHeight)
      const errorHeight = Math.max(0, ((bucket.errorCount ?? 0) / maxCount) * plotHeight)
      ctx.fillStyle = 'rgba(212,212,212,0.58)'
      ctx.fillRect(x, padTop + plotHeight - height, Math.max(1, bucketWidth - 1), height)
      if (errorHeight > 0) {
        ctx.fillStyle = 'rgba(248,113,113,0.85)'
        ctx.fillRect(x, padTop + plotHeight - errorHeight, Math.max(1, bucketWidth - 1), errorHeight)
      }
    }

    ctx.fillStyle = 'rgba(255,255,255,0.08)'
    ctx.fillRect(padX, padTop + plotHeight, plotWidth, 1)
    ctx.fillStyle = 'rgba(163,163,163,0.9)'
    ctx.font = '11px ui-monospace, SFMono-Regular, Menlo, monospace'
    ctx.textBaseline = 'top'
    const labels =
      plotWidth >= 360
        ? [
            { align: 'left' as CanvasTextAlign, label: density.from, x: padX },
            { align: 'center' as CanvasTextAlign, label: new Date(fromMs + durationMs / 2).toISOString(), x: padX + plotWidth / 2 },
            { align: 'right' as CanvasTextAlign, label: density.to, x: padX + plotWidth }
          ]
        : [
            { align: 'left' as CanvasTextAlign, label: density.from, x: padX },
            { align: 'right' as CanvasTextAlign, label: density.to, x: padX + plotWidth }
          ]
    labels.forEach(({ align, label, x }) => {
      ctx.textAlign = align
      ctx.fillText(formatClockUs(label), x, padTop + plotHeight + 8)
    })
    ctx.textAlign = 'left'

    if (brush) {
      const x0 = Math.min(brush.fromX, brush.toX)
      const x1 = Math.max(brush.fromX, brush.toX)
      ctx.fillStyle = 'rgba(255,255,255,0.14)'
      ctx.fillRect(x0, padTop, Math.max(1, x1 - x0), plotHeight)
      ctx.strokeStyle = 'rgba(255,255,255,0.8)'
      ctx.strokeRect(x0, padTop, Math.max(1, x1 - x0), plotHeight)
    }
  }, [brush, density, durationMs, fromMs, maxCount, toMs])

  function xToTimestamp(clientX: number) {
    const root = rootRef.current
    if (!root) return ''
    const rect = root.getBoundingClientRect()
    const padX = 18
    const ratio = clamp((clientX - rect.left - padX) / Math.max(1, rect.width - padX * 2), 0, 1)
    return new Date(fromMs + ratio * durationMs).toISOString()
  }

  return (
    <div
      ref={rootRef}
      className="relative h-full cursor-crosshair bg-black"
      onPointerDown={event => {
        const bounds = event.currentTarget.getBoundingClientRect()
        const x = clamp(event.clientX - bounds.left, 18, bounds.width - 18)
        setBrush({ fromX: x, toX: x })
      }}
      onPointerMove={event => {
        if (!brush) return
        const bounds = event.currentTarget.getBoundingClientRect()
        setBrush(current => current ? { ...current, toX: clamp(event.clientX - bounds.left, 18, bounds.width - 18) } : current)
      }}
      onPointerUp={event => {
        if (!brush) return
        const bounds = event.currentTarget.getBoundingClientRect()
        const toX = clamp(event.clientX - bounds.left, 18, bounds.width - 18)
        const fromX = brush.fromX
        setBrush(null)
        if (Math.abs(toX - fromX) < 4) return
        const createdAfter = xToTimestamp(Math.min(fromX, toX) + bounds.left)
        const createdBefore = xToTimestamp(Math.max(fromX, toX) + bounds.left)
        if (createdAfter && createdBefore) onSelectRange({ createdAfter, createdBefore })
      }}
    >
      <canvas ref={canvasRef} className="absolute inset-0 h-full w-full" />
      <div className="pointer-events-none absolute left-2 top-2 text-[11px] uppercase tracking-[0.08em] text-neutral-500">
        Density {totalCount > 200000 ? '200k+' : totalCount}
      </div>
    </div>
  )
}

function FlamegraphCanvas({
  flamegraph,
  selectedCanvasSpanId,
  selectedEventId,
  onInspect
}: {
  flamegraph: Flamegraph
  selectedCanvasSpanId: string
  selectedEventId: string
  onInspect: (span: FlameSpan) => void
}) {
  const rootRef = useRef<HTMLDivElement | null>(null)
  const canvasRef = useRef<HTMLCanvasElement | null>(null)
  const minimapRef = useRef<HTMLCanvasElement | null>(null)
  const viewportRef = useRef<HTMLDivElement | null>(null)
  const hitRectsRef = useRef<Array<{ h: number; span: FlameSpan; w: number; x: number; y: number }>>([])
  const viewportFrameRef = useRef(0)
  const zoomTargetRef = useRef<null | {
    offset: number
    ratio: number
  }>(null)
  const panRef = useRef<null | {
    moved: boolean
    startX: number
    startY: number
    scrollLeft: number
    scrollTop: number
  }>(null)
  const minimapPanRef = useRef<null | {
    ratio: number
  }>(null)
  const [zoom, setZoom] = useState(1)
  const [panning, setPanning] = useState(false)
  const [hoverX, setHoverX] = useState<number | null>(null)
  const [viewport, setViewport] = useState({
    clientHeight: 0,
    clientWidth: 0,
    scrollLeft: 0,
    scrollTop: 0,
    scrollWidth: 1
  })
  const rowHeight = 32
  const rowGap = 6
  const axisHeight = 26
  const scenePaddingX = 24
  const scenePaddingY = 16
  const baseMsToPx = Math.max(viewport.clientWidth - scenePaddingX * 2, 960) / 10_000
  const msToPx = baseMsToPx * zoom
  const sceneWidth = Math.max(flamegraph.totalDuration * msToPx + scenePaddingX * 2, 960)
  const sceneHeight = Math.max(axisHeight + flamegraph.rows.length * (rowHeight + rowGap) + scenePaddingY * 2, 96)
  const spans = useMemo(() => flamegraph.rows.flat(), [flamegraph.rows])
  const minimapHeight = Math.max(flamegraph.rows.length * 4 + 10, 30)
  const viewportLeft = viewport.scrollWidth > 0 ? viewport.scrollLeft / viewport.scrollWidth : 0
  const viewportWidth = viewport.scrollWidth > 0 ? viewport.clientWidth / viewport.scrollWidth : 1
  const visibleSpans = useMemo(() => {
    const x0 = viewport.scrollLeft - viewport.clientWidth
    const x1 = viewport.scrollLeft + viewport.clientWidth * 2
    const y0 = viewport.scrollTop - (rowHeight + rowGap) * 4
    const y1 = viewport.scrollTop + viewport.clientHeight + (rowHeight + rowGap) * 4

    return spans.filter(span => {
      const left = scenePaddingX + (span.startMs - flamegraph.minStart) * msToPx
      const width = span.kind === 'event' ? eventMarkerWidth : Math.max((span.endMs - span.startMs) * msToPx, 3)
      const top = axisHeight + scenePaddingY + span.lane * (rowHeight + rowGap)
      return left + width >= x0 && left <= x1 && top + rowHeight >= y0 && top <= y1
    })
  }, [axisHeight, flamegraph.minStart, msToPx, spans, viewport.clientHeight, viewport.clientWidth, viewport.scrollLeft, viewport.scrollTop])

  useEffect(() => {
    const element = viewportRef.current
    if (!element) {
      return
    }

    const updateViewportNow = () =>
      setViewport({
        clientHeight: element.clientHeight,
        clientWidth: element.clientWidth,
        scrollLeft: element.scrollLeft,
        scrollTop: element.scrollTop,
        scrollWidth: element.scrollWidth
      })
    const updateViewport = () => {
      if (viewportFrameRef.current) return
      viewportFrameRef.current = window.requestAnimationFrame(() => {
        viewportFrameRef.current = 0
        updateViewportNow()
      })
    }

    updateViewportNow()
    const resizeObserver = new ResizeObserver(updateViewport)
    resizeObserver.observe(element)
    element.addEventListener('scroll', updateViewport)
    return () => {
      if (viewportFrameRef.current) {
        window.cancelAnimationFrame(viewportFrameRef.current)
        viewportFrameRef.current = 0
      }
      resizeObserver.disconnect()
      element.removeEventListener('scroll', updateViewport)
    }
  }, [sceneWidth])

  useEffect(() => {
    const target = zoomTargetRef.current
    const element = viewportRef.current
    if (!target || !element) {
      return
    }

    element.scrollLeft = clamp(
      target.ratio * sceneWidth - target.offset,
      0,
      Math.max(sceneWidth - element.clientWidth, 0)
    )
    zoomTargetRef.current = null
  }, [sceneWidth])

  useEffect(() => {
    const canvas = minimapRef.current
    if (!canvas) return

    const rect = canvas.getBoundingClientRect()
    const dpr = window.devicePixelRatio || 1
    canvas.width = Math.max(1, Math.floor(rect.width * dpr))
    canvas.height = Math.max(1, Math.floor(rect.height * dpr))

    const ctx = canvas.getContext('2d')
    if (!ctx) return

    ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    ctx.clearRect(0, 0, rect.width, rect.height)
    ctx.fillStyle = 'rgba(255,255,255,0.03)'
    for (let x = 0; x < rect.width; x += 64) {
      ctx.fillRect(x, 0, 1, rect.height)
    }

    for (const span of spans) {
      const left = ((span.startMs - flamegraph.minStart) / flamegraph.totalDuration) * rect.width
      const width = Math.max(
        span.kind === 'event'
          ? rect.width * 0.005
          : ((span.endMs - span.startMs) / flamegraph.totalDuration) * rect.width,
        rect.width * 0.0035
      )
      ctx.fillStyle = minimapSpanColor({
        error: isErrorPayload(span.payload),
        selected: span.id === selectedCanvasSpanId,
        kind: span.kind
      })
      ctx.fillRect(left, 4 + span.lane * 4, width, 3)
    }
  }, [flamegraph.minStart, flamegraph.totalDuration, minimapHeight, selectedCanvasSpanId, spans])

  useEffect(() => {
    const canvas = canvasRef.current
    const viewportElement = viewportRef.current
    if (!canvas || !viewportElement) return

    const dpr = window.devicePixelRatio || 1
    const width = Math.max(1, viewport.clientWidth)
    const height = Math.max(1, viewport.clientHeight)
    canvas.width = Math.floor(width * dpr)
    canvas.height = Math.floor(height * dpr)

    const ctx = canvas.getContext('2d')
    if (!ctx) return

    ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    ctx.clearRect(0, 0, width, height)

    const visibleStartMs = clamp(
      flamegraph.minStart + (viewport.scrollLeft - scenePaddingX) / msToPx,
      flamegraph.minStart,
      flamegraph.maxEnd
    )
    const visibleEndMs = clamp(
      flamegraph.minStart + (viewport.scrollLeft + width - scenePaddingX) / msToPx,
      flamegraph.minStart,
      flamegraph.maxEnd
    )
    const majorInterval = niceTimeInterval((visibleEndMs - visibleStartMs) / Math.max(1, width / 190))
    const minorInterval = niceTimeInterval(majorInterval / 5)

    ctx.fillStyle = '#050505'
    ctx.fillRect(0, 0, width, axisHeight)
    ctx.strokeStyle = 'rgba(255,255,255,0.12)'
    ctx.beginPath()
    ctx.moveTo(0, axisHeight - 0.5)
    ctx.lineTo(width, axisHeight - 0.5)
    ctx.stroke()

    if (minorInterval < majorInterval) {
      ctx.fillStyle = 'rgba(255,255,255,0.035)'
      for (let t = Math.ceil(visibleStartMs / minorInterval) * minorInterval; t <= visibleEndMs; t += minorInterval) {
        const x = scenePaddingX + (t - flamegraph.minStart) * msToPx - viewport.scrollLeft
        ctx.fillRect(Math.round(x), axisHeight, 1, height - axisHeight)
      }
    }

    ctx.font = '11px ui-monospace, SFMono-Regular, Menlo, monospace'
    ctx.textBaseline = 'top'
    let lastLabelRight = -Infinity
    for (let t = Math.ceil(visibleStartMs / majorInterval) * majorInterval; t <= visibleEndMs; t += majorInterval) {
      const x = scenePaddingX + (t - flamegraph.minStart) * msToPx - viewport.scrollLeft
      const roundedX = Math.round(x) + 0.5
      ctx.strokeStyle = 'rgba(255,255,255,0.14)'
      ctx.beginPath()
      ctx.moveTo(roundedX, axisHeight)
      ctx.lineTo(roundedX, height)
      ctx.stroke()
      ctx.strokeStyle = 'rgba(255,255,255,0.36)'
      ctx.beginPath()
      ctx.moveTo(roundedX, axisHeight - 7)
      ctx.lineTo(roundedX, axisHeight)
      ctx.stroke()

      const label = formatAxisTick(t, majorInterval)
      const labelWidth = ctx.measureText(label).width
      const labelX = clamp(x + 4, 2, Math.max(2, width - labelWidth - 2))
      if (labelX > lastLabelRight + 12) {
        ctx.fillStyle = 'rgba(212,212,212,0.82)'
        ctx.fillText(label, labelX, 6)
        lastLabelRight = labelX + labelWidth
      }
    }

    ctx.fillStyle = 'rgba(255,255,255,0.05)'
    for (let y = axisHeight + scenePaddingY - viewport.scrollTop; y <= height; y += rowHeight + rowGap) {
      ctx.fillRect(0, Math.round(y + rowHeight), width, 1)
    }

    const hitRects: Array<{ h: number; span: FlameSpan; w: number; x: number; y: number }> = []
    ctx.font = '12px ui-monospace, SFMono-Regular, Menlo, monospace'
    ctx.textBaseline = 'middle'

    for (const span of visibleSpans) {
      const baseLeft = scenePaddingX + (span.startMs - flamegraph.minStart) * msToPx
      const spanWidth = span.kind === 'event' ? eventMarkerWidth : Math.max((span.endMs - span.startMs) * msToPx, 3)
      const left = span.kind === 'event' ? baseLeft - spanWidth / 2 : baseLeft
      const x = left - viewport.scrollLeft
      const y = axisHeight + scenePaddingY + span.lane * (rowHeight + rowGap) - viewport.scrollTop
      const selected = span.id === selectedCanvasSpanId
      const error = isErrorPayload(span.payload)

      ctx.fillStyle = mainSpanFill({ error, kind: span.kind, selected })
      ctx.strokeStyle = mainSpanStroke({ error, selected })
      ctx.lineWidth = selected ? 2 : 1
      ctx.fillRect(x, y, spanWidth, rowHeight)
      ctx.strokeRect(x + 0.5, y + 0.5, Math.max(1, spanWidth - 1), rowHeight - 1)

      if (span.kind !== 'event' && spanWidth > 54) {
        ctx.save()
        ctx.beginPath()
        ctx.rect(x + 4, y, Math.max(0, spanWidth - 8), rowHeight)
        ctx.clip()
        ctx.fillStyle = error ? 'rgba(255,242,242,0.96)' : 'rgba(245,245,245,0.94)'
        ctx.fillText(span.label, x + 8, y + rowHeight / 2)
        if (spanWidth > 130) {
          ctx.textAlign = 'right'
          ctx.fillStyle = 'rgba(212,212,212,0.82)'
          ctx.fillText(formatDurationMs(span.endMs - span.startMs), x + spanWidth - 8, y + rowHeight / 2)
          ctx.textAlign = 'left'
        }
        ctx.restore()
      }

      hitRects.push({ h: rowHeight, span, w: spanWidth, x, y })
    }

    if (hoverX !== null) {
      const x = clamp(hoverX, 0, width)
      const timeMs = clamp(
        flamegraph.minStart + (viewport.scrollLeft + x - scenePaddingX) / msToPx,
        flamegraph.minStart,
        flamegraph.maxEnd
      )
      const label = formatAxisHover(timeMs)
      const labelWidth = ctx.measureText(label).width
      const labelX = clamp(x + 8, 2, Math.max(2, width - labelWidth - 10))
      ctx.strokeStyle = 'rgba(255,255,255,0.48)'
      ctx.beginPath()
      ctx.moveTo(Math.round(x) + 0.5, axisHeight)
      ctx.lineTo(Math.round(x) + 0.5, height)
      ctx.stroke()
      ctx.fillStyle = 'rgba(0,0,0,0.88)'
      ctx.fillRect(labelX - 4, 4, labelWidth + 8, 16)
      ctx.strokeStyle = 'rgba(255,255,255,0.22)'
      ctx.strokeRect(labelX - 4.5, 3.5, labelWidth + 9, 17)
      ctx.fillStyle = 'rgba(245,245,245,0.94)'
      ctx.textBaseline = 'top'
      ctx.fillText(label, labelX, 6)
    }

    hitRectsRef.current = hitRects
  }, [
    axisHeight,
    flamegraph.minStart,
    flamegraph.maxEnd,
    msToPx,
    rowGap,
    rowHeight,
    scenePaddingX,
    scenePaddingY,
    sceneWidth,
    selectedCanvasSpanId,
    hoverX,
    viewport.clientHeight,
    viewport.clientWidth,
    viewport.scrollLeft,
    viewport.scrollTop,
    visibleSpans
  ])

  function setTimelineZoom(nextZoom: number, offset = viewport.clientWidth / 2) {
    const element = viewportRef.current
    if (!element) {
      setZoom(nextZoom)
      return
    }

    zoomTargetRef.current = {
      offset,
      ratio:
        sceneWidth > 0
          ? (element.scrollLeft + offset) / sceneWidth
          : 0
    }
    setZoom(nextZoom)
  }

  function wheelTimeline({
    clientX,
    ctrlKey,
    currentTarget,
    deltaX,
    deltaY,
    preventDefault
  }: {
    clientX: number
    ctrlKey: boolean
    currentTarget: HTMLElement
    deltaX: number
    deltaY: number
    preventDefault: () => void
  }) {
    const element = viewportRef.current
    if (!element) return

    if (ctrlKey) {
      preventDefault()
      const bounds = currentTarget.getBoundingClientRect()
      const offset = clamp(clientX - bounds.left, 0, bounds.width)
      const factor = deltaY > 0 ? 1 / 1.15 : 1.15
      setTimelineZoom(clamp(zoom * factor, 0.00005, 500), offset)
      return
    }

    if (Math.abs(deltaX) > 0) {
      preventDefault()
      element.scrollLeft += deltaX
    }
  }

  useEffect(() => {
    if (!panning) return
    const onPointerMove = (e: PointerEvent) => {
      const pan = panRef.current
      const el = viewportRef.current
      if (!pan || !el) return
      if (Math.abs(e.clientX - pan.startX) > 3 || Math.abs(e.clientY - pan.startY) > 3) {
        pan.moved = true
      }
      el.scrollLeft = pan.scrollLeft - (e.clientX - pan.startX)
      el.scrollTop = pan.scrollTop - (e.clientY - pan.startY)
    }
    const onPointerUp = (e: PointerEvent) => {
      const pan = panRef.current
      if (pan && !pan.moved) {
        inspectCanvasPoint(e.clientX, e.clientY)
      }
      panRef.current = null
      minimapPanRef.current = null
      setPanning(false)
    }
    window.addEventListener('pointermove', onPointerMove)
    window.addEventListener('pointerup', onPointerUp)
    return () => {
      window.removeEventListener('pointermove', onPointerMove)
      window.removeEventListener('pointerup', onPointerUp)
    }
  }, [panning])

  function inspectCanvasPoint(clientX: number, clientY: number) {
    const canvas = canvasRef.current
    if (!canvas) return

    const bounds = canvas.getBoundingClientRect()
    const x = clientX - bounds.left
    const y = clientY - bounds.top
    for (let i = hitRectsRef.current.length - 1; i >= 0; i -= 1) {
      const rect = hitRectsRef.current[i]!
      if (x >= rect.x && x <= rect.x + rect.w && y >= rect.y && y <= rect.y + rect.h) {
        onInspect(rect.span)
        return
      }
    }
  }

  useEffect(() => {
    const root = rootRef.current
    if (!root) return

    const onWheel = (event: WheelEvent) => {
      const handled = event.ctrlKey || Math.abs(event.deltaX) > 0
      if (handled) {
        event.stopPropagation()
      }
      wheelTimeline({
        clientX: event.clientX,
        ctrlKey: event.ctrlKey,
        currentTarget: root,
        deltaX: event.deltaX,
        deltaY: event.deltaY,
        preventDefault: () => event.preventDefault()
      })
    }

    root.addEventListener('wheel', onWheel, { capture: true, passive: false })
    return () => root.removeEventListener('wheel', onWheel, true)
  }, [sceneWidth, viewport.clientWidth, zoom])

  return (
    <div
      ref={rootRef}
      className={cn('flex h-full flex-col', panning ? 'cursor-grabbing' : 'cursor-grab')}
      style={{ overscrollBehavior: 'contain', touchAction: 'none' }}
    >
      <div>
        <div
          className="relative min-w-0 overflow-hidden border-b border-neutral-800 bg-neutral-950"
          role="presentation"
          onPointerDown={event => {
            event.preventDefault()
            const bounds = event.currentTarget.getBoundingClientRect()
            const ratio = clamp((event.clientX - bounds.left) / bounds.width, 0, 1)
            const element = viewportRef.current
            if (!element) {
              return
            }

            minimapPanRef.current = { ratio }
            element.scrollLeft = clamp(
              ratio * sceneWidth - element.clientWidth / 2,
              0,
              Math.max(sceneWidth - element.clientWidth, 0)
            )
            setPanning(true)
          }}
          onPointerMove={event => {
            if (!minimapPanRef.current) return
            const bounds = event.currentTarget.getBoundingClientRect()
            const ratio = clamp((event.clientX - bounds.left) / bounds.width, 0, 1)
            const element = viewportRef.current
            if (!element) return
            minimapPanRef.current.ratio = ratio
            element.scrollLeft = clamp(
              ratio * sceneWidth - element.clientWidth / 2,
              0,
              Math.max(sceneWidth - element.clientWidth, 0)
            )
          }}
        >
          <canvas ref={minimapRef} className="pointer-events-none absolute inset-0 h-full w-full" />
          <div
            className="absolute bottom-0 top-0 border border-white/80 bg-white/8"
            style={{
              left: `${viewportLeft * 100}%`,
              minWidth: '2%',
              width: `${Math.min(viewportWidth, 1) * 100}%`
            }}
          />
          <div className="pointer-events-none" style={{ height: `${minimapHeight}px` }} />
        </div>
      </div>

      <div
        ref={viewportRef}
        className={cn(
          'min-h-0 flex-1 overflow-auto bg-black',
          panning ? 'cursor-grabbing' : 'cursor-grab'
        )}
        onPointerDown={event => {
          if (event.button !== 0 || (event.target as HTMLElement).closest('button')) return
          event.preventDefault()
          const el = viewportRef.current
          if (!el) return
          panRef.current = {
            moved: false,
            startX: event.clientX,
            startY: event.clientY,
            scrollLeft: el.scrollLeft,
            scrollTop: el.scrollTop
          }
          setPanning(true)
        }}
        onPointerLeave={() => setHoverX(null)}
        onPointerMove={event => {
          if (panRef.current) return
          const bounds = event.currentTarget.getBoundingClientRect()
          setHoverX(clamp(event.clientX - bounds.left, 0, bounds.width))
        }}
      >
        <div
          className="relative"
          style={{
            height: `${sceneHeight}px`,
            width: `${sceneWidth}px`
          }}
        >
          <canvas
            ref={canvasRef}
            className="pointer-events-none absolute left-0 top-0"
            style={{
              height: `${viewport.clientHeight}px`,
              transform: `translate(${viewport.scrollLeft}px, ${viewport.scrollTop}px)`,
              width: `${viewport.clientWidth}px`
            }}
          />
        </div>
      </div>
    </div>
  )
}

function minimapSpanColor({
  error,
  kind,
  selected
}: {
  error: boolean
  kind: FlameKind
  selected: boolean
}) {
  if (selected) return 'rgba(255,255,255,1)'
  if (error) return 'rgba(248,113,113,0.9)'
  switch (kind) {
    case 'event':
      return 'rgba(255,255,255,0.2)'
    case 'run':
      return 'rgba(255,255,255,0.25)'
    case 'turn':
      return 'rgba(255,255,255,0.35)'
    case 'model':
      return 'rgba(255,255,255,0.45)'
    case 'tool':
      return 'rgba(255,255,255,0.3)'
  }
}

function mainSpanFill({ error, kind, selected }: { error: boolean; kind: FlameKind; selected: boolean }) {
  if (selected) return error ? 'rgba(127,29,29,0.75)' : 'rgba(255,255,255,0.22)'
  if (error) return 'rgba(127,29,29,0.62)'
  switch (kind) {
    case 'event':
      return 'rgba(255,255,255,0.10)'
    case 'run':
      return 'rgba(255,255,255,0.08)'
    case 'turn':
      return 'rgba(255,255,255,0.12)'
    case 'model':
      return 'rgba(255,255,255,0.16)'
    case 'tool':
      return 'rgba(255,255,255,0.10)'
  }
}

function mainSpanStroke({ error, selected }: { error: boolean; selected: boolean }) {
  if (selected) return error ? 'rgba(248,113,113,0.95)' : 'rgba(255,255,255,0.9)'
  return error ? 'rgba(248,113,113,0.85)' : 'rgba(64,64,64,0.95)'
}

function truncateJson(data: JsonObject, maxLen = 120): string {
  const s = JSON.stringify(data)
  return s.length <= maxLen ? s : `${s.slice(0, maxLen)}…`
}

function eventGridTemplate(columns: string[]) {
  return columns.map(col =>
    col === 'timestamp' ? '10.75rem'
    : col === 'data' ? 'minmax(10rem,2fr)'
    : 'minmax(6rem,1fr)'
  ).join(' ')
}

function renderEventCell(event: TraceEvent, column: string) {
  if (column === 'timestamp') return formatClockUs(event.createdAt)
  if (column === 'data') return truncateJson(event.data)
  return summarizeValue(fieldPathValue(event.data, column))
}

function EventPanel({
  anchorIndex,
  events,
  emptyLabel,
  fields,
  hasMore,
  hasPrevious,
  highlightedEventIds,
  loading,
  loadingAnchor,
  loadingMore,
  loadingPrevious,
  scrollStateKey,
  selectedColumns,
  selectedEventAlign,
  selectedEventId,
  onInspect,
  onLoadMore,
  onLoadPrevious,
  onSetColumns,
  onToggleColumn
}: {
  anchorIndex: number
  events: TraceEvent[]
  emptyLabel: string
  fields: LogField[]
  hasMore: boolean
  hasPrevious: boolean
  highlightedEventIds: string[]
  loading: boolean
  loadingAnchor: boolean
  loadingMore: boolean
  loadingPrevious: boolean
  scrollStateKey: string
  selectedColumns: string[]
  selectedEventAlign: 'auto' | 'center'
  selectedEventId: string
  onInspect: (event: TraceEvent) => void
  onLoadMore: () => void
  onLoadPrevious: () => Promise<void>
  onSetColumns: (paths: string[]) => void
  onToggleColumn: (path: string) => void
}) {
  const [columnsOpen, setColumnsOpen] = useState(false)
  const [savedScrollTop, setSavedScrollTop] = useIndexedDbState({
    initialValue: 0,
    key: scrollStateKey
  })
  const popoverRef = useRef<HTMLDivElement | null>(null)
  const scrollRef = useRef<HTMLDivElement | null>(null)
  const loadMoreEventCountRef = useRef(0)
  const loadPreviousEventCountRef = useRef(0)
  const anchoredScrollKeyRef = useRef('')
  const selectedScrollKeyRef = useRef('')
  const scrollSaveTimeoutRef = useRef<number | null>(null)
  const highlightedEventIdSet = new Set(highlightedEventIds)
  const selectedColumnSet = new Set(selectedColumns)
  const gridTemplateColumns = eventGridTemplate(selectedColumns)
  const allFields = useMemo(() => {
    const fieldPaths = new Set(fields.map(f => f.path))
    const synthetic: LogField[] = []
    if (!fieldPaths.has('timestamp')) synthetic.push({ path: 'timestamp', count: events.length, types: ['string'] })
    if (!fieldPaths.has('data')) synthetic.push({ path: 'data', count: events.length, types: ['object'] })
    return [...synthetic, ...fields]
  }, [fields, events.length])
  const virtualizer = useVirtualizer({
    count: events.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 37,
    overscan: 100
  })

  useEffect(() => {
    if (!selectedEventId) {
      selectedScrollKeyRef.current = ''
      return
    }
    const index = events.findIndex(event => event.id === selectedEventId)
    if (index === -1) {
      return
    }
    const key = `${scrollStateKey}\u0000${selectedEventId}\u0000${selectedEventAlign}`
    if (selectedScrollKeyRef.current === key) {
      return
    }
    selectedScrollKeyRef.current = key
    virtualizer.scrollToIndex(index, { align: selectedEventAlign })
  }, [events, scrollStateKey, selectedEventAlign, selectedEventId, virtualizer])

  function loadMoreIfNeeded(element: HTMLElement | null) {
    if (!element || !hasMore || loadingMore || loadMoreEventCountRef.current === events.length) {
      return
    }
    if (element.scrollHeight - element.scrollTop - element.clientHeight > 800) {
      return
    }
    loadMoreEventCountRef.current = events.length
    onLoadMore()
  }

  function loadPreviousIfNeeded(element: HTMLElement | null) {
    if (!element || !hasPrevious || loadingPrevious || loadPreviousEventCountRef.current === events.length || element.scrollTop > 800) {
      return
    }
    loadPreviousEventCountRef.current = events.length
    const previousHeight = element.scrollHeight
    void onLoadPrevious().then(() => {
      requestAnimationFrame(() => {
        if (scrollRef.current) {
          scrollRef.current.scrollTop += scrollRef.current.scrollHeight - previousHeight
        }
      })
    })
  }

  useEffect(() => {
    loadMoreIfNeeded(scrollRef.current)
  }, [events.length, hasMore, hasPrevious, loadingMore, loadingPrevious])

  useEffect(() => {
    const element = scrollRef.current
    if (!element) return
    element.scrollTop = savedScrollTop
  }, [savedScrollTop, scrollStateKey])

  useEffect(() => {
    if (selectedEventId || anchoredScrollKeyRef.current === scrollStateKey || events.length === 0) {
      return
    }
    anchoredScrollKeyRef.current = scrollStateKey
    virtualizer.scrollToIndex(clamp(anchorIndex, 0, events.length - 1), { align: 'center' })
  }, [anchorIndex, events.length, scrollStateKey, selectedEventId, virtualizer])

  useEffect(() => {
    return () => {
      if (scrollSaveTimeoutRef.current !== null) {
        window.clearTimeout(scrollSaveTimeoutRef.current)
      }
    }
  }, [])

  useEffect(() => {
    if (!columnsOpen) return
    const onClick = (e: MouseEvent) => {
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) {
        setColumnsOpen(false)
      }
    }
    window.addEventListener('pointerdown', onClick)
    return () => window.removeEventListener('pointerdown', onClick)
  }, [columnsOpen])

  return (
    <section className="flex min-h-0 flex-1 flex-col bg-neutral-950">
      <div className="flex items-center gap-2 border-b border-neutral-800 px-2 py-1.5">
        <p className="text-[11px] uppercase tracking-[0.08em] text-neutral-500">Events</p>
        {loadingAnchor ? (
          <span className="inline-flex items-center gap-1.5 text-[11px] text-neutral-500">
            <span className="h-2.5 w-2.5 animate-spin rounded-full border border-neutral-700 border-t-neutral-300" />
            Loading selection
          </span>
        ) : null}
        <div className="relative ml-auto">
          <button
            className={cn(
              'inline-flex items-center gap-1.5 px-1.5 py-0.5 text-[11px] text-neutral-400 hover:text-white',
              columnsOpen && 'text-white'
            )}
            type="button"
            onClick={() => setColumnsOpen(prev => !prev)}
          >
            <Columns3 size={13} strokeWidth={1.6} />
            <span>{selectedColumns.length}/{allFields.length}</span>
          </button>
          {columnsOpen && (
            <div
              ref={popoverRef}
              className="absolute right-0 top-full z-50 mt-1 w-56 border border-neutral-700 bg-neutral-900 shadow-xl"
            >
              <div className="flex items-center justify-between border-b border-neutral-800 px-2 py-1.5">
                <span className="text-[11px] text-neutral-400">Columns</span>
                <button
                  className="px-1.5 py-0.5 text-[10px] text-neutral-500 hover:text-white disabled:text-neutral-700"
                  disabled={selectedColumns.length === 0}
                  type="button"
                  onClick={() => onSetColumns([])}
                >
                  clear
                </button>
              </div>
              <div className="max-h-64 overflow-y-auto py-1">
                {allFields.map(field => (
                  <button
                    key={field.path}
                    className={cn(
                      'flex w-full items-center justify-between px-2.5 py-1 text-left text-[12px] hover:bg-white/[0.05]',
                      selectedColumnSet.has(field.path) ? 'text-white' : 'text-neutral-500'
                    )}
                    type="button"
                    onClick={() => onToggleColumn(field.path)}
                  >
                    <span>{field.path}</span>
                    <span className="text-[10px] text-neutral-600">{field.count}</span>
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-x-auto">
        <div className="flex h-full min-w-[640px] flex-col">
          <div
            className="grid shrink-0 gap-3 border-b border-neutral-800 bg-neutral-950 px-3 py-2 text-[10px] uppercase tracking-[0.08em] text-neutral-500"
            style={{ gridTemplateColumns }}
          >
            {selectedColumns.map(path => (
              <span key={path} className="truncate">{path}</span>
            ))}
          </div>
          <div
            ref={scrollRef}
            className="min-h-0 flex-1 overflow-y-auto overflow-x-hidden overscroll-contain"
            onScroll={event => {
              loadPreviousIfNeeded(event.currentTarget)
              loadMoreIfNeeded(event.currentTarget)
              const nextScrollTop = Math.round(event.currentTarget.scrollTop)
              if (scrollSaveTimeoutRef.current !== null) {
                window.clearTimeout(scrollSaveTimeoutRef.current)
              }
              scrollSaveTimeoutRef.current = window.setTimeout(() => {
                setSavedScrollTop(nextScrollTop)
              }, 120)
            }}
            style={{
              scrollbarColor: '#737373 transparent',
              scrollbarGutter: 'stable',
              scrollbarWidth: 'thin'
            }}
          >
            {loadingPrevious ? <div className="px-3 py-2 text-[11px] text-neutral-500">Loading previous events.</div> : null}
            <div className="relative" style={{ height: `${virtualizer.getTotalSize()}px` }}>
              {virtualizer.getVirtualItems().map(virtualRow => {
                const event = events[virtualRow.index]
                if (!event) return null
                const error = isErrorEvent(event)
                return (
                  <button
                    key={`${event.id}-${event.createdAt}`}
                    className={cn(
                      'absolute left-0 top-0 grid w-full gap-3 border-b border-neutral-900 px-3 py-2 text-left text-[13px] leading-5 hover:bg-white/[0.03]',
                      error && 'border-l-2 border-l-red-400 bg-red-950/25 ring-1 ring-inset ring-red-500/35 hover:bg-red-950/35',
                      highlightedEventIdSet.has(event.id) && 'bg-white/[0.05]',
                      event.id === selectedEventId && 'bg-white/[0.1] ring-1 ring-inset ring-white/40'
                    )}
                    data-index={virtualRow.index}
                    data-trace-event-id={event.id}
                    ref={virtualizer.measureElement}
                    style={{
                      gridTemplateColumns,
                      transform: `translateY(${virtualRow.start}px)`
                    }}
                    type="button"
                    onClick={() => onInspect(event)}
                  >
                    {selectedColumns.map(path => (
                      <span
                        key={path}
                        className={cn(
                          'truncate',
                          (path === 'timestamp' || path === 'data' || path === 'traceId')
                            ? 'font-mono text-[12px] text-neutral-500'
                            : 'text-neutral-300'
                        )}
                      >
                        {renderEventCell(event, path)}
                      </span>
                    ))}
                  </button>
                )
              })}
            </div>
          </div>
          {loading && events.length === 0 ? <EmptyState label="Loading events." /> : null}
          {!loading && events.length === 0 ? <EmptyState label={emptyLabel} /> : null}
          {loadingMore ? <div className="px-3 py-2 text-[11px] text-neutral-500">Loading more events.</div> : null}
        </div>
      </div>
    </section>
  )
}

function JsonTree({
  name,
  value,
  depth = 0
}: {
  name: string
  value: JsonValue
  depth?: number
}) {
  const indent = { paddingLeft: depth * 14 }

  if (value === null || typeof value === 'boolean' || typeof value === 'number') {
    return (
      <div className="flex min-h-6 items-start gap-2 py-0.5" style={indent}>
        <span className="shrink-0 text-neutral-300">{name}</span>
        <span className="shrink-0 text-neutral-500">:</span>
        <span>{formatJsonScalar({ name, value })}</span>
      </div>
    )
  }

  if (typeof value === 'string') {
    return (
      <div className="flex min-h-6 items-start gap-2 py-0.5" style={indent}>
        <span className="shrink-0 text-neutral-300">{name}</span>
        <span className="shrink-0 text-neutral-500">:</span>
        <span className="break-all whitespace-pre-wrap">{value}</span>
      </div>
    )
  }

  if (Array.isArray(value)) {
    return (
      <details className="m-0" open>
        <summary className="flex cursor-pointer items-start gap-2 py-0.5" style={indent}>
          <span className="shrink-0 text-neutral-300">{name}</span>
          <span className="text-neutral-500">[{value.length}]</span>
        </summary>
        {value.length === 0 ? (
          <div className="flex min-h-6 items-start gap-2 py-0.5 text-neutral-500" style={{ paddingLeft: (depth + 1) * 14 }}>
            empty
          </div>
        ) : (
          value.map((item, index) => (
            <JsonTree key={index} name={String(index)} value={item} depth={depth + 1} />
          ))
        )}
      </details>
    )
  }

  const entries = Object.entries(value)
  return (
    <details className="m-0" open>
      <summary className="flex cursor-pointer items-start gap-2 py-0.5" style={indent}>
        <span className="shrink-0 text-neutral-300">{name}</span>
        <span className="text-neutral-500">{`{${entries.length}}`}</span>
      </summary>
      {entries.length === 0 ? (
        <div className="flex min-h-6 items-start gap-2 py-0.5 text-neutral-500" style={{ paddingLeft: (depth + 1) * 14 }}>
          empty
        </div>
      ) : (
        entries.flatMap(([key, item]) =>
          item === undefined ? [] : [<JsonTree key={key} name={key} value={item} depth={depth + 1} />],
        )
      )}
    </details>
  )
}

function FilteredJsonTree({
  depth = 0,
  node
}: {
  depth?: number
  node: JsonTreeNode
}) {
  const indent = { paddingLeft: depth * 14 }

  if (node.type === 'null' || node.type === 'boolean' || node.type === 'number') {
    return (
      <div className="flex min-h-6 items-start gap-2 py-0.5" style={indent}>
        <span className="shrink-0 text-neutral-300">{node.name}</span>
        <span className="shrink-0 text-neutral-500">:</span>
        <span>{formatJsonScalar({ name: node.name, value: node.value })}</span>
      </div>
    )
  }

  if (node.type === 'string') {
    return (
      <div className="flex min-h-6 items-start gap-2 py-0.5" style={indent}>
        <span className="shrink-0 text-neutral-300">{node.name}</span>
        <span className="shrink-0 text-neutral-500">:</span>
        <span className="break-all whitespace-pre-wrap">{node.value}</span>
      </div>
    )
  }

  if (node.type === 'array') {
    return (
      <details className="m-0" open>
        <summary className="flex cursor-pointer items-start gap-2 py-0.5" style={indent}>
          <span className="shrink-0 text-neutral-300">{node.name}</span>
          <span className="text-neutral-500">[{node.length}]</span>
        </summary>
        {node.items.length === 0 ? (
          <div className="flex min-h-6 items-start gap-2 py-0.5 text-neutral-500" style={{ paddingLeft: (depth + 1) * 14 }}>
            empty
          </div>
        ) : (
          node.items.map((item, index) => (
            <FilteredJsonTree key={`${item.name}-${index}`} depth={depth + 1} node={item} />
          ))
        )}
      </details>
    )
  }

  return (
    <details className="m-0" open>
      <summary className="flex cursor-pointer items-start gap-2 py-0.5" style={indent}>
        <span className="shrink-0 text-neutral-300">{node.name}</span>
        <span className="text-neutral-500">{`{${node.size}}`}</span>
      </summary>
      {node.entries.length === 0 ? (
        <div className="flex min-h-6 items-start gap-2 py-0.5 text-neutral-500" style={{ paddingLeft: (depth + 1) * 14 }}>
          empty
        </div>
      ) : (
        node.entries.map(entry => (
          <FilteredJsonTree key={`${entry.name}-${entry.type}`} depth={depth + 1} node={entry} />
        ))
      )}
    </details>
  )
}

function buildFilteredJsonTree({
  filter,
  includeAllDescendants = false,
  isRoot = false,
  name,
  value
}: {
  filter: string
  includeAllDescendants?: boolean
  isRoot?: boolean
  name: string
  value: JsonValue
}): JsonTreeNode | null {
  const type = jsonTreeNodeType(value)
  const queryMatches = filter === '' || name.toLowerCase().includes(filter) || String(value).toLowerCase().includes(filter)
  const keepAllDescendants = includeAllDescendants || (!isRoot && queryMatches)

  if (value === null || typeof value === 'boolean' || typeof value === 'number' || typeof value === 'string') {
    if (!keepAllDescendants && !queryMatches) {
      return null
    }

    return {
      name,
      type,
      value
    } as JsonTreeNode
  }

  if (Array.isArray(value)) {
    const items = value.flatMap((item, index) => {
      const child = buildFilteredJsonTree({
        filter,
        includeAllDescendants: keepAllDescendants,
        name: String(index),
        value: item
      })
      return child ? [child] : []
    })

    if (!keepAllDescendants && !isRoot && !queryMatches && items.length === 0) {
      return null
    }
    if (isRoot && items.length === 0) {
      return null
    }

    return {
      items,
      length: value.length,
      name,
      type: 'array'
    }
  }

  const entries = Object.entries(value).flatMap(([key, item]) => {
    if (item === undefined) {
      return []
    }

    const child = buildFilteredJsonTree({
      filter,
      includeAllDescendants: keepAllDescendants,
      name: key,
      value: item
    })
    return child ? [child] : []
  })

  if (!keepAllDescendants && !isRoot && !queryMatches && entries.length === 0) {
    return null
  }
  if (isRoot && entries.length === 0) {
    return null
  }

  return {
    entries,
    name,
    size: Object.keys(value).length,
    type: 'object'
  }
}

function jsonTreeNodeType(value: JsonValue): JsonTreeNode['type'] {
  if (value === null) {
    return 'null'
  }
  if (Array.isArray(value)) {
    return 'array'
  }
  if (typeof value === 'boolean') {
    return 'boolean'
  }
  if (typeof value === 'number') {
    return 'number'
  }
  if (typeof value === 'string') {
    return 'string'
  }
  return 'object'
}

function buildFlamegraph(events: TraceEvent[]): Flamegraph {
  const latestEventMs = Math.max(...events.map(event => traceTimeMs(event.createdAt)).filter(Number.isFinite), 0)
  const spanCandidates = new Map<
    string,
    {
      eventIds: string[]
      id: string
      label: string
      kind: FlameKind
      parentSpanId?: string
      start: string
      end?: string
      payload: JsonObject
    }
  >()
  const eventSpanIds: Record<string, string> = {}
  const eventCreatedAt = Object.fromEntries(events.map(event => [event.id, event.createdAt]))
  const consumed = new Set<string>()

  for (const event of events) {
    const type = stringField(event.data.type)
    if (type === 'span_start' || type === 'span_end') {
      const id = stringField(event.data.spanId) || event.id
      const current = spanCandidates.get(id)
      const nextStart = stringField(event.data.startedAt) || event.createdAt
      const nextEnd = type === 'span_end' ? stringField(event.data.endedAt) || undefined : undefined
      const payload = type === 'span_end' || !current ? event.data : current.payload
      spanCandidates.set(id, {
        eventIds: [...(current?.eventIds ?? []), event.id],
        id,
        label: eventName(payload),
        kind: spanKind(payload),
        parentSpanId: stringField(event.data.parentSpanId) || current?.parentSpanId,
        start:
          !current || traceTimeMs(nextStart) < traceTimeMs(current.start)
            ? nextStart
            : current.start,
        end: nextEnd ?? current?.end,
        payload
      })
      consumed.add(event.id)
      eventSpanIds[event.id] = id
    }
  }

  const spans: FlameSpan[] = []
  for (const span of spanCandidates.values()) {
    pushSpan(spans, {
      ...span,
      end: span.end ?? new Date(latestEventMs).toISOString()
    })
  }

  for (const event of events) {
    if (consumed.has(event.id)) {
      continue
    }

    const at = traceTimeMs(event.createdAt)
    if (!Number.isFinite(at)) {
      continue
    }

    spans.push({
      eventIds: [event.id],
      id: event.id,
      label: eventName(event.data),
      kind: 'event',
      parentSpanId: stringField(event.data.parentSpanId),
      startMs: at,
      endMs: at + 1,
      lane: 0,
      payload: event.data
    })
  }

  if (spans.length === 0) {
    return { eventCreatedAt, eventSpanIds, minStart: 0, maxEnd: 0, totalDuration: 0, rows: [] }
  }

  spans.sort((a, b) => (a.startMs !== b.startMs ? a.startMs - b.startMs : b.endMs - a.endMs))

  const minStart = Math.min(...spans.map(span => span.startMs))
  const maxEnd = Math.max(...spans.map(span => span.endMs))
  const spansById = new Map(spans.map(span => [span.id, span]))
  const depthCache = new Map<string, number>()
  const rows: FlameSpan[][] = []

  for (const span of spans) {
    const lane = spanDepth({ depthCache, span, spansById })
    span.lane = lane
    rows[lane] ??= []
    rows[lane].push(span)
  }

  return {
    eventSpanIds,
    eventCreatedAt,
    maxEnd,
    minStart,
    totalDuration: Math.max(maxEnd - minStart, 1),
    rows
  }
}

function pushSpan(
  spans: FlameSpan[],
  candidate: {
    eventIds: string[]
    id: string
    label: string
    kind: FlameKind
    parentSpanId?: string
    start: string
    end: string
    payload: JsonObject
  }
) {
  const startMs = traceTimeMs(candidate.start)
  const endMs = traceTimeMs(candidate.end)
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
    return
  }

  spans.push({
    eventIds: candidate.eventIds,
    id: candidate.id,
    label: candidate.label,
    kind: candidate.kind,
    parentSpanId: candidate.parentSpanId,
    startMs,
    endMs,
    lane: 0,
    payload: candidate.payload
  })
}

function spanDepth({
  depthCache,
  seen = new Set<string>(),
  span,
  spansById
}: {
  depthCache: Map<string, number>
  seen?: Set<string>
  span: FlameSpan
  spansById: Map<string, FlameSpan>
}): number {
  const cached = depthCache.get(span.id)
  if (cached !== undefined) {
    return cached
  }
  if (!span.parentSpanId || seen.has(span.id)) {
    depthCache.set(span.id, 0)
    return 0
  }

  const parent = spansById.get(span.parentSpanId)
  if (!parent) {
    depthCache.set(span.id, 0)
    return 0
  }

  seen.add(span.id)
  const depth = spanDepth({ depthCache, seen, span: parent, spansById }) + 1
  depthCache.set(span.id, depth)
  return depth
}

function spanKind(data: JsonObject): FlameKind {
  switch (eventName(data)) {
    case 'run':
    case 'trace':
      return 'run'
    case 'turn':
      return 'turn'
    case 'llm':
      return 'model'
    case 'tool_call':
      return 'tool'
    default:
      return 'event'
  }
}

function stringField(value: JsonValue | undefined) {
  return typeof value === 'string' ? value : ''
}

function objectField(value: JsonValue | undefined) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : null
}

type ClickHouseResponse<T> = {
  data?: T[]
}

type QueryParameters = Record<string, string | number | boolean>

type ResolvedTimeRange = {
  createdAfter?: string
  createdBefore?: string
  key: string
  lookbackMinutes?: number
}

type EventRow = {
  data?: JsonObject
  event_id: string
  event_type?: string
  signal?: string
  span_id?: string
  timestamp: string
  trace_id?: string
}

const defaultTableName = 'observatory.events'
const defaultFacetsTableName = 'observatory.event_facets'
const groupableFields = [
  'traceId',
  'spanId',
  'parentSpanId',
  'type',
  'signal',
  'service',
  'name',
  'tenant_id',
  'environment',
  'host.name',
  'event_type',
  'span_kind',
  'span_status_code',
  'severity_text',
  'metric_name',
  'metric_type',
  'user_id',
  'session_id',
  'account_id'
]

async function fetchGroupOptions({
  apiBaseUrl,
  limit
}: {
  apiBaseUrl: string
  limit: number
}): Promise<{ fields: GroupOption[] }> {
  const response = await postQuery<{
    capped: boolean
    cardinality: number
    path: string
  }>({
    apiBaseUrl,
    parameters: { limit },
    query: [
      'SELECT key AS path',
      ', uniqCombined64(value) AS cardinality',
      ', toBool(0) AS capped',
      `FROM ${facetsTable()}`,
      "WHERE key != ''",
      'GROUP BY path',
      'ORDER BY sum(count) DESC, path ASC',
      'LIMIT {limit:UInt64}'
    ].join(' ')
  })
  const dynamicFields = (response.data ?? []).map(field => ({
    cardinality: Number(field.cardinality) || 0,
    capped: Boolean(field.capped),
    path: displayFacetPath(field.path)
  }))
  if (dynamicFields.length > 0) {
    return { fields: mergeGroupOptions(dynamicFields, limit) }
  }
  return {
    fields: groupableFields.slice(0, limit).map(path => ({
      cardinality: 0,
      capped: false,
      path
    }))
  }
}

async function fetchGroups({
  apiBaseUrl,
  groupBy,
  limit,
  timeRange
}: {
  apiBaseUrl: string
  groupBy: string
  limit: number
  timeRange: ResolvedTimeRange
}): Promise<{ groups: LogGroupSummary[] }> {
  const timeFilter = timeRangeWhereClause(timeRange, 'bucket_time')
  const response = await postQuery<{
    count: number
    durationMs: number
    endedAt: string
    startedAt: string
    value: string
  }>({
    apiBaseUrl,
    parameters: { group_key: facetKey(groupBy), limit, ...timeRangeParameters(timeRange) },
    query: [
      'SELECT value',
      ', min(bucket_time) AS startedAt',
      ', max(bucket_time) AS endedAt',
      ", dateDiff('millisecond', min(bucket_time), max(bucket_time)) AS durationMs",
      ', sum(count) AS count',
      `FROM ${facetsTable()}`,
      `PREWHERE key = {group_key:String}${timeFilter ? ` AND ${timeFilter}` : ''}`,
      "WHERE value != ''",
      'GROUP BY value',
      'ORDER BY endedAt DESC',
      'LIMIT {limit:UInt64}'
    ].join(' ')
  })

  return {
    groups: (response.data ?? []).map(group => ({
      groupBy,
      value: group.value,
      startedAt: normalizeTimestamp(group.startedAt),
      endedAt: normalizeTimestamp(group.endedAt),
      durationMs: Number(group.durationMs) || 0,
      count: Number(group.count) || 0
    }))
  }
}

async function fetchLatest({
  apiBaseUrl,
  groupBy,
  selectedGroupValue
}: {
  apiBaseUrl: string
  groupBy: string
  selectedGroupValue: string
}): Promise<LogLatest> {
  const response = await postQuery<{ lastCreatedAt: string }>({
    apiBaseUrl,
    parameters: { group_key: facetKey(groupBy), group_value: selectedGroupValue },
    query: [
      'SELECT max(bucket_time) AS lastCreatedAt',
      `FROM ${facetsTable()}`,
      'PREWHERE key = {group_key:String}',
      'WHERE value = {group_value:String}'
    ].join(' ')
  })
  return { lastCreatedAt: normalizeTimestamp(response.data?.[0]?.lastCreatedAt) }
}

async function fetchSummary({
  apiBaseUrl,
  eventFilter,
  groupBy,
  selectedGroupValue
}: {
  apiBaseUrl: string
  eventFilter: ParsedEventFilter
  groupBy: string
  selectedGroupValue: string
}): Promise<LogSummary> {
  const filters = eventFilterClauses({ eventFilter, groupBy })
  const response = await postQuery<{ count: number }>({
    apiBaseUrl,
    parameters: eventQueryParameters({ eventFilter, groupBy, selectedGroupValue }),
    query: [
      'SELECT count() AS count',
      `FROM ${eventsTable()} AS e`,
      filters.prewhere.length ? `PREWHERE ${filters.prewhere.join(' AND ')}` : '',
      filters.where.length ? `WHERE ${filters.where.join(' AND ')}` : ''
    ].join(' ')
  })
  const count = Number(response.data?.[0]?.count) || 0
  return { capped: false, count, limit: count }
}

async function fetchEvents({
  apiBaseUrl,
  eventFilter,
  groupBy,
  limit,
  pageParam,
  selectedGroupValue
}: {
  apiBaseUrl: string
  eventFilter: ParsedEventFilter
  groupBy: string
  limit: number
  pageParam: EventPageParam
  selectedGroupValue: string
}): Promise<LogEventsPage> {
  const direction = pageParam.before ? 'before' : 'forward'
  const pageFilter = pageParam.before
    ? "e.timestamp < {cursor:DateTime64(3, 'UTC')}"
    : pageParam.after
      ? "e.timestamp > {cursor:DateTime64(3, 'UTC')}"
      : pageParam.around
        ? "e.timestamp <= {cursor:DateTime64(3, 'UTC')}"
        : ''
  const parameters = {
    ...eventQueryParameters({ eventFilter, groupBy, selectedGroupValue }),
    limit,
    ...(pageParam.before || pageParam.after || pageParam.around
      ? { cursor: clickHouseDateTime64(pageParam.before || pageParam.after || pageParam.around || '') }
      : {})
  }
  const order = direction === 'before' || pageParam.around ? 'DESC' : 'ASC'
  const filters = eventFilterClauses({
    eventFilter,
    groupBy,
    prewhere: pageFilter ? [pageFilter] : []
  })
  const response = await postQuery<EventRow>({
    apiBaseUrl,
    parameters,
    query: [
      'SELECT e.event_id AS event_id, e.timestamp AS timestamp, e.event_type AS event_type, e.signal AS signal, e.trace_id AS trace_id, e.span_id AS span_id, e.data AS data',
      `FROM ${eventsTable()} AS e`,
      filters.prewhere.length ? `PREWHERE ${filters.prewhere.join(' AND ')}` : '',
      'WHERE (e.event_id, e.timestamp) IN (',
      'SELECT event_id, timestamp FROM (',
      'SELECT e.event_id AS event_id, e.timestamp AS timestamp',
      `FROM ${eventsTable()} AS e`,
      filters.prewhere.length ? `PREWHERE ${filters.prewhere.join(' AND ')}` : '',
      filters.where.length ? `WHERE ${filters.where.join(' AND ')}` : '',
      `ORDER BY e.timestamp ${order}, e.event_id ${order}`,
      'LIMIT {limit:UInt64}',
      ')',
      ')',
      `ORDER BY e.timestamp ${order}, e.event_id ${order}`,
    ].join(' ')
  })
  const events = (response.data ?? []).map(rowToTraceEvent)
  if (order === 'DESC') events.reverse()

  return {
    events,
    fields: orderLogFields(inferLogFields(events)),
    group: pageGroupSummary({ events, groupBy, selectedGroupValue }),
    nextCursor: pageParam.after && events.length >= limit ? events[events.length - 1]?.createdAt : undefined,
    prevCursor: events.length >= limit ? events[0]?.createdAt : undefined
  }
}

async function fetchFlamegraph({
  apiBaseUrl,
  eventFilter,
  groupBy,
  maxSpans,
  selectedGroupValue
}: {
  apiBaseUrl: string
  eventFilter: ParsedEventFilter
  groupBy: string
  maxSpans: number
  selectedGroupValue: string
}): Promise<LogFlamegraph> {
  const page = await fetchEvents({
    apiBaseUrl,
    eventFilter,
    groupBy,
    limit: maxSpans,
    pageParam: {},
    selectedGroupValue
  })
  const flamegraph = buildFlamegraph(page.events)
  return {
    ...flamegraph,
    capped: page.events.length >= maxSpans,
    spanCount: flamegraph.rows.reduce((count, row) => count + row.length, 0)
  }
}

async function fetchDensity({
  apiBaseUrl,
  buckets,
  eventFilter,
  groupBy,
  selectedGroupValue
}: {
  apiBaseUrl: string
  buckets: number
  eventFilter: ParsedEventFilter
  groupBy: string
  selectedGroupValue: string
}): Promise<LogDensity> {
  const parameters = eventQueryParameters({ eventFilter, groupBy, selectedGroupValue })
  const filters = eventFilterClauses({ eventFilter, groupBy })
  const range = await postQuery<{ count: number; from: string; to: string }>({
    apiBaseUrl,
    parameters,
    query: [
      'SELECT min(e.timestamp) AS from, max(e.timestamp) AS to, count() AS count',
      `FROM ${eventsTable()} AS e`,
      filters.prewhere.length ? `PREWHERE ${filters.prewhere.join(' AND ')}` : '',
      filters.where.length ? `WHERE ${filters.where.join(' AND ')}` : ''
    ].join(' ')
  })
  const row = range.data?.[0]
  const from = normalizeTimestamp(row?.from)
  const to = normalizeTimestamp(row?.to)
  const fromMs = Date.parse(from)
  const toMs = Date.parse(to)
  if (!Number(row?.count) || !Number.isFinite(fromMs) || !Number.isFinite(toMs)) {
    return { bucketMs: 1, buckets: [], from: '', to: '' }
  }

  const bucketMs = niceTimeInterval(Math.max(1, (toMs - fromMs) / buckets))
  const density = await postQuery<{ bucket: number; count: number; errorCount: number }>({
    apiBaseUrl,
    parameters: { ...parameters, bucket_ms: bucketMs },
    query: [
      'WITH intDiv(toUnixTimestamp64Milli(e.timestamp), {bucket_ms:UInt64}) * {bucket_ms:UInt64} AS bucket',
      'SELECT bucket, count() AS count',
      `, countIf(${errorExpression()}) AS errorCount`,
      `FROM ${eventsTable()} AS e`,
      filters.prewhere.length ? `PREWHERE ${filters.prewhere.join(' AND ')}` : '',
      filters.where.length ? `WHERE ${filters.where.join(' AND ')}` : '',
      'GROUP BY bucket',
      'ORDER BY bucket ASC'
    ].join(' ')
  })

  return {
    bucketMs,
    buckets: (density.data ?? []).map(bucket => ({
      count: Number(bucket.count) || 0,
      errorCount: Number(bucket.errorCount) || 0,
      start: new Date(Number(bucket.bucket)).toISOString()
    })),
    from,
    to
  }
}

async function fetchEvent({
  apiBaseUrl,
  eventId
}: {
  apiBaseUrl: string
  eventId: string
}): Promise<LogEventPayload> {
  try {
    const response = await fetch(eventUrl(apiBaseUrl, eventId), {
      headers: queryHeaders(),
      method: 'GET'
    })
    if (response.ok) {
      const row = await response.json() as EventRow
      return { event: rowToTraceEvent(row) }
    }
  } catch {
    // Fall back to /query. Some local deployments do not have S3 reads configured.
  }

  const response = await postQuery<EventRow>({
    apiBaseUrl,
    parameters: { event_id: eventId },
    query: [
      'SELECT event_id, timestamp, event_type, signal, trace_id, span_id, data',
      `FROM ${eventsTable()}`,
      'WHERE event_id = {event_id:String}',
      'ORDER BY timestamp ASC',
      'LIMIT 1'
    ].join(' ')
  })
  const row = response.data?.[0]
  if (!row) throw new HTTPError({ message: 'event not found', status: 404 })
  return { event: rowToTraceEvent(row) }
}

async function postQuery<T>({
  apiBaseUrl,
  parameters = {},
  query
}: {
  apiBaseUrl: string
  parameters?: QueryParameters
  query: string
}): Promise<ClickHouseResponse<T>> {
  const response = await fetch(queryUrl(apiBaseUrl), {
    body: JSON.stringify({ parameters, query }),
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({
      message: text || response.statusText,
      status: response.status
    })
  }
  return (await response.json()) as ClickHouseResponse<T>
}

function queryUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/query` : '/query'
}

function eventUrl(apiBaseUrl: string, eventId: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  const path = `/events/${encodeURIComponent(eventId)}`
  return base ? `${base}${path}` : path
}

function queryHeaders() {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  const token = import.meta.env.VITE_NANOTRACE_KEY
  if (token) headers.Authorization = `Bearer ${token}`
  return headers
}

function eventsTable() {
  const table = String(import.meta.env.VITE_NANOTRACE_TABLE || defaultTableName).trim()
  return /^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$/.test(table)
    ? table
    : defaultTableName
}

function facetsTable() {
  const configured = String(import.meta.env.VITE_NANOTRACE_FACETS_TABLE || '').trim()
  if (/^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$/.test(configured)) {
    return configured
  }
  const table = eventsTable()
  const database = table.includes('.') ? table.split('.')[0] : ''
  const derived = database ? `${database}.event_facets` : 'event_facets'
  return /^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$/.test(derived)
    ? derived
    : defaultFacetsTableName
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
    return {
      createdAfter,
      createdBefore,
      key: `custom:${createdAfter}:${createdBefore}`
    }
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

function timeRangeParameters(range: ResolvedTimeRange): QueryParameters {
  return {
    ...(range.lookbackMinutes ? { lookback_minutes: range.lookbackMinutes } : {}),
    ...(range.createdAfter ? { created_after: clickHouseDateTime64(range.createdAfter) } : {}),
    ...(range.createdBefore ? { created_before: clickHouseDateTime64(range.createdBefore) } : {})
  }
}

function nullableStringExpression(path: string) {
  const column = promotedStringColumn(path)
  if (column) return `ifNull(toString(nullIf(${column}, '')), '')`
  return `ifNull(toString(${jsonFieldExpression(path)}), '')`
}

function jsonFieldExpression(path: string, alias = '') {
  const normalized = nanotracePath(path)
  if (!/^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$/.test(normalized)) {
    throw new Error(`Unsupported field path: ${path}`)
  }
  return `getSubcolumn(${alias ? `${alias}.` : ''}data, '${sqlString(normalized)}')`
}

function nanotracePath(path: string) {
  switch (path) {
    case 'traceId':
      return 'trace_id'
    case 'spanId':
      return 'span_id'
    case 'parentSpanId':
      return 'parent_span_id'
    case 'startedAt':
      return 'start_time'
    case 'endedAt':
      return 'end_time'
    case 'durationMs':
      return 'duration_ms'
    case 'type':
      return 'event_type'
    default:
      return normalizedPayloadPath(path)
  }
}

function facetKey(path: string) {
  return nanotracePath(path)
}

function displayFacetPath(path: string) {
  switch (path) {
    case 'trace_id':
      return 'traceId'
    case 'span_id':
      return 'spanId'
    case 'parent_span_id':
      return 'parentSpanId'
    case 'start_time':
      return 'startedAt'
    case 'end_time':
      return 'endedAt'
    case 'duration_ms':
      return 'durationMs'
    case 'event_type':
      return 'type'
    default:
      return path
  }
}

function mergeGroupOptions(fields: GroupOption[], limit: number) {
  const seen = new Set<string>()
  const merged: GroupOption[] = []
  for (const option of [...fields, ...groupableFields.map(path => ({ cardinality: 0, capped: false, path }))]) {
    if (seen.has(option.path)) continue
    seen.add(option.path)
    merged.push(option)
    if (merged.length >= limit) break
  }
  return merged
}

function eventFilterClauses({
  eventFilter,
  groupBy,
  prewhere = []
}: {
  eventFilter: ParsedEventFilter
  groupBy: string
  prewhere?: string[]
}) {
  const exactColumn = promotedStringColumn(groupBy, 'e')
  const valueExpression = exactColumn ? '' : eventValueExpression(groupBy)
  const clauses = {
    prewhere: [...prewhere],
    where: [] as string[]
  }
  if (exactColumn) {
    clauses.prewhere.unshift(`${exactColumn} = {group_value:String}`)
  } else {
    clauses.where.push(
      `(${valueExpression} = {group_value:String} OR position(${valueExpression}, concat(char(39), {group_value:String}, char(39))) > 0)`
    )
  }
  if (eventFilter.createdAfter) clauses.prewhere.push("e.timestamp >= {created_after:DateTime64(3, 'UTC')}")
  if (eventFilter.createdBefore) clauses.prewhere.push("e.timestamp <= {created_before:DateTime64(3, 'UTC')}")
  const textFilter = eventTextWhereClause(eventFilter)
  if (textFilter) clauses.where.push(textFilter)
  return clauses
}

function eventValueExpression(path: string) {
  const column = promotedStringColumn(path, 'e')
  if (column) return `ifNull(toString(nullIf(${column}, '')), '')`
  return `ifNull(toString(${jsonFieldExpression(path, 'e')}), '')`
}

function promotedStringColumn(path: string, alias = '') {
  const prefix = alias ? `${alias}.` : ''
  switch (nanotracePath(path)) {
    case 'tenant_id':
      return `${prefix}tenant_id`
    case 'trace_id':
      return `${prefix}trace_id`
    case 'span_id':
      return `${prefix}span_id`
    case 'event_type':
      return `${prefix}event_type`
    case 'signal':
      return `${prefix}signal`
    default:
      return ''
  }
}

function eventTextWhereClause(eventFilter: ParsedEventFilter) {
  return eventFilter.text
    ? "(positionCaseInsensitive(toJSONString(e.data), {event_filter:String}) > 0 OR positionCaseInsensitive(e.event_id, {event_filter:String}) > 0)"
    : ''
}

function eventQueryParameters({
  eventFilter,
  groupBy,
  selectedGroupValue
}: {
  eventFilter: ParsedEventFilter
  groupBy: string
  selectedGroupValue: string
}): QueryParameters {
  return {
    group_key: facetKey(groupBy),
    group_value: selectedGroupValue,
    ...(eventFilter.createdAfter ? { created_after: clickHouseDateTime64(eventFilter.createdAfter) } : {}),
    ...(eventFilter.createdBefore ? { created_before: clickHouseDateTime64(eventFilter.createdBefore) } : {}),
    ...(eventFilter.text ? { event_filter: eventFilter.text } : {})
  }
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

function errorExpression() {
  return [
    "lowerUTF8(ifNull(toString(getSubcolumn(e.data, 'is_error')), '')) IN ('1', 'true')",
    "lowerUTF8(ifNull(toString(getSubcolumn(e.data, 'span_status_code')), '')) = 'error'",
    "endsWith(lowerUTF8(ifNull(toString(getSubcolumn(e.data, 'event_type')), '')), '_error')"
  ].join(' OR ')
}

function rowToTraceEvent(row: EventRow): TraceEvent {
  const data = normalizeEventData(row)
  return {
    id: String(row.event_id),
    createdAt: normalizeTimestamp(row.timestamp),
    data
  }
}

function pageGroupSummary({
  events,
  groupBy,
  selectedGroupValue
}: {
  events: TraceEvent[]
  groupBy: string
  selectedGroupValue: string
}): LogGroupSummary {
  const startedAt = events[0]?.createdAt
  const endedAt = events[events.length - 1]?.createdAt
  const startedMs = startedAt ? Date.parse(startedAt) : Number.NaN
  const endedMs = endedAt ? Date.parse(endedAt) : Number.NaN

  return {
    groupBy,
    value: selectedGroupValue,
    startedAt,
    endedAt,
    durationMs: Number.isFinite(startedMs) && Number.isFinite(endedMs) ? Math.max(endedMs - startedMs, 0) : 0,
    count: events.length,
    errorCount: events.filter(isErrorEvent).length
  }
}

function normalizeEventData(row: EventRow): JsonObject {
  const data = cleanJsonObject(row.data)
  const eventType = stringField(data.event_type) || row.event_type || stringField(data.type)
  const traceId = stringField(data.trace_id) || row.trace_id || stringField(data.traceId)
  const spanId = stringField(data.span_id) || row.span_id || stringField(data.spanId)
  const parentSpanId = stringField(data.parent_span_id) || stringField(data.parentSpanId)
  const startedAt = stringField(data.start_time) || stringField(data.span_start_time) || stringField(data.startedAt)
  const endedAt = stringField(data.end_time) || stringField(data.span_end_time) || stringField(data.endedAt)
  const durationMs = typeof data.duration_ms === 'number' ? data.duration_ms : data.durationMs

  return {
    ...data,
    ...(eventType ? { type: eventType, event_type: eventType } : {}),
    ...(row.signal ? { signal: row.signal } : {}),
    ...(traceId ? { traceId, trace_id: traceId } : {}),
    ...(spanId ? { spanId, span_id: spanId } : {}),
    ...(parentSpanId ? { parentSpanId, parent_span_id: parentSpanId } : {}),
    ...(startedAt ? { startedAt, start_time: startedAt } : {}),
    ...(endedAt ? { endedAt, end_time: endedAt } : {}),
    ...(typeof durationMs === 'number' ? { durationMs, duration_ms: durationMs } : {})
  }
}

function cleanJsonObject(value: unknown): JsonObject {
  const cleaned = pruneNullishJson(value)
  return cleaned && typeof cleaned === 'object' && !Array.isArray(cleaned) ? cleaned as JsonObject : {}
}

function pruneNullishJson(value: unknown): JsonValue | undefined {
  if (value === null || value === undefined) return undefined
  if (Array.isArray(value)) {
    const items = value.flatMap(item => {
      const cleaned = pruneNullishJson(item)
      return cleaned === undefined ? [] : [cleaned]
    })
    return items.length > 0 ? items : undefined
  }
  if (typeof value === 'object') {
    const entries = Object.entries(value).flatMap(([key, child]) => {
      const cleaned = pruneNullishJson(child)
      return cleaned === undefined ? [] : [[key, cleaned] as const]
    })
    if (entries.length === 0) return undefined
    return Object.fromEntries(entries) as JsonObject
  }
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return value
  return undefined
}

function normalizeTimestamp(value: unknown) {
  if (typeof value !== 'string' || !value) return ''
  return value.includes('T') ? value : value.replace(' ', 'T') + 'Z'
}

function compactObject(values: Record<string, string | undefined>): JsonObject {
  return Object.fromEntries(Object.entries(values).filter(([, value]) => value)) as JsonObject
}

function sqlString(value: string) {
  return value.replace(/\\/g, '\\\\').replace(/'/g, "\\'")
}

class HTTPError extends Error {
  status: number

  constructor({ message, status }: { message: string; status: number }) {
    super(`${status} ${message}`)
    this.name = 'HTTPError'
    this.status = status
  }
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : error ? String(error) : ''
}

function formatDateTimeUs(value?: string) {
  if (!value) {
    return 'unknown'
  }

  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) {
    return 'unknown'
  }

  return `${pad2(date.getMonth() + 1)}/${pad2(date.getDate())}/${date.getFullYear()}, ${formatClockParts(date, value)}`
}

function formatClockUs(value?: string) {
  if (!value) {
    return 'unknown'
  }

  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) {
    return 'unknown'
  }

  return `${monthLabel(date.getMonth())} ${pad2(date.getDate())} ${formatClockParts(date, value)}`
}

function formatClockMsFromMs(value: number) {
  if (!Number.isFinite(value) || value <= 0) {
    return 'unknown'
  }

  return new Date(value).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
    fractionalSecondDigits: 3
  })
}

function niceTimeInterval(targetMs: number) {
  const intervals = [
    1, 2, 5, 10, 20, 50,
    100, 200, 500,
    1_000, 2_000, 5_000, 10_000, 15_000, 30_000,
    60_000, 2 * 60_000, 5 * 60_000, 10 * 60_000, 15 * 60_000, 30 * 60_000,
    60 * 60_000, 2 * 60 * 60_000, 6 * 60 * 60_000, 12 * 60 * 60_000,
    24 * 60 * 60_000, 7 * 24 * 60 * 60_000
  ]
  return intervals.find(interval => interval >= targetMs) ?? intervals[intervals.length - 1]!
}

function formatAxisTick(valueMs: number, intervalMs: number) {
  const date = new Date(valueMs)
  if (intervalMs < 60_000) {
    return `${monthLabel(date.getMonth())} ${pad2(date.getDate())} ${pad2(date.getHours())}:${pad2(date.getMinutes())}:${pad2(date.getSeconds())}.${String(date.getMilliseconds()).padStart(3, '0')}`
  }
  if (intervalMs < 60 * 60_000) {
    return `${monthLabel(date.getMonth())} ${pad2(date.getDate())} ${pad2(date.getHours())}:${pad2(date.getMinutes())}:${pad2(date.getSeconds())}`
  }
  return `${monthLabel(date.getMonth())} ${pad2(date.getDate())} ${pad2(date.getHours())}:${pad2(date.getMinutes())}`
}

function formatAxisHover(valueMs: number) {
  const date = new Date(valueMs)
  return `${monthLabel(date.getMonth())} ${pad2(date.getDate())} ${pad2(date.getHours())}:${pad2(date.getMinutes())}:${pad2(date.getSeconds())}.${String(date.getMilliseconds()).padStart(3, '0')}`
}

function formatJsonScalar({ name, value }: { name: string; value: boolean | number | null }) {
  return typeof value === 'number' && /(?:^|\.)(?:durationMs|duration_ms)$/.test(name)
    ? formatDurationMs(value)
    : String(value)
}

function formatDurationMs(value?: number) {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    return 'n/a'
  }
  if (value < 1000) {
    return `${Math.round(value)} ms`
  }

  const totalSeconds = Math.round(value / 1000)
  if (totalSeconds < 60) {
    return value < 10000 ? `${(value / 1000).toFixed(1)} s` : `${totalSeconds} s`
  }

  const units = [
    { label: 'd', value: Math.floor(totalSeconds / 86400) },
    { label: 'h', value: Math.floor((totalSeconds % 86400) / 3600) },
    { label: 'm', value: Math.floor((totalSeconds % 3600) / 60) },
    { label: 's', value: totalSeconds % 60 }
  ].filter(unit => unit.value > 0)

  return units.slice(0, 2).map(unit => `${unit.value}${unit.label}`).join(' ')
}

function traceTimeMs(value: string) {
  const parsed = Date.parse(value)
  if (!Number.isFinite(parsed)) {
    return Number.NaN
  }
  return Math.floor(parsed / 1000) * 1000 + traceFractionMs(value)
}

function traceFractionMs(value: string) {
  const match = value.match(/\.(\d+)(?:Z|[+-]\d\d:?\d\d)?$/)
  return match ? Number(`0.${match[1]}`) * 1000 : 0
}

function formatClockParts(date: Date, raw: string) {
  return `${pad2(date.getHours())}:${pad2(date.getMinutes())}:${pad2(date.getSeconds())}.${traceFractionDigits(raw, date)}`
}

function traceFractionDigits(value: string, date: Date) {
  const match = value.match(/\.(\d+)(?:Z|[+-]\d\d:?\d\d)?$/)
  return (match?.[1] ?? String(date.getMilliseconds()).padStart(3, '0')).padEnd(6, '0').slice(0, 6)
}

function monthLabel(monthIndex: number) {
  return ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][monthIndex] ?? '???'
}

function pad2(value: number) {
  return String(value).padStart(2, '0')
}

function eventName(data: JsonObject) {
  return stringField(data.name) || stringField(data.type) || 'event'
}

function isErrorEvent(event: TraceEvent) {
  return isErrorPayload(event.data)
}

function isErrorPayload(data: JsonObject) {
  return Boolean(data.error) || eventName(data).endsWith('_error')
}

type ParsedEventFilter = {
  createdAfter?: string
  createdBefore?: string
  text: string
}

function defaultTimeRangeFilter({
  lastCreatedAt,
  timeRange
}: {
  lastCreatedAt?: string
  timeRange: ResolvedTimeRange
}): ParsedEventFilter | null {
  if (timeRange.createdAfter || timeRange.createdBefore) {
    return {
      createdAfter: timeRange.createdAfter,
      createdBefore: timeRange.createdBefore,
      text: ''
    }
  }

  if (!lastCreatedAt || !timeRange.lookbackMinutes) return null

  const lastMs = Date.parse(lastCreatedAt)
  if (!Number.isFinite(lastMs)) return null

  return {
    createdAfter: new Date(lastMs - timeRange.lookbackMinutes * 60 * 1000).toISOString(),
    createdBefore: lastCreatedAt,
    text: ''
  }
}

function eventFilterText(filter: ParsedEventFilter) {
  return [
    filter.text,
    filter.createdAfter ? `timestamp>=${filter.createdAfter}` : '',
    filter.createdBefore ? `timestamp<=${filter.createdBefore}` : ''
  ].filter(Boolean).join(' ')
}

function hasAppliedEventFilter(filter: ParsedEventFilter) {
  return filter.text !== '' || Boolean(filter.createdAfter) || Boolean(filter.createdBefore)
}

function parseEventFilter({ referenceTimestamp, value }: { referenceTimestamp?: string; value: string }): ParsedEventFilter {
  const filter: ParsedEventFilter = { text: '' }
  const text = value.replace(
    /(?:^|\s)(?:createdAt|timestamp)\s*(>=|>|<=|<)\s*("[^"]+"|'[^']+'|\S+)/gi,
    (match: string, operator: string, rawTimestamp: string) => {
      const timestamp = normalizeFilterTimestamp({ referenceTimestamp, value: rawTimestamp })
      if (!timestamp) return match

      if (operator === '>' || operator === '>=') {
        filter.createdAfter = timestamp
      } else {
        filter.createdBefore = timestamp
      }

      return ' '
    }
  )

  filter.text = trimBooleanOperators(text.trim().split(/\s+/).filter(Boolean)).join(' ')
  return filter
}

function trimBooleanOperators(tokens: string[]) {
  while (tokens.length > 0 && /^and$/i.test(tokens[0]!)) tokens.shift()
  while (tokens.length > 0 && /^(and|or)$/i.test(tokens[tokens.length - 1]!)) tokens.pop()
  return tokens.filter((token, index) => !/^(and|or)$/i.test(token) || !/^(and|or)$/i.test(tokens[index - 1] ?? ''))
}

function normalizeFilterTimestamp({ referenceTimestamp, value }: { referenceTimestamp?: string; value: string }) {
  value = value.trim().replace(/^["']|["']$/g, '')
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return `${value}T00:00:00Z`
  }
  if (/^\d{1,2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?$/.test(value) && referenceTimestamp) {
    return normalizeClockTimestamp({ referenceTimestamp, value })
  }
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?(?:Z|[+-]\d\d:?\d\d)$/.test(value)) {
    return Number.isFinite(Date.parse(value)) ? value : ''
  }
  const localDateTime = /^(\d{1,2})\/(\d{1,2})\/(\d{4}),?\s+(\d{1,2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?)$/.exec(value)
  if (localDateTime) {
    const [, month, day, year, clock] = localDateTime
    return normalizeLocalTimestamp({ clock: clock!, day: day!, month: month!, year: year! })
  }
  const time = Date.parse(value)
  return Number.isFinite(time) ? new Date(time).toISOString() : ''
}

function normalizeClockTimestamp({ referenceTimestamp, value }: { referenceTimestamp: string; value: string }) {
  const reference = new Date(referenceTimestamp)
  if (!Number.isFinite(reference.getTime())) return ''

  return normalizeLocalTimestamp({
    clock: value,
    day: String(reference.getDate()),
    month: String(reference.getMonth() + 1),
    year: String(reference.getFullYear())
  })
}

function normalizeLocalTimestamp({ clock, day, month, year }: { clock: string; day: string; month: string; year: string }) {
  const [, hour, minute, second = '0', fraction = ''] = /^(\d{1,2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?$/.exec(clock) ?? []
  if (!hour || Number(hour) > 23 || Number(minute) > 59 || Number(second) > 59) return ''

  const ms = fraction.padEnd(3, '0').slice(0, 3)
  const date = new Date(Number(year), Number(month) - 1, Number(day), Number(hour), Number(minute), Number(second), Number(ms))
  return Number.isFinite(date.getTime()) ? date.toISOString().replace(/\.\d{3}Z$/, `.${ms}Z`) : ''
}

function fieldPathValue(data: JsonObject, path: string): JsonValue | undefined {
  const direct = data[normalizedPayloadPath(path)]
  if (direct !== undefined) {
    return direct
  }

  let current: JsonValue | undefined = data
  for (const part of normalizedPayloadPath(path).split('.')) {
    if (!current || typeof current !== 'object' || Array.isArray(current)) {
      return undefined
    }
    current = current[part]
  }
  return current
}

function normalizedPayloadPath(path: string) {
  return path.startsWith('data.') ? path.slice(5) : path
}

function summarizeValue(value: JsonValue | undefined) {
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return String(value)
  }
  if (Array.isArray(value)) {
    return `[${value.length}]`
  }
  if (value && typeof value === 'object') {
    return '{…}'
  }
  return ''
}
