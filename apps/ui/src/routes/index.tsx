import { Link, createFileRoute, useNavigate } from '@tanstack/react-router'
import { keepPreviousData, useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { InfiniteData } from '@tanstack/react-query'
import { useVirtualizer } from '@tanstack/react-virtual'
import { ArrowDown, ArrowUp, Calendar as CalendarIcon, Check, ChevronDown, Columns3, KeyRound, LogOut, PanelLeftOpen, UserCircle, X } from 'lucide-react'
import { format } from 'date-fns'
import type { DateRange } from 'react-day-picker'
import { useEffect, useMemo, useRef, useState } from 'react'
import type { JsonObject, JsonValue } from '../lib/json'
import { clamp, useCookieState, useIndexedDbState } from '../lib/hooks'
import { cn } from '../lib/cn'
import { useAppShell } from '../lib/app-shell'
import { Calendar } from '../components/ui/calendar'
import { Popover, PopoverContent, PopoverTrigger } from '../components/ui/popover'
import {
  HTTPError,
  errorMessage,
  nanotraceApiBaseUrl,
  queryHeaders,
  runtimeNanotraceApiKey
} from '../lib/nanotrace-api'

export const Route = createFileRoute('/')({
  component: IndexRoute
})

function IndexRoute() {
  return <LandingPage />
}

function LandingPage() {
  return (
    <main className="min-h-screen overflow-x-hidden bg-black text-neutral-100">
      <section className="relative flex min-h-screen items-stretch overflow-hidden">
        <LandingConsoleBackdrop />
        <div className="relative z-10 flex w-full flex-col">
          <header className="mx-auto flex h-14 w-full max-w-6xl items-center justify-between px-4 sm:px-6">
            <div className="font-mono text-[13px] font-semibold tracking-tight text-white">Nanotrace</div>
            <nav className="flex items-center gap-2">
              <a
                className="inline-flex h-8 items-center justify-center border border-neutral-800 bg-black/70 px-3 text-[12px] text-neutral-300 backdrop-blur hover:border-neutral-600 hover:text-white"
                href="https://github.com/suhjohn/nanotrace"
                rel="noreferrer"
                target="_blank"
              >
                GitHub
              </a>
              <Link
                className="inline-flex h-8 items-center justify-center border border-white bg-white px-3 text-[12px] font-medium text-black hover:bg-neutral-200"
                to="/logs"
              >
                Sign in
              </Link>
            </nav>
          </header>
          <div className="mx-auto flex w-full max-w-6xl flex-1 items-center px-4 py-16 sm:px-6">
            <div className="max-w-2xl">
              <h1 className="text-balance text-[clamp(44px,7vw,84px)] font-medium leading-[0.94] tracking-normal text-white">
                Nanotrace
              </h1>
              <p className="mt-5 max-w-xl text-balance text-[17px] leading-7 text-neutral-300 sm:text-[19px]">
                One event timeline for product behavior, infrastructure signals, and AI agent execution.
              </p>
              <div className="mt-8 flex flex-wrap items-center gap-3">
                <Link
                  className="inline-flex h-10 items-center justify-center border border-white bg-white px-4 text-[13px] font-medium text-black hover:bg-neutral-200"
                  to="/logs"
                >
                  Sign in
                </Link>
                <a
                  className="inline-flex h-10 items-center justify-center border border-neutral-800 bg-black/70 px-4 text-[13px] text-neutral-300 backdrop-blur hover:border-neutral-600 hover:text-white"
                  href="https://github.com/suhjohn/nanotrace"
                  rel="noreferrer"
                  target="_blank"
                >
                  GitHub
                </a>
              </div>
            </div>
          </div>
        </div>
      </section>
    </main>
  )
}

function LandingConsoleBackdrop() {
  const rows = [
    ['12:40:04.210', 'llm.call', 'agent-runtime', 'ok'],
    ['12:40:04.415', 'tool.call', 'retrieval', 'ok'],
    ['12:40:05.102', 'span.end', 'checkout', '481ms'],
    ['12:40:06.344', 'eval.score', 'answer_quality', '0.92'],
    ['12:40:07.018', 'state.change', 'account.plan', 'pro']
  ]

  return (
    <div aria-hidden="true" className="absolute inset-0">
      <div className="absolute inset-0 bg-black" />
      <div className="absolute right-[-180px] top-20 w-[760px] max-w-none rotate-[-6deg] opacity-55 blur-[0.2px] max-lg:right-[-360px] max-sm:right-[-520px]">
        <div className="border border-neutral-800 bg-neutral-950 shadow-2xl shadow-black">
          <div className="grid h-8 grid-cols-[180px_1fr_140px] border-b border-neutral-800 text-[10px] uppercase text-neutral-600">
            <div className="border-r border-neutral-800 px-3 py-2">timeline</div>
            <div className="border-r border-neutral-800 px-3 py-2">event</div>
            <div className="px-3 py-2">status</div>
          </div>
          {rows.map(([time, name, source, state]) => (
            <div key={`${time}-${name}`} className="grid h-12 grid-cols-[180px_1fr_140px] border-b border-neutral-900 text-[12px] last:border-b-0">
              <div className="border-r border-neutral-900 px-3 py-3 font-mono text-neutral-500">{time}</div>
              <div className="min-w-0 border-r border-neutral-900 px-3 py-3">
                <div className="truncate font-mono text-neutral-200">{name}</div>
                <div className="mt-0.5 truncate text-[11px] text-neutral-600">{source}</div>
              </div>
              <div className="px-3 py-3 font-mono text-neutral-400">{state}</div>
            </div>
          ))}
        </div>
        <div className="mt-5 h-28 border border-neutral-800 bg-neutral-950 p-3">
          <div className="flex h-full items-end gap-1">
            {[18, 42, 28, 64, 48, 78, 44, 32, 58, 90, 52, 38, 68, 46, 72, 35, 54, 82].map((height, index) => (
              <div key={index} className="flex-1 bg-neutral-700/70" style={{ height: `${height}%` }} />
            ))}
          </div>
        </div>
      </div>
      <div className="absolute inset-0 bg-[linear-gradient(90deg,#000_0%,rgba(0,0,0,0.92)_32%,rgba(0,0,0,0.52)_68%,#000_100%)]" />
    </div>
  )
}

export function LogsRoute({ search }: { search: ObservatorySearch }) {
  return (
    <ObservatoryHome
      eventFilterSearchText={search.filter}
      searchCustomRangeEnd={search.rangeEnd}
      searchCustomRangeStart={search.rangeStart}
      searchGroupBy={search.groupBy}
      searchTimeRange={search.timeRange}
      selectedEventId={search.eventId ?? ''}
    />
  )
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

type LogGroupPage = {
  groups: LogGroupSummary[]
  nextOffset?: number
}

type EventCursor = {
  createdAt: string
  eventId: string
}

type LogEventsPage = {
  anchorIndex?: number
  events: TraceEvent[]
  fields: LogField[]
  group?: LogGroupSummary
  nextCursor?: EventCursor
  prevCursor?: EventCursor
  queryPlan?: QueryPlanMetadata
}

type QueryPlanMetadata = {
  allowStaleServing?: boolean
  eventFilters: QueryFilterPlan[]
  freshnessOverrides?: string[]
  planKind?: string
  recommendations: QueryRecommendation[]
  shapeClass?: string
  sourceTables: string[]
  surface?: string
}

type QueryRecommendation = {
  action?: string
  groupBy?: string[]
  kind?: string
  operator?: string
  path?: string
  reportId?: string
  reason?: string
  source?: string
  targetTable?: string
  targetType?: string
}

type DefinitionRecord = {
  config?: Record<string, unknown>
  definition_id: string
  kind: string
  mode: string
  name: string
  updated_at: string
}

type MaterializationJobRecord = {
  completed_at?: string | null
  completed_chunks: number
  failed_chunks: number
  job_id: string
  rows_scanned: number
  rows_written: number
  source_end: string
  source_start: string
  status: string
  target_id: string
  target_table: string
  target_type: string
  target_version: number
  total_chunks: number
  updated_at: string
}

type QueryFilterPlan = {
  negated?: boolean
  operator?: string
  path?: string
  role?: string
  route?: string
  scope?: string
  strategy?: string
}

type EventSearchMode = 'token' | 'prefix' | 'fuzzy' | 'phrase'

type EventPageParam = {
  after?: string
  around?: string
  before?: string
  eventId?: string
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

type DensityYScale = 'linear' | 'sqrt' | 'log'

type LogDensity = {
  bucketMs: number
  buckets: DensityBucket[]
  from: string
  to: string
}

function hasRenderableDensity(density: LogDensity | null | undefined) {
  if (!density || density.buckets.length === 0) return false
  const fromMs = traceTimeMs(density.from)
  const toMs = traceTimeMs(density.to)
  return Number.isFinite(fromMs) && Number.isFinite(toMs) && toMs >= fromMs
}

type LogFlamegraph = Flamegraph & {
  capped?: boolean
  spanCount?: number
}

type GroupOption = {
  aggregateEnabled?: boolean
  cardinality: number
  capped: boolean
  displayName?: string
  indexEnabled?: boolean
  path: string
  removable?: boolean
  servingMode?: string
  source?: string
  valueType?: string
}

const emptyGroupOptions: GroupOption[] = []

type AuthIdentity = {
  auth_type: 'api_key' | 'session'
  email?: string
  name?: string
  role: 'admin' | 'service' | 'viewer'
  subject: string
}

type ApiKeyRecord = {
  id: number
  name: string
  prefix: string
  role: 'admin' | 'service' | 'viewer'
  created_by: string
  created_at: string
  last_used_at?: string | null
  expires_at?: string | null
  revoked_at?: string | null
}

type CreatedApiKey = {
  key: string
  api_key: ApiKeyRecord
}

export type RouteSelection = {
  field: string
  value: string
}

export type ObservatorySearch = {
  eventId?: string
  filter?: string
  groupBy?: string
  rangeEnd?: string
  rangeStart?: string
  timeRange?: TimeRangeKey
}

type FlameKind = 'event' | 'run' | 'turn' | 'model' | 'tool'
type EventSortDirection = 'asc' | 'desc'
type GraphMode = 'flamegraph' | 'histogram'
type GroupSortKey = 'count' | 'duration' | 'recent' | 'value'
type TimeRangeKey = 'live' | '15m' | '1h' | '6h' | '24h' | '7d' | 'custom'

type FlameSpan = {
  eventIds: string[]
  id: string
  label: string
  kind: FlameKind
  marker: boolean
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
  'flex min-h-0 min-w-0 flex-col overflow-hidden bg-neutral-950'
const eventMarkerWidth = 5
const groupPageSize = 120
const noGroupValue = '__nanotrace_no_group__'
const defaultEventColumns: string[] = ['timestamp', 'name', 'traceId', 'data']
const timeRangeOptions: { key: Exclude<TimeRangeKey, 'custom'>; label: string; minutes: number }[] = [
  { key: 'live', label: 'Live', minutes: 15 },
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
  if (
    'groupBy' in search &&
    typeof search.groupBy === 'string' &&
    search.groupBy &&
    search.groupBy !== noGroupValue
  ) {
    parsed.groupBy = search.groupBy
  }
  if ('timeRange' in search && typeof search.timeRange === 'string' && search.timeRange) {
    parsed.timeRange = parseTimeRangeKey(search.timeRange)
  }
  if ('rangeStart' in search && typeof search.rangeStart === 'string' && search.rangeStart) {
    parsed.rangeStart = search.rangeStart
  }
  if ('rangeEnd' in search && typeof search.rangeEnd === 'string' && search.rangeEnd) {
    parsed.rangeEnd = search.rangeEnd
  }
  return parsed
}

function parseStringArray(value: string) {
  const parsed = JSON.parse(value)
  return Array.isArray(parsed) ? parsed.filter((item): item is string => typeof item === 'string') : [...defaultEventColumns]
}

function parseTimeRangeKey(value: string): TimeRangeKey {
  return value === 'custom' || timeRangeOptions.some(option => option.key === value) ? value as TimeRangeKey : '24h'
}

function parseEventSortDirection(value: string): EventSortDirection {
  return value === 'desc' ? 'desc' : 'asc'
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

function timePartFromLocalInput(value: string) {
  const match = value.match(/T(\d{2}:\d{2})/)
  return match?.[1] ?? '00:00'
}

function dateRangeFromLocalInputs(start: string, end: string): DateRange | undefined {
  const from = localDateOnly(start)
  const to = localDateOnly(end)
  if (!from && !to) return undefined
  return { from: from ?? to, to: to ?? from }
}

function localDateOnly(value: string) {
  const parsed = Date.parse(value)
  if (!Number.isFinite(parsed)) return undefined
  const date = new Date(parsed)
  return new Date(date.getFullYear(), date.getMonth(), date.getDate())
}

function localMonthOnly(value: string) {
  const parsed = Date.parse(value)
  if (!Number.isFinite(parsed)) return undefined
  const date = new Date(parsed)
  return new Date(date.getFullYear(), date.getMonth(), 1)
}

function combineLocalDateAndTime(date: Date, time: string) {
  const [hours = '0', minutes = '0'] = time.split(':')
  const next = new Date(date)
  next.setHours(Number(hours) || 0, Number(minutes) || 0, 0, 0)
  return formatDateTimeLocalInput(next)
}

function timePartsFromLocalTime(value: string) {
  const [hourValue = '0', minuteValue = '0'] = value.split(':')
  const hour24 = Number(hourValue) || 0
  const minute = clamp(Number(minuteValue) || 0, 0, 59)
  const period = hour24 >= 12 ? 'PM' : 'AM'
  const hour12 = hour24 % 12 || 12
  return {
    hour: String(hour12).padStart(2, '0'),
    minute: String(minute).padStart(2, '0'),
    period
  }
}

function localTimeFromParts(hour: string, minute: string, period: string) {
  let hourNumber = clamp(Number(hour) || 12, 1, 12)
  const minuteNumber = clamp(Number(minute) || 0, 0, 59)
  if (period === 'PM' && hourNumber < 12) hourNumber += 12
  if (period === 'AM' && hourNumber === 12) hourNumber = 0
  return `${String(hourNumber).padStart(2, '0')}:${String(minuteNumber).padStart(2, '0')}`
}

function normalizeTimeSegment(value: string, min: number, max: number) {
  const digits = value.replace(/\D/g, '')
  if (!digits) return String(min).padStart(2, '0')
  return String(clamp(Number(digits) || min, min, max)).padStart(2, '0')
}

function customRangeLabel(start: string, end: string) {
  const startDate = new Date(start)
  const endDate = new Date(end)
  if (!Number.isFinite(startDate.getTime()) || !Number.isFinite(endDate.getTime())) return 'Custom'
  return `${format(startDate, 'MMM d HH:mm')} - ${format(endDate, 'MMM d HH:mm')}`
}

function getJsonValueType(value: JsonValue) {
  if (value === null) return 'null'
  if (Array.isArray(value)) return 'array'
  return typeof value
}

export function ObservatoryHome({
  eventFilterSearchText,
  routeSelection,
  searchCustomRangeEnd,
  searchCustomRangeStart,
  searchGroupBy,
  searchTimeRange,
  selectedEventId
}: {
  eventFilterSearchText?: string
  routeSelection?: RouteSelection
  searchCustomRangeEnd?: string
  searchCustomRangeStart?: string
  searchGroupBy?: string
  searchTimeRange?: TimeRangeKey
  selectedEventId: string
}) {
  const observatoryUrl = nanotraceApiBaseUrl()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { setSidebarOpen, sidebarOpen } = useAppShell()
  const [runsWidth, setRunsWidth] = useCookieState({
    cookieName: 'observatory-ui-runs-width',
    initialValue: 320
  })
  const runsOpen = true
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
  const [eventSortDirection, setEventSortDirection] = useCookieState<EventSortDirection>({
    cookieName: 'observatory-ui-event-sort-direction-v2',
    initialValue: 'asc',
    parse: parseEventSortDirection
  })
  const [liveEventSortDirection, setLiveEventSortDirection] = useCookieState<EventSortDirection>({
    cookieName: 'observatory-ui-live-event-sort-direction-v1',
    initialValue: 'desc',
    parse: parseEventSortDirection
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
  const [highlightedEventIds, setHighlightedEventIds] = useState<string[]>([])
  const [freshEventIds, setFreshEventIds] = useState<string[]>([])
  const [selectedCanvasSpanId, setSelectedCanvasSpanId] = useState('')
  const [filter, setFilter] = useState('')
  const [groupSelectOpen, setGroupSelectOpen] = useState(false)
  const [eventFilterDraft, setEventFilterDraft] = useState('')
  const [eventFilterGroupKey, setEventFilterGroupKey] = useState('')
  const [eventFilterParams, setEventFilterParams] = useState<ParsedEventFilter>({ text: '' })
  const [eventAnchorOverride, setEventAnchorOverride] = useState<{ eventId: string; key: string; timestamp: string } | null>(null)
  const [inspectorQuery, setInspectorQuery] = useState('')

  const [selectedEventColumns, setSelectedEventColumns] = useIndexedDbState<string[]>({
    initialValue: defaultEventColumns,
    key: 'observatory-ui-event-columns',
    parse: parseStringArray
  })
  const filterTouchedRef = useRef(false)
  const groupListRef = useRef<HTMLDivElement | null>(null)
  const groupLoadMoreRef = useRef<HTMLDivElement | null>(null)
  const freshEventTimeoutRef = useRef<number | null>(null)
  const liveEventSnapshotRef = useRef<{ ids: Set<string>; maxCreatedMs: number; scopeKey: string }>({
    ids: new Set(),
    maxCreatedMs: 0,
    scopeKey: ''
  })
  const seededLatestGroupKeyRef = useRef('')
  const previousGroupKeyRef = useRef('')
  const workspaceRef = useRef<HTMLElement | null>(null)
  const arrowKeyScopeRef = useRef<'events' | 'local'>('events')
  const groupOptionsQuery = useQuery({
    queryKey: ['logs', observatoryUrl, 'group-options'],
    queryFn: () => fetchGroupOptions({ apiBaseUrl: observatoryUrl, limit: 120 })
  })
  const groupOptions = groupOptionsQuery.data?.fields ?? emptyGroupOptions
  const activeFacetPaths = useMemo(() => {
    const paths = new Set<string>()
    for (const option of groupOptions) {
      paths.add(option.path)
      paths.add(facetKey(option.path))
    }
    return paths
  }, [groupOptions])
  const defaultGroupBy = groupOptions.find(option => option.path === 'traceId')?.path || groupOptions[0]?.path || ''
  const routeGroupBy = routeSelection?.field ?? ''
  const requestedGroupBy = routeGroupBy || searchGroupBy || ''
  const requestedGroupByValid = isKnownGroupOption(requestedGroupBy, groupOptions)
  const groupBy = requestedGroupBy ? requestedGroupByValid ? requestedGroupBy : defaultGroupBy : ''
  const groupSortKey: GroupSortKey = isTraceLikeGroup(groupBy) ? 'recent' : 'count'
  const displayedGroupOptions = useMemo(
    () =>
      groupBy && !groupOptions.some(option => option.path === groupBy)
        ? [{ cardinality: 0, capped: false, path: groupBy }, ...groupOptions]
        : groupOptions,
    [groupBy, groupOptions]
  )
  const selectedGroupOption = displayedGroupOptions.find(option => option.path === groupBy) ?? null
  const groupByLabel = selectedGroupOption ? groupOptionLabel(selectedGroupOption) : groupBy
  const selectedGroupValue = routeSelection?.field === groupBy ? routeSelection.value : ''
  const effectiveTimeRangeKey = searchTimeRange ?? timeRangeKey
  const effectiveCustomRangeStart = searchCustomRangeStart ?? customRangeStart
  const effectiveCustomRangeEnd = searchCustomRangeEnd ?? customRangeEnd
  const selectedTimeRange = useMemo(
    () =>
      resolveTimeRange({
        customEnd: effectiveCustomRangeEnd,
        customStart: effectiveCustomRangeStart,
        key: effectiveTimeRangeKey
      }),
    [effectiveCustomRangeEnd, effectiveCustomRangeStart, effectiveTimeRangeKey]
  )
  const selectedTimeRangeCacheKey = timeRangeCacheKey(selectedTimeRange)
  const liveMode = effectiveTimeRangeKey === 'live'
  const effectiveEventSortDirection = liveMode ? liveEventSortDirection : eventSortDirection
  const setEffectiveEventSortDirection = liveMode ? setLiveEventSortDirection : setEventSortDirection
  const liveRefetchInterval = liveMode ? 3000 : false
  const selectedGroupKey = groupBy && selectedGroupValue ? `${groupBy}\u0000${selectedGroupValue}` : ''
  const viewingGroupedEvents = Boolean(selectedGroupKey)
  const eventScopeKey = selectedGroupKey || 'all-events'
  const eventFilterReady = eventFilterGroupKey === eventScopeKey
  const hasEventQuery = viewingGroupedEvents ? eventFilterReady : true
  const groupSearch = filter.trim()
  const groupListTimeRange = useMemo(() => {
    if (eventFilterParams.createdAfter || eventFilterParams.createdBefore) {
      return {
        createdAfter: eventFilterParams.createdAfter,
        createdBefore: eventFilterParams.createdBefore,
        key: `filter:${eventFilterParams.createdAfter ?? ''}:${eventFilterParams.createdBefore ?? ''}`
      }
    }
    return selectedTimeRange
  }, [eventFilterParams.createdAfter, eventFilterParams.createdBefore, selectedTimeRange])
  const groupsQuery = useInfiniteQuery<LogGroupPage, Error, InfiniteData<LogGroupPage>, string[], number>({
    enabled: groupOptions.length > 0 && Boolean(groupBy),
    getNextPageParam: lastPage => lastPage.nextOffset,
    initialPageParam: 0,
    queryKey: [
      'logs',
      observatoryUrl,
      'groups',
      groupBy,
      groupListTimeRange.key,
      groupListTimeRange.createdAfter ?? '',
      groupListTimeRange.createdBefore ?? '',
      groupSortKey,
      groupSearch
    ],
    queryFn: ({ pageParam }) =>
      fetchGroups({
        apiBaseUrl: observatoryUrl,
        groupBy,
        limit: groupPageSize,
        offset: pageParam,
        search: groupSearch,
        sortKey: groupSortKey,
        timeRange: groupListTimeRange
      }),
    refetchInterval: liveRefetchInterval
  })
  const traceList = groupsQuery.data?.pages.flatMap(page => page.groups) ?? []
  useEffect(() => {
    const sentinel = groupLoadMoreRef.current
    const root = groupListRef.current
    if (!sentinel || !root || !groupsQuery.hasNextPage) return

    const observer = new IntersectionObserver(
      entries => {
        if (entries.some(entry => entry.isIntersecting) && !groupsQuery.isFetchingNextPage) {
          void groupsQuery.fetchNextPage()
        }
      },
      {
        root,
        rootMargin: '320px 0px 320px 0px'
      }
    )
    observer.observe(sentinel)
    return () => observer.disconnect()
  }, [groupsQuery.dataUpdatedAt, groupsQuery.hasNextPage, groupsQuery.isFetchingNextPage, groupsQuery.fetchNextPage])
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
    eventScopeKey,
    serializeEventFilter(eventFilterParams),
    selectedTimeRangeCacheKey
  ].join('\u0000')
  const eventAnchorTimestamp = eventAnchorOverride?.key === eventDataKey ? eventAnchorOverride.timestamp : ''
  const eventAnchorEventId = eventAnchorOverride?.key === eventDataKey ? eventAnchorOverride.eventId : ''
  const summaryQuery = useQuery({
    enabled: Boolean(hasEventQuery),
    queryKey: ['logs', observatoryUrl, 'summary', groupBy, selectedGroupValue, eventFilterParams, selectedTimeRangeCacheKey],
    queryFn: () =>
      fetchSummary({
        apiBaseUrl: observatoryUrl,
        eventFilter: eventFilterParams,
        groupBy,
        selectedGroupValue,
        timeRange: selectedTimeRange
      }),
    refetchInterval: liveRefetchInterval,
    retry: false
  })
  const flamegraphDisabledBySummary = Boolean(summaryQuery.data?.capped)
  const graphModeBeforeFlamegraph = flamegraphDisabledBySummary ? 'histogram' : selectedGraphMode
  const eventsQuery = useInfiniteQuery<LogEventsPage, Error, InfiniteData<LogEventsPage>, (string | ParsedEventFilter)[], EventPageParam>({
    enabled: Boolean(hasEventQuery),
    queryKey: ['logs', observatoryUrl, 'events', groupBy, selectedGroupValue, eventFilterParams, selectedTimeRangeCacheKey, eventAnchorTimestamp, eventAnchorEventId, effectiveEventSortDirection],
    initialPageParam: (eventAnchorTimestamp ? { around: eventAnchorTimestamp, eventId: eventAnchorEventId } : {}) as EventPageParam,
    queryFn: ({ pageParam }) =>
      fetchEvents({
        apiBaseUrl: observatoryUrl,
        eventFilter: eventFilterParams,
        groupBy,
        limit: 100,
        pageParam,
        selectedGroupValue,
        sortDirection: effectiveEventSortDirection,
        timeRange: selectedTimeRange
      }),
    getNextPageParam: lastPage => {
      const cursor = lastPage.nextCursor
      if (!cursor) return undefined
      return effectiveEventSortDirection === 'desc'
        ? { before: cursor.createdAt, eventId: cursor.eventId }
        : { after: cursor.createdAt, eventId: cursor.eventId }
    },
    getPreviousPageParam: firstPage => {
      const cursor = firstPage.prevCursor
      if (!cursor) return undefined
      return effectiveEventSortDirection === 'desc'
        ? { after: cursor.createdAt, eventId: cursor.eventId }
        : { before: cursor.createdAt, eventId: cursor.eventId }
    },
    placeholderData: keepPreviousData,
    refetchInterval: liveRefetchInterval,
    retry: false
  })
  const flamegraphQuery = useQuery({
    enabled: Boolean(viewingGroupedEvents && hasEventQuery && summaryQuery.data && graphModeBeforeFlamegraph === 'flamegraph'),
    queryKey: ['logs', observatoryUrl, 'flamegraph', groupBy, selectedGroupValue, eventFilterParams, selectedTimeRangeCacheKey],
    queryFn: () =>
      fetchFlamegraph({
        apiBaseUrl: observatoryUrl,
        eventFilter: eventFilterParams,
        groupBy,
        maxSpans: 20_000,
        selectedGroupValue,
        timeRange: selectedTimeRange
      }),
    refetchInterval: liveRefetchInterval,
    retry: false
  })
  const flamegraphDisabled = !viewingGroupedEvents || flamegraphDisabledBySummary || Boolean(flamegraphQuery.data?.capped)
  const graphMode = flamegraphDisabled ? 'histogram' : selectedGraphMode
  const densityQuery = useQuery({
    enabled: Boolean(hasEventQuery && summaryQuery.data && graphMode === 'histogram'),
    queryKey: ['logs', observatoryUrl, 'density', groupBy, selectedGroupValue, eventFilterParams, selectedTimeRangeCacheKey],
    queryFn: () =>
      fetchDensity({
        apiBaseUrl: observatoryUrl,
        buckets: 700,
        eventFilter: eventFilterParams,
        groupBy,
        selectedGroupValue,
        timeRange: selectedTimeRange
      }),
    refetchInterval: liveRefetchInterval,
    retry: false
  })
  const eventPages = eventsQuery.data?.pages ?? []
  const allEvents = useMemo(
    () => eventPages.flatMap(page => page.events),
    [eventPages]
  )
  const displayedEvents = allEvents
  const displayedFields = mergeLogFields(eventPages.flatMap(page => page.fields))
  const displayedQueryPlan = eventPages.find(page => page.queryPlan)?.queryPlan
  const eventQueryPlan = useMemo(
    () => displayedQueryPlan,
    [displayedQueryPlan]
  )
  const reportRecommendation = eventQueryPlan?.recommendations.find(isReportDefinitionRecommendation)
  const reportRecommendationTargetId = reportRecommendation
    ? reportDefinitionIdFromRecommendation({ groupBy, recommendation: reportRecommendation, selectedGroupValue })
    : ''
  const materializationJobsQuery = useQuery({
    enabled: Boolean(reportRecommendationTargetId),
    queryKey: ['materialization-jobs', observatoryUrl],
    queryFn: () => fetchMaterializationJobs({ apiBaseUrl: observatoryUrl }),
    refetchInterval: reportRecommendationTargetId ? 5000 : false,
    retry: false
  })
  const reportMaterializationJob = reportRecommendationTargetId
    ? latestMaterializationJobForTarget(materializationJobsQuery.data?.jobs ?? [], 'report', reportRecommendationTargetId)
    : null
  const createFieldDefinitionMutation = useMutation({
    mutationFn: (recommendation: QueryRecommendation) =>
      createFieldDefinitionFromRecommendation({
        apiBaseUrl: observatoryUrl,
        recommendation
      }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['definitions', observatoryUrl] })
      await eventsQuery.refetch()
    }
  })
  const createReportDefinitionMutation = useMutation({
    mutationFn: (recommendation: QueryRecommendation) =>
      createReportDefinitionFromRecommendation({
        apiBaseUrl: observatoryUrl,
        eventFilter: eventFilterParams,
        groupBy,
        recommendation,
        selectedGroupValue,
        timeRange: selectedTimeRange
      }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['definitions', observatoryUrl] })
      await queryClient.invalidateQueries({ queryKey: ['materialization-jobs', observatoryUrl] })
      await eventsQuery.refetch()
    }
  })
  const createMeasureDefinitionMutation = useMutation({
    mutationFn: (recommendation: QueryRecommendation) =>
      createMeasureDefinitionFromRecommendation({
        apiBaseUrl: observatoryUrl,
        recommendation
      }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['definitions', observatoryUrl] })
      await eventsQuery.refetch()
    }
  })
  const createSearchDefinitionMutation = useMutation({
    mutationFn: (recommendation: QueryRecommendation) =>
      createSearchDefinitionFromRecommendation({
        apiBaseUrl: observatoryUrl,
        eventFilter: eventFilterParams,
        includeSnippets: true,
        query: '',
        recommendation,
        requireAllTerms: false,
        searchMode: 'phrase'
      }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['definitions', observatoryUrl] })
      await eventsQuery.refetch()
    }
  })
  const liveFreshScopeKey = [
    eventDataKey,
    effectiveEventSortDirection
  ].join('\u0000')
  useEffect(() => {
    if (!liveMode) {
      if (freshEventTimeoutRef.current !== null) {
        window.clearTimeout(freshEventTimeoutRef.current)
        freshEventTimeoutRef.current = null
      }
      liveEventSnapshotRef.current = { ids: new Set(), maxCreatedMs: 0, scopeKey: '' }
      setFreshEventIds([])
      return
    }

    const currentIds = new Set(allEvents.map(event => event.id))
    const currentMaxCreatedMs = allEvents.reduce((max, event) => {
      const createdMs = traceTimeMs(event.createdAt)
      return Number.isFinite(createdMs) ? Math.max(max, createdMs) : max
    }, 0)
    const previous = liveEventSnapshotRef.current

    if (previous.scopeKey !== liveFreshScopeKey) {
      if (freshEventTimeoutRef.current !== null) {
        window.clearTimeout(freshEventTimeoutRef.current)
        freshEventTimeoutRef.current = null
      }
      liveEventSnapshotRef.current = {
        ids: currentIds,
        maxCreatedMs: currentMaxCreatedMs,
        scopeKey: liveFreshScopeKey
      }
      setFreshEventIds([])
      return
    }

    const addedIds = allEvents
      .filter(event => {
        const createdMs = traceTimeMs(event.createdAt)
        return !previous.ids.has(event.id) && Number.isFinite(createdMs) && createdMs > previous.maxCreatedMs
      })
      .map(event => event.id)

    if (addedIds.length > 0) {
      if (freshEventTimeoutRef.current !== null) {
        window.clearTimeout(freshEventTimeoutRef.current)
        freshEventTimeoutRef.current = null
      }
      setFreshEventIds(addedIds)
      freshEventTimeoutRef.current = window.setTimeout(() => {
        setFreshEventIds(current => current.filter(id => !addedIds.includes(id)))
        freshEventTimeoutRef.current = null
      }, 6000)
    }

    liveEventSnapshotRef.current = {
      ids: currentIds,
      maxCreatedMs: Math.max(previous.maxCreatedMs, currentMaxCreatedMs),
      scopeKey: liveFreshScopeKey
    }
  }, [allEvents, eventsQuery.dataUpdatedAt, liveFreshScopeKey, liveMode])
  useEffect(() => {
    return () => {
      if (freshEventTimeoutRef.current !== null) {
        window.clearTimeout(freshEventTimeoutRef.current)
      }
    }
  }, [])
  const traceDetail = viewingGroupedEvents && eventPages[0]?.group
    ? {
        fields: mergeLogFields(eventPages.flatMap(page => page.fields)),
        group: eventPages[0].group,
        events: allEvents,
        relatedEvents: []
      }
    : null
  const eventDetail =
    eventPages.length > 0
      ? {
          fields: displayedFields,
          group: eventPages[0]?.group ?? pageGroupSummary({ events: allEvents, groupBy: '', selectedGroupValue: 'events' }),
          events: displayedEvents,
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
  const emptyGroup = !loadingList && !listError && Boolean(groupBy) && !groupSearch && groupOptions.length > 0 && traceList.length === 0
  const emptyGroupLabel = 'No groups found.'
  const waitingForLatest = Boolean(needsLatest && latestQuery.isPending && !hasEventQuery)
  const waitingForSummary = Boolean(hasEventQuery && summaryQuery.isPending)
  const loadingGraph =
    graphMode === 'histogram'
      ? densityQuery.isPending
      : flamegraphQuery.isPending
  const loadingDetail = hasEventQuery && (waitingForLatest || waitingForSummary || loadingGraph)
  const loadingTableDetail = hasEventQuery && eventsQuery.isPending
  const loadingAnchoredEvents = Boolean(
    eventAnchorOverride?.key === eventDataKey &&
    eventsQuery.isFetching &&
    !eventsQuery.isFetchingNextPage &&
    !eventsQuery.isFetchingPreviousPage
  )
  const draftEventFilterParams = useMemo(
    () =>
      parseEventFilter({
        facetPaths: activeFacetPaths,
        referenceTimestamp: eventDetail?.group.startedAt ?? latestCreatedAt,
        value: eventFilterDraft
      }),
    [activeFacetPaths, eventDetail?.group.startedAt, eventFilterDraft, latestCreatedAt]
  )
  const eventFilterDirty =
    eventFilterInputText(draftEventFilterParams) !== eventFilterInputText(eventFilterParams) ||
    draftEventFilterParams.createdAfter !== eventFilterParams.createdAfter ||
    draftEventFilterParams.createdBefore !== eventFilterParams.createdBefore
  const hasEventFilter =
    eventFilterParams.text !== '' ||
    Boolean(eventFilterParams.createdAfter) ||
    Boolean(eventFilterParams.createdBefore) ||
    Boolean(eventFilterParams.facets?.length) ||
    eventFilterDraft !== ''

  function updateSearch(patch: Partial<ObservatorySearch>) {
    void navigate({
      search: (current: ObservatorySearch) => ({
        ...current,
        ...patch
      })
    } as never)
  }

  function navigateRootSearch(patch: Partial<ObservatorySearch>) {
    void navigate({
      to: '/logs',
      search: (current: ObservatorySearch) => ({
        ...current,
        ...patch
      })
    } as never)
  }

  function navigateSelectedGroup(value: string) {
    void navigate({
      to: '/$field/$value',
      params: {
        field: groupBy,
        value
      },
      search: (current: ObservatorySearch) => ({
        ...current,
        eventId: undefined,
        groupBy: undefined
      })
    } as never)
  }

  function selectGroupBySearch(nextGroupBy: string) {
    resetEventScope('all-events', { force: true })
    setFilter('')
    setGroupSelectOpen(false)
    navigateRootSearch({
      eventId: undefined,
      groupBy: nextGroupBy || undefined
    })
  }

  function setFilterSearch(value: string) {
    if (value) {
      updateSearch({ filter: value })
      return
    }

    void navigate({
      search: (current: ObservatorySearch) => {
        const { filter: _filter, ...next } = current
        return next
      }
    } as never)
  }

  function setTimeRangeSearch(key: TimeRangeKey, start?: string, end?: string) {
    updateSearch({
      rangeEnd: key === 'custom' ? end || undefined : undefined,
      rangeStart: key === 'custom' ? start || undefined : undefined,
      timeRange: key
    })
  }

  function commitEventFilter(nextFilter: ParsedEventFilter, { syncUrl = true }: { syncUrl?: boolean } = {}) {
    const nextFilterText = eventFilterInputText(nextFilter)
    setEventFilterGroupKey(eventScopeKey)
    setEventFilterDraft(nextFilterText)
    setEventFilterParams(nextFilter)
    if (syncUrl) {
      setFilterSearch(nextFilterText)
    }
  }

  function clearFilterTimeBounds() {
    if (!eventFilterParams.createdAfter && !eventFilterParams.createdBefore) {
      return
    }
    commitEventFilter(stripTimeBounds(eventFilterParams))
  }

  function syncTimeRangeControlsFromFilter(filter: ParsedEventFilter) {
    if (!filter.createdAfter && !filter.createdBefore) {
      return
    }

    const start = filter.createdAfter
      ? formatDateTimeLocalInput(new Date(filter.createdAfter))
      : effectiveCustomRangeStart
    const end = filter.createdBefore
      ? formatDateTimeLocalInput(new Date(filter.createdBefore))
      : effectiveCustomRangeEnd
    setCustomRangeStart(start)
    setCustomRangeEnd(end)
    setTimeRangeKey('custom')
    setTimeRangeSearch('custom', start, end)
  }

  function selectTimeRange(key: TimeRangeKey) {
    setTimeRangeKey(key)
    setTimeRangeSearch(key, key === 'custom' ? effectiveCustomRangeStart : undefined, key === 'custom' ? effectiveCustomRangeEnd : undefined)
    clearFilterTimeBounds()
    setEventAnchorOverride(null)
  }

  function setCustomStartRange(value: string) {
    setCustomRangeStart(value)
    setTimeRangeKey('custom')
    setTimeRangeSearch('custom', value, effectiveCustomRangeEnd)
    clearFilterTimeBounds()
    setEventAnchorOverride(null)
  }

  function setCustomEndRange(value: string) {
    setCustomRangeEnd(value)
    setTimeRangeKey('custom')
    setTimeRangeSearch('custom', effectiveCustomRangeStart, value)
    clearFilterTimeBounds()
    setEventAnchorOverride(null)
  }

  function setCustomRange(start: string, end: string) {
    setCustomRangeStart(start)
    setCustomRangeEnd(end)
    setTimeRangeKey('custom')
    setTimeRangeSearch('custom', start, end)
    clearFilterTimeBounds()
    setEventAnchorOverride(null)
  }

  function applyEventFilter() {
    if (eventFilterDraft.trim() === '') {
      clearEventFilter()
      return
    }
    filterTouchedRef.current = true
    syncTimeRangeControlsFromFilter(draftEventFilterParams)
    commitEventFilter(stripTimeBounds(draftEventFilterParams))
  }

  function clearEventFilter() {
    filterTouchedRef.current = true
    commitEventFilter({ text: '' })
  }

  function applyHistogramTimeRange({ createdAfter, createdBefore }: { createdAfter: string; createdBefore: string }) {
    filterTouchedRef.current = true
    setCustomRange(formatDateTimeLocalInput(new Date(createdAfter)), formatDateTimeLocalInput(new Date(createdBefore)))
  }

  function resetEventScope(nextEventScopeKey: string, { force = false }: { force?: boolean } = {}) {
    if (!force && previousGroupKeyRef.current === nextEventScopeKey) {
      return
    }

    previousGroupKeyRef.current = nextEventScopeKey
    seededLatestGroupKeyRef.current = ''
    filterTouchedRef.current = false
    setEventFilterGroupKey('')
    setEventFilterDraft('')
    setEventFilterParams({ text: '' })
    setEventAnchorOverride(null)
    setHighlightedEventIds([])
    setSelectedCanvasSpanId('')
  }

  useEffect(() => {
    resetEventScope(eventScopeKey)
  }, [eventScopeKey])

  useEffect(() => {
    if (eventFilterSearchText === undefined) {
      return
    }

    filterTouchedRef.current = true
    seededLatestGroupKeyRef.current = eventScopeKey
    setEventFilterGroupKey(eventScopeKey)
    const parsedFilter = parseEventFilter({
      facetPaths: activeFacetPaths,
      referenceTimestamp: eventDetail?.group.startedAt ?? latestCreatedAt,
      value: eventFilterSearchText
    })
    syncTimeRangeControlsFromFilter(parsedFilter)
    const nextFilter = stripTimeBounds(parsedFilter)
    setEventFilterDraft(eventFilterInputText(nextFilter))
    setEventFilterParams(nextFilter)
  }, [activeFacetPaths, eventDetail?.group.startedAt, eventFilterSearchText, eventScopeKey, latestCreatedAt])

  useEffect(() => {
    if (
      eventFilterSearchText !== undefined ||
      filterTouchedRef.current ||
      seededLatestGroupKeyRef.current === eventScopeKey
    ) {
      return
    }

    seededLatestGroupKeyRef.current = eventScopeKey
    setEventFilterGroupKey(eventScopeKey)
    setEventFilterDraft('')
    setEventFilterParams({ text: '' })
    setFilterSearch('')
  }, [eventFilterSearchText, eventScopeKey])

  const filteredTraces = traceList
  const emptyFilter = !loadingList && !listError && Boolean(groupSearch) && traceList.length === 0

  const eventTableFilterKey = eventFilterInputText(eventFilterParams)
  const eventTableScrollKey = useMemo(
    () =>
      [
        'observatory-ui-events-scroll',
        viewingGroupedEvents
          ? `/${encodeURIComponent(groupBy)}/${encodeURIComponent(selectedGroupValue)}`
          : '/events',
        eventTableFilterKey,
        selectedTimeRangeCacheKey,
        effectiveEventSortDirection,
        eventAnchorTimestamp,
        eventAnchorEventId
      ].join('\u0000'),
    [effectiveEventSortDirection, eventAnchorEventId, eventAnchorTimestamp, eventTableFilterKey, groupBy, selectedGroupValue, selectedTimeRangeCacheKey, viewingGroupedEvents]
  )
  const selectedEventColumnsForTrace = useMemo(() => {
    if (!eventDetail) return selectedEventColumns
    const available = new Set(['timestamp', 'data', ...eventDetail.fields.map(field => field.path)])
    const kept = selectedEventColumns.filter(path => available.has(path))
    return kept.length > 0 ? kept : [...defaultEventColumns].filter(path => available.has(path))
  }, [eventDetail, selectedEventColumns])
  const selectedEvent =
    displayedEvents.find(event => event.id === selectedEventId) ??
    allEvents.find(event => event.id === selectedEventId) ??
    null
  const eventPayloadQuery = useQuery({
    enabled: Boolean(selectedEventId),
    queryKey: ['logs', observatoryUrl, 'event', selectedEventId],
    queryFn: () => fetchEvent({ apiBaseUrl: observatoryUrl, eventId: selectedEventId }),
    retry: false
  })
  const inspectedEvent = eventPayloadQuery.data?.event ?? (eventPayloadQuery.isFetching ? null : selectedEvent)
  const inspectorPayload = inspectedEvent
    ? {
        title: `${eventName(inspectedEvent.data)} event`,
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
    updateSearch({ eventId: eventId || undefined })
  }

  function selectEvent(event: TraceEvent) {
    setEventSearch(event.id)
    setHighlightedEventIds([event.id])
    setSelectedCanvasSpanId(flamegraph.eventSpanIds[event.id] ?? event.id)
  }

  function inspectSpan(span: FlameSpan) {
    const nextHighlightedEventIds = span.eventIds.length > 0 ? span.eventIds : [span.id]
    const nextEventId = nextHighlightedEventIds[0] ?? ''
    setEventSearch(nextEventId)
    setHighlightedEventIds(nextHighlightedEventIds)
    setSelectedCanvasSpanId(span.id)
    const anchorTimestamp = flamegraph.eventCreatedAt[nextEventId] || (Number.isFinite(span.startMs) ? new Date(span.startMs).toISOString() : '')
    setEventAnchorOverride(nextEventId && anchorTimestamp ? { eventId: nextEventId, key: eventDataKey, timestamp: anchorTimestamp } : null)
  }

  useEffect(() => {
    if (!selectedEventId) {
      setHighlightedEventIds([])
      setSelectedCanvasSpanId('')
      return
    }

    if (!selectedEvent) {
      if (eventPayloadQuery.error instanceof HTTPError && eventPayloadQuery.error.status === 404) {
        setHighlightedEventIds([])
        setSelectedCanvasSpanId('')
        setEventSearch('')
      } else if (eventPayloadQuery.data?.event) {
        setHighlightedEventIds([selectedEventId])
        if (liveMode) {
          return
        }
        const anchorTimestamp = eventPayloadQuery.data.event.createdAt
        if (
          anchorTimestamp &&
          (eventAnchorOverride?.key !== eventDataKey ||
            eventAnchorOverride.eventId !== selectedEventId ||
            eventAnchorOverride.timestamp !== anchorTimestamp)
        ) {
          setEventAnchorOverride({ eventId: selectedEventId, key: eventDataKey, timestamp: anchorTimestamp })
        }
      }
      return
    }

    setHighlightedEventIds([selectedEventId])
    setSelectedCanvasSpanId(flamegraph.eventSpanIds[selectedEventId] ?? selectedEventId)
  }, [eventAnchorOverride, eventDataKey, eventPayloadQuery.data, eventPayloadQuery.error, flamegraph.eventSpanIds, liveMode, selectedEvent, selectedEventId])

  useEffect(() => {
    document.body.style.cursor = dragging ? (dragging === 'flamegraph' ? 'row-resize' : 'col-resize') : ''
    document.body.style.userSelect = dragging ? 'none' : ''
    return () => {
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
    }
  }, [dragging])

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
  }, [dragging, inspectorWidth, runsOpen, runsWidth, setFlamegraphHeight, setInspectorWidth, setRunsWidth])

  return (
    <main className="flex h-full min-h-0 min-w-0 flex-col overflow-hidden bg-black text-[13px] text-neutral-100">
      <header className="relative z-40 flex h-10 shrink-0 items-center gap-2 overflow-visible border-b border-neutral-800 bg-neutral-950 px-2">
        {!sidebarOpen ? (
          <div className="flex shrink-0 items-center gap-2 pr-2">
            <button
              aria-label="Expand navigation"
              className="inline-flex h-7 w-7 shrink-0 items-center justify-center border border-neutral-800 bg-black text-neutral-400 hover:bg-white/[0.04] hover:text-white"
              title="Expand navigation"
              type="button"
              onClick={() => setSidebarOpen(true)}
            >
              <PanelLeftOpen size={15} strokeWidth={1.8} />
            </button>
          </div>
        ) : null}
        <form
          className="flex min-w-0 flex-1 items-center gap-1.5"
          onSubmit={event => {
            event.preventDefault()
            applyEventFilter()
          }}
        >
          <input
            aria-label="Search or filter events"
            className="h-7 min-w-0 flex-1 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none placeholder:text-neutral-600 focus:border-neutral-600"
            value={eventFilterDraft}
            onChange={event => setEventFilterDraft(event.target.value)}
            placeholder='search or filter events, e.g. error service=api "timeout"'
          />
          <button
            aria-label="Apply filter"
            className="inline-flex h-7 w-7 shrink-0 items-center justify-center border border-neutral-800 bg-black text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
            disabled={!eventFilterDirty}
            title="Apply filter"
            type="submit"
          >
            <Check size={13} strokeWidth={1.8} />
          </button>
          {hasEventFilter ? (
            <button
              aria-label="Clear event filter"
              className="inline-flex h-7 w-7 shrink-0 items-center justify-center border border-neutral-800 bg-black text-neutral-400 hover:bg-white/[0.04] hover:text-white"
              title="Clear filter"
              type="button"
              onClick={clearEventFilter}
            >
              <X size={13} strokeWidth={1.8} />
            </button>
          ) : null}
        </form>
        <div className="flex shrink-0 items-center gap-1.5">
          <span className="text-[10px] uppercase tracking-[0.08em] text-neutral-500">Group</span>
          <Popover open={groupSelectOpen} onOpenChange={setGroupSelectOpen}>
            <PopoverTrigger asChild>
              <button
                aria-expanded={groupSelectOpen}
                aria-label="Group by"
                className="flex h-7 w-[190px] items-center justify-between gap-2 border border-neutral-800 bg-black px-2 text-left text-[12px] text-neutral-200 outline-none hover:bg-white/[0.04] focus:border-neutral-600"
                role="combobox"
                type="button"
              >
                <span className="min-w-0 truncate">{groupByLabel || 'No grouping'}</span>
                <ChevronDown size={13} strokeWidth={1.8} className="shrink-0 text-neutral-500" />
              </button>
            </PopoverTrigger>
            <PopoverContent align="start" className="w-[190px] p-1">
              <div className="max-h-[360px] overflow-y-auto" role="listbox">
                <GroupSelectItem
                  selected={!groupBy}
                  value="No grouping"
                  onSelect={() => selectGroupBySearch('')}
                />
                {displayedGroupOptions.map(option => (
                  <GroupSelectItem
                    key={option.path}
                    selected={option.path === groupBy}
                    value={groupOptionLabel(option)}
                    onSelect={() => selectGroupBySearch(option.path)}
                  />
                ))}
              </div>
            </PopoverContent>
          </Popover>
        </div>
        <div className="ml-auto flex shrink-0 items-center justify-end gap-1.5">
          <div className="flex overflow-hidden border border-neutral-800 bg-black">
            {timeRangeOptions.map(option => (
              <button
                key={option.key}
                className={cn(
                  'h-7 border-l border-neutral-800 px-1.5 text-[10px] text-neutral-400 first:border-l-0 hover:bg-white/[0.04] hover:text-white',
                  effectiveTimeRangeKey === option.key && 'bg-neutral-800 text-white'
                )}
                type="button"
                onClick={() => selectTimeRange(option.key)}
              >
                {option.label}
              </button>
            ))}
            <CustomTimeRangePicker
              active={effectiveTimeRangeKey === 'custom'}
              end={effectiveCustomRangeEnd}
              start={effectiveCustomRangeStart}
              onApply={setCustomRange}
            />
          </div>
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
      <section ref={workspaceRef} className="flex min-h-0 min-w-0 flex-1 overflow-hidden">
        {runsOpen && groupBy ? (
          <aside className={cn(panelClass, 'border-r border-neutral-800')} style={{ width: runsWidth, minWidth: runsWidth, maxWidth: runsWidth }}>
          <div className="grid gap-2 border-b border-neutral-800 bg-black/30 px-2 py-2">
            <div className="flex min-w-0 items-center justify-between gap-2">
              <div className="min-w-0 truncate text-[12px] font-medium text-white">{groupByLabel || 'Groups'}</div>
              <div className="shrink-0 text-[11px] text-neutral-600">
                {loadingList ? 'loading' : `${traceList.length} values`}
              </div>
            </div>
            {traceList.length > 0 || filter ? (
              <input
                aria-label="Search groups"
                className="h-7 w-full min-w-0 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none placeholder:text-neutral-600 focus:border-neutral-600"
                value={filter}
                onChange={event => setFilter(event.target.value)}
                placeholder={`exact ${groupByLabel || 'group'} value...`}
              />
            ) : null}
          </div>

          <div ref={groupListRef} className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-1.5 py-1.5">
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
                    resetEventScope(`${groupBy}\u0000${trace.value}`, { force: true })
                    navigateSelectedGroup(trace.value)
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
                  <GroupRowMeta groupBy={groupBy} trace={trace} />
                  {trace.fields ? (
                    <div className="mt-0.5 truncate pl-1.5 text-[11px] text-neutral-600">
                      {previewGroupFields(trace.fields)}
                    </div>
                  ) : null}
                </button>
              ))}
              {loadingList ? (
                <div className="px-3 py-5 text-center text-[12px] text-neutral-600">
                  Loading dimensions...
                </div>
              ) : null}
              {emptyObservatory ? <EmptyState label="No observations yet." /> : null}
              {emptyGroup ? <EmptyState label={emptyGroupLabel} /> : null}
              {emptyFilter ? <EmptyState label="No exact group match." /> : null}
              {groupsQuery.hasNextPage || groupsQuery.isFetchingNextPage ? (
                <div ref={groupLoadMoreRef} className="px-3 py-3 text-center text-[11px] text-neutral-600">
                  {groupsQuery.isFetchingNextPage ? 'Loading more...' : 'Scroll for more'}
                </div>
              ) : null}
            </div>
          </div>
          </aside>
        ) : null}

        {runsOpen && groupBy ? <ResizeHandle onPointerDown={() => setDragging('runs')} /> : null}

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
              <h2 className="truncate">{selectedGroupValue ? `${groupByLabel}=${selectedGroupValue}` : 'Events'}</h2>
            </div>
          </div>

          {hasEventQuery ? (
          <>
          <div className="overflow-hidden border-b border-neutral-800 bg-black" style={{ height: flamegraphHeight, minHeight: flamegraphHeight }}>
            {loadingDetail ? <EmptyState label={viewingGroupedEvents ? 'Loading trace detail.' : 'Loading event histogram.'} /> : null}
            {!loadingDetail && graphMode === 'histogram' && densityQuery.data && hasRenderableDensity(densityQuery.data) ? (
              <DensityHistogramCanvas
                density={densityQuery.data}
                totalCount={summaryQuery.data?.count ?? 0}
                onSelectRange={applyHistogramTimeRange}
              />
            ) : null}
            {!loadingDetail && graphMode === 'histogram' && !hasRenderableDensity(densityQuery.data) ? (
              <EmptyState
                label={
                  densityQuery.data
                    ? (summaryQuery.data?.count ?? 0) > 0
                      ? 'No density data available for this range yet.'
                      : 'No events in selected range.'
                    : 'Loading density histogram.'
                }
              />
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
          </>
          ) : null}

          {hasEventQuery ? (
            <div className="flex items-center gap-2 border-b border-neutral-800 bg-neutral-950 px-2 py-1.5">
              {viewingGroupedEvents ? (
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
              ) : (
                <div className="inline-flex border border-neutral-800 bg-black px-2 py-1 text-[11px] text-white">
                  Histogram
                </div>
              )}
              {viewingGroupedEvents && flamegraphDisabled ? (
                <span className="truncate text-[11px] text-neutral-600">Flamegraph preview capped at 20k events.</span>
              ) : null}
              <QueryPlanBadge
                createFieldError={errorMessage(createFieldDefinitionMutation.error)}
                createMeasureError={errorMessage(createMeasureDefinitionMutation.error)}
                createReportError={errorMessage(createReportDefinitionMutation.error)}
                createSearchError={errorMessage(createSearchDefinitionMutation.error)}
                creatingFieldPath={createFieldDefinitionMutation.variables?.path}
                creatingMeasurePath={createMeasureDefinitionMutation.variables?.path}
                creatingReportKey={
                  createReportDefinitionMutation.variables
                    ? reportRecommendationKey(createReportDefinitionMutation.variables)
                    : undefined
                }
                creatingSearchKey={
                  createSearchDefinitionMutation.variables
                    ? searchRecommendationKey(createSearchDefinitionMutation.variables)
                    : undefined
                }
                plan={eventQueryPlan}
                reportMaterializationStatus={reportMaterializationJob ? materializationJobStatusLabel(reportMaterializationJob) : ''}
                reportMaterializationTitle={reportMaterializationJob ? materializationJobTitle(reportMaterializationJob) : ''}
                onCreateFieldDefinition={recommendation => createFieldDefinitionMutation.mutate(recommendation)}
                onCreateMeasureDefinition={recommendation => createMeasureDefinitionMutation.mutate(recommendation)}
                onCreateReportDefinition={recommendation => createReportDefinitionMutation.mutate(recommendation)}
                onCreateSearchDefinition={recommendation => createSearchDefinitionMutation.mutate(recommendation)}
              />
            </div>
          ) : null}

          {eventDetail ? (
            <EventPanel
              anchorIndex={eventPages[0]?.anchorIndex ?? 0}
              events={displayedEvents}
              emptyLabel={
                hasAppliedEventFilter(eventFilterParams) ? 'No events matched search/filter.' : 'No events.'
              }
              fields={eventDetail.fields}
              freshEventIds={freshEventIds}
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
              sortDirection={effectiveEventSortDirection}
              onLoadMore={() => {
                void eventsQuery.fetchNextPage()
              }}
              onLoadPrevious={() => eventsQuery.fetchPreviousPage().then(() => undefined)}
              onInspect={selectEvent}
              onSetColumns={setSelectedEventColumns}
              onToggleSortDirection={() => setEffectiveEventSortDirection(current => current === 'desc' ? 'asc' : 'desc')}
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
    const type = stringField(event.data.event_type)
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

function groupOptionLabel(option: GroupOption) {
  return option.displayName || displayFacetPath(option.path)
}

function GroupSelectItem({
  onSelect,
  selected,
  value
}: {
  onSelect: () => void
  selected: boolean
  value: string
}) {
  return (
    <button
      aria-selected={selected}
      className={cn(
        'relative flex h-7 w-full items-center px-2 pr-7 text-left text-[12px] text-neutral-300 outline-none hover:bg-white/[0.07] hover:text-white',
        selected && 'text-white'
      )}
      role="option"
      type="button"
      onClick={onSelect}
    >
      <span className="min-w-0 truncate">{value}</span>
      {selected ? (
        <span className="absolute right-2 inline-flex items-center justify-center">
          <Check size={12} strokeWidth={1.8} />
        </span>
      ) : null}
    </button>
  )
}

function isKnownGroupOption(path: string, options: GroupOption[]) {
  if (!path) return false
  const canonicalPath = facetKey(path)
  return (
    options.some(option => option.path === path || facetKey(option.path) === canonicalPath) ||
    groupableFields.some(option => option === path || facetKey(option) === canonicalPath)
  )
}

function isTraceLikeGroup(groupBy: string) {
  return ['trace_id', 'span_id', 'parent_span_id'].includes(facetKey(groupBy))
}

function formatCount(value: number, singular: string, plural = `${singular}s`) {
  const rounded = Math.max(0, Math.floor(value))
  return `${rounded.toLocaleString()} ${rounded === 1 ? singular : plural}`
}

function GroupRowMeta({ groupBy, trace }: { groupBy: string; trace: LogGroupSummary }) {
  const errorCount = trace.errorCount ?? 0
  if (isTraceLikeGroup(groupBy)) {
    return (
      <div className="mt-0.5 flex min-w-0 items-center gap-1.5 pl-1.5 text-[11px] text-neutral-500">
        <span className="shrink-0">{formatCount(trace.count, 'event')}</span>
        {errorCount > 0 ? (
          <>
            <span className="text-neutral-700">&middot;</span>
            <span className="shrink-0 text-red-300">{formatCount(errorCount, 'error')}</span>
          </>
        ) : null}
        <span className="text-neutral-700">&middot;</span>
        <span className="min-w-0 truncate">{formatDurationMs(trace.durationMs)}</span>
      </div>
    )
  }

  return (
    <div className="mt-0.5 flex min-w-0 items-center gap-1.5 pl-1.5 text-[11px] text-neutral-500">
      <span className="shrink-0">{formatCount(trace.count, 'event')}</span>
      <span className="text-neutral-700">&middot;</span>
      <span className={cn('shrink-0', errorCount > 0 ? 'text-red-300' : 'text-neutral-500')}>
        {formatCount(errorCount, 'error')}
      </span>
      <span className="text-neutral-700">&middot;</span>
      <span className="min-w-0 truncate">{formatDurationMs(trace.durationMs)} active</span>
    </div>
  )
}

function identityInitial(label: string) {
  const trimmed = label.trim()
  return (trimmed[0] || '?').toUpperCase()
}

function CustomTimeRangePicker({
  active,
  end,
  onApply,
  start
}: {
  active: boolean
  end: string
  onApply: (start: string, end: string) => void
  start: string
}) {
  const [open, setOpen] = useState(false)
  const [draftStartDate, setDraftStartDate] = useState<Date | undefined>(() => localDateOnly(start))
  const [draftEndDate, setDraftEndDate] = useState<Date | undefined>(() => localDateOnly(end))
  const [draftStartMonth, setDraftStartMonth] = useState<Date>(() => localMonthOnly(start) ?? new Date())
  const [draftEndMonth, setDraftEndMonth] = useState<Date>(() => localMonthOnly(end) ?? new Date())
  const [draftStartTime, setDraftStartTime] = useState(() => timePartFromLocalInput(start))
  const [draftEndTime, setDraftEndTime] = useState(() => timePartFromLocalInput(end))

  const rangeLabel = customRangeLabel(start, end)
  const canApply = Boolean(draftStartDate && draftEndDate && draftStartTime && draftEndTime)

  function resetDraftRange() {
    const nextStartDate = localDateOnly(start)
    const nextEndDate = localDateOnly(end)
    setDraftStartDate(nextStartDate)
    setDraftEndDate(nextEndDate)
    setDraftStartMonth(localMonthOnly(start) ?? nextStartDate ?? new Date())
    setDraftEndMonth(localMonthOnly(end) ?? nextEndDate ?? new Date())
    setDraftStartTime(timePartFromLocalInput(start))
    setDraftEndTime(timePartFromLocalInput(end))
  }

  function handleOpenChange(nextOpen: boolean) {
    setOpen(nextOpen)
    if (nextOpen) resetDraftRange()
  }

  function applyDraft() {
    if (!draftStartDate || !draftEndDate) return
    const nextStart = combineLocalDateAndTime(draftStartDate, draftStartTime)
    const nextEnd = combineLocalDateAndTime(draftEndDate, draftEndTime)
    onApply(nextStart, nextEnd)
    setOpen(false)
  }

  return (
    <Popover open={open} onOpenChange={handleOpenChange}>
      <PopoverTrigger asChild>
        <button
          className={cn(
            'inline-flex h-7 items-center gap-1.5 border-l border-neutral-800 px-1.5 text-[10px] text-neutral-400 hover:bg-white/[0.04] hover:text-white',
            active && 'bg-neutral-800 text-white'
          )}
          type="button"
        >
          <CalendarIcon size={12} strokeWidth={1.8} />
          <span>{active ? rangeLabel : 'Custom'}</span>
        </button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-auto p-0">
        <div className="inline-grid">
          <div className="grid grid-cols-[276px_276px]">
            <div className="w-[276px] border-r border-neutral-800">
              <div className="border-b border-neutral-800 px-3 py-2">
                <div className="text-[11px] uppercase tracking-[0.08em] text-neutral-500">From</div>
                <div className="mt-0.5 truncate text-[12px] text-white">{draftStartDate ? format(draftStartDate, 'MMM d, yyyy') : 'Select date'}</div>
              </div>
              <Calendar
                mode="single"
                month={draftStartMonth}
                selected={draftStartDate}
                onMonthChange={setDraftStartMonth}
                onSelect={setDraftStartDate}
              />
              <div className="border-t border-neutral-800 p-3">
                <TimeField key={draftStartTime} label="Time" value={draftStartTime} onChange={setDraftStartTime} />
              </div>
            </div>
            <div className="w-[276px]">
              <div className="border-b border-neutral-800 px-3 py-2">
                <div className="text-[11px] uppercase tracking-[0.08em] text-neutral-500">To</div>
                <div className="mt-0.5 truncate text-[12px] text-white">{draftEndDate ? format(draftEndDate, 'MMM d, yyyy') : 'Select date'}</div>
              </div>
              <Calendar
                mode="single"
                month={draftEndMonth}
                selected={draftEndDate}
                onMonthChange={setDraftEndMonth}
                onSelect={setDraftEndDate}
              />
              <div className="border-t border-neutral-800 p-3">
                <TimeField key={draftEndTime} label="Time" value={draftEndTime} onChange={setDraftEndTime} />
              </div>
            </div>
          </div>
          <div className="flex items-center justify-end gap-1.5 border-t border-neutral-800 p-3">
            <button
              className="h-8 border border-neutral-800 bg-black px-3 text-[12px] text-neutral-400 hover:bg-white/[0.04] hover:text-white"
              type="button"
              onClick={() => setOpen(false)}
            >
              Cancel
            </button>
              <button
                className="h-8 border border-neutral-700 bg-white px-3 text-[12px] font-medium text-black hover:bg-neutral-200 disabled:border-neutral-900 disabled:bg-black disabled:text-neutral-700"
                disabled={!canApply}
                type="button"
                onClick={applyDraft}
              >
                Apply
              </button>
          </div>
        </div>
      </PopoverContent>
    </Popover>
  )
}

function TimeField({
  label,
  onChange,
  value
}: {
  label: string
  onChange: (value: string) => void
  value: string
}) {
  const parts = timePartsFromLocalTime(value)
  const [draftHour, setDraftHour] = useState(parts.hour)
  const [draftMinute, setDraftMinute] = useState(parts.minute)

  function update(next: Partial<typeof parts>) {
    onChange(localTimeFromParts(next.hour ?? parts.hour, next.minute ?? parts.minute, next.period ?? parts.period))
  }

  function commitHour(value: string) {
    const hour = normalizeTimeSegment(value, 1, 12)
    setDraftHour(hour)
    update({ hour })
  }

  function commitMinute(value: string) {
    const minute = normalizeTimeSegment(value, 0, 59)
    setDraftMinute(minute)
    update({ minute })
  }

  return (
    <div className="grid gap-1">
      <span className="text-[11px] text-neutral-500">{label}</span>
      <div className="grid grid-cols-[1fr_1fr_auto] border border-neutral-800 bg-black">
        <input
          aria-label={`${label} hour`}
          className="h-8 min-w-0 border-r border-neutral-900 bg-transparent px-2 text-center text-[12px] text-white outline-none focus:bg-white/[0.04]"
          inputMode="numeric"
          maxLength={2}
          placeholder="hh"
          value={draftHour}
          onChange={event => setDraftHour(event.target.value.replace(/\D/g, '').slice(0, 2))}
          onBlur={event => commitHour(event.target.value)}
          onKeyDown={event => {
            if (event.key === 'Enter') commitHour(event.currentTarget.value)
          }}
        />
        <input
          aria-label={`${label} minute`}
          className="h-8 min-w-0 border-r border-neutral-900 bg-transparent px-2 text-center text-[12px] text-white outline-none focus:bg-white/[0.04]"
          inputMode="numeric"
          maxLength={2}
          placeholder="mm"
          value={draftMinute}
          onChange={event => setDraftMinute(event.target.value.replace(/\D/g, '').slice(0, 2))}
          onBlur={event => commitMinute(event.target.value)}
          onKeyDown={event => {
            if (event.key === 'Enter') commitMinute(event.currentTarget.value)
          }}
        />
        <button
          aria-label={`${label} period`}
          className="h-8 w-11 text-[12px] text-white hover:bg-white/[0.04]"
          type="button"
          onClick={() => update({ period: parts.period === 'AM' ? 'PM' : 'AM' })}
        >
          {parts.period}
        </button>
      </div>
    </div>
  )
}

function ApiKeyMenu({
  activeApiKeyCount,
  apiKeyName,
  apiKeyRole,
  apiKeys,
  createdApiKey,
  creating,
  error,
  loading,
  revoking,
  onApiKeyName,
  onApiKeyRole,
  onClearCreated,
  onCreate,
  onRevoke
}: {
  activeApiKeyCount: number
  apiKeyName: string
  apiKeyRole: 'admin' | 'service' | 'viewer'
  apiKeys: ApiKeyRecord[]
  createdApiKey: string
  creating: boolean
  error: unknown
  loading: boolean
  revoking: boolean
  onApiKeyName: (value: string) => void
  onApiKeyRole: (value: 'admin' | 'service' | 'viewer') => void
  onClearCreated: () => void
  onCreate: () => void
  onRevoke: (id: number) => void
}) {
  return (
    <div className="grid gap-2 border-b border-neutral-800 pb-2">
      <div className="flex items-center justify-between gap-2 px-2">
        <div className="inline-flex items-center gap-1.5 text-[12px] text-white">
          <KeyRound size={13} strokeWidth={1.8} />
          API keys
        </div>
        <span className="text-[11px] text-neutral-600">{loading ? 'loading' : `${activeApiKeyCount} active`}</span>
      </div>
      <div className="grid grid-cols-[1fr_90px_auto] gap-1.5">
        <input
          className="h-8 min-w-0 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none placeholder:text-neutral-600 focus:border-neutral-600"
          value={apiKeyName}
          onChange={event => onApiKeyName(event.target.value)}
          placeholder="key name"
        />
        <select
          className="h-8 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none focus:border-neutral-600"
          value={apiKeyRole}
          onChange={event => onApiKeyRole(event.target.value as 'admin' | 'service' | 'viewer')}
        >
          <option value="service">service</option>
          <option value="viewer">viewer</option>
          <option value="admin">admin</option>
        </select>
        <button
          className="h-8 border border-neutral-700 bg-black px-2 text-[12px] text-neutral-200 hover:bg-white/[0.04] disabled:border-neutral-900 disabled:text-neutral-700"
          disabled={!apiKeyName.trim() || creating}
          type="button"
          onClick={onCreate}
        >
          Create
        </button>
      </div>
      {createdApiKey ? (
        <div className="flex items-center gap-1.5">
          <input
            readOnly
            className="h-8 min-w-0 flex-1 border border-neutral-800 bg-black px-2 font-mono text-[11px] text-white outline-none"
            value={createdApiKey}
            onFocus={event => event.currentTarget.select()}
          />
          <button
            aria-label="Clear created key"
            className="inline-flex h-8 w-8 shrink-0 items-center justify-center border border-neutral-800 bg-black text-neutral-500 hover:bg-white/[0.04] hover:text-white"
            type="button"
            onClick={onClearCreated}
          >
            <X size={13} strokeWidth={1.8} />
          </button>
        </div>
      ) : null}
      {error ? <div className="px-2 text-[11px] text-red-300">{errorMessage(error)}</div> : null}
      {apiKeys.length > 0 ? (
        <div className="max-h-40 overflow-y-auto border border-neutral-900 bg-black/40">
          {apiKeys.slice(0, 12).map(apiKey => (
            <div
              key={apiKey.id}
              className={cn(
                'flex h-8 items-center gap-2 border-b border-neutral-900 px-2 text-[11px] last:border-b-0',
                apiKey.revoked_at ? 'text-neutral-600' : 'text-neutral-300'
              )}
            >
              <span className="min-w-0 flex-1 truncate">{apiKey.name}</span>
              <span className="text-neutral-600">{apiKey.role}</span>
              <span className="font-mono text-neutral-500">{apiKey.prefix}</span>
              {!apiKey.revoked_at ? (
                <button
                  aria-label={`Revoke ${apiKey.name}`}
                  className="inline-flex h-5 w-5 shrink-0 items-center justify-center text-neutral-500 hover:text-white disabled:text-neutral-700"
                  disabled={revoking}
                  title={`Revoke ${apiKey.name}`}
                  type="button"
                  onClick={() => onRevoke(apiKey.id)}
                >
                  <X size={12} strokeWidth={1.8} />
                </button>
              ) : null}
            </div>
          ))}
        </div>
      ) : null}
    </div>
  )
}

function EmptyState({ label }: { label: string }) {
  return <div className="px-3 py-6 text-center text-neutral-500">{label}</div>
}

function QueryPlanBadge({
  createFieldError,
  createMeasureError,
  createReportError,
  createSearchError,
  creatingFieldPath,
  creatingMeasurePath,
  creatingReportKey,
  creatingSearchKey,
  onCreateFieldDefinition,
  onCreateMeasureDefinition,
  onCreateReportDefinition,
  onCreateSearchDefinition,
  plan,
  reportMaterializationStatus,
  reportMaterializationTitle
}: {
  createFieldError?: string
  createMeasureError?: string
  createReportError?: string
  createSearchError?: string
  creatingFieldPath?: string
  creatingMeasurePath?: string
  creatingReportKey?: string
  creatingSearchKey?: string
  onCreateFieldDefinition?: (recommendation: QueryRecommendation) => void
  onCreateMeasureDefinition?: (recommendation: QueryRecommendation) => void
  onCreateReportDefinition?: (recommendation: QueryRecommendation) => void
  onCreateSearchDefinition?: (recommendation: QueryRecommendation) => void
  plan?: QueryPlanMetadata
  reportMaterializationStatus?: string
  reportMaterializationTitle?: string
}) {
  if (!plan?.planKind) {
    return null
  }

  const sourceLabel = compactSourceTables(plan.sourceTables)
  const fieldRecommendation = plan.recommendations.find(isFieldDefinitionRecommendation)
  const measureRecommendation = plan.recommendations.find(isMeasureDefinitionRecommendation)
  const reportRecommendation = plan.recommendations.find(isReportDefinitionRecommendation)
  const searchRecommendation = plan.recommendations.find(isSearchDefinitionRecommendation)
  const reportKey = reportRecommendation ? reportRecommendationKey(reportRecommendation) : ''
  const searchKey = searchRecommendation ? searchRecommendationKey(searchRecommendation) : ''
  const baseLabel = sourceLabel ? `${planLabel(plan.planKind)} via ${sourceLabel}` : planLabel(plan.planKind)
  const recommendationLabelText = plan.recommendations.length
    ? `Recommend ${recommendationLabel(plan.recommendations[0])}`
    : ''
  const label = recommendationLabelText ? `${baseLabel}: ${recommendationLabelText}` : baseLabel
  const details = [
    `plan: ${plan.planKind}`,
    plan.shapeClass ? `shape: ${plan.shapeClass}` : '',
    plan.surface ? `surface: ${plan.surface}` : '',
    sourceLabel ? `sources: ${sourceLabel}` : '',
    ...plan.eventFilters.map(filterPlanDetail),
    ...plan.recommendations.map(recommendationPlanDetail),
    createFieldError ? `create field failed: ${createFieldError}` : '',
    createMeasureError ? `create measure failed: ${createMeasureError}` : '',
    createReportError ? `create report failed: ${createReportError}` : '',
    createSearchError ? `create search failed: ${createSearchError}` : '',
    reportMaterializationTitle ? `report backfill: ${reportMaterializationTitle}` : '',
    plan.allowStaleServing ? 'stale serving allowed' : '',
    plan.freshnessOverrides?.length ? `freshness overrides: ${plan.freshnessOverrides.join(', ')}` : ''
  ].filter(Boolean).join('\n')
  const rawFallback = isRawFallbackPlan(plan.planKind)
  const hasRecommendation = plan.recommendations.length > 0

  return (
    <span className="ml-auto inline-flex min-w-0 max-w-[520px] items-center gap-1">
      <span
        className={cn(
          'min-w-0 truncate border px-2 py-1 text-[11px]',
          rawFallback || hasRecommendation
            ? 'border-amber-900/70 bg-amber-950/30 text-amber-300'
            : 'border-neutral-800 bg-black text-neutral-500'
        )}
        title={details}
      >
        {label}
      </span>
      {fieldRecommendation && onCreateFieldDefinition ? (
        <button
          className="h-6 shrink-0 border border-amber-900/70 bg-black px-2 text-[11px] text-amber-200 hover:bg-amber-950/40 disabled:text-neutral-700"
          disabled={creatingFieldPath === fieldRecommendation.path}
          title={`Create field definition for ${fieldRecommendation.path}`}
          type="button"
          onClick={() => onCreateFieldDefinition(fieldRecommendation)}
        >
          {creatingFieldPath === fieldRecommendation.path ? 'Creating' : 'Create field'}
        </button>
      ) : null}
      {measureRecommendation && onCreateMeasureDefinition ? (
        <button
          className="h-6 shrink-0 border border-amber-900/70 bg-black px-2 text-[11px] text-amber-200 hover:bg-amber-950/40 disabled:text-neutral-700"
          disabled={creatingMeasurePath === measureRecommendation.path}
          title={`Create measure definition for ${measureRecommendation.path}`}
          type="button"
          onClick={() => onCreateMeasureDefinition(measureRecommendation)}
        >
          {creatingMeasurePath === measureRecommendation.path ? 'Creating' : 'Create measure'}
        </button>
      ) : null}
      {reportRecommendation && onCreateReportDefinition ? (
        <button
          className="h-6 shrink-0 border border-amber-900/70 bg-black px-2 text-[11px] text-amber-200 hover:bg-amber-950/40 disabled:text-neutral-700"
          disabled={creatingReportKey === reportKey}
          title={`Create report definition for ${reportRecommendation.groupBy?.join(', ') || 'this query'}`}
          type="button"
          onClick={() => onCreateReportDefinition(reportRecommendation)}
        >
          {creatingReportKey === reportKey ? 'Creating' : 'Create report'}
        </button>
      ) : null}
      {reportRecommendation && reportMaterializationStatus ? (
        <span
          className="h-6 shrink-0 border border-neutral-800 bg-black px-2 py-1 text-[11px] text-neutral-300"
          title={reportMaterializationTitle}
        >
          {reportMaterializationStatus}
        </span>
      ) : null}
      {searchRecommendation && onCreateSearchDefinition ? (
        <button
          className="h-6 shrink-0 border border-amber-900/70 bg-black px-2 text-[11px] text-amber-200 hover:bg-amber-950/40 disabled:text-neutral-700"
          disabled={creatingSearchKey === searchKey}
          title="Create saved search definition for this query"
          type="button"
          onClick={() => onCreateSearchDefinition(searchRecommendation)}
        >
          {creatingSearchKey === searchKey ? 'Creating' : 'Create search'}
        </button>
      ) : null}
    </span>
  )
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
	const [yScale, setYScale] = useState<DensityYScale>('log')
	const [sizeVersion, setSizeVersion] = useState(0)
	const fromMs = traceTimeMs(density.from)
	const toMs = traceTimeMs(density.to)
  const validRange = Number.isFinite(fromMs) && Number.isFinite(toMs) && toMs >= fromMs
	const durationMs = Math.max(toMs - fromMs, 1)
	const maxScaledCount = Math.max(...density.buckets.map(bucket => scaleDensityCount(bucket.count, yScale)), 1)

	useEffect(() => {
		const root = rootRef.current
		if (!root) return

		let frame = 0
		const handleResize = () => {
			if (frame) cancelAnimationFrame(frame)
			frame = requestAnimationFrame(() => {
				setSizeVersion(current => current + 1)
			})
		}

		handleResize()
		const observer = new ResizeObserver(handleResize)
		observer.observe(root)
		return () => {
			if (frame) cancelAnimationFrame(frame)
			observer.disconnect()
		}
	}, [])

	useEffect(() => {
		const canvas = canvasRef.current
		const root = rootRef.current
		if (!canvas || !root || !validRange) return

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
      const height = Math.max(1, (scaleDensityCount(bucket.count, yScale) / maxScaledCount) * plotHeight)
      const errorHeight = Math.max(0, (scaleDensityCount(bucket.errorCount ?? 0, yScale) / maxScaledCount) * plotHeight)
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
	}, [brush, density, durationMs, fromMs, maxScaledCount, sizeVersion, toMs, validRange, yScale])

  function xToTimestamp(clientX: number) {
    const root = rootRef.current
    if (!root || !validRange) return ''
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
      <div className="absolute left-2 top-2 z-10 text-[11px] uppercase tracking-[0.08em] text-neutral-500">
        Density {totalCount > 200000 ? '200k+' : totalCount} ·{' '}
        <button
          className="text-neutral-300 underline decoration-neutral-700 underline-offset-2 hover:text-white hover:decoration-neutral-300"
          title="Cycle density y scale"
          type="button"
          onClick={event => {
            event.stopPropagation()
            setYScale(current => nextDensityYScale(current))
          }}
          onPointerDown={event => {
            event.stopPropagation()
          }}
        >
          {yScale}
        </button>
      </div>
    </div>
  )
}

function scaleDensityCount(value: number, yScale: DensityYScale) {
  const count = Math.max(0, Number(value) || 0)
  switch (yScale) {
    case 'linear':
      return count
    case 'sqrt':
      return Math.sqrt(count)
    case 'log':
      return Math.log1p(count)
  }
}

function nextDensityYScale(yScale: DensityYScale): DensityYScale {
  switch (yScale) {
    case 'log':
      return 'linear'
    case 'linear':
      return 'sqrt'
    case 'sqrt':
      return 'log'
  }
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
  const baseTimelineWidth = Math.max(viewport.clientWidth - scenePaddingX * 2, 960)
  const baseMsToPx = baseTimelineWidth / Math.max(flamegraph.totalDuration, 1)
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
      const width = span.marker ? eventMarkerWidth : Math.max((span.endMs - span.startMs) * msToPx, 3)
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
        span.marker
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
      const spanWidth = span.marker ? eventMarkerWidth : Math.max((span.endMs - span.startMs) * msToPx, 3)
      const left = span.marker ? baseLeft - spanWidth / 2 : baseLeft
      const x = left - viewport.scrollLeft
      const y = axisHeight + scenePaddingY + span.lane * (rowHeight + rowGap) - viewport.scrollTop
      const selected = span.id === selectedCanvasSpanId
      const error = isErrorPayload(span.payload)

      ctx.fillStyle = mainSpanFill({ error, kind: span.kind, selected })
      ctx.strokeStyle = mainSpanStroke({ error, selected })
      ctx.lineWidth = selected ? 2 : 1
      ctx.fillRect(x, y, spanWidth, rowHeight)
      ctx.strokeRect(x + 0.5, y + 0.5, Math.max(1, spanWidth - 1), rowHeight - 1)

      if (!span.marker && spanWidth > 54) {
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
      element.scrollLeft = clamp(
        element.scrollLeft + deltaX,
        0,
        Math.max(element.scrollWidth - element.clientWidth, 0)
      )
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
        event.preventDefault()
        event.stopPropagation()
        event.stopImmediatePropagation()
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
      style={{ overscrollBehavior: 'none', overscrollBehaviorX: 'none', touchAction: 'none' }}
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
        style={{ overscrollBehavior: 'none', overscrollBehaviorX: 'none' }}
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
  freshEventIds,
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
  sortDirection,
  onInspect,
  onLoadMore,
  onLoadPrevious,
  onSetColumns,
  onToggleSortDirection,
  onToggleColumn
}: {
  anchorIndex: number
  events: TraceEvent[]
  emptyLabel: string
  fields: LogField[]
  freshEventIds: string[]
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
  sortDirection: EventSortDirection
  onInspect: (event: TraceEvent) => void
  onLoadMore: () => void
  onLoadPrevious: () => Promise<void>
  onSetColumns: (paths: string[]) => void
  onToggleSortDirection: () => void
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
  const currentScrollStateKeyRef = useRef(scrollStateKey)
  const selectedSurroundingLoadKeyRef = useRef({ more: '', previous: '' })
  const selectedScrollKeyRef = useRef('')
  const lastScrollTopRef = useRef(0)
  const suppressScrollPaginationRef = useRef(false)
  const userScrolledRef = useRef(false)
  const scrollSaveTimeoutRef = useRef<number | null>(null)
  const highlightedEventIdSet = new Set(highlightedEventIds)
  const freshEventIdSet = new Set(freshEventIds)
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

  function suppressScrollPaginationForProgrammaticScroll() {
    suppressScrollPaginationRef.current = true
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        suppressScrollPaginationRef.current = false
      })
    })
  }

  useEffect(() => {
    if (!selectedEventId) {
      selectedScrollKeyRef.current = ''
      return
    }
    const index = events.findIndex(event => event.id === selectedEventId)
    if (index === -1) {
      return
    }
    const key = `${scrollStateKey}\u0000${selectedEventId}\u0000${selectedEventAlign}\u0000${index}`
    if (selectedScrollKeyRef.current === key) {
      return
    }
    selectedScrollKeyRef.current = key
    suppressScrollPaginationForProgrammaticScroll()
    virtualizer.scrollToIndex(index, { align: selectedEventAlign })
  }, [events, scrollStateKey, selectedEventAlign, selectedEventId, virtualizer])

  useEffect(() => {
    if (selectedEventAlign !== 'center' || !selectedEventId) {
      return
    }
    const index = events.findIndex(event => event.id === selectedEventId)
    if (index === -1) {
      return
    }
    const desiredSurroundingRows = 30
    const loadKey = `${scrollStateKey}\u0000${selectedEventId}\u0000${events.length}`
    if (index < desiredSurroundingRows && hasPrevious && !loadingPrevious) {
      const previousKey = `${loadKey}\u0000previous`
      if (selectedSurroundingLoadKeyRef.current.previous !== previousKey) {
        selectedSurroundingLoadKeyRef.current.previous = previousKey
        void onLoadPrevious()
      }
    }
    if (events.length - index - 1 < desiredSurroundingRows && hasMore && !loadingMore) {
      const moreKey = `${loadKey}\u0000more`
      if (selectedSurroundingLoadKeyRef.current.more !== moreKey) {
        selectedSurroundingLoadKeyRef.current.more = moreKey
        onLoadMore()
      }
    }
  }, [
    events,
    hasMore,
    hasPrevious,
    loadingMore,
    loadingPrevious,
    onLoadMore,
    onLoadPrevious,
    scrollStateKey,
    selectedEventAlign,
    selectedEventId
  ])

  function loadMoreIfNeeded(element: HTMLElement | null, options: { fillOnly?: boolean } = {}) {
    if (!element || !hasMore || loadingMore || loadMoreEventCountRef.current === events.length) {
      return
    }
    const distanceFromBottom = element.scrollHeight - element.scrollTop - element.clientHeight
    if (options.fillOnly ? element.scrollHeight > element.clientHeight + 1 : distanceFromBottom > 800) {
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
          suppressScrollPaginationForProgrammaticScroll()
          scrollRef.current.scrollTop += scrollRef.current.scrollHeight - previousHeight
          lastScrollTopRef.current = scrollRef.current.scrollTop
        }
      })
    })
  }

  useEffect(() => {
    loadMoreIfNeeded(scrollRef.current, { fillOnly: true })
  }, [events.length, hasMore, hasPrevious, loadingMore, loadingPrevious])

  useEffect(() => {
    const element = scrollRef.current
    if (!element) return
    if (currentScrollStateKeyRef.current !== scrollStateKey) {
      currentScrollStateKeyRef.current = scrollStateKey
      lastScrollTopRef.current = 0
      userScrolledRef.current = false
    }
    if (selectedEventAlign === 'center') return
    if (userScrolledRef.current) return
    suppressScrollPaginationForProgrammaticScroll()
    element.scrollTop = savedScrollTop
    lastScrollTopRef.current = element.scrollTop
  }, [savedScrollTop, scrollStateKey, selectedEventAlign])

  useEffect(() => {
    if (selectedEventId || anchoredScrollKeyRef.current === scrollStateKey || events.length === 0) {
      return
    }
    anchoredScrollKeyRef.current = scrollStateKey
    suppressScrollPaginationForProgrammaticScroll()
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
    <section className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden bg-neutral-950">
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
      <div className="min-h-0 min-w-0 flex-1 overflow-x-auto overflow-y-hidden">
        <div className="flex h-full min-w-[640px] flex-col">
          <div
            className="grid shrink-0 gap-3 border-b border-neutral-800 bg-neutral-950 px-3 py-2 text-[10px] uppercase tracking-[0.08em] text-neutral-500"
            style={{ gridTemplateColumns }}
          >
            {selectedColumns.map(path => (
              path === 'timestamp' ? (
                <button
                  key={path}
                  aria-label={`Sort timestamp ${sortDirection === 'desc' ? 'ascending' : 'descending'}`}
                  className="inline-flex min-w-0 items-center gap-1 text-left uppercase text-neutral-400 outline-none hover:text-white focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-neutral-600 disabled:cursor-not-allowed disabled:text-neutral-600 disabled:hover:text-neutral-600"
                  title={`Timestamp ${sortDirection === 'desc' ? 'descending' : 'ascending'}`}
                  type="button"
                  onClick={onToggleSortDirection}
                >
                  <span className="truncate">{path}</span>
                  {sortDirection === 'desc' ? <ArrowDown size={11} strokeWidth={1.8} /> : <ArrowUp size={11} strokeWidth={1.8} />}
                </button>
              ) : (
                <span key={path} className="truncate">{path}</span>
              )
            ))}
          </div>
          <div
            ref={scrollRef}
            className="min-h-0 flex-1 overflow-y-auto overflow-x-hidden overscroll-contain"
            onScroll={event => {
              const nextScrollTop = Math.round(event.currentTarget.scrollTop)
              const previousScrollTop = lastScrollTopRef.current
              const suppressPagination = suppressScrollPaginationRef.current
              suppressScrollPaginationRef.current = false
              lastScrollTopRef.current = nextScrollTop
              if (!suppressPagination) {
                userScrolledRef.current = true
                if (nextScrollTop < previousScrollTop) {
                  loadPreviousIfNeeded(event.currentTarget)
                } else if (nextScrollTop > previousScrollTop) {
                  loadMoreIfNeeded(event.currentTarget)
                }
              }
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
                      'absolute left-0 top-0 grid w-full gap-3 border-b border-neutral-900 px-3 py-2 text-left text-[13px] leading-5 outline-none hover:bg-white/[0.03] focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-neutral-600',
                      error && 'border-l-2 border-l-red-400 bg-red-950/25 ring-1 ring-inset ring-red-500/35 hover:bg-red-950/35',
                      freshEventIdSet.has(event.id) && 'nt-live-row',
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
                    onClick={clickEvent => {
                      clickEvent.currentTarget.blur()
                      onInspect(event)
                    }}
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

function buildFlamegraph(events: TraceEvent[], domain?: TimeDomain | null): Flamegraph {
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
    const type = stringField(event.data.event_type)
    const boundedSpan =
      type === 'span' ||
      (Boolean(stringField(event.data.spanId)) &&
        Boolean(stringField(event.data.startedAt)) &&
        (Boolean(stringField(event.data.endedAt)) || typeof event.data.durationMs === 'number'))
    if (type === 'span_start' || type === 'span_end' || boundedSpan) {
      const id = stringField(event.data.spanId) || event.id
      const current = spanCandidates.get(id)
      const nextStart = stringField(event.data.startedAt) || event.createdAt
      const explicitEnd = stringField(event.data.endedAt)
      const durationMs = typeof event.data.durationMs === 'number' ? event.data.durationMs : Number.NaN
      const nextEnd =
        type === 'span_end' || boundedSpan
          ? explicitEnd || (Number.isFinite(durationMs) && durationMs > 0 ? new Date(traceTimeMs(nextStart) + durationMs).toISOString() : undefined)
          : undefined
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
      marker: true,
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

  const spanMinStart = Math.min(...spans.map(span => span.startMs))
  const spanMaxEnd = Math.max(...spans.map(span => span.endMs))
  const domainFromMs = domain ? traceTimeMs(domain.from) : Number.NaN
  const domainToMs = domain ? traceTimeMs(domain.to) : Number.NaN
  const minStart = Number.isFinite(domainFromMs) ? domainFromMs : spanMinStart
  const maxEnd = Number.isFinite(domainToMs) ? domainToMs : spanMaxEnd
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
    marker: false,
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
  nanotrace?: {
    query?: {
      allowStaleServing?: boolean
      eventFilters?: QueryFilterPlan[]
      freshnessOverrides?: string[]
      planKind?: string
      recommendations?: QueryRecommendation[]
      shapeClass?: string
      sourceTables?: string[]
      surface?: string
    }
  }
}

function queryPlanMetadata<T>(response: ClickHouseResponse<T>): QueryPlanMetadata | undefined {
  const query = response.nanotrace?.query
  if (!query?.planKind) {
    return undefined
  }
  return {
    allowStaleServing: Boolean(query.allowStaleServing),
    eventFilters: Array.isArray(query.eventFilters) ? query.eventFilters : [],
    freshnessOverrides: Array.isArray(query.freshnessOverrides) ? query.freshnessOverrides : [],
    planKind: query.planKind,
    recommendations: Array.isArray(query.recommendations) ? query.recommendations : [],
    shapeClass: query.shapeClass,
    sourceTables: Array.isArray(query.sourceTables) ? query.sourceTables : [],
    surface: query.surface
  }
}

function compactSourceTables(sourceTables: string[]) {
  return sourceTables
    .map(table => table.split('.').filter(Boolean).at(-1) ?? table)
    .filter(Boolean)
    .filter((table, index, tables) => tables.indexOf(table) === index)
    .join(', ')
}

function planLabel(planKind: string) {
  return planKind
    .split('_')
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

function filterPlanDetail(filter: QueryFilterPlan) {
  const path = filter.path || filter.role || 'filter'
  const operator = filter.operator ? ` ${filter.operator}` : ''
  const negated = filter.negated ? 'not ' : ''
  const route = filter.route ? ` -> ${filter.route}` : ''
  const strategy = filter.strategy ? ` (${filter.strategy})` : ''
  const scope = filter.scope ? ` scope=${filter.scope}` : ''
  return `${path}: ${negated}${operator.trim()}${route}${strategy}${scope}`.trim()
}

function recommendationLabel(recommendation: QueryRecommendation) {
  const target = recommendation.targetType || recommendation.targetTable || recommendation.kind || 'target'
  const path = recommendation.path ? ` ${recommendation.path}` : ''
  return `${planLabel(target)}${path}`
}

function recommendationPlanDetail(recommendation: QueryRecommendation) {
  const parts = [
    `recommend: ${recommendation.kind || 'promotion'}`,
    recommendation.targetType ? `target=${recommendation.targetType}` : '',
    recommendation.targetTable ? `table=${recommendation.targetTable}` : '',
    recommendation.path ? `path=${recommendation.path}` : '',
    recommendation.groupBy?.length ? `groupBy=${recommendation.groupBy.join(', ')}` : '',
    recommendation.source ? `source=${recommendation.source}` : '',
    recommendation.action ? `action=${recommendation.action}` : '',
    recommendation.reason || ''
  ].filter(Boolean)
  return parts.join(' | ')
}

function isFieldDefinitionRecommendation(recommendation: QueryRecommendation) {
  return recommendation.targetType === 'field' && Boolean(recommendation.path?.trim())
}

function isMeasureDefinitionRecommendation(recommendation: QueryRecommendation) {
  return recommendation.targetType === 'measure' && Boolean(recommendation.path?.trim())
}

function isReportDefinitionRecommendation(recommendation: QueryRecommendation) {
  return (
    recommendation.targetType === 'report' &&
    recommendation.targetTable === 'report_results' &&
    Array.isArray(recommendation.groupBy) &&
    recommendation.groupBy.some(path => isDefinitionPath(path))
  )
}

function isSearchDefinitionRecommendation(recommendation: QueryRecommendation) {
  return recommendation.targetType === 'search'
}

function reportRecommendationKey(recommendation: QueryRecommendation) {
  return (recommendation.groupBy ?? []).join('|') || recommendation.reason || 'report'
}

function reportDefinitionIdFromRecommendation({
  groupBy,
  recommendation,
  selectedGroupValue
}: {
  groupBy: string
  recommendation: QueryRecommendation
  selectedGroupValue: string
}) {
  const groupByPaths = uniqueDefinitionPaths(
    (recommendation.groupBy?.length ? recommendation.groupBy : [groupBy])
      .map(path => facetKey(path))
      .filter(isDefinitionPath)
  )
  return groupByPaths.length > 0
    ? definitionIdentifierFromParts(['events', ...groupByPaths, selectedGroupValue || 'all'])
    : ''
}

function searchRecommendationKey(recommendation: QueryRecommendation) {
  return recommendation.path || recommendation.reason || recommendation.action || 'search'
}

function isRawFallbackPlan(planKind: string) {
  return planKind === 'raw_events' || planKind === 'raw_text_scan' || planKind === 'table_scan'
}

function latestMaterializationJobForTarget(jobs: MaterializationJobRecord[], targetType: string, targetId: string) {
  return jobs
    .filter(job => job.target_type === targetType && job.target_id === targetId)
    .sort((left, right) => right.updated_at.localeCompare(left.updated_at))[0] ?? null
}

function materializationJobStatusLabel(job: MaterializationJobRecord) {
  if (job.total_chunks > 0 && job.completed_chunks < job.total_chunks && job.status !== 'completed') {
    return `${job.status} ${job.completed_chunks}/${job.total_chunks}`
  }
  return job.status
}

function materializationJobTitle(job: MaterializationJobRecord) {
  const rows = `${job.rows_written.toLocaleString()} written / ${job.rows_scanned.toLocaleString()} scanned`
  return [
    `${job.target_type}/${job.target_id}`,
    `status: ${job.status}`,
    `chunks: ${job.completed_chunks}/${job.total_chunks}`,
    rows,
    `window: ${formatDateTimeUs(job.source_start)} -> ${formatDateTimeUs(job.source_end)}`,
    `updated: ${formatDateTimeUs(job.updated_at)}`
  ].join('\n')
}

type EventsQueryRequest = {
  buckets?: number
  filter?: ParsedEventFilter
  groupBy?: string
  limit?: number
  offset?: number
  orderBy?: {
    direction?: EventSortDirection
    group?: GroupSortKey
  }
  page?: EventPageParam & { eventId?: string }
  search?: string
  selectedGroupValue?: string
  sort?: {
    direction?: EventSortDirection
    group?: GroupSortKey
  }
  timeRange?: ResolvedTimeRange
  view: 'density' | 'event' | 'events' | 'flamegraph' | 'group_options' | 'groups' | 'latest' | 'summary'
}

type TimeDomain = {
  from: string
  to: string
}

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

type FlamegraphEventRow = {
  duration_ms?: number
  end_time?: string
  event_id: string
  event_type?: string
  name?: string
  parent_span_id?: string
  signal?: string
  span_id?: string
  start_time?: string
  timestamp: string
  trace_id?: string
}

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
  const response = await postEventsQuery<{
    aggregateEnabled?: boolean
    capped: boolean
    cardinality: number
    indexEnabled?: boolean
    path: string
    servingMode?: string
    source?: string
    valueType: string
  }>({
    apiBaseUrl,
    request: { limit, view: 'group_options' }
  })
  const dynamicFields = (response.data ?? []).map(field => ({
    cardinality: Number(field.cardinality) || 0,
    capped: Boolean(field.capped),
    aggregateEnabled: Boolean(field.aggregateEnabled),
    indexEnabled: Boolean(field.indexEnabled),
    path: displayFacetPath(field.path),
    servingMode: field.servingMode,
    source: field.source,
    valueType: field.valueType
  }))
  if (dynamicFields.length > 0) {
    return { fields: mergeGroupOptions(dynamicFields, limit) }
  }
  return {
    fields: groupableFields.slice(0, limit).map(path => ({
      cardinality: 0,
      capped: false,
      path,
      servingMode: 'raw',
      source: 'raw'
    }))
  }
}

async function fetchGroups({
  apiBaseUrl,
  groupBy,
  limit,
  offset,
  search,
  sortKey,
  timeRange
}: {
  apiBaseUrl: string
  groupBy: string
  limit: number
  offset: number
  search: string
  sortKey: GroupSortKey
  timeRange: ResolvedTimeRange
}): Promise<LogGroupPage> {
  const response = await postEventsQuery<{
    count: number
    durationMs: number
    endedAt: string
    errorCount: number
    startedAt: string
    value: string
  }>({
    apiBaseUrl,
    request: {
      groupBy,
      limit,
      offset,
      orderBy: { group: sortKey },
      search,
      timeRange,
      view: 'groups'
    }
  })

  return groupPageFromResponse(response.data ?? [], groupBy, limit, offset)
}

function fieldValueTimeRangeWhereClause(timeRange: ResolvedTimeRange) {
  const clauses = []
  if (timeRange.createdAfter) clauses.push("timestamp >= {created_after:DateTime64(3, 'UTC')}")
  if (timeRange.createdBefore) clauses.push("timestamp <= {created_before:DateTime64(3, 'UTC')}")
  if (timeRange.lookbackMinutes) clauses.push('timestamp >= now64(3) - toIntervalMinute({lookback_minutes:UInt64})')
  return clauses.join(' AND ')
}

function fieldValueOrderByClause(sortKey: GroupSortKey) {
  switch (sortKey) {
    case 'value':
      return 'ORDER BY value ASC'
    case 'duration':
      return 'ORDER BY durationMs DESC, endedAt DESC, value ASC'
    case 'recent':
    case 'count':
      return 'ORDER BY endedAt DESC, value ASC'
  }
}

function groupPageFromResponse(
  rows: Array<{
    count: number
    durationMs: number
    endedAt: string
    errorCount?: number
    startedAt: string
    value: string
  }>,
  groupBy: string,
  limit: number,
  offset: number
): LogGroupPage {
  const pageRows = rows.slice(0, limit)
  const groups = pageRows.map(group => ({
    groupBy,
    value: group.value,
    startedAt: normalizeTimestamp(group.startedAt),
    endedAt: normalizeTimestamp(group.endedAt),
    durationMs: Number(group.durationMs) || 0,
    count: Number(group.count) || 0,
    errorCount: Number(group.errorCount) || 0
  }))
  return {
    groups,
    nextOffset: rows.length > limit ? offset + limit : undefined
  }
}

function groupOrderByClause({
  groupBy,
  hasErrorCount,
  sortKey
}: {
  groupBy: string
  hasErrorCount: boolean
  sortKey: GroupSortKey
}) {
  const errorTie = hasErrorCount ? ', errorCount DESC' : ''
  switch (sortKey) {
    case 'count':
      return `ORDER BY count DESC${errorTie}, value ASC`
    case 'duration':
      return `ORDER BY durationMs DESC, count DESC${errorTie}, value ASC`
    case 'value':
      return 'ORDER BY value ASC'
    case 'recent':
      return isTraceLikeGroup(groupBy)
        ? `ORDER BY endedAt DESC, count DESC${errorTie}, value ASC`
        : `ORDER BY count DESC${errorTie}, value ASC`
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
  const response = await postEventsQuery<{ lastCreatedAt: string }>({
    apiBaseUrl,
    request: {
      groupBy,
      selectedGroupValue,
      view: 'latest'
    }
  })
  return { lastCreatedAt: normalizeTimestamp(response.data?.[0]?.lastCreatedAt) }
}

async function fetchSummary({
  apiBaseUrl,
  eventFilter,
  groupBy,
  selectedGroupValue,
  timeRange
}: {
  apiBaseUrl: string
  eventFilter: ParsedEventFilter
  groupBy: string
  selectedGroupValue: string
  timeRange: ResolvedTimeRange
}): Promise<LogSummary> {
  const response = await postEventsQuery<{ count: number }>({
    apiBaseUrl,
    request: {
      filter: eventFilter,
      groupBy,
      selectedGroupValue,
      timeRange,
      view: 'summary'
    }
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
  selectedGroupValue,
  sortDirection,
  timeRange
}: {
  apiBaseUrl: string
  eventFilter: ParsedEventFilter
  groupBy: string
  limit: number
  pageParam: EventPageParam
  selectedGroupValue: string
  sortDirection: EventSortDirection
  timeRange?: ResolvedTimeRange
}): Promise<LogEventsPage> {
  const response = await postEventsQuery<FlamegraphEventRow>({
    apiBaseUrl,
    request: {
      filter: eventFilter,
      groupBy,
      limit,
      orderBy: { direction: sortDirection },
      page: pageParam,
      selectedGroupValue,
      timeRange,
      view: 'events'
    }
  })
  const events = (response.data ?? []).map(rowToFlamegraphEvent)
  const loadedTowardTop = sortDirection === 'desc' ? Boolean(pageParam.after) : Boolean(pageParam.before)

  return {
    events,
    fields: orderLogFields(inferLogFields(events)),
    group: pageGroupSummary({ events, groupBy, selectedGroupValue }),
    nextCursor: events.length >= limit ? eventCursor(events[events.length - 1]) : undefined,
    prevCursor: loadedTowardTop
      ? events.length >= limit ? eventCursor(events[0]) : undefined
      : pageParam.around
        ? eventCursor(events[0])
        : undefined,
    queryPlan: queryPlanMetadata(response)
  }
}

function eventCursor(event: TraceEvent | undefined): EventCursor | undefined {
  return event ? { createdAt: event.createdAt, eventId: event.id } : undefined
}

async function fetchFlamegraph({
  apiBaseUrl,
  eventFilter,
  groupBy,
  maxSpans,
  selectedGroupValue,
  timeRange
}: {
  apiBaseUrl: string
  eventFilter: ParsedEventFilter
  groupBy: string
  maxSpans: number
  selectedGroupValue: string
  timeRange: ResolvedTimeRange
}): Promise<LogFlamegraph> {
  const response = await postEventsQuery<FlamegraphEventRow>({
    apiBaseUrl,
    request: {
      filter: eventFilter,
      groupBy,
      limit: maxSpans,
      selectedGroupValue,
      timeRange,
      view: 'flamegraph'
    }
  })
  const events = (response.data ?? []).map(rowToFlamegraphEvent)
  const domain = queryTimeDomain({ eventFilter, fallbackFrom: events[0]?.createdAt, fallbackTo: events[events.length - 1]?.createdAt, timeRange })
  const flamegraph = buildFlamegraph(events, domain)
  return {
    ...flamegraph,
    capped: events.length >= maxSpans,
    spanCount: flamegraph.rows.reduce((count, row) => count + row.length, 0)
  }
}

async function fetchDensity({
  apiBaseUrl,
  buckets,
  eventFilter,
  groupBy,
  selectedGroupValue,
  timeRange
}: {
  apiBaseUrl: string
  buckets: number
  eventFilter: ParsedEventFilter
  groupBy: string
  selectedGroupValue: string
  timeRange: ResolvedTimeRange
}): Promise<LogDensity> {
  const density = await postEventsQuery<{ bucket: number; count: number; errorCount: number }>({
    apiBaseUrl,
    request: {
      buckets,
      filter: eventFilter,
      groupBy,
      selectedGroupValue,
      timeRange,
      view: 'density'
    }
  })
  const rows = density.data ?? []
  const first = rows[0]
  const last = rows[rows.length - 1]
  const firstStart = first ? new Date(Number(first.bucket)).toISOString() : ''
  const lastStart = last ? new Date(Number(last.bucket)).toISOString() : ''
  const domain = queryTimeDomain({ eventFilter, fallbackFrom: firstStart, fallbackTo: lastStart, timeRange })
  if (!rows.length || !domain) return { bucketMs: 1, buckets: [], from: '', to: '' }
  const bucketMs = rows.length > 1
    ? Math.max(1, Number(rows[1]!.bucket) - Number(rows[0]!.bucket))
    : niceTimeInterval(Math.max(1, (Date.parse(domain.to) - Date.parse(domain.from)) / buckets))
  return {
    bucketMs,
    buckets: rows.map(bucket => ({
      count: Number(bucket.count) || 0,
      errorCount: Number(bucket.errorCount) || 0,
      start: new Date(Number(bucket.bucket)).toISOString()
    })),
    from: domain.from,
    to: domain.to
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
      credentials: 'include',
      headers: queryHeaders(),
      method: 'GET'
    })
    if (response.ok) {
      const row = await response.json() as EventRow
      return { event: rowToTraceEvent(row) }
    }
  } catch {
    // Fall back to the read-only query endpoint when direct event reconstruction fails.
  }

  const response = await postEventsQuery<EventRow>({
    apiBaseUrl,
    request: {
      page: { eventId },
      view: 'event'
    }
  })
  const row = response.data?.[0]
  if (!row) throw new HTTPError({ message: 'event not found', status: 404 })
  return { event: rowToTraceEvent(row) }
}

async function postEventsQuery<T>({
  apiBaseUrl,
  request
}: {
  apiBaseUrl: string
  request: EventsQueryRequest
}): Promise<ClickHouseResponse<T>> {
  const response = await fetch(eventsQueryUrl(apiBaseUrl), {
    body: JSON.stringify({ type: 'events', ...request }),
    credentials: 'include',
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

async function createFieldDefinitionFromRecommendation({
  apiBaseUrl,
  recommendation
}: {
  apiBaseUrl: string
  recommendation: QueryRecommendation
}): Promise<DefinitionRecord> {
  const path = recommendation.path?.trim()
  if (!path) throw new HTTPError({ message: 'field recommendation missing path', status: 400 })
  const response = await fetch(definitionsUrl(apiBaseUrl), {
    body: JSON.stringify({
      config: {
        path,
        value_type: 'string'
      },
      kind: 'field',
      mode: 'facet',
      name: definitionIdentifierFromParts(['field', path])
    }),
    credentials: 'include',
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
  const body = (await response.json()) as { definition?: DefinitionRecord }
  if (!body.definition) throw new HTTPError({ message: 'definition response missing definition', status: 502 })
  return body.definition
}

async function createMeasureDefinitionFromRecommendation({
  apiBaseUrl,
  recommendation
}: {
  apiBaseUrl: string
  recommendation: QueryRecommendation
}): Promise<DefinitionRecord> {
  const path = recommendation.path?.trim()
  if (!path) throw new HTTPError({ message: 'measure recommendation missing path', status: 400 })
  const response = await fetch(definitionsUrl(apiBaseUrl), {
    body: JSON.stringify({
      config: {
        path,
        value_type: 'number'
      },
      kind: 'measure',
      mode: 'measure',
      name: definitionIdentifierFromParts(['measure', path])
    }),
    credentials: 'include',
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
  const body = (await response.json()) as { definition?: DefinitionRecord }
  if (!body.definition) throw new HTTPError({ message: 'definition response missing definition', status: 502 })
  return body.definition
}

async function createSearchDefinitionFromRecommendation({
  apiBaseUrl,
  eventFilter,
  includeSnippets,
  query,
  recommendation,
  requireAllTerms,
  searchMode
}: {
  apiBaseUrl: string
  eventFilter: ParsedEventFilter
  includeSnippets: boolean
  query: string
  recommendation: QueryRecommendation
  requireAllTerms: boolean
  searchMode: EventSearchMode
}): Promise<DefinitionRecord> {
  const rankedQuery = query.trim()
  const filterText = eventFilter.text.trim()
  const savedQuery = rankedQuery || filterText
  if (!savedQuery) throw new HTTPError({ message: 'search recommendation missing query text', status: 400 })
  const path = recommendation.path?.trim()
  const response = await fetch(definitionsUrl(apiBaseUrl), {
    body: JSON.stringify({
      config: {
        include_snippets: rankedQuery ? includeSnippets : true,
        query: savedQuery,
        recommendation: {
          ...(recommendation.action ? { action: recommendation.action } : {}),
          ...(recommendation.reason ? { reason: recommendation.reason } : {}),
          ...(recommendation.source ? { source: recommendation.source } : {}),
          ...(recommendation.targetTable ? { target_table: recommendation.targetTable } : {})
        },
        require_all_terms: rankedQuery ? requireAllTerms : false,
        search_mode: rankedQuery ? searchMode : 'phrase',
        ...(path && isDefinitionPath(path) ? { path } : {})
      },
      kind: 'search',
      mode: 'saved',
      name: definitionIdentifierFromParts(['search', path || savedQuery])
    }),
    credentials: 'include',
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
  const body = (await response.json()) as { definition?: DefinitionRecord }
  if (!body.definition) throw new HTTPError({ message: 'definition response missing definition', status: 502 })
  return body.definition
}

async function createReportDefinitionFromRecommendation({
  apiBaseUrl,
  eventFilter,
  groupBy,
  recommendation,
  selectedGroupValue,
  timeRange
}: {
  apiBaseUrl: string
  eventFilter: ParsedEventFilter
  groupBy: string
  recommendation: QueryRecommendation
  selectedGroupValue: string
  timeRange: ResolvedTimeRange
}): Promise<DefinitionRecord> {
  const groupByPaths = uniqueDefinitionPaths(
    (recommendation.groupBy?.length ? recommendation.groupBy : [groupBy])
      .map(path => facetKey(path))
      .filter(isDefinitionPath)
  )
  if (groupByPaths.length === 0) {
    throw new HTTPError({ message: 'report recommendation missing groupBy path', status: 400 })
  }
  const domain = queryTimeDomain({ eventFilter, timeRange })
  if (!domain) {
    throw new HTTPError({ message: 'report recommendation missing backfill window', status: 400 })
  }
  const reportId = definitionIdentifierFromParts(['events', ...groupByPaths, selectedGroupValue || 'all'])
  const match = reportDefinitionMatch({ eventFilter, groupBy, selectedGroupValue })
  const response = await fetch(definitionsUrl(apiBaseUrl), {
    body: JSON.stringify({
      config: {
        ...(match ? { match } : {}),
        outputs: [
          {
            bucket_seconds: 60,
            dimensions: groupByPaths.map(path => ({
              name: path,
              value: { path }
            })),
            metrics: [
              { name: 'events', op: 'count' },
              { name: 'errors', op: 'error_count' }
            ],
            report_id: reportId,
            target: 'report_results'
          }
        ]
      },
      kind: 'report',
      mode: 'summary',
      name: reportId
    }),
    credentials: 'include',
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
  const body = (await response.json()) as { definition?: DefinitionRecord }
  if (!body.definition) throw new HTTPError({ message: 'definition response missing definition', status: 502 })
  await queueMaterializationJobForDefinition({
    apiBaseUrl,
    definitionId: body.definition.definition_id,
    sourceEnd: domain.to,
    sourceStart: domain.from
  })
  return body.definition
}

async function queueMaterializationJobForDefinition({
  apiBaseUrl,
  definitionId,
  sourceEnd,
  sourceStart
}: {
  apiBaseUrl: string
  definitionId: string
  sourceEnd: string
  sourceStart: string
}): Promise<void> {
  const response = await fetch(materializationsUrl(apiBaseUrl, definitionId), {
    body: JSON.stringify({
      chunk_seconds: 3600,
      source_end: sourceEnd,
      source_start: sourceStart
    }),
    credentials: 'include',
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
}

async function fetchMaterializationJobs({ apiBaseUrl }: { apiBaseUrl: string }): Promise<{ jobs: MaterializationJobRecord[] }> {
  const response = await fetch(backfillsUrl(apiBaseUrl), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({
      message: text || response.statusText,
      status: response.status
    })
  }
  const body = (await response.json()) as { backfills?: MaterializationJobRecord[] }
  return { jobs: body.backfills ?? [] }
}

async function fetchAuthMe({ apiBaseUrl }: { apiBaseUrl: string }): Promise<AuthIdentity> {
  const response = await fetch(authUrl(apiBaseUrl, '/me'), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  return (await response.json()) as AuthIdentity
}

async function requestLoginLink({
  apiBaseUrl,
  email
}: {
  apiBaseUrl: string
  email: string
}): Promise<{ ok: boolean }> {
  const returnTo =
    typeof window === 'undefined'
      ? '/'
      : `${window.location.pathname}${window.location.search}${window.location.hash}`
  const response = await fetch(authUrl(apiBaseUrl, '/login'), {
    body: JSON.stringify({ email: email.trim(), return_to: returnTo }),
    credentials: 'include',
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  return (await response.json()) as { ok: boolean }
}

async function fetchApiKeys({ apiBaseUrl }: { apiBaseUrl: string }): Promise<{ api_keys: ApiKeyRecord[] }> {
  const response = await fetch(apiKeysUrl(apiBaseUrl), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  return (await response.json()) as { api_keys: ApiKeyRecord[] }
}

async function createApiKey({
  apiBaseUrl,
  name,
  role
}: {
  apiBaseUrl: string
  name: string
  role: 'admin' | 'service' | 'viewer'
}): Promise<CreatedApiKey> {
  const response = await fetch(apiKeysUrl(apiBaseUrl), {
    body: JSON.stringify({ name: name.trim(), role }),
    credentials: 'include',
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  return (await response.json()) as CreatedApiKey
}

async function revokeApiKey({
  apiBaseUrl,
  id
}: {
  apiBaseUrl: string
  id: number
}): Promise<ApiKeyRecord> {
  const response = await fetch(`${apiKeysUrl(apiBaseUrl)}/${id}`, {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'DELETE'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  const body = (await response.json()) as { api_key?: ApiKeyRecord }
  if (!body.api_key) throw new HTTPError({ message: 'API key response missing api_key', status: 502 })
  return body.api_key
}

function eventsQueryUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/v1/query` : '/v1/query'
}

function eventUrl(apiBaseUrl: string, eventId: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  const path = `/v1/events/${encodeURIComponent(eventId)}`
  return base ? `${base}${path}` : path
}

function definitionsUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/v1/definitions` : '/v1/definitions'
}

function materializationsUrl(apiBaseUrl: string, definitionId: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  const path = `/v1/definitions/${encodeURIComponent(definitionId)}/backfills`
  return base ? `${base}${path}` : path
}

function backfillsUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/v1/backfills` : '/v1/backfills'
}

function reportDefinitionMatch({
  eventFilter,
  groupBy,
  selectedGroupValue
}: {
  eventFilter: ParsedEventFilter
  groupBy: string
  selectedGroupValue: string
}) {
  const predicates: JsonObject[] = []
  const selectedGroupPath = facetKey(groupBy)
  if (selectedGroupValue && isDefinitionPath(selectedGroupPath)) {
    predicates.push({ op: 'eq', path: selectedGroupPath, value: selectedGroupValue })
  }
  for (const facet of eventFilter.facets ?? []) {
    if (facet.negated) continue
    const path = facetKey(facet.path)
    if (!isDefinitionPath(path)) continue
    if ((facet.operator ?? 'eq') === 'eq' && facet.value) {
      predicates.push({ op: 'eq', path, value: facet.value })
    } else if (facet.operator === 'in') {
      const values = (facet.values ?? []).filter(Boolean)
      if (values.length > 0) predicates.push({ op: 'in', path, value: values })
    }
  }
  const uniquePredicates = uniqueJsonObjects(predicates)
  return uniquePredicates.length > 0 ? { all: uniquePredicates } : null
}

function uniqueDefinitionPaths(paths: string[]) {
  return Array.from(new Set(paths.filter(isDefinitionPath)))
}

function uniqueJsonObjects<T extends JsonObject>(values: T[]) {
  const seen = new Set<string>()
  return values.filter(value => {
    const key = JSON.stringify(value)
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

function definitionIdentifierFromParts(parts: string[]) {
  const id = parts
    .flatMap(part => part.split(/[^A-Za-z0-9_]+/))
    .map(part => part.trim().toLowerCase())
    .filter(Boolean)
    .join('_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
  return id.slice(0, 96) || 'events_report'
}

function isDefinitionPath(path: string) {
  return path.length > 0 &&
    path.length <= 160 &&
    path.split('.').every(part => /^[A-Za-z0-9_]+$/.test(part))
}

function authUrl(apiBaseUrl: string, path: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/auth${path}` : `/auth${path}`
}

function apiKeysUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/v1/api-keys` : '/v1/api-keys'
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
  return {
    key: option.key,
    lookbackMinutes: option.minutes
  }
}

function timeRangeCacheKey(range: ResolvedTimeRange) {
  return [
    range.key,
    range.lookbackMinutes ?? '',
    range.createdAfter ?? '',
    range.createdBefore ?? ''
  ].join('\u0000')
}

function queryTimeDomain({
  eventFilter,
  fallbackFrom,
  fallbackTo,
  timeRange
}: {
  eventFilter: ParsedEventFilter
  fallbackFrom?: string
  fallbackTo?: string
  timeRange: ResolvedTimeRange
}): TimeDomain | null {
  let from = normalizeTimestamp(eventFilter.createdAfter || timeRange.createdAfter)
  let to = normalizeTimestamp(eventFilter.createdBefore || timeRange.createdBefore)

  if (!eventFilter.createdAfter && !eventFilter.createdBefore && timeRange.lookbackMinutes) {
    const toMs = Date.now()
    if (!to) to = new Date(toMs).toISOString()
    if (!from) from = new Date(toMs - timeRange.lookbackMinutes * 60 * 1000).toISOString()
  }

  if (!from) from = normalizeTimestamp(fallbackFrom)
  if (!to) to = normalizeTimestamp(fallbackTo)

  const fromMs = Date.parse(from)
  const toMs = Date.parse(to)
  return Number.isFinite(fromMs) && Number.isFinite(toMs) && toMs >= fromMs ? { from, to } : null
}

const FIELD_PATH_ALIASES: Record<string, string> = {
  durationMs: 'duration_ms',
  endedAt: 'end_time',
  parentSpanId: 'parent_span_id',
  spanId: 'span_id',
  startedAt: 'start_time',
  traceId: 'trace_id'
}

function nanotracePath(path: string) {
  return FIELD_PATH_ALIASES[path] ?? normalizedPayloadPath(path)
}

function facetKey(path: string) {
  return nanotracePath(path)
}

const DISPLAY_FIELD_PATH_ALIASES: Record<string, string> = Object.fromEntries(
  Object.entries(FIELD_PATH_ALIASES).map(([displayPath, storedPath]) => [storedPath, displayPath])
)

function displayFacetPath(path: string) {
  return DISPLAY_FIELD_PATH_ALIASES[path] ?? path
}

function mergeGroupOptions(fields: GroupOption[], limit: number) {
  const seen = new Set<string>()
  const merged: GroupOption[] = []
  for (const option of [...fields, ...groupableFields.map(path => ({ cardinality: 0, capped: false, path, servingMode: 'raw', source: 'raw' }))]) {
    if (seen.has(option.path)) continue
    seen.add(option.path)
    merged.push(option)
    if (merged.length >= limit) break
  }
  return merged
}

function rowToTraceEvent(row: EventRow): TraceEvent {
  const data = normalizeEventData(row)
  return {
    id: String(row.event_id),
    createdAt: normalizeTimestamp(row.timestamp),
    data
  }
}

function rowToFlamegraphEvent(row: FlamegraphEventRow): TraceEvent {
  const traceId = row.trace_id || ''
  const spanId = row.span_id || ''
  const parentSpanId = row.parent_span_id || ''
  const startedAt = normalizeTimestamp(row.start_time)
  const endedAt = normalizeTimestamp(row.end_time)
  const durationMs = Number(row.duration_ms) || 0
  const data: JsonObject = {
    ...compactObject({
      event_type: row.event_type,
      name: row.name,
      parentSpanId,
      parent_span_id: parentSpanId,
      signal: row.signal,
      spanId,
      span_id: spanId,
      startedAt,
      start_time: startedAt,
      traceId,
      trace_id: traceId
    }),
    ...(endedAt ? compactObject({ endedAt, end_time: endedAt }) : {}),
    ...(durationMs > 0 ? { durationMs, duration_ms: durationMs } : {})
  }
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
  const eventType = stringField(data.event_type) || row.event_type
  const traceId = stringField(data.trace_id) || row.trace_id || stringField(data.traceId)
  const spanId = stringField(data.span_id) || row.span_id || stringField(data.spanId)
  const parentSpanId = stringField(data.parent_span_id) || stringField(data.parentSpanId)
  const startedAt = stringField(data.start_time) || stringField(data.span_start_time) || stringField(data.startedAt)
  const endedAt = stringField(data.end_time) || stringField(data.span_end_time) || stringField(data.endedAt)
  const durationMs = typeof data.duration_ms === 'number' ? data.duration_ms : data.durationMs

  return {
    ...data,
    ...(eventType ? { event_type: eventType } : {}),
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
  return stringField(data.name) || stringField(data.event_type) || stringField(data.type) || 'event'
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
  facets?: ParsedFacetFilter[]
  text: string
}

type ParsedFacetJoin = 'and' | 'or'
type ParsedFacetOperator = 'contains' | 'eq' | 'in'

type ParsedFacetFilter = {
  indexed: boolean
  join?: ParsedFacetJoin
  negated?: boolean
  operator?: ParsedFacetOperator
  path: string
  value: string
  values?: string[]
}

function eventFilterInputText(filter: ParsedEventFilter) {
  return [
    ...(filter.facets ?? []).map((facet, index) => facetFilterInputText(facet, index)),
    filter.text
  ].filter(Boolean).join(' ')
}

function stripTimeBounds(filter: ParsedEventFilter): ParsedEventFilter {
  return {
    ...(filter.facets?.length ? { facets: filter.facets } : {}),
    text: filter.text
  }
}

function facetFilterInputText(facet: ParsedFacetFilter, index: number) {
  const join = index > 0 && facet.join === 'or' ? 'OR ' : ''
  const negated = facet.negated ? 'NOT ' : ''
  const operator = facet.operator ?? 'eq'
  const value = operator === 'in'
    ? `[${(facet.values ?? []).map(quoteFilterValue).join(',')}]`
    : quoteFilterValue(facet.value)
  const expression = operator === 'contains'
    ? `${facet.path} CONTAINS ${value}`
    : operator === 'in'
      ? `${facet.path} IN ${value}`
      : `${facet.path}=${value}`
  return `${join}${negated}${expression}`
}

function serializeEventFilter(filter: ParsedEventFilter) {
  return [
    eventFilterInputText(filter),
    filter.createdAfter ? `timestamp>=${filter.createdAfter}` : '',
    filter.createdBefore ? `timestamp<=${filter.createdBefore}` : ''
  ].filter(Boolean).join(' ')
}

function hasAppliedEventFilter(filter: ParsedEventFilter) {
  return filter.text !== '' || Boolean(filter.createdAfter) || Boolean(filter.createdBefore) || Boolean(filter.facets?.length)
}

function parseEventFilter({
  facetPaths,
  referenceTimestamp,
  value
}: {
  facetPaths?: Set<string>
  referenceTimestamp?: string
  value: string
}): ParsedEventFilter {
  const filter: ParsedEventFilter = { text: '' }
  const withoutTimestamps = value.replace(
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

  const facetFilters: ParsedFacetFilter[] = []
  const text = withoutTimestamps.replace(
    /(?:^|\s)(?:(AND|OR)\s+)?(NOT\s+)?([A-Za-z_][A-Za-z0-9_.-]*)\s*(?:(=|!=)|\b(CONTAINS|IN)\b)\s*(\[[^\]]*\]|\([^\)]*\)|"[^"]*"|'[^']*'|\S+)/gi,
    (
      match: string,
      rawJoin: string | undefined,
      rawNegated: string | undefined,
      rawPath: string,
      rawSymbolOperator: string | undefined,
      rawWordOperator: string | undefined,
      rawValue: string
    ) => {
      const path = normalizedPayloadPath(rawPath)
      const operator = facetOperator(rawSymbolOperator, rawWordOperator)
      const values = operator === 'in' ? parseFilterList(rawValue) : [unquoteFilterValue(rawValue)]
      if (!isSupportedFacetFilterPath(path) || values.length === 0 || values.some(value => !value)) {
        return match
      }
      const displayPath = displayFacetPath(path)
      facetFilters.push({
        indexed: Boolean(facetPaths?.has(path) || facetPaths?.has(displayPath)),
        join: rawJoin?.toLowerCase() === 'or' ? 'or' : 'and',
        negated: Boolean(rawNegated) || rawSymbolOperator === '!=',
        operator,
        path: displayPath,
        value: values[0]!,
        ...(operator === 'in' ? { values } : {})
      })
      return ' '
    }
  )

  if (facetFilters.length > 0) {
    filter.facets = facetFilters
  }
  filter.text = trimBooleanOperators(text.trim().split(/\s+/).filter(Boolean)).join(' ')
  return filter
}

function isSupportedFacetFilterPath(path: string) {
  return /^[A-Za-z_][A-Za-z0-9_]*(?:[.-][A-Za-z0-9_]+)*$/.test(path)
}

function facetOperator(symbolOperator?: string, wordOperator?: string): ParsedFacetOperator {
  if (wordOperator?.toLowerCase() === 'contains') return 'contains'
  if (wordOperator?.toLowerCase() === 'in') return 'in'
  return 'eq'
}

function parseFilterList(value: string) {
  const trimmed = value.trim()
  const body = (trimmed.startsWith('[') && trimmed.endsWith(']')) || (trimmed.startsWith('(') && trimmed.endsWith(')'))
    ? trimmed.slice(1, -1)
    : trimmed
  const values: string[] = []
  let current = ''
  let quote = ''
  for (let index = 0; index < body.length; index += 1) {
    const char = body[index]!
    if (quote) {
      if (char === quote) {
        quote = ''
      } else {
        current += char
      }
      continue
    }
    if (char === '"' || char === "'") {
      quote = char
      continue
    }
    if (char === ',') {
      const parsed = current.trim()
      if (parsed) values.push(parsed)
      current = ''
      continue
    }
    current += char
  }
  const parsed = current.trim()
  if (parsed) values.push(parsed)
  return values
}

function unquoteFilterValue(value: string) {
  return value.trim().replace(/^"([^"]*)"$/, '$1').replace(/^'([^']*)'$/, '$1')
}

function quoteFilterValue(value: string) {
  return /\s/.test(value) ? `"${value.replace(/"/g, '\\"')}"` : value
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
