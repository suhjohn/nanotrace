import { createFileRoute } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { QueryClient } from '@tanstack/react-query'
import { CheckCircle2, Columns3, PanelLeftOpen, Play, Plus, RefreshCw, Trash2, WandSparkles } from 'lucide-react'
import { useMemo, useState } from 'react'
import { cn } from '../lib/cn'
import { useAppShell } from '../lib/app-shell'
import {
  HTTPError,
  backfillFacet,
  deleteFacet,
  errorMessage,
  fetchFacetBackfills,
  fetchFacets,
  nanotraceApiBaseUrl,
  putFacet,
  queryHeaders
} from '../lib/nanotrace-api'
import type { FacetBackfill, FacetValueType, HotFacet } from '../lib/nanotrace-api'

export const Route = createFileRoute('/facets')({
  component: FacetsRoute
})

const facetValueTypes: Array<{ label: string; value: FacetValueType }> = [
  { label: 'string', value: 'string' },
  { label: 'low card', value: 'low_cardinality_string' },
  { label: 'int', value: 'integer' },
  { label: 'uint', value: 'unsigned' },
  { label: 'float', value: 'float' },
  { label: 'bool', value: 'bool' },
  { label: 'datetime', value: 'datetime' }
]

function FacetsRoute() {
  const observatoryUrl = nanotraceApiBaseUrl()
  const queryClient = useQueryClient()
  const { setSidebarOpen, sidebarOpen } = useAppShell()
  const [path, setPath] = useState('')
  const [valueType, setValueType] = useState<FacetValueType>('string')
  const [detectedType, setDetectedType] = useState<TypeDetectionResult | null>(null)

  const facetsQuery = useQuery({
    queryKey: ['facets', observatoryUrl],
    queryFn: () => fetchFacets({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const backfillsQuery = useQuery({
    queryKey: ['facets', observatoryUrl, 'backfills'],
    queryFn: () => fetchFacetBackfills({ apiBaseUrl: observatoryUrl }),
    refetchInterval: query => {
      const backfills = query.state.data?.backfills ?? []
      return backfills.some(backfill => backfill.status === 'queued' || backfill.status === 'running') ? 5_000 : false
    },
    retry: false
  })

  const addMutation = useMutation({
    mutationFn: () =>
      putFacet({
        apiBaseUrl: observatoryUrl,
        path,
        valueType
      }),
    onSuccess: async () => {
      setPath('')
      setValueType('string')
      setDetectedType(null)
      await invalidateFacetQueries(queryClient, observatoryUrl)
    }
  })
  const removeMutation = useMutation({
    mutationFn: (facetPath: string) => deleteFacet({ apiBaseUrl: observatoryUrl, path: facetPath }),
    onSuccess: async () => {
      await invalidateFacetQueries(queryClient, observatoryUrl)
    }
  })
  const backfillMutation = useMutation({
    mutationFn: (facetPath: string) => backfillFacet({ apiBaseUrl: observatoryUrl, path: facetPath }),
    onSuccess: async () => {
      await invalidateFacetQueries(queryClient, observatoryUrl)
    }
  })
  const detectTypeMutation = useMutation({
    mutationFn: (facetPath: string) => detectFacetValueType({ apiBaseUrl: observatoryUrl, path: facetPath }),
    onSuccess: detected => {
      setValueType(detected.valueType)
      setDetectedType(detected)
    }
  })

  const facets = facetsQuery.data?.facets ?? []
  const activeFacets = useMemo(() => facets.filter(facet => facet.status === 'active'), [facets])
  const builtins = activeFacets.filter(facet => facet.source === 'builtin')
  const custom = activeFacets.filter(facet => facet.source !== 'builtin')
  const backfills = backfillsQuery.data?.backfills ?? []
  const latestBackfillByPath = useMemo(() => {
    const latest = new Map<string, FacetBackfill>()
    for (const backfill of backfills) {
      if (!latest.has(backfill.path)) latest.set(backfill.path, backfill)
    }
    return latest
  }, [backfills])
  const error = facetsQuery.error || addMutation.error || removeMutation.error || backfillMutation.error || detectTypeMutation.error
  const backfillError = backfillsQuery.error
  const headerStatus = facetsQuery.error ? 'unavailable' : facetsQuery.isFetching ? 'loading' : `${activeFacets.length} active`
  const detectPath = path.trim()

  function detectType() {
    if (!detectPath || detectTypeMutation.isPending) return
    detectTypeMutation.mutate(detectPath)
  }

  return (
    <main className="flex h-full min-h-0 min-w-0 flex-col overflow-hidden bg-black text-[13px] text-neutral-100">
      <header className="flex h-10 shrink-0 items-center gap-2 border-b border-neutral-800 bg-neutral-950 px-3">
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
          <Columns3 size={15} strokeWidth={1.8} className="shrink-0 text-neutral-500" />
          <div className="truncate text-[13px] font-medium text-white">Facets</div>
        </div>
        <div className="ml-auto text-[11px] text-neutral-600">{headerStatus}</div>
      </header>

      <section className="min-h-0 flex-1 overflow-auto bg-black">
        <div className="mx-auto grid w-full max-w-6xl gap-4 px-4 py-4">
          <section className="grid gap-3 border border-neutral-800 bg-neutral-950 p-3">
            <div className="flex min-w-0 items-center justify-between gap-3">
              <div className="min-w-0">
                <h1 className="truncate text-[13px] font-medium text-white">Add facet</h1>
                <p className="mt-0.5 text-[11px] text-neutral-600">Custom facets become groupable fields after backfill.</p>
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <label className="flex h-8 min-w-[280px] flex-1 items-center border border-neutral-800 bg-black focus-within:border-neutral-600">
                <span className="shrink-0 border-r border-neutral-900 px-2 text-[11px] text-neutral-500">Path</span>
                <input
                  className="h-full min-w-0 flex-1 bg-transparent px-2 text-[12px] text-white outline-none placeholder:text-neutral-600"
                  value={path}
                  onChange={event => {
                    setPath(event.target.value)
                    setDetectedType(null)
                  }}
                  onBlur={detectType}
                  placeholder="customer.plan"
                />
              </label>
              <label className="flex h-8 w-[230px] items-center border border-neutral-800 bg-black focus-within:border-neutral-600">
                <span className="shrink-0 border-r border-neutral-900 px-2 text-[11px] text-neutral-500">Type</span>
                <select
                  className="h-full min-w-0 flex-1 bg-transparent px-2 text-[12px] text-white outline-none"
                  value={valueType}
                  onChange={event => {
                    setValueType(event.target.value as FacetValueType)
                    setDetectedType(null)
                  }}
                >
                  {facetValueTypes.map(option => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              <button
                aria-label="Detect type"
                className="inline-flex h-8 w-8 shrink-0 items-center justify-center border border-neutral-800 bg-black text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                disabled={!detectPath || detectTypeMutation.isPending}
                title="Detect type from known fields or sampled values"
                type="button"
                onClick={detectType}
              >
                {detectTypeMutation.isPending ? <RefreshCw size={13} strokeWidth={1.8} /> : <WandSparkles size={13} strokeWidth={1.8} />}
              </button>
              <DetectionChip detectedType={detectedType} detecting={detectTypeMutation.isPending} />
              <button
                className="inline-flex h-8 shrink-0 items-center justify-center gap-1.5 whitespace-nowrap border border-neutral-700 bg-white px-3 text-[12px] font-medium text-black hover:bg-neutral-200 disabled:border-neutral-900 disabled:bg-black disabled:text-neutral-700"
                disabled={!path.trim() || addMutation.isPending}
                type="button"
                onClick={() => addMutation.mutate()}
              >
                <Plus size={13} strokeWidth={2} />
                Add
              </button>
            </div>
            {error ? <div className="text-[11px] text-red-300">{errorMessage(error)}</div> : null}
          </section>

          <section className="min-h-0 border border-neutral-800 bg-neutral-950">
            <div className="flex items-center justify-between gap-2 border-b border-neutral-800 px-3 py-2">
              <h2 className="text-[13px] font-medium text-white">Active facets</h2>
              <button
                className="inline-flex h-7 items-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                disabled={facetsQuery.isFetching}
                type="button"
                onClick={() => void facetsQuery.refetch()}
              >
                <RefreshCw size={12} strokeWidth={1.8} />
                Refresh
              </button>
            </div>
            <FacetTable
              backfillingPath={backfillMutation.variables}
              facets={activeFacets}
              latestBackfillByPath={latestBackfillByPath}
              loading={facetsQuery.isLoading}
              removingPath={removeMutation.variables}
              onBackfill={facetPath => backfillMutation.mutate(facetPath)}
              onRemove={facetPath => removeMutation.mutate(facetPath)}
            />
          </section>

          <section className="min-h-0 border border-neutral-800 bg-neutral-950">
            <div className="flex items-center justify-between gap-2 border-b border-neutral-800 px-3 py-2">
              <div className="min-w-0">
                <h2 className="text-[13px] font-medium text-white">Backfills</h2>
                <p className="mt-0.5 text-[11px] text-neutral-600">
                  {custom.length} custom, {builtins.length} built-in
                </p>
              </div>
              <button
                className="inline-flex h-7 items-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                disabled={backfillsQuery.isFetching}
                type="button"
                onClick={() => void backfillsQuery.refetch()}
              >
                <RefreshCw size={12} strokeWidth={1.8} />
                Refresh
              </button>
            </div>
            {backfillError ? <div className="border-b border-neutral-900 px-3 py-2 text-[11px] text-yellow-300">{errorMessage(backfillError)}</div> : null}
            <BackfillTable backfills={backfills} loading={backfillsQuery.isLoading} />
          </section>
        </div>
      </section>
    </main>
  )
}

function FacetTable({
  backfillingPath,
  facets,
  latestBackfillByPath,
  loading,
  removingPath,
  onBackfill,
  onRemove
}: {
  backfillingPath?: string
  facets: HotFacet[]
  latestBackfillByPath: Map<string, FacetBackfill>
  loading: boolean
  removingPath?: string
  onBackfill: (path: string) => void
  onRemove: (path: string) => void
}) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[760px] border-collapse text-left text-[12px]">
        <thead className="border-b border-neutral-800 text-[11px] uppercase text-neutral-600">
          <tr>
            <th className="px-3 py-2 font-medium">Path</th>
            <th className="px-3 py-2 font-medium">Type</th>
            <th className="px-3 py-2 font-medium">Source</th>
            <th className="px-3 py-2 font-medium">Backfill</th>
            <th className="px-3 py-2 text-right font-medium">Actions</th>
          </tr>
        </thead>
        <tbody>
          {facets.map(facet => {
            const canBackfill = facet.source !== 'builtin'
            const latestBackfill = latestBackfillByPath.get(facet.path)
            const backfillInFlight =
              backfillingPath === facet.path ||
              latestBackfill?.status === 'queued' ||
              latestBackfill?.status === 'running'
            const backfillCompleted = latestBackfill?.status === 'completed'
            const backfillActionLabel = backfillInFlight
              ? latestBackfill?.status === 'queued'
                ? 'Queued'
                : 'Running'
              : backfillCompleted
                ? 'Backfilled'
                : latestBackfill?.status === 'failed'
                  ? 'Retry'
                  : 'Backfill'
            return (
              <tr key={facet.path} className="border-b border-neutral-900 last:border-b-0">
                <td className="max-w-[260px] truncate px-3 py-2 font-mono text-white">{facet.path}</td>
                <td className="px-3 py-2 text-neutral-500">{facet.value_type}</td>
                <td className="px-3 py-2">
                  <span className={cn(facet.source === 'builtin' ? 'text-neutral-500' : 'text-emerald-300')}>
                    {facet.source}
                  </span>
                </td>
                <td className="px-3 py-2">
                  {!canBackfill ? (
                    <span className="text-neutral-600">default</span>
                  ) : latestBackfill ? (
                    <div className="grid gap-0.5">
                      <BackfillStatus status={latestBackfill.status} />
                      <span className="text-[11px] text-neutral-600">
                        {formatNumber(latestBackfill.indexed_events)} events, {formatNumber(latestBackfill.values)} values
                      </span>
                    </div>
                  ) : (
                    <span className="text-neutral-600">not run</span>
                  )}
                </td>
                <td className="px-3 py-2">
                  <div className="flex justify-end gap-1.5">
                    {canBackfill ? (
                      <button
                        aria-label={`${backfillActionLabel} ${facet.path}`}
                        className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                        disabled={backfillInFlight || backfillCompleted}
                        title={`${backfillActionLabel} ${facet.path}`}
                        type="button"
                        onClick={() => onBackfill(facet.path)}
                      >
                        {backfillCompleted ? (
                          <CheckCircle2 size={13} strokeWidth={1.8} />
                        ) : backfillInFlight ? (
                          <RefreshCw size={13} strokeWidth={1.8} />
                        ) : (
                          <Play size={13} strokeWidth={1.8} />
                        )}
                        {backfillActionLabel}
                      </button>
                    ) : null}
                    {facet.removable ? (
                      <button
                        aria-label={`Remove ${facet.path}`}
                        className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                        disabled={removingPath === facet.path}
                        title={`Remove ${facet.path}`}
                        type="button"
                        onClick={() => onRemove(facet.path)}
                      >
                        <Trash2 size={13} strokeWidth={1.8} />
                        Remove
                      </button>
                    ) : null}
                  </div>
                </td>
              </tr>
            )
          })}
          {loading ? (
            <tr>
              <td className="px-3 py-8 text-center text-neutral-600" colSpan={5}>
                Loading facets...
              </td>
            </tr>
          ) : null}
          {!loading && facets.length === 0 ? (
            <tr>
              <td className="px-3 py-8 text-center text-neutral-600" colSpan={5}>
                No facets.
              </td>
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  )
}

function BackfillTable({ backfills, loading }: { backfills: FacetBackfill[]; loading: boolean }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[860px] border-collapse text-left text-[12px]">
        <thead className="border-b border-neutral-800 text-[11px] uppercase text-neutral-600">
          <tr>
            <th className="px-3 py-2 font-medium">Job</th>
            <th className="px-3 py-2 font-medium">Path</th>
            <th className="px-3 py-2 font-medium">Status</th>
            <th className="px-3 py-2 font-medium">Chunks</th>
            <th className="px-3 py-2 font-medium">Events</th>
            <th className="px-3 py-2 font-medium">Values</th>
            <th className="px-3 py-2 font-medium">Error</th>
          </tr>
        </thead>
        <tbody>
          {backfills.map(backfill => (
            <tr key={backfill.job_id} className="border-b border-neutral-900 last:border-b-0">
              <td className="max-w-[190px] truncate px-3 py-2 font-mono text-neutral-500">{backfill.job_id}</td>
              <td className="max-w-[220px] truncate px-3 py-2 font-mono text-white">{backfill.path}</td>
              <td className="px-3 py-2">
                <BackfillStatus status={backfill.status} />
              </td>
              <td className="px-3 py-2 text-neutral-500">
                {backfill.completed_chunks}/{backfill.total_chunks}
                {backfill.failed_chunks ? `, ${backfill.failed_chunks} failed` : ''}
              </td>
              <td className="px-3 py-2 text-neutral-500">{formatNumber(backfill.indexed_events)}</td>
              <td className="px-3 py-2 text-neutral-500">{formatNumber(backfill.values)}</td>
              <td className="max-w-[260px] truncate px-3 py-2 text-red-300">{backfill.error}</td>
            </tr>
          ))}
          {loading ? (
            <tr>
              <td className="px-3 py-8 text-center text-neutral-600" colSpan={7}>
                Loading backfills...
              </td>
            </tr>
          ) : null}
          {!loading && backfills.length === 0 ? (
            <tr>
              <td className="px-3 py-8 text-center text-neutral-600" colSpan={7}>
                No backfills.
              </td>
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  )
}

function BackfillStatus({ status }: { status: string }) {
  if (status === 'completed') {
    return (
      <span className="inline-flex items-center gap-1 text-emerald-300">
        <CheckCircle2 size={12} strokeWidth={1.8} />
        completed
      </span>
    )
  }
  if (status === 'failed') return <span className="text-red-300">failed</span>
  if (status === 'running' || status === 'queued') return <span className="text-yellow-300">{status}</span>
  return <span className="text-neutral-500">{status}</span>
}

function DetectionChip({ detectedType, detecting }: { detectedType: TypeDetectionResult | null; detecting: boolean }) {
  if (detecting) {
    return <span className="inline-flex h-8 shrink-0 items-center border border-neutral-900 px-2 text-[11px] text-neutral-500">detecting</span>
  }
  if (!detectedType) return null

  const source =
    detectedType.source === 'known'
      ? 'known'
      : detectedType.source === 'empty'
        ? 'no values'
        : `${formatNumber(detectedType.values)} sampled`

  return (
    <span
      className="inline-flex h-8 min-w-0 max-w-[220px] shrink items-center truncate border border-neutral-900 px-2 text-[11px] text-neutral-500"
      title={`${detectedType.valueType} · ${source}`}
    >
      <span className="truncate">
        {detectedType.valueType} · {source}
      </span>
    </span>
  )
}

function formatNumber(value: number) {
  return new Intl.NumberFormat().format(value || 0)
}

type ClickHouseResponse<T> = {
  data?: T[]
}

type TypeDetectionRow = {
  bools: number
  cardinality: number
  datetimes: number
  floats: number
  integers: number
  negatives: number
  unsigneds: number
  values: number
}

type TypeDetectionResult = {
  source: 'known' | 'sampled' | 'empty'
  valueType: FacetValueType
  values: number
}

async function detectFacetValueType({
  apiBaseUrl,
  path
}: {
  apiBaseUrl: string
  path: string
}): Promise<TypeDetectionResult> {
  const knownType = knownFacetValueType(path)
  if (knownType) return { source: 'known', valueType: knownType, values: 0 }

  const valueExpression = facetValueExpression(path)
  const datetimePattern = '[' + '-:T' + ']'
  const response = await postQuery<TypeDetectionRow>({
    apiBaseUrl,
    query: [
      'SELECT count() AS values',
      ', uniqCombined64(value) AS cardinality',
      ", countIf(lowerUTF8(value) IN ('true', 'false', '1', '0')) AS bools",
      ', countIf(toInt64OrNull(value) IS NOT NULL) AS integers',
      ', countIf(toUInt64OrNull(value) IS NOT NULL) AS unsigneds',
      ', countIf(toFloat64OrNull(value) IS NOT NULL) AS floats',
      `, countIf(parseDateTime64BestEffortOrNull(value) IS NOT NULL AND match(value, '${datetimePattern}')) AS datetimes`,
      ", countIf(startsWith(value, '-')) AS negatives",
      `FROM (SELECT ${valueExpression} AS value FROM ${eventsTable()} WHERE ${valueExpression} != '' LIMIT 5000)`
    ].join(' ')
  })
  const row = response.data?.[0]
  if (!row || Number(row.values) <= 0) return { source: 'empty', valueType: 'string', values: 0 }

  const values = Number(row.values) || 0
  const cardinality = Number(row.cardinality) || 0
  if (Number(row.bools) === values) return { source: 'sampled', valueType: 'bool', values }
  if (Number(row.datetimes) === values) return { source: 'sampled', valueType: 'datetime', values }
  if (Number(row.integers) === values) {
    return { source: 'sampled', valueType: Number(row.negatives) > 0 ? 'integer' : 'unsigned', values }
  }
  if (Number(row.floats) === values) return { source: 'sampled', valueType: 'float', values }
  return {
    source: 'sampled',
    valueType: cardinality <= Math.min(256, Math.max(32, values / 2)) ? 'low_cardinality_string' : 'string',
    values
  }
}

function knownFacetValueType(path: string): FacetValueType | undefined {
  switch (normalizedAliasPath(path)) {
    case 'trace_id':
    case 'span_id':
    case 'parent_span_id':
      return 'string'
    case 'tenant_id':
    case 'service':
    case 'service.namespace':
    case 'service_version':
    case 'event_type':
    case 'signal':
    case 'environment':
    case 'host.name':
    case 'name':
    case 'scope_name':
    case 'scope_version':
    case 'span_kind':
    case 'span_status_code':
    case 'http.method':
    case 'http.route':
    case 'http.request.method':
    case 'server.address':
    case 'exception.type':
    case 'severity_text':
    case 'logger.name':
    case 'thread.name':
    case 'db.system':
    case 'db.operation':
    case 'rpc.system':
    case 'rpc.service':
    case 'rpc.method':
    case 'messaging.system':
    case 'messaging.destination.name':
    case 'messaging.operation.name':
    case 'metric_name':
    case 'metric_type':
    case 'metric_unit':
    case 'metric.temporality':
    case 'page_path':
    case 'screen_name':
    case 'utm_source':
    case 'utm_medium':
    case 'utm_campaign':
    case 'utm_term':
    case 'utm_content':
    case 'country':
    case 'region':
    case 'city':
    case 'continent':
    case 'device_type':
    case 'device_brand':
    case 'device_manufacturer':
    case 'device_model':
    case 'browser':
    case 'browser_version':
    case 'os':
    case 'os_version':
    case 'app_version':
    case 'locale':
    case 'timezone':
    case 'currency':
    case 'revenue_type':
    case 'experiment_id':
    case 'variant':
    case 'feature_flag':
      return 'low_cardinality_string'
    case 'http.status_code':
    case 'http.response.status_code':
    case 'client.port':
    case 'server.port':
    case 'severity_number':
      return 'unsigned'
    case 'screen_height':
    case 'screen_width':
    case 'viewport_height':
    case 'viewport_width':
    case 'screen_dpi':
      return 'unsigned'
    case 'count':
    case 'metric_count':
      return 'unsigned'
    case 'duration_ms':
    case 'metric_value':
    case 'sum':
    case 'metric_sum':
    case 'metric_min':
    case 'metric_max':
    case 'location_lat':
    case 'location_lng':
    case 'revenue':
    case 'price':
    case 'quantity':
      return 'float'
    case 'metric.is_monotonic':
      return 'bool'
    case 'start_time':
    case 'end_time':
    case 'span_start_time':
    case 'span_end_time':
      return 'datetime'
    default:
      return undefined
  }
}

async function postQuery<T>({
  apiBaseUrl,
  query
}: {
  apiBaseUrl: string
  query: string
}): Promise<ClickHouseResponse<T>> {
  const response = await fetch(queryUrl(apiBaseUrl), {
    body: JSON.stringify({ query }),
    credentials: 'include',
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  return (await response.json()) as ClickHouseResponse<T>
}

function facetValueExpression(path: string) {
  return `ifNull(toString(data.${normalizedPayloadPath(path)}), '')`
}

function normalizedPayloadPath(path: string) {
  const normalized = normalizedAliasPath(path)
  const value = normalized.startsWith('data.') ? normalized.slice('data.'.length) : normalized
  if (!/^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$/.test(value)) {
    throw new Error('Type detection supports dotted paths with letters, numbers, and underscores.')
  }
  return value
}

function normalizedAliasPath(path: string) {
  const normalized = path.trim()
  const value = normalized.startsWith('data.') ? normalized.slice('data.'.length) : normalized
  switch (value) {
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
    default:
      return value
  }
}

function eventsTable() {
  const configured = String(import.meta.env.VITE_NANOTRACE_TABLE || '').trim()
  return /^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$/.test(configured)
    ? configured
    : 'observatory.events'
}

function queryUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/query` : '/query'
}

async function invalidateFacetQueries(queryClient: QueryClient, observatoryUrl: string) {
  await Promise.all([
    queryClient.invalidateQueries({ queryKey: ['facets', observatoryUrl] }),
    queryClient.invalidateQueries({ queryKey: ['facets', observatoryUrl, 'backfills'] }),
    queryClient.invalidateQueries({ queryKey: ['logs', observatoryUrl, 'group-options'] })
  ])
}
