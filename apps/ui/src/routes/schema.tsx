import { createFileRoute } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { PanelLeftOpen, RefreshCw, Trash2 } from 'lucide-react'
import { useState } from 'react'
import { useAppShell } from '../lib/app-shell'
import { HTTPError, errorMessage, nanotraceApiBaseUrl, queryHeaders } from '../lib/nanotrace-api'

export const Route = createFileRoute('/schema')({
  component: SchemaRoute
})

type DefinitionKind = 'field' | 'measure' | 'rollup' | 'metric_rollup' | 'state' | 'search' | 'report' | 'sequence' | 'cohort' | 'alert'

type DefinitionRecord = {
  backfill?: DefinitionBackfillStatus | null
  capabilities?: Record<string, unknown>
  config?: Record<string, unknown>
  created_at: string
  definition_id: string
  enabled: number
  kind: DefinitionKind
  mode: string
  name: string
  tenant_id: string
  updated_at: string
  version: number
}

type DefinitionBackfillStatus = {
  distinct_values: number
  from: string
  rows_matched: number
  status: string
  to: string
  updated_at: string
}

type DefinitionBackfill = {
  definition_id: string
  distinct_values: number
  from: string
  kind: string
  mode: string
  rows_matched: number
  status: string
  to: string
}

type MaterializationJobRecord = {
  completed_at?: string | null
  completed_chunks: number
  config?: Record<string, unknown>
  created_at: string
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

type CreateBackfillJobResponse = {
  backfill: MaterializationJobRecord
  chunks?: unknown[]
}

type QueryRecommendation = {
  action?: string
  groupBy?: string[]
  kind?: string
  operator?: string
  path?: string
  reason?: string
  source?: string
  targetTable?: string
  targetType?: string
}

type QueryRecommendationRecord = {
  elapsed_ms: number
  filter_paths: string[]
  group_by_paths: string[]
  observed_at: string
  plan_kind: string
  query_id: string
  query_shape: string
  recommendations: QueryRecommendation[]
  result_rows: number
  source_tables: string[]
  surface: string
}

type QueryRecommendationItem = {
  index: number
  recommendation: QueryRecommendation
  record: QueryRecommendationRecord
}

type RecommendationApprovalStatus = {
  detail?: string
  label: string
  tone: 'active' | 'error' | 'muted' | 'ok'
}

type DefinitionDraftKind = 'field' | 'measure' | 'state' | 'search' | 'report' | 'sequence' | 'cohort' | 'alert'

type DefinitionDraft = {
  configText: string
  kind: DefinitionDraftKind
  mode: string
  name: string
  runNow: boolean
}

type CreateDefinitionResult = {
  backfill?: DefinitionBackfill
  definition: DefinitionRecord
  job?: MaterializationJobRecord
}

function SchemaRoute() {
  const observatoryUrl = nanotraceApiBaseUrl()
  const queryClient = useQueryClient()
  const { setSidebarOpen, sidebarOpen } = useAppShell()
  const [backfillingDefinitionId, setBackfillingDefinitionId] = useState<string | null>(null)
  const [materializingDefinitionId, setMaterializingDefinitionId] = useState<string | null>(null)
  const [materializationWindow, setMaterializationWindow] = useState(() => defaultMaterializationWindow())
  const [definitionDraft, setDefinitionDraft] = useState<DefinitionDraft>(() => definitionDraftTemplate('state'))

  const definitionsQuery = useQuery({
    queryKey: ['definitions', observatoryUrl],
    queryFn: () => fetchDefinitions({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const definitions = definitionsQuery.data?.definitions ?? []
  const materializationJobsQuery = useQuery({
    queryKey: ['materialization-jobs', observatoryUrl],
    queryFn: () => fetchMaterializationJobs({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const materializationJobs = materializationJobsQuery.data?.jobs ?? []
  const queryRecommendationsQuery = useQuery({
    queryKey: ['query-recommendations', observatoryUrl],
    queryFn: () => fetchQueryRecommendations({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const recommendationItems = (queryRecommendationsQuery.data?.recommendations ?? [])
    .flatMap(record => record.recommendations.map((recommendation, index) => ({ index, recommendation, record })))
    .slice(0, 50)

  const deleteDefinitionMutation = useMutation({
    mutationFn: (definitionId: string) => deleteDefinition({ apiBaseUrl: observatoryUrl, definitionId }),
    onSuccess: deleted => {
      queryClient.setQueryData<{ definitions: DefinitionRecord[] }>(['definitions', observatoryUrl], current => ({
        definitions: (current?.definitions ?? []).filter(definition => definition.definition_id !== deleted.definition_id)
      }))
    }
  })

  const backfillDefinitionMutation = useMutation({
    mutationFn: (definitionId: string) =>
      backfillDefinition({
        apiBaseUrl: observatoryUrl,
        definitionId
      }),
    onMutate: definitionId => setBackfillingDefinitionId(definitionId),
    onSuccess: backfill => {
      queryClient.setQueryData<{ definitions: DefinitionRecord[] }>(['definitions', observatoryUrl], current => ({
        definitions: (current?.definitions ?? []).map(definition =>
          definition.definition_id === backfill.definition_id
            ? {
                ...definition,
                backfill: {
                  distinct_values: backfill.distinct_values,
                  from: backfill.from,
                  rows_matched: backfill.rows_matched,
                  status: backfill.status,
                  to: backfill.to,
                  updated_at: new Date().toISOString()
                }
              }
            : definition
        )
      }))
    },
    onSettled: () => setBackfillingDefinitionId(null)
  })

  const materializationWindowValid = validMaterializationWindow(materializationWindow)

  const createMaterializationMutation = useMutation({
    mutationFn: (definitionId: string) =>
      createMaterializationJob({
        apiBaseUrl: observatoryUrl,
        definitionId,
        sourceEnd: materializationWindow.end,
        sourceStart: materializationWindow.start
      }),
    onMutate: definitionId => setMaterializingDefinitionId(definitionId),
    onSuccess: response => {
      queryClient.setQueryData<{ jobs: MaterializationJobRecord[] }>(['materialization-jobs', observatoryUrl], current => ({
        jobs: upsertMaterializationJob(current?.jobs ?? [], response.backfill)
      }))
    },
    onSettled: async () => {
      setMaterializingDefinitionId(null)
      await queryClient.invalidateQueries({ queryKey: ['materialization-jobs', observatoryUrl] })
    }
  })
  const createDefinitionMutation = useMutation({
    mutationFn: (draft: DefinitionDraft) =>
      createAdvancedDefinition({
        apiBaseUrl: observatoryUrl,
        draft,
        materializationWindow
      }),
    onSuccess: response => {
      const definition = response.backfill
        ? {
            ...response.definition,
            backfill: {
              distinct_values: response.backfill.distinct_values,
              from: response.backfill.from,
              rows_matched: response.backfill.rows_matched,
              status: response.backfill.status,
              to: response.backfill.to,
              updated_at: new Date().toISOString()
            }
          }
        : response.definition
      queryClient.setQueryData<{ definitions: DefinitionRecord[] }>(['definitions', observatoryUrl], current => ({
        definitions: upsertDefinition(current?.definitions ?? [], definition)
      }))
      if (response.job) {
        queryClient.setQueryData<{ jobs: MaterializationJobRecord[] }>(['materialization-jobs', observatoryUrl], current => ({
          jobs: upsertMaterializationJob(current?.jobs ?? [], response.job!)
        }))
      }
    },
    onSettled: async () => {
      await queryClient.invalidateQueries({ queryKey: ['definitions', observatoryUrl] })
      await queryClient.invalidateQueries({ queryKey: ['materialization-jobs', observatoryUrl] })
    }
  })

  const definitionError =
    errorMessage(definitionsQuery.error) ||
    errorMessage(deleteDefinitionMutation.error) ||
    errorMessage(backfillDefinitionMutation.error) ||
    errorMessage(materializationJobsQuery.error) ||
    errorMessage(createMaterializationMutation.error) ||
    errorMessage(queryRecommendationsQuery.error) ||
    errorMessage(createDefinitionMutation.error)
  const headerStatus = definitionsQuery.error
    ? 'unavailable'
    : definitionsQuery.isFetching
      ? 'loading'
      : `${definitions.length} definitions`

  return (
    <main className="flex h-full min-h-0 min-w-0 flex-col overflow-hidden bg-black text-[13px] text-neutral-100">
      <section className="min-h-0 flex-1 overflow-auto bg-black">
        <div className="grid w-full min-w-0 content-start gap-4 p-2 sm:p-4">
          <section className="min-h-0 min-w-0 overflow-hidden border border-neutral-800 bg-neutral-950">
            <div className="flex items-center justify-between gap-2 border-b border-neutral-800 px-3 py-2">
              <div className="flex min-w-0 items-center gap-2">
                <h2 className="text-[13px] font-medium text-white">Create Definition</h2>
                <span className="hidden text-[11px] text-neutral-600 sm:inline">field, measure, state, search, report, sequence, cohort, alert</span>
              </div>
              <button
                className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                disabled={createDefinitionMutation.isPending || (definitionDraft.runNow && !materializationWindowValid)}
                type="button"
                onClick={() => createDefinitionMutation.mutate(definitionDraft)}
              >
                <RefreshCw size={13} strokeWidth={1.8} />
                {createDefinitionMutation.isPending ? 'Creating' : 'Create'}
              </button>
            </div>
            <div className="grid gap-3 p-3 lg:grid-cols-[260px_minmax(0,1fr)]">
              <div className="grid content-start gap-3">
                <label className="grid gap-1 text-[11px] uppercase text-neutral-600">
                  Kind
                  <select
                    className="h-8 border border-neutral-800 bg-black px-2 text-[12px] normal-case text-neutral-200 outline-none focus:border-neutral-600"
                    value={definitionDraft.kind}
                    onChange={event => setDefinitionDraft(definitionDraftTemplate(event.target.value as DefinitionDraftKind))}
                  >
                    <option value="field">field</option>
                    <option value="measure">measure</option>
                    <option value="state">state</option>
                    <option value="search">search</option>
                    <option value="report">report</option>
                    <option value="sequence">sequence</option>
                    <option value="cohort">cohort</option>
                    <option value="alert">alert</option>
                  </select>
                </label>
                <label className="grid gap-1 text-[11px] uppercase text-neutral-600">
                  Name
                  <input
                    className="h-8 border border-neutral-800 bg-black px-2 font-mono text-[12px] normal-case text-neutral-200 outline-none focus:border-neutral-600"
                    value={definitionDraft.name}
                    onChange={event => setDefinitionDraft(current => ({ ...current, name: event.target.value }))}
                  />
                </label>
                <label className="grid gap-1 text-[11px] uppercase text-neutral-600">
                  Mode
                  <input
                    className="h-8 border border-neutral-800 bg-black px-2 font-mono text-[12px] normal-case text-neutral-200 outline-none focus:border-neutral-600"
                    value={definitionDraft.mode}
                    onChange={event => setDefinitionDraft(current => ({ ...current, mode: event.target.value }))}
                  />
                </label>
                <label className="inline-flex items-center gap-2 text-[12px] text-neutral-400">
                  <input
                    checked={definitionDraft.runNow}
                    className="h-3 w-3 accent-neutral-200"
                    type="checkbox"
                    onChange={event => setDefinitionDraft(current => ({ ...current, runNow: event.target.checked }))}
                  />
                  Backfill or queue selected window
                </label>
              </div>
              <label className="grid min-w-0 gap-1 text-[11px] uppercase text-neutral-600">
                Config JSON
                <textarea
                  className="min-h-[220px] resize-y border border-neutral-800 bg-black p-2 font-mono text-[11px] normal-case leading-5 text-neutral-200 outline-none focus:border-neutral-600"
                  spellCheck={false}
                  value={definitionDraft.configText}
                  onChange={event => setDefinitionDraft(current => ({ ...current, configText: event.target.value }))}
                />
              </label>
            </div>
          </section>

          <section className="min-h-0 min-w-0 overflow-hidden border border-neutral-800 bg-neutral-950">
            <div className="flex items-center justify-between gap-2 border-b border-neutral-800 px-3 py-2">
              <div className="flex min-w-0 items-center gap-2">
                <h2 className="text-[13px] font-medium text-white">Query Recommendations</h2>
                <span className="hidden text-[11px] text-neutral-600 sm:inline">
                  {queryRecommendationsQuery.isFetching
                    ? 'loading'
                    : `${recommendationItems.length.toLocaleString()} suggested definitions`}
                </span>
              </div>
              <button
                className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                disabled={queryRecommendationsQuery.isFetching}
                type="button"
                onClick={() => void queryRecommendationsQuery.refetch()}
              >
                <RefreshCw size={13} strokeWidth={1.8} />
                Refresh
              </button>
            </div>
            {recommendationItems.length === 0 ? (
              <div className="px-3 py-6 text-[12px] text-neutral-600">
                No recent query recommendations with enough structure to prefill a definition.
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full min-w-[980px] border-collapse text-left text-[12px]">
                  <thead className="border-b border-neutral-800 bg-black text-[11px] uppercase text-neutral-600">
                    <tr>
                      <th className="px-3 py-2 font-medium">Recommendation</th>
                      <th className="px-3 py-2 font-medium">Plan</th>
                      <th className="px-3 py-2 font-medium">Status</th>
                      <th className="px-3 py-2 font-medium">Query Shape</th>
                      <th className="px-3 py-2 font-medium">Observed</th>
                      <th className="w-28 px-3 py-2 text-right font-medium">Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recommendationItems.map(item => {
                      const draft = definitionDraftFromRecommendation(item)
                      const status = recommendationApprovalStatus(item, definitions, materializationJobs)
                      return (
                        <tr
                          className="border-b border-neutral-900 align-top last:border-0 hover:bg-white/[0.02]"
                          key={`${item.record.query_id}:${item.index}`}
                        >
                          <td className="px-3 py-2">
                            <div className="text-neutral-200">{recommendationLabel(item.recommendation)}</div>
                            <div className="mt-0.5 text-[11px] text-neutral-600">{recommendationDetail(item.recommendation)}</div>
                          </td>
                          <td className="px-3 py-2 text-neutral-500">
                            <div>{item.record.surface}</div>
                            <div className="text-[11px] text-neutral-700">{item.record.plan_kind}</div>
                          </td>
                          <td className="px-3 py-2">
                            <div className={recommendationStatusClassName(status.tone)}>{status.label}</div>
                            {status.detail ? <div className="mt-0.5 text-[11px] text-neutral-600">{status.detail}</div> : null}
                          </td>
                          <td className="max-w-[440px] px-3 py-2 font-mono text-[11px] leading-4 text-neutral-500">
                            <div className="max-h-12 overflow-hidden break-all">{item.record.query_shape}</div>
                          </td>
                          <td className="px-3 py-2 text-neutral-500">{formatDate(item.record.observed_at)}</td>
                          <td className="px-3 py-2 text-right">
                            <button
                              className="inline-flex h-7 items-center justify-center border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                              disabled={!draft || status.label !== 'ready'}
                              type="button"
                              onClick={() => {
                                if (draft) setDefinitionDraft(draft)
                              }}
                            >
                              Prefill
                            </button>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          <section className="min-h-0 min-w-0 overflow-hidden border border-neutral-800 bg-neutral-950">
            <div className="flex items-center justify-between gap-2 border-b border-neutral-800 px-3 py-2">
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
                <h2 className="text-[13px] font-medium text-white">Definitions</h2>
                <span className="hidden text-[11px] text-neutral-600 sm:inline">{headerStatus}</span>
              </div>
              <div className="flex shrink-0 items-center gap-2">
                <span className="text-[11px] text-neutral-600 sm:hidden">{headerStatus}</span>
                <button
                  className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                  disabled={definitionsQuery.isFetching}
                  type="button"
                  onClick={() => void definitionsQuery.refetch()}
                >
                  <RefreshCw size={13} strokeWidth={1.8} />
                  Refresh
                </button>
              </div>
            </div>
            {definitionError ? <div className="border-b border-neutral-800 px-3 py-2 text-[11px] text-red-300">{definitionError}</div> : null}
            <div className="overflow-x-auto">
              <table className="w-full min-w-[900px] border-collapse text-left text-[12px]">
                <thead className="border-b border-neutral-800 text-[11px] uppercase text-neutral-600">
                  <tr>
                    <th className="px-3 py-2 font-medium">JSON path</th>
                    <th className="px-3 py-2 font-medium">Type</th>
                    <th className="px-3 py-2 font-medium">Mode</th>
                    <th className="px-3 py-2 font-medium">Config</th>
                    <th className="px-3 py-2 font-medium">Serving</th>
                    <th className="px-3 py-2 font-medium">Updated</th>
                    <th className="px-3 py-2 text-right font-medium">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {definitions.map(definition => {
                    const queuedDefinition = isQueuedMaterializationDefinition(definition)
                    const synchronousDefinition = isSynchronousBackfillableDefinition(definition)
                    const queuedJob = queuedDefinition ? latestDefinitionMaterializationJob(definition, materializationJobs) : null
                    return (
                    <tr key={definition.definition_id} className="border-b border-neutral-900 align-top last:border-b-0">
                      <td className="max-w-[260px] truncate px-3 py-2 font-mono text-[11px] font-medium text-white">{definitionPathLabel(definition)}</td>
                      <td className="px-3 py-2 text-neutral-400">{definition.kind}</td>
                      <td className="px-3 py-2 text-neutral-400">{definition.mode}</td>
                      <td className="max-w-[360px] truncate px-3 py-2 font-mono text-[11px] text-neutral-500">{definitionConfigLabel(definition)}</td>
                      <td className="px-3 py-2">
                        <DefinitionServingStatus definition={definition} queuedJob={queuedJob} />
                      </td>
                      <td className="px-3 py-2 text-neutral-500">{formatDate(definition.updated_at)}</td>
                      <td className="px-3 py-2 text-right">
                        <div className="flex justify-end gap-2">
                          {queuedDefinition ? (
                            <button
                              className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                              disabled={createMaterializationMutation.isPending || !materializationWindowValid}
                              type="button"
                              onClick={() => createMaterializationMutation.mutate(definition.definition_id)}
                            >
                              <RefreshCw size={13} strokeWidth={1.8} />
                              {materializingDefinitionId === definition.definition_id ? 'Queueing' : 'Materialize'}
                            </button>
                          ) : synchronousDefinition && definition.backfill?.status === 'completed' ? (
                            <span className="inline-flex h-7 items-center px-2 text-[12px] text-neutral-600">Backfilled</span>
                          ) : synchronousDefinition ? (
                            <button
                              className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                              disabled={backfillDefinitionMutation.isPending}
                              type="button"
                              onClick={() => backfillDefinitionMutation.mutate(definition.definition_id)}
                            >
                              <RefreshCw size={13} strokeWidth={1.8} />
                              {backfillingDefinitionId === definition.definition_id ? 'Running' : 'Backfill'}
                            </button>
                          ) : (
                            <span className="inline-flex h-7 items-center px-2 text-[12px] text-neutral-600">{definitionStaticServingLabel(definition)}</span>
                          )}
                          <button
                            aria-label={`Remove ${definition.name}`}
                            className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                            disabled={deleteDefinitionMutation.isPending}
                            type="button"
                            onClick={() => deleteDefinitionMutation.mutate(definition.definition_id)}
                          >
                            <Trash2 size={13} strokeWidth={1.8} />
                            Remove
                          </button>
                        </div>
                      </td>
                    </tr>
                  )})}
                  {definitionsQuery.error ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={7}>
                        Definitions unavailable.
                      </td>
                    </tr>
                  ) : null}
                  {!definitionsQuery.isLoading && !definitionsQuery.error && definitions.length === 0 ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={7}>
                        No definitions.
                      </td>
                    </tr>
                  ) : null}
                  {definitionsQuery.isLoading ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={7}>
                        Loading definitions...
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </section>

          <section className="min-h-0 min-w-0 overflow-hidden border border-neutral-800 bg-neutral-950">
            <div className="flex items-center justify-between gap-2 border-b border-neutral-800 px-3 py-2">
              <div className="flex min-w-0 items-center gap-2">
                <h2 className="text-[13px] font-medium text-white">Backfills</h2>
                <span className="hidden text-[11px] text-neutral-600 sm:inline">{materializationJobsQuery.isFetching ? 'loading' : `${materializationJobs.length} jobs`}</span>
              </div>
              <div className="flex shrink-0 items-center gap-2">
                <label className="grid gap-0.5 text-[10px] uppercase text-neutral-600">
                  Start
                  <input
                    className="h-7 w-[170px] border border-neutral-800 bg-black px-2 font-mono text-[11px] normal-case text-neutral-300 outline-none focus:border-neutral-600"
                    type="datetime-local"
                    value={materializationWindow.start}
                    onChange={event => setMaterializationWindow(current => ({ ...current, start: event.target.value }))}
                  />
                </label>
                <label className="grid gap-0.5 text-[10px] uppercase text-neutral-600">
                  End
                  <input
                    className="h-7 w-[170px] border border-neutral-800 bg-black px-2 font-mono text-[11px] normal-case text-neutral-300 outline-none focus:border-neutral-600"
                    type="datetime-local"
                    value={materializationWindow.end}
                    onChange={event => setMaterializationWindow(current => ({ ...current, end: event.target.value }))}
                  />
                </label>
                <button
                  className="mt-3 inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                  disabled={materializationJobsQuery.isFetching}
                  type="button"
                  onClick={() => void materializationJobsQuery.refetch()}
                >
                  <RefreshCw size={13} strokeWidth={1.8} />
                  Refresh
                </button>
              </div>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[900px] border-collapse text-left text-[12px]">
                <thead className="border-b border-neutral-800 text-[11px] uppercase text-neutral-600">
                  <tr>
                    <th className="px-3 py-2 font-medium">Target</th>
                    <th className="px-3 py-2 font-medium">Status</th>
                    <th className="px-3 py-2 font-medium">Window</th>
                    <th className="px-3 py-2 font-medium">Rows</th>
                    <th className="px-3 py-2 font-medium">Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {materializationJobs.map(job => (
                    <tr key={job.job_id} className="border-b border-neutral-900 align-top last:border-b-0">
                      <td className="max-w-[280px] truncate px-3 py-2 font-mono text-[11px] text-white">
                        {job.target_type}/{job.target_id}
                        <div className="text-[10px] text-neutral-600">{job.target_table} v{job.target_version}</div>
                      </td>
                      <td className="px-3 py-2">
                        <MaterializationJobStatus job={job} />
                      </td>
                      <td className="px-3 py-2 text-neutral-500">{formatMaterializationWindow(job)}</td>
                      <td className="px-3 py-2 text-neutral-500">
                        {job.rows_written.toLocaleString()} written
                        <div className="text-[11px] text-neutral-700">{job.rows_scanned.toLocaleString()} scanned</div>
                      </td>
                      <td className="px-3 py-2 text-neutral-500">{formatDate(job.updated_at)}</td>
                    </tr>
                  ))}
                  {materializationJobsQuery.error ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={5}>
                        Backfill jobs unavailable.
                      </td>
                    </tr>
                  ) : null}
                  {!materializationJobsQuery.isLoading && !materializationJobsQuery.error && materializationJobs.length === 0 ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={5}>
                        No backfill jobs.
                      </td>
                    </tr>
                  ) : null}
                  {materializationJobsQuery.isLoading ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={5}>
                        Loading backfill jobs.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </section>
        </div>
      </section>
    </main>
  )
}

function definitionConfigLabel(definition: DefinitionRecord) {
  const config = definition.config ?? {}
  if (definition.kind === 'measure' || definition.kind === 'rollup') {
    const details = []
    if (definition.kind === 'rollup') details.push(String(config.bucket ?? '5m'))
    if (config.unit) details.push(String(config.unit))
    if (config.dimension) details.push(`by ${String(config.dimension)}`)
    if (definition.kind === 'rollup' && Array.isArray(config.aggregates)) {
      details.push(config.aggregates.map(String).join(', '))
    }
    return details.join(' · ')
  }
  if (definition.kind === 'state') {
    const details = []
    if (config.entity_type) details.push(String(config.entity_type))
    if (config.entity_id_path) details.push(`by ${String(config.entity_id_path)}`)
    if (config.value_type) details.push(String(config.value_type))
    return details.join(' · ')
  }
  if (definition.kind === 'search') {
    const details = []
    if (config.search_mode) details.push(String(config.search_mode))
    if (config.require_all_terms) details.push('all terms')
    if (config.include_snippets) details.push('snippets')
    return details.join(' · ')
  }
  return ''
}

function definitionPathLabel(definition: DefinitionRecord) {
  const config = definition.config ?? {}
  if (definition.kind === 'search') return String(config.query ?? definition.name)
  return String(config.path ?? definition.name)
}

function BackfillStatus({ status }: { status?: DefinitionBackfillStatus | null }) {
  if (!status) return <span className="text-neutral-600">not run</span>
  const completed = status.status === 'completed'
  return (
    <div className="grid gap-0.5">
      <div className={completed ? 'text-emerald-300' : 'text-neutral-300'}>{status.status}</div>
      <div className="text-[11px] text-neutral-600">
        {status.rows_matched.toLocaleString()} rows · {status.distinct_values.toLocaleString()} values
      </div>
      <div className="text-[11px] text-neutral-700">{formatDate(status.updated_at)}</div>
    </div>
  )
}

function DefinitionServingStatus({
  definition,
  queuedJob
}: {
  definition: DefinitionRecord
  queuedJob?: MaterializationJobRecord | null
}) {
  if (isQueuedMaterializationDefinition(definition)) {
    return queuedJob ? <MaterializationJobStatus job={queuedJob} /> : <span className="text-neutral-600">not queued</span>
  }
  if (!isSynchronousBackfillableDefinition(definition)) {
    return <span className="text-neutral-600">{definitionStaticServingLabel(definition)}</span>
  }
  return <BackfillStatus status={definition.backfill} />
}

function MaterializationJobStatus({ job }: { job: MaterializationJobRecord }) {
  const completed = job.status === 'completed'
  const failed = job.status === 'failed' || job.failed_chunks > 0
  const progress = job.total_chunks > 0
    ? `${job.completed_chunks.toLocaleString()}/${job.total_chunks.toLocaleString()} chunks`
    : 'no chunks'
  return (
    <div className="grid gap-0.5">
      <div className={completed ? 'text-emerald-300' : failed ? 'text-red-300' : 'text-neutral-300'}>{job.status}</div>
      <div className="text-[11px] text-neutral-600">{progress}</div>
      {job.failed_chunks > 0 ? <div className="text-[11px] text-red-400">{job.failed_chunks.toLocaleString()} failed</div> : null}
    </div>
  )
}

function upsertDefinition(definitions: DefinitionRecord[], definition: DefinitionRecord) {
  const next = definitions.filter(candidate => candidate.definition_id !== definition.definition_id)
  next.unshift(definition)
  return next.sort((left, right) => right.updated_at.localeCompare(left.updated_at))
}

function upsertMaterializationJob(jobs: MaterializationJobRecord[], job: MaterializationJobRecord) {
  const next = jobs.filter(candidate => candidate.job_id !== job.job_id)
  next.unshift(job)
  return next.sort((left, right) => right.updated_at.localeCompare(left.updated_at))
}

function latestDefinitionMaterializationJob(definition: DefinitionRecord, jobs: MaterializationJobRecord[]) {
  return jobs
    .filter(job => jobDefinitionId(job) === definition.definition_id || jobMatchesDefinitionOutput(job, definition))
    .sort((left, right) => right.updated_at.localeCompare(left.updated_at))[0] ?? null
}

function jobDefinitionId(job: MaterializationJobRecord) {
  const value = job.config?.definition_id
  return typeof value === 'string' ? value : ''
}

function jobMatchesDefinitionOutput(job: MaterializationJobRecord, definition: DefinitionRecord) {
  if (!isQueuedMaterializationDefinition(definition) || job.target_type !== definition.kind) return false
  return definitionOutputObjects(definition).some(output => {
    const target = typeof output.target === 'string' ? output.target : ''
    const idKey = definition.kind === 'cohort' ? 'cohort_id' : 'report_id'
    const id = typeof output[idKey] === 'string' ? output[idKey] : ''
    return target === job.target_table && id === job.target_id
  })
}

function isQueuedMaterializationDefinition(definition: DefinitionRecord) {
  return definition.kind === 'report' || definition.kind === 'sequence' || definition.kind === 'cohort'
}

function isSynchronousBackfillableDefinition(definition: DefinitionRecord) {
  return definition.kind === 'field' || definition.kind === 'measure' || definition.kind === 'rollup' || definition.kind === 'state'
}

function isBackfillableDraftKind(kind: DefinitionDraftKind) {
  return kind === 'field' || kind === 'measure' || kind === 'state'
}

function definitionStaticServingLabel(definition: DefinitionRecord) {
  if (definition.kind === 'metric_rollup') return 'managed'
  if (definition.kind === 'search') return 'saved'
  if (definition.kind === 'alert') return 'streaming'
  return 'no backfill'
}

function formatDate(value?: string | null) {
  if (!value) return 'never'
  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) return value
  return date.toLocaleString([], {
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    month: 'short',
    year: 'numeric'
  })
}

function formatMaterializationWindow(job: MaterializationJobRecord) {
  return `${formatDate(job.source_start)} -> ${formatDate(job.source_end)}`
}

function defaultMaterializationWindow() {
  const now = new Date()
  const start = new Date(now.getTime() - 24 * 60 * 60 * 1000)
  return {
    end: datetimeLocalValue(now),
    start: datetimeLocalValue(start)
  }
}

function datetimeLocalValue(date: Date) {
  const offsetMs = date.getTimezoneOffset() * 60 * 1000
  return new Date(date.getTime() - offsetMs).toISOString().slice(0, 16)
}

function validMaterializationWindow(window: { end: string; start: string }) {
  const start = Date.parse(window.start)
  const end = Date.parse(window.end)
  return Number.isFinite(start) && Number.isFinite(end) && end > start
}

function materializationTimestamp(value: string) {
  return new Date(value).toISOString()
}

function definitionDraftTemplate(kind: DefinitionDraftKind): DefinitionDraft {
  switch (kind) {
    case 'field':
      return {
        configText: jsonTemplate({
          path: 'account.plan',
          value_type: 'string'
        }),
        kind,
        mode: 'facet',
        name: 'account_plan',
        runNow: true
      }
    case 'measure':
      return {
        configText: jsonTemplate({
          path: 'duration_ms',
          value_type: 'number'
        }),
        kind,
        mode: 'measure',
        name: 'duration_ms',
        runNow: true
      }
    case 'state':
      return {
        configText: jsonTemplate({
          entity_id_path: 'account.id',
          entity_type: 'account',
          path: 'account.plan',
          value_type: 'string'
        }),
        kind,
        mode: 'state_transition',
        name: 'account_plan',
        runNow: true
      }
    case 'search':
      return {
        configText: jsonTemplate({
          include_snippets: true,
          query: 'checkout failed',
          require_all_terms: false,
          search_mode: 'token'
        }),
        kind,
        mode: 'saved',
        name: 'checkout_failed',
        runNow: false
      }
    case 'report':
      return {
        configText: jsonTemplate({
          match: {
            all: [
              { op: 'eq', path: 'event_type', value: 'checkout_completed' }
            ]
          },
          outputs: [
            {
              bucket_seconds: 60,
              dimensions: [
                { name: 'service', value: { path: 'service' } }
              ],
              metrics: [
                { name: 'events', op: 'count' },
                { name: 'errors', op: 'error_count' }
              ],
              report_id: 'checkout_by_service',
              target: 'report_results'
            }
          ]
        }),
        kind,
        mode: 'summary',
        name: 'checkout_by_service',
        runNow: true
      }
    case 'sequence':
      return {
        configText: jsonTemplate({
          outputs: [
            {
              bucket_seconds: 60,
              dimensions: [
                { name: 'plan', value: { path: 'account.plan' } }
              ],
              entity_id: { path: 'user_id' },
              report_id: 'signup_checkout_funnel',
              steps: [
                {
                  match: { all: [{ op: 'eq', path: 'event_type', value: 'signup_completed' }] },
                  name: 'signup'
                },
                {
                  match: { all: [{ op: 'eq', path: 'event_type', value: 'checkout_completed' }] },
                  name: 'checkout'
                }
              ],
              target: 'sequence_report_results'
            }
          ]
        }),
        kind,
        mode: 'funnel',
        name: 'signup_checkout_funnel',
        runNow: true
      }
    case 'cohort':
      return {
        configText: jsonTemplate({
          match: {
            all: [
              { op: 'eq', path: 'account.plan', value: 'pro' }
            ]
          },
          outputs: [
            {
              cohort_id: 'pro_accounts',
              entity_id: { path: 'account.id' },
              entity_type: 'account',
              target: 'cohort_memberships'
            }
          ]
        }),
        kind,
        mode: 'membership',
        name: 'pro_accounts',
        runNow: true
      }
    case 'alert':
      return {
        configText: jsonTemplate({
          dedupe_key: 'account.id',
          dedupe_seconds: 300,
          match: {
            all: [
              { op: 'eq', path: 'event_type', value: 'payment_failed' },
              { op: 'eq', path: 'severity', value: 'critical' }
            ]
          },
          notifications: {
            webhooks: [
              {
                headers: {
                  'x-alert-source': 'nanotrace'
                },
                id: 'pager',
                max_attempts: 3,
                url: 'https://alerts.example.com/nanotrace'
              }
            ]
          },
          severity: 'critical'
        }),
        kind,
        mode: 'event_match',
        name: 'payment_failed_critical',
        runNow: false
      }
  }
}

function definitionDraftFromRecommendation(item: QueryRecommendationItem): DefinitionDraft | null {
  const recommendation = item.recommendation
  const path = recommendation.path?.trim()
  if (recommendation.targetType === 'field' && path && isDefinitionPath(path)) {
    return {
      configText: jsonTemplate({
        path,
        value_type: 'string'
      }),
      kind: 'field',
      mode: 'facet',
      name: definitionIdentifierFromParts(['field', path]),
      runNow: true
    }
  }
  if (recommendation.targetType === 'measure' && path && isDefinitionPath(path)) {
    return {
      configText: jsonTemplate({
        path,
        value_type: 'number'
      }),
      kind: 'measure',
      mode: 'measure',
      name: definitionIdentifierFromParts(['measure', path]),
      runNow: true
    }
  }
  if (recommendation.targetType === 'report' && recommendation.targetTable === 'report_results') {
    const groupByPaths = uniqueDefinitionPaths((recommendation.groupBy ?? item.record.group_by_paths).filter(isDefinitionPath))
    if (groupByPaths.length === 0) return null
    const reportId = definitionIdentifierFromParts(['events', ...groupByPaths])
    return {
      configText: jsonTemplate({
        outputs: [
          {
            bucket_seconds: 60,
            dimensions: groupByPaths.map(path => ({
              name: definitionIdentifierFromParts([path]),
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
      }),
      kind: 'report',
      mode: 'summary',
      name: reportId,
      runNow: true
    }
  }
  return null
}

function recommendationApprovalStatus(
  item: QueryRecommendationItem,
  definitions: DefinitionRecord[],
  materializationJobs: MaterializationJobRecord[]
): RecommendationApprovalStatus {
  const draft = definitionDraftFromRecommendation(item)
  if (!draft) {
    return {
      detail: 'needs literal query text or richer semantics',
      label: 'manual review',
      tone: 'muted'
    }
  }

  const definition = definitionMatchingRecommendation(item, definitions)
  if (!definition) {
    return {
      detail: draft.name,
      label: 'ready',
      tone: 'active'
    }
  }

  if (definition.kind === 'report') {
    const targetId = reportIdFromRecommendation(item)
    const job = targetId ? latestMaterializationJob(materializationJobs, targetId, 'report') : null
    if (job) {
      return {
        detail: `${job.completed_chunks.toLocaleString()}/${job.total_chunks.toLocaleString()} chunks · ${formatDate(job.updated_at)}`,
        label: `${job.status} job`,
        tone: job.status === 'completed' ? 'ok' : job.status === 'failed' || job.failed_chunks > 0 ? 'error' : 'active'
      }
    }
    return {
      detail: definition.name,
      label: 'definition exists',
      tone: 'ok'
    }
  }

  if (definition.backfill) {
    return {
      detail: `${definition.backfill.rows_matched.toLocaleString()} rows · ${formatDate(definition.backfill.updated_at)}`,
      label: `${definition.backfill.status} backfill`,
      tone: definition.backfill.status === 'completed' ? 'ok' : 'active'
    }
  }

  return {
    detail: definition.name,
    label: 'definition exists',
    tone: 'ok'
  }
}

function definitionMatchingRecommendation(item: QueryRecommendationItem, definitions: DefinitionRecord[]) {
  const recommendation = item.recommendation
  const path = recommendation.path?.trim()
  if (recommendation.targetType === 'field' && path) {
    return definitions.find(definition => definition.kind === 'field' && String(definition.config?.path ?? '') === path)
  }
  if (recommendation.targetType === 'measure' && path) {
    return definitions.find(definition => definition.kind === 'measure' && String(definition.config?.path ?? '') === path)
  }
  if (recommendation.targetType === 'report' && recommendation.targetTable === 'report_results') {
    const reportId = reportIdFromRecommendation(item)
    if (!reportId) return undefined
    return definitions.find(definition => definition.kind === 'report' && definitionHasReportOutput(definition, reportId))
  }
  return undefined
}

function definitionHasReportOutput(definition: DefinitionRecord, reportId: string) {
  return definitionOutputObjects(definition).some(output => {
    const target = typeof output.target === 'string' ? output.target : ''
    const id = typeof output.report_id === 'string' ? output.report_id : ''
    return target === 'report_results' && id === reportId
  })
}

function definitionOutputObjects(definition: DefinitionRecord) {
  const outputs = definition.config?.outputs
  if (!Array.isArray(outputs)) return []
  return outputs.filter((output): output is Record<string, unknown> => Boolean(output) && typeof output === 'object' && !Array.isArray(output))
}

function reportIdFromRecommendation(item: QueryRecommendationItem) {
  const recommendation = item.recommendation
  const groupByPaths = uniqueDefinitionPaths((recommendation.groupBy ?? item.record.group_by_paths).filter(isDefinitionPath))
  return groupByPaths.length > 0 ? definitionIdentifierFromParts(['events', ...groupByPaths]) : ''
}

function latestMaterializationJob(materializationJobs: MaterializationJobRecord[], targetId: string, targetType: string) {
  return materializationJobs
    .filter(job => job.target_id === targetId && job.target_type === targetType)
    .sort((left, right) => right.updated_at.localeCompare(left.updated_at))[0] ?? null
}

function recommendationStatusClassName(tone: RecommendationApprovalStatus['tone']) {
  switch (tone) {
    case 'active':
      return 'text-sky-300'
    case 'error':
      return 'text-red-300'
    case 'ok':
      return 'text-emerald-300'
    case 'muted':
      return 'text-neutral-500'
  }
}

function recommendationLabel(recommendation: QueryRecommendation) {
  const target = recommendation.targetType || recommendation.targetTable || recommendation.kind || 'definition'
  const path = recommendation.path ? ` ${recommendation.path}` : ''
  return `${labelize(target)}${path}`
}

function recommendationDetail(recommendation: QueryRecommendation) {
  return [
    recommendation.kind ? `kind=${recommendation.kind}` : '',
    recommendation.targetTable ? `table=${recommendation.targetTable}` : '',
    recommendation.groupBy?.length ? `groupBy=${recommendation.groupBy.join(', ')}` : '',
    recommendation.source ? `source=${recommendation.source}` : '',
    recommendation.operator ? `op=${recommendation.operator}` : '',
    recommendation.reason || ''
  ].filter(Boolean).join(' | ')
}

function labelize(value: string) {
  return value
    .split(/[_\s]+/)
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

function isDefinitionPath(value: string) {
  return /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$/.test(value.trim())
}

function uniqueDefinitionPaths(paths: string[]) {
  return paths
    .map(path => path.trim())
    .filter(isDefinitionPath)
    .filter((path, index, values) => values.indexOf(path) === index)
}

function definitionIdentifierFromParts(parts: string[]) {
  const value = parts
    .join('_')
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_')
  return value || 'definition'
}

function jsonTemplate(value: unknown) {
  return JSON.stringify(value, null, 2)
}

async function fetchDefinitions({ apiBaseUrl }: { apiBaseUrl: string }): Promise<{ definitions: DefinitionRecord[] }> {
  const response = await fetch(definitionsUrl(apiBaseUrl), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) throw await httpError(response)
  return (await response.json()) as { definitions: DefinitionRecord[] }
}

async function createAdvancedDefinition({
  apiBaseUrl,
  draft,
  materializationWindow
}: {
  apiBaseUrl: string
  draft: DefinitionDraft
  materializationWindow: { end: string; start: string }
}): Promise<CreateDefinitionResult> {
  const name = draft.name.trim()
  const mode = draft.mode.trim()
  if (!name || !mode) throw new HTTPError({ message: 'definition name and mode are required', status: 400 })
  let config: unknown
  try {
    config = JSON.parse(draft.configText)
  } catch {
    throw new HTTPError({ message: 'definition config must be valid JSON', status: 400 })
  }
  const sourceStart = draft.runNow ? materializationTimestamp(materializationWindow.start) : ''
  const sourceEnd = draft.runNow ? materializationTimestamp(materializationWindow.end) : ''
  if (draft.runNow && Date.parse(sourceEnd) <= Date.parse(sourceStart)) {
    throw new HTTPError({ message: 'backfill window must have an end after start', status: 400 })
  }

  const response = await fetch(definitionsUrl(apiBaseUrl), {
    body: JSON.stringify({
      ...(isBackfillableDraftKind(draft.kind) && draft.runNow ? { backfill: { from: sourceStart, to: sourceEnd } } : {}),
      config,
      kind: draft.kind,
      mode,
      name
    }),
    credentials: 'include',
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { backfill?: DefinitionBackfill; definition?: DefinitionRecord }
  if (!body.definition) throw new HTTPError({ message: 'definition response missing definition', status: 502 })

  if (draft.runNow && isQueuedMaterializationDefinition(body.definition)) {
    const queued = await createMaterializationJob({
      apiBaseUrl,
      definitionId: body.definition.definition_id,
      sourceEnd,
      sourceStart
    })
    return { definition: body.definition, job: queued.backfill }
  }

  return { backfill: body.backfill, definition: body.definition }
}

async function deleteDefinition({
  apiBaseUrl,
  definitionId
}: {
  apiBaseUrl: string
  definitionId: string
}): Promise<DefinitionRecord> {
  const response = await fetch(`${definitionsUrl(apiBaseUrl)}/${encodeURIComponent(definitionId)}`, {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'DELETE'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { definition?: DefinitionRecord }
  if (!body.definition) throw new HTTPError({ message: 'definition response missing definition', status: 502 })
  return body.definition
}

async function backfillDefinition({
  apiBaseUrl,
  definitionId,
  from,
  to
}: {
  apiBaseUrl: string
  definitionId: string
  from?: string
  to?: string
}): Promise<DefinitionBackfill> {
  const response = await fetch(`${definitionsUrl(apiBaseUrl)}/${encodeURIComponent(definitionId)}/backfill`, {
    body: JSON.stringify({
      from,
      to
    }),
    credentials: 'include',
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { backfill?: DefinitionBackfill }
  if (!body.backfill) throw new HTTPError({ message: 'backfill response missing backfill', status: 502 })
  return body.backfill
}

async function fetchMaterializationJobs({ apiBaseUrl }: { apiBaseUrl: string }): Promise<{ jobs: MaterializationJobRecord[] }> {
  const response = await fetch(backfillsUrl(apiBaseUrl), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { backfills?: MaterializationJobRecord[] }
  return { jobs: body.backfills ?? [] }
}

async function fetchQueryRecommendations({
  apiBaseUrl
}: {
  apiBaseUrl: string
}): Promise<{ recommendations: QueryRecommendationRecord[] }> {
  const response = await fetch(`${queryRecommendationsUrl(apiBaseUrl)}?limit=50`, {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) throw await httpError(response)
  return (await response.json()) as { recommendations: QueryRecommendationRecord[] }
}

async function createMaterializationJob({
  apiBaseUrl,
  definitionId,
  sourceEnd,
  sourceStart
}: {
  apiBaseUrl: string
  definitionId: string
  sourceEnd: string
  sourceStart: string
}): Promise<CreateBackfillJobResponse> {
  const response = await fetch(definitionBackfillsUrl(apiBaseUrl, definitionId), {
    body: JSON.stringify({
      chunk_seconds: 3600,
      source_end: materializationTimestamp(sourceEnd),
      source_start: materializationTimestamp(sourceStart)
    }),
    credentials: 'include',
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) throw await httpError(response)
  return (await response.json()) as CreateBackfillJobResponse
}

function definitionsUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/v1/definitions` : '/v1/definitions'
}

function backfillsUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/v1/backfills` : '/v1/backfills'
}

function definitionBackfillsUrl(apiBaseUrl: string, definitionId: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  const path = `/v1/definitions/${encodeURIComponent(definitionId)}/backfills`
  return base ? `${base}${path}` : path
}

function queryRecommendationsUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/v1/query/recommendations` : '/v1/query/recommendations'
}

async function httpError(response: Response) {
  const text = await response.text()
  let message = text || response.statusText
  try {
    const body = JSON.parse(text) as { error?: unknown }
    message = typeof body.error === 'string' ? body.error : JSON.stringify(body)
  } catch {
    // Keep the plain response body when the server did not return JSON.
  }
  return new HTTPError({ message: message || response.statusText, status: response.status })
}
