import { createFileRoute } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { PanelLeftOpen, RefreshCw, Trash2 } from 'lucide-react'
import { useState } from 'react'
import { useAppShell } from '../lib/app-shell'
import { HTTPError, errorMessage, nanotraceApiBaseUrl, queryHeaders } from '../lib/nanotrace-api'

export const Route = createFileRoute('/schema')({
  component: SchemaRoute
})

type DefinitionKind = 'field' | 'measure' | 'rollup' | 'state'

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

function SchemaRoute() {
  const observatoryUrl = nanotraceApiBaseUrl()
  const queryClient = useQueryClient()
  const { setSidebarOpen, sidebarOpen } = useAppShell()
  const [backfillingDefinitionId, setBackfillingDefinitionId] = useState<string | null>(null)

  const definitionsQuery = useQuery({
    queryKey: ['definitions', observatoryUrl],
    queryFn: () => fetchDefinitions({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const definitions = definitionsQuery.data?.definitions ?? []

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

  const definitionError =
    errorMessage(definitionsQuery.error) ||
    errorMessage(deleteDefinitionMutation.error) ||
    errorMessage(backfillDefinitionMutation.error)
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
                    <th className="px-3 py-2 font-medium">Backfill</th>
                    <th className="px-3 py-2 font-medium">Updated</th>
                    <th className="px-3 py-2 text-right font-medium">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {definitions.map(definition => (
                    <tr key={definition.definition_id} className="border-b border-neutral-900 align-top last:border-b-0">
                      <td className="max-w-[260px] truncate px-3 py-2 font-mono text-[11px] font-medium text-white">{definitionPathLabel(definition)}</td>
                      <td className="px-3 py-2 text-neutral-400">{definition.kind}</td>
                      <td className="px-3 py-2 text-neutral-400">{definition.mode}</td>
                      <td className="max-w-[360px] truncate px-3 py-2 font-mono text-[11px] text-neutral-500">{definitionConfigLabel(definition)}</td>
                      <td className="px-3 py-2">
                        <BackfillStatus status={definition.backfill} />
                      </td>
                      <td className="px-3 py-2 text-neutral-500">{formatDate(definition.updated_at)}</td>
                      <td className="px-3 py-2 text-right">
                        <div className="flex justify-end gap-2">
                          {definition.backfill?.status === 'completed' ? (
                            <span className="inline-flex h-7 items-center px-2 text-[12px] text-neutral-600">Backfilled</span>
                          ) : (
                            <button
                              className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                              disabled={backfillDefinitionMutation.isPending}
                              type="button"
                              onClick={() => backfillDefinitionMutation.mutate(definition.definition_id)}
                            >
                              <RefreshCw size={13} strokeWidth={1.8} />
                              {backfillingDefinitionId === definition.definition_id ? 'Running' : 'Backfill'}
                            </button>
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
                  ))}
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
  return ''
}

function definitionPathLabel(definition: DefinitionRecord) {
  const config = definition.config ?? {}
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

function upsertDefinition(definitions: DefinitionRecord[], definition: DefinitionRecord) {
  const next = definitions.filter(candidate => candidate.definition_id !== definition.definition_id)
  next.unshift(definition)
  return next.sort((left, right) => right.updated_at.localeCompare(left.updated_at))
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

async function fetchDefinitions({ apiBaseUrl }: { apiBaseUrl: string }): Promise<{ definitions: DefinitionRecord[] }> {
  const response = await fetch(definitionsUrl(apiBaseUrl), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) throw await httpError(response)
  return (await response.json()) as { definitions: DefinitionRecord[] }
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

function definitionsUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/v1/definitions` : '/v1/definitions'
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
