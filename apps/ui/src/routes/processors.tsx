import { createFileRoute } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { PanelLeftOpen, RefreshCw, Trash2 } from 'lucide-react'
import { useMemo } from 'react'
import { useAppShell } from '../lib/app-shell'
import { HTTPError, errorMessage, nanotraceApiBaseUrl, queryHeaders } from '../lib/nanotrace-api'

export const Route = createFileRoute('/processors')({
  component: ProcessorsRoute
})

type ProcessorArtifact = {
  key: string
  sha256: string
}

type ProcessorManifest = {
  artifact_key?: string | null
  artifact_sha256?: string | null
  artifacts?: Record<string, ProcessorArtifact>
  config?: unknown
  configs?: Record<string, unknown>
  error?: string | null
  name: string
  stages: string[]
  status: string
  updated_at?: string | null
}

function ProcessorsRoute() {
  const observatoryUrl = nanotraceApiBaseUrl()
  const queryClient = useQueryClient()
  const { setSidebarOpen, sidebarOpen } = useAppShell()

  const processorsQuery = useQuery({
    queryKey: ['processors', observatoryUrl],
    queryFn: () => fetchProcessors({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const processors = processorsQuery.data?.processors ?? []
  const statusCounts = useMemo(() => countByStatus(processors), [processors])

  const deleteMutation = useMutation({
    mutationFn: (processorName: string) => deleteProcessor({ apiBaseUrl: observatoryUrl, name: processorName }),
    onSuccess: deleted => {
      queryClient.setQueryData<{ processors: ProcessorManifest[] }>(['processors', observatoryUrl], current => ({
        processors: (current?.processors ?? []).filter(processor => processor.name !== deleted.name)
      }))
    }
  })

  const processorError = errorMessage(processorsQuery.error) || errorMessage(deleteMutation.error)
  const headerStatus = processorsQuery.error ? 'unavailable' : processorsQuery.isFetching ? 'loading' : `${processors.length} processors`

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
                <h2 className="text-[13px] font-medium text-white">Configured processors</h2>
                <div className="hidden text-[11px] text-neutral-600 sm:block">
                  {statusCounts.ready ? `${statusCounts.ready} ready` : ''}
                  {statusCounts.building ? `${statusCounts.ready ? ' · ' : ''}${statusCounts.building} building` : ''}
                  {statusCounts.failed ? `${statusCounts.ready || statusCounts.building ? ' · ' : ''}${statusCounts.failed} failed` : ''}
                </div>
                <span className="hidden text-[11px] text-neutral-600 md:inline">{headerStatus}</span>
              </div>
              <div className="flex shrink-0 items-center gap-2">
                <span className="text-[11px] text-neutral-600 md:hidden">{headerStatus}</span>
                <button
                  className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                  disabled={processorsQuery.isFetching}
                  type="button"
                  onClick={() => void processorsQuery.refetch()}
                >
                  <RefreshCw size={13} strokeWidth={1.8} />
                  Refresh
                </button>
              </div>
            </div>
            {processorError ? <div className="border-b border-neutral-800 px-3 py-2 text-[11px] text-red-300">{processorError}</div> : null}
            <div className="overflow-x-auto">
              <table className="w-full min-w-[920px] border-collapse text-left text-[12px]">
                <thead className="border-b border-neutral-800 text-[11px] uppercase text-neutral-600">
                  <tr>
                    <th className="px-3 py-2 font-medium">Name</th>
                    <th className="px-3 py-2 font-medium">Status</th>
                    <th className="px-3 py-2 font-medium">Stages</th>
                    <th className="px-3 py-2 font-medium">Artifacts</th>
                    <th className="px-3 py-2 font-medium">Updated</th>
                    <th className="px-3 py-2 text-right font-medium">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {processors.map(processor => (
                    <tr key={processor.name} className="border-b border-neutral-900 align-top last:border-b-0">
                      <td className="max-w-[220px] px-3 py-2">
                        <div className="truncate font-medium text-white">{processor.name}</div>
                        {processor.error ? <div className="mt-1 line-clamp-2 text-[11px] text-red-300">{processor.error}</div> : null}
                      </td>
                      <td className="px-3 py-2">
                        <ProcessorStatus status={processor.status} />
                      </td>
                      <td className="px-3 py-2 text-neutral-400">{processor.stages.join(', ') || 'none'}</td>
                      <td className="px-3 py-2">
                        <ArtifactSummary processor={processor} />
                      </td>
                      <td className="px-3 py-2 text-neutral-500">{formatDate(processor.updated_at)}</td>
                      <td className="px-3 py-2 text-right">
                        <button
                          aria-label={`Remove ${processor.name}`}
                          className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                          disabled={deleteMutation.isPending}
                          title={`Remove ${processor.name}`}
                          type="button"
                          onClick={() => deleteMutation.mutate(processor.name)}
                        >
                          <Trash2 size={13} strokeWidth={1.8} />
                          Remove
                        </button>
                      </td>
                    </tr>
                  ))}
                  {processorsQuery.error ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={6}>
                        Processors unavailable.
                      </td>
                    </tr>
                  ) : null}
                  {!processorsQuery.isLoading && !processorsQuery.error && processors.length === 0 ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={6}>
                        No processors.
                      </td>
                    </tr>
                  ) : null}
                  {processorsQuery.isLoading ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={6}>
                        Loading processors...
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

function ProcessorStatus({ status }: { status: string }) {
  const color =
    status === 'ready'
      ? 'text-emerald-300'
      : status === 'failed'
        ? 'text-red-300'
        : status === 'building'
          ? 'text-yellow-300'
          : 'text-neutral-500'
  return <span className={color}>{status || 'unknown'}</span>
}

function ArtifactSummary({ processor }: { processor: ProcessorManifest }) {
  const artifacts = Object.entries(processor.artifacts ?? {})
  if (artifacts.length === 0) return <span className="text-neutral-600">none</span>
  return (
    <div className="grid gap-1">
      {artifacts.map(([stage, artifact]) => (
        <div key={stage} className="min-w-0 text-neutral-500">
          <span className="text-neutral-400">{stage}</span>
          <span className="mx-1 text-neutral-700">·</span>
          <span className="font-mono">{artifact.sha256.slice(0, 12)}</span>
        </div>
      ))}
    </div>
  )
}

function countByStatus(processors: ProcessorManifest[]) {
  return processors.reduce<Record<string, number>>((counts, processor) => {
    counts[processor.status] = (counts[processor.status] ?? 0) + 1
    return counts
  }, {})
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

async function fetchProcessors({ apiBaseUrl }: { apiBaseUrl: string }): Promise<{ processors: ProcessorManifest[] }> {
  const response = await fetch(processorsUrl(apiBaseUrl), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) throw await httpError(response)
  return (await response.json()) as { processors: ProcessorManifest[] }
}

async function deleteProcessor({
  apiBaseUrl,
  name
}: {
  apiBaseUrl: string
  name: string
}): Promise<ProcessorManifest> {
  const response = await fetch(`${processorsUrl(apiBaseUrl)}/${encodeURIComponent(name)}`, {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'DELETE'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { processor?: ProcessorManifest }
  if (!body.processor) throw new HTTPError({ message: 'processor response missing processor', status: 502 })
  return body.processor
}

function processorsUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/v1/processors` : '/v1/processors'
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
