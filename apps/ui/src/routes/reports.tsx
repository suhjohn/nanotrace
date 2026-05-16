import { createFileRoute } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { PanelLeftOpen, RefreshCw, Trash2 } from 'lucide-react'
import { useAppShell } from '../lib/app-shell'
import { HTTPError, errorMessage, nanotraceApiBaseUrl, queryHeaders } from '../lib/nanotrace-api'

export const Route = createFileRoute('/reports')({
  component: ReportsRoute
})

type ReportKind = 'summary' | 'sequence' | 'cohort' | 'retention'

type ReportRecord = {
  config: Record<string, unknown>
  created_at: string
  enabled: boolean
  kind: ReportKind
  name: string
  report_id: string
  updated_at: string
  version: number
}

function ReportsRoute() {
  const observatoryUrl = nanotraceApiBaseUrl()
  const queryClient = useQueryClient()
  const { setSidebarOpen, sidebarOpen } = useAppShell()

  const reportsQuery = useQuery({
    queryKey: ['reports', observatoryUrl],
    queryFn: () => fetchReports({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const reports = reportsQuery.data?.reports ?? []

  const deleteReportMutation = useMutation({
    mutationFn: (reportId: string) => deleteReport({ apiBaseUrl: observatoryUrl, reportId }),
    onSuccess: deleted => {
      queryClient.setQueryData<{ reports: ReportRecord[] }>(['reports', observatoryUrl], current => ({
        reports: (current?.reports ?? []).filter(report => report.report_id !== deleted.report_id)
      }))
    }
  })

  const reportError =
    errorMessage(reportsQuery.error) ||
    errorMessage(deleteReportMutation.error)
  const headerStatus = reportsQuery.error
    ? 'unavailable'
    : reportsQuery.isFetching
      ? 'loading'
      : `${reports.length} reports`

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
                <h2 className="text-[13px] font-medium text-white">Reports</h2>
                <span className="hidden text-[11px] text-neutral-600 sm:inline">{headerStatus}</span>
              </div>
              <div className="flex shrink-0 items-center gap-2">
                <span className="text-[11px] text-neutral-600 sm:hidden">{headerStatus}</span>
                <button
                  className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                  disabled={reportsQuery.isFetching}
                  type="button"
                  onClick={() => void reportsQuery.refetch()}
                >
                  <RefreshCw size={13} strokeWidth={1.8} />
                  Refresh
                </button>
              </div>
            </div>
            {reportError ? <div className="border-b border-neutral-800 px-3 py-2 text-[11px] text-red-300">{reportError}</div> : null}
            <div className="overflow-x-auto">
              <table className="w-full min-w-[860px] border-collapse text-left text-[12px]">
                <thead className="border-b border-neutral-800 text-[11px] uppercase text-neutral-600">
                  <tr>
                    <th className="px-3 py-2 font-medium">Name</th>
                    <th className="px-3 py-2 font-medium">Type</th>
                    <th className="px-3 py-2 font-medium">Config</th>
                    <th className="px-3 py-2 font-medium">Updated</th>
                    <th className="px-3 py-2 text-right font-medium">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {reports.map(report => (
                    <tr key={report.report_id} className="border-b border-neutral-900 align-top last:border-b-0">
                      <td className="max-w-[260px] truncate px-3 py-2 font-medium text-white">{report.name}</td>
                      <td className="px-3 py-2 text-neutral-400">{report.kind}</td>
                      <td className="max-w-[460px] truncate px-3 py-2 font-mono text-[11px] text-neutral-500">
                        {reportConfigLabel(report)}
                      </td>
                      <td className="px-3 py-2 text-neutral-500">{formatDate(report.updated_at)}</td>
                      <td className="px-3 py-2 text-right">
                        <button
                          aria-label={`Remove ${report.name}`}
                          className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                          disabled={deleteReportMutation.isPending}
                          type="button"
                          onClick={() => deleteReportMutation.mutate(report.report_id)}
                        >
                          <Trash2 size={13} strokeWidth={1.8} />
                          Remove
                        </button>
                      </td>
                    </tr>
                  ))}
                  {reportsQuery.error ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={5}>
                        Reports unavailable.
                      </td>
                    </tr>
                  ) : null}
                  {!reportsQuery.isLoading && !reportsQuery.error && reports.length === 0 ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={5}>
                        No reports.
                      </td>
                    </tr>
                  ) : null}
                  {reportsQuery.isLoading ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={5}>
                        Loading reports...
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

function reportConfigLabel(report: ReportRecord) {
  if (report.kind === 'summary') {
    const groupBy = Array.isArray(report.config.group_by) ? report.config.group_by.map(String).join(', ') : ''
    return [String(report.config.metric ?? ''), groupBy ? `by ${groupBy}` : '', String(report.config.bucket ?? '')]
      .filter(Boolean)
      .join(' · ')
  }
  if (report.kind === 'sequence') {
    const steps = Array.isArray(report.config.steps) ? report.config.steps.map(String).join(' -> ') : ''
    return [String(report.config.entity_id_path ?? ''), steps, String(report.config.window ?? '')]
      .filter(Boolean)
      .join(' · ')
  }
  return JSON.stringify(report.config)
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

async function fetchReports({ apiBaseUrl }: { apiBaseUrl: string }): Promise<{ reports: ReportRecord[] }> {
  const response = await fetch(reportsUrl(apiBaseUrl), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) throw await httpError(response)
  return (await response.json()) as { reports: ReportRecord[] }
}

async function deleteReport({
  apiBaseUrl,
  reportId
}: {
  apiBaseUrl: string
  reportId: string
}): Promise<ReportRecord> {
  const response = await fetch(`${reportsUrl(apiBaseUrl)}/${encodeURIComponent(reportId)}`, {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'DELETE'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { report?: ReportRecord }
  if (!body.report) throw new HTTPError({ message: 'report response missing report', status: 502 })
  return body.report
}

function reportsUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/v1/reports` : '/v1/reports'
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
