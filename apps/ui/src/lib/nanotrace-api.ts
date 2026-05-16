export function queryHeaders() {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  const token = runtimeNanotraceApiKey()
  if (token) headers.Authorization = `Bearer ${token}`
  return headers
}

export function nanotraceApiBaseUrl() {
  return String(import.meta.env.VITE_NANOTRACE_URL || '').trim().replace(/\/+$/, '')
}

export function runtimeNanotraceApiKey() {
  const configured = import.meta.env.VITE_NANOTRACE_API_KEY
  if (configured) return configured
  if (typeof window === 'undefined') return ''

  const params = new URLSearchParams(window.location.search)
  const urlKey = params.get('nanotrace_api_key') || params.get('api_key') || ''
  if (urlKey) {
    window.localStorage.setItem('nanotrace.api_key', urlKey)
    params.delete('nanotrace_api_key')
    params.delete('api_key')
    const search = params.toString()
    const nextUrl = `${window.location.pathname}${search ? `?${search}` : ''}${window.location.hash}`
    window.history.replaceState(window.history.state, '', nextUrl)
    return urlKey
  }

  return window.localStorage.getItem('nanotrace.api_key') || ''
}

export type HotFacet = {
  aggregate_enabled?: boolean
  display_name: string
  lookup_enabled?: boolean
  path: string
  removable: boolean
  source: string
  status: string
  value_type: string
}

export type FacetListResponse = {
  facets: HotFacet[]
}

export type FacetBackfill = {
  completed_chunks: number
  error: string
  failed_chunks: number
  indexed_events: number
  job_id: string
  path: string
  status: string
  total_chunks: number
  values: number
}

export type FacetValueType =
  | 'string'
  | 'low_cardinality_string'
  | 'float'
  | 'integer'
  | 'unsigned'
  | 'bool'
  | 'datetime'

export class HTTPError extends Error {
  status: number

  constructor({ message, status }: { message: string; status: number }) {
    super(message)
    this.name = 'HTTPError'
    this.status = status
  }
}

export async function fetchFacets({ apiBaseUrl }: { apiBaseUrl: string }): Promise<FacetListResponse> {
  const response = await fetch(facetsUrl(apiBaseUrl), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) throw await httpError(response)
  return (await response.json()) as FacetListResponse
}

export async function putFacet({
  aggregateEnabled,
  apiBaseUrl,
  displayName,
  lookupEnabled,
  path,
  valueType
}: {
  aggregateEnabled?: boolean
  apiBaseUrl: string
  displayName?: string
  lookupEnabled?: boolean
  path: string
  valueType: string
}): Promise<HotFacet> {
  const response = await fetch(facetsUrl(apiBaseUrl), {
    body: JSON.stringify({
      aggregate_enabled: aggregateEnabled,
      display_name: displayName?.trim() || undefined,
      lookup_enabled: lookupEnabled,
      path: path.trim(),
      value_type: valueType
    }),
    credentials: 'include',
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { facet?: HotFacet }
  if (!body.facet) throw new HTTPError({ message: 'facet response missing facet', status: 502 })
  return body.facet
}

export async function deleteFacet({
  apiBaseUrl,
  path
}: {
  apiBaseUrl: string
  path: string
}): Promise<HotFacet> {
  const response = await fetch(`${facetsUrl(apiBaseUrl)}/${encodeURIComponent(path)}`, {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'DELETE'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { facet?: HotFacet }
  if (!body.facet) throw new HTTPError({ message: 'facet response missing facet', status: 502 })
  return body.facet
}

export async function backfillFacet({
  apiBaseUrl,
  path
}: {
  apiBaseUrl: string
  path: string
}): Promise<FacetBackfill> {
  const response = await fetch(`${facetsUrl(apiBaseUrl)}/${encodeURIComponent(path)}/backfill`, {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { backfill?: FacetBackfill }
  if (!body.backfill) throw new HTTPError({ message: 'backfill response missing backfill', status: 502 })
  return body.backfill
}

export async function fetchFacetBackfills({ apiBaseUrl }: { apiBaseUrl: string }): Promise<{ backfills: FacetBackfill[] }> {
  const response = await fetch(`${facetsUrl(apiBaseUrl)}/backfills`, {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) throw await httpError(response)
  return (await response.json()) as { backfills: FacetBackfill[] }
}

export async function fetchFacetBackfill({
  apiBaseUrl,
  jobId
}: {
  apiBaseUrl: string
  jobId: string
}): Promise<FacetBackfill> {
  const response = await fetch(`${facetsUrl(apiBaseUrl)}/backfills/${encodeURIComponent(jobId)}`, {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { backfill?: FacetBackfill }
  if (!body.backfill) throw new HTTPError({ message: 'backfill response missing backfill', status: 502 })
  return body.backfill
}

export function facetsUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/facets` : '/facets'
}

export function errorMessage(error: unknown) {
  const message = error instanceof Error ? error.message : error ? String(error) : ''
  return conciseErrorMessage(message)
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

function conciseErrorMessage(message: string) {
  if (!message) return ''
  if (message.includes('DB::Exception') || message.includes('Code: ')) {
    const duplicateJsonPath = message.match(/Duplicate path found during parsing JSON object: ([^.\s]+)/)
    if (duplicateJsonPath) {
      return `ClickHouse rejected the operation because existing events contain duplicate JSON key '${duplicateJsonPath[1]}'.`
    }
    const firstLine = message.split('\n')[0]?.trim()
    return firstLine.length > 240 ? `${firstLine.slice(0, 240)}...` : firstLine
  }
  return message.length > 500 ? `${message.slice(0, 500)}...` : message
}
