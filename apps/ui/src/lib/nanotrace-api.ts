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

export class HTTPError extends Error {
  status: number

  constructor({ message, status }: { message: string; status: number }) {
    super(message)
    this.name = 'HTTPError'
    this.status = status
  }
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
