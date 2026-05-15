export const selectedOrganizationStorageKey = 'nanotrace.organization_id'

export function queryHeaders() {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  const token = runtimeNanotraceApiKey()
  const organizationId = selectedOrganizationId()
  if (token) headers.Authorization = `Bearer ${token}`
  if (organizationId) headers['x-nanotrace-organization-id'] = organizationId
  return headers
}

export function selectedOrganizationId() {
  if (typeof window === 'undefined') return ''
  const params = new URLSearchParams(window.location.search)
  const urlOrganizationId =
    params.get('nanotrace_organization_id') ||
    params.get('organization_id') ||
    ''
  if (urlOrganizationId) {
    setSelectedOrganizationId(urlOrganizationId)
    params.delete('nanotrace_organization_id')
    params.delete('organization_id')
    const search = params.toString()
    const nextUrl = `${window.location.pathname}${search ? `?${search}` : ''}${window.location.hash}`
    window.history.replaceState(window.history.state, '', nextUrl)
    return urlOrganizationId
  }
  return window.localStorage.getItem(selectedOrganizationStorageKey) || ''
}

export function setSelectedOrganizationId(organizationId: string) {
  if (typeof window === 'undefined') return
  const value = organizationId.trim()
  if (value) {
    window.localStorage.setItem(selectedOrganizationStorageKey, value)
  } else {
    window.localStorage.removeItem(selectedOrganizationStorageKey)
  }
  window.dispatchEvent(new CustomEvent('nanotrace-organization-change', { detail: value }))
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
