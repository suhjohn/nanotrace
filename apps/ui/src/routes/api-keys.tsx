import { createFileRoute } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Check, Clipboard, PanelLeftOpen, Plus, Trash2, X } from 'lucide-react'
import { useMemo, useState } from 'react'
import { cn } from '../lib/cn'
import { useAppShell } from '../lib/app-shell'
import { nanotraceApiBaseUrl, queryHeaders } from '../lib/nanotrace-api'

export const Route = createFileRoute('/settings/api-keys')({
  component: ApiKeysRoute
})

type ApiKeyRole = 'admin' | 'service' | 'viewer'

type ApiKeyRecord = {
  id: number
  name: string
  prefix: string
  role: ApiKeyRole
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

type AuthIdentity = {
  organization_id: string
  organization_name: string
}

type HTTPErrorInit = {
  message: string
  status: number
}

class HTTPError extends Error {
  status: number

  constructor({ message, status }: HTTPErrorInit) {
    super(message)
    this.name = 'HTTPError'
    this.status = status
  }
}

function ApiKeysRoute() {
  const observatoryUrl = nanotraceApiBaseUrl()
  const queryClient = useQueryClient()
  const { setSidebarOpen, sidebarOpen } = useAppShell()
  const [name, setName] = useState('')
  const [role, setRole] = useState<ApiKeyRole>('service')
  const [expiresAt, setExpiresAt] = useState('')
  const [createdKey, setCreatedKey] = useState('')
  const [copied, setCopied] = useState(false)

  const apiKeysQuery = useQuery({
    queryKey: ['api-keys', observatoryUrl],
    queryFn: () => fetchApiKeys({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const authQuery = useQuery({
    queryKey: ['auth', observatoryUrl, 'me'],
    queryFn: () => fetchAuthMe({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const apiKeys = apiKeysQuery.data?.api_keys ?? []
  const organizationName = authQuery.data?.organization_name || 'selected organization'
  const activeCount = useMemo(() => apiKeys.filter(isActiveApiKey).length, [apiKeys])

  const createMutation = useMutation({
    mutationFn: () =>
      createApiKey({
        apiBaseUrl: observatoryUrl,
        expiresAt: dateTimeLocalInputToIso(expiresAt),
        name,
        role
      }),
    onSuccess: created => {
      setCreatedKey(created.key)
      setCopied(false)
      setName('')
      setExpiresAt('')
      queryClient.setQueryData<{ api_keys: ApiKeyRecord[] }>(['api-keys', observatoryUrl], current => ({
        api_keys: [created.api_key, ...(current?.api_keys ?? [])]
      }))
    }
  })

  const revokeMutation = useMutation({
    mutationFn: (id: number) => revokeApiKey({ apiBaseUrl: observatoryUrl, id }),
    onSuccess: revoked => {
      queryClient.setQueryData<{ api_keys: ApiKeyRecord[] }>(['api-keys', observatoryUrl], current => ({
        api_keys: (current?.api_keys ?? []).map(apiKey => apiKey.id === revoked.id ? revoked : apiKey)
      }))
    }
  })

  async function copyCreatedKey() {
    if (!createdKey) return
    await navigator.clipboard.writeText(createdKey)
    setCopied(true)
  }

  const error = apiKeysQuery.error || createMutation.error || revokeMutation.error
  const headerStatus = apiKeysQuery.error ? 'unavailable' : apiKeysQuery.isFetching ? 'loading' : `${activeCount} active`

  return (
    <main className="flex h-full min-h-0 min-w-0 flex-col overflow-hidden bg-black text-[13px] text-neutral-100">
      <section className="min-h-0 flex-1 overflow-auto bg-black">
        <div className="grid w-full min-w-0 content-start gap-4 p-2 sm:p-4">
          <section className="grid content-start gap-3 border border-neutral-800 bg-neutral-950 p-3">
            <div className="flex min-w-0 items-center justify-between gap-3">
              <div className="flex min-w-0 items-start gap-2">
                {!sidebarOpen ? (
                  <button
                    aria-label="Expand navigation"
                    className="mt-0.5 inline-flex h-7 w-7 shrink-0 items-center justify-center border border-neutral-800 bg-black text-neutral-400 hover:bg-white/[0.04] hover:text-white"
                    title="Expand navigation"
                    type="button"
                    onClick={() => setSidebarOpen(true)}
                  >
                    <PanelLeftOpen size={15} strokeWidth={1.8} />
                  </button>
                ) : null}
                <div className="min-w-0">
                  <h1 className="truncate text-[13px] font-medium text-white">Create API key</h1>
                  <p className="mt-0.5 text-[11px] text-neutral-600">Keys belong only to {organizationName}. The secret is shown once.</p>
                </div>
              </div>
            </div>
            <div className="grid gap-2 lg:grid-cols-[minmax(180px,1fr)_140px_210px_auto]">
              <label className="grid gap-1 text-[11px] text-neutral-500">
                Name
                <input
                  className="h-8 min-w-0 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none placeholder:text-neutral-600 focus:border-neutral-600"
                  value={name}
                  onChange={event => setName(event.target.value)}
                  placeholder="production ingest"
                />
              </label>
              <label className="grid gap-1 text-[11px] text-neutral-500">
                Role
                <select
                  className="h-8 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none focus:border-neutral-600"
                  value={role}
                  onChange={event => setRole(event.target.value as ApiKeyRole)}
                >
                  <option value="service">service</option>
                  <option value="viewer">viewer</option>
                  <option value="admin">admin</option>
                </select>
              </label>
              <label className="grid gap-1 text-[11px] text-neutral-500">
                Expires
                <input
                  className="h-8 min-w-0 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none focus:border-neutral-600"
                  type="datetime-local"
                  value={expiresAt}
                  onChange={event => setExpiresAt(event.target.value)}
                />
              </label>
              <div className="flex items-end">
                <button
                  className="inline-flex h-8 w-full items-center justify-center gap-1.5 border border-neutral-700 bg-white px-3 text-[12px] font-medium text-black hover:bg-neutral-200 disabled:border-neutral-900 disabled:bg-black disabled:text-neutral-700 lg:w-auto"
                  disabled={!name.trim() || createMutation.isPending}
                  type="button"
                  onClick={() => createMutation.mutate()}
                >
                  <Plus size={13} strokeWidth={2} />
                  Create
                </button>
              </div>
            </div>

            {createdKey ? (
              <div className="grid gap-2 border border-neutral-800 bg-black p-2">
                <div className="flex items-center justify-between gap-2">
                  <span className="text-[11px] font-medium text-white">New API key</span>
                  <button
                    aria-label="Dismiss new API key"
                    className="inline-flex h-6 w-6 items-center justify-center text-neutral-500 hover:bg-white/[0.04] hover:text-white"
                    type="button"
                    onClick={() => setCreatedKey('')}
                  >
                    <X size={13} strokeWidth={1.8} />
                  </button>
                </div>
                <div className="grid gap-1.5 md:grid-cols-[minmax(0,1fr)_auto]">
                  <input
                    readOnly
                    className="h-8 min-w-0 border border-neutral-800 bg-neutral-950 px-2 font-mono text-[11px] text-white outline-none"
                    value={createdKey}
                    onFocus={event => event.currentTarget.select()}
                  />
                  <button
                    className="inline-flex h-8 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white"
                    type="button"
                    onClick={() => void copyCreatedKey()}
                  >
                    {copied ? <Check size={13} strokeWidth={1.8} /> : <Clipboard size={13} strokeWidth={1.8} />}
                    {copied ? 'Copied' : 'Copy'}
                  </button>
                </div>
              </div>
            ) : null}

            {error ? <div className="text-[11px] text-red-300">{errorMessage(error)}</div> : null}
          </section>

          <section className="min-h-0 border border-neutral-800 bg-neutral-950">
            <div className="flex items-center justify-between gap-2 border-b border-neutral-800 px-3 py-2">
              <div className="flex min-w-0 items-center gap-2">
                <h2 className="text-[13px] font-medium text-white">Keys</h2>
                <span className="truncate text-[11px] text-neutral-600">{organizationName} · {headerStatus}</span>
              </div>
              <button
                className="h-7 shrink-0 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white"
                disabled={apiKeysQuery.isFetching}
                type="button"
                onClick={() => void apiKeysQuery.refetch()}
              >
                Refresh
              </button>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[760px] border-collapse text-left text-[12px]">
                <thead className="border-b border-neutral-800 text-[11px] uppercase text-neutral-600">
                  <tr>
                    <th className="px-3 py-2 font-medium">Name</th>
                    <th className="px-3 py-2 font-medium">Role</th>
                    <th className="px-3 py-2 font-medium">Prefix</th>
                    <th className="px-3 py-2 font-medium">Created</th>
                    <th className="px-3 py-2 font-medium">Last used</th>
                    <th className="px-3 py-2 font-medium">Status</th>
                    <th className="px-3 py-2 text-right font-medium">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {apiKeys.map(apiKey => (
                    <tr key={apiKey.id} className={cn('border-b border-neutral-900 last:border-b-0', apiKey.revoked_at && 'text-neutral-600')}>
                      <td className="max-w-[220px] truncate px-3 py-2 text-white">{apiKey.name}</td>
                      <td className="px-3 py-2 text-neutral-400">{apiKey.role}</td>
                      <td className="px-3 py-2 font-mono text-neutral-500">{apiKey.prefix}</td>
                      <td className="px-3 py-2 text-neutral-500">{formatDate(apiKey.created_at)}</td>
                      <td className="px-3 py-2 text-neutral-500">{formatDate(apiKey.last_used_at)}</td>
                      <td className="px-3 py-2">
                        <ApiKeyStatus apiKey={apiKey} />
                      </td>
                      <td className="px-3 py-2 text-right">
                        {!apiKey.revoked_at ? (
                          <button
                            aria-label={`Remove ${apiKey.name}`}
                            className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                            disabled={revokeMutation.isPending}
                            title={`Remove ${apiKey.name}`}
                            type="button"
                            onClick={() => revokeMutation.mutate(apiKey.id)}
                          >
                            <Trash2 size={13} strokeWidth={1.8} />
                            Remove
                          </button>
                        ) : null}
                      </td>
                    </tr>
                  ))}
                  {apiKeysQuery.error ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={7}>
                        API keys unavailable.
                      </td>
                    </tr>
                  ) : null}
                  {!apiKeysQuery.isLoading && !apiKeysQuery.error && apiKeys.length === 0 ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={7}>
                        No API keys.
                      </td>
                    </tr>
                  ) : null}
                  {apiKeysQuery.isLoading ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={7}>
                        Loading API keys...
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

function ApiKeyStatus({ apiKey }: { apiKey: ApiKeyRecord }) {
  if (apiKey.revoked_at) {
    return <span className="text-neutral-600">revoked</span>
  }
  if (apiKey.expires_at && Date.parse(apiKey.expires_at) <= Date.now()) {
    return <span className="text-yellow-300">expired</span>
  }
  return <span className="text-emerald-300">active</span>
}

function isActiveApiKey(apiKey: ApiKeyRecord) {
  return !apiKey.revoked_at && (!apiKey.expires_at || Date.parse(apiKey.expires_at) > Date.now())
}

function dateTimeLocalInputToIso(value: string) {
  if (!value) return undefined
  const time = Date.parse(value)
  return Number.isFinite(time) ? new Date(time).toISOString() : undefined
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

async function createApiKey({
  apiBaseUrl,
  expiresAt,
  name,
  role
}: {
  apiBaseUrl: string
  expiresAt?: string
  name: string
  role: ApiKeyRole
}): Promise<CreatedApiKey> {
  const response = await fetch(apiKeysUrl(apiBaseUrl), {
    body: JSON.stringify({
      expires_at: expiresAt,
      name: name.trim(),
      role
    }),
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

function apiKeysUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/v1/api-keys` : '/v1/api-keys'
}

function authUrl(apiBaseUrl: string, path: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/auth${path}` : `/auth${path}`
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : error ? String(error) : ''
}
