import { Link, Outlet, createRootRoute, useRouterState } from '@tanstack/react-router'
import { QueryClient, QueryClientProvider, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { TanStackRouterDevtools } from '@tanstack/react-router-devtools'
import { BarChart3, KeyRound, ListTree, LogOut, Mail, PanelLeftClose, Send, UserCircle, UsersRound } from 'lucide-react'
import { useState } from 'react'
import type { ReactNode } from 'react'
import { cn } from '../lib/cn'
import { AppShellProvider } from '../lib/app-shell'
import { queryHeaders, selectedOrganizationId, setSelectedOrganizationId } from '../lib/nanotrace-api'

export const Route = createRootRoute({
  component: RootDocument,
  notFoundComponent: () => <p style={{ padding: 12 }}>Not found.</p>
})

function RootDocument() {
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            refetchOnWindowFocus: false,
            staleTime: 3_000
          }
        }
      })
  )

  return (
    <QueryClientProvider client={queryClient}>
      <AuthGate>
        <AppShell>
          <Outlet />
        </AppShell>
      </AuthGate>
      <TanStackRouterDevtools position="bottom-right" />
    </QueryClientProvider>
  )
}

function AuthGate({ children }: { children: ReactNode }) {
  const observatoryUrl = import.meta.env.VITE_NANOTRACE_URL || ''
  const authQuery = useQuery({
    queryKey: ['auth', observatoryUrl, 'me'],
    queryFn: () => fetchAuthMe({ apiBaseUrl: observatoryUrl }),
    retry: false
  })

  if (authQuery.isLoading) return <AuthLoading />
  if (isUnauthorizedError(authQuery.error)) return <SignInScreen />
  if (authQuery.error) return <AuthErrorScreen error={authQuery.error} onRetry={() => void authQuery.refetch()} />

  return children
}

function AuthLoading() {
  return (
    <div className="fixed inset-0 grid place-items-center bg-black text-neutral-500">
      <div className="text-[12px]">Loading</div>
    </div>
  )
}

function AuthErrorScreen({ error, onRetry }: { error: unknown; onRetry: () => void }) {
  return (
    <div className="fixed inset-0 grid place-items-center bg-black px-4 text-neutral-100">
      <div className="grid w-full max-w-sm gap-3 border border-neutral-800 bg-neutral-950 p-4">
        <div>
          <div className="text-[13px] font-medium text-white">Unable to load session</div>
          <div className="mt-1 text-[12px] text-neutral-500">{errorMessage(error)}</div>
        </div>
        <button
          className="h-8 border border-neutral-700 bg-black px-3 text-[12px] text-neutral-200 hover:bg-white/[0.04]"
          type="button"
          onClick={onRetry}
        >
          Retry
        </button>
      </div>
    </div>
  )
}

function SignInScreen() {
  const observatoryUrl = import.meta.env.VITE_NANOTRACE_URL || ''
  const queryClient = useQueryClient()
  const [loginEmail, setLoginEmail] = useState('')
  const [loginSent, setLoginSent] = useState(false)
  const loginMutation = useMutation({
    mutationFn: () => requestLoginLink({ apiBaseUrl: observatoryUrl, email: loginEmail }),
    onSuccess: async () => {
      setLoginSent(true)
      await queryClient.invalidateQueries({ queryKey: ['auth', observatoryUrl, 'me'] })
    }
  })

  return (
    <div className="fixed inset-0 grid place-items-center bg-black px-4 text-neutral-100">
      <form
        className="grid w-full max-w-sm gap-3 border border-neutral-800 bg-neutral-950 p-4 shadow-2xl shadow-black/60"
        onSubmit={event => {
          event.preventDefault()
          loginMutation.mutate()
        }}
      >
        <div className="flex items-center gap-2">
          <div className="flex h-8 w-8 items-center justify-center border border-neutral-800 bg-black text-neutral-400">
            <Mail size={15} strokeWidth={1.8} />
          </div>
          <div className="min-w-0">
            <h1 className="truncate text-[14px] font-medium text-white">Sign in to Nanotrace</h1>
            <p className="mt-0.5 text-[12px] text-neutral-500">Receive a magic link by email.</p>
          </div>
        </div>
        <input
          autoComplete="email"
          autoFocus
          className="h-9 w-full border border-neutral-800 bg-black px-2 text-[13px] text-neutral-200 outline-none placeholder:text-neutral-600 focus:border-neutral-600"
          placeholder="email"
          type="email"
          value={loginEmail}
          onChange={event => {
            setLoginEmail(event.target.value)
            setLoginSent(false)
          }}
        />
        <button
          className="inline-flex h-9 items-center justify-center gap-1.5 border border-neutral-700 bg-white px-3 text-[13px] font-medium text-black hover:bg-neutral-200 disabled:border-neutral-900 disabled:bg-black disabled:text-neutral-700"
          disabled={!loginEmail.trim() || loginMutation.isPending}
          type="submit"
        >
          <Send size={14} strokeWidth={1.8} />
          {loginMutation.isPending ? 'Sending' : loginSent ? 'Sent' : 'Send link'}
        </button>
        {loginSent ? <div className="text-[12px] text-neutral-400">Check your email for the sign-in link.</div> : null}
        {loginMutation.error ? (
          <div className="text-[12px] text-red-300">{errorMessage(loginMutation.error)}</div>
        ) : null}
      </form>
    </div>
  )
}

function AppShell({ children }: { children: ReactNode }) {
  const pathname = useRouterState({ select: state => state.location.pathname })
  const isDashboard = pathname.startsWith('/dashboard')
  const isApiKeys = pathname.startsWith('/settings/api-keys')
  const isOrganizations = pathname.startsWith('/settings/organizations')
  const [sidebarOpen, setSidebarOpen] = useState(true)

  return (
    <AppShellProvider value={{ sidebarOpen, setSidebarOpen }}>
      <div className="fixed inset-0 flex min-h-0 min-w-0 overflow-hidden bg-black text-neutral-100">
        {sidebarOpen ? (
          <aside className="relative z-50 flex w-56 shrink-0 flex-col border-r border-neutral-800 bg-neutral-950 p-2">
            <button
              aria-label="Collapse navigation"
              className="mb-3 flex h-8 w-8 items-center justify-center border border-neutral-800 bg-black text-neutral-400 hover:bg-white/[0.04] hover:text-white"
              title="Collapse navigation"
              type="button"
              onClick={() => setSidebarOpen(false)}
            >
              <PanelLeftClose size={16} strokeWidth={1.8} />
            </button>
            <nav className="flex flex-1 flex-col gap-1">
              <NavItem active={!isDashboard && !isApiKeys && !isOrganizations} label="Logs" to="/" icon={<ListTree size={17} strokeWidth={1.8} />} />
              <NavItem active={isDashboard} label="Dashboard" to="/dashboard" icon={<BarChart3 size={17} strokeWidth={1.8} />} />
              <NavItem active={isOrganizations} label="Organizations" to="/settings/organizations" icon={<UsersRound size={17} strokeWidth={1.8} />} />
              <NavItem active={isApiKeys} label="API keys" to="/settings/api-keys" icon={<KeyRound size={17} strokeWidth={1.8} />} />
            </nav>
            <AccountControl />
          </aside>
        ) : null}
        <section className="min-h-0 min-w-0 flex-1 overflow-hidden">{children}</section>
      </div>
    </AppShellProvider>
  )
}

type AuthIdentity = {
  auth_type: 'api_key' | 'session'
  email?: string
  name?: string
  organization_id: string
  organization_name: string
  role: 'admin' | 'service' | 'viewer'
  subject: string
}

type OrganizationRecord = {
  id: string
  name: string
  slug: string
  plan: string
}

function AccountControl() {
  const observatoryUrl = import.meta.env.VITE_NANOTRACE_URL || ''
  const queryClient = useQueryClient()
  const [open, setOpen] = useState(false)
  const authQuery = useQuery({
    queryKey: ['auth', observatoryUrl, 'me'],
    queryFn: () => fetchAuthMe({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const currentUser = authQuery.data ?? null
  const organizationsQuery = useQuery({
    enabled: Boolean(currentUser),
    queryKey: ['organizations', observatoryUrl, currentUser?.organization_id],
    queryFn: () => fetchOrganizations({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const organizations = organizationsQuery.data?.organizations ?? []
  const currentOrganizationId = selectedOrganizationId() || currentUser?.organization_id || ''
  const currentOrganization = organizations.find(organization => organization.id === currentOrganizationId)
  const authLabel = currentUser?.email || currentUser?.name || currentUser?.subject || ''
  const accountInitial = identityInitial(authLabel || 'user')

  async function logout() {
    await fetch(authUrl(observatoryUrl, '/logout'), {
      credentials: 'include',
      headers: queryHeaders(),
      method: 'POST'
    })
    await queryClient.invalidateQueries({ queryKey: ['auth', observatoryUrl, 'me'] })
    setOpen(false)
  }

  return (
    <div className="relative mt-auto">
      <button
        aria-expanded={open}
        aria-label={currentUser ? `Account: ${authLabel}` : 'Sign in'}
        className={cn(
          'flex w-full items-center gap-2 border border-neutral-800 bg-black p-2 text-left text-[11px] font-medium text-neutral-400 hover:bg-white/[0.04] hover:text-white',
          open && 'border-neutral-700 text-white'
        )}
        title={currentUser ? `${authLabel} (${currentUser.role}) in ${currentOrganization?.name || currentUser.organization_name}` : 'Sign in'}
        type="button"
        onClick={() => {
          setOpen(value => !value)
        }}
      >
        <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full border border-neutral-800 bg-neutral-950 text-[11px] text-neutral-300">
          {currentUser ? accountInitial : <UserCircle size={17} strokeWidth={1.8} />}
        </span>
        <span className="min-w-0">
          <span className="block truncate text-[12px] text-neutral-200">{currentOrganization?.name || currentUser?.organization_name || 'Organization'}</span>
          <span className="block truncate text-[10px] text-neutral-600">{authLabel || 'Sign in'}</span>
        </span>
      </button>
      {open && currentUser ? (
        <div className="absolute bottom-0 left-[calc(100%+8px)] z-50 w-[min(340px,calc(100vw-248px))] border border-neutral-800 bg-neutral-950 p-2 shadow-2xl shadow-black/60">
          <div className="grid gap-2">
            <div className="border-b border-neutral-800 px-2 pb-2">
              <div className="truncate text-[12px] text-white">{authLabel}</div>
              <div className="text-[11px] text-neutral-600">{currentUser.role}</div>
            </div>
            {organizations.length > 0 ? (
              <label className="grid gap-1 px-2 text-[11px] text-neutral-500">
                Organization
                <select
                  className="h-8 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none focus:border-neutral-600"
                  value={currentOrganizationId}
                  onChange={event => {
                    setSelectedOrganizationId(event.target.value)
                    void queryClient.invalidateQueries()
                  }}
                >
                  {organizations.map(organization => (
                    <option key={organization.id} value={organization.id}>
                      {organization.name}
                    </option>
                  ))}
                </select>
              </label>
            ) : null}
            <button
              className="inline-flex h-8 items-center gap-2 border border-neutral-800 bg-black px-2 text-left text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white"
              type="button"
              onClick={() => void logout()}
            >
              <LogOut size={14} strokeWidth={1.8} />
              Logout
            </button>
          </div>
        </div>
      ) : null}
    </div>
  )
}

async function fetchOrganizations({ apiBaseUrl }: { apiBaseUrl: string }): Promise<{ organizations: OrganizationRecord[] }> {
  const response = await fetch(organizationsUrl(apiBaseUrl), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(text || response.statusText)
  }
  return (await response.json()) as { organizations: OrganizationRecord[] }
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
    throw new Error(text || response.statusText)
  }
  return (await response.json()) as { ok: boolean }
}

function authUrl(apiBaseUrl: string, path: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/auth${path}` : `/auth${path}`
}

function organizationsUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/organizations` : '/organizations'
}

function identityInitial(label: string) {
  const trimmed = label.trim()
  return (trimmed[0] || '?').toUpperCase()
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : error ? String(error) : ''
}

function isUnauthorizedError(error: unknown) {
  return error instanceof HTTPError && error.status === 401
}

class HTTPError extends Error {
  status: number

  constructor({ message, status }: { message: string; status: number }) {
    super(message)
    this.name = 'HTTPError'
    this.status = status
  }
}

function NavItem({
  active,
  icon,
  label,
  to
}: {
  active: boolean
  icon: ReactNode
  label: string
  to: '/' | '/dashboard' | '/settings/api-keys' | '/settings/organizations'
}) {
  return (
    <Link
      aria-label={label}
      className={cn(
        'group relative flex h-9 items-center gap-2 border border-transparent px-2 text-[12px] text-neutral-500 hover:border-neutral-800 hover:bg-black hover:text-white',
        active && 'border-neutral-700 bg-black text-white'
      )}
      title={label}
      to={to}
    >
      {icon}
      <span className="truncate">{label}</span>
    </Link>
  )
}
