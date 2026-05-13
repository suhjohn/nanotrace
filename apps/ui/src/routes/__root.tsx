import { Link, Outlet, createRootRoute, useRouterState } from '@tanstack/react-router'
import { QueryClient, QueryClientProvider, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { TanStackRouterDevtools } from '@tanstack/react-router-devtools'
import { BarChart3, ListTree, LogOut, PanelLeftClose, UserCircle } from 'lucide-react'
import { useState } from 'react'
import type { ReactNode } from 'react'
import { cn } from '../lib/cn'
import { AppShellProvider } from '../lib/app-shell'

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
      <AppShell>
        <Outlet />
      </AppShell>
      <TanStackRouterDevtools position="bottom-right" />
    </QueryClientProvider>
  )
}

function AppShell({ children }: { children: ReactNode }) {
  const pathname = useRouterState({ select: state => state.location.pathname })
  const isDashboard = pathname.startsWith('/dashboard')
  const [sidebarOpen, setSidebarOpen] = useState(true)

  return (
    <AppShellProvider value={{ sidebarOpen, setSidebarOpen }}>
      <div className="fixed inset-0 flex min-h-0 min-w-0 overflow-hidden bg-black text-neutral-100">
        {sidebarOpen ? (
          <aside className="relative z-50 flex w-14 shrink-0 flex-col items-center border-r border-neutral-800 bg-neutral-950 py-2">
            <button
              aria-label="Collapse navigation"
              className="mb-3 flex h-8 w-8 items-center justify-center border border-neutral-800 bg-black text-neutral-400 hover:bg-white/[0.04] hover:text-white"
              title="Collapse navigation"
              type="button"
              onClick={() => setSidebarOpen(false)}
            >
              <PanelLeftClose size={16} strokeWidth={1.8} />
            </button>
            <nav className="flex flex-1 flex-col items-center gap-1">
              <NavItem active={!isDashboard} label="Logs" to="/" icon={<ListTree size={17} strokeWidth={1.8} />} />
              <NavItem active={isDashboard} label="Dashboard" to="/dashboard" icon={<BarChart3 size={17} strokeWidth={1.8} />} />
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
  role: 'admin' | 'service' | 'viewer'
  subject: string
}

function AccountControl() {
  const observatoryUrl = import.meta.env.VITE_NANOTRACE_URL || ''
  const queryClient = useQueryClient()
  const [open, setOpen] = useState(false)
  const [loginEmail, setLoginEmail] = useState('')
  const [loginSent, setLoginSent] = useState(false)
  const authQuery = useQuery({
    queryKey: ['auth', observatoryUrl, 'me'],
    queryFn: () => fetchAuthMe({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const loginMutation = useMutation({
    mutationFn: () => requestLoginLink({ apiBaseUrl: observatoryUrl, email: loginEmail }),
    onSuccess: () => setLoginSent(true)
  })
  const currentUser = authQuery.data ?? null
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
          'flex h-10 w-10 items-center justify-center rounded-full border border-transparent bg-black text-[11px] font-medium text-neutral-400 hover:border-neutral-800 hover:text-white',
          open && 'border-neutral-700 text-white'
        )}
        title={currentUser ? `${authLabel} (${currentUser.role})` : 'Sign in'}
        type="button"
        onClick={() => {
          setOpen(value => !value)
          setLoginSent(false)
        }}
      >
        {currentUser ? accountInitial : <UserCircle size={17} strokeWidth={1.8} />}
      </button>
      {open ? (
        <div className="absolute bottom-0 left-12 z-50 w-[min(340px,calc(100vw-84px))] border border-neutral-800 bg-neutral-950 p-2 shadow-2xl shadow-black/60">
          {currentUser ? (
            <div className="grid gap-2">
              <div className="border-b border-neutral-800 px-2 pb-2">
                <div className="truncate text-[12px] text-white">{authLabel}</div>
                <div className="text-[11px] text-neutral-600">{currentUser.role}</div>
              </div>
              <button
                className="inline-flex h-8 items-center gap-2 border border-neutral-800 bg-black px-2 text-left text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white"
                type="button"
                onClick={() => void logout()}
              >
                <LogOut size={14} strokeWidth={1.8} />
                Logout
              </button>
            </div>
          ) : (
            <form
              className="grid gap-2"
              onSubmit={event => {
                event.preventDefault()
                loginMutation.mutate()
              }}
            >
              <div>
                <div className="text-[12px] text-white">Sign in</div>
                <div className="text-[11px] text-neutral-600">Receive a magic link by email.</div>
              </div>
              <input
                className="h-8 w-full border border-neutral-800 bg-black px-2 text-[12px] text-neutral-200 outline-none placeholder:text-neutral-600 focus:border-neutral-600"
                placeholder="email"
                type="email"
                value={loginEmail}
                onChange={event => {
                  setLoginEmail(event.target.value)
                  setLoginSent(false)
                }}
              />
              <button
                className="h-8 border border-neutral-700 bg-black px-2 text-[12px] text-neutral-200 hover:bg-white/[0.04] disabled:border-neutral-900 disabled:text-neutral-700"
                disabled={!loginEmail.trim() || loginMutation.isPending}
                type="submit"
              >
                {loginMutation.isPending ? 'Sending' : loginSent ? 'Sent' : 'Send link'}
              </button>
              {loginMutation.error ? (
                <div className="text-[11px] text-red-300">{errorMessage(loginMutation.error)}</div>
              ) : null}
            </form>
          )}
        </div>
      ) : null}
    </div>
  )
}

async function fetchAuthMe({ apiBaseUrl }: { apiBaseUrl: string }): Promise<AuthIdentity> {
  const response = await fetch(authUrl(apiBaseUrl, '/me'), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(text || response.statusText)
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

function queryHeaders() {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  const token = runtimeNanotraceApiKey()
  if (token) headers.Authorization = `Bearer ${token}`
  return headers
}

function runtimeNanotraceApiKey() {
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

function identityInitial(label: string) {
  const trimmed = label.trim()
  return (trimmed[0] || '?').toUpperCase()
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : error ? String(error) : ''
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
  to: '/' | '/dashboard'
}) {
  return (
    <Link
      aria-label={label}
      className={cn(
        'group relative flex h-10 w-10 items-center justify-center border border-transparent text-neutral-500 hover:border-neutral-800 hover:bg-black hover:text-white',
        active && 'border-neutral-700 bg-black text-white'
      )}
      title={label}
      to={to}
    >
      {icon}
      <span className="pointer-events-none absolute left-12 top-1/2 z-50 -translate-y-1/2 whitespace-nowrap border border-neutral-800 bg-neutral-950 px-2 py-1 text-[11px] text-neutral-200 opacity-0 shadow-xl shadow-black/40 group-hover:opacity-100">
        {label}
      </span>
    </Link>
  )
}
