import { createFileRoute } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Building2, PanelLeftOpen, Plus, RefreshCw, Trash2, X } from 'lucide-react'
import { useState } from 'react'
import { useAppShell } from '../lib/app-shell'
import { cn } from '../lib/cn'
import { nanotraceApiBaseUrl, queryHeaders } from '../lib/nanotrace-api'

export const Route = createFileRoute('/settings/organization')({
  component: OrganizationRoute
})

type AuthRole = 'admin' | 'service' | 'viewer'

type AuthIdentity = {
  auth_type: 'api_key' | 'session'
  subject: string
  role: AuthRole
  organization_id: string
  organization_name: string
  organizations?: OrganizationMembershipSummary[]
}

type OrganizationMembershipSummary = {
  organization_id: string
  organization_name: string
  slug: string
  role: AuthRole
}

type OrganizationMember = {
  organization_id: string
  subject: string
  email: string
  name?: string | null
  role: 'admin' | 'viewer'
  created_at: string
  updated_at: string
}

type OrganizationInvitation = {
  id: number
  organization_id: string
  email: string
  role: 'admin' | 'viewer'
  invited_by: string
  created_at: string
  expires_at: string
  accepted_at?: string | null
  revoked_at?: string | null
}

class HTTPError extends Error {
  status: number

  constructor({ message, status }: { message: string; status: number }) {
    super(message)
    this.name = 'HTTPError'
    this.status = status
  }
}

function OrganizationRoute() {
  const observatoryUrl = nanotraceApiBaseUrl()
  const queryClient = useQueryClient()
  const { setSidebarOpen, sidebarOpen } = useAppShell()
  const [inviteEmail, setInviteEmail] = useState('')
  const [inviteRole, setInviteRole] = useState<'admin' | 'viewer'>('viewer')

  const authQuery = useQuery({
    queryKey: ['auth', observatoryUrl, 'me'],
    queryFn: () => fetchAuthMe({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const identity = authQuery.data
  const activeMembership = identity?.organizations?.find(item => item.organization_id === identity.organization_id)
  const organizationId = identity?.organization_id || ''

  const membersQuery = useQuery({
    enabled: Boolean(organizationId),
    queryKey: ['organization-members', observatoryUrl, organizationId],
    queryFn: () => fetchMembers({ apiBaseUrl: observatoryUrl, organizationId }),
    retry: false
  })
  const invitationsQuery = useQuery({
    enabled: Boolean(organizationId),
    queryKey: ['organization-invitations', observatoryUrl, organizationId],
    queryFn: () => fetchInvitations({ apiBaseUrl: observatoryUrl, organizationId }),
    retry: false
  })

  const inviteMutation = useMutation({
    mutationFn: () =>
      createInvitation({
        apiBaseUrl: observatoryUrl,
        email: inviteEmail,
        organizationId,
        role: inviteRole
      }),
    onSuccess: created => {
      setInviteEmail('')
      queryClient.setQueryData<{ invitations: OrganizationInvitation[] }>(
        ['organization-invitations', observatoryUrl, organizationId],
        current => ({ invitations: [created, ...(current?.invitations ?? [])] })
      )
    }
  })
  const roleMutation = useMutation({
    mutationFn: ({ role, subject }: { role: 'admin' | 'viewer'; subject: string }) =>
      updateMemberRole({ apiBaseUrl: observatoryUrl, organizationId, role, subject }),
    onSuccess: member => {
      queryClient.setQueryData<{ members: OrganizationMember[] }>(['organization-members', observatoryUrl, organizationId], current => ({
        members: (current?.members ?? []).map(existing => existing.subject === member.subject ? member : existing)
      }))
      void queryClient.invalidateQueries({ queryKey: ['auth', observatoryUrl, 'me'] })
    }
  })
  const removeMutation = useMutation({
    mutationFn: (subject: string) => removeMember({ apiBaseUrl: observatoryUrl, organizationId, subject }),
    onSuccess: member => {
      queryClient.setQueryData<{ members: OrganizationMember[] }>(['organization-members', observatoryUrl, organizationId], current => ({
        members: (current?.members ?? []).filter(existing => existing.subject !== member.subject)
      }))
      void queryClient.invalidateQueries({ queryKey: ['auth', observatoryUrl, 'me'] })
    }
  })
  const revokeMutation = useMutation({
    mutationFn: (invitationId: number) => revokeInvitation({ apiBaseUrl: observatoryUrl, invitationId, organizationId }),
    onSuccess: invitation => {
      queryClient.setQueryData<{ invitations: OrganizationInvitation[] }>(
        ['organization-invitations', observatoryUrl, organizationId],
        current => ({
          invitations: (current?.invitations ?? []).map(existing => existing.id === invitation.id ? invitation : existing)
        })
      )
    }
  })

  const members = membersQuery.data?.members ?? []
  const invitations = invitationsQuery.data?.invitations ?? []
  const error = authQuery.error || membersQuery.error || invitationsQuery.error || inviteMutation.error || roleMutation.error || removeMutation.error || revokeMutation.error

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
                <div className="flex h-7 w-7 shrink-0 items-center justify-center border border-neutral-800 bg-black text-neutral-400">
                  <Building2 size={15} strokeWidth={1.8} />
                </div>
                <div className="min-w-0">
                  <h1 className="truncate text-[13px] font-medium text-white">{identity?.organization_name || 'Organization'}</h1>
                  <p className="mt-0.5 text-[11px] text-neutral-600">
                    {activeMembership?.slug || organizationId || 'No organization selected'} {activeMembership ? `(${activeMembership.role})` : ''}
                  </p>
                </div>
              </div>
              <button
                className="inline-flex h-7 shrink-0 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white"
                disabled={membersQuery.isFetching || invitationsQuery.isFetching}
                type="button"
                onClick={() => {
                  void membersQuery.refetch()
                  void invitationsQuery.refetch()
                }}
              >
                <RefreshCw size={13} strokeWidth={1.8} />
                Refresh
              </button>
            </div>
            {error ? <div className="text-[11px] text-red-300">{errorMessage(error)}</div> : null}
          </section>

          <section className="grid gap-3 border border-neutral-800 bg-neutral-950 p-3">
            <div>
              <h2 className="text-[13px] font-medium text-white">Invite member</h2>
              <p className="mt-0.5 text-[11px] text-neutral-600">Invites are tied to the exact email address.</p>
            </div>
            <div className="grid gap-2 sm:grid-cols-[minmax(180px,1fr)_140px_auto]">
              <input
                className="h-8 min-w-0 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none placeholder:text-neutral-600 focus:border-neutral-600"
                placeholder="email"
                type="email"
                value={inviteEmail}
                onChange={event => setInviteEmail(event.target.value)}
              />
              <select
                className="h-8 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none focus:border-neutral-600"
                value={inviteRole}
                onChange={event => setInviteRole(event.target.value as 'admin' | 'viewer')}
              >
                <option value="viewer">viewer</option>
                <option value="admin">admin</option>
              </select>
              <button
                className="inline-flex h-8 items-center justify-center gap-1.5 border border-neutral-700 bg-white px-3 text-[12px] font-medium text-black hover:bg-neutral-200 disabled:border-neutral-900 disabled:bg-black disabled:text-neutral-700"
                disabled={!inviteEmail.trim() || inviteMutation.isPending}
                type="button"
                onClick={() => inviteMutation.mutate()}
              >
                <Plus size={13} strokeWidth={1.8} />
                Invite
              </button>
            </div>
          </section>

          <OrganizationTable
            members={members}
            pending={membersQuery.isLoading}
            removePending={removeMutation.isPending}
            rolePending={roleMutation.isPending}
            onRemove={subject => removeMutation.mutate(subject)}
            onRoleChange={(subject, role) => roleMutation.mutate({ role, subject })}
          />

          <InvitationTable
            invitations={invitations}
            pending={invitationsQuery.isLoading}
            revokePending={revokeMutation.isPending}
            onRevoke={id => revokeMutation.mutate(id)}
          />
        </div>
      </section>
    </main>
  )
}

function OrganizationTable({
  members,
  onRemove,
  onRoleChange,
  pending,
  removePending,
  rolePending
}: {
  members: OrganizationMember[]
  onRemove: (subject: string) => void
  onRoleChange: (subject: string, role: 'admin' | 'viewer') => void
  pending: boolean
  removePending: boolean
  rolePending: boolean
}) {
  return (
    <section className="min-h-0 border border-neutral-800 bg-neutral-950">
      <div className="border-b border-neutral-800 px-3 py-2">
        <h2 className="text-[13px] font-medium text-white">Members</h2>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[720px] border-collapse text-left text-[12px]">
          <thead className="border-b border-neutral-800 text-[11px] uppercase text-neutral-600">
            <tr>
              <th className="px-3 py-2 font-medium">Email</th>
              <th className="px-3 py-2 font-medium">Subject</th>
              <th className="px-3 py-2 font-medium">Role</th>
              <th className="px-3 py-2 font-medium">Updated</th>
              <th className="px-3 py-2 text-right font-medium">Action</th>
            </tr>
          </thead>
          <tbody>
            {members.map(member => (
              <tr key={member.subject} className="border-b border-neutral-900 last:border-b-0">
                <td className="max-w-[220px] truncate px-3 py-2 text-white">{member.email}</td>
                <td className="max-w-[260px] truncate px-3 py-2 font-mono text-neutral-500">{member.subject}</td>
                <td className="px-3 py-2">
                  <select
                    className="h-7 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none focus:border-neutral-600"
                    disabled={rolePending}
                    value={member.role}
                    onChange={event => onRoleChange(member.subject, event.target.value as 'admin' | 'viewer')}
                  >
                    <option value="viewer">viewer</option>
                    <option value="admin">admin</option>
                  </select>
                </td>
                <td className="px-3 py-2 text-neutral-500">{formatDate(member.updated_at)}</td>
                <td className="px-3 py-2 text-right">
                  <button
                    aria-label={`Remove ${member.email}`}
                    className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                    disabled={removePending}
                    title={`Remove ${member.email}`}
                    type="button"
                    onClick={() => onRemove(member.subject)}
                  >
                    <Trash2 size={13} strokeWidth={1.8} />
                    Remove
                  </button>
                </td>
              </tr>
            ))}
            {pending ? (
              <tr>
                <td className="px-3 py-8 text-center text-neutral-600" colSpan={5}>
                  Loading members...
                </td>
              </tr>
            ) : null}
            {!pending && members.length === 0 ? (
              <tr>
                <td className="px-3 py-8 text-center text-neutral-600" colSpan={5}>
                  No members.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </section>
  )
}

function InvitationTable({
  invitations,
  onRevoke,
  pending,
  revokePending
}: {
  invitations: OrganizationInvitation[]
  onRevoke: (id: number) => void
  pending: boolean
  revokePending: boolean
}) {
  return (
    <section className="min-h-0 border border-neutral-800 bg-neutral-950">
      <div className="border-b border-neutral-800 px-3 py-2">
        <h2 className="text-[13px] font-medium text-white">Pending invitations</h2>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[720px] border-collapse text-left text-[12px]">
          <thead className="border-b border-neutral-800 text-[11px] uppercase text-neutral-600">
            <tr>
              <th className="px-3 py-2 font-medium">Email</th>
              <th className="px-3 py-2 font-medium">Role</th>
              <th className="px-3 py-2 font-medium">Created</th>
              <th className="px-3 py-2 font-medium">Status</th>
              <th className="px-3 py-2 text-right font-medium">Action</th>
            </tr>
          </thead>
          <tbody>
            {invitations.map(invitation => (
              <tr key={invitation.id} className={cn('border-b border-neutral-900 last:border-b-0', !isPendingInvitation(invitation) && 'text-neutral-600')}>
                <td className="max-w-[260px] truncate px-3 py-2 text-white">{invitation.email}</td>
                <td className="px-3 py-2 text-neutral-400">{invitation.role}</td>
                <td className="px-3 py-2 text-neutral-500">{formatDate(invitation.created_at)}</td>
                <td className="px-3 py-2 text-neutral-500">{invitationStatus(invitation)}</td>
                <td className="px-3 py-2 text-right">
                  {isPendingInvitation(invitation) ? (
                    <button
                      aria-label={`Revoke ${invitation.email}`}
                      className="inline-flex h-7 items-center justify-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                      disabled={revokePending}
                      title={`Revoke ${invitation.email}`}
                      type="button"
                      onClick={() => onRevoke(invitation.id)}
                    >
                      <X size={13} strokeWidth={1.8} />
                      Revoke
                    </button>
                  ) : null}
                </td>
              </tr>
            ))}
            {pending ? (
              <tr>
                <td className="px-3 py-8 text-center text-neutral-600" colSpan={5}>
                  Loading invitations...
                </td>
              </tr>
            ) : null}
            {!pending && invitations.length === 0 ? (
              <tr>
                <td className="px-3 py-8 text-center text-neutral-600" colSpan={5}>
                  No invitations.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </section>
  )
}

async function fetchAuthMe({ apiBaseUrl }: { apiBaseUrl: string }): Promise<AuthIdentity> {
  const response = await fetch(authUrl(apiBaseUrl, '/me'), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) throw await httpError(response)
  return (await response.json()) as AuthIdentity
}

async function fetchMembers({ apiBaseUrl, organizationId }: { apiBaseUrl: string; organizationId: string }) {
  const response = await fetch(v1Url(apiBaseUrl, `/organizations/${encodeURIComponent(organizationId)}/members`), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) throw await httpError(response)
  return (await response.json()) as { members: OrganizationMember[] }
}

async function fetchInvitations({ apiBaseUrl, organizationId }: { apiBaseUrl: string; organizationId: string }) {
  const response = await fetch(v1Url(apiBaseUrl, `/organizations/${encodeURIComponent(organizationId)}/invitations`), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) throw await httpError(response)
  return (await response.json()) as { invitations: OrganizationInvitation[] }
}

async function createInvitation({
  apiBaseUrl,
  email,
  organizationId,
  role
}: {
  apiBaseUrl: string
  email: string
  organizationId: string
  role: 'admin' | 'viewer'
}) {
  const response = await fetch(v1Url(apiBaseUrl, `/organizations/${encodeURIComponent(organizationId)}/invitations`), {
    body: JSON.stringify({ email: email.trim(), role }),
    credentials: 'include',
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { invitation?: OrganizationInvitation }
  if (!body.invitation) throw new HTTPError({ message: 'Invitation response missing invitation', status: 502 })
  return body.invitation
}

async function updateMemberRole({
  apiBaseUrl,
  organizationId,
  role,
  subject
}: {
  apiBaseUrl: string
  organizationId: string
  role: 'admin' | 'viewer'
  subject: string
}) {
  const response = await fetch(v1Url(apiBaseUrl, `/organizations/${encodeURIComponent(organizationId)}/members/${encodeURIComponent(subject)}`), {
    body: JSON.stringify({ role }),
    credentials: 'include',
    headers: queryHeaders(),
    method: 'PATCH'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { member?: OrganizationMember }
  if (!body.member) throw new HTTPError({ message: 'Member response missing member', status: 502 })
  return body.member
}

async function removeMember({
  apiBaseUrl,
  organizationId,
  subject
}: {
  apiBaseUrl: string
  organizationId: string
  subject: string
}) {
  const response = await fetch(v1Url(apiBaseUrl, `/organizations/${encodeURIComponent(organizationId)}/members/${encodeURIComponent(subject)}`), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'DELETE'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { member?: OrganizationMember }
  if (!body.member) throw new HTTPError({ message: 'Member response missing member', status: 502 })
  return body.member
}

async function revokeInvitation({
  apiBaseUrl,
  invitationId,
  organizationId
}: {
  apiBaseUrl: string
  invitationId: number
  organizationId: string
}) {
  const response = await fetch(v1Url(apiBaseUrl, `/organizations/${encodeURIComponent(organizationId)}/invitations/${invitationId}`), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'DELETE'
  })
  if (!response.ok) throw await httpError(response)
  const body = (await response.json()) as { invitation?: OrganizationInvitation }
  if (!body.invitation) throw new HTTPError({ message: 'Invitation response missing invitation', status: 502 })
  return body.invitation
}

function authUrl(apiBaseUrl: string, path: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/auth${path}` : `/auth${path}`
}

function v1Url(apiBaseUrl: string, path: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/v1${path}` : `/v1${path}`
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

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : error ? String(error) : ''
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

function isPendingInvitation(invitation: OrganizationInvitation) {
  return !invitation.accepted_at && !invitation.revoked_at && Date.parse(invitation.expires_at) > Date.now()
}

function invitationStatus(invitation: OrganizationInvitation) {
  if (invitation.accepted_at) return 'accepted'
  if (invitation.revoked_at) return 'revoked'
  if (Date.parse(invitation.expires_at) <= Date.now()) return 'expired'
  return 'pending'
}
