import { createFileRoute } from '@tanstack/react-router'
import { useMutation, useQueries, useQuery, useQueryClient } from '@tanstack/react-query'
import { Building2, Cloud, PanelLeftOpen, Plus, RefreshCcw, Trash2, UserPlus } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'
import { useAppShell } from '../lib/app-shell'
import { queryHeaders, setSelectedOrganizationId as setRuntimeSelectedOrganizationId } from '../lib/nanotrace-api'

export const Route = createFileRoute('/settings/organizations')({
  component: OrganizationsRoute
})

type OrganizationRecord = {
  id: string
  name: string
  slug: string
  plan: string
  created_at: string
  updated_at?: string
}

type OrganizationDataPlaneRecord = {
  organization_id: string
  mode: string
  provider: string
  region: string
  public_base_url: string
  ingest_url: string
  query_url: string
  internal_secret_ref: string
  s3_bucket: string
  processor_prefix: string
  clickhouse_mode: string
  clickhouse_provider: string
  clickhouse_region: string
  clickhouse_service_id: string
  clickhouse_url: string
  clickhouse_database: string
  kms_key_arn: string
  status: string
  status_message: string
  last_provisioning_job_id?: string | null
  created_at: string
  updated_at: string
}

type DataPlaneJobRecord = {
  id: string
  organization_id: string
  kind: string
  status: string
  provider: string
  region: string
  clickhouse_mode: string
  clickhouse_region: string
  error?: string | null
  created_at: string
  updated_at: string
  started_at?: string | null
  finished_at?: string | null
}

type RegionOption = {
  provider: string
  region: string
  clickhouse_provider: string
  clickhouse_region: string
  current: boolean
}

type OrganizationInviteRecord = {
  id: number
  organization_id: string
  email: string
  role: 'viewer' | 'admin' | 'service'
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

function OrganizationsRoute() {
  const observatoryUrl = import.meta.env.VITE_NANOTRACE_URL || ''
  const queryClient = useQueryClient()
  const { setSidebarOpen, sidebarOpen } = useAppShell()
  const [slug, setSlug] = useState('')
  const [name, setName] = useState('')
  const [selectedOrganizationId, setSelectedOrganizationId] = useState('')
  const [inviteOrganizationId, setInviteOrganizationId] = useState('')
  const [inviteEmail, setInviteEmail] = useState('')
  const [inviteRole, setInviteRole] = useState<'viewer' | 'admin'>('viewer')
  const [provisionRegion, setProvisionRegion] = useState('')
  const [provisionClickhouseMode, setProvisionClickhouseMode] = useState('shared-service')

  const organizationsQuery = useQuery({
    queryKey: ['organizations', observatoryUrl],
    queryFn: () => fetchOrganizations({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const organizations = organizationsQuery.data?.organizations ?? []
  const inviteOrganization = organizations.find(organization => organization.id === inviteOrganizationId) ?? organizations[0]
  const selectedInviteOrganizationId = inviteOrganization?.id ?? ''
  const selectedOrganization = organizations.find(organization => organization.id === selectedOrganizationId) ?? organizations[0]
  const activeOrganizationId = selectedOrganization?.id ?? ''

  const regionsQuery = useQuery({
    queryKey: ['regions', observatoryUrl],
    queryFn: () => fetchRegions({ apiBaseUrl: observatoryUrl }),
    retry: false
  })
  const regions = regionsQuery.data?.regions ?? []

  const dataPlaneQueries = useQueries({
    queries: organizations.map(organization => ({
      enabled: Boolean(organization.id),
      queryKey: ['organization-data-plane', observatoryUrl, organization.id],
      queryFn: () => fetchOrganizationDataPlane({ apiBaseUrl: observatoryUrl, organizationId: organization.id }),
      retry: false
    }))
  })
  const dataPlanesByOrganizationId = useMemo(() => {
    const entries = new Map<string, OrganizationDataPlaneRecord>()
    for (const query of dataPlaneQueries) {
      if (query.data) entries.set(query.data.organization_id, query.data)
    }
    return entries
  }, [dataPlaneQueries])
  const activeDataPlane = activeOrganizationId ? dataPlanesByOrganizationId.get(activeOrganizationId) : undefined

  useEffect(() => {
    if (!inviteOrganizationId && organizations[0]) {
      setInviteOrganizationId(organizations[0].id)
    }
  }, [inviteOrganizationId, organizations])

  useEffect(() => {
    if (!selectedOrganizationId && organizations[0]) {
      setSelectedOrganizationId(organizations[0].id)
    }
  }, [organizations, selectedOrganizationId])

  useEffect(() => {
    const current = regions.find(region => region.current) ?? regions[0]
    if (!provisionRegion && current) {
      setProvisionRegion(current.region)
    }
  }, [provisionRegion, regions])

  const invitesQuery = useQuery({
    enabled: Boolean(selectedInviteOrganizationId),
    queryKey: ['organization-invites', observatoryUrl, selectedInviteOrganizationId],
    queryFn: () => fetchOrganizationInvites({ apiBaseUrl: observatoryUrl, organizationId: selectedInviteOrganizationId }),
    retry: false
  })
  const invites = invitesQuery.data?.invites ?? []

  const dataPlaneJobsQuery = useQuery({
    enabled: Boolean(activeOrganizationId),
    queryKey: ['organization-data-plane-jobs', observatoryUrl, activeOrganizationId],
    queryFn: () => fetchDataPlaneJobs({ apiBaseUrl: observatoryUrl, organizationId: activeOrganizationId }),
    retry: false
  })
  const dataPlaneJobs = dataPlaneJobsQuery.data?.jobs ?? []

  const createMutation = useMutation({
    mutationFn: () =>
      createOrganization({
        apiBaseUrl: observatoryUrl,
        name: name.trim() || slug.trim(),
        slug
      }),
    onSuccess: created => {
      setSlug('')
      setName('')
      setInviteOrganizationId(created.id)
      setSelectedOrganizationId(created.id)
      setRuntimeSelectedOrganizationId(created.id)
      queryClient.setQueryData<{ organizations: OrganizationRecord[] }>(['organizations', observatoryUrl], current => ({
        organizations: [...(current?.organizations ?? []), created]
      }))
      void queryClient.invalidateQueries()
    }
  })

  const inviteMutation = useMutation({
    mutationFn: () =>
      createOrganizationInvite({
        apiBaseUrl: observatoryUrl,
        email: inviteEmail,
        organizationId: selectedInviteOrganizationId,
        role: inviteRole
      }),
    onSuccess: created => {
      setInviteEmail('')
      queryClient.setQueryData<{ invites: OrganizationInviteRecord[] }>(
        ['organization-invites', observatoryUrl, selectedInviteOrganizationId],
        current => ({ invites: [created, ...(current?.invites ?? [])] })
      )
    }
  })

  const revokeInviteMutation = useMutation({
    mutationFn: (inviteId: number) =>
      revokeOrganizationInvite({
        apiBaseUrl: observatoryUrl,
        inviteId,
        organizationId: selectedInviteOrganizationId
      }),
    onSuccess: updated => {
      queryClient.setQueryData<{ invites: OrganizationInviteRecord[] }>(
        ['organization-invites', observatoryUrl, selectedInviteOrganizationId],
        current => ({ invites: (current?.invites ?? []).map(invite => invite.id === updated.id ? updated : invite) })
      )
    }
  })

  const provisionMutation = useMutation({
    mutationFn: () =>
      provisionDataPlane({
        apiBaseUrl: observatoryUrl,
        clickhouseMode: provisionClickhouseMode,
        clickhouseRegion: provisionRegion,
        organizationId: activeOrganizationId,
        provider: 'aws',
        region: provisionRegion
      }),
    onSuccess: result => {
      queryClient.setQueryData<OrganizationDataPlaneRecord>(
        ['organization-data-plane', observatoryUrl, result.data_plane.organization_id],
        result.data_plane
      )
      queryClient.setQueryData<{ jobs: DataPlaneJobRecord[] }>(
        ['organization-data-plane-jobs', observatoryUrl, result.job.organization_id],
        current => ({ jobs: [result.job, ...(current?.jobs ?? [])] })
      )
      void queryClient.invalidateQueries({ queryKey: ['organization-data-plane', observatoryUrl, result.data_plane.organization_id] })
      void queryClient.invalidateQueries({ queryKey: ['organization-data-plane-jobs', observatoryUrl, result.job.organization_id] })
    }
  })

  const error = organizationsQuery.error || createMutation.error || invitesQuery.error || inviteMutation.error || revokeInviteMutation.error || provisionMutation.error
  const headerStatus = organizationsQuery.error ? 'unavailable' : organizationsQuery.isFetching ? 'loading' : `${organizations.length} organizations`

  return (
    <main className="flex h-full min-h-0 min-w-0 flex-col overflow-hidden bg-black text-[13px] text-neutral-100">
      <header className="flex h-10 shrink-0 items-center gap-2 border-b border-neutral-800 bg-neutral-950 px-3">
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
          <Building2 size={15} strokeWidth={1.8} className="shrink-0 text-neutral-500" />
          <div className="truncate text-[13px] font-medium text-white">Organizations</div>
        </div>
        <div className="ml-auto text-[11px] text-neutral-600">{headerStatus}</div>
      </header>

      <section className="min-h-0 flex-1 overflow-auto bg-black">
        <div className="mx-auto grid w-full max-w-5xl gap-4 px-4 py-4">
          <section className="grid gap-3 border border-neutral-800 bg-neutral-950 p-3">
            <div className="min-w-0">
              <h1 className="truncate text-[13px] font-medium text-white">Create organization</h1>
              <p className="mt-0.5 text-[11px] text-neutral-600">Creates an access boundary. New organizations use the shared data plane until dedicated infrastructure is configured.</p>
            </div>
            <div className="grid gap-2 md:grid-cols-[160px_minmax(180px,1fr)_auto]">
              <label className="grid gap-1 text-[11px] text-neutral-500">
                Slug
                <input
                  className="h-8 min-w-0 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none placeholder:text-neutral-600 focus:border-neutral-600"
                  value={slug}
                  onChange={event => setSlug(normalizeSlug(event.target.value))}
                  placeholder="acme"
                />
              </label>
              <label className="grid gap-1 text-[11px] text-neutral-500">
                Name
                <input
                  className="h-8 min-w-0 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none placeholder:text-neutral-600 focus:border-neutral-600"
                  value={name}
                  onChange={event => setName(event.target.value)}
                  placeholder="Acme"
                />
              </label>
              <div className="flex items-end">
                <button
                  className="inline-flex h-8 w-full items-center justify-center gap-1.5 border border-neutral-700 bg-white px-3 text-[12px] font-medium text-black hover:bg-neutral-200 disabled:border-neutral-900 disabled:bg-black disabled:text-neutral-700 md:w-auto"
                  disabled={!slug.trim() || createMutation.isPending}
                  type="button"
                  onClick={() => createMutation.mutate()}
                >
                  <Plus size={13} strokeWidth={2} />
                  Create
                </button>
              </div>
            </div>
            {error ? <div className="text-[11px] text-red-300">{errorMessage(error)}</div> : null}
          </section>

          <section className="grid gap-3 border border-neutral-800 bg-neutral-950 p-3">
            <div className="flex min-w-0 items-center justify-between gap-2">
              <div className="flex min-w-0 items-center gap-2">
                <Cloud size={15} strokeWidth={1.8} className="shrink-0 text-neutral-500" />
                <div className="min-w-0">
                  <h2 className="truncate text-[13px] font-medium text-white">Data location</h2>
                  <p className="mt-0.5 text-[11px] text-neutral-600">{selectedOrganization ? selectedOrganization.name : 'Select an organization'}</p>
                </div>
              </div>
              <select
                className="h-7 max-w-[240px] border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none focus:border-neutral-600"
                value={activeOrganizationId}
                onChange={event => {
                  setSelectedOrganizationId(event.target.value)
                  setInviteOrganizationId(event.target.value)
                  setRuntimeSelectedOrganizationId(event.target.value)
                }}
              >
                {organizations.map(organization => (
                  <option key={organization.id} value={organization.id}>{organization.name}</option>
                ))}
              </select>
            </div>

            <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
              <DataPoint label="Data plane" value={activeDataPlane ? dataPlaneMode(activeDataPlane) : 'Loading'} />
              <DataPoint label="Region" value={activeDataPlane ? activeDataPlane.region : 'Loading'} />
              <DataPoint label="Ingest endpoint" value={activeDataPlane ? activeDataPlane.ingest_url || 'Not configured' : 'Loading'} mono />
              <DataPoint label="ClickHouse" value={activeDataPlane ? clickhouseSummary(activeDataPlane) : 'Loading'} />
              <DataPoint label="Object storage" value={activeDataPlane ? activeDataPlane.s3_bucket || 'Not configured' : 'Loading'} mono />
              <DataPoint label="Processor prefix" value={activeDataPlane ? activeDataPlane.processor_prefix || 'Not configured' : 'Loading'} mono />
              <DataPoint label="Status" value={activeDataPlane ? dataPlaneStatus(activeDataPlane) : 'Loading'} />
              <DataPoint label="Updated" value={activeDataPlane ? formatDate(activeDataPlane.updated_at) : 'Loading'} />
            </div>

            <div className="grid gap-2 border-t border-neutral-900 pt-3 md:grid-cols-[180px_190px_auto]">
              <label className="grid gap-1 text-[11px] text-neutral-500">
                Region
                <select
                  className="h-8 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none focus:border-neutral-600"
                  value={provisionRegion}
                  onChange={event => setProvisionRegion(event.target.value)}
                >
                  {regions.map(region => (
                    <option key={`${region.provider}:${region.region}`} value={region.region}>
                      {region.region}
                    </option>
                  ))}
                </select>
              </label>
              <label className="grid gap-1 text-[11px] text-neutral-500">
                ClickHouse
                <select
                  className="h-8 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none focus:border-neutral-600"
                  value={provisionClickhouseMode}
                  onChange={event => setProvisionClickhouseMode(event.target.value)}
                >
                  <option value="shared-service">shared service</option>
                  <option value="dedicated-service">dedicated service</option>
                </select>
              </label>
              <div className="flex items-end">
                <button
                  className="inline-flex h-8 w-full items-center justify-center gap-1.5 border border-neutral-800 bg-black px-3 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700 md:w-auto"
                  disabled={!activeOrganizationId || !provisionRegion || regionsQuery.isLoading || provisionMutation.isPending}
                  type="button"
                  onClick={() => provisionMutation.mutate()}
                >
                  <Plus size={13} strokeWidth={2} />
                  Queue dedicated setup
                </button>
              </div>
            </div>

            {dataPlaneJobs.length > 0 ? (
              <div className="border border-neutral-900">
                <table className="w-full min-w-[640px] border-collapse text-left text-[12px]">
                  <thead className="border-b border-neutral-800 text-[11px] uppercase text-neutral-600">
                    <tr>
                      <th className="px-3 py-2 font-medium">Job</th>
                      <th className="px-3 py-2 font-medium">Status</th>
                      <th className="px-3 py-2 font-medium">Region</th>
                      <th className="px-3 py-2 font-medium">ClickHouse</th>
                      <th className="px-3 py-2 font-medium">Created</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dataPlaneJobs.slice(0, 5).map(job => (
                      <tr key={job.id} className="border-b border-neutral-900 last:border-b-0">
                        <td className="px-3 py-2 font-mono text-neutral-500">{job.id}</td>
                        <td className="px-3 py-2 text-neutral-300">{job.error || job.status}</td>
                        <td className="px-3 py-2 text-neutral-500">{job.region}</td>
                        <td className="px-3 py-2 text-neutral-500">{job.clickhouse_mode} / {job.clickhouse_region}</td>
                        <td className="px-3 py-2 text-neutral-500">{formatDate(job.created_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : dataPlaneJobsQuery.isLoading ? (
              <div className="text-[11px] text-neutral-600">Loading data-plane jobs...</div>
            ) : null}
          </section>

          <section className="grid gap-3 border border-neutral-800 bg-neutral-950 p-3">
            <div className="flex min-w-0 items-center justify-between gap-2">
              <div className="min-w-0">
                <h2 className="truncate text-[13px] font-medium text-white">Invite members</h2>
                <p className="mt-0.5 text-[11px] text-neutral-600">Pending invites create org memberships after the recipient signs in and accepts.</p>
              </div>
              <button
                className="inline-flex h-7 items-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                disabled={!selectedInviteOrganizationId || invitesQuery.isFetching}
                type="button"
                onClick={() => void invitesQuery.refetch()}
              >
                <RefreshCcw size={12} strokeWidth={1.8} />
                Refresh
              </button>
            </div>
            <div className="grid gap-2 md:grid-cols-[minmax(180px,1fr)_minmax(220px,1fr)_120px_auto]">
              <label className="grid gap-1 text-[11px] text-neutral-500">
                Organization
                <select
                  className="h-8 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none focus:border-neutral-600"
                  value={selectedInviteOrganizationId}
                  onChange={event => setInviteOrganizationId(event.target.value)}
                >
                  {organizations.map(organization => (
                    <option key={organization.id} value={organization.id}>{organization.name}</option>
                  ))}
                </select>
              </label>
              <label className="grid gap-1 text-[11px] text-neutral-500">
                Email
                <input
                  className="h-8 min-w-0 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none placeholder:text-neutral-600 focus:border-neutral-600"
                  placeholder="user@acme.com"
                  type="email"
                  value={inviteEmail}
                  onChange={event => setInviteEmail(event.target.value)}
                />
              </label>
              <label className="grid gap-1 text-[11px] text-neutral-500">
                Role
                <select
                  className="h-8 border border-neutral-800 bg-black px-2 text-[12px] text-white outline-none focus:border-neutral-600"
                  value={inviteRole}
                  onChange={event => setInviteRole(event.target.value as 'viewer' | 'admin')}
                >
                  <option value="viewer">viewer</option>
                  <option value="admin">admin</option>
                </select>
              </label>
              <div className="flex items-end">
                <button
                  className="inline-flex h-8 w-full items-center justify-center gap-1.5 border border-neutral-700 bg-white px-3 text-[12px] font-medium text-black hover:bg-neutral-200 disabled:border-neutral-900 disabled:bg-black disabled:text-neutral-700 md:w-auto"
                  disabled={!selectedInviteOrganizationId || !inviteEmail.trim() || inviteMutation.isPending}
                  type="button"
                  onClick={() => inviteMutation.mutate()}
                >
                  <UserPlus size={13} strokeWidth={2} />
                  Invite
                </button>
              </div>
            </div>
            <div className="overflow-x-auto border border-neutral-900">
              <table className="w-full min-w-[760px] border-collapse text-left text-[12px]">
                <thead className="border-b border-neutral-800 text-[11px] uppercase text-neutral-600">
                  <tr>
                    <th className="px-3 py-2 font-medium">Email</th>
                    <th className="px-3 py-2 font-medium">Role</th>
                    <th className="px-3 py-2 font-medium">Status</th>
                    <th className="px-3 py-2 font-medium">Expires</th>
                    <th className="px-3 py-2 text-right font-medium">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {invites.map(invite => (
                    <tr key={invite.id} className="border-b border-neutral-900 last:border-b-0">
                      <td className="px-3 py-2 text-white">{invite.email}</td>
                      <td className="px-3 py-2 text-neutral-400">{invite.role}</td>
                      <td className="px-3 py-2 text-neutral-500">{inviteStatus(invite)}</td>
                      <td className="px-3 py-2 text-neutral-500">{formatDate(invite.expires_at)}</td>
                      <td className="px-3 py-2 text-right">
                        <button
                          aria-label={`Revoke invite for ${invite.email}`}
                          className="inline-flex h-7 w-7 items-center justify-center border border-neutral-800 bg-black text-neutral-400 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                          disabled={Boolean(invite.accepted_at || invite.revoked_at) || revokeInviteMutation.isPending}
                          title="Revoke invite"
                          type="button"
                          onClick={() => revokeInviteMutation.mutate(invite.id)}
                        >
                          <Trash2 size={12} strokeWidth={1.8} />
                        </button>
                      </td>
                    </tr>
                  ))}
                  {!invitesQuery.isLoading && invites.length === 0 ? (
                    <tr>
                      <td className="px-3 py-6 text-center text-neutral-600" colSpan={5}>No invites.</td>
                    </tr>
                  ) : null}
                  {invitesQuery.isLoading ? (
                    <tr>
                      <td className="px-3 py-6 text-center text-neutral-600" colSpan={5}>Loading invites...</td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </section>

          <section className="min-h-0 border border-neutral-800 bg-neutral-950">
            <div className="flex items-center justify-between gap-2 border-b border-neutral-800 px-3 py-2">
              <h2 className="text-[13px] font-medium text-white">Organizations</h2>
              <button
                className="inline-flex h-7 items-center gap-1.5 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white disabled:text-neutral-700"
                disabled={organizationsQuery.isFetching}
                type="button"
                onClick={() => void organizationsQuery.refetch()}
              >
                <RefreshCcw size={12} strokeWidth={1.8} />
                Refresh
              </button>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[1060px] border-collapse text-left text-[12px]">
                <thead className="border-b border-neutral-800 text-[11px] uppercase text-neutral-600">
                  <tr>
                    <th className="px-3 py-2 font-medium">Organization</th>
                    <th className="px-3 py-2 font-medium">Slug</th>
                    <th className="px-3 py-2 font-medium">Plan</th>
                    <th className="px-3 py-2 font-medium">Data plane</th>
                    <th className="px-3 py-2 font-medium">Region</th>
                    <th className="px-3 py-2 font-medium">Ingest</th>
                    <th className="px-3 py-2 font-medium">Created</th>
                    <th className="px-3 py-2 text-right font-medium">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {organizations.map(organization => {
                    const dataPlane = dataPlanesByOrganizationId.get(organization.id)
                    return (
                      <tr key={organization.id} className="border-b border-neutral-900 last:border-b-0">
                        <td className="max-w-[260px] px-3 py-2">
                          <div className="truncate text-white">{organization.name}</div>
                          <div className="truncate font-mono text-[11px] text-neutral-600">{organization.id}</div>
                        </td>
                        <td className="px-3 py-2 text-neutral-500">{organization.slug}</td>
                        <td className="px-3 py-2 text-neutral-500">{organization.plan}</td>
                        <td className="px-3 py-2 text-neutral-400">{dataPlane ? dataPlaneMode(dataPlane) : 'Loading'}</td>
                        <td className="px-3 py-2 text-neutral-500">{dataPlane?.region ?? 'Loading'}</td>
                        <td className="max-w-[260px] truncate px-3 py-2 font-mono text-[11px] text-neutral-500">{dataPlane?.ingest_url || 'Loading'}</td>
                        <td className="px-3 py-2 text-neutral-500">{formatDate(organization.created_at)}</td>
                        <td className="px-3 py-2 text-right">
                          <button
                            className="h-7 border border-neutral-800 bg-black px-2 text-[12px] text-neutral-300 hover:bg-white/[0.04] hover:text-white"
                            type="button"
                            onClick={() => {
                              setSelectedOrganizationId(organization.id)
                              setInviteOrganizationId(organization.id)
                              setRuntimeSelectedOrganizationId(organization.id)
                              void queryClient.invalidateQueries()
                            }}
                          >
                            Select
                          </button>
                        </td>
                      </tr>
                    )
                  })}
                  {organizationsQuery.error ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={8}>Organizations unavailable.</td>
                    </tr>
                  ) : null}
                  {!organizationsQuery.isLoading && !organizationsQuery.error && organizations.length === 0 ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={8}>No organizations.</td>
                    </tr>
                  ) : null}
                  {organizationsQuery.isLoading ? (
                    <tr>
                      <td className="px-3 py-8 text-center text-neutral-600" colSpan={8}>Loading organizations...</td>
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

function DataPoint({ label, mono, value }: { label: string; mono?: boolean; value: string }) {
  return (
    <div className="min-w-0 border border-neutral-900 bg-black p-2">
      <div className="text-[10px] uppercase text-neutral-600">{label}</div>
      <div className={['mt-1 truncate text-[12px] text-neutral-200', mono ? 'font-mono text-[11px]' : ''].filter(Boolean).join(' ')}>
        {value}
      </div>
    </div>
  )
}

function dataPlaneMode(dataPlane: OrganizationDataPlaneRecord) {
  return dataPlane.mode === 'shared' ? 'Shared Nanotrace stack' : 'Dedicated data plane'
}

function dataPlaneStatus(dataPlane: OrganizationDataPlaneRecord) {
  if (dataPlane.status_message) return `${dataPlane.status}: ${dataPlane.status_message}`
  return dataPlane.status
}

function clickhouseSummary(dataPlane: OrganizationDataPlaneRecord) {
  const region = dataPlane.clickhouse_region || dataPlane.region
  const database = dataPlane.clickhouse_database || 'observatory'
  return `${dataPlane.clickhouse_mode || 'shared-service'} / ${region} / ${database}`
}

async function fetchOrganizations({ apiBaseUrl }: { apiBaseUrl: string }): Promise<{ organizations: OrganizationRecord[] }> {
  const response = await fetch(organizationsUrl(apiBaseUrl), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  return (await response.json()) as { organizations: OrganizationRecord[] }
}

async function fetchRegions({ apiBaseUrl }: { apiBaseUrl: string }): Promise<{ regions: RegionOption[] }> {
  const response = await fetch(regionsUrl(apiBaseUrl), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  return (await response.json()) as { regions: RegionOption[] }
}

async function fetchOrganizationDataPlane({
  apiBaseUrl,
  organizationId
}: {
  apiBaseUrl: string
  organizationId: string
}): Promise<OrganizationDataPlaneRecord> {
  const response = await fetch(organizationDataPlaneUrl(apiBaseUrl, organizationId), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  const body = (await response.json()) as { data_plane?: OrganizationDataPlaneRecord }
  if (!body.data_plane) throw new HTTPError({ message: 'data-plane response missing data_plane', status: 502 })
  return body.data_plane
}

async function fetchDataPlaneJobs({
  apiBaseUrl,
  organizationId
}: {
  apiBaseUrl: string
  organizationId: string
}): Promise<{ jobs: DataPlaneJobRecord[] }> {
  const response = await fetch(`${organizationDataPlaneUrl(apiBaseUrl, organizationId)}/jobs`, {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  return (await response.json()) as { jobs: DataPlaneJobRecord[] }
}

async function provisionDataPlane({
  apiBaseUrl,
  clickhouseMode,
  clickhouseRegion,
  organizationId,
  provider,
  region
}: {
  apiBaseUrl: string
  clickhouseMode: string
  clickhouseRegion: string
  organizationId: string
  provider: string
  region: string
}): Promise<{ data_plane: OrganizationDataPlaneRecord; job: DataPlaneJobRecord }> {
  const response = await fetch(`${organizationDataPlaneUrl(apiBaseUrl, organizationId)}/provision`, {
    body: JSON.stringify({
      clickhouse_mode: clickhouseMode,
      clickhouse_region: clickhouseRegion,
      provider,
      region
    }),
    credentials: 'include',
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  return (await response.json()) as { data_plane: OrganizationDataPlaneRecord; job: DataPlaneJobRecord }
}

async function createOrganization({
  apiBaseUrl,
  name,
  slug
}: {
  apiBaseUrl: string
  name: string
  slug: string
}): Promise<OrganizationRecord> {
  const response = await fetch(organizationsUrl(apiBaseUrl), {
    body: JSON.stringify({
      name: name.trim(),
      slug: slug.trim()
    }),
    credentials: 'include',
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  const body = (await response.json()) as { organization?: OrganizationRecord }
  if (!body.organization) throw new HTTPError({ message: 'organization response missing organization', status: 502 })
  return body.organization
}

async function fetchOrganizationInvites({
  apiBaseUrl,
  organizationId
}: {
  apiBaseUrl: string
  organizationId: string
}): Promise<{ invites: OrganizationInviteRecord[] }> {
  const response = await fetch(organizationInvitesUrl(apiBaseUrl, organizationId), {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'GET'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  return (await response.json()) as { invites: OrganizationInviteRecord[] }
}

async function createOrganizationInvite({
  apiBaseUrl,
  email,
  organizationId,
  role
}: {
  apiBaseUrl: string
  email: string
  organizationId: string
  role: 'viewer' | 'admin'
}): Promise<OrganizationInviteRecord> {
  const response = await fetch(organizationInvitesUrl(apiBaseUrl, organizationId), {
    body: JSON.stringify({ email: email.trim(), role }),
    credentials: 'include',
    headers: queryHeaders(),
    method: 'POST'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  const body = (await response.json()) as { invite?: OrganizationInviteRecord }
  if (!body.invite) throw new HTTPError({ message: 'invite response missing invite', status: 502 })
  return body.invite
}

async function revokeOrganizationInvite({
  apiBaseUrl,
  inviteId,
  organizationId
}: {
  apiBaseUrl: string
  inviteId: number
  organizationId: string
}): Promise<OrganizationInviteRecord> {
  const response = await fetch(`${organizationInvitesUrl(apiBaseUrl, organizationId)}/${inviteId}`, {
    credentials: 'include',
    headers: queryHeaders(),
    method: 'DELETE'
  })
  if (!response.ok) {
    const text = await response.text()
    throw new HTTPError({ message: text || response.statusText, status: response.status })
  }
  const body = (await response.json()) as { invite?: OrganizationInviteRecord }
  if (!body.invite) throw new HTTPError({ message: 'invite response missing invite', status: 502 })
  return body.invite
}

function organizationsUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/organizations` : '/organizations'
}

function regionsUrl(apiBaseUrl: string) {
  const base = apiBaseUrl.trim().replace(/\/+$/, '')
  return base ? `${base}/regions` : '/regions'
}

function organizationDataPlaneUrl(apiBaseUrl: string, organizationId: string) {
  return `${organizationsUrl(apiBaseUrl)}/${encodeURIComponent(organizationId)}/data-plane`
}

function organizationInvitesUrl(apiBaseUrl: string, organizationId: string) {
  return `${organizationsUrl(apiBaseUrl)}/${encodeURIComponent(organizationId)}/invites`
}

function normalizeSlug(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-+/g, '')
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

function inviteStatus(invite: OrganizationInviteRecord) {
  if (invite.accepted_at) return 'accepted'
  if (invite.revoked_at) return 'revoked'
  if (new Date(invite.expires_at).getTime() < Date.now()) return 'expired'
  return 'pending'
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : error ? String(error) : ''
}
