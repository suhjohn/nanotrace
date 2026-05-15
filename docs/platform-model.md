# Nanotrace Platform Model

Nanotrace should evolve toward a separate control plane and data plane. An
organization is the tenant boundary. The production data plane is a shared
multi-tenant cluster.

There is no customer-facing "project" layer in the target model. If a project
table or project id exists in code, treat it as a compatibility artifact until it
is removed or folded into organization-owned metadata.

## End State

```text
Nanotrace hoster
  -> runs the control plane
  -> owns provisioning credentials
  -> owns public gateway domains
  -> optionally runs shared data planes

Organization
  -> one tenant
  -> one API-key namespace
  -> one telemetry namespace
  -> one dashboard/config namespace

Data plane
  -> one shared multi-tenant cluster
  -> ingest
  -> transformation/loader/processors
  -> S3/SQS/raw parts
  -> shared multi-tenant ClickHouse cluster
  -> query/event-read service
```

The core invariant:

```text
organization_id is the isolation key
tenant_id == organization_id
```

Telemetry rows should be stamped with `organization_id` and `tenant_id`. Query,
event detail, facets, processors, dashboard visualizations, API keys, and
provisioning records must scope by organization.

## Roles

### Hoster

The hoster is the operator of a Nanotrace installation. In SaaS this is
Nanotrace. In self-host this is the customer/operator running Nanotrace.

The hoster owns:

- Control-plane deployment.
- Postgres for auth, organizations, memberships, API keys, invites, and UI
  state.
- Provisioning credentials for AWS or another infrastructure provider.
- Domain configuration.
- Shared data-plane lifecycle.

The hoster should not have to manually wire tenant routing after an organization
is created. The control plane should provision or assign the data plane and
record its endpoints.

### User

The user belongs to one or more organizations. The user:

- Logs in to the control plane.
- Selects an organization.
- Creates organization-scoped API keys.
- Sends telemetry using those keys.
- Views traces, dashboards, facets, processors, and settings for that
  organization.

Users join organizations through membership rows or invites. Membership is
org-level. Project-level membership should not exist in the target model.

## Services

### Control Plane

Responsibilities:

- Login/session/auth.
- Organization CRUD and membership/invites.
- Organization-scoped API key management.
- Shared data-plane routing configuration.
- Dashboard visualization persistence.
- Facet and processor configuration metadata.
- UI static serving.

The control plane should not ingest customer telemetry directly for dedicated
tenants. It can keep a shared/all-in-one mode for local development and simple
self-host installs.

### Gateway

Responsibilities:

- Public `POST /events`.
- Public `POST /query`.
- Public `GET /events/{event_id}`.
- Validate user sessions or organization API keys.
- Resolve the organization.
- Forward to the shared data plane over internal-authenticated endpoints.

The gateway presents stable public APIs. Data planes can move without changing
customer SDK configuration.

### Data Plane

Responsibilities:

- Internal `POST /internal/events`.
- Internal `POST /internal/query`.
- Internal `GET /internal/events/{event_id}`.
- Durable local event writes.
- Raw part upload to object storage.
- Loader/transformation into ClickHouse.
- Processor execution/sync.
- Reads from shared ClickHouse with mandatory tenant filters.
- Uses organization-scoped S3/object prefixes.

A shared data plane trusts only gateway internal auth:

```text
x-nanotrace-internal-organization-id: org_xxx
x-nanotrace-internal-secret: <shared secret>
```

It should not validate customer API keys directly. Customer API keys are
control-plane/gateway concerns.

## Data Model

Target core tables:

```text
nanotrace_organizations
  id
  slug
  name
  plan
  created_at
  updated_at

nanotrace_organization_members
  organization_id
  subject
  role
  created_at

nanotrace_organization_invites
  organization_id
  email
  role
  token_hash
  expires_at
  accepted_at

nanotrace_api_keys
  organization_id
  key_hash
  prefix
  name
  role
  scopes
  expires_at
  revoked_at

```

There should be no customer-facing project table in the clean target schema.
There should also be no `nanotrace_organization_data_planes` table in the shared
cluster target. The deployed Nanotrace installation has one configured data
plane, and every organization is a tenant inside it.

## Provisioning Flow

Organization creation:

```text
user/admin creates organization
  -> control plane inserts organization
  -> control plane inserts org membership
  -> gateway forwards telemetry/query traffic to the shared data-plane cluster
```

Shared data-plane provisioning is hoster-level, not per organization:

```text
Create or scale shared data-plane cluster
  -> configure control plane with NANOTRACE_SHARED_DATA_PLANE_* URLs/secret
  -> configure data-plane services with NANOTRACE_DATA_PLANE_SHARED_SECRET
  -> leave NANOTRACE_DATA_PLANE_ORGANIZATION_ID unset for multi-tenant mode
  -> every forwarded request carries x-nanotrace-internal-organization-id
```

Per-organization data-plane provisioning is intentionally out of the clean
target model. If that returns later, it should be added as a separate enterprise
feature instead of leaking routing state into the default organization schema.

Production analytics uses a hoster-managed shared ClickHouse cluster and shared
ingest/query/loader capacity. This is closer to Grafana Cloud's shared
Loki/Tempo/Mimir cells than database-per-tenant infrastructure. Tenant isolation
is enforced by:

```text
ingest stamps tenant_id = organization_id
query APIs inject tenant_id filters
event detail lookup filters by tenant_id and event_id
facet and dashboard metadata scope by organization_id
processor artifacts scope by organization_id
object storage prefixes include organization_id
```

## Domains

Domains must be configuration, not product assumptions.

SaaS default:

```text
https://api.nanotrace.dev/events
https://api.nanotrace.dev/query
https://app.nanotrace.dev
```

Self-host example:

```text
https://trace.example.com/events
https://trace.example.com/query
https://trace.example.com
```

Per-organization vanity domains can be added later, but the first-class model is
a stable gateway domain plus organization-scoped API keys. The API key selects
the organization for ingest. Logged-in UI requests send:

```text
x-nanotrace-organization-id: org_xxx
```

Requests select organizations with `x-nanotrace-organization-id`.

## Login Email DNS

The control plane sends magic-link login email. When the hoster provides a
domain, the deploy derives a sender unless explicitly configured:

```text
NANOTRACE_DOMAIN_NAME=trace.example.com
NANOTRACE_EMAIL_FROM=login@mail.trace.example.com
```

`NANOTRACE_EMAIL_FROM` is optional. If omitted, the default is:

```text
login@mail.<NANOTRACE_DOMAIN_NAME>
```

Pulumi provisions an SES domain identity for that sender domain and, when DNS is
managed through Cloudflare or Route53, creates:

```text
DKIM CNAME records for mail.trace.example.com
MX record for bounce.mail.trace.example.com
TXT SPF record for bounce.mail.trace.example.com
TXT DMARC record for _dmarc.mail.trace.example.com
```

Cloudflare records for SES must be DNS-only, not proxied. The app/API records
may use the deployment's chosen edge mode, but SES verification records are pure
DNS.

For hosters that use Cloudflare or Route53, Pulumi can create these records
directly. For any other DNS provider, set:

```text
NANOTRACE_DNS_PROVIDER=external
NANOTRACE_EDGE_TLS_MODE=edge-flexible
```

In external DNS mode, Pulumi does not call a DNS provider. It exports
`manualDnsRecordsOutput`, a list of records the hoster must create in their DNS
system. The public app/API records point at the ALB DNS name; apex domains need
the provider's CNAME-flattening equivalent such as ALIAS or ANAME. SES records
must stay DNS-only and must not be proxied.

The only normal knobs are:

```text
NANOTRACE_DOMAIN_NAME
NANOTRACE_EMAIL_FROM              optional override
NANOTRACE_MANAGE_LOGIN_EMAIL_DNS  optional false to print/manage manually later
```

## API Surface

Control plane:

```text
GET  /auth/me
POST /auth/login
GET  /organizations
POST /organizations
PUT  /organizations/{organization_id}/data-plane
GET  /api-keys
POST /api-keys
DELETE /api-keys/{id}
```

Gateway/public data API:

```text
POST /events
POST /query
GET  /events/{event_id}
GET  /facets
PUT  /facets/{path}
GET  /processors
PUT  /processors/{name}
GET/POST/PUT/DELETE /dashboards/{dashboard_id}/visualizations
```

Data-plane internal API:

```text
POST /internal/events
POST /internal/query
GET  /internal/events/{event_id}
```

## Current Migration Notes

The clean implementation direction is:

- UI uses Organizations.
- New session selection uses `x-nanotrace-organization-id`.
- API keys resolve to one organization.
- Ingest stamps `tenant_id` and `organization_id` from the organization.
- Gateway forwards dedicated org traffic to internal data-plane endpoints.
- Dedicated data-plane processes expose internal data routes only.

No customer-facing project routes or project-owned persisted metadata should be reintroduced.
