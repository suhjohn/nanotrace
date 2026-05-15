# Data-Plane Provisioning

Nanotrace deploys a control plane plus data-plane capacity. Small/free tenants
can share a default ClickHouse Cloud service while still getting their own
ClickHouse database and credentials. Larger tenants can be moved to a dedicated
ClickHouse Cloud service without changing runtime code.

## Public Contract

Customers and SDKs talk to the public gateway domain:

```text
POST /events
POST /query
GET  /events/{event_id}
```

The gateway validates a session or organization API key, resolves the
organization, and forwards internally:

```text
POST /internal/events
POST /internal/query
GET  /internal/events/{event_id}
```

Forwarded requests carry:

```text
x-nanotrace-internal-organization-id: org_xxx
x-nanotrace-internal-secret: <shared secret>
```

Dedicated org data planes can also expose the same public API directly and
validate org-local API keys.

## Runtime Configuration

Configure the control-plane/gateway service with the shared data-plane URLs:

```bash
NANOTRACE_SHARED_DATA_PLANE_INGEST_URL=https://data.nanotrace.dev
NANOTRACE_SHARED_DATA_PLANE_QUERY_URL=https://query.nanotrace.dev
NANOTRACE_SHARED_DATA_PLANE_SECRET=...
```

Configure data-plane services with the same secret:

```bash
NANOTRACE_DATA_PLANE_SHARED_SECRET=...
```

Leave `NANOTRACE_DATA_PLANE_ORGANIZATION_ID` unset for the shared cluster. The
organization id comes from the trusted internal request headers.

## Async Provisioner

Dedicated organization data planes are not created in request handlers.
`POST /organizations/{organization_id}/data-plane/provision` only records a
queued provisioning job and marks the organization data-plane state as queued.

Run the provisioner worker outside the API process:

```bash
NANOTRACE_PROVISIONER_API_BASE_URL=https://api.nanotrace.example.com \
NANOTRACE_PROVISIONER_API_KEY=ntak_... \
npm run provision:data-plane -- --poll
```

The worker claims queued jobs from
`POST /data-plane/provisioning/jobs/claim`, deploys the Pulumi stack with
`NANOTRACE_DATA_PLANE_ORGANIZATION_ID` set to the target organization, then
calls `POST /data-plane/provisioning/jobs/{job_id}/complete` with the data-plane
URLs, bucket, ClickHouse placement, and status. Once a dedicated record is
active, the gateway forwards ingest and query traffic for that organization to
the dedicated data plane; otherwise it uses the shared data-plane URLs.

The provisioner must run with the credentials for the placement it is allowed
to create. For `shared-service` jobs, inject the shared ClickHouse
`CLICKHOUSE_URL`, `CLICKHOUSE_USER`, and `CLICKHOUSE_PASSWORD`. For
`dedicated-service` jobs, inject the ClickHouse Cloud API credentials.
Dedicated org stacks default `CLICKHOUSE_DATABASE` to a normalized organization
database name such as `org_acme`; override with
`NANOTRACE_PROVISIONER_CLICKHOUSE_DATABASE` only for one-off operator work.

## ClickHouse Allocation

Runtime always receives the same four values:

```bash
CLICKHOUSE_URL=...
CLICKHOUSE_USER=...
CLICKHOUSE_PASSWORD=...
CLICKHOUSE_DATABASE=org_xxx
```

The deployment can produce those values in three ways.

### Shared ClickHouse Service

With `NANOTRACE_CLICKHOUSE_MODE=shared-service`, the shared/main data plane
creates or uses the platform shared ClickHouse Cloud service:

```bash
NANOTRACE_CLICKHOUSE_MODE=shared-service
CLICKHOUSE_CLOUD_API_KEY=...
CLICKHOUSE_CLOUD_API_SECRET=...
CLICKHOUSE_CLOUD_ORG_ID=...
CLICKHOUSE_CLOUD_PROVIDER=aws
CLICKHOUSE_CLOUD_REGION=us-west-2
```

`CLICKHOUSE_CLOUD_API_KEY_ID` and `CLICKHOUSE_CLOUD_API_KEY_SECRET` are also
accepted as aliases for the API key and secret.

Pulumi creates the service, generates the default-user password, derives the
HTTPS endpoint, applies the Nanotrace schema, and injects the resulting
`CLICKHOUSE_*` env vars into the server/query/loader containers.

For a dedicated org data plane using `shared-service`, the provisioner injects
the existing shared-service `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, and
`CLICKHOUSE_PASSWORD`; Pulumi does not create another ClickHouse service.

Optional defaults:

```bash
NANOTRACE_DEFAULT_CLICKHOUSE_SERVICE_NAME=nanotrace-prod-default
NANOTRACE_DEFAULT_CLICKHOUSE_IDLE_SCALING=true
NANOTRACE_DEFAULT_CLICKHOUSE_IDLE_TIMEOUT_MINUTES=15
NANOTRACE_DEFAULT_CLICKHOUSE_MIN_TOTAL_MEMORY_GB=24  # production tier only
NANOTRACE_DEFAULT_CLICKHOUSE_MAX_TOTAL_MEMORY_GB=24  # production tier only
NANOTRACE_DEFAULT_CLICKHOUSE_NUM_REPLICAS=3           # production tier only
NANOTRACE_DEFAULT_CLICKHOUSE_IP_ACCESS=0.0.0.0/0
```

`NANOTRACE_DEFAULT_CLICKHOUSE_TIER` is intentionally omitted by default because
ClickHouse Cloud organizations on newer pricing plans reject explicit `tier`
values during service creation. Set it only for legacy orgs that still require
`development` or `production`.

### Dedicated ClickHouse Service

With `NANOTRACE_CLICKHOUSE_MODE=dedicated-service`, Pulumi creates a ClickHouse
Cloud service for that stack, applies the Nanotrace schema, and injects the
resulting `CLICKHOUSE_*` env vars into the server/query/loader containers:

```bash
NANOTRACE_CLICKHOUSE_MODE=dedicated-service
CLICKHOUSE_CLOUD_API_KEY=...
CLICKHOUSE_CLOUD_API_SECRET=...
CLICKHOUSE_CLOUD_ORG_ID=...
CLICKHOUSE_CLOUD_PROVIDER=aws
CLICKHOUSE_CLOUD_REGION=us-west-2
```

### External ClickHouse Service

With `NANOTRACE_CLICKHOUSE_MODE=external`, Pulumi does not create ClickHouse
Cloud infrastructure:

```bash
NANOTRACE_CLICKHOUSE_MODE=external
CLICKHOUSE_URL=https://...
CLICKHOUSE_USER=...
CLICKHOUSE_PASSWORD=...
CLICKHOUSE_DATABASE=observatory
```

Pulumi still applies the Nanotrace schema to the configured database.

## Tenant Isolation

Every event is stamped with:

```text
organization_id = org_xxx
tenant_id       = org_xxx
```

Reads and event detail lookups must include tenant filters before querying
ClickHouse. Object-storage prefixes include the organization id, and
control-plane metadata tables scope by `organization_id`.

In the target model, each organization is assigned to a ClickHouse service
allocation plus an organization database:

```text
small org      -> default shared ClickHouse service -> org_small.events
enterprise org -> dedicated ClickHouse service      -> org_enterprise.events
```

Shared and dedicated services use the same runtime env contract. Placement is a
provisioning decision, not a runtime mode.

## Domains

SaaS can use:

```text
https://api.nanotrace.dev
https://app.nanotrace.dev
```

Self-hosted installations can use any domain:

```text
https://trace.example.com
```

Customers should configure SDKs with the stable gateway URL, not internal
data-plane URLs.
