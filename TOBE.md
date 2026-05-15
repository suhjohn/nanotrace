# To-Be State

Nanotrace becomes a shared control-plane plus isolated per-organization data-plane
system. The organization is no longer just a row-filter boundary; it is the
compute, storage, IAM, KMS, and endpoint boundary.

## System Shape

```text
Nanotrace control plane
  -> global user login
  -> organization provisioning
  -> billing/admin
  -> organization registry
  -> data-plane lifecycle
  -> optional global UI

Organization data plane
  -> public org endpoint
  -> org-local API keys
  -> org-local metadata DB
  -> org-local ingest/query/loader compute
  -> org-local S3/SQS/ClickHouse/processors
  -> org-local IAM/KMS boundary
```

The control plane provisions and discovers data planes. Tenant operational
traffic goes directly to the tenant data plane.

## Public Endpoints

Each organization gets its own public endpoint:

```text
https://org-123.data.nanotrace.dev
```

Supported directly on that endpoint:

```text
POST   /events
POST   /query
GET    /events/{event_id}
GET    /api-keys
POST   /api-keys
DELETE /api-keys/{id}
GET/PUT/DELETE /processors/*
GET/PUT/DELETE /facets/*
GET/POST/PUT/DELETE /dashboards/*
```

The shared control plane can still provide a global UI, but tenant operational
API calls are served by that tenant's data plane.

## Per-Org Deployment

For each organization:

```text
org-123 ALB / public endpoint
  -> ingest/server target group
       -> nanotrace-server x N
       -> optional colocated nanotrace-loader x N

  -> query target group
       -> nanotrace-query x N
```

Backing services:

```text
org-123 Postgres
  -> API keys
  -> dashboards
  -> facets
  -> processor metadata
  -> small org-local config

org-123 S3 bucket
  -> raw event parts
  -> processor source/artifacts
  -> debug/bootstrap logs

org-123 SQS queue
  -> S3 object-created notifications for loader

org-123 ClickHouse
  -> events
  -> facets
  -> event index
  -> hot dimensions

org-123 KMS key
  -> S3 SSE-KMS
  -> SQS encryption
  -> EBS encryption
  -> Postgres/storage encryption where supported

org-123 IAM role
  -> can access only org-123 resources
```

Every org is assigned to a ClickHouse service allocation and an org database.
By default, small tenants use the platform's default shared ClickHouse Cloud
service with separate org databases/users. Enterprise tenants can be assigned
to a dedicated ClickHouse Cloud service with the same runtime contract.

## POST /events Flow

```text
client
  -> POST https://org-123.data.nanotrace.dev/events
  -> org-123 ALB
  -> org-123 nanotrace-server
  -> validate org-local API key from in-memory cache
  -> require ingest:write
  -> stamp tenant_id = org-123
  -> stamp organization_id = org-123
  -> append NDJSON to local encrypted /data spool
  -> return write receipt
```

Async continuation:

```text
uploader on org-123 server node
  -> closes local part
  -> runs org-123 upload processors, if enabled
  -> uploads part to org-123 S3 with SSE-KMS
  -> S3 sends notification to org-123 SQS

org-123 loader
  -> consumes SQS
  -> reads org-123 S3 object
  -> runs org-123 loader processors
  -> bulk inserts into org-123 ClickHouse
```

The synchronous ingest response only means the event has been durably appended
to the local spool. S3 upload, SQS delivery, loader processing, and ClickHouse
insert remain asynchronous.

## Query Flow

```text
client
  -> POST https://org-123.data.nanotrace.dev/query
  -> org-123 ALB query route
  -> org-123 nanotrace-query
  -> validate org-local API key from in-memory cache
  -> require query:read
  -> query org-123 ClickHouse
```

Event detail:

```text
GET /events/{event_id}
  -> query ClickHouse for source_file/source_offset/source_length
  -> S3 Range GET from org-123 bucket
  -> return raw event JSON
```

Because S3 encryption is SSE-KMS, offset/range reads continue to work unchanged.
The offsets are still plaintext object offsets as seen through the S3 API.

## API Key Model

Org-local Postgres stores only small metadata:

```text
api_keys
  id
  prefix
  key_hash
  name
  scopes
  role
  expires_at
  revoked_at
  created_at
  updated_at
```

Each horizontally scaled node does:

```text
startup: load all active keys
refresh: poll or LISTEN/NOTIFY every few seconds
request: validate from in-memory map
```

No Postgres call happens on ingest/query request hot paths. With roughly tens of
keys per org, this remains trivial even with many horizontally scaled nodes.
Revocation latency is the cache refresh interval; use a short interval plus a
periodic full reload.

Store only key hashes. The plaintext key is returned once at creation time and
is never persisted.

## Encryption

Use infrastructure encryption first:

```text
Public TLS:
  client -> org endpoint

Internal TLS:
  node -> S3/SQS/KMS/Postgres/ClickHouse

S3:
  org bucket encrypted with org KMS key using SSE-KMS

SQS:
  org queue encrypted with org KMS key

EBS/local spool:
  org compute volumes encrypted with org KMS key

Postgres:
  encrypted storage, preferably org KMS key if supported

ClickHouse:
  default shared ClickHouse Cloud service for small tenants
  dedicated ClickHouse Cloud service for enterprise tenants
  org-specific database and credentials in either case

Processor artifacts:
  stored in org S3 bucket with SSE-KMS
```

Do not put KMS on the per-event hot path. S3/SQS/EBS/storage layers use KMS
underneath; application code does not call KMS per event.

Application-level envelope encryption is not part of the initial design because
whole-object app encryption breaks efficient `source_offset`/`source_length`
range reads unless Nanotrace also adds chunked encryption and a plaintext-to-
ciphertext chunk index.

## Processor Boundary

Processors remain a code execution risk. Per-org compute and storage reduce the
blast radius to one organization, but a malicious processor can still affect its
own org's data-plane process if it is loaded as native Rust code.

The per-org deployment must therefore use org-scoped processor resources:

```text
PROCESSOR_S3_BUCKET=org-123 bucket
PROCESSOR_PREFIX=organizations/org-123/processors
```

For stronger safety, move processor execution to a sandboxed boundary such as
WASM/WASI or isolated worker processes with CPU, memory, filesystem, network,
and timeout controls.

## Provisioning Lifecycle

Organization creation should provision or assign:

```text
public hostname
ALB/target groups
server/query/loader compute
Postgres metadata store
S3 bucket
SQS queue
ClickHouse target
KMS key
IAM role/policies
processor prefix
DNS/TLS certificate
```

The control plane stores the data-plane status and endpoint metadata needed for
admin UX and lifecycle operations, but it is not in the ingest/query hot path.

## Isolation Boundary

The hard boundary is per organization:

```text
compute: separate ASGs/ECS services per org
storage: separate S3/SQS/Postgres/ClickHouse per org
crypto: separate KMS key per org
IAM: separate role per org
processors: separate runtime/artifacts per org
endpoint: separate public hostname per org
```

`tenant_id == organization_id` remains in data as defense-in-depth, but tenant
isolation no longer relies primarily on ClickHouse filters or shared process
correctness.
