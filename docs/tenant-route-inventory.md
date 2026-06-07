# Tenant Route Inventory

This inventory documents the tenant source and authorization boundary for public backend routes. Tenant-scoped data access must use the authenticated identity's `organization_id` / `tenant_id`; request payload tenant fields are not authoritative.

Organization slugs are mutable through `PATCH /v1/organizations/{organization_id}` but remain globally unique. `DELETE /v1/organizations/{organization_id}` is an explicit archive operation: archived organizations no longer resolve as active session memberships or API-key identities.

| Route | Auth Method | Required Role/Scope | Tenant Source | Backing Store | Coverage |
| --- | --- | --- | --- | --- | --- |
| `POST /v1/events` | API key or session | `ingest:write` | `AuthIdentity.organization_id` stamped by server | Kafka raw ingest | `tests/integration/kafka-e2e.mjs` tenant stamping/spoofing |
| `GET /v1/events/{event_id}` | API key or session | `query:read` | `AuthIdentity.organization_id` | ClickHouse read store | Kafka integration event isolation |
| `POST /v1/query` | API key or session | `query:read` | `AuthIdentity.organization_id` injected into planners | `apps/read` ClickHouse planners | Rust query scoping tests and Kafka session switch isolation |
| `GET /v1/query/recommendations` | API key or session | `query:read` | `AuthIdentity.organization_id` | ClickHouse recommendation query | Kafka tenant isolation variants |
| `GET /v1/definitions` | API key or session | `query:read` | `AuthIdentity.organization_id` | ClickHouse definitions | Kafka definitions isolation |
| `POST /v1/definitions` | API key or session | admin plus `definitions:write` | `AuthIdentity.organization_id` | ClickHouse definitions/backfill path | Rust definition tests and Kafka backfill coverage |
| `GET /v1/definitions/{definition_id}` | API key or session | `query:read` | `AuthIdentity.organization_id` | ClickHouse definitions | Server/OpenAPI tests |
| `DELETE /v1/definitions/{definition_id}` | API key or session | admin plus `definitions:write` | `AuthIdentity.organization_id` | ClickHouse definitions | Server definition tests |
| `POST /v1/definitions/{definition_id}/backfill` | API key or session | admin plus `definitions:write` | `AuthIdentity.organization_id` | ClickHouse synchronous backfill | Rust definition backfill tests |
| `POST /v1/definitions/{definition_id}/backfills` | API key or session | admin plus `definitions:write` | `AuthIdentity.organization_id` | Postgres/ClickHouse materialization jobs | Rust materialization tests |
| `GET /v1/backfills` | API key or session | `query:read` | `AuthIdentity.organization_id` | Postgres materialization jobs | Server/OpenAPI tests |
| `GET /v1/backfills/{job_id}` | API key or session | `query:read` | `AuthIdentity.organization_id` | Postgres materialization jobs | Server/OpenAPI tests |
| `GET /v1/api-keys` | API key or session | admin plus `api_keys:write` | `AuthIdentity.organization_id` | Postgres API keys | Kafka active-org switch isolation |
| `POST /v1/api-keys` | API key or session | admin plus `api_keys:write` | `AuthIdentity.organization_id` | Postgres API keys and audit table | Auth tests and audit persistence test |
| `DELETE /v1/api-keys/{id}` | API key or session | admin | `AuthIdentity.organization_id` | Postgres API keys and audit table | Auth tests and audit persistence test |
| `GET /auth/providers` | public | none | none | server config | Server/OpenAPI tests |
| `GET /auth/google` | public | Google OAuth configured | none; one-time state stored before redirect | Postgres OAuth states | Auth OAuth state test |
| `GET /auth/google/callback` | public callback | valid Google code/state and verified email | resolved user session after OAuth verification | Postgres users/sessions/orgs/OAuth states | Auth external-login test |
| `GET /v1/organizations` | API key or session | authenticated | API key owning org, or session memberships | Postgres organizations/memberships | Kafka session switch isolation |
| `POST /v1/organizations` | session only | authenticated session | created org becomes active session org | Postgres organizations/memberships/sessions/audit | Auth first-login and create/switch tests |
| `PATCH /v1/organizations/{organization_id}` | session only | target org admin | target org membership check | Postgres organizations/audit | Auth update/archive tests |
| `DELETE /v1/organizations/{organization_id}` | session only | target org admin | target org membership check | Postgres organizations/API keys/sessions/audit | Auth archive tests |
| `POST /v1/organizations/{organization_id}/switch` | session only | target org member | target org membership check | Postgres sessions | Kafka session switch isolation |
| `POST /v1/organizations/{organization_id}/leave` | session only | target org member; last-admin protected | session subject and target org | Postgres memberships/sessions/audit | Auth last-admin tests |
| `GET /v1/organizations/{organization_id}/members` | session only | target org admin | target org membership check | Postgres members | Kafka API-key forbidden check |
| `PATCH /v1/organizations/{organization_id}/members/{subject}` | session only | target org admin; last-admin protected | target org membership check | Postgres members/audit | Auth last-admin tests |
| `DELETE /v1/organizations/{organization_id}/members/{subject}` | session only | target org admin; last-admin protected | target org membership check | Postgres members/sessions/audit | Auth last-admin tests |
| `GET /v1/organizations/{organization_id}/invitations` | session only | target org admin | target org membership check | Postgres invitations | Kafka API-key forbidden check |
| `POST /v1/organizations/{organization_id}/invitations` | session only | target org admin | target org membership check | Postgres invitations/audit | Auth invite lifecycle tests |
| `DELETE /v1/organizations/{organization_id}/invitations/{invitation_id}` | session only | target org admin | target org membership check | Postgres invitations/audit | Auth revoked invite test |
| `POST /v1/organizations/{organization_id}/invitations/{invitation_id}/resend` | session only | target org admin | target org membership check | Postgres invitations/audit | Auth resend test |
| `POST /v1/organization-invitations/accept` | session only | matching signed-in email | invite organization after token/email validation | Postgres invitations/members/sessions/audit | Auth accept-once/mismatch tests |

Internal workers and maintenance tools may operate across tenants only outside the public API surface. Those paths must document their all-tenant mode and must preserve tenant IDs in derived rows.
