# Multi-Tenant Backend Hardening Plan

## Goal

Make Nanotrace's organization-as-tenant model production-grade at the API, persistence, authorization, and operations layers. The current implementation has tenant-scoped ingest/read behavior, active browser-session organizations, org creation, switching, invitations, and member management. This plan covers the remaining backend work needed to make those guarantees durable and harder to regress.

## Final Form

Nanotrace should have one tenant model: organizations. There should be no legacy default tenant behavior, no automatic `org_default` fallback, and no bootstrap organization magic in production code paths.

Target state:

- Every user belongs to one or more real organizations through `nanotrace_organization_members`.
- Every browser session has a valid `active_organization_id` or is in a short-lived onboarding state before data access.
- Every API key belongs to exactly one real organization.
- Every tenant-scoped data read/write uses `identity.organization_id` / `identity.tenant_id`, derived from session membership or API-key ownership.
- Login never adds users to `org_default`.
- Session validation never falls back to `org_default`.
- Server startup never creates `org_default`.
- Bootstrap API key creation is removed from the app server. If local/dev needs seed data, it should be created by an explicit seed script or migration fixture.
- `AuthStore::ensure_schema` is not a schema authoring mechanism. Schema is owned by versioned SQL migrations.
- Legacy data migration is a one-time operation, not permanent runtime behavior.

## Principles

- Every public data route must derive tenant scope from the authenticated identity, never from client input.
- Account lifecycle mutations should be atomic where user-visible state spans multiple tables.
- Session auth and API-key auth should have explicit, separate capabilities.
- Postgres remains the source of truth for users, sessions, organizations, memberships, invitations, and API keys.
- ClickHouse remains tenant-keyed by `tenant_id`; no serving-table tenant-model changes are planned.
- No runtime code should preserve or depend on `org_default`.
- Local/dev seed data should use explicit organization IDs created by setup scripts, not hidden fallback behavior.

## Phase 1: Versioned SQL Migrations

Add a formal migration system for Postgres auth/control-plane schema.

Tasks:

- Choose migration runner integration:
  - Prefer `sqlx::migrate!` if the server can own auth/control-plane migrations at startup.
  - Alternatively add a small `nanotrace-db-migrate` binary if deploys should run migrations separately.
- Add a migrations directory, for example `crates/auth/migrations` or repo-level `migrations/postgres`.
- Move schema currently embedded in `AuthStore::ensure_schema` into ordered SQL migration files.
- Replace `AuthStore::ensure_schema` with a migration runner call or remove it entirely.
- Remove inline `CREATE TABLE`, `ALTER TABLE`, default org seeding, and bootstrap API-key seeding from normal server startup.
- Add migrations for:
  - `nanotrace_organizations`
  - `nanotrace_auth_users`
  - `nanotrace_organization_members`
  - `nanotrace_auth_sessions`
  - `nanotrace_magic_links`
  - `nanotrace_organization_invitations`
  - `nanotrace_api_keys`
  - additive columns added during multi-tenant work
- Document migration execution in local dev and deploy runbooks.
- Add explicit local/dev seed scripts if needed:
  - create a named dev organization
  - create dev users/memberships
  - create dev API keys.

Acceptance checks:

- Fresh Postgres bootstraps from migrations only.
- Existing dev Postgres upgrades without data loss.
- Server startup does not create tables or seed tenants outside the migration/seed path.
- No `org_default` runtime fallback remains.
- `cargo test --all-features` still passes.
- `npm run integration:kafka` still passes from a migrated database.

## Phase 2: Database Constraints And Indexes

Strengthen the database so invalid tenant/account state is harder to create.

Tasks:

- Add role checks:
  - `organization_members.role IN ('admin', 'viewer')`
  - `organization_invitations.role IN ('admin', 'viewer')`
  - API key roles remain `admin`, `service`, `viewer`.
- Add indexes:
  - `nanotrace_auth_sessions(active_organization_id)`
  - `nanotrace_organization_members(subject)`
  - `nanotrace_organization_invitations(organization_id)`
  - active invitation lookup indexes.
- Add an active invitation uniqueness rule, likely one pending invite per `(organization_id, email)`.
- Decide whether lowercase email should be enforced by application code only or also by a DB constraint.
- Verify foreign key behavior for deletion/revocation paths.

Acceptance checks:

- Invalid roles fail at the DB layer.
- Duplicate pending invites are rejected or idempotently handled.
- Existing integration seeds still work.

## Phase 3: Transactional Account Operations

Make multi-step account mutations atomic.

Tasks:

- Wrap organization creation in one transaction:
  - create org
  - add creator admin membership
  - update current session active org
  - record any account metadata
- Wrap invite acceptance in one transaction:
  - claim invitation
  - create/update membership
  - switch active session
- Wrap member removal in one transaction:
  - enforce last-admin invariant
  - delete membership
  - clear affected active sessions
- Decide how to handle SDK default definition seeding because it writes to ClickHouse and cannot be in the same Postgres transaction.
  - Recommended: commit Postgres org creation first, then best-effort seed defaults with retry/idempotency.

Acceptance checks:

- Failed partial operations do not leave broken memberships/sessions.
- Invite acceptance remains single-use under concurrent requests.
- Last-admin protection holds under concurrent demote/remove requests.

## Phase 4: Authorization Policy Cleanup

Make backend authorization rules explicit and difficult to misuse.

Tasks:

- Add narrow helper functions for route authorization:
  - `authorize_session`
  - `authorize_current_org_scope(scope)`
  - `authorize_current_org_admin`
  - `authorize_target_org_member(org_id)`
  - `authorize_target_org_admin(org_id)`
  - `reject_api_key_identity`
- Decide account-management policy for API keys:
  - Recommended: API keys cannot create/switch orgs and cannot manage members/invitations.
  - If API-key admins must manage account state, create explicit scoped permissions rather than relying on `admin`.
- Apply helpers consistently across organization/member/invite/API-key routes.
- Add tests for every account route covering session auth, API-key auth, admin, viewer, non-member, and removed member behavior.

Acceptance checks:

- API keys cannot call session-only routes.
- Viewers cannot manage members/invites/API keys.
- Removed or demoted users lose access on the next request.

## Phase 5: Organization API Completeness

Round out backend organization lifecycle endpoints.

Tasks:

- Add `PATCH /v1/organizations/{organization_id}` for name/slug updates.
- Add `DELETE /v1/organizations/{organization_id}` or archive semantics.
- Add `POST /v1/organizations/{organization_id}/leave` for current user self-removal.
- Define ownership/last-admin transfer behavior.
- Decide whether org slugs are mutable forever or become locked after creation.
- Add typed response structs instead of returning `serde_json::Value` for new organization APIs.

Acceptance checks:

- Last admin cannot leave/delete without a replacement or explicit archive flow.
- Slug uniqueness is enforced.
- OpenAPI exposes stable typed contracts.

## Phase 6: Invitation Lifecycle

Make invitations robust and operationally manageable.

Tasks:

- Make create-invite idempotent for an existing active invite.
- Add resend invite endpoint.
- Add explicit expiration cleanup or background maintenance.
- Add tests for:
  - duplicate pending invites
  - revoked invite cannot be accepted
  - expired invite cannot be accepted
  - accepted invite cannot be accepted twice
  - mismatched email cannot accept
  - role on accept updates existing membership intentionally.
- Consider including invitation ID in accept links in addition to token.
- Add dev/local invite delivery behavior, such as logging accept links when SES is not configured.

Acceptance checks:

- Invite lifecycle is deterministic under retries.
- Local/dev invite testing does not require real email delivery.

## Phase 7: Tenant Data Route Audit

Systematically verify that every public backend route is tenant-scoped.

Routes and stores to audit:

- `POST /v1/events`
- `GET /v1/events/{event_id}`
- `POST /v1/query`
- `GET /v1/query/recommendations`
- `GET/POST/DELETE /v1/definitions...`
- `GET/POST /v1/backfills...`
- `GET/POST/DELETE /v1/api-keys`
- organization/member/invitation routes
- any future dashboard, visualization, alert, or admin route.

Tasks:

- Create a route inventory with:
  - auth method
  - required role/scope
  - tenant source
  - backing store method
  - test coverage.
- Add integration tests for any route not already covered.
- Ensure no route accepts client-provided tenant/org IDs for data access unless separately authorized against membership.

Acceptance checks:

- Every public route has a documented tenant source.
- Cross-tenant attempts fail or return empty scoped results.

## Phase 8: Background Job And Derived Data Invariants

Document and test tenant preservation in workers and derived tables.

Tasks:

- Confirm normalizer always stamps tenant and organization from auth context, not event payload.
- Confirm materialization jobs carry tenant IDs through:
  - job records
  - chunks
  - watermarks
  - derived serving tables.
- Add targeted tests for derived rows never crossing tenant boundaries.
- Audit rebuild/backfill tools for any assumptions that operate globally without tenant filters.
- Decide whether internal maintenance tooling needs an explicit all-tenant admin mode.

Acceptance checks:

- Backfill/materialization outputs preserve tenant IDs.
- Worker-level all-tenant operations are internal-only and documented.

## Phase 9: Remove Legacy And Bootstrap Runtime Behavior

Delete legacy tenant fallback and bootstrap behavior from runtime code.

Tasks:

- Remove `DEFAULT_ORGANIZATION_ID` and all references to `org_default` from runtime auth/server logic.
- Remove `ensure_default_control_plane`.
- Remove `ensure_bootstrap_api_key` from server startup.
- Remove session validation fallback to `org_default`.
- Remove any tests that rely on implicit default org behavior.
- Add explicit dev seed tooling for local compose:
  - seed a named dev organization, for example `org_dev`
  - seed the dev bootstrap API key against that real org if local workflows still need it
  - seed dev memberships explicitly.
- Add a one-time migration/runbook for existing `org_default` data:
  - create a real organization for the legacy data
  - update API keys and memberships to point at that organization
  - preserve ClickHouse tenant IDs only if changing historical tenant IDs is too expensive; otherwise document whether historical `org_default` data is archived or migrated.
- Add a guard that fails startup if production config still references `org_default` after the removal migration.

Acceptance checks:

- `rg "org_default|DEFAULT_ORGANIZATION_ID|ensure_default_control_plane|ensure_bootstrap_api_key"` finds no runtime references.
- New users without memberships either get a real personal organization or remain in invite/onboarding state.
- API keys always belong to a real organization row.
- Local compose still works through explicit seed data.
- Production cannot silently fall back to a default tenant.

## Phase 10: Observability And Audit Trail

Add backend observability for account changes and tenant access.

Tasks:

- Add structured logs for:
  - org creation
  - org switching
  - invitation create/revoke/accept
  - member role changes/removal
  - API key create/revoke.
- Add an audit table for durable account events.
- Include actor subject, actor auth type, target org, target subject/email, and timestamp.
- Add metrics for org/account API failures by status class.

Acceptance checks:

- Account lifecycle changes can be audited after the fact.
- Suspicious forbidden cross-tenant attempts are visible in logs/metrics.

## Suggested Order

1. Versioned SQL migrations.
2. DB constraints and indexes.
3. Transactional account mutations.
4. Authorization helper cleanup and API-key account policy.
5. Route/store tenant audit with tests.
6. Organization API completeness.
7. Invitation lifecycle hardening.
8. Worker/derived-data invariant tests.
9. Remove legacy/default tenant runtime behavior.
10. Audit trail and metrics.

## Current Verification Baseline

As of the current implementation, these checks have passed:

- `cargo fmt --check`
- `cargo test --all-features`
- `npm run typecheck` with Node `22.17.1`
- `npm run integration:kafka`

The Kafka integration currently covers tenant stamping, API key isolation, definitions isolation, query isolation, and browser-session active organization switching for query, definitions, and API keys.
