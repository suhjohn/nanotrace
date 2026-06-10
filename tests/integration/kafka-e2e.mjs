#!/usr/bin/env node
/*
Integration scenarios covered by this file:

1. Tenant bootstrap and API key isolation.
2. Kafka ingest returns accepted mode.
3. ClickHouse raw event persistence.
4. Tableflow topic materialization and serving watermarks.
5. Tenant stamping overrides spoofed tenant fields.
6. SQL query tenant isolation.
7. SDK metric definitions are tenant-local.
8. Arbitrary KVs remain filterable-only unless explicitly defined.
9. Nested KV indexing covers nested objects, scalar arrays, and arrays of objects.
10. Structured KV filters match raw event_kv_index parity.
11. Explicit field definition backfill writes field_index.
12. Re-running Tableflow materialization does not duplicate serving rows.
13. Deterministic 10k+ synthetic SDK-like events are generated.
14. Measure cube p90/count/sum/min/max/avg by [service], [plan],
    [service, route, status], and [plan, country, llm.model].
15. Unmaterialized measure grouping fails clearly.
16. Funnel counts and conversions by plan.
17. Cohort membership for pro accounts that completed checkout.
18. Cohort-scoped measure query using materialized cohort membership plus
    cube/raw drilldown as appropriate.
19. Browser-session active organization switching scopes query, definitions,
    and API keys.
*/
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";

const composeFile = process.env.NANOTRACE_COMPOSE_FILE || "docker-compose.dev.yml";
const baseUrl = (process.env.NANOTRACE_URL || "http://localhost:18473").replace(/\/$/, "");
const clickhouseUrl = process.env.CLICKHOUSE_URL || "http://localhost:18123";
const clickhouseUser = process.env.CLICKHOUSE_USER || "default";
const clickhousePassword = process.env.CLICKHOUSE_PASSWORD || "nanotrace";
const clickhouseDatabase = process.env.CLICKHOUSE_DATABASE || "observatory";
const clickhouseTable = process.env.CLICKHOUSE_TABLE || "events";
const clickhouseKvIndexTable = process.env.CLICKHOUSE_EVENT_KV_INDEX_TABLE || "event_kv_index";
const suffix = `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
const runId = `it_kafka_${suffix}`;

const tenantA = `org_it_a_${suffix}`;
const tenantB = `org_it_b_${suffix}`;
const keyA = `ntak_it_a_${suffix}`;
const keyB = `ntak_it_b_${suffix}`;
const sessionToken = `nts_it_${suffix}`;
const sessionSubject = `email:shared_${suffix}@example.com`;
const sessionEmail = `shared_${suffix}@example.com`;
const viewerSessionToken = `nts_viewer_${suffix}`;
const viewerSubject = `email:viewer_${suffix}@example.com`;
const viewerEmail = `viewer_${suffix}@example.com`;
const outsiderSessionToken = `nts_outsider_${suffix}`;
const outsiderSubject = `email:outsider_${suffix}@example.com`;
const outsiderEmail = `outsider_${suffix}@example.com`;

const sharedEventId = `evt_shared_${suffix}`;
const spoofEventId = `evt_spoof_${suffix}`;
const spanStartId = `evt_span_start_${suffix}`;
const spanEndId = `evt_span_end_${suffix}`;
const nestedEventId = `evt_nested_${suffix}`;
const replayEventId = `evt_replay_${suffix}`;
const syntheticPointCount = Math.max(10_000, numberEnv("NANOTRACE_SYNTHETIC_POINTS", 10_000));
const syntheticSessionsPerBatch = Math.max(1, numberEnv("NANOTRACE_SYNTHETIC_SESSIONS_PER_BATCH", 250));
const syntheticMeasureName = `checkout.latency.${suffix}`;
const syntheticFunnelId = `signup_to_checkout_${suffix}`;
const syntheticCohortId = `pro_completed_accounts_${suffix}`;

await main();

async function main() {
  console.log(`integrationRun=${runId}`);
  await waitForHealthz();
  seedTenants();
  await sleep(5500);
  await waitForServer();

  await postEvents(keyA, tenantAEvents());
  await postEvents(keyB, tenantBEvents());

  await waitForClickHouseRows(6);
  await waitForServingWatermark(clickhouseKvIndexTable);
  await assertTenantStamping();
  await assertApiTenantIsolation();
  await assertApiTenantIsolationVariants();
  await assertUnifiedQuerySurface();
  await assertDefinitions();
  await assertAccountRouteSessionAuthorization();
  await assertSessionOrganizationSwitchIsolation();
  await assertApiKeyCannotUseSessionOnlyOrgRoutes();
  await assertEventKvIndexRows();
  await assertArbitraryKvFilters();
  await assertRawKvQueryParity();
  await assertBackfilledDefinitionWatermark();
  await assertTableflowMaterializerReplayIsIdempotent();
  await assertSyntheticAnalyticsOracle();

  console.log("integrationResult=ok");
}

function seedTenants() {
  const sql = `
INSERT INTO nanotrace_organizations (id, slug, name)
VALUES
  (${q(tenantA)}, ${q(`it-a-${suffix}`)}, 'Integration Tenant A'),
  (${q(tenantB)}, ${q(`it-b-${suffix}`)}, 'Integration Tenant B')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, updated_at = now();

INSERT INTO nanotrace_api_keys
  (organization_id, key_hash, prefix, name, role, scopes, created_by, revoked_at)
VALUES
  (${q(tenantA)}, ${q(tokenHash(keyA))}, ${q(keyA.slice(0, 16))}, 'integration-a', 'admin', '{}'::text[], 'integration', NULL),
  (${q(tenantB)}, ${q(tokenHash(keyB))}, ${q(keyB.slice(0, 16))}, 'integration-b', 'admin', '{}'::text[], 'integration', NULL)
ON CONFLICT (key_hash) DO UPDATE
SET organization_id = EXCLUDED.organization_id,
    name = EXCLUDED.name,
    role = EXCLUDED.role,
    scopes = EXCLUDED.scopes,
    revoked_at = NULL;

INSERT INTO nanotrace_auth_users (subject, email, name, role, updated_at)
VALUES
  (${q(sessionSubject)}, ${q(sessionEmail)}, 'Integration Shared User', 'viewer', now()),
  (${q(viewerSubject)}, ${q(viewerEmail)}, 'Integration Viewer User', 'viewer', now()),
  (${q(outsiderSubject)}, ${q(outsiderEmail)}, 'Integration Outsider User', 'viewer', now())
ON CONFLICT (subject) DO UPDATE
SET email = EXCLUDED.email,
    name = EXCLUDED.name,
    updated_at = now();

INSERT INTO nanotrace_organization_members (organization_id, subject, role, updated_at)
VALUES
  (${q(tenantA)}, ${q(sessionSubject)}, 'admin', now()),
  (${q(tenantB)}, ${q(sessionSubject)}, 'admin', now()),
  (${q(tenantA)}, ${q(viewerSubject)}, 'viewer', now())
ON CONFLICT (organization_id, subject) DO UPDATE
SET role = EXCLUDED.role,
    updated_at = now();

INSERT INTO nanotrace_auth_sessions
  (token_hash, subject, email, name, role, active_organization_id, expires_at)
VALUES
  (${q(tokenHash(sessionToken))}, ${q(sessionSubject)}, ${q(sessionEmail)}, 'Integration Shared User', 'viewer', ${q(tenantA)}, now() + interval '1 hour'),
  (${q(tokenHash(viewerSessionToken))}, ${q(viewerSubject)}, ${q(viewerEmail)}, 'Integration Viewer User', 'viewer', ${q(tenantA)}, now() + interval '1 hour'),
  (${q(tokenHash(outsiderSessionToken))}, ${q(outsiderSubject)}, ${q(outsiderEmail)}, 'Integration Outsider User', 'viewer', NULL, now() + interval '1 hour')
ON CONFLICT (token_hash) DO UPDATE
SET active_organization_id = EXCLUDED.active_organization_id,
    expires_at = EXCLUDED.expires_at,
    last_seen_at = now();
`;
  execFileSync(
    "docker",
    [
      "compose",
      "-f",
      composeFile,
      "exec",
      "-T",
      "postgres",
      "psql",
      "-U",
      "nanotrace",
      "-d",
      "nanotrace",
      "-v",
      "ON_ERROR_STOP=1",
    ],
    { input: sql, stdio: ["pipe", "inherit", "inherit"] },
  );
}

function tenantAEvents() {
  return [
    {
      event_id: spoofEventId,
      timestamp: "2026-06-04T22:00:00.000Z",
      data: {
        tenant_id: tenantB,
        event_type: "track",
        name: "Checkout Completed",
        revenue: 42.5,
        product_id: "sku-test",
        plan: "pro",
        _integration: { run_id: runId, tenant: "a" },
      },
    },
    {
      event_id: sharedEventId,
      timestamp: "2026-06-04T22:00:01.000Z",
      data: {
        event_type: "metric",
        metric_name: "checkout.latency",
        metric_type: "histogram",
        metric_value: 125.5,
        service: "api",
        environment: "test",
        _integration: { run_id: runId, tenant: "a" },
      },
    },
    {
      event_id: spanStartId,
      timestamp: "2026-06-04T22:00:02.000Z",
      data: {
        event_type: "span_start",
        trace_id: `trace_${suffix}`,
        span_id: `span_${suffix}`,
        parent_span_id: `root_${suffix}`,
        name: "POST /checkout",
        service: "api",
        environment: "test",
        _integration: { run_id: runId, tenant: "a" },
      },
    },
    {
      event_id: spanEndId,
      timestamp: "2026-06-04T22:00:03.000Z",
      data: {
        event_type: "span_end",
        trace_id: `trace_${suffix}`,
        span_id: `span_${suffix}`,
        name: "POST /checkout",
        service: "api",
        environment: "test",
        duration_ms: 57.25,
        span_status_code: "ok",
        _integration: { run_id: runId, tenant: "a" },
      },
    },
    {
      event_id: nestedEventId,
      timestamp: "2026-06-04T22:00:04.000Z",
      data: {
        event_type: "track",
        name: "Nested KV Probe",
        plan: "enterprise",
        latency_ms: 175,
        llm: {
          model: "gpt-4.1",
          usage: {
            prompt_tokens: 1200,
            completion_tokens: 320,
          },
        },
        tags: ["checkout", "mobile"],
        items: [
          { sku: "sku_1", price: 10 },
          { sku: "sku_2", price: 20 },
        ],
        _integration: { run_id: runId, tenant: "a" },
      },
    },
  ];
}

function tenantBEvents() {
  return [
    {
      event_id: sharedEventId,
      timestamp: "2026-06-04T22:00:05.000Z",
      data: {
        tenant_id: tenantA,
        event_type: "track",
        name: "Viewed Docs",
        service: "docs",
        _integration: { run_id: runId, tenant: "b" },
      },
    },
  ];
}

function replayTenantAEvents() {
  return [
    {
      event_id: replayEventId,
      timestamp: "2026-06-04T22:00:06.000Z",
      data: {
        event_type: "track",
        name: "Replay Guard",
        service: "api",
        replay_guard: "kv",
        _integration: { run_id: runId, tenant: "a", replay: true },
      },
    },
  ];
}

async function postEvents(apiKey, events) {
  const response = await fetch(`${baseUrl}/v1/events`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${apiKey}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(events),
  });
  const body = await response.text();
  assert(response.status === 202, `expected 202 ingest response, got ${response.status}: ${body}`);
  const parsed = JSON.parse(body);
  assert(parsed.mode === "kafka", `expected Kafka ingest mode, got ${body}`);
}

async function assertTenantStamping() {
  const rows = await clickhouseJson(`
SELECT
  event_id,
  tenant_id,
  toString(getSubcolumn(data, 'tenant_id')) AS data_tenant_id,
  event_type,
  signal
FROM ${ident(clickhouseDatabase)}.${ident(clickhouseTable)}
WHERE getSubcolumn(data, '_integration.run_id') = ${s(runId)}
ORDER BY tenant_id, event_id
FORMAT JSON
`);
  assert(rows.length === 6, `expected 6 persisted integration rows, got ${rows.length}`);
  const spoof = rows.find((row) => row.event_id === spoofEventId);
  assert(spoof, "spoofed tenant event was not persisted");
  assert(spoof.tenant_id === tenantA, `top-level tenant should be ${tenantA}, got ${spoof.tenant_id}`);
  assert(spoof.data_tenant_id === tenantA, `data.tenant_id should be ${tenantA}, got ${spoof.data_tenant_id}`);

  const sharedRows = rows.filter((row) => row.event_id === sharedEventId);
  assert(sharedRows.length === 2, `expected shared event id in both tenants, got ${sharedRows.length}`);
  assert(new Set(sharedRows.map((row) => row.tenant_id)).size === 2, "shared event id should remain tenant-scoped");

  const spanRows = rows.filter((row) => row.event_id === spanStartId || row.event_id === spanEndId);
  assert(spanRows.every((row) => row.signal === "trace"), "span_start/span_end should persist with trace signal");
}

async function assertApiTenantIsolation() {
  const query = {
    query: `
SELECT event_id, tenant_id
FROM events
WHERE tenant_id = {tenant_id:String}
  AND getSubcolumn(data, '_integration.run_id') = {run_id:String}
ORDER BY event_id
`,
  };
  const [a, b] = await Promise.all([
    postQuery(keyA, { ...query, parameters: { run_id: runId, tenant_id: tenantA } }),
    postQuery(keyB, { ...query, parameters: { run_id: runId, tenant_id: tenantB } }),
  ]);
  const rowsA = a.data || [];
  const rowsB = b.data || [];
  assert(rowsA.length === 5, `tenant A API query should see 5 rows, got ${rowsA.length}`);
  assert(rowsB.length === 1, `tenant B API query should see 1 row, got ${rowsB.length}`);
  assert(rowsA.every((row) => row.tenant_id === tenantA), "tenant A API query leaked another tenant");
  assert(rowsB.every((row) => row.tenant_id === tenantB), "tenant B API query leaked another tenant");
}

async function assertApiTenantIsolationVariants() {
  const variants = [
    `
SELECT event_id, tenant_id
FROM
  events
WHERE tenant_id = {tenant_id:String}
  AND getSubcolumn(data, '_integration.run_id') = {run_id:String}
ORDER BY event_id
`,
    `
SELECT e.event_id, e.tenant_id
FROM /* tenant scope must survive comments */ events AS e
WHERE e.tenant_id = {tenant_id:String}
  AND getSubcolumn(e.data, '_integration.run_id') = {run_id:String}
ORDER BY e.event_id
`,
    `
SELECT event_id, tenant_id
FROM observatory.events
WHERE tenant_id = {tenant_id:String}
  AND getSubcolumn(data, '_integration.run_id') = {run_id:String}
ORDER BY event_id
`,
  ];

  for (const query of variants) {
    const [a, b] = await Promise.all([
      postQuery(keyA, { query, parameters: { run_id: runId, tenant_id: tenantA } }),
      postQuery(keyB, { query, parameters: { run_id: runId, tenant_id: tenantB } }),
    ]);
    const rowsA = a.data || [];
    const rowsB = b.data || [];
    assert(rowsA.length === 5, `tenant A variant query should see 5 rows, got ${rowsA.length}`);
    assert(rowsB.length === 1, `tenant B variant query should see 1 row, got ${rowsB.length}`);
    assert(rowsA.every((row) => row.tenant_id === tenantA), "tenant A variant query leaked another tenant");
    assert(rowsB.every((row) => row.tenant_id === tenantB), "tenant B variant query leaked another tenant");
  }
}

async function assertUnifiedQuerySurface() {
  for (const path of ["/v1/events/query", "/v1/measures/query", "/v1/funnels/query", "/v1/cohorts/query", "/v1/sql/query"]) {
    const response = await fetch(`${baseUrl}${path}`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${keyA}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ type: "events", view: "summary" }),
    });
    assert([404, 405].includes(response.status), `${path} should not be part of the public query surface; got ${response.status}`);
  }

  for (const body of [
    { view: "summary" },
    { type: "sql", query: "SELECT 1" },
    { type: "unknown", view: "summary" },
  ]) {
    const response = await fetch(`${baseUrl}/v1/query`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${keyA}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    });
    assert(response.status === 400, `/v1/query should reject ${JSON.stringify(body)} with 400, got ${response.status}`);
  }

  const report = await postReportQuery(keyA, {
    reportId: `empty_report_${suffix}`,
    allowStaleServing: true,
  });
  assert(Array.isArray(report.rows), "unified report query should return a rows array");

  const state = await postStateQuery(keyA, {
    entityType: "account",
    stateName: "account.plan",
    allowStaleServing: true,
  });
  assert(Array.isArray(state.rows), "unified state query should return a rows array");
}

async function assertDefinitions() {
  const definitionsA = await waitForDefinitions(keyA, (definitions) =>
    hasDefinition(definitions, "metric_rollup", "metric.checkout.latency"),
  );
  const definitionsB = await getDefinitions(keyB);

  assert(
    definitionsA.every((definition) => definition.tenant_id === tenantA),
    "tenant A definitions contain another tenant id",
  );
  assert(
    definitionsB.definitions.every((definition) => definition.tenant_id === tenantB),
    "tenant B definitions contain another tenant id",
  );
  assert(
    !hasDefinition(definitionsB.definitions, "measure", "revenue"),
    "tenant B should not receive tenant A revenue definition",
  );
  assert(!hasDefinition(definitionsA, "field", "plan"), "plain plan field should stay unpromoted");
  assert(!hasDefinition(definitionsA, "measure", "revenue"), "plain revenue field should stay unpromoted");
  assert(!hasDefinition(definitionsA, "measure", "duration_ms"), "plain span duration should stay unpromoted");
  console.log(`definitionsA=${definitionsA.length}`);
  console.log(`definitionsB=${definitionsB.definitions.length}`);
}

async function assertAccountRouteSessionAuthorization() {
  const adminCookie = sessionCookie();
  const viewerCookie = sessionCookie(viewerSessionToken);
  const outsiderCookie = sessionCookie(outsiderSessionToken);

  const viewerMembers = await fetch(`${baseUrl}/v1/organizations/${tenantA}/members`, {
    headers: { cookie: viewerCookie },
  });
  assert(viewerMembers.status === 403, `viewer member listing should be forbidden, got ${viewerMembers.status}`);

  const viewerInvite = await fetch(`${baseUrl}/v1/organizations/${tenantA}/invitations`, {
    method: "POST",
    headers: { cookie: viewerCookie, "content-type": "application/json" },
    body: JSON.stringify({ email: `viewer-forbidden-${suffix}@example.com`, role: "viewer" }),
  });
  assert(viewerInvite.status === 403, `viewer invite creation should be forbidden, got ${viewerInvite.status}`);

  const outsiderMembers = await fetch(`${baseUrl}/v1/organizations/${tenantA}/members`, {
    headers: { cookie: outsiderCookie },
  });
  assert(outsiderMembers.status === 403, `non-member listing should be forbidden, got ${outsiderMembers.status}`);

  const removeViewer = await fetch(`${baseUrl}/v1/organizations/${tenantA}/members/${encodeURIComponent(viewerSubject)}`, {
    method: "DELETE",
    headers: { cookie: adminCookie },
  });
  const removeBody = await removeViewer.text();
  assert(removeViewer.ok, `admin should remove viewer member, got ${removeViewer.status}: ${removeBody}`);

  const viewerMe = await fetchJson(`${baseUrl}/v1/auth/me`, {
    headers: { cookie: viewerCookie },
  });
  assert(viewerMe.organization_id === "", `removed viewer should have no active organization, got ${viewerMe.organization_id}`);
  assert((viewerMe.organizations || []).length === 0, "removed viewer should have no memberships");

  const viewerDefinitions = await fetch(`${baseUrl}/v1/definitions`, {
    headers: { cookie: viewerCookie },
  });
  assert(viewerDefinitions.status === 403, `removed viewer data access should be forbidden, got ${viewerDefinitions.status}`);
}

async function assertSessionOrganizationSwitchIsolation() {
  const cookie = sessionCookie();
  const meA = await fetchJson(`${baseUrl}/v1/auth/me`, {
    headers: { cookie },
  });
  assert(meA.organization_id === tenantA, `session should start in tenant A, got ${meA.organization_id}`);
  assert((meA.organizations || []).length === 2, "shared session user should see two organization memberships");

  const keysA = await fetchJson(`${baseUrl}/v1/api-keys`, {
    headers: { cookie },
  });
  assert(
    (keysA.api_keys || []).some((key) => key.organization_id === tenantA && key.prefix === keyA.slice(0, 16)),
    "session in tenant A should list tenant A API keys",
  );
  assert(
    !(keysA.api_keys || []).some((key) => key.organization_id === tenantB),
    "session in tenant A should not list tenant B API keys",
  );

  const definitionsA = await fetchJson(`${baseUrl}/v1/definitions`, {
    headers: { cookie },
  });
  assert(
    (definitionsA.definitions || []).every((definition) => definition.tenant_id === tenantA),
    "session in tenant A definitions leaked another tenant",
  );
  assert(
    hasDefinition(definitionsA.definitions || [], "metric_rollup", "metric.checkout.latency"),
    "session in tenant A should see tenant A managed metric definition",
  );

  const queryA = await postEventsQueryWithCookie(cookie, {
    view: "summary",
    filter: {
      facets: [{ path: "_integration.run_id", operator: "eq", value: runId }],
    },
    allow_stale_serving: true,
  });
  const countA = Number((queryA.data || [])[0]?.count ?? 0);
  assert(countA === 5, `session in tenant A query should see 5 rows, got ${countA}`);

  await fetchJson(`${baseUrl}/v1/organizations/${tenantB}/switch`, {
    method: "POST",
    headers: { cookie, "content-type": "application/json" },
  });
  const meB = await fetchJson(`${baseUrl}/v1/auth/me`, {
    headers: { cookie },
  });
  assert(meB.organization_id === tenantB, `session should switch to tenant B, got ${meB.organization_id}`);

  const keysB = await fetchJson(`${baseUrl}/v1/api-keys`, {
    headers: { cookie },
  });
  assert(
    (keysB.api_keys || []).some((key) => key.organization_id === tenantB && key.prefix === keyB.slice(0, 16)),
    "session in tenant B should list tenant B API keys",
  );
  assert(
    !(keysB.api_keys || []).some((key) => key.organization_id === tenantA),
    "session in tenant B should not list tenant A API keys",
  );

  const definitionsB = await fetchJson(`${baseUrl}/v1/definitions`, {
    headers: { cookie },
  });
  assert(
    (definitionsB.definitions || []).every((definition) => definition.tenant_id === tenantB),
    "session in tenant B definitions leaked another tenant",
  );

  const queryB = await postEventsQueryWithCookie(cookie, {
    view: "summary",
    filter: {
      facets: [{ path: "_integration.run_id", operator: "eq", value: runId }],
    },
    allow_stale_serving: true,
  });
  const countB = Number((queryB.data || [])[0]?.count ?? 0);
  assert(countB === 1, `session in tenant B query should see 1 row, got ${countB}`);
}

async function assertApiKeyCannotUseSessionOnlyOrgRoutes() {
  const createResponse = await fetch(`${baseUrl}/v1/organizations`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${keyA}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ name: `API key forbidden ${suffix}` }),
  });
  assert(createResponse.status === 403, `API key org creation should be forbidden, got ${createResponse.status}`);

  const switchResponse = await fetch(`${baseUrl}/v1/organizations/${tenantB}/switch`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${keyA}`,
      "content-type": "application/json",
    },
  });
  assert(switchResponse.status === 403, `API key org switch should be forbidden, got ${switchResponse.status}`);

  const updateResponse = await fetch(`${baseUrl}/v1/organizations/${tenantA}`, {
    method: "PATCH",
    headers: {
      authorization: `Bearer ${keyA}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ name: `API key forbidden ${suffix}` }),
  });
  assert(updateResponse.status === 403, `API key org update should be forbidden, got ${updateResponse.status}`);

  const membersResponse = await fetch(`${baseUrl}/v1/organizations/${tenantA}/members`, {
    headers: { authorization: `Bearer ${keyA}` },
  });
  assert(membersResponse.status === 403, `API key member listing should be forbidden, got ${membersResponse.status}`);

  const inviteResponse = await fetch(`${baseUrl}/v1/organizations/${tenantA}/invitations`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${keyA}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ email: `api-key-forbidden-${suffix}@example.com`, role: "viewer" }),
  });
  assert(inviteResponse.status === 403, `API key invite creation should be forbidden, got ${inviteResponse.status}`);
}

async function assertEventKvIndexRows() {
  await waitUntil(async () => {
    const rows = await clickhouseJson(`
SELECT count() AS count
FROM ${ident(clickhouseDatabase)}.${ident(clickhouseKvIndexTable)}
WHERE tenant_id = ${s(tenantA)}
  AND event_id = ${s(nestedEventId)}
FORMAT JSON
`);
    return Number(rows[0]?.count || 0) >= 12 ? true : null;
  }, "nested event_kv_index rows");

  const rows = await clickhouseJson(`
SELECT DISTINCT path, value_type, string_value, number_value, bool_value, scope_path, scope_index
FROM ${ident(clickhouseDatabase)}.${ident(clickhouseKvIndexTable)}
WHERE tenant_id = ${s(tenantA)}
  AND event_id = ${s(nestedEventId)}
ORDER BY path, scope_index, string_value
FORMAT JSON
`);
  assert(rows.some((row) => row.path === "llm.model" && row.string_value === "gpt-4.1"), "missing llm.model KV row");
  assert(rows.some((row) => row.path === "llm.usage.prompt_tokens" && Number(row.number_value) === 1200), "missing prompt token KV row");
  assert(rows.some((row) => row.path === "tags" && row.string_value === "mobile"), "missing scalar array tag KV row");
  assert(rows.some((row) => row.path === "items[].sku" && row.string_value === "sku_1" && row.scope_path === "items" && Number(row.scope_index) === 0), "missing scoped item sku KV row");
  assert(rows.some((row) => row.path === "items[].price" && Number(row.number_value) === 20 && row.scope_path === "items" && Number(row.scope_index) === 1), "missing scoped item price KV row");
}

async function assertArbitraryKvFilters() {
  const response = await postEventsQuery(keyA, {
    view: "summary",
    filter: {
      facets: [{ path: "plan", operator: "eq", value: "pro" }],
    },
    allow_stale_serving: true,
  });
  const count = Number((response.data || [])[0]?.count ?? 0);
  assert(count === 1, `structured plan=pro query should find 1 row through KV index, got ${count}`);

  const nested = await postEventsQuery(keyA, {
    view: "summary",
    filter: {
      facets: [
        { path: "llm.model", operator: "eq", value: "gpt-4.1" },
        { path: "llm.usage.prompt_tokens", operator: "gte", value: "1000" },
        { path: "tags", operator: "eq", value: "mobile" },
      ],
    },
  });
  const nestedCount = Number((nested.data || [])[0]?.count ?? 0);
  assert(nestedCount === 1, `nested arbitrary KV filters should find 1 row, got ${nestedCount}`);

  const sameItem = await postEventsQuery(keyA, {
    view: "summary",
    filter: {
      facets: [
        { path: "items[].sku", operator: "eq", value: "sku_1" },
        { path: "items[].price", operator: "eq", value: "10" },
      ],
    },
  });
  assert(Number((sameItem.data || [])[0]?.count ?? 0) === 1, "same-item array filter should match sku_1 price 10");

  const mismatchedItem = await postEventsQuery(keyA, {
    view: "summary",
    filter: {
      facets: [
        { path: "items[].sku", operator: "eq", value: "sku_1" },
        { path: "items[].price", operator: "eq", value: "20" },
      ],
    },
  });
  assert(Number((mismatchedItem.data || [])[0]?.count ?? 0) === 0, "same-item array filter should reject sku_1 price 20");
}

async function assertRawKvQueryParity() {
  const response = await postQuery(keyA, {
    query: `
SELECT countDistinct(event_id) AS count
FROM event_kv_index
WHERE tenant_id = {tenant_id:String}
  AND path = {path:String}
  AND value_type = 'string'
  AND string_value = {value:String}
`,
    parameters: { tenant_id: tenantA, path: "llm.model", value: "gpt-4.1" },
  });
  const count = Number((response.data || [])[0]?.count ?? 0);
  assert(count === 1, `raw event_kv_index query should find exactly tenant A nested row, got ${count}`);
}

async function assertBackfilledDefinitionWatermark() {
  const mutation = await createDefinition(keyA, {
    name: "product_id",
    kind: "field",
    mode: "facet",
    config: { path: "product_id", value_type: "string" },
    backfill: {
      from: "2026-06-04T21:59:00.000Z",
      to: "2026-06-04T22:01:00.000Z",
    },
  });
  const definition = mutation.definition;
  assert(definition?.definition_id, "created definition did not return definition_id");
  assert(mutation.backfill?.status === "completed", `expected completed backfill, got ${JSON.stringify(mutation.backfill)}`);

  const indexRows = await postQuery(keyA, {
    query: `
SELECT event_id, field_name, value, definition_id, definition_version
FROM field_index
WHERE tenant_id = {tenant_id:String}
  AND definition_id = {definition_id:String}
ORDER BY event_id
`,
    parameters: { tenant_id: tenantA, definition_id: definition.definition_id },
  });
  assert((indexRows.data || []).length === 1, `expected one product_id field_index row, got ${JSON.stringify(indexRows.data || [])}`);
  assert(indexRows.data[0].value === "sku-test", `expected field_index value sku-test, got ${indexRows.data[0].value}`);

  const watermarks = await postQuery(keyA, {
    query: `
SELECT target_type, target_id, target_version, status
FROM materialization_watermarks
WHERE tenant_id = {tenant_id:String}
  AND target_id = {definition_id:String}
`,
    parameters: { tenant_id: tenantA, definition_id: definition.definition_id },
  });
  assert((watermarks.data || []).length === 1, `expected one materialization watermark, got ${JSON.stringify(watermarks.data || [])}`);
  assert(watermarks.data[0].target_type === "field", `expected field watermark, got ${watermarks.data[0].target_type}`);
  assert(watermarks.data[0].status === "active", `expected active watermark, got ${watermarks.data[0].status}`);

  const strictQuery = await postEventsQuery(keyA, {
    view: "summary",
    filter: {
      facets: [{ path: "product_id", operator: "eq", value: "sku-test" }],
    },
    timeRange: {
      createdAfter: "2026-06-04T21:59:00.000Z",
      createdBefore: "2026-06-04T22:01:00.000Z",
    },
  });
  const count = Number((strictQuery.data || [])[0]?.count ?? 0);
  assert(count === 1, `strict product_id query should find 1 indexed row, got ${count}`);
}

async function assertTableflowMaterializerReplayIsIdempotent() {
  const restartMaterializer = composeServiceRunning("materializer");
  if (restartMaterializer) {
    dockerCompose(["stop", "materializer"]);
  }
  try {
    await postEvents(keyA, replayTenantAEvents());
    runTableflowMaterializerOnce();
    runTableflowMaterializerOnce();

    const kvRows = await clickhouseJson(`
SELECT count() AS count
FROM ${ident(clickhouseDatabase)}.${ident(clickhouseKvIndexTable)}
WHERE tenant_id = ${s(tenantA)}
  AND event_id = ${s(replayEventId)}
  AND path = 'replay_guard'
  AND value_type = 'string'
  AND string_value = 'kv'
FORMAT JSON
`);
    const kvCount = Number(kvRows[0]?.count || 0);
    assert(kvCount === 1, `replayed normalizer offset should leave exactly one replay_guard KV row, got ${kvCount}`);
  } finally {
    if (restartMaterializer) {
      dockerCompose(["up", "-d", "materializer"]);
    }
  }
}

async function assertSyntheticAnalyticsOracle() {
  const synthetic = buildSyntheticAnalyticsOracle(syntheticPointCount, syntheticSessionsPerBatch);
  console.log(
    `syntheticDatapoints=${synthetic.expected.pointCount} syntheticEvents=${synthetic.expected.totalEvents} syntheticBatches=${synthetic.batches.length}`,
  );

  const restartMaterializer = composeServiceRunning("materializer");
  if (restartMaterializer) {
    dockerCompose(["stop", "materializer"]);
  }
  try {
    const definitions = await createSyntheticDefinitions();
    await postEventBatches(keyA, synthetic.batches);

    runTableflowMaterializerOnce();
    await waitForSyntheticEventRows(synthetic.expected.totalEvents);
    await waitForSyntheticKvIndex(synthetic.expected.sampleRequestId);
    await waitForSyntheticMaterialization(definitions, synthetic.expected);

    await assertSyntheticArbitraryKvQueries(synthetic.expected);
    await assertSyntheticMeasureRollups(definitions.measure, synthetic.expected);
    await assertSyntheticFunnel(definitions.sequence, synthetic.expected);
    await assertSyntheticCohort(definitions.cohort, definitions.measure, synthetic.expected);
    await assertSyntheticDefinitionsNotPromoted();
  } finally {
    if (restartMaterializer) {
      dockerCompose(["up", "-d", "materializer"]);
    }
  }
}

function buildSyntheticAnalyticsOracle(pointCount, sessionsPerBatch) {
  const plans = ["free", "pro", "enterprise"];
  const routes = ["/checkout", "/pricing", "/settings", "/docs"];
  const models = ["gpt-4.1", "gpt-4.1-mini", "claude-sonnet"];
  const countries = ["US", "CA", "GB", "DE"];
  const dimensions = ["plan", "route", "llm.model", "country", "plan_route"];
  const selectedModel = models[0];
  const baseMs = Date.parse("2026-06-04T23:00:00.000Z");
  const batches = [];
  let currentBatch = [];
  let currentBatchSessions = 0;

  const expected = {
    pointCount,
    dimensions,
    totalEvents: 0,
    metricEvents: pointCount,
    selectedModel,
    sampleRequestId: "",
    sampleMetricEventId: "",
    modelCounts: new Map(models.map((model) => [model, 0])),
    rollupStats: new Map(),
    funnelByPlan: new Map(
      plans.map((plan) => [
        plan,
        {
          signup: 0,
          checkout_started: 0,
          payment_submitted: 0,
          checkout_completed: 0,
        },
      ]),
    ),
    cohortAccounts: new Set(),
    cohortLatencies: [],
  };

  for (let index = 0; index < pointCount; index += 1) {
    const plan = plans[index % plans.length];
    const route = routes[(index * 7 + 1) % routes.length];
    const model = models[(index * 11 + 2) % models.length];
    const country = countries[(index * 13 + 3) % countries.length];
    const planRoute = `${plan}|${route}`;
    const accountId = `acct_${suffix}_${index}`;
    const userId = `user_${suffix}_${index}`;
    const sessionId = `sess_${suffix}_${index}`;
    const requestId = `req_${suffix}_${String(index).padStart(5, "0")}`;
    const orderId = `ord_${suffix}_${String(index).padStart(5, "0")}`;
    const latencyMs =
      80 +
      (index * 37) % 900 +
      (plan === "enterprise" ? 70 : plan === "pro" ? 35 : 0) +
      (route === "/checkout" ? 45 : 0) +
      (model === "gpt-4.1" ? 20 : 0);
    const checkoutStarted = index % 10 !== 9;
    const paymentSubmitted = checkoutStarted && index % 5 !== 4;
    const checkoutCompleted = paymentSubmitted && index % 4 !== 3;
    const common = {
      plan,
      route,
      country,
      entities: {
        account_id: accountId,
        user_id: userId,
        session_id: sessionId,
      },
      _oracle: {
        run_id: runId,
        index,
      },
    };
    const batchIndex = Math.floor(index / sessionsPerBatch);
    const sessionBaseMs = baseMs + batchIndex * 60_000;
    const offsetMs = index % 750;
    const sessionEvents = [];

    const metricEvent = {
      event_id: `evt_oracle_metric_${suffix}_${index}`,
      timestamp: new Date(sessionBaseMs + 40_000 + offsetMs).toISOString(),
      data: {
        ...common,
        event_type: "metric",
        name: "Checkout Latency",
        metric_name: syntheticMeasureName,
        metric_type: "histogram",
        metric_value: latencyMs,
        unit: "ms",
        request_id: requestId,
        order_id: orderId,
        llm: { model },
        slice: { plan_route: planRoute },
      },
    };
    sessionEvents.push(metricEvent);

    sessionEvents.push(trackEvent(index, "Signup Started", sessionBaseMs + offsetMs, common));
    expected.funnelByPlan.get(plan).signup += 1;

    if (checkoutStarted) {
      sessionEvents.push(trackEvent(index, "Checkout Started", sessionBaseMs + 10_000 + offsetMs, common));
      expected.funnelByPlan.get(plan).checkout_started += 1;
    }
    if (paymentSubmitted) {
      sessionEvents.push(trackEvent(index, "Payment Submitted", sessionBaseMs + 20_000 + offsetMs, common));
      expected.funnelByPlan.get(plan).payment_submitted += 1;
    }
    if (checkoutCompleted) {
      sessionEvents.push(
        trackEvent(index, "Checkout Completed", sessionBaseMs + 30_000 + offsetMs, {
          ...common,
          order_id: orderId,
        }),
      );
      expected.funnelByPlan.get(plan).checkout_completed += 1;
      if (plan === "pro") {
        expected.cohortAccounts.add(accountId);
        expected.cohortLatencies.push(latencyMs);
      }
    }

    expected.modelCounts.set(model, expected.modelCounts.get(model) + 1);
    addRollupValue(expected.rollupStats, "plan", plan, latencyMs);
    addRollupValue(expected.rollupStats, "route", route, latencyMs);
    addRollupValue(expected.rollupStats, "llm.model", model, latencyMs);
    addRollupValue(expected.rollupStats, "country", country, latencyMs);
    addRollupValue(expected.rollupStats, "plan_route", planRoute, latencyMs);
    if (index === 137) {
      expected.sampleRequestId = requestId;
      expected.sampleMetricEventId = metricEvent.event_id;
    }

    expected.totalEvents += sessionEvents.length;
    currentBatch.push(...sessionEvents);
    currentBatchSessions += 1;
    if (currentBatchSessions >= sessionsPerBatch) {
      batches.push(currentBatch);
      currentBatch = [];
      currentBatchSessions = 0;
    }
  }
  if (currentBatch.length > 0) {
    batches.push(currentBatch);
  }

  for (const stat of expected.rollupStats.values()) {
    finalizeStat(stat);
  }
  expected.funnelRows = expectedFunnelRows(expected.funnelByPlan);
  expected.cohortCount = expected.cohortAccounts.size;
  expected.cohortStat = finalizeStat({
    values: [...expected.cohortLatencies],
    count: expected.cohortLatencies.length,
    sum: expected.cohortLatencies.reduce((sum, value) => sum + value, 0),
    min: Math.min(...expected.cohortLatencies),
    max: Math.max(...expected.cohortLatencies),
  });

  return { batches, expected };
}

function trackEvent(index, name, timestampMs, data) {
  return {
    event_id: `evt_oracle_${slugName(name)}_${suffix}_${index}`,
    timestamp: new Date(timestampMs).toISOString(),
    data: {
      ...data,
      event_type: "track",
      name,
    },
  };
}

function expectedFunnelRows(funnelByPlan) {
  const steps = [
    ["signup", "signup_started"],
    ["checkout_started", "checkout_started"],
    ["payment_submitted", "payment_submitted"],
    ["checkout_completed", "checkout_completed"],
  ];
  const rows = [];
  for (const [plan, counts] of funnelByPlan) {
    for (let index = 0; index < steps.length; index += 1) {
      const [key, name] = steps[index];
      const nextKey = steps[index + 1]?.[0] ?? key;
      rows.push({
        plan,
        step_index: index,
        step_name: name,
        entity_count: counts[key],
        conversion_count: counts[nextKey],
      });
    }
  }
  return rows;
}

async function postEventBatches(apiKey, batches) {
  for (let index = 0; index < batches.length; index += 1) {
    await postEvents(apiKey, batches[index]);
    if ((index + 1) % 10 === 0 || index + 1 === batches.length) {
      console.log(`syntheticPostedBatches=${index + 1}/${batches.length}`);
    }
  }
}

async function waitForSyntheticEventRows(expectedRows) {
  await waitUntil(async () => {
    const rows = await clickhouseJson(`
SELECT count() AS count
FROM ${ident(clickhouseDatabase)}.${ident(clickhouseTable)}
WHERE getSubcolumn(data, '_oracle.run_id') = ${s(runId)}
FORMAT JSON
`);
    return Number(rows[0]?.count || 0) >= expectedRows ? true : null;
  }, `${expectedRows} synthetic ClickHouse rows`, 300_000, 2_000);
}

async function waitForSyntheticKvIndex(sampleRequestId) {
  await waitUntil(async () => {
    const rows = await clickhouseJson(`
SELECT count() AS count
FROM ${ident(clickhouseDatabase)}.${ident(clickhouseKvIndexTable)}
WHERE tenant_id = ${s(tenantA)}
  AND path = 'request_id'
  AND value_type = 'string'
  AND string_value = ${s(sampleRequestId)}
FORMAT JSON
`);
    return Number(rows[0]?.count || 0) === 1 ? true : null;
  }, "synthetic request_id KV index row", 300_000, 2_000);
}

async function createSyntheticDefinitions() {
  const measure = await createDefinition(keyA, {
    name: syntheticMeasureName,
    kind: "measure",
    mode: "cube",
    config: {
      match: {
        all: [
          { path: "event_type", op: "eq", value: "metric" },
          { path: "metric_name", op: "eq", value: syntheticMeasureName },
          { path: "_oracle.run_id", op: "eq", value: runId },
        ],
      },
      outputs: [
        {
          target: "measure_cube_rollups",
          measure_name: syntheticMeasureName,
          value: { path: "metric_value" },
          unit: "ms",
          dimension_sets: [
            { id: "plan", dimensions: [{ name: "plan", value: { path: "plan" } }] },
            { id: "route", dimensions: [{ name: "route", value: { path: "route" } }] },
            { id: "llm.model", dimensions: [{ name: "llm.model", value: { path: "llm.model" } }] },
            { id: "country", dimensions: [{ name: "country", value: { path: "country" } }] },
            { id: "plan_route", dimensions: [{ name: "plan_route", value: { path: "slice.plan_route" } }] },
          ],
          bucket_seconds: 300,
        },
      ],
    },
  });

  const sequence = await createDefinition(keyA, {
    name: syntheticFunnelId,
    kind: "sequence",
    mode: "funnel",
    config: {
      match: {
        all: [
          { path: "event_type", op: "eq", value: "track" },
          { path: "_oracle.run_id", op: "eq", value: runId },
        ],
      },
      outputs: [
        {
          target: "sequence_report_results",
          report_id: syntheticFunnelId,
          entity_id: { path: "entities.user_id" },
          dimensions: [{ name: "plan", value: { path: "plan" } }],
          steps: [
            {
              name: "signup_started",
              match: { all: [{ path: "name", op: "eq", value: "Signup Started" }] },
            },
            {
              name: "checkout_started",
              match: { all: [{ path: "name", op: "eq", value: "Checkout Started" }] },
            },
            {
              name: "payment_submitted",
              match: { all: [{ path: "name", op: "eq", value: "Payment Submitted" }] },
            },
            {
              name: "checkout_completed",
              match: { all: [{ path: "name", op: "eq", value: "Checkout Completed" }] },
            },
          ],
          bucket_seconds: 60,
        },
      ],
    },
  });

  const cohort = await createDefinition(keyA, {
    name: syntheticCohortId,
    kind: "cohort",
    mode: "membership",
    config: {
      match: {
        all: [
          { path: "event_type", op: "eq", value: "track" },
          { path: "_oracle.run_id", op: "eq", value: runId },
          { path: "name", op: "eq", value: "Checkout Completed" },
          { path: "plan", op: "eq", value: "pro" },
        ],
      },
      outputs: [
        {
          target: "cohort_memberships",
          cohort_id: syntheticCohortId,
          entity_type: "account",
          entity_id: { path: "entities.account_id" },
        },
      ],
    },
  });

  return {
    measure: measure.definition,
    sequence: sequence.definition,
    cohort: cohort.definition,
  };
}

function runTableflowMaterializerOnce() {
  dockerCompose(
    [
      "run",
      "--rm",
      "--no-deps",
      "-e",
      "NANOTRACE_TABLEFLOW_MATERIALIZE_ONCE=true",
      "-e",
      "NANOTRACE_TABLEFLOW_MATERIALIZE_IDLE_SECS=15",
      "-e",
      "NANOTRACE_TABLEFLOW_MATERIALIZER_GROUP_ID=nanotrace-tableflow-materializer-dev",
      "-e",
      "NANOTRACE_TABLEFLOW_MATERIALIZER_CLIENT_ID=nanotrace-tableflow-materializer-once",
      "server",
      "/usr/local/bin/nanotrace-lakehouse-rebuild",
    ],
    { timeout: 300_000 },
  );
}

async function waitForSyntheticMaterialization(definitions, expected) {
  const expectedMeasureRows = expected.metricEvents * expected.dimensions.length;
  const expectedRollupRows = expected.rollupStats.size;
  const expectedFunnelRowsCount = expected.funnelRows.length;

  await waitUntil(async () => {
    const [measureRows, rollupRows, funnelRows, cohortRows] = await Promise.all([
      clickhouseCount(`
SELECT count() AS count
FROM ${ident(clickhouseDatabase)}.${ident("measure_cube_points")}
WHERE definition_id = ${s(definitions.measure.definition_id)}
FORMAT JSON
`),
      clickhouseCount(`
SELECT count() AS count
FROM ${ident(clickhouseDatabase)}.${ident("measure_cube_rollups")}
WHERE definition_id = ${s(definitions.measure.definition_id)}
FORMAT JSON
`),
      clickhouseCount(`
SELECT count() AS count
FROM ${ident(clickhouseDatabase)}.${ident("sequence_report_results")}
WHERE report_id = ${s(syntheticFunnelId)}
FORMAT JSON
`),
      clickhouseCount(`
SELECT count() AS count
FROM ${ident(clickhouseDatabase)}.${ident("cohort_memberships")}
WHERE cohort_id = ${s(syntheticCohortId)}
FORMAT JSON
`),
    ]);
    return measureRows >= expectedMeasureRows &&
      rollupRows >= expectedRollupRows &&
      funnelRows >= expectedFunnelRowsCount &&
      cohortRows >= expected.cohortCount
      ? true
      : null;
  }, "synthetic derived materialization", 300_000, 2_000);
}

async function assertSyntheticArbitraryKvQueries(expected) {
  const requestQuery = await postEventsQuery(keyA, {
    view: "summary",
    filter: {
      facets: [
        { path: "_oracle.run_id", operator: "eq", value: runId },
        { path: "request_id", operator: "eq", value: expected.sampleRequestId },
      ],
    },
    allow_stale_serving: true,
  });
  assert(
    Number((requestQuery.data || [])[0]?.count ?? 0) === 1,
    `synthetic request_id filter should find 1 event, got ${JSON.stringify(requestQuery.data || [])}`,
  );

  const modelQuery = await postEventsQuery(keyA, {
    view: "summary",
    filter: {
      facets: [
        { path: "_oracle.run_id", operator: "eq", value: runId },
        { path: "llm.model", operator: "eq", value: expected.selectedModel },
      ],
    },
    allow_stale_serving: true,
  });
  const expectedModelCount = expected.modelCounts.get(expected.selectedModel);
  assert(
    Number((modelQuery.data || [])[0]?.count ?? 0) === expectedModelCount,
    `synthetic llm.model filter should find ${expectedModelCount} events, got ${JSON.stringify(modelQuery.data || [])}`,
  );
}

async function assertSyntheticMeasureRollups(definition, expected) {
  const response = await postQuery(keyA, {
    query: `
SELECT
  dimension_set_id,
  dimension_values[1] AS dimension_value,
  toUInt64(sumMerge(count_state)) AS count,
  sumMerge(sum_state) AS sum_value,
  minMerge(min_state) AS min_value,
  maxMerge(max_state) AS max_value,
  avgMerge(avg_state) AS avg_value,
  quantilesTDigestMerge(0.5, 0.9, 0.95, 0.99)(quantiles_state)[2] AS p90_value
FROM measure_cube_rollups
WHERE tenant_id = {tenant_id:String}
  AND definition_id = {definition_id:String}
GROUP BY dimension_set_id, dimension_values
ORDER BY dimension_set_id, dimension_value
`,
    parameters: { tenant_id: tenantA, definition_id: definition.definition_id },
  });
  const actual = new Map(
    (response.data || []).map((row) => [
      rollupKey(row.dimension_set_id, row.dimension_value),
      {
        count: Number(row.count),
        sum: Number(row.sum_value),
        min: Number(row.min_value),
        max: Number(row.max_value),
        avg: Number(row.avg_value),
        p90: Number(row.p90_value),
      },
    ]),
  );
  assert(
    actual.size === expected.rollupStats.size,
    `expected ${expected.rollupStats.size} synthetic rollup rows, got ${actual.size}`,
  );
  for (const [key, stat] of expected.rollupStats) {
    const row = actual.get(key);
    assert(row, `missing synthetic measure rollup ${key}`);
    assert(row.count === stat.count, `rollup ${key} count expected ${stat.count}, got ${row.count}`);
    assertApprox(row.sum, stat.sum, 0.001, `rollup ${key} sum`);
    assertApprox(row.min, stat.min, 0.001, `rollup ${key} min`);
    assertApprox(row.max, stat.max, 0.001, `rollup ${key} max`);
    assertApprox(row.avg, stat.sum / stat.count, 0.001, `rollup ${key} avg`);
    assertApprox(row.p90, stat.p90, Math.max(25, stat.p90 * 0.08), `rollup ${key} p90`);
  }

  const typed = await postMeasureQuery(keyA, {
    measureName: syntheticMeasureName,
    from: "2026-06-04T23:00:00.000Z",
    to: "2026-06-05T23:00:00.000Z",
    bucketSeconds: 300,
    groupBy: ["plan"],
    allowStaleServing: true,
  });
  assert((typed.rows || []).length > 0, "typed measure cube query should return plan rows");

  const failed = await fetch(`${baseUrl}/v1/query`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${keyA}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      type: "measure",
      measureName: syntheticMeasureName,
      from: "2026-06-04T23:00:00.000Z",
      to: "2026-06-05T23:00:00.000Z",
      bucketSeconds: 300,
      groupBy: ["plan", "route"],
      allowStaleServing: true,
    }),
  });
  assert(!failed.ok, "unmaterialized measure grouping should fail clearly");
}

async function assertSyntheticFunnel(_definition, expected) {
  const response = await postQuery(keyA, {
    query: `
SELECT
  toString(getSubcolumn(segment, 'plan')) AS plan,
  step_index,
  any(step_name) AS step_name,
  toUInt64(sum(entity_count)) AS entity_count,
  toUInt64(sum(conversion_count)) AS conversion_count
FROM sequence_report_results
WHERE tenant_id = {tenant_id:String}
  AND report_id = {report_id:String}
GROUP BY plan, step_index
ORDER BY plan, step_index
`,
    parameters: { tenant_id: tenantA, report_id: syntheticFunnelId },
  });
  const actual = new Map(
    (response.data || []).map((row) => [
      `${row.plan}\u0000${Number(row.step_index)}`,
      {
        step_name: row.step_name,
        entity_count: Number(row.entity_count),
        conversion_count: Number(row.conversion_count),
      },
    ]),
  );
  assert(actual.size === expected.funnelRows.length, `expected ${expected.funnelRows.length} funnel rows, got ${actual.size}`);
  for (const row of expected.funnelRows) {
    const actualRow = actual.get(`${row.plan}\u0000${row.step_index}`);
    assert(actualRow, `missing funnel row plan=${row.plan} step=${row.step_index}`);
    assert(actualRow.step_name === row.step_name, `funnel step name mismatch for ${row.plan}/${row.step_index}`);
    assert(
      actualRow.entity_count === row.entity_count,
      `funnel ${row.plan}/${row.step_name} entity_count expected ${row.entity_count}, got ${actualRow.entity_count}`,
    );
    assert(
      actualRow.conversion_count === row.conversion_count,
      `funnel ${row.plan}/${row.step_name} conversion_count expected ${row.conversion_count}, got ${actualRow.conversion_count}`,
    );
  }

  const typed = await postFunnelQuery(keyA, {
    reportId: syntheticFunnelId,
    allowStaleServing: true,
  });
  const typedKeys = new Set(
    (typed.rows || []).map((row) => `${row.dimensions?.plan}\u0000${Number(row.stepIndex)}`),
  );
  for (const row of expected.funnelRows) {
    assert(typedKeys.has(`${row.plan}\u0000${row.step_index}`), `typed funnel query missing ${row.plan}/${row.step_index}`);
  }
}

async function assertSyntheticCohort(_cohortDefinition, measureDefinition, expected) {
  const typed = await postCohortQuery(keyA, {
    cohortId: syntheticCohortId,
    entityType: "account",
    limit: 10_000,
    allowStaleServing: true,
  });
  assert(Number(typed.count || 0) === expected.cohortCount, `typed cohort query expected ${expected.cohortCount}, got ${typed.count}`);

  const membership = await postQuery(keyA, {
    query: `
SELECT count() AS count
FROM cohort_memberships
WHERE tenant_id = {tenant_id:String}
  AND cohort_id = {cohort_id:String}
`,
    parameters: { tenant_id: tenantA, cohort_id: syntheticCohortId },
  });
  assert(
    Number((membership.data || [])[0]?.count ?? 0) === expected.cohortCount,
    `cohort ${syntheticCohortId} expected ${expected.cohortCount} members, got ${JSON.stringify(membership.data || [])}`,
  );

  const sampleAccount = [...expected.cohortAccounts][0];
  const sample = await postQuery(keyA, {
    query: `
SELECT entity_id, first_seen, last_seen
FROM cohort_memberships
WHERE tenant_id = {tenant_id:String}
  AND cohort_id = {cohort_id:String}
  AND entity_type = 'account'
  AND entity_id = {entity_id:String}
`,
    parameters: { tenant_id: tenantA, cohort_id: syntheticCohortId, entity_id: sampleAccount },
  });
  assert((sample.data || []).length === 1, `missing sample cohort account ${sampleAccount}`);

  const cohortMeasure = await postQuery(keyA, {
    query: `
SELECT
  countDistinct(em.event_id) AS count,
  avg(em.value) AS avg_value,
  quantileExact(0.9)(em.value) AS p90_value
FROM measure_cube_points AS em
INNER JOIN event_kv_index AS kv
  ON kv.tenant_id = em.tenant_id
 AND kv.event_id = em.event_id
 AND kv.path = 'entities.account_id'
 AND kv.value_type = 'string'
INNER JOIN cohort_memberships AS c
  ON c.tenant_id = em.tenant_id
 AND c.cohort_id = {cohort_id:String}
 AND c.entity_type = 'account'
 AND c.entity_id = kv.string_value
WHERE em.definition_id = {definition_id:String}
  AND em.tenant_id = {tenant_id:String}
  AND em.measure_name = {measure_name:String}
  AND em.dimension_set_id = 'plan'
`,
    parameters: {
      tenant_id: tenantA,
      cohort_id: syntheticCohortId,
      definition_id: measureDefinition.definition_id,
      measure_name: syntheticMeasureName,
    },
  });
  const row = (cohortMeasure.data || [])[0] || {};
  assert(Number(row.count || 0) === expected.cohortStat.count, `cohort p90 count expected ${expected.cohortStat.count}, got ${row.count}`);
  assertApprox(Number(row.avg_value), expected.cohortStat.sum / expected.cohortStat.count, 0.001, "cohort avg latency");
  assertApprox(Number(row.p90_value), expected.cohortStat.p90, 25, "cohort p90 latency");
}

async function assertSyntheticDefinitionsNotPromoted() {
  const definitions = (await getDefinitions(keyA)).definitions;
  for (const field of ["request_id", "order_id", "llm.model"]) {
    assert(!hasDefinition(definitions, "field", field), `${field} should remain filterable-only and unpromoted`);
    assert(!hasDefinition(definitions, "measure", field), `${field} should not become a measure`);
  }
}

async function clickhouseCount(query) {
  const rows = await clickhouseJson(query);
  return Number(rows[0]?.count || 0);
}

async function waitForDefinitions(apiKey, predicate) {
  return waitUntil(async () => {
    const response = await getDefinitions(apiKey);
    return predicate(response.definitions) ? response.definitions : null;
  }, "managed metric definitions");
}

async function getDefinitions(apiKey) {
  return fetchJson(`${baseUrl}/v1/definitions`, {
    headers: { authorization: `Bearer ${apiKey}` },
  });
}

async function postQuery(apiKey, body) {
  void apiKey;
  return {
    data: await clickhouseJson(renderClickHouseQuery(body.query, body.parameters || {})),
  };
}

async function postEventsQuery(apiKey, body) {
  return fetchJson(`${baseUrl}/v1/query`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${apiKey}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ type: "events", ...body }),
  });
}

async function postEventsQueryWithCookie(cookie, body) {
  return fetchJson(`${baseUrl}/v1/query`, {
    method: "POST",
    headers: {
      cookie,
      "content-type": "application/json",
    },
    body: JSON.stringify({ type: "events", ...body }),
  });
}

async function postMeasureQuery(apiKey, body) {
  return fetchJson(`${baseUrl}/v1/query`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${apiKey}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ type: "measure", ...body }),
  });
}

async function postFunnelQuery(apiKey, body) {
  return fetchJson(`${baseUrl}/v1/query`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${apiKey}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ type: "funnel", ...body }),
  });
}

async function postCohortQuery(apiKey, body) {
  return fetchJson(`${baseUrl}/v1/query`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${apiKey}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ type: "cohort", ...body }),
  });
}

async function postReportQuery(apiKey, body) {
  return fetchJson(`${baseUrl}/v1/query`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${apiKey}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ type: "report", ...body }),
  });
}

async function postStateQuery(apiKey, body) {
  return fetchJson(`${baseUrl}/v1/query`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${apiKey}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ type: "state", ...body }),
  });
}

async function createDefinition(apiKey, body) {
  return fetchJson(`${baseUrl}/v1/definitions`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${apiKey}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(body),
  });
}

async function fetchJson(url, init = {}) {
  const response = await fetch(url, init);
  const text = await response.text();
  assert(response.ok, `${url} failed with ${response.status}: ${text}`);
  return text ? JSON.parse(text) : {};
}

async function waitForServer() {
  await waitUntil(async () => {
    try {
      const response = await fetch(`${baseUrl}/v1/definitions`, {
        headers: { authorization: `Bearer ${keyA}` },
      });
      return response.ok ? true : null;
    } catch {
      return null;
    }
  }, "server API");
}

async function waitForHealthz() {
  await waitUntil(async () => {
    try {
      const response = await fetch(`${baseUrl}/healthz`);
      return response.ok ? true : null;
    } catch {
      return null;
    }
  }, "server health");
}

async function waitForClickHouseRows(expected) {
  await waitUntil(async () => {
    const rows = await clickhouseJson(`
SELECT count() AS count
FROM ${ident(clickhouseDatabase)}.${ident(clickhouseTable)}
WHERE getSubcolumn(data, '_integration.run_id') = ${s(runId)}
FORMAT JSON
`);
    return Number(rows[0]?.count || 0) >= expected ? true : null;
  }, `${expected} ClickHouse rows`);
}

async function waitForClickHouseEventRows(eventId, expected) {
  await waitUntil(async () => {
    const rows = await clickhouseJson(`
SELECT count() AS count
FROM ${ident(clickhouseDatabase)}.${ident(clickhouseTable)}
WHERE event_id = ${s(eventId)}
  AND getSubcolumn(data, '_integration.run_id') = ${s(runId)}
FORMAT JSON
`);
    return Number(rows[0]?.count || 0) >= expected ? true : null;
  }, `${expected} ClickHouse rows for ${eventId}`);
}

async function waitForServingWatermark(servingTable) {
  await waitUntil(async () => {
    const rows = await clickhouseJson(`
SELECT count() AS count
FROM ${ident(clickhouseDatabase)}.${ident("serving_watermarks")}
WHERE serving_table = ${s(servingTable)}
  AND source_namespace = 'nanotrace'
  AND source_table = 'events'
FORMAT JSON
`);
    return Number(rows[0]?.count || 0) > 0 ? true : null;
  }, `${servingTable} serving watermark`);
}

async function waitUntil(fn, label, timeoutMs = 120_000, intervalMs = 1000) {
  const deadline = Date.now() + timeoutMs;
  let last = null;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      last = await fn();
      lastError = null;
    } catch (error) {
      lastError = error;
      last = null;
    }
    if (last) return last;
    await sleep(intervalMs);
  }
  const suffix = lastError ? `; last error: ${lastError.message || String(lastError)}` : "";
  throw new Error(`timed out waiting for ${label}${suffix}`);
}

async function clickhouseJson(query) {
  const url = new URL(clickhouseUrl);
  url.searchParams.set("database", clickhouseDatabase);
  url.searchParams.set("date_time_input_format", "best_effort");
  url.searchParams.set("type_json_skip_duplicated_paths", "1");
  const attempts = 5;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          authorization: `Basic ${Buffer.from(`${clickhouseUser}:${clickhousePassword}`).toString("base64")}`,
          "content-type": "text/plain; charset=utf-8",
        },
        body: query,
      });
      const text = await response.text();
      assert(response.ok, `ClickHouse query failed ${response.status}: ${text}`);
      return JSON.parse(text).data || [];
    } catch (error) {
      if (
        attempt === attempts ||
        error?.message?.startsWith("ClickHouse query failed") ||
        error instanceof SyntaxError
      ) {
        throw error;
      }
      await sleep(250 * attempt);
    }
  }
  return [];
}

function renderClickHouseQuery(query, parameters) {
  const rendered = query.replace(/\{([A-Za-z_][A-Za-z0-9_]*):[^}]+}/g, (match, name) => {
    assert(Object.prototype.hasOwnProperty.call(parameters, name), `missing query parameter: ${name}`);
    const value = parameters[name];
    if (typeof value === "number" || typeof value === "bigint") return String(value);
    if (typeof value === "boolean") return value ? "1" : "0";
    return s(value);
  });
  return /\bFORMAT\s+JSON\b/i.test(rendered) ? rendered : `${rendered}\nFORMAT JSON\n`;
}

function hasDefinition(definitions, kind, name) {
  return definitions.some((definition) => definition.kind === kind && definition.name === name);
}

function tokenHash(token) {
  return createHash("sha256").update(token).digest("hex");
}

function sessionCookie(token = sessionToken) {
  return `nanotrace_session=${token}`;
}

function q(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function s(value) {
  return q(value);
}

function ident(value) {
  assert(/^[A-Za-z_][A-Za-z0-9_]*$/.test(value), `invalid identifier: ${value}`);
  return `\`${value.replaceAll("`", "``")}\``;
}

function dockerCompose(args, options = {}) {
  execFileSync("docker", ["compose", "-f", composeFile, ...args], {
    stdio: options.stdio || ["ignore", "inherit", "inherit"],
    timeout: options.timeout,
  });
}

function composeServiceRunning(service) {
  const output = execFileSync(
    "docker",
    ["compose", "-f", composeFile, "ps", "--status", "running", "--services"],
    { stdio: ["ignore", "pipe", "inherit"], encoding: "utf8" },
  );
  return output
    .split(/\r?\n/)
    .map((value) => value.trim())
    .includes(service);
}

function numberEnv(key, fallback) {
  const value = process.env[key];
  if (!value) {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${key} must be a positive number`);
  }
  return Math.floor(parsed);
}

function addRollupValue(stats, dimensionName, dimensionValue, value) {
  const key = rollupKey(dimensionName, dimensionValue);
  let stat = stats.get(key);
  if (!stat) {
    stat = {
      dimensionName,
      dimensionValue,
      values: [],
      count: 0,
      sum: 0,
      min: Number.POSITIVE_INFINITY,
      max: Number.NEGATIVE_INFINITY,
    };
    stats.set(key, stat);
  }
  stat.values.push(value);
  stat.count += 1;
  stat.sum += value;
  stat.min = Math.min(stat.min, value);
  stat.max = Math.max(stat.max, value);
}

function finalizeStat(stat) {
  stat.values.sort((left, right) => left - right);
  stat.p90 = quantileNearest(stat.values, 0.9);
  return stat;
}

function quantileNearest(values, level) {
  assert(values.length > 0, "cannot compute quantile over empty values");
  const index = Math.min(values.length - 1, Math.max(0, Math.floor(level * (values.length - 1))));
  return values[index];
}

function rollupKey(dimensionName, dimensionValue) {
  return `${dimensionName}\u0000${dimensionValue}`;
}

function slugName(value) {
  return value.toLowerCase().replaceAll(/[^a-z0-9]+/g, "_").replaceAll(/^_+|_+$/g, "");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function assertApprox(actual, expected, tolerance, label) {
  assert(
    Number.isFinite(actual) && Math.abs(actual - expected) <= tolerance,
    `${label} expected ${expected} ± ${tolerance}, got ${actual}`,
  );
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}
