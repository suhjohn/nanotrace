#!/usr/bin/env node
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";

const composeFile = process.env.NANOTRACE_COMPOSE_FILE || "docker-compose.dev.yml";
const organizationId = process.env.NANOTRACE_DEV_ORGANIZATION_ID || "org_dev";
const organizationSlug = process.env.NANOTRACE_DEV_ORGANIZATION_SLUG || "dev";
const organizationName = process.env.NANOTRACE_DEV_ORGANIZATION_NAME || "Development";
const adminEmail = process.env.NANOTRACE_DEV_ADMIN_EMAIL || "dev@localhost";
const apiKey = process.env.NANOTRACE_DEV_API_KEY || "ntak_dev";

const subject = `email:${adminEmail.toLowerCase()}`;
const sql = `
INSERT INTO nanotrace_organizations (id, slug, name, plan, updated_at)
VALUES (${q(organizationId)}, ${q(organizationSlug)}, ${q(organizationName)}, 'developer', now())
ON CONFLICT (id) DO UPDATE
SET slug = EXCLUDED.slug,
    name = EXCLUDED.name,
    updated_at = now();

INSERT INTO nanotrace_auth_users (subject, email, name, role, updated_at)
VALUES (${q(subject)}, ${q(adminEmail.toLowerCase())}, 'Dev Admin', 'admin', now())
ON CONFLICT (subject) DO UPDATE
SET email = EXCLUDED.email,
    name = EXCLUDED.name,
    role = EXCLUDED.role,
    updated_at = now();

INSERT INTO nanotrace_organization_members (organization_id, subject, role, updated_at)
VALUES (${q(organizationId)}, ${q(subject)}, 'admin', now())
ON CONFLICT (organization_id, subject) DO UPDATE
SET role = EXCLUDED.role,
    updated_at = now();

INSERT INTO nanotrace_api_keys
  (organization_id, key_hash, prefix, name, role, scopes, created_by, revoked_at)
VALUES
  (${q(organizationId)}, ${q(tokenHash(apiKey))}, ${q(apiKey.slice(0, 16))}, 'dev', 'admin',
   ARRAY['ingest:write','query:read','definitions:write','api_keys:write','facets:write']::text[],
   ${q(subject)}, NULL)
ON CONFLICT (key_hash) DO UPDATE
SET organization_id = EXCLUDED.organization_id,
    name = EXCLUDED.name,
    role = EXCLUDED.role,
    scopes = EXCLUDED.scopes,
    created_by = EXCLUDED.created_by,
    revoked_at = NULL;
`;

try {
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
} catch (error) {
  console.error("Failed to seed dev auth data. Start the server once first so SQL migrations run.");
  throw error;
}

console.log(`seeded_dev_organization=${organizationId}`);
console.log(`seeded_dev_admin=${adminEmail.toLowerCase()}`);
console.log(`seeded_dev_api_key=${apiKey}`);

function tokenHash(token) {
  return createHash("sha256").update(token).digest("hex");
}

function q(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}
