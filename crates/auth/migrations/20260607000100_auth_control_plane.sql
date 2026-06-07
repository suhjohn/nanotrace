CREATE TABLE IF NOT EXISTS nanotrace_organizations (
    id text PRIMARY KEY,
    slug text NOT NULL UNIQUE,
    name text NOT NULL,
    plan text NOT NULL DEFAULT 'developer',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS nanotrace_auth_users (
    subject text PRIMARY KEY,
    email text NOT NULL,
    name text,
    role text NOT NULL DEFAULT 'viewer',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS nanotrace_organization_members (
    organization_id text NOT NULL REFERENCES nanotrace_organizations(id) ON DELETE CASCADE,
    subject text NOT NULL REFERENCES nanotrace_auth_users(subject) ON DELETE CASCADE,
    role text NOT NULL DEFAULT 'viewer',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, subject)
);

ALTER TABLE nanotrace_organization_members
ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS nanotrace_auth_sessions (
    token_hash text PRIMARY KEY,
    subject text NOT NULL REFERENCES nanotrace_auth_users(subject) ON DELETE CASCADE,
    email text NOT NULL,
    name text,
    role text NOT NULL,
    active_organization_id text REFERENCES nanotrace_organizations(id) ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL
);

ALTER TABLE nanotrace_auth_sessions
ADD COLUMN IF NOT EXISTS active_organization_id text REFERENCES nanotrace_organizations(id) ON DELETE SET NULL;

CREATE TABLE IF NOT EXISTS nanotrace_magic_links (
    token_hash text PRIMARY KEY,
    email text NOT NULL,
    return_to text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS nanotrace_organization_invitations (
    id bigserial PRIMARY KEY,
    organization_id text NOT NULL REFERENCES nanotrace_organizations(id) ON DELETE CASCADE,
    email text NOT NULL,
    role text NOT NULL DEFAULT 'viewer',
    token_hash text NOT NULL UNIQUE,
    invited_by text NOT NULL REFERENCES nanotrace_auth_users(subject) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    accepted_at timestamptz,
    revoked_at timestamptz
);

CREATE TABLE IF NOT EXISTS nanotrace_api_keys (
    id bigserial PRIMARY KEY,
    organization_id text NOT NULL REFERENCES nanotrace_organizations(id) ON DELETE CASCADE,
    key_hash text NOT NULL UNIQUE,
    prefix text NOT NULL,
    name text NOT NULL,
    role text NOT NULL DEFAULT 'service',
    scopes text[] NOT NULL DEFAULT '{}',
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz,
    expires_at timestamptz,
    revoked_at timestamptz
);

ALTER TABLE nanotrace_api_keys
ALTER COLUMN organization_id DROP DEFAULT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'nanotrace_api_keys_organization_id_fkey'
    ) THEN
        ALTER TABLE nanotrace_api_keys
        ADD CONSTRAINT nanotrace_api_keys_organization_id_fkey
        FOREIGN KEY (organization_id)
        REFERENCES nanotrace_organizations(id)
        ON DELETE CASCADE;
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS nanotrace_auth_sessions_expires_at_idx
ON nanotrace_auth_sessions (expires_at);

CREATE INDEX IF NOT EXISTS nanotrace_auth_sessions_active_organization_id_idx
ON nanotrace_auth_sessions (active_organization_id);

CREATE INDEX IF NOT EXISTS nanotrace_magic_links_expires_at_idx
ON nanotrace_magic_links (expires_at);

CREATE INDEX IF NOT EXISTS nanotrace_api_keys_active_idx
ON nanotrace_api_keys (key_hash)
WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS nanotrace_organization_members_subject_idx
ON nanotrace_organization_members (subject);

CREATE INDEX IF NOT EXISTS nanotrace_organization_invitations_organization_id_idx
ON nanotrace_organization_invitations (organization_id);

CREATE INDEX IF NOT EXISTS nanotrace_organization_invitations_pending_email_idx
ON nanotrace_organization_invitations (email)
WHERE accepted_at IS NULL AND revoked_at IS NULL;

WITH duplicate_pending_invitations AS (
    SELECT id,
           row_number() OVER (
               PARTITION BY organization_id, email
               ORDER BY created_at DESC, id DESC
           ) AS duplicate_rank
    FROM nanotrace_organization_invitations
    WHERE accepted_at IS NULL AND revoked_at IS NULL
)
UPDATE nanotrace_organization_invitations AS invitation
SET revoked_at = now()
FROM duplicate_pending_invitations AS duplicate
WHERE invitation.id = duplicate.id
  AND duplicate.duplicate_rank > 1;

CREATE UNIQUE INDEX IF NOT EXISTS nanotrace_organization_invitations_pending_org_email_idx
ON nanotrace_organization_invitations (organization_id, email)
WHERE accepted_at IS NULL AND revoked_at IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'nanotrace_auth_users_role_check'
    ) THEN
        ALTER TABLE nanotrace_auth_users
        ADD CONSTRAINT nanotrace_auth_users_role_check
        CHECK (role IN ('admin', 'viewer'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'nanotrace_organization_members_role_check'
    ) THEN
        ALTER TABLE nanotrace_organization_members
        ADD CONSTRAINT nanotrace_organization_members_role_check
        CHECK (role IN ('admin', 'viewer'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'nanotrace_organization_invitations_role_check'
    ) THEN
        ALTER TABLE nanotrace_organization_invitations
        ADD CONSTRAINT nanotrace_organization_invitations_role_check
        CHECK (role IN ('admin', 'viewer'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'nanotrace_api_keys_role_check'
    ) THEN
        ALTER TABLE nanotrace_api_keys
        ADD CONSTRAINT nanotrace_api_keys_role_check
        CHECK (role IN ('admin', 'service', 'viewer'));
    END IF;
END
$$;

-- One-time Postgres control-plane migration away from the removed runtime
-- default tenant. Historical ClickHouse data may still contain org_default;
-- serving behavior must not rely on a fallback to it.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM nanotrace_organizations WHERE id = 'org_default') THEN
        INSERT INTO nanotrace_organizations (id, slug, name, plan, created_at, updated_at)
        SELECT 'org_legacy_default',
               'legacy-default',
               'Legacy Default',
               plan,
               created_at,
               now()
        FROM nanotrace_organizations
        WHERE id = 'org_default'
        ON CONFLICT (id) DO NOTHING;

        UPDATE nanotrace_api_keys
        SET organization_id = 'org_legacy_default'
        WHERE organization_id = 'org_default';

        UPDATE nanotrace_organization_members
        SET organization_id = 'org_legacy_default'
        WHERE organization_id = 'org_default'
          AND NOT EXISTS (
              SELECT 1
              FROM nanotrace_organization_members existing
              WHERE existing.organization_id = 'org_legacy_default'
                AND existing.subject = nanotrace_organization_members.subject
          );

        DELETE FROM nanotrace_organization_members
        WHERE organization_id = 'org_default';

        UPDATE nanotrace_auth_sessions
        SET active_organization_id = 'org_legacy_default'
        WHERE active_organization_id = 'org_default';

        UPDATE nanotrace_organization_invitations
        SET organization_id = 'org_legacy_default'
        WHERE organization_id = 'org_default';

        DELETE FROM nanotrace_organizations
        WHERE id = 'org_default';
    END IF;
END
$$;
