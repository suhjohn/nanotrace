ALTER TABLE nanotrace_organizations
ADD COLUMN IF NOT EXISTS archived_at timestamptz;

UPDATE nanotrace_auth_users
SET email = lower(email)
WHERE email <> lower(email);

UPDATE nanotrace_organization_invitations
SET email = lower(email)
WHERE email <> lower(email);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'nanotrace_auth_users_email_lower_check'
    ) THEN
        ALTER TABLE nanotrace_auth_users
        ADD CONSTRAINT nanotrace_auth_users_email_lower_check
        CHECK (email = lower(email));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'nanotrace_organization_invitations_email_lower_check'
    ) THEN
        ALTER TABLE nanotrace_organization_invitations
        ADD CONSTRAINT nanotrace_organization_invitations_email_lower_check
        CHECK (email = lower(email));
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS nanotrace_organizations_active_idx
ON nanotrace_organizations (id)
WHERE archived_at IS NULL;

CREATE TABLE IF NOT EXISTS nanotrace_account_audit_events (
    id bigserial PRIMARY KEY,
    event_type text NOT NULL,
    actor_subject text NOT NULL,
    actor_auth_type text NOT NULL,
    organization_id text REFERENCES nanotrace_organizations(id) ON DELETE SET NULL,
    target_subject text,
    target_email text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS nanotrace_account_audit_events_organization_id_idx
ON nanotrace_account_audit_events (organization_id, created_at DESC);

CREATE INDEX IF NOT EXISTS nanotrace_account_audit_events_actor_subject_idx
ON nanotrace_account_audit_events (actor_subject, created_at DESC);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'nanotrace_account_audit_events_auth_type_check'
    ) THEN
        ALTER TABLE nanotrace_account_audit_events
        ADD CONSTRAINT nanotrace_account_audit_events_auth_type_check
        CHECK (actor_auth_type IN ('session', 'api_key', 'login', 'system'));
    END IF;
END
$$;
