CREATE TABLE IF NOT EXISTS nanotrace_projects (
    id text PRIMARY KEY,
    organization_id text NOT NULL REFERENCES nanotrace_organizations(id) ON DELETE CASCADE,
    slug text NOT NULL,
    name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    archived_at timestamptz,
    UNIQUE (organization_id, slug)
);

CREATE INDEX IF NOT EXISTS nanotrace_projects_organization_id_idx
ON nanotrace_projects (organization_id);

INSERT INTO nanotrace_projects (id, organization_id, slug, name, created_at, updated_at)
SELECT 'proj_' || substr(md5(o.id || ':default'), 1, 24),
       o.id,
       'default',
       'Default',
       o.created_at,
       now()
FROM nanotrace_organizations AS o
WHERE NOT EXISTS (
    SELECT 1
    FROM nanotrace_projects AS p
    WHERE p.organization_id = o.id
      AND p.slug = 'default'
)
ON CONFLICT DO NOTHING;

ALTER TABLE nanotrace_api_keys
ADD COLUMN IF NOT EXISTS project_ids text[] NOT NULL DEFAULT '{}';

ALTER TABLE nanotrace_api_keys
ADD COLUMN IF NOT EXISTS default_project_id text REFERENCES nanotrace_projects(id) ON DELETE SET NULL;

UPDATE nanotrace_api_keys AS k
SET default_project_id = p.id
FROM nanotrace_projects AS p
WHERE p.organization_id = k.organization_id
  AND p.slug = 'default'
  AND k.default_project_id IS NULL;

CREATE INDEX IF NOT EXISTS nanotrace_api_keys_default_project_id_idx
ON nanotrace_api_keys (default_project_id);
