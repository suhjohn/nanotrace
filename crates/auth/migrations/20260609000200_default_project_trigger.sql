CREATE OR REPLACE FUNCTION nanotrace_insert_default_project()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO nanotrace_projects (id, organization_id, slug, name, created_at, updated_at)
    VALUES ('proj_' || substr(md5(NEW.id || ':default'), 1, 24),
            NEW.id,
            'default',
            'Default',
            NEW.created_at,
            now())
    ON CONFLICT (organization_id, slug) DO NOTHING;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS nanotrace_organizations_default_project_trg ON nanotrace_organizations;

CREATE TRIGGER nanotrace_organizations_default_project_trg
AFTER INSERT ON nanotrace_organizations
FOR EACH ROW
EXECUTE FUNCTION nanotrace_insert_default_project();
