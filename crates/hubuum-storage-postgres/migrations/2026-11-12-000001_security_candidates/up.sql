SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE credential_approvals DROP CONSTRAINT credential_approvals_operation_check; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE credential_approvals ADD CONSTRAINT credential_approvals_operation_check
    CHECK (operation IN ('create_token', 'renew_token', 'create_user', 'update_user',
                         'import_credentials', 'confirm_restore', 'activate_security_candidate')) NOT VALID;
ALTER TABLE credential_approvals VALIDATE CONSTRAINT credential_approvals_operation_check;

ALTER TABLE credential_approvals ADD COLUMN security_candidate_id UUID;
ALTER TABLE credential_approvals ADD CONSTRAINT credential_approvals_security_candidate_check
    CHECK ((operation = 'activate_security_candidate') = (security_candidate_id IS NOT NULL)
           AND security_candidate_id IS DISTINCT FROM '00000000-0000-0000-0000-000000000000'::uuid) NOT VALID;
ALTER TABLE credential_approvals VALIDATE CONSTRAINT credential_approvals_security_candidate_check;

-- Ephemeral, installation-local authority. Never include it in logical backups.
CREATE TABLE security_candidates (
    id UUID PRIMARY KEY,
    actor_id INTEGER NOT NULL CHECK (actor_id > 0),
    token_id INTEGER NOT NULL CHECK (token_id > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    document JSONB NOT NULL CHECK (jsonb_typeof(document) = 'object'),
    CHECK (document->>'id' = id::text),
    CHECK ((document->'owner'->>'actor_id')::integer = actor_id),
    CHECK ((document->'owner'->>'token_id')::integer = token_id),
    CHECK (document->>'group' IN ('authentication', 'authorization')),
    CHECK (jsonb_typeof(document->'checks') = 'array'),
    CHECK (jsonb_array_length(document->'checks') BETWEEN 1 AND 128)
);
-- Both indexes are built on the new, empty installation-local table.
CREATE INDEX security_candidates_expiry ON security_candidates (expires_at); -- hubuum-compat: bounded-transactional-index
CREATE INDEX security_candidates_actor ON security_candidates (actor_id); -- hubuum-compat: bounded-transactional-index

-- These bindings belong to the installation, like runtime configuration, and
-- survive removal of a provider and logical restoration of application data.
CREATE TABLE authentication_provider_bindings (
    name TEXT PRIMARY KEY CHECK (length(name) BETWEEN 1 AND 255 AND name <> 'local'),
    provider_id UUID NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('ldap', 'oidc', 'oauth')),
    namespace TEXT NOT NULL CHECK (length(namespace) BETWEEN 1 AND 4096)
);

CREATE FUNCTION hubuum_preserve_provider_identity_binding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR OLD.name IS DISTINCT FROM NEW.name
       OR OLD.provider_id IS DISTINCT FROM NEW.provider_id
       OR OLD.kind IS DISTINCT FROM NEW.kind
       OR OLD.namespace IS DISTINCT FROM NEW.namespace THEN
        RAISE EXCEPTION 'Authentication provider identity bindings are immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER authentication_provider_binding_immutable
    BEFORE UPDATE OR DELETE ON authentication_provider_bindings
    FOR EACH ROW EXECUTE FUNCTION hubuum_preserve_provider_identity_binding();
