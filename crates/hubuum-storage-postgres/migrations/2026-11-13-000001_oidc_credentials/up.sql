SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Installation-local credentials never enter portable backup documents.
CREATE TABLE auth_upstream_credentials (
    session_id UUID PRIMARY KEY,
    principal_id INTEGER NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    provider_id UUID NOT NULL REFERENCES authentication_provider_bindings(provider_id),
    revision BIGINT NOT NULL CHECK (revision > 0),
    document JSONB NOT NULL CHECK (jsonb_typeof(document) = 'object'),
    FOREIGN KEY (session_id, principal_id) REFERENCES auth_sessions(id, principal_id) ON DELETE CASCADE,
    CHECK (pg_column_size(document) <= 262144),
    CHECK ((document->>'session' = session_id::text) IS TRUE),
    CHECK (((document->>'principal')::integer = principal_id) IS TRUE),
    CHECK ((document#>>'{binding,provider}' = provider_id::text) IS TRUE),
    CHECK (((document#>>'{binding,revision}')::bigint = revision) IS TRUE)
);
CREATE INDEX auth_upstream_principal ON auth_upstream_credentials(principal_id, session_id); -- hubuum-compat: bounded-transactional-index

CREATE FUNCTION hubuum_destroy_upstream_session_credentials() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.revoked_at IS NOT NULL THEN
        DELETE FROM auth_upstream_credentials WHERE session_id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER auth_upstream_session_revoked AFTER UPDATE OF revoked_at ON auth_sessions
    FOR EACH ROW EXECUTE FUNCTION hubuum_destroy_upstream_session_credentials();

CREATE FUNCTION hubuum_destroy_changed_upstream_credentials() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.setting_group = 'authentication' AND NEW.revision IS DISTINCT FROM OLD.revision THEN
        DELETE FROM auth_upstream_credentials;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER auth_upstream_configuration_changed AFTER UPDATE ON runtime_settings
    FOR EACH ROW EXECUTE FUNCTION hubuum_destroy_changed_upstream_credentials();
