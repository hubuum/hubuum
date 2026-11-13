SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Short-lived installation-local authority, excluded from logical backups.
CREATE TABLE auth_interactions (
    id UUID PRIMARY KEY CHECK (id <> '00000000-0000-0000-0000-000000000000'),
    client_id TEXT NOT NULL CHECK (client_id ~ '^[A-Za-z0-9._:-]{1,128}$'),
    actor_id INTEGER CHECK (actor_id > 0),
    user_code_digest TEXT UNIQUE CHECK (user_code_digest ~ '^[a-f0-9]{64}$'),
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    retain_until TIMESTAMPTZ NOT NULL,
    document JSONB NOT NULL CHECK (jsonb_typeof(document) = 'object'),
    CHECK (pg_column_size(document) <= 524288),
    CHECK (expires_at > created_at AND expires_at <= created_at + interval '600 seconds'),
    CHECK (retain_until >= expires_at AND retain_until <= expires_at + interval '3600 seconds'),
    CHECK ((document#>>'{lifecycle,binding,id}' = id::text) IS TRUE),
    CHECK ((document#>>'{lifecycle,binding,client}' = client_id) IS TRUE),
    CHECK (((document#>>'{lifecycle,created_at}')::timestamptz = created_at) IS TRUE),
    CHECK (((document#>>'{lifecycle,expires_at}')::timestamptz = expires_at) IS TRUE),
    CHECK ((document#>>'{lifecycle,binding,authority,actor,principal_id}')::integer IS NOT DISTINCT FROM actor_id),
    CHECK (document->>'user_code' IS NOT DISTINCT FROM user_code_digest),
    CHECK ((document->>'redemption' ~ '^[a-f0-9]{64}$') IS TRUE),
    CHECK ((document#>>'{lifecycle,phase,state}' IN (
        'pending','authenticating','awaiting_approval','ready','denied',
        'expired','cancelled','session_consumed','approval_consumed',
        'verification_consumed','failed'
    )) IS TRUE)
);
CREATE INDEX auth_interactions_expiry ON auth_interactions (retain_until,id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX auth_interactions_client ON auth_interactions (client_id,expires_at); -- hubuum-compat: bounded-transactional-index
CREATE INDEX auth_interactions_actor ON auth_interactions (actor_id,expires_at); -- hubuum-compat: bounded-transactional-index

CREATE FUNCTION hubuum_preserve_terminal_auth_interaction() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.document#>>'{lifecycle,phase,state}' IN (
        'denied','expired','cancelled','session_consumed',
        'approval_consumed','verification_consumed','failed'
    ) AND NEW.document IS DISTINCT FROM OLD.document THEN
        RAISE EXCEPTION 'Terminal authentication interactions cannot be reopened';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER auth_interaction_terminal
    BEFORE UPDATE ON auth_interactions
    FOR EACH ROW EXECUTE FUNCTION hubuum_preserve_terminal_auth_interaction();
