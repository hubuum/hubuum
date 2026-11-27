SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Admission and cleanup run concurrently. Count retained rows, including
-- terminal interactions, in the same transaction as every insertion/deletion.
ALTER TABLE auth_interactions ADD COLUMN active_until TIMESTAMPTZ;
CREATE TABLE auth_interaction_capacity (
    singleton BOOLEAN PRIMARY KEY CHECK (singleton),
    retained BIGINT NOT NULL CHECK (retained BETWEEN 0 AND 65536)
);
INSERT INTO auth_interaction_capacity SELECT true, count(*) FROM auth_interactions;

CREATE FUNCTION hubuum_count_auth_interactions() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    delta BIGINT;
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        UPDATE auth_interaction_capacity SET retained = 0;
        RETURN NULL;
    ELSIF TG_OP = 'INSERT' THEN
        SELECT count(*) INTO delta FROM inserted;
    ELSE
        SELECT -count(*) INTO delta FROM deleted;
    END IF;
    IF delta <> 0 THEN
        UPDATE auth_interaction_capacity SET retained = retained + delta;
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER auth_interactions_count_insert AFTER INSERT ON auth_interactions
    REFERENCING NEW TABLE AS inserted FOR EACH STATEMENT EXECUTE FUNCTION hubuum_count_auth_interactions();
CREATE TRIGGER auth_interactions_count_delete AFTER DELETE ON auth_interactions
    REFERENCING OLD TABLE AS deleted FOR EACH STATEMENT EXECUTE FUNCTION hubuum_count_auth_interactions();
CREATE TRIGGER auth_interactions_count_truncate AFTER TRUNCATE ON auth_interactions
    FOR EACH STATEMENT EXECUTE FUNCTION hubuum_count_auth_interactions();

CREATE FUNCTION hubuum_index_auth_interaction_deadline() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.active_until := CASE
        WHEN NEW.document#>>'{lifecycle,phase,state}' IN ('pending','ready','awaiting_approval') THEN NEW.expires_at
        WHEN NEW.document#>>'{lifecycle,phase,state}' = 'authenticating' THEN
            LEAST(NEW.expires_at, (NEW.document#>>'{lifecycle,phase,deadline}')::timestamptz)
        END;
    RETURN NEW;
END;
$$;
CREATE TRIGGER auth_interactions_index_deadline BEFORE INSERT OR UPDATE ON auth_interactions
    FOR EACH ROW EXECUTE FUNCTION hubuum_index_auth_interaction_deadline();
UPDATE auth_interactions SET active_until = expires_at
    WHERE document#>>'{lifecycle,phase,state}' IN ('pending','ready','awaiting_approval','authenticating');
CREATE INDEX auth_interactions_active_until ON auth_interactions(active_until) INCLUDE (created_at, client_id, actor_id)
    WHERE active_until IS NOT NULL; -- hubuum-compat: bounded-transactional-index

-- Previous OAuth namespaces omitted profile headers. Upgrade only when the
-- append-only settings history identifies exactly one header set for the ID.
-- Leave room for the digest on a previously maximum-length binding.
ALTER TABLE authentication_provider_bindings DROP CONSTRAINT authentication_provider_bindings_namespace_check; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE authentication_provider_bindings ADD CONSTRAINT authentication_provider_bindings_namespace_check
    CHECK (length(namespace) BETWEEN 1 AND 8192) NOT VALID;
ALTER TABLE authentication_provider_bindings VALIDATE CONSTRAINT authentication_provider_bindings_namespace_check;
ALTER TABLE authentication_provider_bindings DISABLE TRIGGER authentication_provider_binding_immutable;
WITH selections AS (
    SELECT value FROM runtime_settings WHERE setting_group='authentication'
    UNION ALL SELECT value FROM runtime_setting_changes WHERE setting_group='authentication'
    UNION ALL SELECT previous_value FROM runtime_setting_changes WHERE setting_group='authentication'
), definitions AS (
    SELECT DISTINCT provider->>'provider_id' AS id, coalesce(provider#>'{configuration,profile_headers}', '{}'::jsonb) AS headers
    FROM selections CROSS JOIN LATERAL jsonb_array_elements(value->'providers') AS provider
    WHERE provider->>'kind'='oauth'
), bindings AS (
    SELECT id, min(encode(sha256(convert_to(coalesce((
        SELECT string_agg(key || ':' || value || chr(10), '' ORDER BY key COLLATE "C")
        FROM jsonb_each_text(headers)
    ), ''), 'UTF8')), 'hex')) AS digest
    FROM definitions GROUP BY id HAVING count(*) = 1
)
UPDATE authentication_provider_bindings b SET namespace = left(b.namespace, -1) || ',"' || bindings.digest || '"]'
FROM bindings WHERE b.kind='oauth' AND b.provider_id::text=bindings.id;
ALTER TABLE authentication_provider_bindings ENABLE TRIGGER authentication_provider_binding_immutable;

-- A consistent header history also establishes the namespace of retained
-- grants. A legacy three-part namespace cannot match newly presented proof.
UPDATE auth_upstream_credentials c
SET document = jsonb_set(c.document, '{namespace}', to_jsonb(b.namespace))
FROM authentication_provider_bindings b
WHERE c.provider_id=b.provider_id AND CASE WHEN b.kind='oauth' THEN jsonb_array_length(b.namespace::jsonb)=4 ELSE false END;

CREATE FUNCTION hubuum_clear_provider_memberships(scopes TEXT[]) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    -- Match manual assignment's lock order before deciding which memberships
    -- lose their final source. The FK cascades deletes to the removed sources.
    PERFORM m.principal_id FROM group_memberships m
    WHERE EXISTS (SELECT FROM group_membership_sources s JOIN identity_scopes i ON i.id=s.source_scope_id
        WHERE (s.principal_id,s.group_id)=(m.principal_id,m.group_id) AND s.source='external' AND i.name=ANY(scopes))
    ORDER BY m.principal_id,m.group_id FOR UPDATE;
    DELETE FROM group_memberships m USING group_membership_sources s, identity_scopes i
    WHERE (s.principal_id,s.group_id)=(m.principal_id,m.group_id) AND i.id=s.source_scope_id
        AND s.source='external' AND i.name=ANY(scopes)
        AND NOT EXISTS (SELECT FROM group_membership_sources other JOIN identity_scopes oi ON oi.id=other.source_scope_id
            WHERE (other.principal_id,other.group_id)=(m.principal_id,m.group_id)
                AND (other.source<>'external' OR oi.name<>ALL(scopes)));
    DELETE FROM group_membership_sources s USING identity_scopes i
    WHERE i.id=s.source_scope_id AND s.source='external' AND i.name=ANY(scopes);
END;
$$;

-- Repair installations that already activated local-only membership.
SELECT hubuum_clear_provider_memberships(array_agg(provider->>'name'))
FROM runtime_settings CROSS JOIN LATERAL jsonb_array_elements(value->'providers') AS provider
WHERE setting_group='authentication' AND provider->>'kind' IN ('oidc','oauth')
    AND provider#>>'{configuration,membership,mode}'='local_only'
HAVING count(*) > 0;
