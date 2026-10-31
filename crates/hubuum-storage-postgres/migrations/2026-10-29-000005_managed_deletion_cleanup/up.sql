SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

-- V1 material remains byte-compatible with existing signed checkpoints. V2
-- adds explicit managed-copy bindings and receipts; old copies stay unbound.
ALTER TABLE hubuum_control.deletion_authority DROP CONSTRAINT deletion_authority_material_check; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE hubuum_control.deletion_authority
    ADD CONSTRAINT deletion_authority_material_check CHECK ((
        jsonb_typeof(material) = 'object'
        AND octet_length(material::text) <= 4194304
        AND material->>'format' IN ('1', '2')
        AND material ?& ARRAY['lineage', 'epoch', 'revision', 'intents']
        AND (material->>'revision')::bigint >= 0
        AND jsonb_typeof(material->'intents') = 'object'
    ) IS TRUE) NOT VALID;
ALTER TABLE hubuum_control.deletion_authority VALIDATE CONSTRAINT deletion_authority_material_check;

-- No expiry, heartbeat heuristic, foreign key or logical-restore replacement.
-- An uncertain/crashed runtime blocks cleanup until a future fenced recovery.
CREATE TABLE hubuum_control.runtime_registrations (
    id UUID PRIMARY KEY CHECK (id <> '00000000-0000-0000-0000-000000000000'),
    ownership_token UUID NOT NULL CHECK (ownership_token <> '00000000-0000-0000-0000-000000000000')
);
REVOKE ALL ON hubuum_control.runtime_registrations FROM PUBLIC;

CREATE FUNCTION public.hubuum_register_runtime(registration UUID, ownership UUID)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE authority JSONB;
BEGIN
    SELECT material INTO STRICT authority FROM hubuum_control.deletion_authority WHERE id = 1 FOR UPDATE;
    IF authority->'intents' <> '{}'::jsonb
        OR (SELECT count(*) FROM hubuum_control.runtime_registrations) >= 1024 THEN
        RETURN FALSE;
    END IF;
    INSERT INTO hubuum_control.runtime_registrations (id, ownership_token) VALUES (registration, ownership);
    RETURN TRUE;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_register_runtime(UUID, UUID) FROM PUBLIC;

CREATE FUNCTION public.hubuum_unregister_runtime(registration UUID, ownership UUID)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id = 1 FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Deletion authority is unavailable; retain runtime registration';
    END IF;
    DELETE FROM hubuum_control.runtime_registrations WHERE id = registration AND ownership_token = ownership;
    RETURN NOT EXISTS (SELECT 1 FROM hubuum_control.runtime_registrations WHERE id = registration);
END $$;
REVOKE ALL ON FUNCTION public.hubuum_unregister_runtime(UUID, UUID) FROM PUBLIC;

-- Compare parsed identities, including valid alternate UUID spellings retained
-- by an imported snapshot. Ambiguous or malformed identity material fails the
-- upgrade instead of allowing cleanup to claim a matching copy is absent.
CREATE UNIQUE INDEX hubuumobject_history_cleanup_version_uuid
    ON public.hubuumobject_history (((logical_identity #>> '{subject,version}')::uuid)); -- hubuum-compat: bounded-transactional-index
