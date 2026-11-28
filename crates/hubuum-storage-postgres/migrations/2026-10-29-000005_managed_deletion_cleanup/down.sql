SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

-- Serialize the emptiness checks with runtime registration and cleanup.
LOCK TABLE hubuum_control.deletion_authority IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM hubuum_control.runtime_registrations)
        OR EXISTS (SELECT 1 FROM hubuum_control.deletion_authority WHERE material->>'format' = '2') THEN
        RAISE EXCEPTION 'Cannot downgrade retained runtime ownership or managed deletion authority';
    END IF;
END $$;
DROP INDEX public.hubuumobject_history_cleanup_version_uuid;
DROP FUNCTION public.hubuum_unregister_runtime(UUID, UUID);
DROP FUNCTION public.hubuum_register_runtime(UUID, UUID);
DROP TABLE hubuum_control.runtime_registrations;
ALTER TABLE hubuum_control.deletion_authority
    DROP CONSTRAINT deletion_authority_material_check;
ALTER TABLE hubuum_control.deletion_authority
    ADD CONSTRAINT deletion_authority_material_check CHECK ((
        jsonb_typeof(material) = 'object'
        AND octet_length(material::text) <= 4194304
        AND material->>'format' = '1'
        AND material ?& ARRAY['lineage', 'epoch', 'revision', 'intents']
        AND (material->>'revision')::bigint >= 0
        AND jsonb_typeof(material->'intents') = 'object'
    ) IS TRUE) NOT VALID;
ALTER TABLE hubuum_control.deletion_authority VALIDATE CONSTRAINT deletion_authority_material_check;
