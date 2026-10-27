DO $columns$
DECLARE layout RECORD;
BEGIN
    FOR layout IN SELECT * FROM public.hubuum_identity_layout() LOOP
        EXECUTE format('DROP TRIGGER %I ON public.%I',
            CASE WHEN layout.mode = 'schema_revision' THEN 'a_logical_identity' ELSE 'zz_logical_identity' END,
            layout.relation_name);
        EXECUTE format('ALTER TABLE public.%I DROP COLUMN logical_identity', layout.relation_name);
    END LOOP;
END
$columns$;
DROP FUNCTION hubuum_backfill_logical_identities(INTEGER);
DROP FUNCTION hubuum_validate_logical_identities();
DROP FUNCTION hubuum_manage_logical_identity();
DROP FUNCTION hubuum_legacy_identity(TEXT, JSONB);
DROP FUNCTION hubuum_legacy_version_identity(TEXT, JSONB);
DROP FUNCTION hubuum_capture_identity_references(JSONB, JSONB, BOOLEAN);
DROP FUNCTION hubuum_identity_reference(TEXT, BIGINT, BOOLEAN);
DROP FUNCTION hubuum_identity_version(TEXT, BIGINT, BIGINT, BOOLEAN);
DROP FUNCTION hubuum_identity_reference_layout();
DROP FUNCTION hubuum_unresolved_identity(BIGINT, BIGINT);
DROP FUNCTION hubuum_known_identity(UUID);
DROP FUNCTION hubuum_legacy_resource_identity(TEXT, JSONB);
DROP FUNCTION hubuum_identity_layout();
DROP TABLE hubuum_control.identity_mapping;
DROP TABLE hubuum_control.identity_versions;
DROP TABLE hubuum_control.identity_backfill;
DROP SCHEMA hubuum_control;
