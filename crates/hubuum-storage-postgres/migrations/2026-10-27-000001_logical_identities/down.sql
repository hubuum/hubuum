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

CREATE OR REPLACE TRIGGER refresh_class_reachability_cache
AFTER INSERT OR UPDATE ON public.hubuumclass_relation
FOR EACH STATEMENT EXECUTE FUNCTION public.refresh_class_reachability_cache();
CREATE OR REPLACE TRIGGER check_object_relation
BEFORE INSERT OR UPDATE ON public.hubuumobject_relation
FOR EACH ROW EXECUTE FUNCTION public.validate_object_relation();
CREATE OR REPLACE FUNCTION public.update_modified_column() RETURNS TRIGGER
LANGUAGE plpgsql AS $function$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END
$function$;
ALTER FUNCTION public.update_modified_column() RESET search_path;

CREATE OR REPLACE TRIGGER retain_export_discovery AFTER INSERT OR UPDATE ON public.export_task_outputs
FOR EACH ROW EXECUTE FUNCTION public.retain_task_output_discovery();
CREATE OR REPLACE TRIGGER retain_backup_discovery AFTER INSERT OR UPDATE ON public.backup_task_outputs
FOR EACH ROW EXECUTE FUNCTION public.retain_task_output_discovery();
CREATE OR REPLACE TRIGGER retain_schema_profile_output AFTER INSERT OR UPDATE ON public.schema_profile_outputs
FOR EACH ROW EXECUTE FUNCTION public.retain_schema_profile_output();
CREATE OR REPLACE TRIGGER retain_task_terminal_discovery BEFORE INSERT OR UPDATE ON public.tasks
FOR EACH ROW EXECUTE FUNCTION public.retain_task_terminal_discovery();
CREATE OR REPLACE FUNCTION public.update_task_modified_column() RETURNS TRIGGER
LANGUAGE plpgsql AS $function$
BEGIN
    NEW.updated_at := clock_timestamp() AT TIME ZONE 'UTC';
    RETURN NEW;
END
$function$;
ALTER FUNCTION public.update_task_modified_column() RESET search_path;
