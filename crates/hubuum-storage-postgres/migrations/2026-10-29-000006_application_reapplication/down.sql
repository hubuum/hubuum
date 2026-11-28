SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';
LOCK TABLE hubuum_control.deletion_authority IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM hubuum_control.runtime_registrations)
       OR EXISTS (SELECT 1 FROM hubuum_control.application_control WHERE reapplication_revision IS NOT NULL OR pending_restore)
       OR EXISTS (SELECT 1 FROM hubuum_control.task_execution_claims) THEN
        RAISE EXCEPTION 'Cannot downgrade retained application reapplication, runtime ownership, or active task claims';
    END IF;
END $$;
-- Older identity triggers discard provenance on history insertion. Do not
-- return to those semantics while retained records depend on that evidence.
DO $provenance$ DECLARE layout RECORD; retained BOOLEAN;
BEGIN
    FOR layout IN SELECT * FROM public.hubuum_identity_layout() LOOP
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM public.%I t,
            LATERAL jsonb_object_keys(COALESCE(t.logical_identity->''references'', ''{}''::jsonb)) source(key)
            WHERE source.key LIKE ''%%/restore_source_versions/%%''
               OR source.key LIKE ''%%/restore_unknown_source_versions/%%'')', layout.relation_name)
            INTO retained;
        IF retained THEN
            RAISE EXCEPTION 'Cannot downgrade retained restored-version provenance';
        END IF;
    END LOOP;
END $provenance$;
CREATE OR REPLACE FUNCTION public.hubuum_manage_logical_identity()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE
    layout RECORD;
    source_row JSONB := to_jsonb(NEW);
    previous_row JSONB := CASE WHEN TG_OP = 'UPDATE' THEN to_jsonb(OLD) ELSE NULL END;
    evidence JSONB;
    subject JSONB;
    class_reference JSONB;
    trusted_restore BOOLEAN := current_setting('hubuum.restore_identities', TRUE) IS NOT DISTINCT FROM 'on'
        AND pg_has_role(session_user, current_user, 'MEMBER');
BEGIN
    IF trusted_restore THEN
        IF NEW.logical_identity IS NULL THEN
            RAISE EXCEPTION 'Restore requires upgraded logical identity evidence' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    SELECT * INTO STRICT layout FROM public.hubuum_identity_layout() WHERE relation_name = TG_TABLE_NAME;
    IF TG_OP = 'INSERT' AND NEW.logical_identity IS NOT NULL
        AND layout.mode NOT IN ('history', 'schema_history') THEN
        IF TG_TABLE_NAME = 'restore_success_receipts' AND pg_has_role(session_user, current_user, 'MEMBER') THEN
            -- The executor copies the pre-replacement requester evidence from
            -- its retained stage. It must not resolve that integer after restore.
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'New records cannot supply logical identity evidence' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF NEW.logical_identity IS DISTINCT FROM OLD.logical_identity THEN
            RAISE EXCEPTION 'Logical identity evidence cannot be supplied by an update' USING ERRCODE = '23514';
        END IF;
        evidence := COALESCE(OLD.logical_identity, public.hubuum_legacy_identity(TG_TABLE_NAME, previous_row));
        previous_row := jsonb_set(previous_row, '{logical_identity}', evidence);
        subject := evidence->'subject';
        IF layout.mode = 'resource'
            AND (source_row - 'logical_identity') IS DISTINCT FROM (previous_row - 'logical_identity') THEN
            subject := jsonb_set(subject, '{version}', to_jsonb(gen_random_uuid()));
        END IF;
    ELSIF layout.mode IN ('resource', 'principal', 'task') THEN
        IF NEW.logical_identity IS NOT NULL THEN
            RAISE EXCEPTION 'New resources must mint a fresh logical identity' USING ERRCODE = '23514';
        END IF;
        subject := jsonb_build_object('kind', CASE WHEN layout.mode = 'resource' THEN 'resource' ELSE layout.mode END,
            'identity', public.hubuum_known_identity(gen_random_uuid()));
        IF layout.mode = 'resource' THEN
            subject := subject || jsonb_build_object('version', gen_random_uuid());
        END IF;
    ELSIF layout.mode = 'history' THEN
        subject := NEW.logical_identity->'subject';
        IF subject IS NULL THEN
            -- Deletion can race a bounded backfill before its live row is visited.
            subject := public.hubuum_legacy_identity(TG_TABLE_NAME, source_row)->'subject';
        END IF;
        IF NEW.op = 'D' THEN
            subject := jsonb_set(subject, '{version}', to_jsonb(gen_random_uuid()));
        END IF;
    ELSIF layout.mode = 'principal_extension' THEN
        class_reference := public.hubuum_identity_reference('principal', (source_row->>'id')::BIGINT, FALSE);
        subject := jsonb_build_object('kind', 'principal', 'identity', class_reference->'reference');
    ELSIF layout.mode IN ('definition_history', 'schema_revision', 'schema_history') THEN
        class_reference := public.hubuum_identity_reference('class', (source_row->>'class_id')::BIGINT, FALSE);
        subject := jsonb_build_object('kind', 'resource', 'identity',
            COALESCE(source_row #> '{snapshot,logical_identity,subject,identity}', class_reference->'reference'),
            'version', gen_random_uuid());
    ELSIF layout.family = 'event' THEN
        subject := jsonb_build_object('kind', 'event', 'identity', source_row->'event_id');
    END IF;
    -- Closure changes neither the captured references nor the historical UUID.
    IF TG_OP = 'UPDATE' AND layout.mode IN ('history', 'definition_history', 'schema_history') THEN
        NEW.logical_identity := evidence;
    ELSE
        NEW.logical_identity := jsonb_build_object('subject', subject, 'references',
            public.hubuum_capture_identity_references(source_row, previous_row, FALSE));
    END IF;
    RETURN NEW;
END
$function$;

DROP FUNCTION public.hubuum_pending_restore_job();
DROP FUNCTION public.hubuum_rotate_application_for_restore();
DROP TRIGGER task_execution_claim_settle ON public.tasks;
DROP TRIGGER task_execution_claim_delete ON public.tasks;
DROP FUNCTION public.hubuum_forget_task_execution_claim();
DROP FUNCTION public.hubuum_task_execution_claim_allowed(INTEGER, UUID);
DROP FUNCTION public.hubuum_record_task_execution_claim(INTEGER, UUID);
DROP TABLE hubuum_control.task_execution_claims;
DROP FUNCTION public.hubuum_acknowledge_runtime_drain(UUID, UUID, UUID, BIGINT);
DROP FUNCTION public.hubuum_capture_application_execution(UUID, UUID);
CREATE OR REPLACE FUNCTION public.hubuum_register_runtime(registration UUID, ownership UUID)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE authority JSONB;
BEGIN
    SELECT material INTO STRICT authority FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE;
    IF authority->'intents' <> '{}'::jsonb OR (SELECT count(*) FROM hubuum_control.runtime_registrations) >= 1024 THEN RETURN FALSE; END IF;
    INSERT INTO hubuum_control.runtime_registrations (id, ownership_token) VALUES (registration, ownership);
    RETURN TRUE;
END $$;
DROP FUNCTION public.hubuum_register_runtime_isolated(UUID, UUID, JSONB);
DROP FUNCTION public.hubuum_application_execution_allowed(UUID, UUID, UUID, BIGINT);
CREATE OR REPLACE FUNCTION public.hubuum_deletion_execution_allowed() RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE authority jsonb;
BEGIN
    SELECT material INTO STRICT authority FROM hubuum_control.deletion_authority WHERE id=1 FOR SHARE;
    RETURN authority->'intents' = '{}'::jsonb;
END $$;
CREATE OR REPLACE FUNCTION public.hubuum_deletion_exposure_allowed() RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT public.hubuum_deletion_control_read()->'intents' = '{}'::jsonb
$$;
DROP FUNCTION public.hubuum_application_control_read();
CREATE OR REPLACE FUNCTION public.hubuum_unregister_runtime(registration UUID, ownership UUID)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Deletion authority is unavailable; retain runtime registration'; END IF;
    DELETE FROM hubuum_control.runtime_registrations WHERE id=registration AND ownership_token=ownership;
    RETURN NOT EXISTS (SELECT 1 FROM hubuum_control.runtime_registrations WHERE id=registration);
END $$;
ALTER TABLE hubuum_control.runtime_registrations DROP CONSTRAINT runtime_drain_pair_check;
ALTER TABLE hubuum_control.runtime_registrations DROP COLUMN drained_revision;
ALTER TABLE hubuum_control.runtime_registrations DROP COLUMN drained_incarnation;
ALTER TABLE hubuum_control.runtime_registrations DROP COLUMN isolation;
DROP TABLE hubuum_control.application_control;
