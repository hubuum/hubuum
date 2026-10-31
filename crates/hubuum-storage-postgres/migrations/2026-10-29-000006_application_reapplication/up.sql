SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

CREATE TABLE hubuum_control.application_control (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    incarnation UUID NOT NULL CHECK (incarnation <> '00000000-0000-0000-0000-000000000000'),
    reapplication_revision BIGINT CHECK (reapplication_revision >= 0),
    released_revision BIGINT CHECK (released_revision >= 0),
    checkpoint TEXT CHECK (checkpoint ~ '^[0-9a-f]{64}$'),
    pending_restore BOOLEAN NOT NULL DEFAULT FALSE,
    CHECK ((checkpoint IS NULL OR (released_revision IS NOT NULL AND reapplication_revision = released_revision)) IS TRUE)
);
INSERT INTO hubuum_control.application_control (id, incarnation, released_revision)
    SELECT 1, gen_random_uuid(), CASE WHEN material->'intents' = '{}'::jsonb THEN (material->>'revision')::bigint END
    FROM hubuum_control.deletion_authority WHERE id=1;
REVOKE ALL ON hubuum_control.application_control FROM PUBLIC;
ALTER TABLE hubuum_control.runtime_registrations ADD COLUMN isolation JSONB;
ALTER TABLE hubuum_control.runtime_registrations ADD COLUMN drained_incarnation UUID;
ALTER TABLE hubuum_control.runtime_registrations ADD COLUMN drained_revision BIGINT;
ALTER TABLE hubuum_control.runtime_registrations ADD CONSTRAINT runtime_drain_pair_check
    CHECK (((drained_incarnation IS NULL AND drained_revision IS NULL) OR
            (drained_incarnation IS NOT NULL AND drained_revision >= 0)) IS TRUE) NOT VALID;
ALTER TABLE hubuum_control.runtime_registrations VALIDATE CONSTRAINT runtime_drain_pair_check;

-- Ordinary transactions lock their owner row before domain access. Retirement
-- deletes that physical tuple, fencing even older repeatable-read snapshots
-- without waiting for unrelated owners' long-running transactions.
CREATE OR REPLACE FUNCTION public.hubuum_unregister_runtime(registration UUID, ownership UUID)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id=1 FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Deletion authority is unavailable; retain runtime registration'; END IF;
    DELETE FROM hubuum_control.runtime_registrations WHERE id=registration AND ownership_token=ownership;
    RETURN NOT EXISTS (SELECT 1 FROM hubuum_control.runtime_registrations WHERE id=registration);
END $$;

CREATE FUNCTION public.hubuum_application_control_read() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT jsonb_build_object('incarnation', a.incarnation, 'revision', (d.material->>'revision')::bigint,
        'reapplication_revision', a.reapplication_revision, 'released_revision', a.released_revision,
        'checkpoint', a.checkpoint)
    FROM hubuum_control.application_control a CROSS JOIN hubuum_control.deletion_authority d
    WHERE a.id=1 AND d.id=1
$$;
REVOKE ALL ON FUNCTION public.hubuum_application_control_read() FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.hubuum_deletion_exposure_allowed() RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT COALESCE(a.released_revision = (d.material->>'revision')::bigint
        AND (d.material->'intents' = '{}'::jsonb OR
             (a.reapplication_revision = a.released_revision AND a.checkpoint IS NOT NULL)), FALSE)
    FROM hubuum_control.application_control a CROSS JOIN hubuum_control.deletion_authority d
    WHERE a.id=1 AND d.id=1
$$;
CREATE OR REPLACE FUNCTION public.hubuum_deletion_execution_allowed() RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id=1 FOR SHARE;
    RETURN public.hubuum_deletion_exposure_allowed();
END $$;

CREATE FUNCTION public.hubuum_application_execution_allowed(registration UUID, ownership UUID, image UUID, revision BIGINT)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id=1 FOR SHARE;
    IF registration IS NOT NULL OR ownership IS NOT NULL THEN
        PERFORM 1 FROM hubuum_control.runtime_registrations
          WHERE id=registration AND ownership_token=ownership FOR KEY SHARE;
        IF NOT FOUND THEN RETURN FALSE; END IF;
    END IF;
    RETURN public.hubuum_deletion_exposure_allowed()
       AND EXISTS (SELECT 1 FROM hubuum_control.application_control a CROSS JOIN hubuum_control.deletion_authority d
                   WHERE a.id=1 AND d.id=1 AND a.incarnation=image AND (d.material->>'revision')::bigint=revision);
END $$;
REVOKE ALL ON FUNCTION public.hubuum_application_execution_allowed(UUID, UUID, UUID, BIGINT) FROM PUBLIC;

CREATE FUNCTION public.hubuum_register_runtime_isolated(registration UUID, ownership UUID, isolation_domain JSONB)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id=1 FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Deletion authority is unavailable; runtime startup denied'; END IF;
    -- Serialize capacity checks without excluding ordinary authority readers.
    -- Touch the tuple so an old repeatable-read registration cannot count an
    -- obsolete registry after another registration has committed.
    UPDATE hubuum_control.application_control SET incarnation=incarnation WHERE id=1;
    IF NOT FOUND THEN RAISE EXCEPTION 'Application control is unavailable; runtime startup denied'; END IF;
    IF public.hubuum_deletion_exposure_allowed() IS NOT TRUE OR
       (SELECT count(*) FROM hubuum_control.runtime_registrations) >= 1024 THEN RETURN FALSE; END IF;
    INSERT INTO hubuum_control.runtime_registrations (id, ownership_token, isolation)
        VALUES (registration, ownership, isolation_domain);
    RETURN TRUE;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_register_runtime_isolated(UUID, UUID, JSONB) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.hubuum_register_runtime(registration UUID, ownership UUID)
RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT public.hubuum_register_runtime_isolated(registration, ownership, NULL)
$$;

CREATE FUNCTION public.hubuum_capture_application_execution(registration UUID, ownership UUID) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id=1 FOR SHARE;
    IF public.hubuum_deletion_exposure_allowed() IS NOT TRUE THEN
        RETURN NULL;
    END IF;
    UPDATE hubuum_control.runtime_registrations SET drained_incarnation=NULL, drained_revision=NULL
      WHERE id=registration AND ownership_token=ownership AND drained_revision IS NOT NULL;
    IF registration IS NOT NULL OR ownership IS NOT NULL THEN
        PERFORM 1 FROM hubuum_control.runtime_registrations
          WHERE id=registration AND ownership_token=ownership FOR KEY SHARE;
        IF NOT FOUND THEN RETURN NULL; END IF;
    END IF;
    RETURN public.hubuum_application_control_read();
END $$;
REVOKE ALL ON FUNCTION public.hubuum_capture_application_execution(UUID, UUID) FROM PUBLIC;

CREATE FUNCTION public.hubuum_acknowledge_runtime_drain(registration UUID, ownership UUID, image UUID, revision BIGINT)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id=1 FOR SHARE;
    IF public.hubuum_deletion_exposure_allowed() OR NOT EXISTS (
        SELECT 1 FROM hubuum_control.application_control a CROSS JOIN hubuum_control.deletion_authority d
        WHERE a.id=1 AND d.id=1 AND a.incarnation=image AND (d.material->>'revision')::bigint=revision) THEN RETURN FALSE; END IF;
    UPDATE hubuum_control.runtime_registrations SET drained_incarnation=image, drained_revision=revision
       WHERE id=registration AND ownership_token=ownership;
    RETURN FOUND;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_acknowledge_runtime_drain(UUID, UUID, UUID, BIGINT) FROM PUBLIC;

CREATE TABLE hubuum_control.task_execution_claims (
    task_id INTEGER PRIMARY KEY,
    lease_token UUID NOT NULL,
    incarnation UUID NOT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 0)
);
REVOKE ALL ON hubuum_control.task_execution_claims FROM PUBLIC;
CREATE FUNCTION public.hubuum_record_task_execution_claim(task INTEGER, token UUID) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE material JSONB;
BEGIN
    IF NOT public.hubuum_deletion_execution_allowed() THEN RAISE EXCEPTION 'Application is quarantined'; END IF;
    material := public.hubuum_application_control_read();
    INSERT INTO hubuum_control.task_execution_claims(task_id, lease_token, incarnation, revision)
       VALUES(task, token, (material->>'incarnation')::uuid, (material->>'revision')::bigint)
       ON CONFLICT (task_id) DO UPDATE SET lease_token=EXCLUDED.lease_token, incarnation=EXCLUDED.incarnation, revision=EXCLUDED.revision;
    RETURN material;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_record_task_execution_claim(INTEGER, UUID) FROM PUBLIC;
CREATE FUNCTION public.hubuum_task_execution_claim_allowed(task INTEGER, token UUID) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF NOT public.hubuum_deletion_execution_allowed() THEN RETURN FALSE; END IF;
    RETURN EXISTS (SELECT 1 FROM hubuum_control.task_execution_claims c
        CROSS JOIN hubuum_control.application_control a CROSS JOIN hubuum_control.deletion_authority d
        WHERE c.task_id=task AND c.lease_token=token AND a.id=1 AND d.id=1
          AND c.incarnation=a.incarnation AND c.revision=(d.material->>'revision')::bigint);
END $$;
REVOKE ALL ON FUNCTION public.hubuum_task_execution_claim_allowed(INTEGER, UUID) FROM PUBLIC;
CREATE FUNCTION public.hubuum_forget_task_execution_claim() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    DELETE FROM hubuum_control.task_execution_claims WHERE task_id=OLD.id;
    RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_forget_task_execution_claim() FROM PUBLIC;
CREATE TRIGGER task_execution_claim_delete AFTER DELETE ON public.tasks
FOR EACH ROW EXECUTE FUNCTION public.hubuum_forget_task_execution_claim();
CREATE TRIGGER task_execution_claim_settle AFTER UPDATE OF lease_token ON public.tasks
FOR EACH ROW WHEN (NEW.lease_token IS NULL) EXECUTE FUNCTION public.hubuum_forget_task_execution_claim();

-- Logical restore changes the application image; protected authority and runtime
-- ownership survive. Empty authority can safely release without a repository.
CREATE FUNCTION public.hubuum_rotate_application_for_restore() RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE authority JSONB; empty_authority BOOLEAN;
BEGIN
    SELECT material INTO STRICT authority FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE;
    UPDATE hubuum_control.deletion_authority SET material=material WHERE id=1;
    empty_authority := authority->'intents' = '{}'::jsonb;
    UPDATE hubuum_control.application_control SET incarnation=gen_random_uuid(), reapplication_revision=NULL,
        released_revision=CASE WHEN empty_authority THEN (authority->>'revision')::bigint END,
        checkpoint=NULL, pending_restore=NOT empty_authority WHERE id=1;
    RETURN empty_authority;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_rotate_application_for_restore() FROM PUBLIC;

CREATE FUNCTION public.hubuum_pending_restore_job() RETURNS bigint
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT r.id FROM public.restore_success_receipts r
    WHERE EXISTS (SELECT 1 FROM hubuum_control.application_control WHERE id=1 AND pending_restore)
    LIMIT 1
$$;
REVOKE ALL ON FUNCTION public.hubuum_pending_restore_job() FROM PUBLIC;

-- Preserve restored source provenance when copying or refreshing a version.
CREATE OR REPLACE FUNCTION public.hubuum_manage_logical_identity()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE
    layout RECORD;
    source_row JSONB := to_jsonb(NEW);
    previous_row JSONB := CASE WHEN TG_OP = 'UPDATE' THEN to_jsonb(OLD) ELSE NULL END;
    evidence JSONB;
    subject JSONB;
    class_reference JSONB;
    restored_sources JSONB;
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
        -- A history snapshot and a refresh of the same immutable version must
        -- retain the sources represented by its payload. A genuinely authored
        -- version has a new UUID and intentionally starts a new ancestry.
        evidence := COALESCE(evidence, source_row->'logical_identity');
        IF (TG_OP = 'INSERT' AND layout.mode = 'history')
            OR subject->>'version' = evidence #>> '{subject,version}' THEN
            SELECT jsonb_object_agg(key, value) INTO restored_sources
            FROM jsonb_each(COALESCE(evidence->'references', '{}'::jsonb))
            WHERE key LIKE '/restore_source_versions/%'
               OR key LIKE '/restore_unknown_source_versions/%';
        END IF;
        NEW.logical_identity := jsonb_build_object('subject', subject, 'references',
            public.hubuum_capture_identity_references(source_row, previous_row, FALSE)
                || COALESCE(restored_sources, '{}'::jsonb));
    END IF;
    RETURN NEW;
END
$function$;
