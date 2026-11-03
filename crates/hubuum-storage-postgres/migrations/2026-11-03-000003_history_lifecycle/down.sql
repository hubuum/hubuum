DO $rollback_guard$
DECLARE retained_members BOOLEAN := FALSE;
BEGIN
    IF to_regclass('hubuum_control.history_expiry_members') IS NOT NULL THEN
        EXECUTE 'SELECT EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members)' INTO retained_members;
    END IF;
    -- Revision alone is insufficient: an issued review can exist at revision
    -- zero. Normalize only the documented absent expiry default, preserving
    -- every policy, hold, preview, receipt and unknown future field.
    IF EXISTS(SELECT 1 FROM hubuum_control.history_lifecycle_control
        WHERE jsonb_set(material, '{authority,expiry}',
            coalesce(material #> '{authority,expiry}', '[]'::JSONB)) IS DISTINCT FROM
            '{"authority":{"revision":"0","policies":[],"holds":[],"expiry":[]},"previews":[]}'::JSONB)
       OR retained_members
       OR EXISTS(SELECT 1 FROM hubuum_control.history_publication_owners)
       OR EXISTS(SELECT 1 FROM hubuum_control.history_publication_control WHERE generation<>0) THEN
        RAISE EXCEPTION 'Retained history lifecycle authority prevents rollback; preserve independent control recovery and roll forward';
    END IF;
END
$rollback_guard$;

CREATE OR REPLACE FUNCTION public.hubuum_rotate_application_for_restore()
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE authority JSONB; empty_authority BOOLEAN; task_activation JSONB;
BEGIN
    SELECT material INTO STRICT authority FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE;
    UPDATE hubuum_control.deletion_authority SET material=material WHERE id=1;
    SELECT activation INTO STRICT task_activation FROM hubuum_control.task_retention_control WHERE id=1 FOR UPDATE;
    UPDATE hubuum_control.task_retention_control SET reapplied_incarnation=NULL,reapplied_revision=NULL WHERE id=1;
    empty_authority := authority->'intents'='{}'::jsonb AND task_activation->>'state'='disabled';
    UPDATE hubuum_control.application_control SET incarnation=gen_random_uuid(),reapplication_revision=NULL,
        released_revision=CASE WHEN empty_authority THEN (authority->>'revision')::bigint END,
        checkpoint=NULL,pending_restore=NOT empty_authority WHERE id=1;
    RETURN empty_authority;
END $$;


CREATE OR REPLACE FUNCTION public.hubuum_task_control_capture() RETURNS JSON
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $$
    WITH position AS MATERIALIZED (SELECT pg_current_wal_insert_lsn() AS lsn)
    SELECT json_build_object('task_control',public.hubuum_task_control_read(),
        'deletion',d.material,'application',public.hubuum_application_control_read(),
        'system_identifier',(pg_control_system()).system_identifier::text,
        'wal_file',pg_walfile_name(position.lsn),'lsn',position.lsn::text,
        'task_reapplication',CASE WHEN c.reapplied_revision<=c.revision AND c.reapplied_incarnation=a.incarnation
           THEN json_build_object('incarnation',c.reapplied_incarnation,'task_revision',c.reapplied_revision) END)
    FROM hubuum_control.deletion_authority d CROSS JOIN hubuum_control.task_retention_control c
    CROSS JOIN hubuum_control.application_control a CROSS JOIN position
    WHERE d.id=1 AND c.id=1 AND a.id=1
$$;

CREATE OR REPLACE FUNCTION public.hubuum_register_runtime_isolated(registration UUID, ownership UUID, isolation_domain JSONB)
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
DROP FUNCTION IF EXISTS public.hubuum_register_history_runtime(UUID,UUID,JSONB);
DROP FUNCTION IF EXISTS public.hubuum_history_runtime_register(UUID,UUID,JSONB,SMALLINT);
ALTER TABLE hubuum_control.runtime_registrations DROP COLUMN IF EXISTS history_protocol;

DROP FUNCTION IF EXISTS public.hubuum_history_requested_as_of_visible(TEXT,UUID,TIMESTAMPTZ);
DROP FUNCTION IF EXISTS public.hubuum_history_as_of_visible(TEXT,JSONB,TIMESTAMPTZ,TIMESTAMPTZ);
DROP FUNCTION IF EXISTS public.hubuum_history_version_visible(TEXT,JSONB);
DROP FUNCTION IF EXISTS public.hubuum_history_expiry_sync();
DROP TABLE IF EXISTS hubuum_control.history_expiry_members;

DROP FUNCTION public.hubuum_history_lifecycle_commit(JSONB,JSONB,UUID,BIGINT);
DROP FUNCTION public.hubuum_history_lifecycle_capture();
DROP FUNCTION IF EXISTS public.hubuum_history_producers_fenced(UUID,BIGINT);
DROP FUNCTION public.hubuum_history_read_authority(UUID,UUID,BOOLEAN);
DROP FUNCTION public.hubuum_history_publication_generation();
DROP FUNCTION public.hubuum_history_publication_admit(UUID,UUID,TEXT,UUID,UUID,UUID,BIGINT,BIGINT,BIGINT);
DROP FUNCTION public.hubuum_history_publication_execution(UUID,UUID,UUID,BIGINT,BIGINT,BIGINT);
DROP FUNCTION public.hubuum_history_publication_settle(UUID,UUID);
DROP TABLE hubuum_control.history_publication_owners;
DROP TABLE hubuum_control.history_publication_control;
DROP TABLE hubuum_control.history_lifecycle_control;
