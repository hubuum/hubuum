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
       OR EXISTS(SELECT 1 FROM hubuum_control.history_archive_captures WHERE material ? 'execution')
       OR EXISTS(SELECT 1 FROM hubuum_control.history_archive_publication_claims)
       OR EXISTS(SELECT 1 FROM hubuum_control.history_archive_captures WHERE publication_claimed)
       OR EXISTS(SELECT 1 FROM hubuum_control.history_publication_owners)
       OR EXISTS(SELECT 1 FROM hubuum_control.history_publication_producers)
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
CREATE OR REPLACE FUNCTION public.hubuum_archive_history_requires_legacy_runtime() RETURNS boolean
LANGUAGE sql IMMUTABLE SECURITY DEFINER SET search_path=pg_catalog AS $$ SELECT TRUE $$;
REVOKE ALL ON FUNCTION public.hubuum_archive_history_requires_legacy_runtime() FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.hubuum_archive_history_entry(registration uuid, ownership uuid, image uuid, revision bigint)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE legacy boolean;
BEGIN
    IF registration IS NULL OR ownership IS NULL OR NOT public.hubuum_archive_history_requires_legacy_runtime()
        OR NOT public.hubuum_application_execution_allowed(registration,ownership,image,revision) THEN RETURN FALSE; END IF;
    -- No absent-column/error catch: only the explicitly supported schema can
    -- interpret an unversioned registration as Legacy.
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='hubuum_control.runtime_registrations'::regclass
               AND attname='history_protocol' AND NOT attisdropped) THEN
        EXECUTE 'SELECT history_protocol=0 FROM hubuum_control.runtime_registrations WHERE id=$1 AND ownership_token=$2 FOR SHARE'
            INTO STRICT legacy USING registration,ownership;
    ELSE
        IF (SELECT max(version) FROM public.__diesel_schema_migrations) <> '20261103000004' THEN
            RAISE EXCEPTION 'Unversioned archive runtime schema is not supported';
        END IF;
        SELECT TRUE INTO STRICT legacy FROM hubuum_control.runtime_registrations
            WHERE id=registration AND ownership_token=ownership FOR SHARE;
    END IF;
    RETURN legacy IS TRUE;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_archive_history_entry(uuid,uuid,uuid,bigint) FROM PUBLIC;


DROP FUNCTION public.hubuum_history_archive_resource_available(UUID);
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
DROP FUNCTION public.hubuum_history_publication_admit(UUID,UUID,TEXT,UUID,UUID,UUID,BIGINT,BIGINT,BIGINT,UUID);
DROP FUNCTION public.hubuum_history_publication_execution(UUID,UUID,UUID,BIGINT,BIGINT,BIGINT);
DROP FUNCTION public.hubuum_history_publication_settle(UUID,UUID);
DROP FUNCTION public.hubuum_history_producer_execution(UUID,UUID,UUID);
DROP FUNCTION public.hubuum_history_producer_admit(UUID,UUID,UUID,UUID,UUID,UUID,UUID,BIGINT,BIGINT,BIGINT);
DROP FUNCTION public.hubuum_history_producer_settle(UUID,UUID,UUID);
DROP FUNCTION public.hubuum_history_archive_publication_claim(UUID,UUID,UUID,UUID,UUID,UUID,BYTEA,BYTEA,JSONB);
DROP FUNCTION public.hubuum_history_producer_claim_execution(UUID,UUID,UUID);
DROP TABLE hubuum_control.history_archive_publication_claims;
DROP TABLE hubuum_control.history_publication_producers;
DROP TABLE hubuum_control.history_publication_owners;
DROP TABLE hubuum_control.history_publication_control;
DROP TABLE hubuum_control.history_lifecycle_control;

CREATE OR REPLACE FUNCTION public.hubuum_history_archive_capture_save(replacement jsonb, expected_work bigint,
    registration uuid, ownership uuid, image uuid, revision bigint) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE target uuid := (replacement->>'id')::uuid; encoded integer := octet_length(replacement::text)+64;
    counter hubuum_control.history_archive_capture_capacity%ROWTYPE; expired record;
BEGIN
    IF NOT public.hubuum_archive_history_entry(registration,ownership,image,revision)
        OR (replacement #>> '{fence,incarnation}')::uuid<>image OR (replacement #>> '{fence,revision}')::bigint<>revision
        OR (replacement->>'runtime')::uuid<>registration THEN RETURN FALSE; END IF;
    IF expected_work IS NOT NULL THEN
        UPDATE hubuum_control.history_archive_captures SET material=replacement
            WHERE id=target AND principal=(replacement->>'principal')::uuid AND expires_at>clock_timestamp()
            AND material-'reserved_read_bytes'=replacement-'reserved_read_bytes'
            AND (material->>'reserved_read_bytes')::bigint=expected_work
            AND (replacement->>'reserved_read_bytes')::bigint>=expected_work;
        RETURN FOUND;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('hubuum-history-archive:'||(replacement #>> '{selection,resource}'),0));
    IF COALESCE((SELECT generation FROM hubuum_control.history_archive_catalogs WHERE resource=(replacement #>> '{selection,resource}')::uuid),0)
        <>(replacement->>'generation')::bigint THEN RETURN FALSE; END IF;
    SELECT * INTO STRICT counter FROM hubuum_control.history_archive_capture_capacity WHERE id=1 FOR UPDATE;
    FOR expired IN DELETE FROM hubuum_control.history_archive_captures WHERE id IN
        (SELECT id FROM hubuum_control.history_archive_captures WHERE expires_at<=clock_timestamp() ORDER BY expires_at,id LIMIT 16 FOR UPDATE) RETURNING bytes LOOP
        counter.captures := counter.captures-1; counter.bytes := counter.bytes-expired.bytes;
    END LOOP;
    IF encoded>5242880 OR counter.captures>=1024 OR counter.bytes+encoded>1073741824 THEN
        UPDATE hubuum_control.history_archive_capture_capacity SET captures=counter.captures,bytes=counter.bytes WHERE id=1;
        RETURN FALSE;
    END IF;
    INSERT INTO hubuum_control.history_archive_captures(id,principal,expires_at,material,bytes)
        VALUES(target,(replacement->>'principal')::uuid,(replacement->>'available_until')::timestamptz,replacement,encoded);
    UPDATE hubuum_control.history_archive_capture_capacity SET captures=counter.captures+1,bytes=counter.bytes+encoded WHERE id=1;
    RETURN TRUE;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_history_archive_capture_save(jsonb,bigint,uuid,uuid,uuid,bigint) FROM PUBLIC;
ALTER TABLE hubuum_control.history_archive_captures DROP COLUMN publication_claimed;
