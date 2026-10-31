SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

-- Protected cleanup calls these invoker triggers from a pg_catalog-only
-- capability. Resolve their existing unqualified application tables explicitly;
-- keep temporary schemas last so they cannot shadow the protected relations.
ALTER FUNCTION public.hubuum_guard_artifact_attempt() SET search_path = pg_catalog, public, pg_temp;
ALTER FUNCTION public.hubuum_guard_artifact_chunk() SET search_path = pg_catalog, public, pg_temp;

-- This authority is deliberately outside the logical restore replacement set.
-- Runtime access is restricted to the operation-shaped functions below.
CREATE TABLE hubuum_control.task_retention_control (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    hold_release_revision BIGINT NOT NULL DEFAULT 0 CHECK (hold_release_revision >= 0 AND hold_release_revision <= revision),
    activation JSONB NOT NULL DEFAULT '{"state":"disabled"}',
    receipt_expiry_floor TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 UTC',
    current_policy_revision BIGINT,
    receipt_count INTEGER NOT NULL DEFAULT 0 CHECK (receipt_count BETWEEN 0 AND 16384),
    receipt_bytes BIGINT NOT NULL DEFAULT 0 CHECK (receipt_bytes BETWEEN 0 AND 12582912),
    retention_bytes BIGINT NOT NULL DEFAULT 0 CHECK (retention_bytes BETWEEN 0 AND 4194304),
    reapplied_incarnation UUID,
    reapplied_revision BIGINT,
    CHECK ((reapplied_incarnation IS NULL) = (reapplied_revision IS NULL))
);
INSERT INTO hubuum_control.task_retention_control (id) VALUES (1);

CREATE TABLE hubuum_control.task_retention_policies (
    revision BIGINT PRIMARY KEY CHECK (revision >= 0),
    material JSONB NOT NULL,
    encoded_bytes INTEGER NOT NULL CHECK (encoded_bytes > 0)
);
INSERT INTO hubuum_control.task_retention_policies(revision,material,encoded_bytes)
    SELECT 0, policy::jsonb, octet_length(policy) FROM (VALUES
    ('{"revision":0,"deduplication_after_terminal_secs":604800,"details_after_terminal_secs":null,"pending_protection_timeout_secs":300,"max_receipts":16384,"max_receipt_bytes":12582912,"cleanup_budget":{"max_rows":1000,"max_bytes":16777216}}')) initial(policy);
UPDATE hubuum_control.task_retention_control SET current_policy_revision=0,
    retention_bytes=(SELECT octet_length(jsonb_build_object('policies',jsonb_build_object('0',material),'holds','{}'::jsonb,'purge_batches','{}'::jsonb)::text) FROM hubuum_control.task_retention_policies WHERE revision=0) WHERE id=1;
CREATE TABLE hubuum_control.task_receipts (
    task_identity UUID PRIMARY KEY CHECK (task_identity <> '00000000-0000-0000-0000-000000000000'),
    principal_identity UUID NOT NULL CHECK (principal_identity <> '00000000-0000-0000-0000-000000000000'),
    protected_key TEXT NOT NULL CHECK (protected_key ~ '^[0-9a-f]{64}$'),
    generation UUID NOT NULL UNIQUE CHECK (generation <> '00000000-0000-0000-0000-000000000000'),
    material JSONB NOT NULL,
    encoded_bytes INTEGER NOT NULL CHECK (encoded_bytes BETWEEN 1 AND 1024),
    expires_at TIMESTAMPTZ,
    UNIQUE (principal_identity, protected_key)
);
CREATE INDEX task_receipts_expiry ON hubuum_control.task_receipts (expires_at, task_identity)
    WHERE expires_at IS NOT NULL; -- hubuum-compat: bounded-transactional-index
CREATE INDEX task_receipts_recovery ON hubuum_control.task_receipts(task_identity)
    WHERE material #>> '{outcome,state}'='may_execute'; -- hubuum-compat: bounded-transactional-index
CREATE TABLE hubuum_control.task_retention_holds (
    id UUID PRIMARY KEY,
    task_identity UUID NOT NULL,
    material JSONB NOT NULL,
    encoded_bytes INTEGER NOT NULL CHECK (encoded_bytes > 0)
);
CREATE INDEX task_retention_holds_task ON hubuum_control.task_retention_holds (task_identity); -- hubuum-compat: bounded-transactional-index
CREATE TABLE hubuum_control.task_purge_batches (
    id UUID PRIMARY KEY,
    last_attempted_at TIMESTAMPTZ,
    material JSONB NOT NULL,
    encoded_bytes INTEGER NOT NULL CHECK (encoded_bytes > 0)
);
CREATE TABLE hubuum_control.task_purge_targets (
    task_identity UUID NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('task_details','expired_outputs')),
    batch_id UUID NOT NULL REFERENCES hubuum_control.task_purge_batches(id) ON DELETE RESTRICT,
    PRIMARY KEY(task_identity,scope)
);
CREATE INDEX task_purge_targets_batch ON hubuum_control.task_purge_targets (batch_id); -- hubuum-compat: bounded-transactional-index
REVOKE ALL ON hubuum_control.task_retention_control, hubuum_control.task_retention_policies,
    hubuum_control.task_receipts, hubuum_control.task_retention_holds,
    hubuum_control.task_purge_batches, hubuum_control.task_purge_targets FROM PUBLIC;

-- Replaceable admission bookkeeping is not part of signed receipt material.
ALTER TABLE public.tasks ADD COLUMN receipt_generation UUID;
ALTER TABLE public.tasks ADD COLUMN receipt_incarnation UUID;
ALTER TABLE public.tasks ADD COLUMN receipt_created_revision BIGINT;
ALTER TABLE public.tasks ADD COLUMN receipt_protection_deadline TIMESTAMP;
ALTER TABLE public.tasks ADD COLUMN receipt_checkpoint TEXT;
ALTER TABLE public.tasks ADD COLUMN receipt_legacy BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE public.tasks ADD COLUMN admission_event JSONB;
ALTER TABLE public.tasks ADD COLUMN retention_policy_revision BIGINT;
ALTER TABLE public.tasks ADD COLUMN detail_expires_at TIMESTAMP;
ALTER TABLE public.tasks ADD COLUMN details_purge_pending BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE public.tasks ADD COLUMN recovery_unavailable BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE public.tasks ADD CONSTRAINT task_receipt_admission_pair CHECK (
    ((receipt_generation IS NULL AND receipt_incarnation IS NULL AND receipt_created_revision IS NULL
       AND receipt_protection_deadline IS NULL AND receipt_checkpoint IS NULL)
     OR (receipt_generation IS NOT NULL AND receipt_incarnation IS NOT NULL
       AND receipt_created_revision > 0 AND receipt_protection_deadline IS NOT NULL
       AND (receipt_checkpoint IS NULL OR receipt_checkpoint ~ '^[0-9a-f]{64}$'))) IS TRUE
) NOT VALID;
ALTER TABLE public.tasks VALIDATE CONSTRAINT task_receipt_admission_pair;
ALTER TABLE public.tasks DROP CONSTRAINT tasks_submitted_by_idempotency_key_key; -- hubuum-compat: reviewed-offline-drop
CREATE UNIQUE INDEX task_legacy_submission_key ON public.tasks (submitted_by, idempotency_key)
    WHERE receipt_generation IS NULL AND idempotency_key IS NOT NULL; -- hubuum-compat: bounded-transactional-index
CREATE INDEX task_retention_eligible ON public.tasks (detail_expires_at, id)
    WHERE detail_expires_at IS NOT NULL; -- hubuum-compat: bounded-transactional-index
CREATE INDEX task_stable_identity ON public.tasks ((logical_identity #>> '{subject,identity,identity}')); -- hubuum-compat: bounded-transactional-index
CREATE INDEX task_claim_order ON public.tasks (status, kind, created_at, id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX import_task_results_purge ON public.import_task_results (task_id, id); -- hubuum-compat: bounded-transactional-index

CREATE FUNCTION public.hubuum_task_retention_read() RETURNS JSONB
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT jsonb_build_object('revision', c.revision, 'activation', c.activation,
       'receipt_expiry_floor', c.receipt_expiry_floor, 'policy', p.material,
       'receipt_count', c.receipt_count, 'receipt_bytes', c.receipt_bytes,
       'retention_bytes', c.retention_bytes,
       'lineage', d.material->'lineage', 'epoch', d.material->'epoch')
    FROM hubuum_control.task_retention_control c
    CROSS JOIN hubuum_control.deletion_authority d
    LEFT JOIN hubuum_control.task_retention_policies p ON p.revision=c.current_policy_revision
    WHERE c.id=1 AND d.id=1
$$;
REVOKE ALL ON FUNCTION public.hubuum_task_retention_read() FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_retention_lock() RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id=1 FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Deletion authority is unavailable'; END IF;
    PERFORM 1 FROM hubuum_control.task_retention_control WHERE id=1 FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Task retention authority is unavailable'; END IF;
    RETURN public.hubuum_task_retention_read();
END $$;
REVOKE ALL ON FUNCTION public.hubuum_task_retention_lock() FROM PUBLIC;

-- Called under the retention lock before ordinary artifact locks. Published
-- task results require protected purge; abandoned staging retains its existing
-- lease-fenced cleanup path without bypassing a newly committed hold.
CREATE FUNCTION public.hubuum_task_artifact_cleanup_allowed(artifact UUID) RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT c.activation->>'state'='disabled' OR NOT EXISTS (
        SELECT 1 FROM public.artifact_attempts a JOIN public.artifact_task_owners o USING(artifact_id)
        WHERE a.artifact_id=artifact AND (a.manifest_sha256 IS NOT NULL OR EXISTS (
            SELECT 1 FROM hubuum_control.task_retention_holds h WHERE h.task_identity=o.task_identity
            AND h.material->>'released_at' IS NULL
            AND (h.material->>'expires_at' IS NULL OR (h.material->>'expires_at')::timestamptz>clock_timestamp()))))
    FROM hubuum_control.task_retention_control c WHERE c.id=1
$$;
REVOKE ALL ON FUNCTION public.hubuum_task_artifact_cleanup_allowed(UUID) FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_control_read() RETURNS JSONB
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT jsonb_build_object('lineage',d.material->'lineage','epoch',d.material->'epoch',
        'revision',c.revision,'hold_release_revision',c.hold_release_revision,'activation',c.activation,'receipt_expiry_floor',c.receipt_expiry_floor,
        'receipts',COALESCE((SELECT jsonb_agg(material ORDER BY principal_identity,protected_key) FROM hubuum_control.task_receipts),'[]'::jsonb),
        'policies',COALESCE((SELECT jsonb_object_agg(revision::text,material) FROM hubuum_control.task_retention_policies),'{}'::jsonb),
        'holds',COALESCE((SELECT jsonb_object_agg(id::text,material) FROM hubuum_control.task_retention_holds),'{}'::jsonb),
        'purge_batches',COALESCE((SELECT jsonb_object_agg(id::text,material) FROM hubuum_control.task_purge_batches),'{}'::jsonb))
    FROM hubuum_control.task_retention_control c CROSS JOIN hubuum_control.deletion_authority d
    WHERE c.id=1 AND d.id=1
$$;
REVOKE ALL ON FUNCTION public.hubuum_task_control_read() FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_control_capture() RETURNS JSONB
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $$
    WITH position AS MATERIALIZED (SELECT pg_current_wal_insert_lsn() AS lsn)
    SELECT jsonb_build_object('task_control',public.hubuum_task_control_read(),
        'deletion',d.material,'application',public.hubuum_application_control_read(),
        'system_identifier',(pg_control_system()).system_identifier::text,
        'wal_file',pg_walfile_name(position.lsn),'lsn',position.lsn::text,
        'task_reapplication',CASE WHEN c.reapplied_revision<=c.revision AND c.reapplied_incarnation=a.incarnation
           THEN jsonb_build_object('incarnation',c.reapplied_incarnation,'task_revision',c.reapplied_revision) END)
    FROM hubuum_control.deletion_authority d CROSS JOIN hubuum_control.task_retention_control c
    CROSS JOIN hubuum_control.application_control a CROSS JOIN position
    WHERE d.id=1 AND c.id=1 AND a.id=1
$$;
REVOKE ALL ON FUNCTION public.hubuum_task_control_capture() FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_receipt_read(owner UUID, protected_key TEXT) RETURNS JSONB
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT material FROM hubuum_control.task_receipts
    WHERE principal_identity=owner AND task_receipts.protected_key=$2
$$;
REVOKE ALL ON FUNCTION public.hubuum_task_receipt_read(UUID, TEXT) FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_receipt_for_task(task UUID) RETURNS JSONB
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT material FROM hubuum_control.task_receipts WHERE task_identity=task
$$;
REVOKE ALL ON FUNCTION public.hubuum_task_receipt_for_task(UUID) FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_details_visible(task INTEGER) RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT EXISTS (SELECT 1 FROM public.tasks t WHERE t.id=task AND NOT t.details_purge_pending AND NOT t.recovery_unavailable
        AND NOT EXISTS (SELECT 1 FROM hubuum_control.task_purge_targets p
            WHERE p.scope='task_details' AND p.task_identity=(t.logical_identity #>> '{subject,identity,identity}')::uuid))
$$;
REVOKE ALL ON FUNCTION public.hubuum_task_details_visible(INTEGER) FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_outputs_visible(task INTEGER) RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT public.hubuum_task_details_visible(task) AND NOT EXISTS (
        SELECT 1 FROM hubuum_control.task_purge_targets p JOIN public.tasks t
          ON p.task_identity=(t.logical_identity #>> '{subject,identity,identity}')::uuid
        WHERE t.id=task AND p.scope='expired_outputs')
$$;
REVOKE ALL ON FUNCTION public.hubuum_task_outputs_visible(INTEGER) FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_admission_allowed(task INTEGER) RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT EXISTS (SELECT 1 FROM public.tasks t CROSS JOIN hubuum_control.application_control a
        CROSS JOIN hubuum_control.task_retention_control c
        WHERE t.id=task AND a.id=1 AND c.id=1 AND public.hubuum_task_details_visible(t.id)
        AND ((t.receipt_generation IS NULL AND (t.idempotency_key IS NULL OR c.activation->>'state'='disabled'))
          OR (t.receipt_checkpoint IS NOT NULL AND c.activation->>'state'='protected'
            AND t.receipt_incarnation=a.incarnation AND EXISTS (
                SELECT 1 FROM hubuum_control.task_receipts r
                WHERE r.task_identity=(t.logical_identity #>> '{subject,identity,identity}')::uuid
                  AND r.generation=t.receipt_generation
                  AND r.material #>> '{outcome,state}'='may_execute'))))
$$;
REVOKE ALL ON FUNCTION public.hubuum_task_admission_allowed(INTEGER) FROM PUBLIC;

CREATE FUNCTION public.hubuum_guard_task_detail_write() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE task INTEGER; parent public.tasks;
BEGIN
    IF current_setting('hubuum.restore_identities', TRUE) IS NOT DISTINCT FROM 'on'
       AND pg_has_role(session_user, current_user, 'MEMBER') THEN RETURN NEW; END IF;
    task := (to_jsonb(NEW)->>CASE WHEN TG_TABLE_NAME='class_computation_state' THEN 'active_task_id' ELSE 'task_id' END)::INTEGER;
    IF task IS NULL THEN RETURN NEW; END IF;
    -- Purge preparation and removal both take FOR UPDATE on the parent.
    -- KEY SHARE excludes those operations without blocking cancellation or
    -- lease renewal while an import still holds uncommitted child receipts.
    SELECT * INTO parent FROM public.tasks WHERE id=task FOR KEY SHARE;
    IF NOT FOUND OR parent.details_purge_pending OR NOT public.hubuum_task_details_visible(task)
       OR (TG_TABLE_NAME IN ('export_task_outputs','backup_task_outputs','schema_profile_outputs')
           AND NOT public.hubuum_task_outputs_visible(task)) THEN
        RAISE EXCEPTION 'Task detail retention prevents new references' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_guard_task_detail_write() FROM PUBLIC;
CREATE TRIGGER task_retention_guard BEFORE INSERT OR UPDATE ON public.import_task_results
    FOR EACH ROW EXECUTE FUNCTION public.hubuum_guard_task_detail_write();
CREATE TRIGGER task_retention_guard BEFORE INSERT OR UPDATE ON public.export_task_outputs
    FOR EACH ROW EXECUTE FUNCTION public.hubuum_guard_task_detail_write();
CREATE TRIGGER task_retention_guard BEFORE INSERT OR UPDATE ON public.backup_task_outputs
    FOR EACH ROW EXECUTE FUNCTION public.hubuum_guard_task_detail_write();
CREATE TRIGGER task_retention_guard BEFORE INSERT OR UPDATE ON public.schema_profile_outputs
    FOR EACH ROW EXECUTE FUNCTION public.hubuum_guard_task_detail_write();
CREATE TRIGGER task_retention_guard BEFORE INSERT OR UPDATE ON public.remote_call_results
    FOR EACH ROW EXECUTE FUNCTION public.hubuum_guard_task_detail_write();
CREATE TRIGGER task_retention_guard BEFORE INSERT OR UPDATE ON public.schema_validation_work
    FOR EACH ROW EXECUTE FUNCTION public.hubuum_guard_task_detail_write();
CREATE TRIGGER task_retention_guard BEFORE INSERT OR UPDATE ON public.schema_impact_findings
    FOR EACH ROW EXECUTE FUNCTION public.hubuum_guard_task_detail_write();
CREATE TRIGGER task_retention_guard BEFORE INSERT OR UPDATE ON public.schema_repair_reports
    FOR EACH ROW EXECUTE FUNCTION public.hubuum_guard_task_detail_write();
CREATE TRIGGER task_retention_guard BEFORE INSERT OR UPDATE OF active_task_id ON public.class_computation_state
    FOR EACH ROW EXECUTE FUNCTION public.hubuum_guard_task_detail_write();

CREATE FUNCTION public.hubuum_task_receipt_admit(task INTEGER, expected_revision BIGINT,
    canonical TEXT, key_fingerprint TEXT) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE c hubuum_control.task_retention_control; t public.tasks; policy JSONB;
    r JSONB := canonical::jsonb; bytes INTEGER := octet_length(canonical);
    identity UUID; owner UUID; generation UUID; image UUID;
BEGIN
    PERFORM public.hubuum_task_retention_lock();
    SELECT * INTO STRICT c FROM hubuum_control.task_retention_control WHERE id=1;
    IF c.revision<>expected_revision OR c.activation->>'state'<>'protected'
       OR c.activation->>'key_fingerprint' IS DISTINCT FROM key_fingerprint THEN
        RAISE EXCEPTION 'Task receipt authority changed or is unavailable' USING ERRCODE='40001';
    END IF;
    SELECT * INTO STRICT t FROM public.tasks WHERE id=task FOR UPDATE;
    SELECT incarnation INTO STRICT image FROM hubuum_control.application_control WHERE id=1;
    SELECT material INTO STRICT policy FROM hubuum_control.task_retention_policies WHERE revision=c.current_policy_revision;
    identity := (r #>> '{ticket,receipt,task}')::uuid;
    owner := (r #>> '{ticket,receipt,submission,principal}')::uuid;
    generation := (r #>> '{ticket,receipt,generation}')::uuid;
    IF (bytes BETWEEN 1 AND 1024 AND t.status='queued' AND t.receipt_generation IS NULL
        AND t.lease_token IS NULL AND NOT t.details_purge_pending
        AND r->>'kind'=t.kind AND r #>> '{outcome,state}'='may_execute'
        AND identity::text=t.logical_identity #>> '{subject,identity,identity}'
        AND owner::text=t.logical_identity #>> '{references,/submitted_by,reference,identity}'
        AND (r #>> '{ticket,created_revision}')::bigint=c.revision+1
        AND (r #>> '{ticket,admitted_incarnation}')::uuid=image
        AND (r->>'policy_revision')::bigint=c.current_policy_revision
        AND (r->>'window_secs')::bigint=(policy->>'deduplication_after_terminal_secs')::bigint
        AND r->'details_after_terminal_secs' IS NOT DISTINCT FROM policy->'details_after_terminal_secs'
        AND (r->>'admitted_at')::timestamptz=t.created_at AT TIME ZONE 'UTC'
        AND (r #>> '{ticket,protection_deadline}')::timestamptz=
            (t.created_at AT TIME ZONE 'UTC') + make_interval(secs=>(policy->>'pending_protection_timeout_secs')::double precision)
        AND r->'fingerprint'->>'version'='canonical_v1'
        AND r->'fingerprint'->>'digest' ~ '^[0-9a-f]{64}$') IS NOT TRUE THEN
        RAISE EXCEPTION 'Task receipt does not match the exact pending task admission' USING ERRCODE='23514';
    END IF;
    -- Reserve each complete record envelope, including list framing, so a
    -- terminal/failure settlement never requires new receipt capacity.
    IF c.receipt_count >= (policy->>'max_receipts')::integer
       OR 2+(c.receipt_count::bigint+1)*1025 > (policy->>'max_receipt_bytes')::bigint THEN
        RAISE EXCEPTION 'Task receipt capacity is exhausted' USING ERRCODE='53100';
    END IF;
    INSERT INTO hubuum_control.task_receipts(task_identity,principal_identity,protected_key,generation,material,encoded_bytes)
        VALUES(identity,owner,r #>> '{ticket,receipt,submission,protected_key}',generation,r,bytes);
    UPDATE hubuum_control.task_retention_control SET revision=revision+1,
        receipt_count=receipt_count+1,receipt_bytes=receipt_bytes+bytes WHERE id=1;
    UPDATE public.tasks SET receipt_generation=generation,receipt_incarnation=image,
        receipt_created_revision=c.revision+1,
        receipt_protection_deadline=(r #>> '{ticket,protection_deadline}')::timestamptz AT TIME ZONE 'UTC',
        retention_policy_revision=c.current_policy_revision,idempotency_key=NULL WHERE id=task;
    RETURN TRUE;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_task_receipt_admit(INTEGER, BIGINT, TEXT, TEXT) FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_receipt_settle(task INTEGER, expected_revision BIGINT, canonical TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE c hubuum_control.task_retention_control; t public.tasks; previous hubuum_control.task_receipts;
    next JSONB := canonical::jsonb; bytes INTEGER := octet_length(canonical); expiry TIMESTAMPTZ;
BEGIN
    PERFORM public.hubuum_task_retention_lock();
    SELECT * INTO STRICT c FROM hubuum_control.task_retention_control WHERE id=1;
    IF c.revision<>expected_revision THEN RAISE EXCEPTION 'Task receipt authority changed' USING ERRCODE='40001'; END IF;
    SELECT * INTO STRICT t FROM public.tasks WHERE id=task FOR UPDATE;
    SELECT * INTO STRICT previous FROM hubuum_control.task_receipts
        WHERE task_identity=(t.logical_identity #>> '{subject,identity,identity}')::uuid;
    IF previous.material=next THEN RETURN FALSE; END IF;
    IF (bytes BETWEEN 1 AND 1024 AND previous.generation=t.receipt_generation
        AND (previous.material-'outcome')=(next-'outcome')
        AND previous.material #>> '{outcome,state}'='may_execute'
        AND next #>> '{outcome,state}'='terminal'
        AND t.status IN ('succeeded','partially_succeeded','failed','cancelled')
        AND next #>> '{outcome,status}'=t.status
        AND (next #>> '{outcome,finished_at}')::timestamptz=t.finished_at AT TIME ZONE 'UTC'
        AND ((t.kind<>'remote_call' AND next #>> '{outcome,remote_side_effect}' IS NULL)
          OR (t.kind='remote_call' AND next #>> '{outcome,remote_side_effect}'=CASE WHEN t.remote_dispatched_at IS NOT NULL THEN 'possibly_sent' WHEN t.execution_deadline_at IS NOT NULL THEN 'not_sent' ELSE 'legacy_unknown' END))) IS NOT TRUE THEN
        RAISE EXCEPTION 'Task receipt settlement does not match the immutable terminal outcome' USING ERRCODE='23514';
    END IF;
    expiry := (next #>> '{outcome,finished_at}')::timestamptz + make_interval(secs=>(next->>'window_secs')::double precision);
    UPDATE hubuum_control.task_receipts SET material=next,encoded_bytes=bytes,expires_at=expiry
        WHERE task_identity=previous.task_identity;
    UPDATE hubuum_control.task_retention_control SET revision=revision+1,
        receipt_bytes=receipt_bytes-previous.encoded_bytes+bytes WHERE id=1;
    UPDATE public.tasks SET detail_expires_at=CASE WHEN next->>'details_after_terminal_secs' IS NULL THEN NULL ELSE
        t.finished_at+make_interval(secs=>(next->>'details_after_terminal_secs')::double precision) END
        WHERE id=task;
    RETURN TRUE;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_task_receipt_settle(INTEGER, BIGINT, TEXT) FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_receipt_activate(task INTEGER, generation UUID, image UUID,
    created_revision BIGINT, covered_revision BIGINT, lineage UUID, epoch UUID, checkpoint TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE c hubuum_control.task_retention_control; t public.tasks; authority JSONB; receipt JSONB;
BEGIN
    PERFORM public.hubuum_task_retention_lock();
    SELECT * INTO STRICT c FROM hubuum_control.task_retention_control WHERE id=1;
    SELECT material INTO STRICT authority FROM hubuum_control.deletion_authority WHERE id=1;
    SELECT * INTO STRICT t FROM public.tasks WHERE id=task FOR UPDATE;
    SELECT material INTO receipt FROM hubuum_control.task_receipts
        WHERE task_identity=(t.logical_identity #>> '{subject,identity,identity}')::uuid
          AND task_receipts.generation=$2;
    IF (c.activation->>'state' IN ('backfilling','protected') AND receipt IS NOT NULL
        AND t.receipt_generation=$2 AND t.receipt_incarnation=image
        AND t.receipt_created_revision=created_revision AND covered_revision>=created_revision
        AND covered_revision<=c.revision AND authority->>'lineage'=lineage::text
        AND authority->>'epoch'=epoch::text AND checkpoint ~ '^[0-9a-f]{64}$'
        AND EXISTS (SELECT 1 FROM hubuum_control.application_control a WHERE a.id=1 AND a.incarnation=image)
        AND public.hubuum_deletion_exposure_allowed()) IS NOT TRUE THEN
        RAISE EXCEPTION 'Task admission generation is no longer activatable' USING ERRCODE='40001';
    END IF;
    IF t.receipt_checkpoint IS NOT NULL THEN RETURN FALSE; END IF;
    IF receipt #>> '{outcome,state}' IS DISTINCT FROM 'may_execute'
       OR NOT (t.status='queued' OR (t.receipt_legacy AND t.status IN ('validating','running'))) THEN
        RAISE EXCEPTION 'Task admission already settled without activation' USING ERRCODE='23514';
    END IF;
    IF t.receipt_protection_deadline <= clock_timestamp() AT TIME ZONE 'UTC' THEN
        RAISE EXCEPTION 'Task admission protection deadline expired' USING ERRCODE='23514';
    END IF;
    UPDATE public.tasks SET receipt_checkpoint=checkpoint WHERE id=task;
    RETURN TRUE;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_task_receipt_activate(INTEGER, UUID, UUID, BIGINT, BIGINT, UUID, UUID, TEXT) FROM PUBLIC;

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

CREATE FUNCTION public.hubuum_pin_task_retention_policy() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE policy JSONB;
BEGIN
    IF current_setting('hubuum.restore_identities',TRUE) IS NOT DISTINCT FROM 'on'
       AND pg_has_role(session_user,current_user,'MEMBER') THEN RETURN NEW; END IF;
    IF TG_OP='INSERT' AND NEW.retention_policy_revision IS NULL THEN
        SELECT current_policy_revision INTO NEW.retention_policy_revision
          FROM hubuum_control.task_retention_control WHERE id=1 AND activation->>'state'='protected';
    END IF;
    IF NEW.finished_at IS NOT NULL AND NEW.retention_policy_revision IS NOT NULL THEN
        SELECT material INTO STRICT policy FROM hubuum_control.task_retention_policies WHERE revision=NEW.retention_policy_revision;
        NEW.detail_expires_at := CASE WHEN policy->>'details_after_terminal_secs' IS NULL THEN NULL ELSE
            NEW.finished_at+make_interval(secs=>(policy->>'details_after_terminal_secs')::double precision) END;
    END IF;
    RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_pin_task_retention_policy() FROM PUBLIC;
CREATE TRIGGER task_retention_policy BEFORE INSERT OR UPDATE OF finished_at ON public.tasks
    FOR EACH ROW EXECUTE FUNCTION public.hubuum_pin_task_retention_policy();

CREATE FUNCTION public.hubuum_task_receipts_expired(batch_limit INTEGER) RETURNS SETOF JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF batch_limit<1 OR batch_limit>1000 THEN RAISE EXCEPTION 'Invalid receipt expiry batch bound'; END IF;
    RETURN QUERY SELECT material FROM hubuum_control.task_receipts
       WHERE expires_at<=clock_timestamp() ORDER BY expires_at,task_identity LIMIT batch_limit;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_task_receipts_expired(INTEGER) FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_admissions_pending(batch_limit INTEGER) RETURNS SETOF JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF batch_limit<1 OR batch_limit>64 THEN RAISE EXCEPTION 'Invalid pending admission batch bound'; END IF;
    RETURN QUERY SELECT r.material->'ticket' FROM hubuum_control.task_receipts r
       JOIN public.tasks t ON t.receipt_generation=r.generation
       JOIN hubuum_control.application_control a ON a.id=1 AND a.incarnation=t.receipt_incarnation
       WHERE t.receipt_checkpoint IS NULL AND r.material #>> '{outcome,state}'='may_execute'
         AND t.status IN ('queued','validating','running')
         AND t.receipt_protection_deadline>clock_timestamp() AT TIME ZONE 'UTC'
       ORDER BY r.principal_identity,r.protected_key LIMIT batch_limit;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_task_admissions_pending(INTEGER) FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_receipts_expire(selected_revision BIGINT, expiry_floor TIMESTAMPTZ,
    targets JSONB, covered_revision BIGINT, lineage UUID, epoch UUID) RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE c hubuum_control.task_retention_control; authority JSONB; target JSONB;
    receipt hubuum_control.task_receipts; removed_count INTEGER := 0; removed_bytes BIGINT := 0;
BEGIN
    PERFORM public.hubuum_task_retention_lock();
    SELECT * INTO STRICT c FROM hubuum_control.task_retention_control WHERE id=1;
    SELECT material INTO STRICT authority FROM hubuum_control.deletion_authority WHERE id=1;
    IF (selected_revision<=covered_revision AND covered_revision<=c.revision
        AND authority->>'lineage'=lineage::text AND authority->>'epoch'=epoch::text
        AND expiry_floor<=clock_timestamp() AND jsonb_array_length(targets) BETWEEN 1 AND 1000) IS NOT TRUE THEN
        RAISE EXCEPTION 'Receipt expiry protection does not cover the selected authority' USING ERRCODE='40001';
    END IF;
    FOR target IN SELECT value FROM jsonb_array_elements(targets) LOOP
        SELECT * INTO receipt FROM hubuum_control.task_receipts
          WHERE principal_identity=(target #>> '{submission,principal}')::uuid
            AND protected_key=target #>> '{submission,protected_key}';
        IF NOT FOUND OR receipt.generation IS DISTINCT FROM (target->>'generation')::uuid THEN
            IF c.receipt_expiry_floor>=expiry_floor THEN CONTINUE; END IF;
            RAISE EXCEPTION 'Receipt expiry generation changed' USING ERRCODE='40001';
        END IF;
        IF (receipt.task_identity=(target->>'task')::uuid AND receipt.expires_at<=expiry_floor) IS NOT TRUE THEN
            RAISE EXCEPTION 'Receipt expiry requires the exact fixed terminal anchor' USING ERRCODE='23514';
        END IF;
        DELETE FROM hubuum_control.task_receipts WHERE task_identity=receipt.task_identity;
        removed_count:=removed_count+1;
        removed_bytes:=removed_bytes+receipt.encoded_bytes;
    END LOOP;
    IF removed_count=0 AND c.receipt_expiry_floor>=expiry_floor THEN RETURN c.revision; END IF;
    UPDATE hubuum_control.task_retention_control SET revision=revision+1,
        receipt_expiry_floor=GREATEST(receipt_expiry_floor,expiry_floor),
        receipt_count=receipt_count-removed_count,receipt_bytes=receipt_bytes-removed_bytes WHERE id=1;
    RETURN c.revision+1;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_task_receipts_expire(BIGINT, TIMESTAMPTZ, JSONB, BIGINT, UUID, UUID) FROM PUBLIC;

-- Child attachment guards pin the parent row. Cleanup deliberately uses a fresh
-- READ COMMITTED view after that pin; an inherited historical snapshot cannot
-- establish absence of a child which committed while the parent was locked.
CREATE FUNCTION public.hubuum_task_purge_block(task UUID, scope TEXT, cutoff TIMESTAMPTZ)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE t public.tasks;
BEGIN
    IF EXISTS (SELECT 1 FROM hubuum_control.task_retention_holds h WHERE h.task_identity=task
        AND h.material->>'released_at' IS NULL
        AND (h.material->>'expires_at' IS NULL OR (h.material->>'expires_at')::timestamptz>clock_timestamp())) THEN RETURN 'held'; END IF;
    IF EXISTS (SELECT 1 FROM public.artifact_task_owners o JOIN public.artifact_attempts a USING(artifact_id)
        WHERE o.task_identity=task AND a.state='published' AND o.output_expires_at>cutoff) THEN RETURN 'output_available'; END IF;
    IF (SELECT count(*) FROM public.tasks WHERE logical_identity #>> '{subject,identity,identity}'=task::text)>1 THEN
        RETURN 'unknown_identity';
    END IF;
    SELECT * INTO t FROM public.tasks WHERE logical_identity #>> '{subject,identity,identity}'=task::text;
    IF NOT FOUND THEN RETURN NULL; END IF;
    IF t.logical_identity #>> '{subject,kind}' IS DISTINCT FROM 'task'
       OR t.logical_identity #>> '{subject,identity,state}' IS DISTINCT FROM 'known' THEN RETURN 'unknown_identity'; END IF;
    IF t.status NOT IN ('succeeded','failed','partially_succeeded','cancelled') OR t.finished_at IS NULL THEN RETURN 'nonterminal'; END IF;
    IF t.lease_token IS NOT NULL OR EXISTS (SELECT 1 FROM hubuum_control.task_execution_claims WHERE task_id=t.id)
       OR EXISTS (SELECT 1 FROM public.class_computation_state WHERE active_task_id=t.id)
       OR EXISTS (SELECT 1 FROM public.schema_validation_work WHERE task_id=t.id AND active) THEN RETURN 'dependency'; END IF;
    IF scope='task_details' AND EXISTS (SELECT 1 FROM public.schema_repair_reports WHERE task_id=t.id) THEN RETURN 'output_available'; END IF;
    IF EXISTS (SELECT 1 FROM public.export_task_outputs WHERE task_id=t.id AND output_expires_at>cutoff AT TIME ZONE 'UTC')
       OR EXISTS (SELECT 1 FROM public.backup_task_outputs WHERE task_id=t.id AND output_expires_at>cutoff AT TIME ZONE 'UTC')
       OR EXISTS (SELECT 1 FROM public.schema_profile_outputs WHERE task_id=t.id AND output_expires_at>cutoff AT TIME ZONE 'UTC') THEN RETURN 'output_available'; END IF;
    RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_task_purge_block(UUID,TEXT,TIMESTAMPTZ) FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_purge_prepare(batch UUID, scope TEXT, max_tasks INTEGER)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE c hubuum_control.task_retention_control; candidate public.tasks; selected UUID[] := '{}';
    identity UUID; policy BIGINT; cutoff TIMESTAMPTZ := clock_timestamp(); ticket JSONB; decision JSONB;
    bytes INTEGER; pending INTEGER; batches INTEGER;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'Task purge requires READ COMMITTED' USING ERRCODE='25001'; END IF;
    IF scope NOT IN ('task_details','expired_outputs') OR max_tasks NOT BETWEEN 1 AND 64 OR batch IS NULL
       OR batch='00000000-0000-0000-0000-000000000000' THEN RAISE EXCEPTION 'Invalid bounded task purge request' USING ERRCODE='22023'; END IF;
    PERFORM public.hubuum_task_retention_lock();
    SELECT * INTO STRICT c FROM hubuum_control.task_retention_control WHERE id=1;
    IF c.activation->>'state'<>'protected' THEN RAISE EXCEPTION 'Task retention is not activated'; END IF;
    PERFORM public.hubuum_task_capacity_lock();
    FOR candidate IN SELECT * FROM public.tasks t WHERE t.status IN ('succeeded','failed','partially_succeeded','cancelled')
        AND t.finished_at IS NOT NULL
        AND t.logical_identity #>> '{subject,identity,state}'='known'
        AND NOT EXISTS (SELECT 1 FROM hubuum_control.task_purge_targets p
            WHERE p.task_identity=(t.logical_identity #>> '{subject,identity,identity}')::uuid AND p.scope=$2)
        AND ((scope='task_details' AND t.detail_expires_at<=cutoff AT TIME ZONE 'UTC')
          OR (scope='expired_outputs' AND (
             EXISTS (SELECT 1 FROM public.export_task_outputs WHERE task_id=t.id AND output_expires_at<=cutoff AT TIME ZONE 'UTC')
             OR EXISTS (SELECT 1 FROM public.backup_task_outputs WHERE task_id=t.id AND output_expires_at<=cutoff AT TIME ZONE 'UTC')
             OR EXISTS (SELECT 1 FROM public.schema_profile_outputs WHERE task_id=t.id AND output_expires_at<=cutoff AT TIME ZONE 'UTC')
             OR EXISTS (SELECT 1 FROM public.artifact_task_owners o JOIN public.artifact_attempts a USING(artifact_id)
                 WHERE o.task_identity=(t.logical_identity #>> '{subject,identity,identity}')::uuid
                 AND (a.state<>'published' OR o.output_expires_at<=cutoff)))))
        AND public.hubuum_task_purge_block((t.logical_identity #>> '{subject,identity,identity}')::uuid,scope,cutoff) IS NULL
        ORDER BY t.id LIMIT max_tasks FOR UPDATE OF t
    LOOP
        identity := (candidate.logical_identity #>> '{subject,identity,identity}')::uuid;
        -- Recheck after locking, including children which committed while waiting.
        IF public.hubuum_task_purge_block(identity,scope,cutoff) IS NOT NULL THEN CONTINUE; END IF;
        IF policy IS NULL THEN policy:=COALESCE(candidate.retention_policy_revision,c.current_policy_revision); END IF;
        IF scope='task_details' AND candidate.retention_policy_revision IS DISTINCT FROM policy THEN CONTINUE; END IF;
        selected := array_append(selected,identity);
        -- Touch the physical tuple for both scopes. A writer using an older
        -- repeatable-read snapshot must fail rather than miss a new output
        -- tombstone after acquiring its parent lock.
        UPDATE public.tasks SET details_purge_pending=details_purge_pending OR scope='task_details'
            WHERE id=candidate.id;
    END LOOP;
    IF cardinality(selected)=0 THEN RETURN NULL; END IF;
    SELECT array_agg(value ORDER BY value) INTO selected FROM unnest(selected) value;
    ticket:=jsonb_build_object('manifest',jsonb_build_object('id',batch,'scope',scope,'tasks',to_jsonb(selected),
        'policy_revision',policy,'eligible_at',cutoff),'generation',1,'prepared_revision',c.revision+1);
    decision:=jsonb_build_object('ticket',ticket,'removed',NULL);
    bytes:=octet_length(decision::text);
    SELECT count(*),count(*) FILTER(WHERE material->>'removed' IS NULL) INTO batches,pending FROM hubuum_control.task_purge_batches;
    IF batches>=1024 OR c.retention_bytes+bytes+39+(pending+1)*512>4194304 THEN RAISE EXCEPTION 'Task retention obligation capacity is exhausted'; END IF;
    INSERT INTO hubuum_control.task_purge_batches(id,material,encoded_bytes) VALUES(batch,decision,bytes);
    INSERT INTO hubuum_control.task_purge_targets(task_identity,scope,batch_id) SELECT value,scope,batch FROM unnest(selected) value;
    UPDATE hubuum_control.task_retention_control SET revision=revision+1,retention_bytes=retention_bytes+bytes+39 WHERE id=1;
    RETURN ticket;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_task_purge_prepare(UUID,TEXT,INTEGER) FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_purges_pending(max_batches INTEGER) RETURNS SETOF JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF max_batches NOT BETWEEN 1 AND 64 THEN RAISE EXCEPTION 'Invalid task purge list bound' USING ERRCODE='22023'; END IF;
    RETURN QUERY SELECT material->'ticket' FROM hubuum_control.task_purge_batches
        WHERE material->>'removed' IS NULL ORDER BY last_attempted_at NULLS FIRST,id LIMIT max_batches;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_task_purges_pending(INTEGER) FROM PUBLIC;

CREATE FUNCTION public.hubuum_task_purge_execute(request_ticket JSONB, checkpoint JSONB, max_rows INTEGER, max_bytes BIGINT)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE c hubuum_control.task_retention_control; d JSONB; b hubuum_control.task_purge_batches;
    scope TEXT; cutoff TIMESTAMPTZ; task UUID; v_task_id INTEGER; blocker TEXT; first_block TEXT;
    relation TEXT; child RECORD; used_rows INTEGER:=0; used_bytes BIGINT:=0; remaining BOOLEAN:=FALSE;
    relations TEXT[]; old_bytes INTEGER; next_material JSONB; row_bytes BIGINT; found_parent BOOLEAN;
    artifact RECORD;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'Task purge requires READ COMMITTED' USING ERRCODE='25001'; END IF;
    IF max_rows NOT BETWEEN 1 AND 1000 OR max_bytes NOT BETWEEN 1 AND 16777216 THEN RAISE EXCEPTION 'Invalid task purge budget' USING ERRCODE='22023'; END IF;
    PERFORM public.hubuum_task_retention_lock();
    SELECT * INTO STRICT c FROM hubuum_control.task_retention_control WHERE id=1;
    SELECT material INTO STRICT d FROM hubuum_control.deletion_authority WHERE id=1;
    SELECT * INTO STRICT b FROM hubuum_control.task_purge_batches WHERE id=(request_ticket #>> '{manifest,id}')::uuid;
    -- The typed client normalizes equivalent RFC3339 spellings (+00:00/Z).
    -- Compare that timestamp as an instant; every other immutable field stays exact.
    IF (((b.material->'ticket') #- '{manifest,eligible_at}')=(request_ticket #- '{manifest,eligible_at}')
       AND (b.material #>> '{ticket,manifest,eligible_at}')::timestamptz=(request_ticket #>> '{manifest,eligible_at}')::timestamptz
       AND checkpoint->'lineage'=d->'lineage' AND checkpoint->'epoch'=d->'epoch'
       AND (checkpoint->>'task_revision')::bigint BETWEEN GREATEST((request_ticket->>'prepared_revision')::bigint,c.hold_release_revision) AND c.revision
       AND checkpoint->>'checkpoint' ~ '^[0-9a-f]{64}$') IS NOT TRUE THEN RAISE EXCEPTION 'Protected task purge coverage changed' USING ERRCODE='40001'; END IF;
    PERFORM public.hubuum_task_capacity_lock();
    UPDATE hubuum_control.task_purge_batches SET last_attempted_at=clock_timestamp() WHERE id=b.id;
    scope:=request_ticket #>> '{manifest,scope}'; cutoff:=(request_ticket #>> '{manifest,eligible_at}')::timestamptz;
    relations:=CASE WHEN scope='task_details' THEN ARRAY['import_task_results','schema_impact_findings','remote_call_results','export_task_outputs','backup_task_outputs','schema_profile_outputs','schema_validation_work']
        ELSE ARRAY['export_task_outputs','backup_task_outputs','schema_profile_outputs'] END;
    FOR task IN SELECT value::uuid FROM jsonb_array_elements_text(request_ticket #> '{manifest,tasks}') value LOOP
        v_task_id:=NULL;
        SELECT t.id INTO v_task_id FROM public.tasks t WHERE t.logical_identity #>> '{subject,identity,identity}'=task::text FOR UPDATE;
        found_parent:=FOUND;
        blocker:=public.hubuum_task_purge_block(task,scope,cutoff);
        IF blocker IS NOT NULL THEN first_block:=COALESCE(first_block,blocker); remaining:=TRUE; CONTINUE; END IF;
        -- Ownership uses the immutable task UUID and survives task deletion and
        -- restore. Drain physical obligations before removing their references.
        FOR artifact IN SELECT a.artifact_id,a.state,o.spool_bytes,o.cleanup_after
            FROM public.artifact_attempts a JOIN public.artifact_task_owners o USING(artifact_id)
            WHERE o.task_identity=task ORDER BY a.artifact_id LIMIT max_rows-used_rows FOR UPDATE OF a,o LOOP
            EXIT WHEN used_rows>=max_rows;
            IF artifact.cleanup_after>clock_timestamp() THEN
                first_block:=COALESCE(first_block,'dependency'); remaining:=TRUE; CONTINUE;
            END IF;
            IF artifact.spool_bytes>0 AND artifact.cleanup_after IS NULL THEN
                SELECT octet_length(to_jsonb(o)::text) INTO row_bytes FROM public.artifact_task_owners o WHERE artifact_id=artifact.artifact_id;
                IF row_bytes<=max_bytes-used_bytes THEN
                    -- Versioned local-spool protocol: match LOCAL_SPOOL_CLEANUP_GRACE.
                    UPDATE public.artifact_task_owners SET cleanup_after=clock_timestamp()+INTERVAL '10 seconds' WHERE artifact_id=artifact.artifact_id;
                    used_rows:=used_rows+1; used_bytes:=used_bytes+row_bytes;
                ELSE first_block:=COALESCE(first_block,'oversized_row'); END IF;
                remaining:=TRUE; CONTINUE;
            END IF;
            IF artifact.state<>'discarded' THEN
                UPDATE public.artifact_attempts SET state='deleting' WHERE artifact_id=artifact.artifact_id;
                FOR child IN SELECT chunk_row.ordinal,octet_length(to_jsonb(chunk_row)::text)::bigint AS bytes FROM public.artifact_chunks chunk_row
                    WHERE chunk_row.artifact_id=artifact.artifact_id ORDER BY chunk_row.ordinal LIMIT max_rows-used_rows FOR UPDATE LOOP
                    IF child.bytes>max_bytes-used_bytes THEN remaining:=TRUE; first_block:=COALESCE(first_block,'oversized_row'); CONTINUE; END IF;
                    DELETE FROM public.artifact_chunks WHERE artifact_id=artifact.artifact_id AND ordinal=child.ordinal;
                    used_rows:=used_rows+1; used_bytes:=used_bytes+child.bytes;
                END LOOP;
                IF EXISTS (SELECT 1 FROM public.artifact_chunks WHERE artifact_id=artifact.artifact_id) THEN
                    remaining:=TRUE; CONTINUE;
                END IF;
                SELECT octet_length(to_jsonb(a)::text) INTO row_bytes FROM public.artifact_attempts a WHERE artifact_id=artifact.artifact_id;
                IF used_rows>=max_rows OR row_bytes>max_bytes-used_bytes THEN
                    remaining:=TRUE;
                    IF row_bytes>max_bytes THEN first_block:=COALESCE(first_block,'oversized_row'); END IF;
                    CONTINUE;
                END IF;
                UPDATE public.artifact_attempts SET state='discarded',manifest=NULL WHERE artifact_id=artifact.artifact_id;
                used_rows:=used_rows+1; used_bytes:=used_bytes+row_bytes;
            END IF;
            SELECT octet_length(to_jsonb(o)::text) INTO row_bytes FROM public.artifact_task_owners o WHERE artifact_id=artifact.artifact_id;
            IF used_rows>=max_rows OR row_bytes>max_bytes-used_bytes THEN
                remaining:=TRUE;
                IF row_bytes>max_bytes THEN first_block:=COALESCE(first_block,'oversized_row'); END IF;
                CONTINUE;
            END IF;
            DELETE FROM public.artifact_task_owners WHERE artifact_id=artifact.artifact_id;
            used_rows:=used_rows+1; used_bytes:=used_bytes+row_bytes;
        END LOOP;
        IF EXISTS (SELECT 1 FROM public.artifact_task_owners WHERE task_identity=task) THEN remaining:=TRUE; CONTINUE; END IF;
        IF NOT found_parent THEN CONTINUE; END IF;
        IF scope='task_details' THEN UPDATE public.tasks SET details_purge_pending=TRUE WHERE id=v_task_id; END IF;
        FOREACH relation IN ARRAY relations LOOP
            IF relation='schema_validation_work' AND EXISTS(SELECT 1 FROM public.schema_impact_findings r WHERE r.task_id=v_task_id) THEN
                remaining:=TRUE; CONTINUE;
            END IF;
            -- Relations are a closed internal allowlist, never caller supplied.
            -- Only identities and byte counts cross into the adapter process.
            FOR child IN EXECUTE format('SELECT ctid AS row_id,octet_length(to_jsonb(r)::text)::bigint AS bytes FROM public.%I r WHERE task_id=$1 ORDER BY ctid LIMIT $2 FOR UPDATE',relation)
                USING v_task_id,max_rows-used_rows LOOP
                IF child.bytes>max_bytes-used_bytes THEN remaining:=TRUE; first_block:=COALESCE(first_block,'oversized_row'); CONTINUE; END IF;
                EXECUTE format('DELETE FROM public.%I WHERE ctid=$1',relation) USING child.row_id;
                used_rows:=used_rows+1; used_bytes:=used_bytes+child.bytes;
            END LOOP;
            EXECUTE format('SELECT EXISTS(SELECT 1 FROM public.%I WHERE task_id=$1)',relation) INTO found_parent USING v_task_id;
            IF found_parent THEN remaining:=TRUE; END IF;
            EXIT WHEN used_rows>=max_rows;
        END LOOP;
        IF scope='task_details' THEN
            -- Explicit absence checks prevent FK cascades from exceeding either budget.
            SELECT EXISTS(SELECT 1 FROM public.import_task_results r WHERE r.task_id=v_task_id)
                OR EXISTS(SELECT 1 FROM public.remote_call_results r WHERE r.task_id=v_task_id)
                OR EXISTS(SELECT 1 FROM public.export_task_outputs r WHERE r.task_id=v_task_id)
                OR EXISTS(SELECT 1 FROM public.backup_task_outputs r WHERE r.task_id=v_task_id)
                OR EXISTS(SELECT 1 FROM public.schema_profile_outputs r WHERE r.task_id=v_task_id)
                OR EXISTS(SELECT 1 FROM public.schema_validation_work r WHERE r.task_id=v_task_id)
                INTO found_parent;
            IF NOT found_parent AND used_rows<max_rows THEN
                SELECT octet_length(to_jsonb(t)::text) INTO row_bytes FROM public.tasks t WHERE id=v_task_id;
                IF row_bytes<=max_bytes-used_bytes THEN
                    DELETE FROM public.tasks WHERE id=v_task_id;
                    used_rows:=used_rows+1; used_bytes:=used_bytes+row_bytes;
                ELSE remaining:=TRUE; first_block:=COALESCE(first_block,'oversized_row'); END IF;
            ELSE remaining:=TRUE; END IF;
        END IF;
        IF used_rows>=max_rows THEN remaining:=TRUE; EXIT; END IF;
    END LOOP;
    -- A later call checks every target again, including already Removed decisions.
    IF remaining THEN
        IF used_rows=0 AND first_block IS NOT NULL THEN RETURN jsonb_build_object('state','blocked','reason',first_block); END IF;
        RETURN jsonb_build_object('state','more','rows',used_rows,'bytes',used_bytes);
    END IF;
    IF b.material->>'removed' IS NULL THEN
        old_bytes:=b.encoded_bytes; next_material:=jsonb_set(b.material,'{removed}',checkpoint);
        UPDATE hubuum_control.task_purge_batches SET material=next_material,encoded_bytes=octet_length(next_material::text) WHERE id=b.id;
        UPDATE hubuum_control.task_retention_control SET revision=revision+1,
            retention_bytes=retention_bytes-old_bytes+octet_length(next_material::text) WHERE id=1 RETURNING revision INTO c.revision;
    END IF;
    RETURN jsonb_build_object('state','removed','revision',c.revision,'rows',used_rows,'bytes',used_bytes);
END $$;
REVOKE ALL ON FUNCTION public.hubuum_task_purge_execute(JSONB,JSONB,INTEGER,BIGINT) FROM PUBLIC;

-- Internal schema/computed supersession may occur inside a class transaction.
-- Its short conservative receipt lag is reconciled under coordinator-before-task
-- ordering, preserving the original terminal clock without replaying work.
CREATE FUNCTION public.hubuum_task_receipt_settlement_pending(task INTEGER) RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
    SELECT EXISTS(SELECT 1 FROM public.tasks t JOIN hubuum_control.task_receipts r
        ON r.task_identity=(t.logical_identity #>> '{subject,identity,identity}')::uuid
        AND r.generation=t.receipt_generation WHERE t.id=task
        AND t.status IN ('succeeded','failed','partially_succeeded','cancelled')
        AND r.material #>> '{outcome,state}'='may_execute')
$$;
REVOKE ALL ON FUNCTION public.hubuum_task_receipt_settlement_pending(INTEGER) FROM PUBLIC;
CREATE INDEX task_retention_terminal_settlement ON public.tasks(finished_at,id)
    WHERE receipt_generation IS NOT NULL AND status IN ('succeeded','failed','partially_succeeded','cancelled'); -- hubuum-compat: bounded-transactional-index


-- Receipt activation and visibility are candidate eligibility checks. Skipped
-- admissions must not acquire a lease or advance either scheduling lane.
CREATE OR REPLACE FUNCTION hubuum_schedule_task(worker UUID, supported TEXT[], lease_ms BIGINT) RETURNS INTEGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE started TIMESTAMPTZ := clock_timestamp(); lock_wait BIGINT; p hubuum_control.task_policy; s hubuum_control.task_scheduler;
    settings JSONB; snapshot JSONB; selected RECORD; rotation TEXT[]; pos INTEGER; promote BOOLEAN; overdue_before TIMESTAMP;
    total_used BIGINT; ordinary_used BIGINT; internal_capable_ordinary BIGINT; ordinary_allowed BOOLEAN; reserved BIGINT;
BEGIN
    IF lease_ms <= 0 OR cardinality(supported) NOT BETWEEN 1 AND 7
        OR cardinality(supported) <> (SELECT count(DISTINCT k) FROM unnest(supported) k)
        OR NOT supported <@ ARRAY['import','export','backup','reindex','remote_call','schema_validation','schema_profiling'] THEN
        RAISE EXCEPTION 'Invalid worker registration';
    END IF;
    PERFORM public.hubuum_task_capacity_lock();
    lock_wait := (extract(epoch FROM clock_timestamp()-started)*1000000)::BIGINT;
    SELECT * INTO STRICT p FROM hubuum_control.task_policy WHERE singleton;
    SELECT * INTO STRICT s FROM hubuum_control.task_scheduler WHERE singleton;
    IF EXISTS (SELECT 1 FROM hubuum_control.task_workers w JOIN hubuum_control.task_accounting a
        ON a.task_identity=w.task_identity AND a.lease_token=w.lease_token AND a.generation=p.generation
        WHERE w.worker_identity=worker) THEN RETURN NULL; END IF;
    DELETE FROM hubuum_control.task_workers w WHERE (expires_at <= clock_timestamp() OR generation <> p.generation)
        AND NOT EXISTS (SELECT 1 FROM hubuum_control.task_accounting a WHERE a.task_identity=w.task_identity AND a.lease_token=w.lease_token AND a.generation=p.generation);
    INSERT INTO hubuum_control.task_workers(worker_identity,kinds,expires_at,generation,last_lock_wait_us)
        VALUES(worker,supported,clock_timestamp()+lease_ms*interval '1 millisecond',p.generation,
            lock_wait)
        ON CONFLICT(worker_identity) DO UPDATE SET kinds=EXCLUDED.kinds,expires_at=EXCLUDED.expires_at,generation=EXCLUDED.generation,
            task_identity=NULL,lease_token=NULL,last_lock_wait_us=EXCLUDED.last_lock_wait_us;
    snapshot := public.hubuum_task_scheduling_snapshot();
    settings := p.policy->'scheduling';
    reserved := (settings->>'internal_reserved_slots')::BIGINT;
    SELECT count(*),count(*) FILTER (WHERE NOT internal) INTO total_used,ordinary_used
        FROM hubuum_control.task_accounting WHERE generation=p.generation AND lease_token IS NOT NULL;
    IF total_used >= (snapshot->>'eligible_slots')::BIGINT THEN RETURN NULL; END IF;
    SELECT count(*) INTO internal_capable_ordinary FROM hubuum_control.task_workers w
        JOIN hubuum_control.task_accounting a ON a.task_identity=w.task_identity AND a.lease_token=w.lease_token AND a.generation=p.generation
        WHERE w.expires_at > clock_timestamp() AND w.generation=p.generation AND NOT a.internal AND w.kinds && ARRAY['reindex','schema_validation'];
    ordinary_allowed := snapshot #>> '{mode,mode}' <> 'reserved' OR (
        ordinary_used < (snapshot->>'eligible_slots')::BIGINT-reserved AND
        (NOT supported && ARRAY['reindex','schema_validation'] OR internal_capable_ordinary < (snapshot->>'internal_capable_slots')::BIGINT-reserved));
    SELECT array_agg(kind ORDER BY ordinal,weight_slot) INTO rotation
        FROM unnest(ARRAY['import','export','backup','reindex','remote_call','schema_validation','schema_profiling']) WITH ORDINALITY AS kinds(kind,ordinal)
        JOIN jsonb_array_elements(settings->'weights') w ON w->>'kind'=kind
        CROSS JOIN LATERAL generate_series(1,(w->>'weight')::INTEGER) weight_slot;
    pos := s.ordinary_position % cardinality(rotation);
    promote := s.successful_claims+1-s.last_promoted_at >= (settings->>'promotion_interval')::BIGINT;
    overdue_before := (clock_timestamp() AT TIME ZONE 'UTC')-(settings->>'overdue_seconds')::INTEGER*interval '1 second';
    IF promote THEN
        -- The existing status/created_at index proves absence cheaply. A due
        -- promotion must not force every ordinary poll to inspect all heads
        -- while the queue is still young. Eligibility is checked below.
        SELECT coalesce(min(created_at)<=overdue_before,FALSE) INTO promote
            FROM public.tasks WHERE status='queued';
    END IF;
    WITH occupied AS MATERIALIZED (
        SELECT * FROM hubuum_control.task_accounting WHERE generation=p.generation AND lease_token IS NOT NULL
    ), totals AS (SELECT count(*) FILTER (WHERE internal) AS internal,
        count(*) FILTER (WHERE NOT internal) AS ordinary FROM occupied),
    principals AS (SELECT principal_identity,count(*) AS used FROM occupied WHERE NOT internal GROUP BY principal_identity),
    kinds AS (SELECT kind,count(*) AS used FROM occupied GROUP BY kind),
    collections AS (SELECT identity FROM occupied CROSS JOIN LATERAL unnest(collection_identities) identity
        WHERE NOT internal GROUP BY identity HAVING count(*) >= (p.policy #>> '{collection,executing}')::BIGINT),
    distances AS (SELECT kind,min((ordinal-1-pos+cardinality(rotation)) % cardinality(rotation)) AS distance
        FROM unnest(rotation) WITH ORDINALITY AS kinds(kind,ordinal) GROUP BY kind),
    groups AS NOT MATERIALIZED (
        SELECT service.*,d.distance,
            CASE WHEN promote THEN 0 ELSE d.distance END AS lane_distance,
            CASE WHEN promote THEN 0 ELSE service.last_served END AS lane_served
        FROM hubuum_control.task_principal_service service
        JOIN distances d USING(kind) CROSS JOIN totals
        LEFT JOIN principals ON principals.principal_identity=service.principal_identity
        LEFT JOIN kinds ON kinds.kind=service.kind
        JOIN jsonb_array_elements(p.policy->'kinds') limits ON limits->>'kind'=service.kind
        WHERE service.kind=ANY(supported)
        AND coalesce(kinds.used,0) < (limits #>> '{capacity,executing}')::BIGINT
        AND ((service.kind IN ('reindex','schema_validation') AND totals.internal < (p.policy #>> '{internal,executing}')::BIGINT)
            OR (service.kind NOT IN ('reindex','schema_validation') AND ordinary_allowed
                AND totals.ordinary < (p.policy #>> '{deployment,executing}')::BIGINT
                AND coalesce(principals.used,0) < (p.policy #>> '{principal,executing}')::BIGINT))
        ORDER BY lane_distance,lane_served
    ) SELECT candidate.*,g.kind,g.principal_identity AS principal,g.last_served,g.distance,promotion.promoted
    INTO selected FROM groups g CROSS JOIN LATERAL (
            SELECT t.id,t.created_at FROM public.tasks t
            WHERE t.status='queued' AND t.kind=g.kind
                AND public.hubuum_task_admission_allowed(t.id)
                AND coalesce((t.logical_identity #>> '{references,/submitted_by,reference,identity}')::UUID,
                    '00000000-0000-0000-0000-000000000000'::UUID)=g.principal_identity
                -- Keep eligibility as a per-row lookup so LIMIT can stop the
                -- ordered queue index. A join can sort the entire backlog first.
                AND (SELECT a.identity_resolved AND a.lease_token IS NULL
                    AND a.cumulative_input_bytes::NUMERIC+a.resident_input_bytes <= (p.policy->>'cumulative_input_bytes_per_task')::BIGINT
                    AND (a.internal OR NOT EXISTS (SELECT 1 FROM collections c WHERE c.identity=ANY(a.collection_identities)))
                    FROM hubuum_control.task_accounting a WHERE a.task_id=t.id AND a.generation=p.generation)
            ORDER BY t.created_at,t.id FOR UPDATE OF t SKIP LOCKED LIMIT 1
        ) candidate
    CROSS JOIN LATERAL (SELECT promote AND candidate.created_at <= overdue_before AS promoted) promotion
    -- Preserve the sorted group prefix so ordinary claims can stop after the
    -- first eligible service-history tie. Promotion compares every queue head
    -- by age instead: both prefix keys are zero on those turns.
    ORDER BY g.lane_distance,g.lane_served,promotion.promoted DESC,
        CASE WHEN promotion.promoted THEN candidate.created_at END,CASE WHEN promotion.promoted THEN candidate.id END,
        g.distance,g.last_served,candidate.created_at,candidate.id LIMIT 1;
    IF NOT FOUND THEN RETURN NULL; END IF;
    UPDATE hubuum_control.task_scheduler SET successful_claims=successful_claims+1,
        promoted_claims=promoted_claims+CASE WHEN selected.promoted THEN 1 ELSE 0 END,
        last_promoted_at=CASE WHEN selected.promoted THEN successful_claims+1 ELSE last_promoted_at END,
        ordinary_position=CASE WHEN selected.promoted THEN ordinary_position ELSE (pos+selected.distance+1) % cardinality(rotation) END WHERE singleton;
    INSERT INTO hubuum_control.task_principal_service VALUES(selected.kind,selected.principal,s.successful_claims+1)
        ON CONFLICT(kind,principal_identity) DO UPDATE SET last_served=EXCLUDED.last_served;
    UPDATE hubuum_control.task_workers SET last_claim_sequence=s.successful_claims+1,last_claim_promoted=selected.promoted,
        task_identity=(SELECT task_identity FROM hubuum_control.task_accounting WHERE task_id=selected.id AND generation=p.generation)
        WHERE worker_identity=worker;
    RETURN selected.id;
END
$function$;
