-- Schema installation preserves existing workers. Drain older workers before
-- enabling checkpoint producers: older binaries cannot read the waiting status.
-- Bound validation/index scans and roll back on contention.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE tasks
    ADD COLUMN task_checkpoint TEXT,
    ADD COLUMN wait_token UUID;

ALTER TABLE tasks DROP CONSTRAINT tasks_status_check; -- hubuum-compat: widen-enum-check
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check CHECK (
    status IN ('queued','validating','running','waiting','succeeded','failed','partially_succeeded','cancelled')
) NOT VALID;
ALTER TABLE tasks VALIDATE CONSTRAINT tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_wait_shape CHECK (
    (status = 'waiting') = (wait_token IS NOT NULL)
    AND (wait_token IS NULL OR wait_token <> '00000000-0000-0000-0000-000000000000'::UUID)
    AND (status <> 'waiting' OR task_checkpoint IS NOT NULL)
    AND (task_checkpoint IS NULL OR (
        kind IN ('export','backup')
        AND status IN ('queued','validating','running','waiting')
        AND attempt_count > 0 AND started_at IS NOT NULL AND finished_at IS NULL
        AND execution_deadline_at IS NOT NULL
        AND import_effects_committed_at IS NULL AND remote_dispatched_at IS NULL
        AND octet_length(task_checkpoint) BETWEEN 1 AND 65536
        AND jsonb_typeof(task_checkpoint::JSONB) = 'object'
        AND jsonb_typeof(task_checkpoint::JSONB->'format') = 'string'
        AND (task_checkpoint::JSONB->>'format') ~ '^[a-z0-9][a-z0-9._-]{0,63}$'
        AND jsonb_typeof(task_checkpoint::JSONB->'version') = 'number'
        AND (task_checkpoint::JSONB->>'version') ~ '^[1-9][0-9]*$'
        AND (task_checkpoint::JSONB->>'version')::NUMERIC <= 4294967295
        AND jsonb_typeof(task_checkpoint::JSONB->'data') = 'object'
        AND (task_checkpoint::JSONB - ARRAY['format','version','data']) = '{}'::JSONB
    ) IS TRUE)
    AND (status <> 'waiting' OR (lease_token IS NULL AND lease_expires_at IS NULL))
) NOT VALID;
ALTER TABLE tasks VALIDATE CONSTRAINT tasks_wait_shape;

-- Every terminal path, including principal disablement, physically removes the
-- private checkpoint. Accounting observes the cleared bytes in the same write.
CREATE FUNCTION hubuum_clear_terminal_task_checkpoint() RETURNS TRIGGER
LANGUAGE plpgsql SET search_path = pg_catalog AS $function$
BEGIN
    IF NEW.status IN ('succeeded','failed','partially_succeeded','cancelled') THEN
        NEW.task_checkpoint := NULL;
        NEW.wait_token := NULL;
    END IF;
    RETURN NEW;
END
$function$;
CREATE TRIGGER task_checkpoint_cleanup BEFORE INSERT OR UPDATE ON tasks
FOR EACH ROW EXECUTE FUNCTION hubuum_clear_terminal_task_checkpoint();

CREATE INDEX idx_tasks_active_capacity_with_waiting ON tasks (submitted_by, kind)
WHERE submitted_by IS NOT NULL AND deleted_at IS NULL
    AND status IN ('queued','validating','running','waiting'); -- hubuum-compat: bounded-transactional-index
CREATE INDEX tasks_execution_deadline_waiting_idx ON tasks (execution_deadline_at, id)
WHERE status IN ('queued','validating','running','waiting') AND execution_deadline_at IS NOT NULL; -- hubuum-compat: bounded-transactional-index

CREATE OR REPLACE FUNCTION hubuum_account_task() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE candidate hubuum_control.task_accounting; previous hubuum_control.task_accounting;
    current_generation BIGINT; policy JSONB; rejection TEXT; identity UUID; restoring BOOLEAN; reconciling BOOLEAN;
BEGIN
    restoring := current_setting('hubuum.restore_identities',TRUE) IS NOT DISTINCT FROM 'on'
        AND pg_has_role(session_user,current_user,'MEMBER');
    IF restoring THEN RETURN NULL; END IF;
    reconciling := current_setting('hubuum.reconcile_task_capacity',TRUE) IS NOT DISTINCT FROM 'on'
        AND pg_has_role(session_user,current_user,'MEMBER');
    IF TG_OP='UPDATE' AND (NEW.status,NEW.request_payload,NEW.task_checkpoint,NEW.lease_token,NEW.processed_items)
        IS NOT DISTINCT FROM (OLD.status,OLD.request_payload,OLD.task_checkpoint,OLD.lease_token,OLD.processed_items) THEN RETURN NULL; END IF;
    IF TG_OP='DELETE' THEN
        UPDATE hubuum_control.task_accounting SET task_id=NULL,outstanding=FALSE,lease_token=NULL,resident_input_bytes=0,resident_output_bytes=0
        WHERE task_identity=(OLD.logical_identity #>> '{subject,identity,identity}')::UUID;
        RETURN NULL;
    END IF;
    identity := (NEW.logical_identity #>> '{subject,identity,identity}')::UUID;
    IF identity IS NULL THEN RAISE EXCEPTION 'task_capacity_identity_unavailable'; END IF;
    IF TG_OP='UPDATE' AND NEW.status NOT IN ('queued','validating','running','waiting')
        AND (COALESCE(octet_length(NEW.request_payload::TEXT),0)::BIGINT + COALESCE(octet_length(NEW.task_checkpoint),0)) <= (COALESCE(octet_length(OLD.request_payload::TEXT),0)::BIGINT + COALESCE(octet_length(OLD.task_checkpoint),0)) THEN
        -- Terminalization/redaction only releases capacity. Preserve work
        -- history and serialize on this owner without taking the policy lock.
        UPDATE hubuum_control.task_accounting SET outstanding=FALSE,lease_token=NULL,
            resident_input_bytes=(COALESCE(octet_length(NEW.request_payload::TEXT),0)::BIGINT + COALESCE(octet_length(NEW.task_checkpoint),0)),
            cumulative_items=cumulative_items+GREATEST(NEW.processed_items-processed_items,0),
            processed_items=NEW.processed_items
        WHERE task_identity=identity AND task_id=NEW.id;
        IF FOUND THEN RETURN NULL; END IF;
    END IF;
    PERFORM public.hubuum_task_capacity_lock();
    SELECT p.generation,p.policy INTO STRICT current_generation,policy FROM hubuum_control.task_policy p WHERE singleton;
    SELECT * INTO previous FROM hubuum_control.task_accounting WHERE task_identity=identity;
    candidate := previous;
    IF candidate.task_identity IS NULL THEN
        candidate.task_identity := identity;
        candidate.principal_identity := (NEW.logical_identity #>> '{references,/submitted_by,reference,identity}')::UUID;
        IF NEW.status IN ('queued','validating','running','waiting') AND NEW.submitted_by IS NOT NULL AND candidate.principal_identity IS NULL THEN
            RAISE EXCEPTION 'task_capacity_identity_unavailable';
        END IF;
        candidate.collection_identities := CASE WHEN NEW.status IN ('queued','validating','running','waiting') THEN public.hubuum_task_collection_identities(NEW.logical_identity) ELSE '{}' END;
        candidate.kind := NEW.kind;
        candidate.internal := NEW.kind IN ('reindex','schema_validation');
        candidate.identity_resolved := TRUE;
        candidate.resident_output_bytes := 0;
        candidate.cumulative_input_bytes := 0;
        candidate.cumulative_output_bytes := 0;
        candidate.cumulative_items := 0;
        candidate.processed_items := NEW.processed_items;
    END IF;
    candidate.task_id := NEW.id;
    candidate.generation := current_generation;
    candidate.outstanding := NEW.status IN ('queued','validating','running','waiting');
    candidate.lease_token := CASE WHEN NEW.status IN ('validating','running') THEN NEW.lease_token ELSE NULL END;
    candidate.resident_input_bytes := (COALESCE(octet_length(NEW.request_payload::TEXT),0)::BIGINT + COALESCE(octet_length(NEW.task_checkpoint),0));
    IF candidate.lease_token IS NOT NULL AND candidate.lease_token IS DISTINCT FROM previous.lease_token THEN
        candidate.cumulative_input_bytes := candidate.cumulative_input_bytes + candidate.resident_input_bytes;
        IF candidate.cumulative_input_bytes > (policy->>'cumulative_input_bytes_per_task')::BIGINT THEN
            RAISE EXCEPTION 'task_capacity_cumulative_work';
        END IF;
    END IF;
    candidate.cumulative_items := candidate.cumulative_items + GREATEST(NEW.processed_items-candidate.processed_items,0);
    candidate.processed_items := NEW.processed_items;
    IF NOT reconciling AND candidate.outstanding AND NOT COALESCE(previous.outstanding,FALSE)
        AND candidate.cumulative_input_bytes::NUMERIC+candidate.resident_input_bytes > (policy->>'cumulative_input_bytes_per_task')::BIGINT THEN
        RAISE EXCEPTION 'task_capacity_cumulative_work';
    END IF;
    IF NOT reconciling AND (
       (candidate.outstanding AND NOT COALESCE(previous.outstanding,FALSE))
       OR (candidate.lease_token IS NOT NULL AND previous.lease_token IS NULL)
       OR candidate.resident_input_bytes > COALESCE(previous.resident_input_bytes,0)) THEN
        rejection := public.hubuum_task_capacity_rejection(candidate,
            candidate.outstanding AND NOT COALESCE(previous.outstanding,FALSE),
            candidate.lease_token IS NOT NULL AND previous.lease_token IS NULL,
            candidate.resident_input_bytes > COALESCE(previous.resident_input_bytes,0));
        IF rejection IS NOT NULL THEN RAISE EXCEPTION '%',rejection; END IF;
    END IF;
    INSERT INTO hubuum_control.task_accounting SELECT candidate.*
    ON CONFLICT (task_identity) DO UPDATE SET task_id=EXCLUDED.task_id,outstanding=EXCLUDED.outstanding,
        lease_token=EXCLUDED.lease_token,generation=EXCLUDED.generation,resident_input_bytes=EXCLUDED.resident_input_bytes,
        cumulative_input_bytes=EXCLUDED.cumulative_input_bytes,cumulative_items=EXCLUDED.cumulative_items,processed_items=EXCLUDED.processed_items;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION hubuum_backfill_task_capacity(batch_limit INTEGER) RETURNS INTEGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE task public.tasks; identity UUID; principal UUID; collections UUID[]; resolved BOOLEAN;
    current_generation BIGINT; changed INTEGER := 0;
BEGIN
    IF NOT pg_has_role(session_user,current_user,'MEMBER') THEN RAISE EXCEPTION 'Owner role required'; END IF;
    IF batch_limit NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'Task backfill batch must be between 1 and 1000'; END IF;
    PERFORM public.hubuum_task_capacity_lock();
    SELECT generation INTO STRICT current_generation FROM hubuum_control.task_policy WHERE singleton;
    FOR task IN SELECT t.* FROM public.tasks t
        WHERE NOT EXISTS (SELECT 1 FROM hubuum_control.task_accounting a WHERE a.task_identity=(t.logical_identity #>> '{subject,identity,identity}')::UUID)
        ORDER BY t.id FOR UPDATE OF t SKIP LOCKED LIMIT batch_limit LOOP
        identity := (task.logical_identity #>> '{subject,identity,identity}')::UUID;
        IF identity IS NULL THEN RAISE EXCEPTION 'task_capacity_identity_unavailable'; END IF;
        principal := (task.logical_identity #>> '{references,/submitted_by,reference,identity}')::UUID;
        resolved := task.submitted_by IS NULL OR principal IS NOT NULL;
        collections := '{}';
        IF task.status IN ('queued','validating','running','waiting') THEN
            BEGIN
                collections := public.hubuum_task_collection_identities(task.logical_identity);
            EXCEPTION WHEN raise_exception THEN
                IF SQLERRM <> 'task_capacity_identity_unavailable' THEN RAISE; END IF;
                resolved := FALSE;
            END;
        END IF;
        INSERT INTO hubuum_control.task_accounting
            (task_identity,task_id,principal_identity,collection_identities,kind,internal,identity_resolved,
             outstanding,lease_token,generation,resident_input_bytes,resident_output_bytes,processed_items,cumulative_items)
        VALUES (identity,task.id,principal,collections,task.kind,task.kind IN ('reindex','schema_validation'),resolved,
            task.status IN ('queued','validating','running','waiting'),task.lease_token,current_generation,
            (COALESCE(octet_length(task.request_payload::TEXT),0)::BIGINT + COALESCE(octet_length(task.task_checkpoint),0)),public.hubuum_task_output_bytes(task.id),task.processed_items,task.processed_items);
        changed := changed+1;
    END LOOP;
    RETURN changed;
END
$function$;

CREATE OR REPLACE FUNCTION hubuum_reconcile_task_capacity() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE task public.tasks; identity UUID; current_generation BIGINT;
BEGIN
    IF NOT pg_has_role(session_user,current_user,'MEMBER') THEN RAISE EXCEPTION 'Owner role required'; END IF;
    PERFORM public.hubuum_task_capacity_lock();
    UPDATE hubuum_control.task_policy SET generation=generation+1 WHERE singleton RETURNING generation INTO current_generation;
    UPDATE hubuum_control.task_accounting SET task_id=NULL,outstanding=FALSE,lease_token=NULL,resident_input_bytes=0,resident_output_bytes=0;
    FOR task IN SELECT * FROM public.tasks ORDER BY id LOOP
        identity := (task.logical_identity #>> '{subject,identity,identity}')::UUID;
        IF identity IS NULL THEN RAISE EXCEPTION 'task_capacity_identity_unavailable'; END IF;
        -- Only terminal tasks are currently restorable. New repair work is
        -- admitted after reconciliation using fresh identities and leases.
        IF task.status IN ('queued','validating','running','waiting') OR task.lease_token IS NOT NULL THEN
            RAISE EXCEPTION 'task_capacity_reconciliation_required';
        END IF;
        INSERT INTO hubuum_control.task_accounting
            (task_identity,task_id,principal_identity,kind,internal,outstanding,generation,resident_input_bytes,resident_output_bytes,processed_items)
        VALUES (identity,task.id,(task.logical_identity #>> '{references,/submitted_by,reference,identity}')::UUID,
            task.kind,task.kind IN ('reindex','schema_validation'),FALSE,current_generation,
            (COALESCE(octet_length(task.request_payload::TEXT),0)::BIGINT + COALESCE(octet_length(task.task_checkpoint),0)),public.hubuum_task_output_bytes(task.id),task.processed_items)
        ON CONFLICT(task_identity) DO UPDATE SET task_id=EXCLUDED.task_id,generation=EXCLUDED.generation,
            resident_input_bytes=EXCLUDED.resident_input_bytes,resident_output_bytes=EXCLUDED.resident_output_bytes,processed_items=EXCLUDED.processed_items;
    END LOOP;
END
$function$;
