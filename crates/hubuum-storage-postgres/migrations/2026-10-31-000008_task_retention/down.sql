SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';
LOCK TABLE hubuum_control.deletion_authority IN ACCESS EXCLUSIVE MODE;
LOCK TABLE hubuum_control.task_retention_control IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM hubuum_control.task_retention_control WHERE activation->>'state'<>'disabled' OR revision<>0)
       OR EXISTS (SELECT 1 FROM hubuum_control.task_receipts)
       OR EXISTS (SELECT 1 FROM hubuum_control.task_purge_batches)
       OR EXISTS (SELECT 1 FROM hubuum_control.task_retention_holds)
       OR EXISTS (SELECT 1 FROM public.tasks WHERE receipt_generation IS NOT NULL OR retention_policy_revision IS NOT NULL OR details_purge_pending OR recovery_unavailable) THEN
        RAISE EXCEPTION 'Cannot downgrade retained task receipts, retention decisions or admission bindings';
    END IF;
END $$;
DROP INDEX public.task_retention_terminal_settlement;
DROP FUNCTION public.hubuum_task_receipt_settlement_pending(INTEGER);
DROP FUNCTION public.hubuum_task_purge_execute(JSONB,JSONB,INTEGER,BIGINT);
ALTER FUNCTION public.hubuum_guard_artifact_attempt() RESET search_path;
ALTER FUNCTION public.hubuum_guard_artifact_chunk() RESET search_path;
DROP FUNCTION public.hubuum_task_purges_pending(INTEGER);
DROP FUNCTION public.hubuum_task_purge_prepare(UUID,TEXT,INTEGER);
DROP FUNCTION public.hubuum_task_purge_block(UUID,TEXT,TIMESTAMPTZ);
DROP FUNCTION public.hubuum_task_receipts_expire(BIGINT, TIMESTAMPTZ, JSONB, BIGINT, UUID, UUID);
DROP FUNCTION public.hubuum_task_receipts_expired(INTEGER);
DROP FUNCTION public.hubuum_task_admissions_pending(INTEGER);
CREATE OR REPLACE FUNCTION public.hubuum_rotate_application_for_restore()
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
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
DROP TRIGGER task_retention_policy ON public.tasks;
DROP FUNCTION public.hubuum_pin_task_retention_policy();
DROP FUNCTION public.hubuum_task_receipt_activate(INTEGER, UUID, UUID, BIGINT, BIGINT, UUID, UUID, TEXT);
DROP FUNCTION public.hubuum_task_receipt_settle(INTEGER, BIGINT, TEXT);
DROP FUNCTION public.hubuum_task_receipt_admit(INTEGER, BIGINT, TEXT, TEXT);
DROP TRIGGER task_retention_guard ON public.class_computation_state;
DROP TRIGGER task_retention_guard ON public.schema_repair_reports;
DROP TRIGGER task_retention_guard ON public.schema_impact_findings;
DROP TRIGGER task_retention_guard ON public.schema_validation_work;
DROP TRIGGER task_retention_guard ON public.remote_call_results;
DROP TRIGGER task_retention_guard ON public.schema_profile_outputs;
DROP TRIGGER task_retention_guard ON public.backup_task_outputs;
DROP TRIGGER task_retention_guard ON public.export_task_outputs;
DROP TRIGGER task_retention_guard ON public.import_task_results;
DROP FUNCTION public.hubuum_guard_task_detail_write();
-- Restore the scheduler before removing receipt eligibility helpers.
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
DROP FUNCTION public.hubuum_task_admission_allowed(INTEGER);
DROP FUNCTION public.hubuum_task_outputs_visible(INTEGER);
DROP FUNCTION public.hubuum_task_details_visible(INTEGER);
DROP FUNCTION public.hubuum_task_receipt_for_task(UUID);
DROP FUNCTION public.hubuum_task_receipt_read(UUID, TEXT);
DROP FUNCTION public.hubuum_task_artifact_cleanup_allowed(UUID);
DROP FUNCTION public.hubuum_task_retention_lock();
DROP FUNCTION public.hubuum_task_control_capture();
DROP FUNCTION public.hubuum_task_control_read();
DROP FUNCTION public.hubuum_task_retention_read();
DROP INDEX public.import_task_results_purge;
DROP INDEX public.task_claim_order;
DROP INDEX public.task_stable_identity;
DROP INDEX public.task_retention_eligible;
DROP INDEX public.task_legacy_submission_key;
ALTER TABLE public.tasks ADD CONSTRAINT tasks_submitted_by_idempotency_key_key UNIQUE(submitted_by,idempotency_key);
ALTER TABLE public.tasks DROP CONSTRAINT task_receipt_admission_pair;
ALTER TABLE public.tasks DROP COLUMN recovery_unavailable;
ALTER TABLE public.tasks DROP COLUMN details_purge_pending;
ALTER TABLE public.tasks DROP COLUMN detail_expires_at;
ALTER TABLE public.tasks DROP COLUMN retention_policy_revision;
ALTER TABLE public.tasks DROP COLUMN admission_event;
ALTER TABLE public.tasks DROP COLUMN receipt_checkpoint;
ALTER TABLE public.tasks DROP COLUMN receipt_legacy;
ALTER TABLE public.tasks DROP COLUMN receipt_protection_deadline;
ALTER TABLE public.tasks DROP COLUMN receipt_created_revision;
ALTER TABLE public.tasks DROP COLUMN receipt_incarnation;
ALTER TABLE public.tasks DROP COLUMN receipt_generation;
DROP TABLE hubuum_control.task_purge_targets;
DROP TABLE hubuum_control.task_purge_batches;
DROP TABLE hubuum_control.task_retention_holds;
DROP TABLE hubuum_control.task_receipts;
DROP TABLE hubuum_control.task_retention_policies;
DROP TABLE hubuum_control.task_retention_control;
