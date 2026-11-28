SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Policy and service history survive logical replacement. Worker leases do not.
CREATE TABLE hubuum_control.task_scheduler (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    successful_claims BIGINT NOT NULL DEFAULT 0 CHECK (successful_claims >= 0),
    promoted_claims BIGINT NOT NULL DEFAULT 0 CHECK (promoted_claims >= 0),
    last_promoted_at BIGINT NOT NULL DEFAULT 0 CHECK (last_promoted_at >= 0),
    ordinary_position INTEGER NOT NULL DEFAULT 0 CHECK (ordinary_position >= 0)
);
INSERT INTO hubuum_control.task_scheduler (singleton) VALUES (TRUE);

CREATE TABLE hubuum_control.task_principal_service (
    kind TEXT NOT NULL,
    principal_identity UUID NOT NULL,
    last_served BIGINT NOT NULL DEFAULT 0 CHECK (last_served >= 0),
    PRIMARY KEY (kind, principal_identity)
);

-- Locate a principal's oldest queued task without scanning its backlog. This
-- index is built offline; service history is protected control-plane state.
CREATE INDEX tasks_scheduler_queue ON public.tasks (
    kind, (coalesce((logical_identity #>> '{references,/submitted_by,reference,identity}')::UUID,
        '00000000-0000-0000-0000-000000000000'::UUID)), created_at, id
) WHERE status='queued'; -- hubuum-compat: bounded-transactional-index

CREATE FUNCTION hubuum_register_task_principal() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
BEGIN
    IF NEW.status='queued' THEN
        INSERT INTO hubuum_control.task_principal_service(kind,principal_identity)
        VALUES (NEW.kind,coalesce((NEW.logical_identity #>> '{references,/submitted_by,reference,identity}')::UUID,
            '00000000-0000-0000-0000-000000000000'::UUID)) ON CONFLICT DO NOTHING;
    END IF;
    RETURN NULL;
END
$function$;
CREATE TRIGGER task_scheduler_group AFTER INSERT OR UPDATE ON public.tasks
FOR EACH ROW EXECUTE FUNCTION hubuum_register_task_principal();
INSERT INTO hubuum_control.task_principal_service(kind,principal_identity)
SELECT DISTINCT kind,coalesce((logical_identity #>> '{references,/submitted_by,reference,identity}')::UUID,
    '00000000-0000-0000-0000-000000000000'::UUID) FROM public.tasks WHERE status='queued';

-- Claims inspect occupied slots, not all outstanding reservations. Admission
-- and resident-byte mutations retain their original aggregate checks.
ALTER FUNCTION hubuum_task_capacity_rejection(hubuum_control.task_accounting,BOOLEAN,BOOLEAN,BOOLEAN)
    RENAME TO hubuum_task_capacity_rejection_before_scheduling; -- hubuum-compat: reviewed-offline-function-rename
CREATE FUNCTION hubuum_task_capacity_rejection(candidate hubuum_control.task_accounting,
    check_outstanding BOOLEAN, check_execution BOOLEAN, check_resident BOOLEAN) RETURNS TEXT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE policy JSONB; occupied BIGINT; limit_value BIGINT;
BEGIN
    IF NOT check_execution OR check_outstanding OR check_resident THEN
        RETURN public.hubuum_task_capacity_rejection_before_scheduling(candidate,check_outstanding,check_execution,check_resident);
    END IF;
    SELECT p.policy INTO STRICT policy FROM hubuum_control.task_policy p WHERE singleton;
    SELECT count(*) INTO occupied FROM hubuum_control.task_accounting
        WHERE generation=candidate.generation AND lease_token IS NOT NULL
            AND task_identity<>candidate.task_identity AND internal=candidate.internal;
    IF occupied >= (policy #>> ARRAY[CASE WHEN candidate.internal THEN 'internal' ELSE 'deployment' END,'executing'])::BIGINT THEN
        RETURN CASE WHEN candidate.internal THEN 'task_capacity_internal' ELSE 'task_capacity_deployment' END;
    END IF;
    SELECT (value #>> '{capacity,executing}')::BIGINT INTO limit_value
        FROM jsonb_array_elements(policy->'kinds') WHERE value->>'kind'=candidate.kind;
    SELECT count(*) INTO occupied FROM hubuum_control.task_accounting
        WHERE generation=candidate.generation AND lease_token IS NOT NULL AND task_identity<>candidate.task_identity AND kind=candidate.kind;
    IF occupied >= limit_value THEN RETURN 'task_capacity_kind'; END IF;
    IF candidate.internal THEN RETURN NULL; END IF;
    SELECT count(*) INTO occupied FROM hubuum_control.task_accounting
        WHERE generation=candidate.generation AND lease_token IS NOT NULL AND NOT internal
            AND task_identity<>candidate.task_identity AND principal_identity=candidate.principal_identity;
    IF occupied >= (policy #>> '{principal,executing}')::BIGINT THEN RETURN 'task_capacity_principal'; END IF;
    IF EXISTS (SELECT identity FROM hubuum_control.task_accounting CROSS JOIN LATERAL unnest(collection_identities) identity
        WHERE generation=candidate.generation AND lease_token IS NOT NULL AND NOT internal
            AND task_identity<>candidate.task_identity AND identity=ANY(candidate.collection_identities)
        GROUP BY identity HAVING count(*) >= (policy #>> '{collection,executing}')::BIGINT) THEN
        RETURN 'task_capacity_collection';
    END IF;
    RETURN NULL;
END
$function$;

CREATE TABLE hubuum_control.task_workers (
    worker_identity UUID PRIMARY KEY,
    kinds TEXT[] NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    generation BIGINT NOT NULL,
    task_identity UUID,
    lease_token UUID,
    last_lock_wait_us BIGINT NOT NULL DEFAULT 0,
    last_claim_sequence BIGINT NOT NULL DEFAULT 0,
    last_claim_promoted BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX task_workers_claim ON hubuum_control.task_workers (lease_token) WHERE lease_token IS NOT NULL; -- hubuum-compat: bounded-transactional-index
CREATE INDEX task_accounting_occupied ON hubuum_control.task_accounting (generation) WHERE lease_token IS NOT NULL; -- hubuum-compat: bounded-transactional-index

UPDATE hubuum_control.task_policy SET policy = policy || jsonb_build_object('scheduling', jsonb_build_object(
    'execution_slots',64,'internal_reserved_slots',1,'promotion_interval',8,'overdue_seconds',300,
    'weights',(SELECT jsonb_agg(jsonb_build_object('kind',kind,'weight',1) ORDER BY ordinal)
        FROM unnest(ARRAY['import','export','backup','reindex','remote_call','schema_validation','schema_profiling']) WITH ORDINALITY AS kinds(kind,ordinal))
));

ALTER FUNCTION hubuum_validate_task_policy(JSONB) RENAME TO hubuum_validate_task_admission_policy; -- hubuum-compat: reviewed-offline-function-rename
CREATE FUNCTION hubuum_validate_task_policy(candidate JSONB) RETURNS VOID
LANGUAGE plpgsql SET search_path = pg_catalog AS $function$
DECLARE s JSONB; member JSONB; key TEXT; kinds TEXT[]; slots INTEGER; reserved INTEGER;
BEGIN
    PERFORM public.hubuum_validate_task_admission_policy(candidate - 'scheduling');
    s := candidate->'scheduling';
    IF jsonb_typeof(s) IS DISTINCT FROM 'object' OR (SELECT count(*) FROM jsonb_object_keys(s)) <> 5 THEN
        RAISE EXCEPTION 'Invalid scheduling policy shape';
    END IF;
    FOREACH key IN ARRAY ARRAY['execution_slots','internal_reserved_slots','promotion_interval','overdue_seconds'] LOOP
        IF jsonb_typeof(s->key) IS DISTINCT FROM 'number' OR (s->>key) !~ '^[1-9][0-9]*$'
            OR (s->>key)::NUMERIC > 604800 THEN RAISE EXCEPTION 'Invalid scheduling limit'; END IF;
    END LOOP;
    slots := (s->>'execution_slots')::INTEGER;
    reserved := (s->>'internal_reserved_slots')::INTEGER;
    IF slots > 1024 OR reserved > slots OR (slots > 1 AND reserved = slots)
        OR (s->>'promotion_interval')::INTEGER NOT BETWEEN 2 AND 1024 THEN
        RAISE EXCEPTION 'Scheduling reservation must leave ordinary capacity; one slot uses shared mode';
    END IF;
    IF jsonb_typeof(s->'weights') IS DISTINCT FROM 'array' OR jsonb_array_length(s->'weights') <> 7 THEN
        RAISE EXCEPTION 'Every task kind requires a scheduling weight';
    END IF;
    SELECT array_agg(DISTINCT value->>'kind' ORDER BY value->>'kind') INTO kinds FROM jsonb_array_elements(s->'weights');
    IF kinds IS DISTINCT FROM ARRAY['backup','export','import','reindex','remote_call','schema_profiling','schema_validation'] THEN
        RAISE EXCEPTION 'Scheduling weights must contain every kind exactly once';
    END IF;
    FOR member IN SELECT value FROM jsonb_array_elements(s->'weights') LOOP
        IF jsonb_typeof(member) IS DISTINCT FROM 'object' OR (SELECT count(*) FROM jsonb_object_keys(member)) <> 2
            OR jsonb_typeof(member->'weight') IS DISTINCT FROM 'number' OR (member->>'weight') !~ '^[1-9][0-9]*$'
            OR (member->>'weight')::NUMERIC > 32 THEN RAISE EXCEPTION 'Scheduling weight must be between 1 and 32'; END IF;
    END LOOP;
END
$function$;

CREATE FUNCTION hubuum_task_scheduling_snapshot() RETURNS JSONB
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $function$
    WITH policy AS MATERIALIZED (SELECT policy->'scheduling' AS settings,generation FROM hubuum_control.task_policy WHERE singleton),
    live AS MATERIALIZED (SELECT w.* FROM hubuum_control.task_workers w JOIN policy p USING(generation) WHERE expires_at > clock_timestamp()),
    pool AS (SELECT least(count(*),(SELECT (settings->>'execution_slots')::BIGINT FROM policy)) AS slots,
        count(*) FILTER (WHERE kinds && ARRAY['reindex','schema_validation']) AS internal,
        count(*) FILTER (WHERE kinds && ARRAY['import','export','backup','remote_call','schema_profiling']) AS ordinary FROM live),
    mode AS (SELECT CASE
        WHEN (settings->>'execution_slots')::INTEGER=1 THEN 'single_slot'
        WHEN slots <= (settings->>'internal_reserved_slots')::INTEGER THEN 'insufficient_workers'
        WHEN internal < (settings->>'internal_reserved_slots')::INTEGER THEN 'insufficient_internal_workers'
        WHEN ordinary=0 THEN 'no_ordinary_workers' END AS reason FROM pool CROSS JOIN policy)
    SELECT jsonb_build_object(
        'mode',CASE WHEN reason IS NULL THEN jsonb_build_object('mode','reserved') ELSE jsonb_build_object('mode','shared','reason',reason) END,
        'eligible_slots',slots,'internal_capable_slots',internal,
        'unsupported_kinds',(SELECT coalesce(jsonb_agg(kind),'[]') FROM unnest(ARRAY['import','export','backup','reindex','remote_call','schema_validation','schema_profiling']) kind
            WHERE NOT EXISTS (SELECT 1 FROM live WHERE kind=ANY(kinds))),
        'successful_claims',s.successful_claims,'promoted_claims',s.promoted_claims,'ordinary_position',s.ordinary_position)
    FROM hubuum_control.task_scheduler s CROSS JOIN pool CROSS JOIN mode WHERE singleton
$function$;

CREATE FUNCTION hubuum_schedule_task(worker UUID, supported TEXT[], lease_ms BIGINT) RETURNS INTEGER
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

-- The selected pending slot binds to the task's token in the same statement
-- that acquires execution. Failed updates roll back selection and registration.
CREATE FUNCTION hubuum_renew_task_worker() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
BEGIN
    IF NEW.lease_token IS NOT NULL AND NEW.lease_expires_at IS DISTINCT FROM OLD.lease_expires_at THEN
        UPDATE hubuum_control.task_workers SET expires_at=NEW.lease_expires_at AT TIME ZONE 'UTC',lease_token=NEW.lease_token
        WHERE (lease_token IS NULL OR lease_token=NEW.lease_token) AND task_identity=(NEW.logical_identity #>> '{subject,identity,identity}')::UUID;
    END IF;
    RETURN NULL;
END
$function$;
CREATE TRIGGER task_worker_heartbeat AFTER UPDATE ON tasks FOR EACH ROW EXECUTE FUNCTION hubuum_renew_task_worker();

ALTER FUNCTION hubuum_task_capacity_snapshot() RENAME TO hubuum_task_capacity_snapshot_without_scheduling; -- hubuum-compat: reviewed-offline-function-rename
CREATE FUNCTION hubuum_task_capacity_snapshot() RETURNS JSONB
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $function$
    SELECT public.hubuum_task_capacity_snapshot_without_scheduling() || jsonb_build_object('scheduling',public.hubuum_task_scheduling_snapshot())
$function$;

-- The generation join already fences old worker leases during replay. Remove
-- them as part of the same restore transaction, preserving service history.
ALTER FUNCTION hubuum_reconcile_task_capacity() RENAME TO hubuum_reconcile_task_capacity_without_scheduling; -- hubuum-compat: reviewed-offline-function-rename
CREATE FUNCTION hubuum_reconcile_task_capacity() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
BEGIN
    PERFORM public.hubuum_reconcile_task_capacity_without_scheduling();
    DELETE FROM hubuum_control.task_workers;
END
$function$;

REVOKE ALL ON TABLE hubuum_control.task_scheduler,hubuum_control.task_principal_service,hubuum_control.task_workers FROM PUBLIC;
REVOKE ALL ON FUNCTION hubuum_schedule_task(UUID,TEXT[],BIGINT),
    hubuum_task_scheduling_snapshot(),hubuum_task_capacity_snapshot(),hubuum_renew_task_worker(),hubuum_reconcile_task_capacity(),
    hubuum_register_task_principal(),
    hubuum_task_capacity_rejection(hubuum_control.task_accounting,BOOLEAN,BOOLEAN,BOOLEAN) FROM PUBLIC;
