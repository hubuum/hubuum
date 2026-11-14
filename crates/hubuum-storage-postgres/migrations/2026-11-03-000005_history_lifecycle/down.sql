DROP INDEX IF EXISTS public.tasks_history_expiry_scope_active;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

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
       OR EXISTS(SELECT 1 FROM public.tasks WHERE kind='history_expiry')
       OR EXISTS(SELECT 1 FROM hubuum_control.task_accounting WHERE kind='history_expiry')
       OR EXISTS(SELECT 1 FROM hubuum_control.task_receipts WHERE material->>'kind'='history_expiry')
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

-- No retained eighth-kind work exists after the rollback guard above.
SELECT singleton FROM hubuum_control.task_policy WHERE singleton FOR UPDATE;
UPDATE hubuum_control.task_policy SET revision=revision+1,
    policy=jsonb_set(jsonb_set(policy,'{kinds}',
        (SELECT jsonb_agg(value ORDER BY ordinal) FROM jsonb_array_elements(policy->'kinds') WITH ORDINALITY e(value,ordinal) WHERE value->>'kind'<>'history_expiry')),
        '{scheduling,weights}',
        (SELECT jsonb_agg(value ORDER BY ordinal) FROM jsonb_array_elements(policy #> '{scheduling,weights}') WITH ORDINALITY e(value,ordinal) WHERE value->>'kind'<>'history_expiry'))
WHERE singleton;
ALTER TABLE public.tasks DROP CONSTRAINT tasks_kind_check; -- hubuum-compat: widen-enum-check
ALTER TABLE public.tasks ADD CONSTRAINT tasks_kind_check CHECK (kind IN ('import','export','backup','reindex','remote_call','schema_validation','schema_profiling')) NOT VALID;
ALTER TABLE public.tasks VALIDATE CONSTRAINT tasks_kind_check;
ALTER TABLE hubuum_control.task_accounting DROP CONSTRAINT task_accounting_kind_check; -- hubuum-compat: widen-enum-check
ALTER TABLE hubuum_control.task_accounting ADD CONSTRAINT task_accounting_kind_check CHECK (kind IN ('import','export','backup','reindex','remote_call','schema_validation','schema_profiling')) NOT VALID;
ALTER TABLE hubuum_control.task_accounting VALIDATE CONSTRAINT task_accounting_kind_check;

ALTER TABLE public.tasks DROP CONSTRAINT tasks_wait_shape; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE public.tasks ADD CONSTRAINT tasks_wait_shape CHECK (
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
ALTER TABLE public.tasks VALIDATE CONSTRAINT tasks_wait_shape;

CREATE OR REPLACE FUNCTION hubuum_task_capacity_rejection(candidate hubuum_control.task_accounting,
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

CREATE OR REPLACE FUNCTION hubuum_validate_task_admission_policy(candidate JSONB) RETURNS VOID
LANGUAGE plpgsql SET search_path = pg_catalog AS $function$
DECLARE scope JSONB; member JSONB; key TEXT; kinds TEXT[];
BEGIN
    IF jsonb_typeof(candidate) IS DISTINCT FROM 'object'
       OR (SELECT count(*) FROM jsonb_object_keys(candidate)) <> 6 THEN
        RAISE EXCEPTION 'Invalid task policy shape';
    END IF;
    FOR scope IN
        SELECT candidate->name FROM unnest(ARRAY['deployment','principal','collection','internal']) AS names(name)
        UNION ALL SELECT value->'capacity' FROM jsonb_array_elements(candidate->'kinds')
    LOOP
        IF jsonb_typeof(scope) IS DISTINCT FROM 'object'
           OR (SELECT count(*) FROM jsonb_object_keys(scope)) <> 3 THEN
            RAISE EXCEPTION 'Invalid task policy scope';
        END IF;
        FOREACH key IN ARRAY ARRAY['outstanding','executing','resident_bytes'] LOOP
            IF jsonb_typeof(scope->key) IS DISTINCT FROM 'number'
               OR (scope->>key) !~ '^[1-9][0-9]*$' OR (scope->>key)::NUMERIC > 9223372036854775807 THEN
                RAISE EXCEPTION 'Invalid task policy capacity';
            END IF;
        END LOOP;
    END LOOP;
    IF jsonb_array_length(candidate->'kinds') IS DISTINCT FROM 7 THEN
        RAISE EXCEPTION 'Task policy must contain every task kind';
    END IF;
    SELECT array_agg(DISTINCT value->>'kind' ORDER BY value->>'kind') INTO kinds FROM jsonb_array_elements(candidate->'kinds');
    IF kinds IS DISTINCT FROM ARRAY['backup','export','import','reindex','remote_call','schema_profiling','schema_validation'] THEN
        RAISE EXCEPTION 'Task policy must contain every task kind exactly once';
    END IF;
    FOR member IN SELECT value FROM jsonb_array_elements(candidate->'kinds') LOOP
        IF (SELECT count(*) FROM jsonb_object_keys(member)) <> 3
           OR jsonb_typeof(member->'principal_outstanding') IS DISTINCT FROM 'number'
           OR (member->>'principal_outstanding') !~ '^[1-9][0-9]*$'
           OR (member->>'principal_outstanding')::NUMERIC > 9223372036854775807 THEN
            RAISE EXCEPTION 'Invalid per-principal kind capacity';
        END IF;
    END LOOP;
    IF jsonb_typeof(candidate->'cumulative_input_bytes_per_task') IS DISTINCT FROM 'number'
       OR (candidate->>'cumulative_input_bytes_per_task') !~ '^[1-9][0-9]*$'
       OR (candidate->>'cumulative_input_bytes_per_task')::NUMERIC > 9223372036854775807 THEN
        RAISE EXCEPTION 'Invalid cumulative task byte capacity';
    END IF;
END
$function$;

CREATE OR REPLACE FUNCTION hubuum_validate_task_policy(candidate JSONB) RETURNS VOID
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

CREATE OR REPLACE FUNCTION hubuum_task_scheduling_snapshot() RETURNS JSONB
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

CREATE OR REPLACE FUNCTION hubuum_reconcile_task_capacity_without_scheduling() RETURNS VOID
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
SELECT public.hubuum_validate_task_policy(policy) FROM hubuum_control.task_policy WHERE singleton;


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
DROP FUNCTION public.hubuum_history_policy_references(TEXT,JSONB,JSONB,JSONB);

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
        -- Both checkpoints qualify the same unversioned Legacy registry. The
        -- synchronized runtime-settings migrations can remain applied when
        -- this earlier lifecycle migration is rolled back independently.
        IF COALESCE((SELECT max(version) FROM public.__diesel_schema_migrations), '')
            NOT IN ('20261103000004', '20261111000001') THEN
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
DROP FUNCTION IF EXISTS public.hubuum_history_expiry_remove(JSONB,UUID,UUID,UUID,TEXT,UUID,BIGINT);
DROP FUNCTION IF EXISTS public.hubuum_history_expiry_clock(TIMESTAMPTZ);
DROP FUNCTION IF EXISTS public.hubuum_history_expiry_admit(JSONB,JSONB,UUID,BIGINT);
DROP FUNCTION IF EXISTS public.hubuum_history_expiry_dependency(TEXT,JSONB);
DROP FUNCTION IF EXISTS public.hubuum_history_expiry_sync();
DROP TABLE IF EXISTS hubuum_control.history_expiry_members;

DROP FUNCTION public.hubuum_history_lifecycle_commit(JSONB,JSONB,UUID,BIGINT);
DROP FUNCTION public.hubuum_history_lifecycle_capture();
DROP FUNCTION public.hubuum_history_logical_bytes_v1(JSONB);
DROP FUNCTION hubuum_control.history_logical_bytes_v1(JSONB,INTEGER);
DROP FUNCTION IF EXISTS public.hubuum_history_producers_fenced(UUID,BIGINT);
DROP FUNCTION public.hubuum_history_read_authority(UUID,UUID,BOOLEAN);
DROP FUNCTION public.hubuum_history_publication_generation();
DROP FUNCTION public.hubuum_history_publication_admit(UUID,UUID,TEXT,UUID,UUID,UUID,BIGINT,BIGINT,BIGINT,UUID);
DROP FUNCTION public.hubuum_history_publication_execution(UUID,UUID,UUID,BIGINT,BIGINT,BIGINT,TEXT,BIGINT);
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
DROP FUNCTION hubuum_control.history_read_revision(JSONB);

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

DROP INDEX public.tasks_history_expiry_recovery_active;
DROP INDEX public.tasks_history_expiry_recovery;

-- Preserve the seven-kind discovery contract while changing the expiry arm.
CREATE OR REPLACE FUNCTION valid_task_discovery(metadata jsonb, task_kind text) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE data jsonb := metadata->'data';
DECLARE target jsonb := data->'target';
DECLARE output jsonb := data->'output';
DECLARE allowed text[];
DECLARE field text;
DECLARE scalar text;
BEGIN
    IF metadata IS NULL THEN RETURN true; END IF;
    IF jsonb_typeof(metadata) IS DISTINCT FROM 'object' OR metadata->'version' IS DISTINCT FROM '1'::jsonb
       OR (metadata - ARRAY['version','data']) <> '{}'::jsonb OR jsonb_typeof(data) IS DISTINCT FROM 'object'
       OR data->>'kind' IS DISTINCT FROM task_kind THEN RETURN false; END IF;
    allowed := CASE task_kind
        WHEN 'import' THEN ARRAY['kind','dry_run','atomicity','collision_policy','permission_policy','has_failed_items']
        WHEN 'export' THEN ARRAY['kind','scope_kind','target','template_id','missing_data_policy','max_items','max_output_bytes','warning_count','truncated','output']
        WHEN 'backup' THEN ARRAY['kind','include_history','output']
        WHEN 'schema_profiling' THEN ARRAY['kind','class_id','output']
        WHEN 'schema_validation' THEN ARRAY['kind','class_id','schema_revision','work_kind']
        WHEN 'reindex' THEN ARRAY['kind','class_id','computation_revision']
        WHEN 'remote_call' THEN ARRAY['kind','remote_target_id','target']
        ELSE NULL END;
    IF allowed IS NULL OR (data - allowed) <> '{}'::jsonb THEN RETURN false; END IF;
    FOREACH field IN ARRAY ARRAY['dry_run','has_failed_items','truncated','include_history'] LOOP
        IF data->>field IS NOT NULL AND jsonb_typeof(data->field) <> 'boolean' THEN RETURN false; END IF;
    END LOOP;
    FOREACH field IN ARRAY ARRAY['class_id','remote_target_id','template_id','schema_revision','computation_revision','max_items','max_output_bytes','warning_count'] LOOP
        scalar := data->>field;
        IF scalar IS NOT NULL THEN
            IF jsonb_typeof(data->field) <> 'number' OR scalar !~ '^[0-9]+$' THEN RETURN false; END IF;
            IF scalar::numeric < (CASE WHEN field IN ('warning_count','computation_revision') THEN 0 ELSE 1 END) THEN RETURN false; END IF;
            IF field IN ('class_id','remote_target_id','template_id','warning_count') AND scalar::numeric > 2147483647 THEN RETURN false; END IF;
            IF field IN ('schema_revision','computation_revision') AND scalar::numeric > 9223372036854775807 THEN RETURN false; END IF;
            IF field IN ('max_items','max_output_bytes') AND scalar::numeric > 18446744073709551615 THEN RETURN false; END IF;
        END IF;
    END LOOP;
    IF (data->>'schema_revision' IS NOT NULL OR data->>'computation_revision' IS NOT NULL) AND data->>'class_id' IS NULL THEN RETURN false; END IF;
    IF data->>'atomicity' IS NOT NULL AND data->>'atomicity' NOT IN ('strict','best_effort') THEN RETURN false; END IF;
    IF data->>'collision_policy' IS NOT NULL AND data->>'collision_policy' NOT IN ('abort','overwrite') THEN RETURN false; END IF;
    IF data->>'permission_policy' IS NOT NULL AND data->>'permission_policy' NOT IN ('abort','continue') THEN RETURN false; END IF;
    IF data->>'missing_data_policy' IS NOT NULL AND data->>'missing_data_policy' NOT IN ('strict','null','omit') THEN RETURN false; END IF;
    IF data->>'work_kind' IS NOT NULL AND data->>'work_kind' NOT IN ('impact','revalidation') THEN RETURN false; END IF;
    IF data->>'scope_kind' IS NOT NULL AND data->>'scope_kind' NOT IN ('collections','classes','objects_in_class','class_relations','object_relations','related_objects') THEN RETURN false; END IF;
    IF target IS NOT NULL AND target <> 'null'::jsonb THEN
        IF jsonb_typeof(target) <> 'object' THEN RETURN false; END IF;
        allowed := CASE target->>'type'
            WHEN 'collection' THEN ARRAY['type','collection_id']
            WHEN 'class' THEN ARRAY['type','class_id']
            WHEN 'object' THEN ARRAY['type','class_id','object_id']
            WHEN 'class_relation' THEN ARRAY['type','relation_id']
            WHEN 'object_relation' THEN ARRAY['type','relation_id'] ELSE NULL END;
        IF allowed IS NULL OR target - allowed <> '{}'::jsonb THEN RETURN false; END IF;
        FOREACH field IN ARRAY allowed LOOP
            IF field = 'type' THEN CONTINUE; END IF;
            scalar := target->>field;
            IF target->>'type' = 'object' AND field = 'class_id' AND scalar IS NULL THEN CONTINUE; END IF;
            IF scalar IS NULL OR jsonb_typeof(target->field) <> 'number' OR scalar !~ '^[1-9][0-9]*$' OR scalar::numeric > 2147483647 THEN RETURN false; END IF;
        END LOOP;
        IF task_kind = 'export' AND (
            (data->>'scope_kind' = 'objects_in_class' AND target->>'type' = 'class') OR
            (data->>'scope_kind' = 'related_objects' AND target->>'type' = 'object' AND target->>'class_id' IS NOT NULL)
        ) IS NOT TRUE THEN RETURN false; END IF;
    END IF;
    IF output IS NOT NULL THEN
        IF jsonb_typeof(output) <> 'object' OR output->>'state' IS NULL THEN RETURN false; END IF;
        IF output->>'state' = 'produced' THEN
            IF output - ARRAY['state','expires_at'] <> '{}'::jsonb OR jsonb_typeof(output->'expires_at') IS DISTINCT FROM 'string'
                OR output->>'expires_at' !~ '(Z|[+-][0-9]{2}:[0-9]{2})$' THEN RETURN false; END IF;
            PERFORM (output->>'expires_at')::timestamptz;
        ELSIF output->>'state' NOT IN ('unknown','not_produced') OR output - 'state' <> '{}'::jsonb THEN RETURN false;
        END IF;
    END IF;
    RETURN true;
EXCEPTION WHEN OTHERS THEN RETURN false;
END;
$$;
