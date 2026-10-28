-- The control schema is deliberately absent from logical backup/replay. No
-- control row has a cascading foreign key into replaceable application data.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE hubuum_control.task_policy (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    revision BIGINT NOT NULL CHECK (revision > 0),
    initialized BOOLEAN NOT NULL DEFAULT FALSE,
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
    policy JSONB NOT NULL
);

INSERT INTO hubuum_control.task_policy (revision, policy)
SELECT 1, jsonb_build_object(
    'deployment', jsonb_build_object('outstanding', 10000, 'executing', 64, 'resident_bytes', 4294967296::BIGINT),
    'principal', jsonb_build_object('outstanding', 500, 'executing', 16, 'resident_bytes', 1073741824::BIGINT),
    'collection', jsonb_build_object('outstanding', 1000, 'executing', 32, 'resident_bytes', 2147483648::BIGINT),
    'internal', jsonb_build_object('outstanding', 10000, 'executing', 64, 'resident_bytes', 1073741824::BIGINT),
    'kinds', (SELECT jsonb_agg(jsonb_build_object(
        'kind', kind,
        'capacity', jsonb_build_object('outstanding', 5000, 'executing', 64, 'resident_bytes', 4294967296::BIGINT),
        'principal_outstanding', CASE kind WHEN 'backup' THEN 1 WHEN 'schema_profiling' THEN 2 ELSE 100 END
    ) ORDER BY ordinal) FROM unnest(ARRAY['import','export','backup','reindex','remote_call','schema_validation','schema_profiling']) WITH ORDINALITY AS kinds(kind, ordinal)),
    'cumulative_input_bytes_per_task', 1099511627776::BIGINT
);

CREATE TABLE hubuum_control.task_accounting (
    task_identity UUID PRIMARY KEY,
    -- Only a current-generation lookup aid. Ownership is always the UUID.
    task_id INTEGER UNIQUE,
    principal_identity UUID,
    collection_identities UUID[] NOT NULL DEFAULT '{}',
    kind TEXT NOT NULL CHECK (kind IN ('import','export','backup','reindex','remote_call','schema_validation','schema_profiling')),
    internal BOOLEAN NOT NULL,
    identity_resolved BOOLEAN NOT NULL DEFAULT TRUE,
    outstanding BOOLEAN NOT NULL,
    lease_token UUID,
    generation BIGINT NOT NULL CHECK (generation >= 0),
    resident_input_bytes BIGINT NOT NULL DEFAULT 0 CHECK (resident_input_bytes >= 0),
    resident_output_bytes BIGINT NOT NULL DEFAULT 0 CHECK (resident_output_bytes >= 0),
    cumulative_input_bytes BIGINT NOT NULL DEFAULT 0 CHECK (cumulative_input_bytes >= 0),
    cumulative_output_bytes BIGINT NOT NULL DEFAULT 0 CHECK (cumulative_output_bytes >= 0),
    cumulative_items BIGINT NOT NULL DEFAULT 0 CHECK (cumulative_items >= 0),
    processed_items INTEGER NOT NULL DEFAULT 0 CHECK (processed_items >= 0)
);
-- These indexes are built on the newly created, empty control table. Bounded
-- backfill runs after the migration, outside this DDL transaction.
CREATE INDEX task_accounting_principal ON hubuum_control.task_accounting (principal_identity) WHERE task_id IS NOT NULL; -- hubuum-compat: bounded-transactional-index
CREATE INDEX task_accounting_kind ON hubuum_control.task_accounting (internal, kind) WHERE task_id IS NOT NULL; -- hubuum-compat: bounded-transactional-index
CREATE INDEX task_accounting_collections ON hubuum_control.task_accounting USING GIN (collection_identities) WHERE task_id IS NOT NULL; -- hubuum-compat: bounded-transactional-index

CREATE FUNCTION hubuum_validate_task_policy(candidate JSONB) RETURNS VOID
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

CREATE FUNCTION hubuum_task_policy_read() RETURNS JSONB
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $function$
    SELECT jsonb_build_object('revision', revision, 'policy', policy)
    FROM hubuum_control.task_policy WHERE singleton
$function$;

CREATE FUNCTION hubuum_task_policy_initialize(candidate JSONB) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
BEGIN
    PERFORM public.hubuum_validate_task_policy(candidate);
    PERFORM 1 FROM hubuum_control.task_policy WHERE singleton FOR UPDATE;
    IF EXISTS (SELECT 1 FROM public.tasks t WHERE NOT EXISTS (
        SELECT 1 FROM hubuum_control.task_accounting a WHERE a.task_identity=(t.logical_identity #>> '{subject,identity,identity}')::UUID
            AND a.task_id=t.id
    )) THEN RAISE EXCEPTION 'task_capacity_reconciliation_required'; END IF;
    UPDATE hubuum_control.task_policy SET policy=candidate, initialized=TRUE WHERE singleton AND NOT initialized;
    RETURN public.hubuum_task_policy_read();
END
$function$;

CREATE FUNCTION hubuum_task_policy_replace(expected BIGINT, candidate JSONB) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
BEGIN
    PERFORM public.hubuum_validate_task_policy(candidate);
    UPDATE hubuum_control.task_policy SET policy=candidate, initialized=TRUE, revision=revision+1
    WHERE singleton AND revision=expected;
    IF NOT FOUND THEN RETURN NULL; END IF;
    RETURN public.hubuum_task_policy_read();
END
$function$;

-- One short coordination lock orders policy changes and capacity acquisition.
-- Releases only lock their accounting row: cascading deletion must not retain
-- this lock while waiting for unrelated graph locks. Until a release commits,
-- admission conservatively sees its old occupancy. Claim selection takes the
-- coordination lock before FOR UPDATE SKIP LOCKED and never waits on a task row.
CREATE FUNCTION hubuum_task_capacity_lock() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
BEGIN
    PERFORM 1 FROM hubuum_control.task_policy WHERE singleton FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'task_capacity_reconciliation_required'; END IF;
END
$function$;

CREATE FUNCTION hubuum_task_scope_rejection(limits JSONB, active BIGINT, occupied BIGINT, bytes NUMERIC,
    next_active BIGINT, next_occupied BIGINT, next_bytes BIGINT, reason TEXT,
    check_outstanding BOOLEAN, check_execution BOOLEAN, check_resident BOOLEAN) RETURNS TEXT
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $function$
    SELECT CASE
        WHEN check_outstanding AND active + next_active > (limits->>'outstanding')::BIGINT THEN reason
        WHEN check_execution AND occupied + next_occupied > (limits->>'executing')::BIGINT THEN reason
        WHEN check_resident AND bytes + next_bytes > (limits->>'resident_bytes')::BIGINT THEN 'task_capacity_resident_bytes'
        ELSE NULL END
$function$;

CREATE FUNCTION hubuum_task_capacity_rejection(candidate hubuum_control.task_accounting,
    check_outstanding BOOLEAN, check_execution BOOLEAN, check_resident BOOLEAN) RETURNS TEXT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE policy JSONB; limits JSONB; scope RECORD; member UUID; rejection TEXT;
    next_active BIGINT := CASE WHEN candidate.outstanding THEN 1 ELSE 0 END;
    next_occupied BIGINT := CASE WHEN candidate.lease_token IS NOT NULL THEN 1 ELSE 0 END;
    next_bytes BIGINT := candidate.resident_input_bytes + candidate.resident_output_bytes;
BEGIN
    SELECT p.policy INTO STRICT policy FROM hubuum_control.task_policy p WHERE singleton;
    IF candidate.internal THEN
        SELECT count(*) FILTER (WHERE outstanding) AS active,
            count(*) FILTER (WHERE lease_token IS NOT NULL) AS occupied,
            COALESCE(sum(resident_input_bytes::NUMERIC + resident_output_bytes),0) AS bytes INTO scope
        FROM hubuum_control.task_accounting WHERE task_identity<>candidate.task_identity AND task_id IS NOT NULL AND internal;
        RETURN public.hubuum_task_scope_rejection(policy->'internal',scope.active,scope.occupied,scope.bytes,
            next_active,next_occupied,next_bytes,'task_capacity_internal',check_outstanding,check_execution,check_resident);
    END IF;
    IF check_outstanding AND EXISTS(SELECT 1 FROM hubuum_control.task_accounting WHERE outstanding AND NOT identity_resolved AND NOT internal) THEN
        RETURN 'task_capacity_reconciliation_required';
    END IF;
    SELECT count(*) FILTER (WHERE outstanding) AS active,
        count(*) FILTER (WHERE lease_token IS NOT NULL) AS occupied,
        COALESCE(sum(resident_input_bytes::NUMERIC + resident_output_bytes),0) AS bytes INTO scope
    FROM hubuum_control.task_accounting WHERE task_identity<>candidate.task_identity AND task_id IS NOT NULL AND NOT internal;
    rejection := public.hubuum_task_scope_rejection(policy->'deployment',scope.active,scope.occupied,scope.bytes,
        next_active,next_occupied,next_bytes,'task_capacity_deployment',check_outstanding,check_execution,check_resident);
    IF rejection IS NOT NULL THEN RETURN rejection; END IF;
    IF candidate.principal_identity IS NOT NULL THEN
        SELECT count(*) FILTER (WHERE outstanding) AS active,
            count(*) FILTER (WHERE lease_token IS NOT NULL) AS occupied,
            COALESCE(sum(resident_input_bytes::NUMERIC + resident_output_bytes),0) AS bytes INTO scope
        FROM hubuum_control.task_accounting WHERE task_identity<>candidate.task_identity AND task_id IS NOT NULL AND NOT internal
            AND principal_identity=candidate.principal_identity;
        rejection := public.hubuum_task_scope_rejection(policy->'principal',scope.active,scope.occupied,scope.bytes,
            next_active,next_occupied,next_bytes,'task_capacity_principal',check_outstanding,check_execution,check_resident);
        IF rejection IS NOT NULL THEN RETURN rejection; END IF;
    END IF;
    SELECT value INTO STRICT limits FROM jsonb_array_elements(policy->'kinds') WHERE value->>'kind'=candidate.kind;
    SELECT count(*) FILTER (WHERE outstanding) AS active,
        count(*) FILTER (WHERE lease_token IS NOT NULL) AS occupied,
        COALESCE(sum(resident_input_bytes::NUMERIC + resident_output_bytes),0) AS bytes,
        count(*) FILTER (WHERE outstanding AND principal_identity=candidate.principal_identity) AS principal_active INTO scope
    FROM hubuum_control.task_accounting WHERE task_identity<>candidate.task_identity AND task_id IS NOT NULL AND NOT internal AND kind=candidate.kind;
    rejection := public.hubuum_task_scope_rejection(limits->'capacity',scope.active,scope.occupied,scope.bytes,
        next_active,next_occupied,next_bytes,'task_capacity_kind',check_outstanding,check_execution,check_resident);
    IF rejection IS NOT NULL THEN RETURN rejection; END IF;
    IF check_outstanding AND scope.principal_active + next_active > (limits->>'principal_outstanding')::BIGINT THEN
        RETURN 'task_capacity_principal_kind';
    END IF;
    FOREACH member IN ARRAY candidate.collection_identities LOOP
        SELECT count(*) FILTER (WHERE outstanding) AS active,
            count(*) FILTER (WHERE lease_token IS NOT NULL) AS occupied,
            COALESCE(sum(resident_input_bytes::NUMERIC + resident_output_bytes),0) AS bytes INTO scope
        FROM hubuum_control.task_accounting WHERE task_identity<>candidate.task_identity AND task_id IS NOT NULL AND NOT internal
            AND collection_identities @> ARRAY[member];
        rejection := public.hubuum_task_scope_rejection(policy->'collection',scope.active,scope.occupied,scope.bytes,
            next_active,next_occupied,next_bytes,'task_capacity_collection',check_outstanding,check_execution,check_resident);
        IF rejection IS NOT NULL THEN RETURN rejection; END IF;
    END LOOP;
    RETURN NULL;
END
$function$;

-- Resolve through captured UUIDs, never through a possibly reused compatibility
-- integer. Membership is captured once when a logical reservation is acquired.
CREATE FUNCTION hubuum_task_collection_identities(evidence JSONB) RETURNS UUID[]
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE result UUID[] := '{}'; reference RECORD; identity UUID; collection UUID; endpoint_count INTEGER;
BEGIN
    FOR reference IN SELECT value FROM jsonb_each(evidence->'references')
        WHERE value->>'kind'='resource' AND value->>'family' IN ('collection','class','object','class_relation','object_relation') LOOP
        IF reference.value #>> '{reference,state}' IS DISTINCT FROM 'known' THEN
            RAISE EXCEPTION 'task_capacity_identity_unavailable';
        END IF;
        identity := (reference.value #>> '{reference,identity}')::UUID;
        IF reference.value->>'family'='collection' THEN result := array_append(result, identity);
        ELSIF reference.value->>'family'='class' THEN
            SELECT (owner.logical_identity #>> '{subject,identity,identity}')::UUID INTO collection
            FROM public.hubuumclass c JOIN public.collections owner ON owner.id=c.collection_id
            WHERE c.logical_identity #>> '{subject,identity,identity}'=identity::TEXT;
            IF collection IS NULL THEN RAISE EXCEPTION 'task_capacity_identity_unavailable'; END IF;
            result := array_append(result, collection);
        ELSIF reference.value->>'family'='object' THEN
            SELECT (owner.logical_identity #>> '{subject,identity,identity}')::UUID INTO collection
            FROM public.hubuumobject o JOIN public.collections owner ON owner.id=o.collection_id
            WHERE o.logical_identity #>> '{subject,identity,identity}'=identity::TEXT;
            IF collection IS NULL THEN RAISE EXCEPTION 'task_capacity_identity_unavailable'; END IF;
            result := array_append(result, collection);
        ELSE
            -- Resolve the captured relation UUID first, then follow its current
            -- foreign keys. Legacy restore deliberately leaves historical
            -- reference evidence unresolved; live membership is authoritative
            -- for newly admitted work and does not assert historical identity.
            endpoint_count := 0;
            FOR collection IN
                SELECT (owner.logical_identity #>> '{subject,identity,identity}')::UUID
                FROM public.hubuumclass_relation r
                JOIN public.hubuumclass c ON c.id IN (r.from_hubuum_class_id,r.to_hubuum_class_id)
                JOIN public.collections owner ON owner.id=c.collection_id
                WHERE reference.value->>'family'='class_relation' AND r.logical_identity #>> '{subject,identity,identity}'=identity::TEXT
                UNION ALL
                SELECT (owner.logical_identity #>> '{subject,identity,identity}')::UUID
                FROM public.hubuumobject_relation r
                JOIN public.hubuumobject o ON o.id IN (r.from_hubuum_object_id,r.to_hubuum_object_id)
                JOIN public.collections owner ON owner.id=o.collection_id
                WHERE reference.value->>'family'='object_relation' AND r.logical_identity #>> '{subject,identity,identity}'=identity::TEXT
            LOOP
                IF collection IS NULL THEN RAISE EXCEPTION 'task_capacity_identity_unavailable'; END IF;
                endpoint_count := endpoint_count+1;
                result := array_append(result,collection);
            END LOOP;
            IF endpoint_count<>2 THEN RAISE EXCEPTION 'task_capacity_identity_unavailable'; END IF;
        END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM unnest(result) AS captured(identity) WHERE NOT EXISTS (
        SELECT 1 FROM public.collections c WHERE c.logical_identity #>> '{subject,identity,identity}'=captured.identity::TEXT
    )) THEN RAISE EXCEPTION 'task_capacity_identity_unavailable'; END IF;
    RETURN ARRAY(SELECT DISTINCT value FROM unnest(result) value ORDER BY value);
END
$function$;

CREATE FUNCTION hubuum_account_task() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE candidate hubuum_control.task_accounting; previous hubuum_control.task_accounting;
    current_generation BIGINT; policy JSONB; rejection TEXT; identity UUID; restoring BOOLEAN; reconciling BOOLEAN;
BEGIN
    restoring := current_setting('hubuum.restore_identities',TRUE) IS NOT DISTINCT FROM 'on'
        AND pg_has_role(session_user,current_user,'MEMBER');
    IF restoring THEN RETURN NULL; END IF;
    reconciling := current_setting('hubuum.reconcile_task_capacity',TRUE) IS NOT DISTINCT FROM 'on'
        AND pg_has_role(session_user,current_user,'MEMBER');
    IF TG_OP='UPDATE' AND (NEW.status,NEW.request_payload,NEW.lease_token,NEW.processed_items)
        IS NOT DISTINCT FROM (OLD.status,OLD.request_payload,OLD.lease_token,OLD.processed_items) THEN RETURN NULL; END IF;
    IF TG_OP='DELETE' THEN
        UPDATE hubuum_control.task_accounting SET task_id=NULL,outstanding=FALSE,lease_token=NULL,resident_input_bytes=0,resident_output_bytes=0
        WHERE task_identity=(OLD.logical_identity #>> '{subject,identity,identity}')::UUID;
        RETURN NULL;
    END IF;
    identity := (NEW.logical_identity #>> '{subject,identity,identity}')::UUID;
    IF identity IS NULL THEN RAISE EXCEPTION 'task_capacity_identity_unavailable'; END IF;
    IF TG_OP='UPDATE' AND NEW.status NOT IN ('queued','validating','running')
        AND COALESCE(octet_length(NEW.request_payload::TEXT),0) <= COALESCE(octet_length(OLD.request_payload::TEXT),0) THEN
        -- Terminalization/redaction only releases capacity. Preserve work
        -- history and serialize on this owner without taking the policy lock.
        UPDATE hubuum_control.task_accounting SET outstanding=FALSE,lease_token=NULL,
            resident_input_bytes=COALESCE(octet_length(NEW.request_payload::TEXT),0),
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
        IF NEW.status IN ('queued','validating','running') AND NEW.submitted_by IS NOT NULL AND candidate.principal_identity IS NULL THEN
            RAISE EXCEPTION 'task_capacity_identity_unavailable';
        END IF;
        candidate.collection_identities := CASE WHEN NEW.status IN ('queued','validating','running') THEN public.hubuum_task_collection_identities(NEW.logical_identity) ELSE '{}' END;
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
    candidate.outstanding := NEW.status IN ('queued','validating','running');
    candidate.lease_token := CASE WHEN NEW.status IN ('validating','running') THEN NEW.lease_token ELSE NULL END;
    candidate.resident_input_bytes := COALESCE(octet_length(NEW.request_payload::TEXT),0);
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

CREATE TRIGGER task_capacity_accounting AFTER INSERT OR UPDATE OR DELETE ON tasks
FOR EACH ROW EXECUTE FUNCTION hubuum_account_task();

-- Compute execution eligibility once per claim decision. Repeating aggregate
-- scans for every queued row would make a saturated queue quadratic.
CREATE FUNCTION hubuum_task_claimable_tasks() RETURNS SETOF INTEGER
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $function$
    WITH policy AS MATERIALIZED (SELECT policy,generation FROM hubuum_control.task_policy WHERE singleton),
    occupied AS MATERIALIZED (
        SELECT a.* FROM hubuum_control.task_accounting a JOIN policy p USING(generation)
        WHERE lease_token IS NOT NULL
    ), totals AS (
        SELECT count(*) FILTER (WHERE NOT internal) AS ordinary,
               count(*) FILTER (WHERE internal) AS internal FROM occupied
    ), principals AS (
        SELECT principal_identity,count(*) AS used FROM occupied WHERE NOT internal GROUP BY principal_identity
    ), kinds AS (
        SELECT kind,count(*) AS used FROM occupied WHERE NOT internal GROUP BY kind
    ), collections AS (
        SELECT identity,count(*) AS used FROM occupied CROSS JOIN LATERAL unnest(collection_identities) identity
        WHERE NOT internal GROUP BY identity
    )
    SELECT a.task_id FROM hubuum_control.task_accounting a
    JOIN policy p USING(generation) CROSS JOIN totals
    LEFT JOIN principals ON principals.principal_identity IS NOT DISTINCT FROM a.principal_identity
    LEFT JOIN kinds ON kinds.kind=a.kind
    CROSS JOIN LATERAL (SELECT value FROM jsonb_array_elements(p.policy->'kinds') WHERE value->>'kind'=a.kind) k
    WHERE a.outstanding AND a.identity_resolved AND a.lease_token IS NULL
        AND a.cumulative_input_bytes::NUMERIC+a.resident_input_bytes <= (p.policy->>'cumulative_input_bytes_per_task')::BIGINT
        AND (
            (a.internal AND totals.internal < (p.policy #>> '{internal,executing}')::BIGINT)
            OR (NOT a.internal
                AND totals.ordinary < (p.policy #>> '{deployment,executing}')::BIGINT
                AND coalesce(principals.used,0) < (p.policy #>> '{principal,executing}')::BIGINT
                AND coalesce(kinds.used,0) < (k.value #>> '{capacity,executing}')::BIGINT
                AND NOT EXISTS (SELECT 1 FROM collections c WHERE c.identity=ANY(a.collection_identities)
                    AND c.used >= (p.policy #>> '{collection,executing}')::BIGINT))
        )
$function$;

CREATE FUNCTION hubuum_task_output_bytes(candidate_id INTEGER) RETURNS BIGINT
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $function$
    SELECT COALESCE(sum(bytes),0)::BIGINT FROM (
        SELECT COALESCE(octet_length(json_output::TEXT),0)::BIGINT + COALESCE(octet_length(text_output),0)
            + octet_length(meta_json::TEXT) + octet_length(warnings_json::TEXT) AS bytes
        FROM public.export_task_outputs WHERE task_id=candidate_id
        UNION ALL SELECT octet_length(document)::BIGINT FROM public.backup_task_outputs WHERE task_id=candidate_id
        UNION ALL SELECT octet_length(report::TEXT)::BIGINT FROM public.schema_profile_outputs WHERE task_id=candidate_id
        UNION ALL SELECT octet_length(document::TEXT)::BIGINT FROM public.schema_repair_reports WHERE task_id=candidate_id
        UNION ALL SELECT COALESCE(octet_length(response_headers::TEXT),0)::BIGINT + COALESCE(octet_length(response_body_preview),0)
            FROM public.remote_call_results WHERE task_id=candidate_id
    ) outputs
$function$;

CREATE FUNCTION hubuum_account_task_output() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE candidate hubuum_control.task_accounting; previous_bytes BIGINT; rejection TEXT; candidate_id INTEGER; written_bytes BIGINT := 0; changed BOOLEAN := FALSE;
BEGIN
    IF current_setting('hubuum.restore_identities',TRUE) IS NOT DISTINCT FROM 'on'
        AND pg_has_role(session_user,current_user,'MEMBER') THEN RETURN NULL; END IF;
    candidate_id := CASE WHEN TG_OP='DELETE' THEN OLD.task_id ELSE NEW.task_id END;
    IF TG_OP <> 'DELETE' THEN PERFORM public.hubuum_task_capacity_lock(); END IF;
    -- Serialize output changes for this owner before recomputing resident
    -- bytes, including concurrent removal from a different output table.
    SELECT * INTO candidate FROM hubuum_control.task_accounting WHERE task_id=candidate_id FOR UPDATE;
    IF candidate.task_identity IS NULL THEN
        IF TG_OP='DELETE' THEN RETURN NULL; END IF;
        RAISE EXCEPTION 'task_capacity_identity_unavailable';
    END IF;
    previous_bytes := candidate.resident_output_bytes;
    candidate.resident_output_bytes := public.hubuum_task_output_bytes(candidate_id);
    IF candidate.resident_output_bytes > previous_bytes THEN
        rejection := public.hubuum_task_capacity_rejection(candidate,FALSE,FALSE,TRUE);
        IF rejection IS NOT NULL THEN RAISE EXCEPTION '%',rejection; END IF;
    END IF;
    -- Charge newly published content, including replacement with equal or
    -- smaller content. Identity backfill and retention metadata are not work.
    IF TG_OP <> 'DELETE' THEN
        CASE TG_TABLE_NAME
        WHEN 'export_task_outputs' THEN
            changed := TG_OP='INSERT' OR (NEW.json_output,NEW.text_output,NEW.meta_json,NEW.warnings_json)
                IS DISTINCT FROM (OLD.json_output,OLD.text_output,OLD.meta_json,OLD.warnings_json);
            written_bytes := COALESCE(octet_length(NEW.json_output::TEXT),0)::BIGINT + COALESCE(octet_length(NEW.text_output),0)
                + octet_length(NEW.meta_json::TEXT) + octet_length(NEW.warnings_json::TEXT);
        WHEN 'backup_task_outputs' THEN
            changed := TG_OP='INSERT' OR NEW.document IS DISTINCT FROM OLD.document;
            written_bytes := octet_length(NEW.document);
        WHEN 'schema_profile_outputs' THEN
            changed := TG_OP='INSERT' OR NEW.report IS DISTINCT FROM OLD.report;
            written_bytes := octet_length(NEW.report::TEXT);
        WHEN 'schema_repair_reports' THEN
            changed := TG_OP='INSERT' OR NEW.document IS DISTINCT FROM OLD.document;
            written_bytes := octet_length(NEW.document::TEXT);
        WHEN 'remote_call_results' THEN
            changed := TG_OP='INSERT' OR (NEW.response_headers,NEW.response_body_preview)
                IS DISTINCT FROM (OLD.response_headers,OLD.response_body_preview);
            written_bytes := COALESCE(octet_length(NEW.response_headers::TEXT),0)::BIGINT + COALESCE(octet_length(NEW.response_body_preview),0);
        END CASE;
    END IF;
    UPDATE hubuum_control.task_accounting SET resident_output_bytes=candidate.resident_output_bytes,
        cumulative_output_bytes=cumulative_output_bytes+CASE WHEN changed THEN written_bytes ELSE 0 END
    WHERE task_identity=candidate.task_identity;
    RETURN NULL;
END
$function$;

DO $triggers$
DECLARE relation_name TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY['export_task_outputs','backup_task_outputs','schema_profile_outputs','schema_repair_reports','remote_call_results'] LOOP
        EXECUTE format('CREATE TRIGGER task_output_capacity AFTER INSERT OR UPDATE OR DELETE ON public.%I FOR EACH ROW EXECUTE FUNCTION public.hubuum_account_task_output()', relation_name);
    END LOOP;
END
$triggers$;

CREATE FUNCTION hubuum_backfill_task_capacity(batch_limit INTEGER) RETURNS INTEGER
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
        IF task.status IN ('queued','validating','running') THEN
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
            task.status IN ('queued','validating','running'),task.lease_token,current_generation,
            COALESCE(octet_length(task.request_payload::TEXT),0),public.hubuum_task_output_bytes(task.id),task.processed_items,task.processed_items);
        changed := changed+1;
    END LOOP;
    RETURN changed;
END
$function$;

-- Replay calls this after replacing application rows and before creating repair
-- work. Old owners lose current-generation lookup and occupancy, not their work
-- history. No UUID correspondence is inferred from the integer task_id.
CREATE FUNCTION hubuum_reconcile_task_capacity() RETURNS VOID
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
        IF task.status IN ('queued','validating','running') OR task.lease_token IS NOT NULL THEN
            RAISE EXCEPTION 'task_capacity_reconciliation_required';
        END IF;
        INSERT INTO hubuum_control.task_accounting
            (task_identity,task_id,principal_identity,kind,internal,outstanding,generation,resident_input_bytes,resident_output_bytes,processed_items)
        VALUES (identity,task.id,(task.logical_identity #>> '{references,/submitted_by,reference,identity}')::UUID,
            task.kind,task.kind IN ('reindex','schema_validation'),FALSE,current_generation,
            COALESCE(octet_length(task.request_payload::TEXT),0),public.hubuum_task_output_bytes(task.id),task.processed_items)
        ON CONFLICT(task_identity) DO UPDATE SET task_id=EXCLUDED.task_id,generation=EXCLUDED.generation,
            resident_input_bytes=EXCLUDED.resident_input_bytes,resident_output_bytes=EXCLUDED.resident_output_bytes,processed_items=EXCLUDED.processed_items;
    END LOOP;
END
$function$;

-- Backfill is performed after #505's resumable identity backfill has completed.
-- It must not route historical rows through current admission ceilings.
REVOKE ALL ON TABLE hubuum_control.task_policy, hubuum_control.task_accounting FROM PUBLIC;
REVOKE ALL ON FUNCTION hubuum_task_policy_read(), hubuum_task_policy_initialize(JSONB),
    hubuum_task_policy_replace(BIGINT,JSONB), hubuum_task_capacity_lock(),
    hubuum_task_capacity_rejection(hubuum_control.task_accounting,BOOLEAN,BOOLEAN,BOOLEAN), hubuum_task_collection_identities(JSONB),
    hubuum_account_task(), hubuum_task_claimable_tasks(), hubuum_reconcile_task_capacity() FROM PUBLIC;
REVOKE ALL ON FUNCTION hubuum_backfill_task_capacity(INTEGER), hubuum_task_output_bytes(INTEGER),
    hubuum_account_task_output() FROM PUBLIC;

CREATE FUNCTION hubuum_task_capacity_snapshot() RETURNS JSONB
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $function$
    WITH lanes AS (SELECT FALSE AS internal UNION ALL SELECT TRUE), usage AS (
        SELECT lanes.internal, jsonb_build_object(
            'outstanding',count(*) FILTER (WHERE a.outstanding),
            'executing',count(*) FILTER (WHERE a.lease_token IS NOT NULL),
            'resident_bytes',coalesce(sum(a.resident_input_bytes::NUMERIC+a.resident_output_bytes),0),
            'cumulative_input_bytes',coalesce(sum(a.cumulative_input_bytes),0),
            'cumulative_output_bytes',coalesce(sum(a.cumulative_output_bytes),0),
            'cumulative_items',coalesce(sum(a.cumulative_items),0)) AS value
        FROM lanes LEFT JOIN hubuum_control.task_accounting a USING(internal) GROUP BY lanes.internal
    ) SELECT jsonb_build_object(
        'policy',jsonb_build_object('revision',p.revision,'policy',p.policy),
        'generation',p.generation,
        'ordinary',(SELECT value FROM usage WHERE NOT internal),
        'internal',(SELECT value FROM usage WHERE internal),
        'blocked_owners',(SELECT count(*) FROM hubuum_control.task_accounting WHERE outstanding AND NOT identity_resolved),
        'work_budget_blocked',(SELECT count(*) FROM hubuum_control.task_accounting WHERE outstanding AND lease_token IS NULL
            AND cumulative_input_bytes::NUMERIC+resident_input_bytes > (p.policy->>'cumulative_input_bytes_per_task')::BIGINT)
    ) FROM hubuum_control.task_policy p WHERE singleton
$function$;
REVOKE ALL ON FUNCTION hubuum_task_capacity_snapshot() FROM PUBLIC;
