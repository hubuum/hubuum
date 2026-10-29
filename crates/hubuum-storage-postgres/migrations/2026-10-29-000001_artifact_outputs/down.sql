DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM artifact_task_owners) THEN
        RAISE EXCEPTION 'Reconcile all task artifact obligations before downgrading';
    END IF;
END $$;
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM export_task_outputs WHERE artifact_reference IS NOT NULL)
        OR EXISTS(SELECT 1 FROM backup_task_outputs WHERE artifact_reference IS NOT NULL) THEN
        RAISE EXCEPTION 'Cannot downgrade while artifact-backed task outputs remain';
    END IF;
END $$;
DROP TRIGGER export_artifact_reference ON export_task_outputs;
DROP TRIGGER backup_artifact_reference ON backup_task_outputs;
DROP FUNCTION hubuum_check_task_artifact_reference();
ALTER TABLE export_task_outputs DROP CONSTRAINT export_task_outputs_content;
ALTER TABLE export_task_outputs DROP COLUMN artifact_reference;
ALTER TABLE export_task_outputs ADD CONSTRAINT export_task_outputs_check CHECK (
    (json_output IS NOT NULL AND text_output IS NULL) OR (json_output IS NULL AND text_output IS NOT NULL)) NOT VALID;
ALTER TABLE backup_task_outputs DROP CONSTRAINT backup_task_outputs_size_matches_document;
ALTER TABLE backup_task_outputs DROP COLUMN artifact_reference;
ALTER TABLE backup_task_outputs ADD CONSTRAINT backup_task_outputs_size_matches_document
    CHECK(byte_size=octet_length(document)) NOT VALID;

DROP TRIGGER artifact_publication_accounting ON artifact_attempts;
DROP FUNCTION hubuum_account_published_artifact();
DROP FUNCTION hubuum_bind_task_artifact(UUID,UUID,INTEGER,UUID,UUID,BIGINT,BIGINT,TIMESTAMPTZ);
CREATE OR REPLACE FUNCTION hubuum_task_capacity_rejection(candidate hubuum_control.task_accounting,
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
CREATE OR REPLACE FUNCTION hubuum_task_capacity_snapshot() RETURNS JSONB
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
DROP VIEW hubuum_control.capacity_occupancy;

CREATE OR REPLACE FUNCTION hubuum_guard_artifact_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'staging' OR NEW.byte_size <> 0 OR NEW.block_count <> 0
            OR NEW.manifest IS NOT NULL OR NEW.manifest_sha256 IS NOT NULL THEN
            RAISE EXCEPTION 'An artifact must begin as empty staging';
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Artifact attempt tombstones preserve cancellation fences';
    END IF;
    IF ROW(NEW.artifact_id, NEW.attempt_id, NEW.destination_id, NEW.object_version,
           NEW.block_bytes, NEW.max_bytes, NEW.max_blocks, NEW.max_manifest_bytes, NEW.expires_at)
       IS DISTINCT FROM
       ROW(OLD.artifact_id, OLD.attempt_id, OLD.destination_id, OLD.object_version,
           OLD.block_bytes, OLD.max_bytes, OLD.max_blocks, OLD.max_manifest_bytes, OLD.expires_at) THEN
        RAISE EXCEPTION 'Artifact identity and limits are immutable';
    END IF;
    IF NOT (NEW.state = OLD.state
        OR (OLD.state = 'staging' AND NEW.state IN ('sealed', 'deleting'))
        OR (OLD.state = 'sealed' AND NEW.state IN ('published', 'deleting'))
        OR (OLD.state = 'deleting' AND NEW.state = 'discarded')) THEN
        RAISE EXCEPTION 'Invalid artifact lifecycle transition';
    END IF;
    IF OLD.state <> 'staging' AND ROW(NEW.byte_size, NEW.block_count) IS DISTINCT FROM ROW(OLD.byte_size, OLD.block_count) THEN
        RAISE EXCEPTION 'Sealed artifact accounting is immutable';
    END IF;
    IF NEW.manifest IS DISTINCT FROM OLD.manifest AND NOT (
        (OLD.state = 'staging' AND NEW.state = 'sealed')
        OR (OLD.state = 'deleting' AND NEW.state = 'discarded' AND NEW.manifest IS NULL)) THEN
        RAISE EXCEPTION 'Sealed artifact manifest is immutable';
    END IF;
    IF NEW.manifest_sha256 IS DISTINCT FROM OLD.manifest_sha256 AND NOT (OLD.state = 'sealed' AND NEW.state = 'published') THEN
        RAISE EXCEPTION 'Published artifact verification is immutable';
    END IF;
    IF NEW.state IN ('sealed', 'published') AND NEW.state <> OLD.state AND OLD.expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'Artifact staging expired';
    END IF;
    IF NEW.state = 'discarded' AND (NEW.manifest IS NOT NULL OR EXISTS (
        SELECT 1 FROM artifact_chunks WHERE artifact_id = NEW.artifact_id)) THEN
        RAISE EXCEPTION 'Discarded artifacts cannot retain physical bytes';
    END IF;
    RETURN NEW;
END;
$$;

DROP TABLE artifact_task_owners;
ALTER TABLE artifact_attempts DROP CONSTRAINT artifact_attempts_max_bytes_check;
ALTER TABLE artifact_attempts ADD CONSTRAINT artifact_attempts_max_bytes_check CHECK(max_bytes BETWEEN 0 AND 1073741824);
ALTER TABLE artifact_attempts DROP CONSTRAINT artifact_attempts_max_blocks_check;
ALTER TABLE artifact_attempts ADD CONSTRAINT artifact_attempts_max_blocks_check CHECK(max_blocks BETWEEN 0 AND 1024);
ALTER TABLE artifact_attempts DROP CONSTRAINT artifact_attempts_max_manifest_bytes_check;
ALTER TABLE artifact_attempts ADD CONSTRAINT artifact_attempts_max_manifest_bytes_check CHECK(max_manifest_bytes BETWEEN 117 AND 262144);
ALTER TABLE artifact_chunks DROP CONSTRAINT artifact_chunks_ordinal_check;
ALTER TABLE artifact_chunks ADD CONSTRAINT artifact_chunks_ordinal_check CHECK(ordinal BETWEEN 0 AND 1023);
