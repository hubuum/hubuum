SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE artifact_attempts DROP CONSTRAINT artifact_attempts_max_bytes_check; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE artifact_attempts ADD CONSTRAINT artifact_attempts_max_bytes_check CHECK(max_bytes BETWEEN 0 AND 68719476736) NOT VALID;
ALTER TABLE artifact_attempts VALIDATE CONSTRAINT artifact_attempts_max_bytes_check;
ALTER TABLE artifact_attempts DROP CONSTRAINT artifact_attempts_max_blocks_check; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE artifact_attempts ADD CONSTRAINT artifact_attempts_max_blocks_check CHECK(max_blocks BETWEEN 0 AND 65536) NOT VALID;
ALTER TABLE artifact_attempts VALIDATE CONSTRAINT artifact_attempts_max_blocks_check;
ALTER TABLE artifact_attempts DROP CONSTRAINT artifact_attempts_max_manifest_bytes_check; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE artifact_attempts ADD CONSTRAINT artifact_attempts_max_manifest_bytes_check CHECK(max_manifest_bytes BETWEEN 117 AND 4194304) NOT VALID;
ALTER TABLE artifact_attempts VALIDATE CONSTRAINT artifact_attempts_max_manifest_bytes_check;
ALTER TABLE artifact_chunks DROP CONSTRAINT artifact_chunks_ordinal_check; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE artifact_chunks ADD CONSTRAINT artifact_chunks_ordinal_check CHECK(ordinal BETWEEN 0 AND 65535) NOT VALID;
ALTER TABLE artifact_chunks VALIDATE CONSTRAINT artifact_chunks_ordinal_check;
-- Durable ownership and reservations outlive application tasks. Deliberately
-- no task foreign key: task purge/restore cannot erase a physical obligation.
CREATE TABLE artifact_task_owners (
    artifact_id UUID PRIMARY KEY REFERENCES artifact_attempts(artifact_id),
    task_identity UUID NOT NULL,
    task_id INTEGER NOT NULL,
    lease_token UUID NOT NULL,
    worker_identity UUID NOT NULL,
    spool_bytes BIGINT NOT NULL CHECK(spool_bytes >= 0),
    reserved_bytes BIGINT NOT NULL CHECK(reserved_bytes >= 0),
    output_expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX artifact_task_owners_worker ON artifact_task_owners(worker_identity); -- hubuum-compat: bounded-transactional-index
CREATE INDEX artifact_task_owners_task ON artifact_task_owners(task_identity); -- hubuum-compat: bounded-transactional-index


ALTER TABLE export_task_outputs ADD COLUMN artifact_reference JSONB;
ALTER TABLE export_task_outputs DROP CONSTRAINT export_task_outputs_check; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE export_task_outputs ADD CONSTRAINT export_task_outputs_content CHECK (
    (artifact_reference IS NULL AND ((json_output IS NOT NULL AND text_output IS NULL)
        OR (json_output IS NULL AND text_output IS NOT NULL)))
    OR (artifact_reference IS NOT NULL AND json_output IS NULL AND text_output IS NULL)
) NOT VALID;
ALTER TABLE backup_task_outputs ADD COLUMN artifact_reference JSONB;
ALTER TABLE backup_task_outputs DROP CONSTRAINT backup_task_outputs_size_matches_document; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE backup_task_outputs ADD CONSTRAINT backup_task_outputs_size_matches_document CHECK (
    (artifact_reference IS NULL AND byte_size = octet_length(document))
    OR (artifact_reference IS NOT NULL AND octet_length(document) = 0)
) NOT VALID;

-- Bind the catalog reference to a published attempt and this exact task
-- incarnation. Integer IDs can be reused by logical restore.
CREATE FUNCTION hubuum_check_task_artifact_reference() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE artifact UUID; expected_task_identity UUID;
BEGIN
    IF NEW.artifact_reference IS NULL THEN RETURN NEW; END IF;
    artifact := (NEW.artifact_reference #>> '{identity,artifact}')::UUID;
    SELECT (logical_identity #>> '{subject,identity,identity}')::UUID INTO STRICT expected_task_identity
        FROM tasks WHERE id=NEW.task_id;
    IF NOT EXISTS (SELECT 1 FROM artifact_attempts a JOIN artifact_task_owners o USING(artifact_id)
        WHERE a.artifact_id=artifact AND a.state='published' AND o.task_identity=expected_task_identity
          AND a.attempt_id=(NEW.artifact_reference #>> '{identity,attempt}')::UUID
          AND a.destination_id=(NEW.artifact_reference #>> '{identity,destination}')::UUID
          AND a.object_version=(NEW.artifact_reference #>> '{identity,version}')::UUID
          AND to_jsonb(ARRAY(SELECT get_byte(a.manifest_sha256,i) FROM generate_series(0,31) i))=NEW.artifact_reference->'manifest_digest') THEN
        RAISE EXCEPTION 'Task output requires its published artifact';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER export_artifact_reference BEFORE INSERT OR UPDATE ON export_task_outputs
    FOR EACH ROW EXECUTE FUNCTION hubuum_check_task_artifact_reference();
CREATE TRIGGER backup_artifact_reference BEFORE INSERT OR UPDATE ON backup_task_outputs
    FOR EACH ROW EXECUTE FUNCTION hubuum_check_task_artifact_reference();


-- Include physical obligations even after their originating task is purged.
-- Task accounting retains the stable principal/collection ownership metadata.
CREATE VIEW hubuum_control.capacity_occupancy AS
SELECT * FROM hubuum_control.task_accounting
UNION ALL SELECT o.artifact_id, o.task_id, a.principal_identity, a.collection_identities,
    a.kind, a.internal, a.identity_resolved, FALSE, NULL::UUID, a.generation,
    0::BIGINT, o.reserved_bytes, 0::BIGINT, 0::BIGINT, 0::BIGINT, 0
FROM public.artifact_task_owners o JOIN hubuum_control.task_accounting a USING(task_identity);
REVOKE ALL ON hubuum_control.capacity_occupancy FROM PUBLIC;

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
        FROM hubuum_control.capacity_occupancy WHERE task_identity<>candidate.task_identity AND task_id IS NOT NULL AND internal;
        RETURN public.hubuum_task_scope_rejection(policy->'internal',scope.active,scope.occupied,scope.bytes,
            next_active,next_occupied,next_bytes,'task_capacity_internal',check_outstanding,check_execution,check_resident);
    END IF;
    IF check_outstanding AND EXISTS(SELECT 1 FROM hubuum_control.capacity_occupancy WHERE outstanding AND NOT identity_resolved AND NOT internal) THEN
        RETURN 'task_capacity_reconciliation_required';
    END IF;
    SELECT count(*) FILTER (WHERE outstanding) AS active,
        count(*) FILTER (WHERE lease_token IS NOT NULL) AS occupied,
        COALESCE(sum(resident_input_bytes::NUMERIC + resident_output_bytes),0) AS bytes INTO scope
    FROM hubuum_control.capacity_occupancy WHERE task_identity<>candidate.task_identity AND task_id IS NOT NULL AND NOT internal;
    rejection := public.hubuum_task_scope_rejection(policy->'deployment',scope.active,scope.occupied,scope.bytes,
        next_active,next_occupied,next_bytes,'task_capacity_deployment',check_outstanding,check_execution,check_resident);
    IF rejection IS NOT NULL THEN RETURN rejection; END IF;
    IF candidate.principal_identity IS NOT NULL THEN
        SELECT count(*) FILTER (WHERE outstanding) AS active,
            count(*) FILTER (WHERE lease_token IS NOT NULL) AS occupied,
            COALESCE(sum(resident_input_bytes::NUMERIC + resident_output_bytes),0) AS bytes INTO scope
        FROM hubuum_control.capacity_occupancy WHERE task_identity<>candidate.task_identity AND task_id IS NOT NULL AND NOT internal
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
    FROM hubuum_control.capacity_occupancy WHERE task_identity<>candidate.task_identity AND task_id IS NOT NULL AND NOT internal AND kind=candidate.kind;
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
        FROM hubuum_control.capacity_occupancy WHERE task_identity<>candidate.task_identity AND task_id IS NOT NULL AND NOT internal
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
        FROM lanes LEFT JOIN hubuum_control.capacity_occupancy a USING(internal) GROUP BY lanes.internal
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

CREATE FUNCTION hubuum_bind_task_artifact(artifact UUID, attempt UUID, owner_id INTEGER,
    claim UUID, worker UUID, spool BIGINT, worker_limit BIGINT, expires TIMESTAMPTZ) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE owner hubuum_control.task_accounting; staged public.artifact_attempts;
    reservation BIGINT; rejection TEXT; worker_used NUMERIC;
BEGIN
    IF spool<0 OR worker_limit<=0 OR worker='00000000-0000-0000-0000-000000000000'::UUID OR expires<=clock_timestamp() THEN
        RAISE EXCEPTION 'Invalid artifact admission settings';
    END IF;
    PERFORM public.hubuum_task_capacity_lock();
    PERFORM 1 FROM public.tasks t WHERE t.id=owner_id AND t.lease_token=claim
        AND t.lease_expires_at>clock_timestamp() AND t.status IN ('running','validating')
        AND t.cancel_requested_at IS NULL AND (t.execution_deadline_at IS NULL OR t.execution_deadline_at>clock_timestamp()) FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Artifact claim is not live'; END IF;
    SELECT * INTO STRICT owner FROM hubuum_control.task_accounting WHERE task_id=owner_id AND lease_token=claim;
    SELECT * INTO STRICT staged FROM public.artifact_attempts WHERE artifact_id=artifact AND attempt_id=attempt
        AND state='staging' AND expires_at>clock_timestamp() FOR UPDATE;
    reservation := staged.max_bytes + spool;
    SELECT coalesce(sum(o.reserved_bytes),0) INTO worker_used FROM public.artifact_task_owners o
        JOIN public.artifact_attempts a USING(artifact_id) WHERE o.worker_identity=worker AND a.state<>'published';
    IF worker_used + reservation > worker_limit THEN RAISE EXCEPTION 'Artifact worker resident capacity exhausted'; END IF;
    INSERT INTO public.artifact_task_owners(artifact_id,task_identity,task_id,lease_token,worker_identity,spool_bytes,reserved_bytes,output_expires_at)
        VALUES(artifact,owner.task_identity,owner_id,claim,worker,spool,reservation,expires);
    rejection := public.hubuum_task_capacity_rejection(owner,FALSE,FALSE,TRUE);
    IF rejection IS NOT NULL THEN RAISE EXCEPTION '%',rejection; END IF;
END
$function$;
REVOKE ALL ON FUNCTION hubuum_bind_task_artifact(UUID,UUID,INTEGER,UUID,UUID,BIGINT,BIGINT,TIMESTAMPTZ) FROM PUBLIC;

CREATE FUNCTION hubuum_account_published_artifact() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
BEGIN
    IF NEW.state='published' AND OLD.state<>'published' THEN
        UPDATE hubuum_control.task_accounting SET cumulative_output_bytes=cumulative_output_bytes+NEW.byte_size
        WHERE task_identity=(SELECT task_identity FROM public.artifact_task_owners WHERE artifact_id=NEW.artifact_id);
    END IF;
    RETURN NULL;
END
$function$;
REVOKE ALL ON FUNCTION hubuum_account_published_artifact() FROM PUBLIC;
CREATE TRIGGER artifact_publication_accounting AFTER UPDATE ON artifact_attempts
    FOR EACH ROW EXECUTE FUNCTION hubuum_account_published_artifact();

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
        OR (OLD.state = 'deleting' AND NEW.state = 'discarded')
        OR (OLD.state = 'published' AND NEW.state = 'deleting' AND EXISTS (
            SELECT 1 FROM artifact_task_owners o WHERE o.artifact_id=OLD.artifact_id AND (
                o.output_expires_at <= clock_timestamp()
                OR (NOT EXISTS (SELECT 1 FROM export_task_outputs e WHERE (e.artifact_reference #>> '{identity,artifact}')::UUID=OLD.artifact_id)
                    AND NOT EXISTS (SELECT 1 FROM backup_task_outputs b WHERE (b.artifact_reference #>> '{identity,artifact}')::UUID=OLD.artifact_id))
                OR NOT EXISTS (SELECT 1 FROM tasks t
                WHERE t.id=o.task_id AND t.deleted_at IS NULL AND
                (t.logical_identity #>> '{subject,identity,identity}')::UUID=o.task_identity))))) THEN
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
