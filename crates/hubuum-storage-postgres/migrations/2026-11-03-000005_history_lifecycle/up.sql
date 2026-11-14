SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Compact UTF-8 JSON v1, with normalized plain decimal numbers. This is a
-- logical record measure, not PostgreSQL heap/index or compressed storage size.
CREATE FUNCTION hubuum_control.history_logical_bytes_v1(input_data JSONB, depth INTEGER)
RETURNS BIGINT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
SET search_path=pg_catalog AS $function$
DECLARE result BIGINT;
BEGIN
    IF depth NOT BETWEEN 0 AND 128 THEN
        RAISE EXCEPTION 'Logical history JSON exceeds the v1 depth bound' USING ERRCODE='23514';
    END IF;
    CASE jsonb_typeof(input_data)
        WHEN 'object' THEN
            SELECT 2 + coalesce(sum(octet_length(convert_to(to_jsonb(entry.key)::TEXT,'UTF8'))
                + 1 + hubuum_control.history_logical_bytes_v1(entry.value,depth+1)),0)
                + greatest(count(*)-1,0) INTO result FROM jsonb_each(input_data) entry;
        WHEN 'array' THEN
            SELECT 2 + coalesce(sum(hubuum_control.history_logical_bytes_v1(entry.value,depth+1)),0)
                + greatest(count(*)-1,0) INTO result FROM jsonb_array_elements(input_data) entry(value);
        WHEN 'number' THEN
            result:=octet_length(convert_to(trim_scale((input_data #>> '{}')::NUMERIC)::TEXT,'UTF8'));
        ELSE result:=octet_length(convert_to(input_data::TEXT,'UTF8'));
    END CASE;
    RETURN result;
END
$function$;
REVOKE ALL ON FUNCTION hubuum_control.history_logical_bytes_v1(JSONB,INTEGER) FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_logical_bytes_v1(input_data JSONB) RETURNS BIGINT
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE SECURITY DEFINER
SET search_path=pg_catalog RETURN hubuum_control.history_logical_bytes_v1(input_data,0);
REVOKE ALL ON FUNCTION public.hubuum_history_logical_bytes_v1(JSONB) FROM PUBLIC;

-- No references into replaceable application tables. Original policy/hold
-- identities and clocks survive logical replacement.
-- Immutable read decisions exclude physical-removal receipts. Material remains
-- the complete retained authority; stored scalars avoid decoding it per guard.
CREATE FUNCTION hubuum_control.history_read_revision(material JSONB) RETURNS BIGINT
LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog AS $function$
    SELECT jsonb_array_length(material #> '{authority,policies}')::BIGINT
        + jsonb_array_length(material #> '{authority,holds}')::BIGINT
        + coalesce((SELECT sum(1 + CASE WHEN intent->'cancellation' IS DISTINCT FROM 'null'::JSONB THEN 1 ELSE 0 END)
            FROM jsonb_array_elements(coalesce(material #> '{authority,expiry}','[]'::JSONB)) intent),0)
$function$;
REVOKE ALL ON FUNCTION hubuum_control.history_read_revision(JSONB) FROM PUBLIC;

CREATE TABLE hubuum_control.history_lifecycle_control (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    material JSONB NOT NULL CHECK (octet_length(material::text) <= 4194304),
    authority_revision BIGINT GENERATED ALWAYS AS ((material #>> '{authority,revision}')::BIGINT) STORED,
    read_revision BIGINT GENERATED ALWAYS AS (hubuum_control.history_read_revision(material)) STORED
);
INSERT INTO hubuum_control.history_lifecycle_control(id,material) VALUES
    (1, '{"authority":{"revision":"0","policies":[],"holds":[]},"previews":[]}');
REVOKE ALL ON hubuum_control.history_lifecycle_control FROM PUBLIC;

-- Payload-free projection retained through logical replacement and copy removal.
-- Only protected control operations maintain it; runtime reads use the predicates.
CREATE TABLE hubuum_control.history_expiry_members (
    family TEXT NOT NULL CHECK (family IN ('collection','class','object','class_relation','object_relation','export_template','remote_target','computed_definitions','class_schema')),
    resource_uuid UUID NOT NULL,
    version_uuid UUID NOT NULL,
    intent_uuid UUID NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    -- Equality retains a native deletion point, whose exclusion protects the
    -- subsequent gap just like the older unbounded deletion representation.
    valid_to TIMESTAMPTZ CHECK (valid_to IS NULL OR valid_to >= valid_from),
    PRIMARY KEY (family,resource_uuid,version_uuid)
);
-- The table is new and empty in this transaction; the timeouts above bound DDL waits.
CREATE INDEX history_expiry_members_as_of ON hubuum_control.history_expiry_members(family,resource_uuid,valid_from); -- hubuum-compat: bounded-transactional-index
REVOKE ALL ON hubuum_control.history_expiry_members FROM PUBLIC;

ALTER TABLE hubuum_control.runtime_registrations ADD COLUMN history_protocol SMALLINT NOT NULL DEFAULT 0 CHECK (history_protocol IN (0,1));

CREATE TABLE hubuum_control.history_publication_control (
    id SMALLINT PRIMARY KEY CHECK (id=1),
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation>=0),
    admission_revision BIGINT NOT NULL DEFAULT 0 CHECK (admission_revision>=0)
);
INSERT INTO hubuum_control.history_publication_control(id) VALUES(1);
CREATE TABLE hubuum_control.history_publication_owners (
    id UUID PRIMARY KEY,
    ownership_token UUID NOT NULL,
    runtime_id UUID NOT NULL,
    isolation JSONB,
    incarnation UUID NOT NULL,
    deletion_revision BIGINT NOT NULL CHECK (deletion_revision>=0),
    history_revision BIGINT NOT NULL CHECK (history_revision>=0),
    read_revision BIGINT NOT NULL CHECK (read_revision>=0),
    generation BIGINT NOT NULL CHECK (generation>=0),
    kind TEXT NOT NULL CHECK (kind IN ('http_response','task_output','backup','export','archive_read')),
    -- Original API occupancy is a retained binding, not publication lifetime.
    -- No FK/cascade: exact API settlement must neither remove this obligation
    -- nor wait on a history parent that itself waits for API-owner SHARE.
    api_request_owner UUID CHECK (api_request_owner IS NULL OR api_request_owner <> '00000000-0000-0000-0000-000000000000'),
    admitted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    producer_count SMALLINT NOT NULL DEFAULT 0 CHECK (producer_count IN (0,1)),
    producer_assigned BOOLEAN NOT NULL DEFAULT FALSE,
    CHECK (producer_count=0 OR producer_assigned)
);
-- One assignment per parent lifetime. A retired tombstone prevents accidental
-- slot reuse; loss/drop/timeout never decrements the occupied parent counter.
CREATE TABLE hubuum_control.history_publication_producers (
    id UUID NOT NULL CHECK (id <> '00000000-0000-0000-0000-000000000000'),
    ownership_token UUID NOT NULL,
    publication_id UUID PRIMARY KEY,
    publication_ownership UUID NOT NULL,
    retired BOOLEAN NOT NULL DEFAULT FALSE
);
REVOKE ALL ON hubuum_control.history_publication_producers FROM PUBLIC;
-- One claim per parent lifetime. No foreign key, trigger or secondary unique
-- key: no cross-parent index conflict can wait while holding another parent.
-- Claims and their captured input survive restore and remain charged until an
-- exact independently proven outcome/reconciliation is supported.
CREATE TABLE hubuum_control.history_archive_publication_claims (
    publication_id UUID PRIMARY KEY,
    publication_ownership UUID NOT NULL,
    producer_id UUID NOT NULL,
    producer_ownership UUID NOT NULL,
    capture_id UUID NOT NULL,
    material BYTEA NOT NULL,
    encoded_intent BYTEA NOT NULL CHECK(octet_length(encoded_intent)>0),
    request_commitment BYTEA GENERATED ALWAYS AS (pg_catalog.sha256(material)) STORED,
    intent_commitment BYTEA GENERATED ALWAYS AS (pg_catalog.sha256(encoded_intent)) STORED,
    retained_bytes INTEGER GENERATED ALWAYS AS (octet_length(material)+octet_length(encoded_intent)+128) STORED CHECK(retained_bytes BETWEEN 129 AND 262272),
    reserved_artifact_bytes BIGINT NOT NULL DEFAULT 16777216 CHECK(reserved_artifact_bytes=16777216),
    CHECK(octet_length(material)+octet_length(encoded_intent)<=262144),
    CHECK ((pg_catalog.convert_from(material,'UTF8')::JSONB->>'capture')::UUID=capture_id)
);
REVOKE ALL ON hubuum_control.history_archive_publication_claims FROM PUBLIC;
ALTER TABLE hubuum_control.history_publication_producers ADD COLUMN publication_claimed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE hubuum_control.history_archive_captures ADD COLUMN publication_claimed BOOLEAN NOT NULL DEFAULT FALSE;

-- Both indexes cover the new, still-empty owner table under the same DDL timeouts.
CREATE INDEX history_publication_runtime ON hubuum_control.history_publication_owners(runtime_id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX history_publication_generation ON hubuum_control.history_publication_owners(generation); -- hubuum-compat: bounded-transactional-index
REVOKE ALL ON hubuum_control.history_publication_control,hubuum_control.history_publication_owners FROM PUBLIC;

-- Productive entry takes this gate AFTER application/deletion authority and
-- BEFORE API-owner validation. Owner validation follows the API-owner gate.
CREATE FUNCTION public.hubuum_history_read_authority(runtime UUID, ownership UUID, write_mode BOOLEAN)
RETURNS SMALLINT LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE material JSONB; protocol SMALLINT;
BEGIN
    IF write_mode IS TRUE THEN
        -- Control commits recheck application authority. Pin it before history
        -- UPDATE so concurrent registration cannot hold app and wait on history
        -- while this transaction later waits on app. Exposure alone is a read.
        PERFORM 1 FROM hubuum_control.deletion_authority WHERE id=1 FOR SHARE;
        IF NOT FOUND THEN RAISE EXCEPTION 'Deletion authority is unavailable'; END IF;
        PERFORM 1 FROM hubuum_control.application_control WHERE id=1 FOR SHARE;
        IF NOT FOUND THEN RAISE EXCEPTION 'Application authority is unavailable'; END IF;
        PERFORM 1 FROM hubuum_control.history_lifecycle_control c WHERE id=1 FOR UPDATE;
    ELSE
        PERFORM 1 FROM hubuum_control.history_lifecycle_control c WHERE id=1 FOR SHARE;
    END IF;
    IF NOT FOUND THEN RAISE EXCEPTION 'History authority is unavailable'; END IF;
    IF runtime IS NULL AND ownership IS NULL THEN
        SELECT c.material INTO material FROM hubuum_control.history_lifecycle_control c WHERE id=1;
        IF coalesce(material #> '{authority,expiry}','[]'::JSONB)<>'[]'::JSONB
            OR EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members) THEN
            RAISE EXCEPTION 'Retained history requires an owned publication scope';
        END IF;
        RETURN 0;
    END IF;
    SELECT r.history_protocol INTO protocol FROM hubuum_control.runtime_registrations r
        WHERE r.id=runtime AND r.ownership_token=ownership;
    IF NOT FOUND OR protocol NOT IN (0,1) THEN RAISE EXCEPTION 'History runtime ownership is unavailable'; END IF;
    IF protocol=0 THEN
        SELECT c.material INTO material FROM hubuum_control.history_lifecycle_control c WHERE id=1;
    END IF;
    IF protocol=0 AND (coalesce(material #> '{authority,expiry}','[]'::JSONB)<>'[]'::JSONB
        OR EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members)) THEN
        RAISE EXCEPTION 'Legacy history producer is fenced by retained expiry';
    END IF;
    RETURN protocol;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_read_authority(UUID,UUID,BOOLEAN) FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_publication_generation() RETURNS BIGINT
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
    SELECT generation FROM hubuum_control.history_publication_control WHERE id=1
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_publication_generation() FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_publication_admit(identity UUID, ownership UUID, producer_kind TEXT,
    runtime UUID, runtime_ownership UUID, incarnation UUID, deletion_revision BIGINT,
    history_revision BIGINT, expected_generation BIGINT, original_api_owner UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
BEGIN
    IF identity IS NULL OR ownership IS NULL OR producer_kind IS NULL
        OR producer_kind NOT IN ('http_response','task_output','backup','export','archive_read') THEN RETURN FALSE; END IF;
    IF public.hubuum_application_execution_allowed(runtime,runtime_ownership,incarnation,deletion_revision) IS NOT TRUE THEN RETURN FALSE; END IF;
    IF producer_kind='archive_read' THEN
        -- Legacy archive bridge: read_authority itself rejects retained expiry.
        PERFORM public.hubuum_history_read_authority(runtime,runtime_ownership,FALSE);
    ELSIF public.hubuum_history_read_authority(runtime,runtime_ownership,FALSE) IS DISTINCT FROM 1 THEN
        RETURN FALSE;
    END IF;
    IF (SELECT authority_revision FROM hubuum_control.history_lifecycle_control WHERE id=1) IS DISTINCT FROM history_revision THEN RETURN FALSE; END IF;
    -- Authority -> API -> publication-control/parent. Never reacquire API
    -- during exact owner settlement, or after acquiring a child/parent lock.
    IF original_api_owner IS NOT NULL THEN
        PERFORM public.hubuum_api_request_execution(original_api_owner);
    END IF;
    -- Touching this tuple serializes capacity and rejects retained RR snapshots.
    UPDATE hubuum_control.history_publication_control SET admission_revision=admission_revision+1
        WHERE id=1 AND generation=expected_generation;
    IF NOT FOUND OR (SELECT count(*) FROM hubuum_control.history_publication_owners)>=256
        OR (SELECT count(*) FROM hubuum_control.history_publication_owners WHERE runtime_id=runtime)>=64 THEN RETURN FALSE; END IF;
    INSERT INTO hubuum_control.history_publication_owners(id,ownership_token,runtime_id,isolation,incarnation,deletion_revision,history_revision,read_revision,generation,kind,api_request_owner)
        SELECT identity,ownership,runtime,r.isolation,incarnation,deletion_revision,history_revision,
            (SELECT c.read_revision FROM hubuum_control.history_lifecycle_control c WHERE c.id=1),
            expected_generation,producer_kind,original_api_owner
        FROM hubuum_control.runtime_registrations r WHERE r.id=runtime AND r.ownership_token=runtime_ownership;
    RETURN FOUND;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_publication_admit(UUID,UUID,TEXT,UUID,UUID,UUID,BIGINT,BIGINT,BIGINT,UUID) FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_publication_execution(identity UUID, ownership UUID,
    expected_incarnation UUID, expected_deletion BIGINT, expected_history BIGINT, expected_generation BIGINT,
    expected_kind TEXT DEFAULT NULL, expected_read BIGINT DEFAULT NULL)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE current_history BIGINT; current_read BIGINT;
BEGIN
    -- Authority is held before API/parent ownership. Never refresh the original
    -- owner: only an unassigned task control scope tolerates receipt progress.
    SELECT c.authority_revision,c.read_revision INTO current_history,current_read
        FROM hubuum_control.history_lifecycle_control c WHERE c.id=1;
    IF (SELECT generation FROM hubuum_control.history_publication_control WHERE id=1) IS DISTINCT FROM expected_generation THEN RETURN FALSE; END IF;
    PERFORM 1 FROM hubuum_control.history_publication_owners p
        WHERE p.id=identity AND p.ownership_token=ownership AND p.incarnation=expected_incarnation
        AND p.deletion_revision=expected_deletion AND p.history_revision=expected_history
        AND p.generation=expected_generation
        AND (expected_kind IS NULL OR p.kind=expected_kind)
        AND (expected_read IS NULL OR p.read_revision=expected_read)
        AND (current_history=expected_history OR
            (expected_kind='task_output' AND p.kind='task_output' AND NOT p.producer_assigned
                AND expected_read IS NOT NULL AND current_read=expected_read)) FOR SHARE;
    RETURN FOUND;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_publication_execution(UUID,UUID,UUID,BIGINT,BIGINT,BIGINT,TEXT,BIGINT) FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_publication_settle(identity UUID, ownership UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
BEGIN
    -- Exact-owner-only: waits its productive SHARE holders, never reacquires
    -- application/history/API-owner locks, and has no timer/reaper path.
    DELETE FROM hubuum_control.history_publication_owners p
        WHERE p.id=identity AND p.ownership_token=ownership AND p.producer_count=0
        AND NOT EXISTS (SELECT 1 FROM hubuum_control.history_archive_publication_claims a WHERE a.publication_id=p.id)
        AND NOT EXISTS (SELECT 1 FROM hubuum_control.history_publication_producers c
            WHERE c.publication_id=p.id AND NOT c.retired);
    IF NOT FOUND THEN RETURN FALSE; END IF;
    DELETE FROM hubuum_control.history_publication_producers c
        WHERE c.publication_id=identity AND c.publication_ownership=ownership AND c.retired;
    RETURN TRUE;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_publication_settle(UUID,UUID) FROM PUBLIC;

-- Productive entry already holds deletion/application/history authority before
-- API-owner SHARE. Take parent UPDATE directly (not SHARE then an upgrade).
-- The changed parent tuple and UNIQUE publication_id reject stale RC/RR
-- reservations racing retirement; a serialization failure is never admission.
-- assigned is monotonic even after child settlement. Every same-parent insert
-- must first change that tuple from FALSE, so it cannot wait on an existing
-- child index entry while a settler waits for this parent. The ONLY child unique
-- index is publication_id: cross-parent child nonce collisions cannot conflict.
-- No child FK, trigger, secondary unique index or reinsertion path exists.
CREATE FUNCTION public.hubuum_history_producer_admit(identity UUID, ownership UUID,
    parent UUID, parent_ownership UUID, runtime UUID, runtime_ownership UUID,
    expected_incarnation UUID, expected_deletion BIGINT, expected_history BIGINT,
    expected_generation BIGINT)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
BEGIN
    IF identity IS NULL OR identity='00000000-0000-0000-0000-000000000000'
        OR ownership IS NULL THEN RETURN FALSE; END IF;
    IF public.hubuum_application_execution_allowed(runtime,runtime_ownership,expected_incarnation,expected_deletion) IS NOT TRUE THEN RETURN FALSE; END IF;
    PERFORM public.hubuum_history_read_authority(runtime,runtime_ownership,FALSE);
    IF (SELECT generation FROM hubuum_control.history_publication_control WHERE id=1) IS DISTINCT FROM expected_generation
        OR (SELECT authority_revision FROM hubuum_control.history_lifecycle_control WHERE id=1) IS DISTINCT FROM expected_history THEN RETURN FALSE; END IF;
    UPDATE hubuum_control.history_publication_owners p SET producer_count=1,producer_assigned=TRUE
        WHERE p.id=parent AND p.ownership_token=parent_ownership AND p.runtime_id=runtime
        AND p.incarnation=expected_incarnation AND p.deletion_revision=expected_deletion
        AND p.history_revision=expected_history AND p.generation=expected_generation
        AND p.producer_count=0 AND NOT p.producer_assigned AND NOT EXISTS (
            SELECT 1 FROM hubuum_control.history_publication_producers c WHERE c.publication_id=parent);
    IF NOT FOUND THEN RETURN FALSE; END IF;
    INSERT INTO hubuum_control.history_publication_producers(id,ownership_token,publication_id,publication_ownership)
        VALUES(identity,ownership,parent,parent_ownership);
    RETURN TRUE;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_producer_admit(UUID,UUID,UUID,UUID,UUID,UUID,UUID,BIGINT,BIGINT,BIGINT) FROM PUBLIC;

-- Exact-owner-only settlement. No application/history/API locks and no current
-- epoch requirement: positively completed old work must still be settleable.
CREATE FUNCTION public.hubuum_history_producer_settle(parent UUID, identity UUID, ownership UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE child hubuum_control.history_publication_producers%ROWTYPE;
BEGIN
    SELECT c.* INTO child FROM hubuum_control.history_publication_producers c
        WHERE c.publication_id=parent AND c.id=identity AND c.ownership_token=ownership FOR UPDATE;
    IF NOT FOUND THEN RETURN FALSE; END IF;
    IF child.publication_claimed THEN RETURN FALSE; END IF;
    IF child.retired THEN RETURN TRUE; END IF;
    UPDATE hubuum_control.history_publication_owners p SET producer_count=0
        WHERE p.id=child.publication_id AND p.ownership_token=child.publication_ownership
        AND p.producer_count=1;
    IF NOT FOUND THEN RAISE EXCEPTION 'History producer counter is inconsistent'; END IF;
    UPDATE hubuum_control.history_publication_producers SET retired=TRUE WHERE publication_id=parent;
    RETURN TRUE;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_producer_settle(UUID,UUID,UUID) FROM PUBLIC;

-- Productive archive entry already owns application/history/API authority.
-- Child SHARE is FIRST. Contended settlement rejects immediately; no spin,
-- deadline refresh, counter release or inference from contention is permitted.
CREATE FUNCTION public.hubuum_history_producer_execution(parent UUID, identity UUID, ownership UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
BEGIN
    PERFORM 1 FROM hubuum_control.history_publication_producers c
        WHERE c.publication_id=parent AND c.id=identity AND c.ownership_token=ownership
            AND NOT c.retired FOR SHARE NOWAIT;
    RETURN FOUND;
EXCEPTION WHEN lock_not_available THEN
    RETURN FALSE;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_producer_execution(UUID,UUID,UUID) FROM PUBLIC;

-- Claim admission is productive, but needs child UPDATE at its OUTERMOST
-- validator before parent SHARE. NOWAIT prohibits upgrade cycles and retry/spin.
-- Touching that tuple is essential: an old RR settlement snapshot must conflict,
-- rather than miss a claim inserted after its snapshot and retire the parent.
CREATE FUNCTION public.hubuum_history_producer_claim_execution(parent UUID, identity UUID, ownership UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
BEGIN
    PERFORM 1 FROM hubuum_control.history_publication_producers c
        WHERE c.publication_id=parent AND c.id=identity AND c.ownership_token=ownership
            AND NOT c.retired AND NOT c.publication_claimed FOR UPDATE NOWAIT;
    RETURN FOUND;
EXCEPTION WHEN lock_not_available THEN RETURN FALSE;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_producer_claim_execution(UUID,UUID,UUID) FROM PUBLIC;

-- The specialized entry already holds authority/API, child UPDATE NOWAIT and
-- parent SHARE. Capture is the next target. Every same-parent admission holds
-- that exact child lock, so a duplicate cannot wait on the PK behind a settler.
CREATE FUNCTION public.hubuum_history_archive_publication_claim(parent UUID, parent_ownership UUID,
    producer UUID, producer_ownership UUID, runtime UUID, runtime_ownership UUID, manifest_wire BYTEA, encoded_intent BYTEA, expected_capture JSONB)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE manifest JSONB; source UUID; execution_cutoff TIMESTAMPTZ;
BEGIN
    -- Reject aggregate metadata overflow before decoding or touching authority.
    IF manifest_wire IS NULL OR encoded_intent IS NULL
        OR octet_length(manifest_wire)=0 OR octet_length(encoded_intent)=0
        OR octet_length(manifest_wire)>262144 OR octet_length(encoded_intent)>262144
        OR octet_length(manifest_wire)+octet_length(encoded_intent)>262144 THEN RETURN FALSE; END IF;
    manifest := pg_catalog.convert_from(manifest_wire,'UTF8')::JSONB;
    source := (manifest->>'capture')::UUID;
    execution_cutoff := LEAST((manifest->>'available_until')::TIMESTAMPTZ,
        COALESCE((manifest #>> '{execution,deadline}')::TIMESTAMPTZ,
            (manifest->>'available_until')::TIMESTAMPTZ));
    IF execution_cutoff IS NULL OR execution_cutoff<=clock_timestamp() THEN RETURN FALSE; END IF;
    -- Raw SQL callers must also establish original authority before any owner
    -- lock. The specialized SDK entry already owns these exact locks; no
    -- authority is refreshed or substituted after API validation.
    IF (expected_capture->>'runtime')::UUID IS DISTINCT FROM runtime
        OR public.hubuum_application_execution_allowed(runtime,runtime_ownership,
            (expected_capture #>> '{fence,incarnation}')::UUID,
            (expected_capture #>> '{fence,revision}')::BIGINT) IS NOT TRUE THEN RETURN FALSE; END IF;
    PERFORM public.hubuum_history_read_authority(runtime,runtime_ownership,FALSE);
    IF public.hubuum_history_producer_claim_execution(parent,producer,producer_ownership) IS NOT TRUE THEN RETURN FALSE; END IF;
    PERFORM 1 FROM hubuum_control.history_publication_owners p
        WHERE p.id=parent AND p.ownership_token=parent_ownership AND p.producer_count=1
        AND p.kind='archive_read' AND p.runtime_id=runtime
        AND p.incarnation=(expected_capture #>> '{fence,incarnation}')::UUID
        AND p.deletion_revision=(expected_capture #>> '{fence,revision}')::BIGINT
        AND p.history_revision=(SELECT authority_revision FROM hubuum_control.history_lifecycle_control WHERE id=1)
        AND p.generation=(SELECT generation FROM hubuum_control.history_publication_control WHERE id=1) FOR SHARE;
    IF NOT FOUND THEN RETURN FALSE; END IF;
    IF EXISTS(SELECT 1 FROM hubuum_control.history_archive_publication_claims c WHERE c.publication_id=parent)
        OR (manifest #>> '{execution,parent}')::UUID IS DISTINCT FROM parent
        OR (manifest #>> '{execution,child}')::UUID IS DISTINCT FROM producer
        OR manifest->'execution' IS DISTINCT FROM expected_capture->'execution' THEN RETURN FALSE; END IF;
    UPDATE hubuum_control.history_archive_captures c SET publication_claimed=TRUE
        WHERE c.id=source AND NOT c.publication_claimed AND c.expires_at>clock_timestamp()
        AND c.material-'reserved_read_bytes'=expected_capture-'reserved_read_bytes';
    IF NOT FOUND THEN RETURN FALSE; END IF;
    INSERT INTO hubuum_control.history_archive_publication_claims
        (publication_id,publication_ownership,producer_id,producer_ownership,capture_id,material,encoded_intent)
        VALUES(parent,parent_ownership,producer,producer_ownership,source,manifest_wire,encoded_intent);
    UPDATE hubuum_control.history_publication_producers c SET publication_claimed=TRUE
        WHERE c.publication_id=parent AND c.id=producer AND c.ownership_token=producer_ownership;
    RETURN TRUE;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_archive_publication_claim(UUID,UUID,UUID,UUID,UUID,UUID,BYTEA,BYTEA,JSONB) FROM PUBLIC;

-- Producer declarations are durable registrations, never heartbeats. Serialize
-- registration with first intent admission through application/history tuples.
CREATE FUNCTION public.hubuum_history_runtime_register(registration UUID, ownership UUID, isolation_domain JSONB, protocol SMALLINT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE archive_requires_legacy BOOLEAN;
BEGIN
    IF protocol IS NULL OR protocol NOT IN (0,1) THEN RAISE EXCEPTION 'Unsupported history producer protocol'; END IF;
    -- An independently installed archive reader may deliberately support only
    -- Legacy registration. Never advertise the combined producer as OnlineV1
    -- until its real read/publication gate replaces that restriction. The fixed
    -- optional SQL hook avoids an unlanded SDK or archive implementation join.
    IF protocol=1 AND to_regprocedure('public.hubuum_archive_history_requires_legacy_runtime()') IS NOT NULL THEN
        EXECUTE 'SELECT public.hubuum_archive_history_requires_legacy_runtime()' INTO archive_requires_legacy;
        IF archive_requires_legacy IS DISTINCT FROM FALSE THEN RETURN FALSE; END IF;
    END IF;
    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id=1 FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Deletion authority is unavailable; runtime startup denied'; END IF;
    UPDATE hubuum_control.application_control SET incarnation=incarnation WHERE id=1;
    IF NOT FOUND THEN RAISE EXCEPTION 'Application control is unavailable; runtime startup denied'; END IF;
    IF public.hubuum_deletion_exposure_allowed() IS NOT TRUE OR
       (SELECT count(*) FROM hubuum_control.runtime_registrations)>=1024 THEN RETURN FALSE; END IF;
    -- A locking read also rejects a stale repeatable-read snapshot after an
    -- intent changed this tuple; app SHARE/UPDATE waiting alone is insufficient.
    PERFORM 1 FROM hubuum_control.history_lifecycle_control WHERE id=1 FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'History authority is unavailable; runtime startup denied'; END IF;
    IF protocol=0 AND (EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members)
        OR EXISTS(SELECT 1 FROM hubuum_control.history_lifecycle_control WHERE id=1
            AND coalesce(material #> '{authority,expiry}','[]'::JSONB)<>'[]'::JSONB)) THEN RETURN FALSE; END IF;
    INSERT INTO hubuum_control.runtime_registrations(id,ownership_token,isolation,history_protocol)
        VALUES(registration,ownership,isolation_domain,protocol);
    RETURN TRUE;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_runtime_register(UUID,UUID,JSONB,SMALLINT) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.hubuum_register_runtime_isolated(registration UUID, ownership UUID, isolation_domain JSONB)
RETURNS BOOLEAN LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $function$
    SELECT public.hubuum_history_runtime_register(registration,ownership,isolation_domain,0::SMALLINT)
$function$;
CREATE FUNCTION public.hubuum_register_history_runtime(registration UUID, ownership UUID, isolation_domain JSONB)
RETURNS BOOLEAN LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $function$
    SELECT public.hubuum_history_runtime_register(registration,ownership,isolation_domain,1::SMALLINT)
$function$;
REVOKE ALL ON FUNCTION public.hubuum_register_history_runtime(UUID,UUID,JSONB) FROM PUBLIC;

-- Internal protocol check only: this is not intent validation or deletion
-- permission. Native admission is separate from the generic policy CAS below.
CREATE FUNCTION public.hubuum_history_producers_fenced(expected_incarnation UUID, expected_deletion BIGINT)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
BEGIN
    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id=1
        AND (material->>'revision')::BIGINT=expected_deletion FOR SHARE;
    IF NOT FOUND THEN RETURN FALSE; END IF;
    PERFORM 1 FROM hubuum_control.application_control WHERE id=1
        AND incarnation=expected_incarnation FOR SHARE;
    IF NOT FOUND THEN RETURN FALSE; END IF;
    PERFORM 1 FROM hubuum_control.history_lifecycle_control WHERE id=1 FOR UPDATE;
    IF NOT FOUND THEN RETURN FALSE; END IF;
    RETURN NOT EXISTS(SELECT 1 FROM hubuum_control.runtime_registrations WHERE history_protocol<>1);
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_producers_fenced(UUID,BIGINT) FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_version_visible(history_family TEXT, evidence JSONB)
RETURNS BOOLEAN LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE resource UUID; version UUID; captured_references JSONB; unknown_source BOOLEAN;
    source_path TEXT; source_version UUID;
BEGIN
    -- Preserve pre-expiry legacy reads. Once this family has retained authority,
    -- missing correspondence cannot establish that a copy is unrelated.
    IF NOT EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members WHERE family=history_family) THEN RETURN TRUE; END IF;
    IF evidence #>> '{subject,kind}' IS DISTINCT FROM 'resource'
       OR evidence #>> '{subject,identity,state}' IS DISTINCT FROM 'known'
       OR evidence #>> '{subject,identity,identity}' IS NULL
       OR evidence #>> '{subject,version}' IS NULL THEN RETURN FALSE; END IF;
    resource := (evidence #>> '{subject,identity,identity}')::UUID;
    version := (evidence #>> '{subject,version}')::UUID;
    captured_references := coalesce(evidence->'references','{}'::JSONB);
    unknown_source := EXISTS(SELECT 1 FROM jsonb_object_keys(captured_references) AS source_keys(path)
        WHERE starts_with(path,'/restore_unknown_source_versions/'))
        OR (substring(version::TEXT FROM 15 FOR 1)='8'
            AND NOT EXISTS(SELECT 1 FROM jsonb_object_keys(captured_references) AS source_keys(path)
                WHERE starts_with(path,'/restore_source_versions/')));
    IF unknown_source THEN
        RETURN NOT EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members retained
            WHERE retained.family=history_family AND retained.resource_uuid=resource);
    END IF;
    -- Keep exact copies and each captured ancestor as primary-key probes. An
    -- OR over all retained versions makes every ordinary source row scan the
    -- resource's complete exclusion set, including during dependency checks.
    IF EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members retained
        WHERE retained.family=history_family AND retained.resource_uuid=resource
          AND retained.version_uuid=version) THEN RETURN FALSE; END IF;
    FOR source_path IN SELECT path FROM jsonb_object_keys(captured_references) source_keys(path)
        WHERE path ~ '^/restore_source_versions/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    LOOP
        source_version := substring(source_path FROM length('/restore_source_versions/')+1)::UUID;
        IF captured_references->source_path = jsonb_build_object('kind','version','reference',
                jsonb_build_object('state','known','identity',source_version))
            AND EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members retained
                WHERE retained.family=history_family AND retained.resource_uuid=resource
                  AND retained.version_uuid=source_version) THEN RETURN FALSE; END IF;
    END LOOP;
    RETURN TRUE;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_version_visible(TEXT,JSONB) FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_as_of_visible(history_family TEXT, evidence JSONB, at_time TIMESTAMPTZ, selected_from TIMESTAMPTZ)
RETURNS BOOLEAN LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $function$
    SELECT public.hubuum_history_version_visible(history_family,evidence)
       AND NOT EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members
        WHERE family=history_family
          AND resource_uuid=(evidence #>> '{subject,identity,identity}')::UUID
          AND valid_from<=at_time AND (valid_to IS NULL OR valid_to=valid_from OR at_time<valid_to)
          AND valid_from>=selected_from)
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_as_of_visible(TEXT,JSONB,TIMESTAMPTZ,TIMESTAMPTZ) FROM PUBLIC;

-- A physically missing payload cannot supply its own resource identity. Use
-- only the original request binding; never infer it from a different version.
CREATE FUNCTION public.hubuum_history_requested_as_of_visible(history_family TEXT, resource UUID, at_time TIMESTAMPTZ)
RETURNS BOOLEAN LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $function$
    SELECT resource IS NOT NULL AND at_time IS NOT NULL
       AND history_family IN ('collection','class','object','class_relation','object_relation','export_template','remote_target','computed_definitions','class_schema')
       AND NOT EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members
        WHERE family=history_family AND resource_uuid=resource
          AND valid_from<=at_time AND (valid_to IS NULL OR valid_to=valid_from OR at_time<valid_to))
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_requested_as_of_visible(TEXT,UUID,TIMESTAMPTZ) FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_expiry_sync() RETURNS VOID
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
BEGIN
    -- Never discard an exclusion to repair a conflicting or older projection.
    IF EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members retained WHERE NOT EXISTS(
        SELECT 1 FROM hubuum_control.history_lifecycle_control control,
          LATERAL jsonb_array_elements(coalesce(control.material #> '{authority,expiry}','[]'::JSONB)) intent,
          LATERAL jsonb_array_elements(intent #> '{manifest,members}') member
        WHERE member->>'family'=retained.family
          AND (member #>> '{target,resource}')::UUID=retained.resource_uuid
          AND (member #>> '{target,version}')::UUID=retained.version_uuid
          AND (intent #>> '{manifest,identity}')::UUID=retained.intent_uuid
          AND (member #>> '{read_span,valid_from}')::TIMESTAMPTZ=retained.valid_from
          AND (member #>> '{read_span,valid_to}')::TIMESTAMPTZ IS NOT DISTINCT FROM retained.valid_to
    )) THEN RAISE EXCEPTION 'Retained history exclusion conflicts with control authority'; END IF;
    INSERT INTO hubuum_control.history_expiry_members(family,resource_uuid,version_uuid,intent_uuid,valid_from,valid_to)
    SELECT member->>'family',(member #>> '{target,resource}')::UUID,(member #>> '{target,version}')::UUID,
        (intent #>> '{manifest,identity}')::UUID,(member #>> '{read_span,valid_from}')::TIMESTAMPTZ,
        (member #>> '{read_span,valid_to}')::TIMESTAMPTZ
    FROM hubuum_control.history_lifecycle_control control,
      LATERAL jsonb_array_elements(coalesce(control.material #> '{authority,expiry}','[]'::JSONB)) intent,
      LATERAL jsonb_array_elements(intent #> '{manifest,members}') member
    ON CONFLICT (family,resource_uuid,version_uuid) DO NOTHING;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_expiry_sync() FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_lifecycle_capture() RETURNS JSONB
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $function$
    SELECT jsonb_build_object('observed_at', clock_timestamp(), 'control', h.material, 'incarnation', a.incarnation,
        'deletion_revision', d.material->>'revision')
    FROM hubuum_control.deletion_authority d
    CROSS JOIN hubuum_control.application_control a
    CROSS JOIN hubuum_control.history_lifecycle_control h
    WHERE d.id=1 AND a.id=1 AND h.id=1
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_lifecycle_capture() FROM PUBLIC;

-- The operation accepts a validated Rust transition and still fences exact
-- authority and application incarnation in the database. It grants no table access.
CREATE FUNCTION public.hubuum_history_lifecycle_commit(expected JSONB, replacement JSONB, expected_incarnation UUID, expected_deletion BIGINT)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE current_material JSONB; old_authority JSONB; new_authority JSONB;
    previous_count INTEGER; next_count INTEGER; additions INTEGER; pending_completions INTEGER; pending_size INTEGER;
    old_size JSONB; new_size JSONB;
    new_policy JSONB; issued JSONB; old_preview JSONB; next_preview JSONB;
BEGIN
    PERFORM id FROM hubuum_control.deletion_authority WHERE id=1
        AND (material->>'revision')::BIGINT=expected_deletion FOR SHARE;
    IF NOT FOUND THEN RETURN FALSE; END IF;
    PERFORM id FROM hubuum_control.application_control WHERE id=1
        AND incarnation=expected_incarnation FOR SHARE;
    IF NOT FOUND THEN RETURN FALSE; END IF;
    SELECT material INTO current_material FROM hubuum_control.history_lifecycle_control WHERE id=1 FOR UPDATE;
    IF current_material IS DISTINCT FROM expected THEN RETURN FALSE; END IF;
    IF replacement=current_material THEN RETURN TRUE; END IF;
    -- Generic policy/hold CAS preserves retained expiry byte-for-byte. Native
    -- intent/outcome admission needs its separate protected executor boundary.
    IF replacement #> '{authority,expiry}' IS DISTINCT FROM current_material #> '{authority,expiry}'
       THEN RETURN FALSE; END IF;
    IF jsonb_typeof(replacement) IS DISTINCT FROM 'object'
       OR replacement - ARRAY['authority','previews'] <> '{}'::jsonb
       OR jsonb_typeof(replacement->'authority') IS DISTINCT FROM 'object'
       OR (replacement->'authority') - ARRAY['revision','policies','holds','expiry','size_transitions'] <> '{}'::jsonb
       OR jsonb_typeof(replacement #> '{authority,policies}') IS DISTINCT FROM 'array'
       OR jsonb_typeof(replacement #> '{authority,holds}') IS DISTINCT FROM 'array'
       OR jsonb_typeof(coalesce(replacement #> '{authority,size_transitions}','[]')) IS DISTINCT FROM 'array'
       OR jsonb_typeof(replacement->'previews') IS DISTINCT FROM 'array'
       OR octet_length(replacement::text)>4194304 THEN RETURN FALSE; END IF;
    old_authority := current_material->'authority';
    new_authority := replacement->'authority';
    old_size := coalesce(old_authority->'size_transitions','[]'::JSONB);
    new_size := coalesce(new_authority->'size_transitions','[]'::JSONB);
    IF jsonb_array_length(new_size)>1024 THEN RETURN FALSE; END IF;
    SELECT count(*) INTO pending_size FROM (
        SELECT DISTINCT ON (value #> '{pressure,policy}') value
        FROM jsonb_array_elements(new_size) WITH ORDINALITY AS entries(value,ordinal)
        ORDER BY value #> '{pressure,policy}',ordinal DESC
    ) latest WHERE value #>> '{pressure,reclaiming}'='true';
    IF jsonb_array_length(new_size)+pending_size>1024 THEN RETURN FALSE; END IF;
    IF jsonb_array_length(new_authority->'policies')>4096 OR jsonb_array_length(new_authority->'holds')>4096
       OR jsonb_array_length(replacement->'previews')>256 THEN RETURN FALSE; END IF;
    SELECT count(*) INTO pending_completions FROM (
        SELECT DISTINCT ON (value #>> '{hold,identity}') value
        FROM jsonb_array_elements(new_authority->'holds') WITH ORDINALITY AS entries(value,ordinal)
        ORDER BY value #>> '{hold,identity}',ordinal DESC
    ) latest WHERE value->>'transition'='created';
    IF jsonb_array_length(new_authority->'holds')+pending_completions>4096
       OR octet_length(replacement::text)+(pending_completions+pending_size)*1024>4194304 THEN RETURN FALSE; END IF;
    -- Prefix comparisons preserve order and exact immutable records. JSONB
    -- containment alone would incorrectly accept reordered or rewritten history.
    IF (SELECT coalesce(jsonb_agg(value ORDER BY ordinal),'[]') FROM jsonb_array_elements(new_authority->'policies') WITH ORDINALITY AS entries(value,ordinal)
          WHERE ordinal<=jsonb_array_length(old_authority->'policies')) <> old_authority->'policies'
       OR (SELECT coalesce(jsonb_agg(value ORDER BY ordinal),'[]') FROM jsonb_array_elements(new_authority->'holds') WITH ORDINALITY AS entries(value,ordinal)
          WHERE ordinal<=jsonb_array_length(old_authority->'holds')) <> old_authority->'holds'
       OR (SELECT coalesce(jsonb_agg(value ORDER BY ordinal),'[]') FROM jsonb_array_elements(new_size) WITH ORDINALITY AS entries(value,ordinal)
          WHERE ordinal<=jsonb_array_length(old_size)) <> old_size THEN RETURN FALSE; END IF;
    previous_count := jsonb_array_length(old_authority->'policies')+jsonb_array_length(old_authority->'holds')+jsonb_array_length(old_size);
    next_count := jsonb_array_length(new_authority->'policies')+jsonb_array_length(new_authority->'holds')+jsonb_array_length(new_size);
    additions := next_count-previous_count;
    IF additions NOT IN (0,1) OR jsonb_typeof(new_authority->'revision') IS DISTINCT FROM 'string'
       OR new_authority->>'revision' !~ '^(0|[1-9][0-9]*)$'
       OR (new_authority->>'revision')::BIGINT<>(old_authority->>'revision')::BIGINT+additions THEN RETURN FALSE; END IF;
    IF additions=0 AND new_authority<>old_authority THEN RETURN FALSE; END IF;
    IF additions=1 AND replacement->'previews'<>current_material->'previews' THEN RETURN FALSE; END IF;
    IF jsonb_array_length(new_authority->'policies')>jsonb_array_length(old_authority->'policies') THEN
        new_policy := new_authority->'policies'->-1;
        IF new_policy #>> '{proposal,application,kind}'='retrospective' AND
           (new_policy->'review' IS NULL OR new_policy->'review'='null'::jsonb) THEN RETURN FALSE; END IF;
        IF new_policy->'review' IS NOT NULL AND new_policy->'review'<>'null'::jsonb THEN
            SELECT value INTO issued FROM jsonb_array_elements(current_material->'previews')
                WHERE value->'reference'=new_policy #> '{review,reference}';
            IF issued IS NULL OR issued->'proposal'<>new_policy->'proposal'
               OR issued->'principal'<>new_policy->'principal'
               OR new_policy->'review'<>jsonb_build_object('reference',issued->'reference','principal',issued->'principal',
                    'fence',issued->'fence','issued_at',issued->'issued_at','expires_at',issued->'expires_at','coverage',issued #> '{impact,coverage}')
               OR issued #>> '{fence,incarnation}'<>expected_incarnation::TEXT
               OR issued #>> '{fence,authority_revision}'<>old_authority->>'revision'
               OR issued #>> '{fence,deletion_revision}'<>expected_deletion::TEXT
               OR (issued->>'issued_at')::TIMESTAMPTZ>clock_timestamp()
               OR (issued->>'expires_at')::TIMESTAMPTZ<=clock_timestamp() THEN RETURN FALSE; END IF;
            -- A natural hold expiry changes protection even without a write.
            IF EXISTS (SELECT 1 FROM (
                SELECT DISTINCT ON (value #>> '{hold,identity}') value->'hold' AS hold
                FROM jsonb_array_elements(old_authority->'holds') WITH ORDINALITY AS entries(value,ordinal)
                ORDER BY value #>> '{hold,identity}',ordinal DESC
            ) latest WHERE (hold->>'created_at')::TIMESTAMPTZ <= (issued->>'issued_at')::TIMESTAMPTZ
                AND (hold->>'released_at' IS NULL OR (hold->>'released_at')::TIMESTAMPTZ>(issued->>'issued_at')::TIMESTAMPTZ)
                AND (hold->>'expires_at')::TIMESTAMPTZ>(issued->>'issued_at')::TIMESTAMPTZ
                AND (hold->>'expires_at')::TIMESTAMPTZ<=clock_timestamp()) THEN RETURN FALSE; END IF;
        END IF;
    END IF;
    IF additions=0 THEN
        -- Old issuance records can only be retained byte-for-byte or pruned
        -- after their original expiry; they can never be renewed or rewritten.
        FOR old_preview IN SELECT value FROM jsonb_array_elements(current_material->'previews') LOOP
            SELECT value INTO next_preview FROM jsonb_array_elements(replacement->'previews')
                WHERE value #>> '{reference,preview_id}'=old_preview #>> '{reference,preview_id}';
            IF next_preview IS NULL THEN
                IF (old_preview->>'expires_at')::TIMESTAMPTZ>clock_timestamp() THEN RETURN FALSE; END IF;
            ELSIF next_preview<>old_preview THEN RETURN FALSE;
            END IF;
        END LOOP;
    END IF;
    UPDATE hubuum_control.history_lifecycle_control SET material=replacement WHERE id=1;
    RETURN TRUE;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_lifecycle_commit(JSONB,JSONB,UUID,BIGINT) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.hubuum_task_control_capture() RETURNS JSON
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $$
    WITH position AS MATERIALIZED (SELECT pg_current_wal_insert_lsn() AS lsn)
    SELECT json_build_object('history_lifecycle',(SELECT material->'authority' FROM hubuum_control.history_lifecycle_control WHERE id=1),
        'task_control',public.hubuum_task_control_read(),
        'deletion',d.material,'application',public.hubuum_application_control_read(),
        'system_identifier',(pg_control_system()).system_identifier::text,
        'wal_file',pg_walfile_name(position.lsn),'lsn',position.lsn::text,
        'task_reapplication',CASE WHEN c.reapplied_revision<=c.revision AND c.reapplied_incarnation=a.incarnation
           THEN json_build_object('incarnation',c.reapplied_incarnation,'task_revision',c.reapplied_revision) END)
    FROM hubuum_control.deletion_authority d CROSS JOIN hubuum_control.task_retention_control c
    CROSS JOIN hubuum_control.application_control a CROSS JOIN position
    WHERE d.id=1 AND c.id=1 AND a.id=1
$$;

CREATE OR REPLACE FUNCTION public.hubuum_rotate_application_for_restore()
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE authority JSONB; empty_authority BOOLEAN; task_activation JSONB;
BEGIN
    SELECT material INTO STRICT authority FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE;
    UPDATE hubuum_control.deletion_authority SET material=material WHERE id=1;
    SELECT activation INTO STRICT task_activation FROM hubuum_control.task_retention_control WHERE id=1 FOR UPDATE;
    UPDATE hubuum_control.task_retention_control SET reapplied_incarnation=NULL,reapplied_revision=NULL WHERE id=1;
    empty_authority := authority->'intents'='{}'::jsonb AND task_activation->>'state'='disabled'
        AND (SELECT material #>> '{authority,revision}'='0' FROM hubuum_control.history_lifecycle_control WHERE id=1);
    UPDATE hubuum_control.application_control SET incarnation=gen_random_uuid(),reapplication_revision=NULL,
        released_revision=CASE WHEN empty_authority THEN (authority->>'revision')::bigint END,
        checkpoint=NULL,pending_restore=NOT empty_authority WHERE id=1;
    RETURN empty_authority;
END $$;


-- Claimed capture pins are not retired by expiry/accounting cleanup.
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
        (SELECT id FROM hubuum_control.history_archive_captures WHERE expires_at<=clock_timestamp() AND NOT publication_claimed ORDER BY expires_at,id LIMIT 16 FOR UPDATE) RETURNING bytes LOOP
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

-- The combined adapter now enters archive work through the same publication
-- parent/producer boundary as history. Legacy registrations remain usable only
-- before retained expiry; the old hook stays present for older startup code.
CREATE OR REPLACE FUNCTION public.hubuum_archive_history_requires_legacy_runtime() RETURNS boolean
LANGUAGE sql IMMUTABLE SECURITY DEFINER SET search_path=pg_catalog AS $$ SELECT FALSE $$;
CREATE OR REPLACE FUNCTION public.hubuum_archive_history_entry(registration uuid, ownership uuid, image uuid, revision bigint)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE protocol smallint;
BEGIN
    IF registration IS NULL OR ownership IS NULL OR
       public.hubuum_application_execution_allowed(registration,ownership,image,revision) IS NOT TRUE THEN RETURN FALSE; END IF;
    protocol := public.hubuum_history_read_authority(registration,ownership,FALSE);
    RETURN protocol IN (0,1);
END $$;

CREATE FUNCTION public.hubuum_history_archive_resource_available(subject uuid) RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
    SELECT NOT EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members WHERE family='collection' AND resource_uuid=subject)
$$;
REVOKE ALL ON FUNCTION public.hubuum_history_archive_resource_available(uuid) FROM PUBLIC;

-- Protected current/open copies and retained schema revisions cannot be turned
-- into gaps by intent admission. Missing correspondence fails closed.
CREATE FUNCTION public.hubuum_history_expiry_dependency(history_family TEXT, evidence JSONB)
RETURNS BOOLEAN LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE current_table TEXT; history_table TEXT; dependent BOOLEAN; predicate TEXT;
BEGIN
    CASE history_family
      WHEN 'collection' THEN current_table:='collections'; history_table:='collections_history';
      WHEN 'class' THEN current_table:='hubuumclass'; history_table:='hubuumclass_history';
      WHEN 'object' THEN current_table:='hubuumobject'; history_table:='hubuumobject_history';
      WHEN 'class_relation' THEN current_table:='hubuumclass_relation'; history_table:='hubuumclass_relation_history';
      WHEN 'object_relation' THEN current_table:='hubuumobject_relation'; history_table:='hubuumobject_relation_history';
      WHEN 'export_template' THEN current_table:='export_templates'; history_table:='export_templates_history';
      WHEN 'remote_target' THEN current_table:='remote_targets'; history_table:='remote_targets_history';
      WHEN 'class_schema' THEN current_table:='class_schema_revisions';
      WHEN 'computed_definitions' THEN history_table:='computed_field_history';
      ELSE RAISE EXCEPTION 'Unsupported expiry family';
    END CASE;
    predicate := $predicate$(c.logical_identity #>> '{subject,identity,identity}' IS NULL
       OR (c.logical_identity #>> '{subject,identity,identity}'=$1 #>> '{subject,identity,identity}'
       AND (c.logical_identity #>> '{subject,version}'=$1 #>> '{subject,version}'
         OR public.hubuum_history_version_visible($2,c.logical_identity) IS NOT TRUE
         OR EXISTS(SELECT 1 FROM jsonb_object_keys(coalesce(c.logical_identity->'references','{}')) paths(path)
             WHERE starts_with(path,'/restore_unknown_source_versions/'))
         OR (substring(c.logical_identity #>> '{subject,version}' FROM 15 FOR 1)='8'
           AND NOT EXISTS(SELECT 1 FROM jsonb_object_keys(coalesce(c.logical_identity->'references','{}')) paths(path)
             WHERE starts_with(path,'/restore_source_versions/')))
         OR c.logical_identity->'references'->('/restore_source_versions/'||($1 #>> '{subject,version}'))
            =jsonb_build_object('kind','version','reference',jsonb_build_object('state','known','identity',$1 #>> '{subject,version}')))))$predicate$;
    IF history_family='class_schema' THEN
      -- Event and revision version UUIDs differ. Bind dependencies to
      -- their captured class UUID, never a reusable numeric class identifier.
      predicate := $class$(c.logical_identity #>> '{references,/class_id,reference,identity}' IS NULL
        OR c.logical_identity #>> '{references,/class_id,reference,identity}'=$1 #>> '{references,/class_id,reference,identity}')$class$;
    END IF;
    IF current_table IS NOT NULL THEN
      EXECUTE format('SELECT EXISTS(SELECT 1 FROM public.%I c WHERE %s)',current_table,predicate)
        INTO dependent USING evidence,history_family;
      IF dependent THEN RETURN TRUE; END IF;
    END IF;
    IF history_table IS NOT NULL THEN
      EXECUTE format('SELECT EXISTS(SELECT 1 FROM public.%I c WHERE valid_to IS NULL %s AND %s)',history_table,
          CASE WHEN history_family='computed_definitions' THEN '' ELSE 'AND op<>''D''' END,predicate)
        INTO dependent USING evidence,history_family;
      IF dependent THEN RETURN TRUE; END IF;
    END IF;
    RETURN FALSE;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_expiry_dependency(TEXT,JSONB) FROM PUBLIC;

-- Separate from policy/hold CAS: exact predecessor, one immutable new intent,
-- native source correspondence and the fleet fence are required together.
-- Rust validates policy/hold decisions while holding these same authority locks.
-- This function neither authorizes removal nor acknowledges independent protection.
CREATE FUNCTION public.hubuum_history_expiry_admit(expected JSONB, replacement JSONB, expected_incarnation UUID, expected_deletion BIGINT)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE current_material JSONB; old_expiry JSONB; new_expiry JSONB; additions JSONB;
  intent JSONB; manifest JSONB; member JSONB; source JSONB; source_table TEXT;
  source_from TIMESTAMPTZ; source_until TIMESTAMPTZ; source_bytes BIGINT; total_bytes BIGINT:=0;
BEGIN
    IF public.hubuum_history_producers_fenced(expected_incarnation,expected_deletion) IS NOT TRUE THEN RETURN FALSE; END IF;
    SELECT material INTO current_material FROM hubuum_control.history_lifecycle_control WHERE id=1 FOR UPDATE;
    IF current_material IS DISTINCT FROM expected OR replacement IS NULL
      OR jsonb_typeof(replacement) IS DISTINCT FROM 'object'
      OR replacement - 'authority' IS DISTINCT FROM current_material - 'authority'
      OR (replacement->'authority') - ARRAY['revision','expiry'] IS DISTINCT FROM (current_material->'authority') - ARRAY['revision','expiry']
      OR (replacement #>> '{authority,revision}')::BIGINT<>(current_material #>> '{authority,revision}')::BIGINT+1
      OR octet_length(replacement::text)>4194304 THEN RETURN FALSE; END IF;
    old_expiry:=coalesce(current_material #> '{authority,expiry}','[]'::JSONB);
    new_expiry:=replacement #> '{authority,expiry}';
    IF jsonb_typeof(new_expiry) IS DISTINCT FROM 'array' OR jsonb_array_length(new_expiry)<>jsonb_array_length(old_expiry)+1
       OR jsonb_array_length(new_expiry)>256 THEN RETURN FALSE; END IF;
    SELECT jsonb_agg(candidate.value) INTO additions FROM jsonb_array_elements(new_expiry) candidate(value)
      WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements(old_expiry) retained(value)
        WHERE retained.value #>> '{manifest,identity}'=candidate.value #>> '{manifest,identity}');
    IF jsonb_array_length(additions) IS DISTINCT FROM 1 THEN RETURN FALSE; END IF;
    intent:=additions->0; manifest:=intent->'manifest';
    IF (SELECT coalesce(jsonb_agg(value ORDER BY ordinal),'[]') FROM jsonb_array_elements(new_expiry) WITH ORDINALITY entries(value,ordinal)
        WHERE value #>> '{manifest,identity}'<>manifest->>'identity') IS DISTINCT FROM old_expiry
      OR coalesce(intent->'removals','[]')<>'[]'::JSONB OR intent->'cancellation' IS NOT NULL AND intent->'cancellation'<>'null'::JSONB
      OR manifest #>> '{fence,incarnation}'<>expected_incarnation::TEXT
      OR manifest #>> '{fence,deletion_revision}'<>expected_deletion::TEXT
      OR manifest #>> '{fence,authority_revision}'<>current_material #>> '{authority,revision}'
      OR (manifest->>'selected_at')::TIMESTAMPTZ>clock_timestamp()
      OR jsonb_array_length(manifest->'members') NOT BETWEEN 1 AND 1000
      OR octet_length(manifest::text)>1048576 THEN RETURN FALSE; END IF;
    FOR member IN SELECT value FROM jsonb_array_elements(manifest->'members') LOOP
      source_table:=CASE member->>'family'
        WHEN 'collection' THEN 'collections_history' WHEN 'class' THEN 'hubuumclass_history'
        WHEN 'object' THEN 'hubuumobject_history' WHEN 'class_relation' THEN 'hubuumclass_relation_history'
        WHEN 'object_relation' THEN 'hubuumobject_relation_history' WHEN 'export_template' THEN 'export_templates_history'
        WHEN 'remote_target' THEN 'remote_targets_history' WHEN 'class_schema' THEN 'class_schema_history'
        WHEN 'computed_definitions' THEN 'computed_field_history' ELSE NULL END;
      IF source_table IS NULL OR member->>'family'<>manifest #>> '{key,family}' THEN RETURN FALSE; END IF;
      EXECUTE format('SELECT to_jsonb(h.*) FROM public.%I h WHERE logical_identity #>> ''{subject,identity,identity}''=$1 AND logical_identity #>> ''{subject,version}''=$2 FOR SHARE',source_table)
        INTO STRICT source USING member #>> '{target,resource}',member #>> '{target,version}';
      source_bytes:=octet_length(source::TEXT); total_bytes:=total_bytes+source_bytes;
      source_from:=(source->>CASE WHEN member->>'family'='class_schema' THEN 'occurred_at' ELSE 'valid_from' END)::TIMESTAMPTZ;
      source_until:=(source->>'valid_to')::TIMESTAMPTZ;
      IF source_from IS DISTINCT FROM (member #>> '{read_span,valid_from}')::TIMESTAMPTZ
        OR source_until IS DISTINCT FROM (member #>> '{read_span,valid_to}')::TIMESTAMPTZ
        OR source_bytes<>(member->>'input_bytes')::BIGINT OR total_bytes>16777216
        OR (source->>'op'='D' AND (member #>> '{read_span,kind}' IS DISTINCT FROM 'deleted'
          OR (source_until IS NOT NULL AND source_until<>source_from)))
        OR public.hubuum_history_expiry_dependency(member->>'family',source->'logical_identity')
        OR (member->>'family'<>'class_schema' AND source_until IS NULL AND source->>'op' IS DISTINCT FROM 'D')
        OR EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members retained
           WHERE retained.family=member->>'family' AND retained.resource_uuid=(member #>> '{target,resource}')::UUID
             AND retained.version_uuid=(member #>> '{target,version}')::UUID) THEN RETURN FALSE; END IF;
    END LOOP;
    IF total_bytes<>(manifest->>'input_bytes')::BIGINT THEN RETURN FALSE; END IF;
    UPDATE hubuum_control.history_lifecycle_control SET material=replacement WHERE id=1;
    PERFORM public.hubuum_history_expiry_sync();
    RETURN TRUE;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_expiry_admit(JSONB,JSONB,UUID,BIGINT) FROM PUBLIC;

-- Retained control uses Chrono/Serde's UTC RFC3339 AutoSi encoding. JSONB
-- timestamps otherwise keep +00:00 or a different fractional precision, which
-- breaks later exact predecessor CAS even when the parsed instants are equal.
CREATE FUNCTION public.hubuum_history_expiry_clock(at_time TIMESTAMPTZ) RETURNS TEXT
LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog AS $function$
    SELECT to_char(at_time AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS')
        || CASE WHEN EXTRACT(MICROSECONDS FROM at_time)::BIGINT % 1000000=0 THEN ''
             WHEN EXTRACT(MICROSECONDS FROM at_time)::BIGINT % 1000=0
               THEN '.' || to_char(at_time AT TIME ZONE 'UTC','MS')
             ELSE '.' || to_char(at_time AT TIME ZONE 'UTC','US') END || 'Z'
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_expiry_clock(TIMESTAMPTZ) FROM PUBLIC;

-- One exact physical outcome and its first receipt commit together. The trusted
-- adapter rechecks policy/holds in this transaction before calling this function.
-- No current/open copy, absence, clock expiry or missing runtime retires a producer.
CREATE FUNCTION public.hubuum_history_expiry_remove(expected JSONB, batch UUID,
    resource UUID, version UUID, checkpoint TEXT, expected_incarnation UUID, expected_deletion BIGINT)
RETURNS JSONB LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE material JSONB; intent JSONB; member JSONB; source JSONB; source_table TEXT;
    replacement JSONB; updated_intent JSONB; receipts JSONB; removed_at TIMESTAMPTZ;
    affected BIGINT; family TEXT; quarantined BOOLEAN; already_removed BOOLEAN;
BEGIN
    -- Restore needs UPDATE on this gate. Keep productive entry's SHARE mode:
    -- upgrading after history authority would deadlock with a reader holding
    -- this gate while waiting for history authority. The history tuple below
    -- serializes removals and runtime registration. Replay uses no old lease.
    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id=1 FOR SHARE;
    quarantined:=public.hubuum_deletion_exposure_allowed() IS FALSE;
    IF NOT quarantined AND public.hubuum_history_producers_fenced(expected_incarnation,expected_deletion) IS NOT TRUE THEN
        RAISE EXCEPTION 'History producer fencing is unavailable';
    END IF;
    IF quarantined THEN
        PERFORM 1 FROM hubuum_control.deletion_authority c WHERE id=1
            AND (c.material->>'revision')::BIGINT=expected_deletion;
        IF NOT FOUND THEN RAISE EXCEPTION 'History replay deletion image changed'; END IF;
        PERFORM 1 FROM hubuum_control.application_control WHERE id=1
            AND incarnation=expected_incarnation FOR SHARE;
        IF NOT FOUND THEN RAISE EXCEPTION 'History replay application image changed'; END IF;
    END IF;
    IF quarantined AND (EXISTS(SELECT 1 FROM hubuum_control.runtime_registrations
        WHERE drained_incarnation IS DISTINCT FROM expected_incarnation OR drained_revision IS DISTINCT FROM expected_deletion)
        OR EXISTS(SELECT 1 FROM hubuum_control.history_publication_owners)) THEN
        RAISE EXCEPTION 'History replay requires positive runtime and publication drain';
    END IF;
    SELECT c.material INTO material FROM hubuum_control.history_lifecycle_control c WHERE id=1 FOR UPDATE;
    IF material IS DISTINCT FROM expected OR checkpoint !~ '^[0-9a-f]{64}$' OR checkpoint IS NULL THEN
        RAISE EXCEPTION 'History removal predecessor or checkpoint changed';
    END IF;
    SELECT value INTO STRICT intent FROM jsonb_array_elements(material #> '{authority,expiry}')
        WHERE value #>> '{manifest,identity}'=batch::TEXT;
    IF NOT quarantined AND (intent #>> '{manifest,fence,incarnation}'<>expected_incarnation::TEXT
        OR intent #>> '{manifest,fence,deletion_revision}'<>expected_deletion::TEXT
        OR EXISTS(SELECT 1 FROM hubuum_control.history_publication_owners p
          WHERE p.incarnation<>expected_incarnation
             OR p.history_revision<=(intent #>> '{manifest,fence,authority_revision}')::BIGINT)) THEN
        RAISE EXCEPTION 'Pre-exclusion publication has not positively retired';
    END IF;
    SELECT value INTO STRICT member FROM jsonb_array_elements(intent #> '{manifest,members}')
        WHERE value #>> '{target,resource}'=resource::TEXT AND value #>> '{target,version}'=version::TEXT;
    already_removed:=EXISTS(SELECT 1 FROM jsonb_array_elements(intent->'removals') receipt
        WHERE receipt #>> '{target,resource}'=resource::TEXT AND receipt #>> '{target,version}'=version::TEXT);
    IF already_removed AND NOT quarantined THEN
        RAISE EXCEPTION 'History copy already has its first receipt';
    END IF;
    family:=member->>'family';
    source_table:=CASE family
        WHEN 'collection' THEN 'collections_history' WHEN 'class' THEN 'hubuumclass_history'
        WHEN 'object' THEN 'hubuumobject_history' WHEN 'class_relation' THEN 'hubuumclass_relation_history'
        WHEN 'object_relation' THEN 'hubuumobject_relation_history' WHEN 'export_template' THEN 'export_templates_history'
        WHEN 'remote_target' THEN 'remote_targets_history' WHEN 'class_schema' THEN 'class_schema_history'
        WHEN 'computed_definitions' THEN 'computed_field_history' ELSE NULL END;
    IF source_table IS NULL THEN RAISE EXCEPTION 'Unsupported history family'; END IF;
    EXECUTE format('SELECT to_jsonb(h.*) FROM public.%I h WHERE logical_identity #>> ''{subject,identity,identity}''=$1 AND logical_identity #>> ''{subject,version}''=$2 FOR UPDATE',source_table)
        INTO source USING resource::TEXT,version::TEXT;
    IF source IS NULL THEN
        RAISE EXCEPTION 'History copy is absent without an exact physical outcome';
    END IF;
    IF octet_length(source::TEXT)>16777216
        OR (source->>CASE WHEN family='class_schema' THEN 'occurred_at' ELSE 'valid_from' END)::TIMESTAMPTZ
            IS DISTINCT FROM (member #>> '{read_span,valid_from}')::TIMESTAMPTZ
        OR (source->>'valid_to')::TIMESTAMPTZ IS DISTINCT FROM (member #>> '{read_span,valid_to}')::TIMESTAMPTZ
        OR (family<>'class_schema' AND source->>'valid_to' IS NULL AND source->>'op' IS DISTINCT FROM 'D')
        OR public.hubuum_history_expiry_dependency(family,source->'logical_identity') THEN
        RAISE EXCEPTION 'History copy changed or has a protected dependency';
    END IF;
    EXECUTE format('DELETE FROM public.%I WHERE logical_identity #>> ''{subject,identity,identity}''=$1 AND logical_identity #>> ''{subject,version}''=$2',source_table)
        USING resource::TEXT,version::TEXT;
    GET DIAGNOSTICS affected=ROW_COUNT;
    IF affected<>1 THEN RAISE EXCEPTION 'Exact history removal did not delete one copy'; END IF;
    removed_at:=clock_timestamp();
    IF removed_at<(intent #>> '{manifest,selected_at}')::TIMESTAMPTZ THEN
        RAISE EXCEPTION 'History removal clock precedes selection';
    END IF;
    IF already_removed THEN
        -- A restored exact copy is removed again, but its first receipt and
        -- canonical audit identity remain byte-for-byte unchanged.
        RETURN jsonb_build_object('affected_rows',affected,'removed_at',removed_at,'control',material);
    END IF;
    SELECT jsonb_agg(receipt ORDER BY receipt #>> '{target,resource}',receipt #>> '{target,version}') INTO receipts
        FROM jsonb_array_elements((intent->'removals') || jsonb_build_array(jsonb_build_object(
            'target',member->'target','checkpoint',checkpoint,'removed_at',public.hubuum_history_expiry_clock(removed_at)))) receipt;
    updated_intent:=jsonb_set(intent,'{removals}',receipts);
    SELECT jsonb_set(material,'{authority,expiry}',jsonb_agg(CASE
        WHEN value #>> '{manifest,identity}'=batch::TEXT THEN updated_intent ELSE value END ORDER BY ordinal))
        INTO replacement FROM jsonb_array_elements(material #> '{authority,expiry}') WITH ORDINALITY entries(value,ordinal);
    replacement:=jsonb_set(replacement,'{authority,revision}',to_jsonb(((material #>> '{authority,revision}')::BIGINT+1)::TEXT));
    IF octet_length(replacement::TEXT)>4194304 THEN RAISE EXCEPTION 'History completion capacity exhausted'; END IF;
    UPDATE hubuum_control.history_lifecycle_control SET material=replacement WHERE id=1;
    RETURN jsonb_build_object('affected_rows',affected,'removed_at',removed_at,'control',replacement);
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_expiry_remove(JSONB,UUID,UUID,UUID,TEXT,UUID,BIGINT) FROM PUBLIC;

-- Capture policy attribution at version creation. Never derive an older
-- historical version's scope from today's class/object location.
CREATE FUNCTION public.hubuum_history_policy_references(relation_name TEXT, source_row JSONB, previous_row JSONB, captured JSONB)
RETURNS JSONB LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE result JSONB:='{}'; inherited JSONB; endpoint TEXT; endpoint_row JSONB;
  collection_reference JSONB; class_reference JSONB; column_name TEXT;
BEGIN
    IF relation_name NOT IN ('hubuumclass_relation','hubuumobject_relation','hubuumclass_relation_history',
       'hubuumobject_relation_history','computed_field_history','class_schema_revisions','class_schema_history') THEN RETURN result; END IF;
    IF previous_row IS NOT NULL AND (relation_name LIKE '%\_history' ESCAPE '\'
       OR captured #>> '{subject,version}'=previous_row #>> '{logical_identity,subject,version}') THEN
        -- Closure and same-version refresh preserve evidence, including its
        -- absence. A metadata backfill must not manufacture historical scope.
        inherited:=previous_row #> '{logical_identity,references}';
    ELSIF relation_name IN ('hubuumclass_relation_history','hubuumobject_relation_history') THEN
        inherited:=source_row #> '{logical_identity,references}';
    ELSIF relation_name='class_schema_history' THEN
        inherited:=coalesce(source_row #> '{logical_identity,references}',source_row #> '{snapshot,logical_identity,references}');
    END IF;
    IF inherited IS NOT NULL THEN
        SELECT coalesce(jsonb_object_agg(key,value),'{}') INTO result FROM jsonb_each(inherited)
          WHERE key IN ('/collection_id','/class_id') OR starts_with(key,'/lifecycle/');
        RETURN result;
    END IF;
    IF previous_row IS NOT NULL AND relation_name LIKE '%\_history' ESCAPE '\' THEN RETURN result; END IF;
    IF relation_name IN ('hubuumclass_relation_history','hubuumobject_relation_history','class_schema_history') THEN RETURN result; END IF;
    IF relation_name IN ('computed_field_history','class_schema_revisions') THEN
        SELECT c.logical_identity INTO endpoint_row FROM public.hubuumclass c WHERE c.id=(source_row->>'class_id')::INTEGER
          AND c.logical_identity #>> '{subject,identity,identity}'=captured #>> '{references,/class_id,reference,identity}';
        collection_reference:=endpoint_row #> '{references,/collection_id}';
        IF collection_reference IS NOT NULL THEN result:=jsonb_build_object('/collection_id',collection_reference); END IF;
        RETURN result;
    END IF;
    FOREACH endpoint IN ARRAY ARRAY['from','to'] LOOP
        IF relation_name='hubuumclass_relation' THEN
            column_name:=endpoint||'_hubuum_class_id';
            SELECT c.logical_identity INTO endpoint_row FROM public.hubuumclass c WHERE c.id=(source_row->>column_name)::INTEGER;
            collection_reference:=endpoint_row #> '{references,/collection_id}';
            class_reference:=CASE WHEN endpoint_row->'subject'->'identity' IS NULL THEN NULL ELSE
                jsonb_build_object('kind','resource','family','class','reference',endpoint_row #> '{subject,identity}') END;
        ELSE
            column_name:=endpoint||'_hubuum_object_id';
            SELECT o.logical_identity INTO endpoint_row FROM public.hubuumobject o WHERE o.id=(source_row->>column_name)::INTEGER;
            collection_reference:=endpoint_row #> '{references,/collection_id}';
            class_reference:=endpoint_row #> '{references,/class_id}';
        END IF;
        IF collection_reference IS NOT NULL AND class_reference IS NOT NULL THEN
            result:=result||jsonb_build_object('/lifecycle/'||endpoint||'/collection',collection_reference,
                                             '/lifecycle/'||endpoint||'/class',class_reference);
        END IF;
    END LOOP;
    RETURN result;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_policy_references(TEXT,JSONB,JSONB,JSONB) FROM PUBLIC;

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
    NEW.logical_identity := jsonb_set(NEW.logical_identity,'{references}',
        coalesce(NEW.logical_identity->'references','{}') || public.hubuum_history_policy_references(
            TG_TABLE_NAME,source_row,previous_row,NEW.logical_identity));
    RETURN NEW;
END
$function$;

-- Coordinated closed task vocabulary upgrade. Preserve all configured limits,
-- scheduler lanes and reservations. Upgrade all workers before this policy is
-- read; production history expiry remains unadvertised and disabled.
SELECT singleton FROM hubuum_control.task_policy WHERE singleton FOR UPDATE;
SELECT public.hubuum_validate_task_policy(policy) FROM hubuum_control.task_policy WHERE singleton;
ALTER TABLE public.tasks DROP CONSTRAINT tasks_kind_check; -- hubuum-compat: widen-enum-check
ALTER TABLE public.tasks ADD CONSTRAINT tasks_kind_check CHECK (kind IN ('import','export','backup','reindex','remote_call','schema_validation','schema_profiling','history_expiry')) NOT VALID;
ALTER TABLE public.tasks VALIDATE CONSTRAINT tasks_kind_check;
ALTER TABLE hubuum_control.task_accounting DROP CONSTRAINT task_accounting_kind_check; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE hubuum_control.task_accounting ADD CONSTRAINT task_accounting_kind_check CHECK (kind IN ('import','export','backup','reindex','remote_call','schema_validation','schema_profiling','history_expiry')) NOT VALID;
ALTER TABLE hubuum_control.task_accounting VALIDATE CONSTRAINT task_accounting_kind_check;

ALTER TABLE public.tasks DROP CONSTRAINT tasks_wait_shape; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE public.tasks ADD CONSTRAINT tasks_wait_shape CHECK (
    (status = 'waiting') = (wait_token IS NOT NULL)
    AND (wait_token IS NULL OR wait_token <> '00000000-0000-0000-0000-000000000000'::UUID)
    AND (status <> 'waiting' OR task_checkpoint IS NOT NULL)
    AND (task_checkpoint IS NULL OR (
        kind IN ('export','backup','history_expiry')
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
    limits JSONB; scope RECORD; rejection TEXT;
BEGIN
    -- Expiry uses reserved internal execution, while retaining its own bounded
    -- outstanding, resident-byte and original-principal reservations.
    IF candidate.kind='history_expiry' THEN
        SELECT p.policy INTO STRICT policy FROM hubuum_control.task_policy p WHERE singleton;
        SELECT value INTO STRICT limits FROM jsonb_array_elements(policy->'kinds') WHERE value->>'kind'=candidate.kind;
        SELECT count(*) FILTER (WHERE outstanding) AS active,
            count(*) FILTER (WHERE lease_token IS NOT NULL) AS occupied,
            COALESCE(sum(resident_input_bytes::NUMERIC + resident_output_bytes),0) AS bytes,
            count(*) FILTER (WHERE outstanding AND principal_identity IS NOT DISTINCT FROM candidate.principal_identity) AS principal_active INTO scope
        FROM hubuum_control.task_accounting
        WHERE task_identity<>candidate.task_identity AND task_id IS NOT NULL AND kind=candidate.kind;
        rejection := public.hubuum_task_scope_rejection(limits->'capacity',scope.active,scope.occupied,scope.bytes,
            CASE WHEN candidate.outstanding THEN 1 ELSE 0 END,
            CASE WHEN candidate.lease_token IS NOT NULL THEN 1 ELSE 0 END,
            candidate.resident_input_bytes + candidate.resident_output_bytes,
            'task_capacity_kind',check_outstanding,check_execution,check_resident);
        IF rejection IS NOT NULL THEN RETURN rejection; END IF;
        IF check_outstanding AND scope.principal_active + (CASE WHEN candidate.outstanding THEN 1 ELSE 0 END) > (limits->>'principal_outstanding')::BIGINT THEN
            RETURN 'task_capacity_principal_kind';
        END IF;
    END IF;
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
    IF jsonb_array_length(candidate->'kinds') IS DISTINCT FROM 8 THEN
        RAISE EXCEPTION 'Task policy must contain every task kind';
    END IF;
    SELECT array_agg(DISTINCT value->>'kind' ORDER BY value->>'kind') INTO kinds FROM jsonb_array_elements(candidate->'kinds');
    IF kinds IS DISTINCT FROM ARRAY['backup','export','history_expiry','import','reindex','remote_call','schema_profiling','schema_validation'] THEN
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
    IF jsonb_typeof(s->'weights') IS DISTINCT FROM 'array' OR jsonb_array_length(s->'weights') <> 8 THEN
        RAISE EXCEPTION 'Every task kind requires a scheduling weight';
    END IF;
    SELECT array_agg(DISTINCT value->>'kind' ORDER BY value->>'kind') INTO kinds FROM jsonb_array_elements(s->'weights');
    IF kinds IS DISTINCT FROM ARRAY['backup','export','history_expiry','import','reindex','remote_call','schema_profiling','schema_validation'] THEN
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
        count(*) FILTER (WHERE kinds && ARRAY['reindex','schema_validation','history_expiry']) AS internal,
        count(*) FILTER (WHERE kinds && ARRAY['import','export','backup','remote_call','schema_profiling']) AS ordinary FROM live),
    mode AS (SELECT CASE
        WHEN (settings->>'execution_slots')::INTEGER=1 THEN 'single_slot'
        WHEN slots <= (settings->>'internal_reserved_slots')::INTEGER THEN 'insufficient_workers'
        WHEN internal < (settings->>'internal_reserved_slots')::INTEGER THEN 'insufficient_internal_workers'
        WHEN ordinary=0 THEN 'no_ordinary_workers' END AS reason FROM pool CROSS JOIN policy)
    SELECT jsonb_build_object(
        'mode',CASE WHEN reason IS NULL THEN jsonb_build_object('mode','reserved') ELSE jsonb_build_object('mode','shared','reason',reason) END,
        'eligible_slots',slots,'internal_capable_slots',internal,
        'unsupported_kinds',(SELECT coalesce(jsonb_agg(kind),'[]') FROM unnest(ARRAY['import','export','backup','reindex','remote_call','schema_validation','schema_profiling','history_expiry']) kind
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
    IF lease_ms <= 0 OR cardinality(supported) NOT BETWEEN 1 AND 8
        OR cardinality(supported) <> (SELECT count(DISTINCT k) FROM unnest(supported) k)
        OR NOT supported <@ ARRAY['import','export','backup','reindex','remote_call','schema_validation','schema_profiling','history_expiry'] THEN
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
        WHERE w.expires_at > clock_timestamp() AND w.generation=p.generation AND NOT a.internal AND w.kinds && ARRAY['reindex','schema_validation','history_expiry'];
    ordinary_allowed := snapshot #>> '{mode,mode}' <> 'reserved' OR (
        ordinary_used < (snapshot->>'eligible_slots')::BIGINT-reserved AND
        (NOT supported && ARRAY['reindex','schema_validation','history_expiry'] OR internal_capable_ordinary < (snapshot->>'internal_capable_slots')::BIGINT-reserved));
    SELECT array_agg(kind ORDER BY ordinal,weight_slot) INTO rotation
        FROM unnest(ARRAY['import','export','backup','reindex','remote_call','schema_validation','schema_profiling','history_expiry']) WITH ORDINALITY AS kinds(kind,ordinal)
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
        AND ((service.kind IN ('reindex','schema_validation','history_expiry') AND totals.internal < (p.policy #>> '{internal,executing}')::BIGINT)
            OR (service.kind NOT IN ('reindex','schema_validation','history_expiry') AND ordinary_allowed
                AND totals.ordinary < (p.policy #>> '{deployment,executing}')::BIGINT
                AND coalesce(principals.used,0) < (p.policy #>> '{principal,executing}')::BIGINT))
        ORDER BY lane_distance,lane_served
    ) SELECT candidate.*,g.kind,g.principal_identity AS principal,g.last_served,g.distance,promotion.promoted
    INTO selected FROM groups g CROSS JOIN LATERAL (
            SELECT t.id,t.created_at FROM public.tasks t
            WHERE t.status='queued' AND t.kind=g.kind
                AND public.hubuum_task_admission_allowed(t.id)
                -- A kind advertisement never authorizes reinterpreting a future
                -- payload or checkpoint. Rust validates its complete binding
                -- before this claim transaction can commit.
                AND (t.kind<>'history_expiry' OR (
                    ((t.request_payload->'version'='1'::JSONB
                        AND t.request_payload->>'action'='expire_configured_hot_history'
                        AND NOT t.request_payload ? 'recovery')
                     OR (t.request_payload->'version'='2'::JSONB
                        AND t.request_payload->>'action'='resume_retained_batch'
                        AND jsonb_typeof(t.request_payload->'recovery')='object'))
                    AND (t.task_checkpoint IS NULL OR (
                        t.task_checkpoint::JSONB->>'format'='hubuum.history_expiry'
                        AND t.task_checkpoint::JSONB->'version'='1'::JSONB))))
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
        candidate.internal := NEW.kind IN ('reindex','schema_validation','history_expiry');
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
        VALUES (identity,task.id,principal,collections,task.kind,task.kind IN ('reindex','schema_validation','history_expiry'),resolved,
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
            task.kind,task.kind IN ('reindex','schema_validation','history_expiry'),FALSE,current_generation,
            (COALESCE(octet_length(task.request_payload::TEXT),0)::BIGINT + COALESCE(octet_length(task.task_checkpoint),0)),public.hubuum_task_output_bytes(task.id),task.processed_items)
        ON CONFLICT(task_identity) DO UPDATE SET task_id=EXCLUDED.task_id,generation=EXCLUDED.generation,
            resident_input_bytes=EXCLUDED.resident_input_bytes,resident_output_bytes=EXCLUDED.resident_output_bytes,processed_items=EXCLUDED.processed_items;
    END LOOP;
END
$function$;

UPDATE hubuum_control.task_policy SET revision=revision+1,
    policy=jsonb_set(jsonb_set(policy,'{kinds}',policy->'kinds'||
        '[{"kind":"history_expiry","capacity":{"outstanding":64,"executing":1,"resident_bytes":67108864},"principal_outstanding":1}]'::JSONB),
        '{scheduling,weights}',(policy #> '{scheduling,weights}')||'[{"kind":"history_expiry","weight":1}]'::JSONB)
WHERE singleton;
SELECT public.hubuum_validate_task_policy(policy) FROM hubuum_control.task_policy WHERE singleton;

-- Bounded owner lookup and one active cleanup owner per retained batch. Terminal
-- rows remain independently purgeable; history authority serializes adoption.
CREATE INDEX tasks_history_expiry_recovery ON public.tasks
    ((discovery_metadata #>> '{data,recovery_batch}'), id DESC)
    WHERE kind='history_expiry'; -- hubuum-compat: bounded-transactional-index
CREATE UNIQUE INDEX tasks_history_expiry_recovery_active ON public.tasks
    ((discovery_metadata #>> '{data,recovery_batch}'))
    WHERE kind='history_expiry' AND status IN ('queued','validating','running','waiting'); -- hubuum-compat: bounded-transactional-index

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
        WHEN 'history_expiry' THEN ARRAY['kind','recovery_batch','admission_scope']
        WHEN 'import' THEN ARRAY['kind','dry_run','atomicity','collision_policy','permission_policy','has_failed_items']
        WHEN 'export' THEN ARRAY['kind','scope_kind','target','template_id','missing_data_policy','max_items','max_output_bytes','warning_count','truncated','output']
        WHEN 'backup' THEN ARRAY['kind','include_history','output']
        WHEN 'schema_profiling' THEN ARRAY['kind','class_id','output']
        WHEN 'schema_validation' THEN ARRAY['kind','class_id','schema_revision','work_kind']
        WHEN 'reindex' THEN ARRAY['kind','class_id','computation_revision']
        WHEN 'remote_call' THEN ARRAY['kind','remote_target_id','target']
        ELSE NULL END;
    IF allowed IS NULL OR (data - allowed) <> '{}'::jsonb THEN RETURN false; END IF;
    IF data->>'recovery_batch' IS NOT NULL AND (
        jsonb_typeof(data->'recovery_batch') <> 'string'
        OR data->>'recovery_batch' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR data->>'recovery_batch' = '00000000-0000-0000-0000-000000000000'
    ) THEN RETURN false; END IF;
    IF data->>'admission_scope' IS NOT NULL THEN
        IF jsonb_typeof(data->'admission_scope') <> 'object'
           OR ((data->'admission_scope') - ARRAY['family','scope']) <> '{}'::jsonb
           OR jsonb_typeof(data #> '{admission_scope,family}') IS DISTINCT FROM 'string'
           OR data #>> '{admission_scope,family}' NOT IN ('collection','class','object','class_relation','object_relation','export_template','remote_target','computed_definitions','class_schema')
           OR jsonb_typeof(data #> '{admission_scope,scope}') IS DISTINCT FROM 'object'
        THEN RETURN false; END IF;
        target := data #> '{admission_scope,scope}';
        IF target->>'kind' = 'deployment' THEN
            IF target - 'kind' <> '{}'::jsonb THEN RETURN false; END IF;
        ELSIF target->>'kind' IN ('collection','class') THEN
            IF target - ARRAY['kind','identity'] <> '{}'::jsonb
               OR jsonb_typeof(target->'identity') IS DISTINCT FROM 'string'
               OR target->>'identity' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               OR target->>'identity' = '00000000-0000-0000-0000-000000000000'
               OR (target->>'kind' = 'class' AND data #>> '{admission_scope,family}' = 'collection')
            THEN RETURN false; END IF;
        ELSE RETURN false; END IF;
        target := data->'target';
    END IF;
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

-- Coalescing covers fresh work and cleanup, with legacy payload fallback.
CREATE UNIQUE INDEX tasks_history_expiry_scope_active ON public.tasks
    ((coalesce(nullif(discovery_metadata #> '{data,admission_scope}','null'::jsonb), request_payload #> '{scan,key}')))
    WHERE kind='history_expiry' AND status IN ('queued','validating','running','waiting'); -- hubuum-compat: bounded-transactional-index
