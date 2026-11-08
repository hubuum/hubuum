SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- No references into replaceable application tables. Original policy/hold
-- identities and clocks survive logical replacement.
CREATE TABLE hubuum_control.history_lifecycle_control (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    material JSONB NOT NULL CHECK (octet_length(material::text) <= 4194304)
);
INSERT INTO hubuum_control.history_lifecycle_control VALUES
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
    valid_to TIMESTAMPTZ CHECK (valid_to IS NULL OR valid_to > valid_from),
    PRIMARY KEY (family,resource_uuid,version_uuid)
);
CREATE INDEX history_expiry_members_as_of ON hubuum_control.history_expiry_members(family,resource_uuid,valid_from);
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

CREATE INDEX history_publication_runtime ON hubuum_control.history_publication_owners(runtime_id);
CREATE INDEX history_publication_generation ON hubuum_control.history_publication_owners(generation);
REVOKE ALL ON hubuum_control.history_publication_control,hubuum_control.history_publication_owners FROM PUBLIC;

-- Productive entry takes this gate AFTER application/deletion authority and
-- BEFORE API-owner validation. Owner validation follows the API-owner gate.
CREATE FUNCTION public.hubuum_history_read_authority(runtime UUID, ownership UUID, write_mode BOOLEAN)
RETURNS SMALLINT LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE material JSONB; protocol SMALLINT;
BEGIN
    IF write_mode IS TRUE THEN
        SELECT c.material INTO material FROM hubuum_control.history_lifecycle_control c WHERE id=1 FOR UPDATE;
    ELSE
        SELECT c.material INTO material FROM hubuum_control.history_lifecycle_control c WHERE id=1 FOR SHARE;
    END IF;
    IF NOT FOUND THEN RAISE EXCEPTION 'History authority is unavailable'; END IF;
    IF runtime IS NULL AND ownership IS NULL THEN
        IF coalesce(material #> '{authority,expiry}','[]'::JSONB)<>'[]'::JSONB
            OR EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members) THEN
            RAISE EXCEPTION 'Retained history requires an owned publication scope';
        END IF;
        RETURN 0;
    END IF;
    SELECT r.history_protocol INTO protocol FROM hubuum_control.runtime_registrations r
        WHERE r.id=runtime AND r.ownership_token=ownership;
    IF NOT FOUND OR protocol NOT IN (0,1) THEN RAISE EXCEPTION 'History runtime ownership is unavailable'; END IF;
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
    IF (SELECT (material #>> '{authority,revision}')::BIGINT FROM hubuum_control.history_lifecycle_control WHERE id=1) IS DISTINCT FROM history_revision THEN RETURN FALSE; END IF;
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
    INSERT INTO hubuum_control.history_publication_owners(id,ownership_token,runtime_id,isolation,incarnation,deletion_revision,history_revision,generation,kind,api_request_owner)
        SELECT identity,ownership,runtime,r.isolation,incarnation,deletion_revision,history_revision,expected_generation,producer_kind,original_api_owner
        FROM hubuum_control.runtime_registrations r WHERE r.id=runtime AND r.ownership_token=runtime_ownership;
    RETURN FOUND;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_publication_admit(UUID,UUID,TEXT,UUID,UUID,UUID,BIGINT,BIGINT,BIGINT,UUID) FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_publication_execution(identity UUID, ownership UUID,
    expected_incarnation UUID, expected_deletion BIGINT, expected_history BIGINT, expected_generation BIGINT)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
BEGIN
    -- Authority is already held by the specialized entry. Do not acquire it
    -- after API ownership: doing so would invert the productive lock order.
    IF (SELECT generation FROM hubuum_control.history_publication_control WHERE id=1) IS DISTINCT FROM expected_generation
        OR (SELECT (material #>> '{authority,revision}')::BIGINT FROM hubuum_control.history_lifecycle_control WHERE id=1) IS DISTINCT FROM expected_history THEN RETURN FALSE; END IF;
    PERFORM 1 FROM hubuum_control.history_publication_owners p
        WHERE p.id=identity AND p.ownership_token=ownership AND p.incarnation=expected_incarnation
        AND p.deletion_revision=expected_deletion AND p.history_revision=expected_history
        AND p.generation=expected_generation FOR SHARE;
    RETURN FOUND;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_publication_execution(UUID,UUID,UUID,BIGINT,BIGINT,BIGINT) FROM PUBLIC;

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
        OR (SELECT (material #>> '{authority,revision}')::BIGINT FROM hubuum_control.history_lifecycle_control WHERE id=1) IS DISTINCT FROM expected_history THEN RETURN FALSE; END IF;
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
        AND p.history_revision=(SELECT (material #>> '{authority,revision}')::BIGINT FROM hubuum_control.history_lifecycle_control WHERE id=1)
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
    RETURN NOT EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members retained
        WHERE retained.family=history_family AND retained.resource_uuid=resource
          AND (retained.version_uuid=version OR unknown_source
            OR captured_references -> ('/restore_source_versions/' || retained.version_uuid::TEXT)
                = jsonb_build_object('kind','version','reference',
                    jsonb_build_object('state','known','identity',retained.version_uuid))));
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_version_visible(TEXT,JSONB) FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_as_of_visible(history_family TEXT, evidence JSONB, at_time TIMESTAMPTZ, selected_from TIMESTAMPTZ)
RETURNS BOOLEAN LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $function$
    SELECT public.hubuum_history_version_visible(history_family,evidence)
       AND NOT EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members
        WHERE family=history_family
          AND resource_uuid=(evidence #>> '{subject,identity,identity}')::UUID
          AND valid_from<=at_time AND (valid_to IS NULL OR at_time<valid_to)
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
          AND valid_from<=at_time AND (valid_to IS NULL OR at_time<valid_to))
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
    previous_count INTEGER; next_count INTEGER; additions INTEGER; pending_completions INTEGER;
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
       OR (replacement->'authority') - ARRAY['revision','policies','holds','expiry'] <> '{}'::jsonb
       OR jsonb_typeof(replacement #> '{authority,policies}') IS DISTINCT FROM 'array'
       OR jsonb_typeof(replacement #> '{authority,holds}') IS DISTINCT FROM 'array'
       OR jsonb_typeof(replacement->'previews') IS DISTINCT FROM 'array'
       OR octet_length(replacement::text)>4194304 THEN RETURN FALSE; END IF;
    old_authority := current_material->'authority';
    new_authority := replacement->'authority';
    IF jsonb_array_length(new_authority->'policies')>4096 OR jsonb_array_length(new_authority->'holds')>4096
       OR jsonb_array_length(replacement->'previews')>256 THEN RETURN FALSE; END IF;
    SELECT count(*) INTO pending_completions FROM (
        SELECT DISTINCT ON (value #>> '{hold,identity}') value
        FROM jsonb_array_elements(new_authority->'holds') WITH ORDINALITY AS entries(value,ordinal)
        ORDER BY value #>> '{hold,identity}',ordinal DESC
    ) latest WHERE value->>'transition'='created';
    IF jsonb_array_length(new_authority->'holds')+pending_completions>4096
       OR octet_length(replacement::text)+pending_completions*1024>4194304 THEN RETURN FALSE; END IF;
    -- Prefix comparisons preserve order and exact immutable records. JSONB
    -- containment alone would incorrectly accept reordered or rewritten history.
    IF (SELECT coalesce(jsonb_agg(value ORDER BY ordinal),'[]') FROM jsonb_array_elements(new_authority->'policies') WITH ORDINALITY AS entries(value,ordinal)
          WHERE ordinal<=jsonb_array_length(old_authority->'policies')) <> old_authority->'policies'
       OR (SELECT coalesce(jsonb_agg(value ORDER BY ordinal),'[]') FROM jsonb_array_elements(new_authority->'holds') WITH ORDINALITY AS entries(value,ordinal)
          WHERE ordinal<=jsonb_array_length(old_authority->'holds')) <> old_authority->'holds' THEN RETURN FALSE; END IF;
    previous_count := jsonb_array_length(old_authority->'policies')+jsonb_array_length(old_authority->'holds');
    next_count := jsonb_array_length(new_authority->'policies')+jsonb_array_length(new_authority->'holds');
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
    SELECT jsonb_agg(value) INTO additions FROM jsonb_array_elements(new_expiry) value
      WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements(old_expiry) old WHERE old #>> '{manifest,identity}'=value #>> '{manifest,identity}');
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
