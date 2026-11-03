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

-- Producer declarations are durable registrations, never heartbeats. Serialize
-- registration with first intent admission through application/history tuples.
CREATE FUNCTION public.hubuum_history_runtime_register(registration UUID, ownership UUID, isolation_domain JSONB, protocol SMALLINT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
BEGIN
    IF protocol IS NULL OR protocol NOT IN (0,1) THEN RAISE EXCEPTION 'Unsupported history producer protocol'; END IF;
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
-- permission. Complete native admission remains disabled in the CAS below.
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
    IF replacement #> '{authority,expiry}' IS DISTINCT FROM current_material #> '{authority,expiry}'
       AND public.hubuum_history_producers_fenced(expected_incarnation,expected_deletion) IS NOT TRUE THEN RETURN FALSE; END IF;
    IF jsonb_typeof(replacement) IS DISTINCT FROM 'object'
       OR replacement - ARRAY['authority','previews'] <> '{}'::jsonb
       OR jsonb_typeof(replacement->'authority') IS DISTINCT FROM 'object'
       OR (replacement->'authority') - ARRAY['revision','policies','holds'] <> '{}'::jsonb
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
       OR (new_authority->>'revision')::BIGINT<>next_count THEN RETURN FALSE; END IF;
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

