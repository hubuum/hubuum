SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Control state survives logical restore and has no application foreign keys.
CREATE TABLE hubuum_control.api_request_policy (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    revision BIGINT NOT NULL CHECK (revision > 0),
    policy JSONB NOT NULL,
    admitted_attempts NUMERIC NOT NULL DEFAULT 0 CHECK (admitted_attempts >= 0),
    charged_milliseconds NUMERIC NOT NULL DEFAULT 0 CHECK (charged_milliseconds >= 0)
);
INSERT INTO hubuum_control.api_request_policy (revision, policy) VALUES (1,
    '{"deployment":16,"principal":2,"duration":15000,"input_bytes":16777216,"output_bytes":16777216}');

CREATE TABLE hubuum_control.api_request_owners (
    owner UUID PRIMARY KEY CHECK (owner <> '00000000-0000-0000-0000-000000000000'),
    principal_identity UUID NOT NULL,
    incarnation UUID NOT NULL,
    class TEXT NOT NULL CHECK (class IN ('aggregate','history','audit','import_submission','export_submission')),
    deadline TIMESTAMPTZ NOT NULL
);
CREATE INDEX api_request_owners_principal ON hubuum_control.api_request_owners (principal_identity); -- hubuum-compat: bounded-transactional-index

CREATE FUNCTION public.hubuum_api_request_policy_read() RETURNS JSONB
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT jsonb_build_object('revision',revision,'policy',policy)
    FROM hubuum_control.api_request_policy WHERE singleton
$$;

CREATE FUNCTION public.hubuum_api_request_policy_replace(expected BIGINT, candidate JSONB) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE key TEXT;
BEGIN
    IF jsonb_typeof(candidate) IS DISTINCT FROM 'object'
       OR (SELECT count(*) FROM jsonb_object_keys(candidate)) <> 5 THEN
        RAISE EXCEPTION 'Invalid API request policy shape';
    END IF;
    FOREACH key IN ARRAY ARRAY['deployment','principal','duration','input_bytes','output_bytes'] LOOP
        IF jsonb_typeof(candidate->key) IS DISTINCT FROM 'number'
           OR (candidate->>key) !~ '^[1-9][0-9]*$'
           OR (candidate->>key)::NUMERIC > 9223372036854775807 THEN
            RAISE EXCEPTION 'Invalid API request policy limit';
        END IF;
    END LOOP;
    IF (candidate->>'duration')::BIGINT > 60000 THEN
        RAISE EXCEPTION 'Invalid API request policy duration';
    END IF;
    UPDATE hubuum_control.api_request_policy SET policy=candidate, revision=revision+1
        WHERE singleton AND revision=expected;
    IF NOT FOUND THEN RETURN NULL; END IF;
    RETURN public.hubuum_api_request_policy_read();
END $$;

CREATE FUNCTION public.hubuum_api_request_admit(principal INTEGER, operation TEXT, input_bytes BIGINT) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE current_policy hubuum_control.api_request_policy; identity UUID; image UUID; request_owner UUID;
BEGIN
    -- Authority precedes policy and owner locks, as in ordinary execution.
    IF public.hubuum_deletion_execution_allowed() IS NOT TRUE THEN
        RAISE EXCEPTION 'api_capacity_reconciliation_required';
    END IF;
    SELECT * INTO current_policy FROM hubuum_control.api_request_policy WHERE singleton FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'api_capacity_reconciliation_required'; END IF;
    IF operation NOT IN ('aggregate','history','audit','import_submission','export_submission')
       OR input_bytes IS NULL OR input_bytes < 0 THEN
        RAISE EXCEPTION 'Invalid API request admission';
    END IF;
    SELECT incarnation INTO image FROM hubuum_control.application_control WHERE id=1;
    SELECT (logical_identity #>> '{subject,identity,identity}')::UUID INTO identity
        FROM public.principals WHERE id=principal;
    IF identity IS NULL OR image IS NULL THEN RAISE EXCEPTION 'api_capacity_identity_unavailable'; END IF;
    -- Expiry fences execution, but does not prove that HTTP-side CPU work or
    -- issued database work has stopped. Only explicit settlement frees capacity.
    IF input_bytes > (current_policy.policy->>'input_bytes')::BIGINT THEN
        RAISE EXCEPTION 'api_capacity_input_bytes';
    END IF;
    IF (SELECT count(*) FROM hubuum_control.api_request_owners WHERE principal_identity=identity) >= (current_policy.policy->>'principal')::BIGINT THEN
        RAISE EXCEPTION 'api_capacity_principal';
    END IF;
    IF (SELECT count(*) FROM hubuum_control.api_request_owners) >= (current_policy.policy->>'deployment')::BIGINT THEN
        RAISE EXCEPTION 'api_capacity_deployment';
    END IF;
    request_owner := gen_random_uuid();
    INSERT INTO hubuum_control.api_request_owners (owner,principal_identity,incarnation,class,deadline)
        VALUES (request_owner,identity,image,operation,clock_timestamp() + (current_policy.policy->>'duration')::BIGINT * interval '1 millisecond');
    -- Charge the full configured time allowance on every physical attempt.
    -- Settlement never refunds these monotonic charges, nor does logical restore.
    UPDATE hubuum_control.api_request_policy SET admitted_attempts=admitted_attempts+1,
        charged_milliseconds=charged_milliseconds+(current_policy.policy->>'duration')::BIGINT WHERE singleton;
    RETURN jsonb_build_object('owner',request_owner,'revision',current_policy.revision,'policy',current_policy.policy);
END $$;

CREATE FUNCTION public.hubuum_api_request_execution(request_owner UUID) RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE admitted hubuum_control.api_request_owners; remaining BIGINT;
BEGIN
    SELECT * INTO admitted FROM hubuum_control.api_request_owners WHERE owner=request_owner FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'api_capacity_reconciliation_required'; END IF;
    IF NOT EXISTS (SELECT 1 FROM hubuum_control.application_control WHERE id=1 AND incarnation=admitted.incarnation) THEN
        RAISE EXCEPTION 'api_capacity_reconciliation_required';
    END IF;
    remaining := floor(extract(epoch FROM (admitted.deadline-clock_timestamp())) * 1000)::BIGINT;
    IF remaining <= 0 THEN RAISE EXCEPTION 'api_capacity_deadline_exceeded'; END IF;
    RETURN remaining;
END $$;

CREATE FUNCTION public.hubuum_api_request_settle(request_owner UUID) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    -- Deleting the tuple fences old repeatable-read snapshots too. This waits
    -- for productive FOR SHARE locks rather than interpreting disconnect as stop.
    DELETE FROM hubuum_control.api_request_owners WHERE owner=request_owner;
END $$;

REVOKE ALL ON TABLE hubuum_control.api_request_policy, hubuum_control.api_request_owners FROM PUBLIC;
REVOKE ALL ON FUNCTION public.hubuum_api_request_policy_read(), public.hubuum_api_request_policy_replace(BIGINT,JSONB),
    public.hubuum_api_request_admit(INTEGER,TEXT,BIGINT), public.hubuum_api_request_execution(UUID),
    public.hubuum_api_request_settle(UUID) FROM PUBLIC;
