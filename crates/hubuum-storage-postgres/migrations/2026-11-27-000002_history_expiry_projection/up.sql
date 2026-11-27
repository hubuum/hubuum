SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE OR REPLACE FUNCTION public.hubuum_history_expiry_sync() RETURNS VOID
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE inconsistent BOOLEAN;
BEGIN
    -- The authority table has exactly one possible key. Decode its bounded
    -- journal once into scalar rows before checking or inserting exclusions.
    WITH projected(family, resource_uuid, version_uuid, intent_uuid, valid_from, valid_to) AS MATERIALIZED (
        SELECT member->>'family',
            (member #>> '{target,resource}')::UUID,
            (member #>> '{target,version}')::UUID,
            (intent #>> '{manifest,identity}')::UUID,
            (member #>> '{read_span,valid_from}')::TIMESTAMPTZ,
            (member #>> '{read_span,valid_to}')::TIMESTAMPTZ
        FROM hubuum_control.history_lifecycle_control control
        CROSS JOIN LATERAL jsonb_array_elements(coalesce(control.material #> '{authority,expiry}', '[]'::JSONB)) intent
        CROSS JOIN LATERAL jsonb_array_elements(intent #> '{manifest,members}') member
        WHERE control.id=1
    ), missing AS MATERIALIZED (
        SELECT family, resource_uuid, version_uuid, intent_uuid, valid_from, valid_to
        FROM hubuum_control.history_expiry_members
        EXCEPT
        SELECT family, resource_uuid, version_uuid, intent_uuid, valid_from, valid_to FROM projected
    ), added AS (
        INSERT INTO hubuum_control.history_expiry_members(family,resource_uuid,version_uuid,intent_uuid,valid_from,valid_to)
        SELECT family, resource_uuid, version_uuid, intent_uuid, valid_from, valid_to
        FROM projected WHERE NOT EXISTS (SELECT 1 FROM missing)
        ON CONFLICT (family,resource_uuid,version_uuid) DO NOTHING
    )
    SELECT EXISTS (SELECT 1 FROM missing) INTO inconsistent;
    IF inconsistent THEN
        RAISE EXCEPTION 'Retained history exclusion conflicts with control authority';
    END IF;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_expiry_sync() FROM PUBLIC;
