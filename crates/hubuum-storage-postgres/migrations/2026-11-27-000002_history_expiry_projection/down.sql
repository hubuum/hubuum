SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE OR REPLACE FUNCTION public.hubuum_history_expiry_sync() RETURNS VOID
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
