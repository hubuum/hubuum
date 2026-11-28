-- Runtime receives read-only capabilities, never direct control-table grants.
CREATE FUNCTION public.hubuum_deletion_control_read() RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE authority jsonb;
BEGIN
    SELECT material INTO STRICT authority FROM hubuum_control.deletion_authority WHERE id = 1;
    RETURN authority;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_deletion_control_read() FROM PUBLIC;

-- This deliberately restricts the entire deployment until individual lifecycle
-- consumers can reapply retained decisions. Missing authority raises an error.
CREATE FUNCTION public.hubuum_deletion_exposure_allowed() RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT (public.hubuum_deletion_control_read()->'intents') = '{}'::jsonb
$$;
REVOKE ALL ON FUNCTION public.hubuum_deletion_exposure_allowed() FROM PUBLIC;
