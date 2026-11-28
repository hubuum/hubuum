-- Productive task transactions take this lock before task or domain rows.
-- Committing a deletion intent takes the conflicting authority-row write lock:
-- either the transaction finishes first, or it observes recovery quarantine.
CREATE FUNCTION public.hubuum_deletion_execution_allowed() RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE authority jsonb;
BEGIN
    SELECT material INTO STRICT authority
    FROM hubuum_control.deletion_authority WHERE id = 1 FOR SHARE;
    RETURN (authority->'intents') = '{}'::jsonb;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_deletion_execution_allowed() FROM PUBLIC;
