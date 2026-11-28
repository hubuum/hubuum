DO $$ BEGIN
    IF NOT public.hubuum_deletion_exposure_allowed() THEN
        RAISE EXCEPTION 'Cannot remove execution fencing while deletion decisions remain';
    END IF;
END $$;
DROP FUNCTION public.hubuum_deletion_execution_allowed();
