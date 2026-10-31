DO $$ BEGIN
    IF NOT public.hubuum_deletion_exposure_allowed() THEN
        RAISE EXCEPTION 'Cannot remove recovery enforcement while deletion decisions remain';
    END IF;
END $$;
DROP FUNCTION public.hubuum_deletion_exposure_allowed();
DROP FUNCTION public.hubuum_deletion_control_read();
