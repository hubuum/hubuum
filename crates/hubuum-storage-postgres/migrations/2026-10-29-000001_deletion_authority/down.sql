DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM hubuum_control.deletion_authority
               WHERE material->'intents' <> '{}'::jsonb) THEN
        RAISE EXCEPTION 'Retained deletion authority prevents downgrade';
    END IF;
END $$;
DROP FUNCTION public.hubuum_check_deletion_restore_gate();
DROP TABLE hubuum_control.deletion_authority;
