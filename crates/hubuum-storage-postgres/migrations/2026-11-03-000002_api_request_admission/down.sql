DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM hubuum_control.api_request_owners) THEN
        RAISE EXCEPTION 'Request owners must settle before rolling back API admission';
    END IF;
END $$;

DROP FUNCTION public.hubuum_api_request_settle(UUID);
DROP FUNCTION public.hubuum_api_request_execution(UUID);
DROP FUNCTION public.hubuum_api_request_admit(INTEGER,TEXT,BIGINT);
DROP FUNCTION public.hubuum_api_request_policy_replace(BIGINT,JSONB);
DROP FUNCTION public.hubuum_api_request_policy_read();
DROP TABLE hubuum_control.api_request_owners;
DROP TABLE hubuum_control.api_request_policy;
