SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE OR REPLACE FUNCTION hubuum_destroy_changed_upstream_credentials() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.setting_group = 'authentication' AND NEW.revision IS DISTINCT FROM OLD.revision THEN
        -- Activation advances only credentials whose complete provider definition is unchanged.
        DELETE FROM auth_upstream_credentials
        WHERE (document->>'current_revision')::bigint IS DISTINCT FROM NEW.revision;
    END IF;
    RETURN NEW;
END;
$$;
