SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Runtime settings do not change the unversioned Legacy registry layout.
-- This checkpoint qualifies that exact synchronized schema; unknown future
-- checkpoints still fail closed. Never replace an installed versioned protocol
-- or change the fixed Legacy-only hook, regardless of migration application order.
DO $compatibility$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
        WHERE attrelid='hubuum_control.runtime_registrations'::regclass
          AND attname='history_protocol' AND NOT attisdropped) THEN
        EXECUTE $definition$
CREATE OR REPLACE FUNCTION public.hubuum_archive_history_entry(registration uuid, ownership uuid, image uuid, revision bigint)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $entry$
DECLARE legacy boolean;
BEGIN
    IF registration IS NULL OR ownership IS NULL OR NOT public.hubuum_archive_history_requires_legacy_runtime()
        OR NOT public.hubuum_application_execution_allowed(registration,ownership,image,revision) THEN RETURN FALSE; END IF;
    -- No absent-column/error catch: only the explicitly supported schema can
    -- interpret an unversioned registration as Legacy.
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='hubuum_control.runtime_registrations'::regclass
               AND attname='history_protocol' AND NOT attisdropped) THEN
        EXECUTE 'SELECT history_protocol=0 FROM hubuum_control.runtime_registrations WHERE id=$1 AND ownership_token=$2 FOR SHARE'
            INTO STRICT legacy USING registration,ownership;
    ELSE
        IF (SELECT max(version) FROM public.__diesel_schema_migrations) <> '20261111000001' THEN
            RAISE EXCEPTION 'Unversioned archive runtime schema is not supported';
        END IF;
        SELECT TRUE INTO STRICT legacy FROM hubuum_control.runtime_registrations
            WHERE id=registration AND ownership_token=ownership FOR SHARE;
    END IF;
    RETURN legacy IS TRUE;
END $entry$;
        $definition$;
    END IF;
END $compatibility$;
