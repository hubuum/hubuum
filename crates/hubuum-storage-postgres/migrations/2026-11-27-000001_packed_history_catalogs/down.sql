SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

LOCK TABLE hubuum_control.history_archive_catalogs IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM hubuum_control.history_archive_catalogs WHERE packed IS NOT NULL) THEN
        RAISE EXCEPTION 'Packed history archive catalogs remain. Roll forward or restore the pre-upgrade backup.';
    END IF;
END $$;

CREATE OR REPLACE FUNCTION public.hubuum_history_archive_catalog_read(subject uuid, locked boolean,
    registration uuid, ownership uuid, image uuid, revision bigint) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
    IF NOT public.hubuum_archive_history_entry(registration,ownership,image,revision) THEN RAISE EXCEPTION 'Archive Legacy owner is unavailable'; END IF;
    IF locked THEN PERFORM pg_advisory_xact_lock(hashtextextended('hubuum-history-archive:'||subject::text,0)); END IF;
    SELECT material INTO result FROM hubuum_control.history_archive_catalogs WHERE resource=subject;
    RETURN result;
END $$;

DROP FUNCTION public.hubuum_history_archive_catalog_store(uuid,bigint,jsonb,bytea,integer,uuid,uuid,uuid,bigint);
DROP FUNCTION public.hubuum_history_archive_catalog_fetch(uuid,boolean,uuid,uuid,uuid,bigint);
ALTER TABLE hubuum_control.history_archive_catalogs ALTER COLUMN material SET NOT NULL;
ALTER TABLE hubuum_control.history_archive_catalogs DROP CONSTRAINT history_catalog_representation;
ALTER TABLE hubuum_control.history_archive_catalogs DROP COLUMN packed, DROP COLUMN version_count;
