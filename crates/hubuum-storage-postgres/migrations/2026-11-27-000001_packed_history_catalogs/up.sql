SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

ALTER TABLE hubuum_control.history_archive_catalogs ADD COLUMN packed bytea;
ALTER TABLE hubuum_control.history_archive_catalogs ADD COLUMN version_count integer;
ALTER TABLE hubuum_control.history_archive_catalogs ALTER COLUMN material DROP NOT NULL;

-- HUBC v1: marker, resource UUID, generation, count and frame size in network
-- order, one Zstandard frame, then count complete 16-byte UUID / 32-byte digests.
ALTER TABLE hubuum_control.history_archive_catalogs ADD CONSTRAINT history_catalog_representation CHECK (
    (material IS NOT NULL AND packed IS NULL AND version_count IS NULL) OR
    (material IS NULL AND packed IS NOT NULL AND version_count BETWEEN 1 AND 10000
     AND octet_length(packed) BETWEEN 38 AND 4194304
     AND octet_length(packed) > 37 + 48 * version_count
     AND substr(packed, 1, 5) = decode('4855424301', 'hex')
     AND substr(packed, 6, 16) = uuid_send(resource)
     AND substr(packed, 22, 8) = int8send(generation)
     AND substr(packed, 30, 4) = int4send(version_count)
     AND substr(packed, 34, 4) = int4send(octet_length(packed) - 37 - 48 * version_count)) IS TRUE
) NOT VALID;
ALTER TABLE hubuum_control.history_archive_catalogs VALIDATE CONSTRAINT history_catalog_representation;

CREATE FUNCTION public.hubuum_history_archive_catalog_fetch(subject uuid, locked boolean,
    registration uuid, ownership uuid, image uuid, revision bigint)
RETURNS TABLE (material jsonb, packed bytea)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
    IF public.hubuum_archive_history_entry(registration,ownership,image,revision) IS NOT TRUE THEN
        RAISE EXCEPTION 'Archive Legacy owner is unavailable';
    END IF;
    IF locked THEN
        PERFORM pg_advisory_xact_lock(hashtextextended('hubuum-history-archive:' || subject::text, 0));
    END IF;
    RETURN QUERY SELECT c.material, c.packed FROM hubuum_control.history_archive_catalogs c WHERE c.resource=subject;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_history_archive_catalog_fetch(uuid,boolean,uuid,uuid,uuid,bigint) FROM PUBLIC;

-- An older reader treats a missing row as an empty catalog. Never let it
-- observe SQL NULL for an existing catalog which uses the newer encoding.
CREATE OR REPLACE FUNCTION public.hubuum_history_archive_catalog_read(subject uuid, locked boolean,
    registration uuid, ownership uuid, image uuid, revision bigint) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE catalog record;
BEGIN
    SELECT * INTO catalog FROM public.hubuum_history_archive_catalog_fetch(subject,locked,registration,ownership,image,revision);
    IF catalog.packed IS NOT NULL THEN
        RAISE EXCEPTION 'Packed history archive catalog requires an upgraded reader';
    END IF;
    RETURN catalog.material;
END $$;

CREATE FUNCTION public.hubuum_history_archive_catalog_store(subject uuid, expected bigint,
    raw_material jsonb, encoded bytea, members integer, registration uuid, ownership uuid, image uuid, revision bigint)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE current_generation bigint;
BEGIN
    IF public.hubuum_archive_history_entry(registration,ownership,image,revision) IS NOT TRUE THEN RETURN FALSE; END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('hubuum-history-archive:' || subject::text, 0));
    SELECT c.generation INTO current_generation FROM hubuum_control.history_archive_catalogs c WHERE c.resource=subject;
    IF expected IS DISTINCT FROM COALESCE(current_generation,0) THEN RETURN FALSE; END IF;
    IF raw_material IS NOT NULL AND (
        (raw_material->>'generation')::bigint IS DISTINCT FROM expected+1 OR
        (raw_material->>'resource')::uuid IS DISTINCT FROM subject OR
        jsonb_typeof(raw_material->'versions') IS DISTINCT FROM 'array'
    ) THEN RETURN FALSE; END IF;
    INSERT INTO hubuum_control.history_archive_catalogs(resource,generation,material,packed,version_count)
        VALUES(subject, expected+1, raw_material, encoded, members)
        ON CONFLICT(resource) DO UPDATE SET generation=EXCLUDED.generation,material=EXCLUDED.material,
                                            packed=EXCLUDED.packed,version_count=EXCLUDED.version_count;
    RETURN TRUE;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_history_archive_catalog_store(uuid,bigint,jsonb,bytea,integer,uuid,uuid,uuid,bigint) FROM PUBLIC;
