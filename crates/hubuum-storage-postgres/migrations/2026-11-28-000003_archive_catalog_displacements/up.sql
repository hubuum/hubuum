SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

-- One physical reference may also have been preserved when a query expired.
ALTER TABLE hubuum_control.history_archive_reclaimed_pins
    ADD COLUMN catalog_displaced boolean NOT NULL DEFAULT false;

-- This table must be empty at commit. A publisher that only knows how to
-- replace the catalog cannot discard the prior generation's physical evidence.
CREATE TABLE hubuum_control.history_archive_pending_displacements (
    resource uuid PRIMARY KEY,
    generation bigint NOT NULL CHECK (generation > 1),
    expected_digest bytea CHECK (octet_length(expected_digest)=32)
);
REVOKE ALL ON hubuum_control.history_archive_pending_displacements FROM PUBLIC;

CREATE FUNCTION hubuum_control.archive_displacements_digest(locations jsonb) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog AS $digest$
    SELECT sha256(convert_to(COALESCE(jsonb_agg(value ORDER BY value),'[]'::jsonb)::text,'UTF8'))
    FROM jsonb_array_elements(locations)
$digest$;
REVOKE ALL ON FUNCTION hubuum_control.archive_displacements_digest(jsonb) FROM PUBLIC;

CREATE FUNCTION hubuum_control.require_archive_catalog_accounting() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $guard$
DECLARE
    expected jsonb;
BEGIN
    IF NEW.resource <> OLD.resource OR NEW.generation <> OLD.generation + 1 THEN
        RAISE EXCEPTION 'Invalid archive catalog generation transition';
    END IF;
    -- The database can independently verify both endpoints for logical JSON.
    -- Packed metadata is decoded and validated by its owning storage adapter.
    IF OLD.material IS NOT NULL AND NEW.material IS NOT NULL THEN
        SELECT COALESCE(jsonb_agg(location),'[]'::jsonb) INTO expected FROM (
            (SELECT jsonb_build_array(v #> '{identity,version}',v->'location') AS location
               FROM jsonb_array_elements(OLD.material->'versions') v)
            EXCEPT
            (SELECT jsonb_build_array(v #> '{identity,version}',v->'location')
               FROM jsonb_array_elements(NEW.material->'versions') v)
            LIMIT 1001
        ) displaced;
        IF jsonb_array_length(expected)>1000 THEN RAISE EXCEPTION 'Too many archive catalog displacements'; END IF;
    END IF;
    INSERT INTO hubuum_control.history_archive_pending_displacements(resource,generation,expected_digest)
        VALUES (NEW.resource,NEW.generation,hubuum_control.archive_displacements_digest(expected));
    RETURN NULL;
END $guard$;
REVOKE ALL ON FUNCTION hubuum_control.require_archive_catalog_accounting() FROM PUBLIC;
CREATE TRIGGER require_archive_catalog_accounting AFTER UPDATE ON hubuum_control.history_archive_catalogs
    FOR EACH ROW EXECUTE FUNCTION hubuum_control.require_archive_catalog_accounting();

CREATE FUNCTION hubuum_control.check_archive_catalog_accounting() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $guard$
BEGIN
    IF EXISTS(SELECT 1 FROM hubuum_control.history_archive_pending_displacements
              WHERE resource=NEW.resource AND generation=NEW.generation) THEN
        RAISE EXCEPTION 'Archive catalog replacement requires matching displacement evidence';
    END IF;
    RETURN NULL;
END $guard$;
REVOKE ALL ON FUNCTION hubuum_control.check_archive_catalog_accounting() FROM PUBLIC;
CREATE CONSTRAINT TRIGGER check_archive_catalog_accounting
    AFTER INSERT ON hubuum_control.history_archive_pending_displacements
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION hubuum_control.check_archive_catalog_accounting();

-- The complete storage adapter owns decoding the installed representation and
-- supplies only former immutable references from its validated catalog update.
CREATE FUNCTION public.hubuum_history_archive_record_displacements(
    subject uuid, committed_generation bigint, locations jsonb,
    registration uuid, ownership uuid, image uuid, revision bigint) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $record$
DECLARE
    item jsonb;
    location jsonb;
    digest jsonb;
    digest_bytes bytea;
BEGIN
    IF public.hubuum_archive_history_entry(registration,ownership,image,revision) IS NOT TRUE THEN RETURN false; END IF;
    IF jsonb_typeof(locations) IS DISTINCT FROM 'array' OR jsonb_array_length(locations)>1000
        OR octet_length(locations::text)>1048576 THEN RAISE EXCEPTION 'Invalid archive catalog displacement batch'; END IF;

    DELETE FROM hubuum_control.history_archive_pending_displacements
        WHERE resource=subject AND generation=committed_generation
          AND (expected_digest IS NULL OR expected_digest=hubuum_control.archive_displacements_digest(locations));
    IF NOT FOUND THEN RETURN false; END IF;
    FOR item IN SELECT value FROM jsonb_array_elements(locations) LOOP
        IF jsonb_typeof(item) IS DISTINCT FROM 'array' OR jsonb_array_length(item)<>2 THEN
            RAISE EXCEPTION 'Invalid archive catalog displacement';
        END IF;
        location := item->1;
        digest := location->'manifest_digest';
        IF jsonb_typeof(digest) IS DISTINCT FROM 'array' OR jsonb_array_length(digest)<>32 THEN
            RAISE EXCEPTION 'Invalid archive catalog manifest digest';
        END IF;
        SELECT decode(string_agg(lpad(to_hex(b.value::text::integer),2,'0'),'' ORDER BY b.i),'hex')
          INTO digest_bytes FROM jsonb_array_elements(digest) WITH ORDINALITY b(value,i)
          WHERE jsonb_typeof(b.value)='number' AND b.value::text ~ '^(0|[1-9][0-9]{0,2})$'
            AND b.value::text::integer BETWEEN 0 AND 255;
        INSERT INTO hubuum_control.history_archive_reclaimed_pins AS history
            (resource,version,destination,artifact,attempt,object_version,manifest_digest,catalog_displaced)
        VALUES (subject,(item->>0)::uuid,(location #>> '{identity,destination}')::uuid,
            (location #>> '{identity,artifact}')::uuid,(location #>> '{identity,attempt}')::uuid,
            (location #>> '{identity,version}')::uuid,digest_bytes,true)
        ON CONFLICT (resource,version,destination,artifact,attempt,object_version,manifest_digest)
        DO UPDATE SET catalog_displaced=true WHERE NOT history.catalog_displaced;
    END LOOP;
    RETURN true;
END $record$;
REVOKE ALL ON FUNCTION public.hubuum_history_archive_record_displacements(uuid,bigint,jsonb,uuid,uuid,uuid,bigint) FROM PUBLIC;
