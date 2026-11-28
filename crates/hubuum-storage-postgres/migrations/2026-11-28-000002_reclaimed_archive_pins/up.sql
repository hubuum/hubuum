SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

-- Keep only the exact logical subject and immutable physical reference.
-- Retries by any number of query owners share a single record.
CREATE TABLE hubuum_control.history_archive_reclaimed_pins (
    resource uuid NOT NULL, version uuid NOT NULL, destination uuid NOT NULL,
    artifact uuid NOT NULL, attempt uuid NOT NULL, object_version uuid NOT NULL,
    manifest_digest bytea NOT NULL CHECK (octet_length(manifest_digest)=32),
    PRIMARY KEY(resource, version, destination, artifact, attempt, object_version, manifest_digest)
);
REVOKE ALL ON hubuum_control.history_archive_reclaimed_pins FROM PUBLIC;

CREATE FUNCTION hubuum_control.retain_reclaimed_history_archive_pin() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $pin$
DECLARE
    descriptor jsonb := OLD.material->'archive';
    location jsonb := descriptor->'location';
    digest jsonb := location->'manifest_digest';
    digest_bytes bytea;
BEGIN
    IF descriptor IS NULL OR descriptor='null'::jsonb THEN RETURN OLD; END IF;
    IF jsonb_typeof(digest) IS DISTINCT FROM 'array' OR jsonb_array_length(digest)<>32 THEN
        RAISE EXCEPTION 'Invalid retained archive pin manifest digest';
    END IF;
    SELECT decode(string_agg(lpad(to_hex((b.value::text)::integer), 2, '0'), '' ORDER BY b.ordinal), 'hex')
      INTO digest_bytes FROM jsonb_array_elements(digest) WITH ORDINALITY b(value,ordinal)
      WHERE jsonb_typeof(b.value)='number' AND b.value::text ~ '^(0|[1-9][0-9]{0,2})$'
        AND (b.value::text)::integer BETWEEN 0 AND 255;
    INSERT INTO hubuum_control.history_archive_reclaimed_pins
        (resource,version,destination,artifact,attempt,object_version,manifest_digest)
    VALUES ((descriptor #>> '{identity,resource}')::uuid,(descriptor #>> '{identity,version}')::uuid,
            (location #>> '{identity,destination}')::uuid,(location #>> '{identity,artifact}')::uuid,
            (location #>> '{identity,attempt}')::uuid,(location #>> '{identity,version}')::uuid,digest_bytes)
    ON CONFLICT DO NOTHING;
    RETURN OLD;
END $pin$;
REVOKE ALL ON FUNCTION hubuum_control.retain_reclaimed_history_archive_pin() FROM PUBLIC;
CREATE TRIGGER retain_reclaimed_archive_pin AFTER DELETE ON hubuum_control.history_archive_captures
FOR EACH ROW EXECUTE FUNCTION hubuum_control.retain_reclaimed_history_archive_pin();
