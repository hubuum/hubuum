SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

-- Fixed compatibility boundary, not an administrator setting. Online expiry
-- must refuse protocol-1 registration while this hook requires Legacy producers.
CREATE FUNCTION public.hubuum_archive_history_requires_legacy_runtime() RETURNS boolean
LANGUAGE sql IMMUTABLE SECURITY DEFINER SET search_path=pg_catalog AS $$ SELECT TRUE $$;
REVOKE ALL ON FUNCTION public.hubuum_archive_history_requires_legacy_runtime() FROM PUBLIC;

CREATE FUNCTION public.hubuum_archive_history_entry(registration uuid, ownership uuid, image uuid, revision bigint)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
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
        IF (SELECT max(version) FROM public.__diesel_schema_migrations) <> '20261103000004' THEN
            RAISE EXCEPTION 'Unversioned archive runtime schema is not supported';
        END IF;
        SELECT TRUE INTO STRICT legacy FROM hubuum_control.runtime_registrations
            WHERE id=registration AND ownership_token=ownership FOR SHARE;
    END IF;
    RETURN legacy IS TRUE;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_archive_history_entry(uuid,uuid,uuid,bigint) FROM PUBLIC;

CREATE TABLE hubuum_control.history_archive_catalogs (
    resource uuid PRIMARY KEY CHECK(resource <> '00000000-0000-0000-0000-000000000000'),
    generation bigint NOT NULL CHECK(generation > 0),
    material jsonb NOT NULL CHECK(jsonb_typeof(material)='object' AND octet_length(material::text)<=4194304),
    CHECK((material->>'generation')::bigint=generation AND (material->>'resource')::uuid=resource),
    CHECK(jsonb_array_length(material->'versions')<=10000)
);
CREATE TABLE hubuum_control.history_archive_capture_capacity (
    id smallint PRIMARY KEY CHECK(id=1), captures integer NOT NULL CHECK(captures BETWEEN 0 AND 1024),
    bytes bigint NOT NULL CHECK(bytes BETWEEN 0 AND 1073741824)
);
INSERT INTO hubuum_control.history_archive_capture_capacity VALUES(1,0,0);
CREATE TABLE hubuum_control.history_archive_captures (
    id uuid PRIMARY KEY CHECK(id <> '00000000-0000-0000-0000-000000000000'),
    principal uuid NOT NULL, expires_at timestamptz NOT NULL,
    material jsonb NOT NULL CHECK(jsonb_typeof(material)='object'),
    bytes integer NOT NULL CHECK(bytes BETWEEN 1 AND 5242880),
    CHECK((material->>'id')::uuid=id AND (material->>'principal')::uuid=principal),
    CHECK((material->>'reserved_read_bytes')::bigint BETWEEN 0 AND 67108864)
);
CREATE INDEX history_archive_capture_expiry ON hubuum_control.history_archive_captures(expires_at,id); -- hubuum-compat: bounded-transactional-index
REVOKE ALL ON hubuum_control.history_archive_catalogs, hubuum_control.history_archive_captures,
    hubuum_control.history_archive_capture_capacity FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_archive_catalog_read(subject uuid, locked boolean,
    registration uuid, ownership uuid, image uuid, revision bigint) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
    IF NOT public.hubuum_archive_history_entry(registration,ownership,image,revision) THEN RAISE EXCEPTION 'Archive Legacy owner is unavailable'; END IF;
    IF locked THEN PERFORM pg_advisory_xact_lock(hashtextextended('hubuum-history-archive:'||subject::text,0)); END IF;
    SELECT material INTO result FROM hubuum_control.history_archive_catalogs WHERE resource=subject;
    RETURN result;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_history_archive_catalog_read(uuid,boolean,uuid,uuid,uuid,bigint) FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_archive_catalog_publish(subject uuid, expected bigint, replacement jsonb,
    registration uuid, ownership uuid, image uuid, revision bigint) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE current_generation bigint;
BEGIN
    IF NOT public.hubuum_archive_history_entry(registration,ownership,image,revision) THEN RETURN FALSE; END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('hubuum-history-archive:'||subject::text,0));
    SELECT generation INTO current_generation FROM hubuum_control.history_archive_catalogs WHERE resource=subject;
    IF COALESCE(current_generation,0)<>expected OR (replacement->>'generation')::bigint<>expected+1 THEN RETURN FALSE; END IF;
    INSERT INTO hubuum_control.history_archive_catalogs(resource,generation,material) VALUES(subject,expected+1,replacement)
        ON CONFLICT(resource) DO UPDATE SET generation=EXCLUDED.generation,material=EXCLUDED.material;
    RETURN TRUE;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_history_archive_catalog_publish(uuid,bigint,jsonb,uuid,uuid,uuid,bigint) FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_archive_capture_read(capture uuid, actor uuid, locked boolean,
    registration uuid, ownership uuid, image uuid, revision bigint) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
    IF NOT public.hubuum_archive_history_entry(registration,ownership,image,revision) THEN RAISE EXCEPTION 'Archive Legacy owner is unavailable'; END IF;
    IF locked THEN SELECT material INTO result FROM hubuum_control.history_archive_captures WHERE id=capture AND principal=actor FOR UPDATE;
    ELSE SELECT material INTO result FROM hubuum_control.history_archive_captures WHERE id=capture AND principal=actor; END IF;
    RETURN result;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_history_archive_capture_read(uuid,uuid,boolean,uuid,uuid,uuid,bigint) FROM PUBLIC;

CREATE FUNCTION public.hubuum_history_archive_capture_save(replacement jsonb, expected_work bigint,
    registration uuid, ownership uuid, image uuid, revision bigint) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE target uuid := (replacement->>'id')::uuid; encoded integer := octet_length(replacement::text)+64;
    counter hubuum_control.history_archive_capture_capacity%ROWTYPE; expired record;
BEGIN
    IF NOT public.hubuum_archive_history_entry(registration,ownership,image,revision)
        OR (replacement #>> '{fence,incarnation}')::uuid<>image OR (replacement #>> '{fence,revision}')::bigint<>revision
        OR (replacement->>'runtime')::uuid<>registration THEN RETURN FALSE; END IF;
    IF expected_work IS NOT NULL THEN
        UPDATE hubuum_control.history_archive_captures SET material=replacement
            WHERE id=target AND principal=(replacement->>'principal')::uuid AND expires_at>clock_timestamp()
            AND material-'reserved_read_bytes'=replacement-'reserved_read_bytes'
            AND (material->>'reserved_read_bytes')::bigint=expected_work
            AND (replacement->>'reserved_read_bytes')::bigint>=expected_work;
        RETURN FOUND;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('hubuum-history-archive:'||(replacement #>> '{selection,resource}'),0));
    IF COALESCE((SELECT generation FROM hubuum_control.history_archive_catalogs WHERE resource=(replacement #>> '{selection,resource}')::uuid),0)
        <>(replacement->>'generation')::bigint THEN RETURN FALSE; END IF;
    SELECT * INTO STRICT counter FROM hubuum_control.history_archive_capture_capacity WHERE id=1 FOR UPDATE;
    FOR expired IN DELETE FROM hubuum_control.history_archive_captures WHERE id IN
        (SELECT id FROM hubuum_control.history_archive_captures WHERE expires_at<=clock_timestamp() ORDER BY expires_at,id LIMIT 16 FOR UPDATE) RETURNING bytes LOOP
        counter.captures := counter.captures-1; counter.bytes := counter.bytes-expired.bytes;
    END LOOP;
    IF encoded>5242880 OR counter.captures>=1024 OR counter.bytes+encoded>1073741824 THEN
        UPDATE hubuum_control.history_archive_capture_capacity SET captures=counter.captures,bytes=counter.bytes WHERE id=1;
        RETURN FALSE;
    END IF;
    INSERT INTO hubuum_control.history_archive_captures(id,principal,expires_at,material,bytes)
        VALUES(target,(replacement->>'principal')::uuid,(replacement->>'available_until')::timestamptz,replacement,encoded);
    UPDATE hubuum_control.history_archive_capture_capacity SET captures=counter.captures+1,bytes=counter.bytes+encoded WHERE id=1;
    RETURN TRUE;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_history_archive_capture_save(jsonb,bigint,uuid,uuid,uuid,bigint) FROM PUBLIC;
