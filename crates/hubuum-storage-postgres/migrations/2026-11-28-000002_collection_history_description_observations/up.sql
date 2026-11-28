SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

-- The _history suffix keeps derived commitments read-only for runtime logins
-- under both existing and upgraded grant reconciliation.
CREATE TABLE public.collection_description_digest_history (
    history_id bigint PRIMARY KEY REFERENCES public.collections_history(history_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    digest bytea NOT NULL CHECK (octet_length(digest) = 32),
    encoded_bytes integer NOT NULL CHECK (encoded_bytes >= 0)
);
REVOKE ALL ON public.collection_description_digest_history FROM PUBLIC;

-- Match primary_commitment_v1's UTF-8 digest and the escaped description
-- length used for capture admission. The two JSON quotes are framing.
CREATE FUNCTION public.hubuum_observe_collection_description(value text)
RETURNS TABLE (digest bytea, encoded_bytes integer)
LANGUAGE sql STABLE STRICT SET search_path = pg_catalog AS $$
    SELECT sha256(convert_to(value, 'UTF8')),
           octet_length(convert_to(to_json(value)::text, 'UTF8')) - 2
$$;

CREATE FUNCTION public.hubuum_refresh_collection_description_observation()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF octet_length(NEW.description) BETWEEN 1048576 AND 4194304 THEN
        INSERT INTO public.collection_description_digest_history (history_id, digest, encoded_bytes)
            SELECT NEW.history_id, digest, encoded_bytes
            FROM public.hubuum_observe_collection_description(NEW.description)
            ON CONFLICT (history_id) DO UPDATE
                SET digest = EXCLUDED.digest, encoded_bytes = EXCLUDED.encoded_bytes;
    ELSE
        DELETE FROM public.collection_description_digest_history WHERE history_id = NEW.history_id;
    END IF;
    RETURN NULL;
END $$;

CREATE TRIGGER observe_large_collection_description AFTER INSERT ON public.collections_history
    FOR EACH ROW WHEN (octet_length(NEW.description) BETWEEN 1048576 AND 4194304)
    EXECUTE FUNCTION public.hubuum_refresh_collection_description_observation();
CREATE TRIGGER refresh_large_collection_description AFTER UPDATE ON public.collections_history
    FOR EACH ROW WHEN (
        (octet_length(NEW.description) BETWEEN 1048576 AND 4194304
         OR octet_length(OLD.description) BETWEEN 1048576 AND 4194304)
        AND OLD.description COLLATE "C" IS DISTINCT FROM NEW.description COLLATE "C"
    ) EXECUTE FUNCTION public.hubuum_refresh_collection_description_observation();

-- CREATE TRIGGER holds the source table against concurrent writes until the
-- backfill commits. Existing and subsequently restored histories use the same
-- derivation as new writes.
INSERT INTO public.collection_description_digest_history (history_id, digest, encoded_bytes)
    SELECT h.history_id, o.digest, o.encoded_bytes
    FROM public.collections_history h
    CROSS JOIN LATERAL public.hubuum_observe_collection_description(h.description) o
    WHERE octet_length(h.description) BETWEEN 1048576 AND 4194304;
