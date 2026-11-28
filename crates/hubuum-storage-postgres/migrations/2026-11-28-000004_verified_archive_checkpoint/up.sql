SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- The identity and destination already live in immutable claim material. The
-- fixed ledger allowance covers this optional SHA-256 manifest digest.
ALTER TABLE hubuum_control.history_archive_publication_claims
    ADD COLUMN verified_manifest_digest BYTEA,
    ADD CONSTRAINT history_archive_verified_digest_size CHECK (octet_length(verified_manifest_digest)=32) NOT VALID;

CREATE FUNCTION public.hubuum_history_archive_checkpoint(parent UUID, parent_token UUID,
    child UUID, child_token UUID, runtime UUID, runtime_token UUID,
    original_request BYTEA, original_intent BYTEA, digest BYTEA)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE manifest JSONB; image UUID; deletion BIGINT; subject UUID; cutoff TIMESTAMPTZ;
BEGIN
    -- An older repeatable snapshot could miss the first catalog insertion even
    -- after taking the resource lock. The normal runtime uses READ COMMITTED.
    IF current_setting('transaction_isolation') NOT IN ('read committed','read uncommitted')
        OR octet_length(digest)<>32 THEN RETURN FALSE; END IF;
    -- These coordinates are immutable. Recheck ownership under locks before
    -- storing evidence. Raw runtime calls go through the same authority gates.
    SELECT convert_from(a.material,'UTF8')::JSONB INTO manifest
      FROM hubuum_control.history_archive_publication_claims a
      WHERE a.publication_id=parent AND a.publication_ownership=parent_token
        AND a.producer_id=child AND a.producer_ownership=child_token
        AND a.request_commitment=original_request AND a.intent_commitment=original_intent;
    IF NOT FOUND THEN RETURN FALSE; END IF;
    image := (manifest #>> '{execution,history,incarnation}')::UUID;
    deletion := (manifest #>> '{execution,history,deletion_revision}')::BIGINT;
    subject := (manifest #>> '{identity,resource}')::UUID;
    cutoff := LEAST((manifest->>'available_until')::TIMESTAMPTZ,
                    (manifest #>> '{execution,deadline}')::TIMESTAMPTZ);
    IF public.hubuum_archive_history_entry(runtime,runtime_token,image,deletion) IS NOT TRUE
        OR public.hubuum_history_producer_execution(parent,child,child_token) IS NOT TRUE
        OR public.hubuum_history_publication_execution(parent,parent_token,image,deletion,
            (manifest #>> '{execution,history,authority_revision}')::BIGINT,
            (manifest #>> '{execution,generation}')::BIGINT,'archive_read') IS NOT TRUE THEN
        RETURN FALSE;
    END IF;
    -- Serialize with catalog publication, including its first insertion.
    PERFORM pg_advisory_xact_lock(hashtextextended('hubuum-history-archive:'||subject::text,0));
    IF COALESCE((SELECT generation FROM hubuum_control.history_archive_catalogs WHERE resource=subject),0)
            IS DISTINCT FROM (manifest->>'generation')::BIGINT
        OR public.hubuum_history_archive_resource_available(subject) IS NOT TRUE THEN RETURN FALSE; END IF;
    PERFORM 1 FROM hubuum_control.history_archive_publication_claims a
        JOIN hubuum_control.history_archive_captures h ON h.id=a.capture_id
        JOIN hubuum_control.history_publication_owners p ON p.id=a.publication_id
        JOIN hubuum_control.history_publication_producers c ON c.publication_id=p.id
        WHERE a.publication_id=parent AND a.publication_ownership=parent_token
          AND a.producer_id=child AND a.producer_ownership=child_token
          AND a.request_commitment=original_request AND a.intent_commitment=original_intent
          AND (a.verified_manifest_digest IS NULL OR a.verified_manifest_digest=digest)
          AND c.id=child AND c.ownership_token=child_token AND c.publication_claimed
          AND p.runtime_id=runtime AND p.producer_count=1 AND h.publication_claimed
          AND h.expires_at>clock_timestamp()
        FOR UPDATE OF a NOWAIT;
    IF NOT FOUND OR cutoff IS NULL OR cutoff<=clock_timestamp() THEN RETURN FALSE; END IF;
    UPDATE hubuum_control.history_archive_publication_claims
       SET verified_manifest_digest=digest
       WHERE publication_id=parent AND verified_manifest_digest IS NULL;
    RETURN TRUE;
EXCEPTION WHEN lock_not_available THEN RETURN FALSE;
END
$function$;
REVOKE ALL ON FUNCTION public.hubuum_history_archive_checkpoint(UUID,UUID,UUID,UUID,UUID,UUID,BYTEA,BYTEA,BYTEA) FROM PUBLIC;
