-- Independent artifact staging. No foreign keys into replaceable application
-- data: cleanup obligations must survive task purge and logical restore.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE artifact_destinations (
    id UUID PRIMARY KEY CHECK (id <> '00000000-0000-0000-0000-000000000000'),
    name TEXT NOT NULL UNIQUE CHECK (name ~ '^[A-Za-z0-9_.-]{1,128}$')
);

CREATE TABLE artifact_attempts (
    artifact_id UUID PRIMARY KEY CHECK (artifact_id <> '00000000-0000-0000-0000-000000000000'),
    attempt_id UUID NOT NULL UNIQUE CHECK (attempt_id <> '00000000-0000-0000-0000-000000000000'),
    destination_id UUID NOT NULL REFERENCES artifact_destinations(id) ON DELETE RESTRICT,
    object_version UUID NOT NULL UNIQUE CHECK (object_version <> '00000000-0000-0000-0000-000000000000'),
    state TEXT NOT NULL DEFAULT 'staging' CHECK (state IN ('staging', 'sealed', 'published', 'deleting', 'discarded')),
    block_bytes INT NOT NULL CHECK (block_bytes BETWEEN 1 AND 1048576),
    max_bytes BIGINT NOT NULL CHECK (max_bytes BETWEEN 0 AND 1073741824),
    max_blocks INT NOT NULL CHECK (max_blocks BETWEEN 0 AND 1024),
    max_manifest_bytes INT NOT NULL CHECK (max_manifest_bytes BETWEEN 117 AND 262144),
    byte_size BIGINT NOT NULL DEFAULT 0 CHECK (byte_size BETWEEN 0 AND max_bytes),
    block_count INT NOT NULL DEFAULT 0 CHECK (block_count BETWEEN 0 AND max_blocks),
    manifest BYTEA CHECK (octet_length(manifest) BETWEEN 117 AND max_manifest_bytes),
    manifest_sha256 BYTEA CHECK (octet_length(manifest_sha256) = 32),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (clock_timestamp() + INTERVAL '1 hour'),
    CHECK (max_bytes <= block_bytes::bigint * max_blocks),
    CHECK (state NOT IN ('sealed', 'published') OR manifest IS NOT NULL),
    CHECK (state <> 'published' OR manifest_sha256 IS NOT NULL)
);
-- This table is empty until this transaction commits; the build is bounded.
CREATE INDEX artifact_attempts_cleanup ON artifact_attempts(expires_at, artifact_id)
    WHERE state IN ('staging', 'sealed', 'deleting'); -- hubuum-compat: bounded-transactional-index

CREATE TABLE artifact_chunks (
    artifact_id UUID NOT NULL REFERENCES artifact_attempts(artifact_id) ON DELETE CASCADE,
    ordinal INT NOT NULL CHECK (ordinal BETWEEN 0 AND 1023),
    content BYTEA NOT NULL CHECK (octet_length(content) BETWEEN 1 AND 1048576),
    PRIMARY KEY (artifact_id, ordinal)
);

CREATE FUNCTION hubuum_guard_artifact_destination() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Artifact destination identities and names are immutable';
END;
$$;
CREATE TRIGGER artifact_destination_immutable BEFORE UPDATE ON artifact_destinations
    FOR EACH ROW EXECUTE FUNCTION hubuum_guard_artifact_destination();

CREATE FUNCTION hubuum_guard_artifact_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'staging' OR NEW.byte_size <> 0 OR NEW.block_count <> 0
            OR NEW.manifest IS NOT NULL OR NEW.manifest_sha256 IS NOT NULL THEN
            RAISE EXCEPTION 'An artifact must begin as empty staging';
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Artifact attempt tombstones preserve cancellation fences';
    END IF;
    IF ROW(NEW.artifact_id, NEW.attempt_id, NEW.destination_id, NEW.object_version,
           NEW.block_bytes, NEW.max_bytes, NEW.max_blocks, NEW.max_manifest_bytes, NEW.expires_at)
       IS DISTINCT FROM
       ROW(OLD.artifact_id, OLD.attempt_id, OLD.destination_id, OLD.object_version,
           OLD.block_bytes, OLD.max_bytes, OLD.max_blocks, OLD.max_manifest_bytes, OLD.expires_at) THEN
        RAISE EXCEPTION 'Artifact identity and limits are immutable';
    END IF;
    IF NOT (NEW.state = OLD.state
        OR (OLD.state = 'staging' AND NEW.state IN ('sealed', 'deleting'))
        OR (OLD.state = 'sealed' AND NEW.state IN ('published', 'deleting'))
        OR (OLD.state = 'deleting' AND NEW.state = 'discarded')) THEN
        RAISE EXCEPTION 'Invalid artifact lifecycle transition';
    END IF;
    IF OLD.state <> 'staging' AND ROW(NEW.byte_size, NEW.block_count) IS DISTINCT FROM ROW(OLD.byte_size, OLD.block_count) THEN
        RAISE EXCEPTION 'Sealed artifact accounting is immutable';
    END IF;
    IF NEW.manifest IS DISTINCT FROM OLD.manifest AND NOT (
        (OLD.state = 'staging' AND NEW.state = 'sealed')
        OR (OLD.state = 'deleting' AND NEW.state = 'discarded' AND NEW.manifest IS NULL)) THEN
        RAISE EXCEPTION 'Sealed artifact manifest is immutable';
    END IF;
    IF NEW.manifest_sha256 IS DISTINCT FROM OLD.manifest_sha256 AND NOT (OLD.state = 'sealed' AND NEW.state = 'published') THEN
        RAISE EXCEPTION 'Published artifact verification is immutable';
    END IF;
    IF NEW.state IN ('sealed', 'published') AND NEW.state <> OLD.state AND OLD.expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'Artifact staging expired';
    END IF;
    IF NEW.state = 'discarded' AND (NEW.manifest IS NOT NULL OR EXISTS (
        SELECT 1 FROM artifact_chunks WHERE artifact_id = NEW.artifact_id)) THEN
        RAISE EXCEPTION 'Discarded artifacts cannot retain physical bytes';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER artifact_attempt_guard BEFORE INSERT OR UPDATE OR DELETE ON artifact_attempts
    FOR EACH ROW EXECUTE FUNCTION hubuum_guard_artifact_attempt();

CREATE FUNCTION hubuum_guard_artifact_chunk() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    attempt artifact_attempts%ROWTYPE;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'Artifact chunks are immutable';
    ELSIF TG_OP = 'DELETE' THEN
        SELECT * INTO STRICT attempt FROM artifact_attempts WHERE artifact_id = OLD.artifact_id FOR UPDATE;
        IF attempt.state <> 'deleting' THEN
            RAISE EXCEPTION 'Artifact cleanup must fence publication first';
        END IF;
        RETURN OLD;
    END IF;
    SELECT * INTO STRICT attempt FROM artifact_attempts WHERE artifact_id = NEW.artifact_id FOR UPDATE;
    IF attempt.state <> 'staging' OR attempt.expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'Artifact attempt is not writable';
    END IF;
    IF NEW.ordinal <> attempt.block_count OR octet_length(NEW.content) > attempt.block_bytes
        OR 117 + (NEW.ordinal + 1) * 36 > attempt.max_manifest_bytes
        OR EXISTS (SELECT 1 FROM artifact_chunks WHERE artifact_id = NEW.artifact_id
                   AND ordinal = NEW.ordinal - 1 AND octet_length(content) <> attempt.block_bytes) THEN
        RAISE EXCEPTION 'Artifact block position, length or manifest budget is invalid';
    END IF;
    UPDATE artifact_attempts SET byte_size = byte_size + octet_length(NEW.content), block_count = block_count + 1
        WHERE artifact_id = NEW.artifact_id;
    RETURN NEW;
END;
$$;
CREATE TRIGGER artifact_chunk_guard BEFORE INSERT OR UPDATE OR DELETE ON artifact_chunks
    FOR EACH ROW EXECUTE FUNCTION hubuum_guard_artifact_chunk();
