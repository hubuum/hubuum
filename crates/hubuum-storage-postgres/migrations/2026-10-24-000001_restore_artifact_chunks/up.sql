-- Durable, bounded restore uploads shared by API and separately deployed executors.
-- Existing stages retain their BYTEA document and remain readable.
CREATE TABLE restore_artifact_chunks (
    restore_job_id BIGINT NOT NULL REFERENCES restore_jobs(id) ON DELETE CASCADE,
    ordinal INT NOT NULL CHECK (ordinal >= 0),
    content BYTEA NOT NULL CHECK (octet_length(content) BETWEEN 1 AND 1048576),
    PRIMARY KEY (restore_job_id, ordinal)
);

CREATE FUNCTION erase_terminal_restore_chunks() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status IN ('failed', 'expired') THEN
        DELETE FROM restore_artifact_chunks WHERE restore_job_id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER erase_terminal_restore_chunks
AFTER UPDATE OF status ON restore_jobs
FOR EACH ROW EXECUTE FUNCTION erase_terminal_restore_chunks();
