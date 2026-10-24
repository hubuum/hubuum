-- Refuse rollback while new-format stages need an executor from this release.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM restore_artifact_chunks) THEN
        RAISE EXCEPTION 'Complete or expire chunked restore stages before rolling back';
    END IF;
END $$;
DROP TRIGGER erase_terminal_restore_chunks ON restore_jobs;
DROP FUNCTION erase_terminal_restore_chunks();
DROP TABLE restore_artifact_chunks;
