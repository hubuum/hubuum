SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

-- Serialize the emptiness check with the final trigger execution.
LOCK TABLE hubuum_control.history_archive_captures, hubuum_control.history_archive_reclaimed_pins
    IN ACCESS EXCLUSIVE MODE;
DO $pin$
BEGIN
    IF EXISTS(SELECT 1 FROM hubuum_control.history_archive_reclaimed_pins) THEN
        RAISE EXCEPTION 'Reclaimed archive pin evidence exists; retain this migration and roll forward';
    END IF;
END $pin$;
DROP TRIGGER retain_reclaimed_archive_pin ON hubuum_control.history_archive_captures;
DROP FUNCTION hubuum_control.retain_reclaimed_history_archive_pin();
DROP TABLE hubuum_control.history_archive_reclaimed_pins;
