SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

LOCK TABLE hubuum_control.history_archive_publication_claims IN ACCESS EXCLUSIVE MODE;
DO $block$
BEGIN
    IF EXISTS (SELECT 1 FROM hubuum_control.history_archive_publication_claims
               WHERE verified_manifest_digest IS NOT NULL) THEN
        RAISE EXCEPTION 'Reconcile verified history archive claims before downgrade';
    END IF;
END
$block$;
DROP FUNCTION public.hubuum_history_archive_checkpoint(UUID,UUID,UUID,UUID,UUID,UUID,BYTEA,BYTEA,BYTEA);
ALTER TABLE hubuum_control.history_archive_publication_claims DROP COLUMN verified_manifest_digest;
