SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

LOCK TABLE hubuum_control.history_archive_catalogs,
    hubuum_control.history_archive_pending_displacements,
    hubuum_control.history_archive_reclaimed_pins IN ACCESS EXCLUSIVE MODE;
DO $guard$
BEGIN
    IF EXISTS(SELECT 1 FROM hubuum_control.history_archive_reclaimed_pins WHERE catalog_displaced) THEN
        RAISE EXCEPTION 'Catalog displacement evidence exists; retain this migration and roll forward';
    END IF;
END $guard$;
DROP FUNCTION public.hubuum_history_archive_record_displacements(uuid,bigint,jsonb,uuid,uuid,uuid,bigint);
DROP TRIGGER require_archive_catalog_accounting ON hubuum_control.history_archive_catalogs;
DROP FUNCTION hubuum_control.require_archive_catalog_accounting();
DROP FUNCTION hubuum_control.archive_displacements_digest(jsonb);
DROP TABLE hubuum_control.history_archive_pending_displacements;
DROP FUNCTION hubuum_control.check_archive_catalog_accounting();
ALTER TABLE hubuum_control.history_archive_reclaimed_pins DROP COLUMN catalog_displaced;
