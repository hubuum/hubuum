-- Removing this restriction beside retained online-expiry authority would make
-- an older producer unsafe. Require explicit qualified forward integration.
DO $$ BEGIN
    IF to_regclass('hubuum_control.history_lifecycle_control') IS NOT NULL
        OR to_regclass('hubuum_control.history_expiry_members') IS NOT NULL
        OR to_regclass('hubuum_control.history_publication_control') IS NOT NULL
        OR to_regclass('hubuum_control.history_publication_owners') IS NOT NULL THEN
        RAISE EXCEPTION 'Cannot remove Legacy archive restriction beside history lifecycle authority';
    END IF;
    IF EXISTS(SELECT 1 FROM hubuum_control.history_archive_catalogs) OR EXISTS(SELECT 1 FROM hubuum_control.history_archive_captures) THEN
        RAISE EXCEPTION 'Cannot discard retained archive catalog or query pins';
    END IF;
END $$;
DROP FUNCTION public.hubuum_history_archive_capture_save(jsonb,bigint,uuid,uuid,uuid,bigint);
DROP FUNCTION public.hubuum_history_archive_capture_read(uuid,uuid,boolean,uuid,uuid,uuid,bigint);
DROP FUNCTION public.hubuum_history_archive_catalog_publish(uuid,bigint,jsonb,uuid,uuid,uuid,bigint);
DROP FUNCTION public.hubuum_history_archive_catalog_read(uuid,boolean,uuid,uuid,uuid,bigint);
DROP TABLE hubuum_control.history_archive_captures,hubuum_control.history_archive_capture_capacity,hubuum_control.history_archive_catalogs;
DROP FUNCTION public.hubuum_archive_history_entry(uuid,uuid,uuid,bigint);
DROP FUNCTION public.hubuum_archive_history_requires_legacy_runtime();
