-- Timestamp-only import restores must not be discarded as no-op temporal
-- updates. The transaction-local setting is enabled only by trusted import
-- writes that explicitly preserve supplied timestamps.
CREATE OR REPLACE FUNCTION hubuum_skip_unchanged_temporal_update()
RETURNS TRIGGER AS $$
BEGIN
    IF current_setting('hubuum.preserve_imported_timestamps', true) IS DISTINCT FROM 'on'
       AND to_jsonb(OLD) - 'updated_at' = to_jsonb(NEW) - 'updated_at'
    THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
