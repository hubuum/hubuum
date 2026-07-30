-- Timestamp-only import restores must not be discarded as no-op temporal
-- updates. The transaction-local setting is enabled only by authorized import
-- writes that explicitly preserve supplied timestamps.
CREATE OR REPLACE FUNCTION hubuum_skip_unchanged_temporal_update()
RETURNS TRIGGER AS $$
DECLARE
    preserve_imported_timestamps BOOLEAN :=
        current_setting('hubuum.preserve_imported_timestamps', true) IS NOT DISTINCT FROM 'on';
BEGIN
    IF (
        preserve_imported_timestamps
        AND to_jsonb(OLD) = to_jsonb(NEW)
    ) OR (
        NOT preserve_imported_timestamps
        AND to_jsonb(OLD) - 'updated_at' = to_jsonb(NEW) - 'updated_at'
    )
    THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Relation tables were omitted when no-op suppression was introduced. Imports
-- can now update relation timestamps, so apply the same history behavior there.
CREATE TRIGGER hubuumclass_relation_skip_unchanged_temporal_update_trg
BEFORE UPDATE ON hubuumclass_relation
FOR EACH ROW EXECUTE FUNCTION hubuum_skip_unchanged_temporal_update();

CREATE TRIGGER hubuumobject_relation_skip_unchanged_temporal_update_trg
BEFORE UPDATE ON hubuumobject_relation
FOR EACH ROW EXECUTE FUNCTION hubuum_skip_unchanged_temporal_update();
