DROP TRIGGER IF EXISTS hubuumclass_relation_skip_unchanged_temporal_update_trg
ON hubuumclass_relation;

DROP TRIGGER IF EXISTS hubuumobject_relation_skip_unchanged_temporal_update_trg
ON hubuumobject_relation;

CREATE OR REPLACE FUNCTION hubuum_skip_unchanged_temporal_update()
RETURNS TRIGGER AS $$
BEGIN
    IF to_jsonb(OLD) - 'updated_at' = to_jsonb(NEW) - 'updated_at' THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
