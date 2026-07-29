CREATE OR REPLACE FUNCTION hubuum_skip_unchanged_temporal_update()
RETURNS TRIGGER AS $$
BEGIN
    IF to_jsonb(OLD) - 'updated_at' = to_jsonb(NEW) - 'updated_at' THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
