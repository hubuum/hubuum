-- Imports can explicitly restore created_at/updated_at values. Ordinary updates
-- still receive the current timestamp; only transaction-scoped import writes
-- that opt in may preserve their supplied updated_at value.
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    IF current_setting('hubuum.preserve_imported_timestamps', true) IS DISTINCT FROM 'on' THEN
        NEW.updated_at = now();
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';
