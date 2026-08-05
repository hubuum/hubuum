CREATE INDEX CONCURRENTLY events_after_revision_id_idx ON events (after_revision, id) WHERE after_revision IS NOT NULL;
