CREATE INDEX CONCURRENTLY events_before_revision_id_idx ON events (before_revision, id) WHERE before_revision IS NOT NULL;
