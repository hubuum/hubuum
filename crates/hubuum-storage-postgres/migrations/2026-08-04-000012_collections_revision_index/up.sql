CREATE INDEX CONCURRENTLY collections_parent_revision_id_idx ON collections (parent_collection_id, revision, id);
