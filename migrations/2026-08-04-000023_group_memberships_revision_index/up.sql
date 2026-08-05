CREATE INDEX CONCURRENTLY group_memberships_group_revision_idx ON group_memberships (group_id, revision, principal_id);
