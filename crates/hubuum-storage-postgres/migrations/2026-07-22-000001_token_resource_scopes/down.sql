-- Removing the resource-scope dimension must never widen an existing bearer
-- token. Preserve token metadata and task attribution, but make every token
-- that depended on this schema unusable before its boundary is discarded.
UPDATE tokens
SET revoked_at = COALESCE(revoked_at, CURRENT_TIMESTAMP)
WHERE resource_scoped;

DROP TABLE token_object_scopes;
DROP TABLE token_class_scopes;
DROP TABLE token_collection_scopes;

ALTER TABLE tokens DROP COLUMN resource_scoped;
ALTER TABLE tokens RENAME COLUMN permission_scoped TO scoped;
