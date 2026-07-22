DROP TABLE token_object_scopes;
DROP TABLE token_class_scopes;
DROP TABLE token_collection_scopes;

ALTER TABLE tokens DROP COLUMN resource_scoped;
ALTER TABLE tokens RENAME COLUMN permission_scoped TO scoped;
