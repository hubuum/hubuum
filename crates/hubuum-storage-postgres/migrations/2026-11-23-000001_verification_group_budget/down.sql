SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- An oversized verification must expire and be cleaned up before downgrade.
ALTER TABLE auth_interactions DROP CONSTRAINT auth_interactions_document_check1;
ALTER TABLE auth_interactions ADD CONSTRAINT auth_interactions_document_check1
    CHECK (pg_column_size(document) <= 524288);
