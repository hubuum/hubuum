SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Give only hosted verification groups additional room in the interaction.
ALTER TABLE auth_interactions DROP CONSTRAINT auth_interactions_document_check1; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE auth_interactions ADD CONSTRAINT auth_interactions_document_check1
    CHECK (pg_column_size(document) <= 524288 OR (
        (document#>>'{lifecycle,binding,authority,purpose}' = 'provider_verification') IS TRUE
        AND (jsonb_typeof(document#>'{lifecycle,phase,verification,groups}') = 'array') IS TRUE
        AND pg_column_size(document #- '{lifecycle,phase,verification,groups}') <= 524288
        AND pg_column_size(document) <= 1048576
    )) NOT VALID;
ALTER TABLE auth_interactions VALIDATE CONSTRAINT auth_interactions_document_check1;
