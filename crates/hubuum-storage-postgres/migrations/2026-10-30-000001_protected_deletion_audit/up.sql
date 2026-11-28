SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

-- This is an offline writer boundary. Existing signed v1/v2 checkpoints keep
-- their exact bytes; newly audited decisions use v3 with minimum event evidence.
ALTER TABLE hubuum_control.deletion_authority DROP CONSTRAINT deletion_authority_material_check; -- hubuum-compat: reviewed-offline-drop
ALTER TABLE hubuum_control.deletion_authority
    ADD CONSTRAINT deletion_authority_material_check CHECK ((
        jsonb_typeof(material) = 'object'
        AND octet_length(material::text) <= 4194304
        AND material->>'format' IN ('1', '2', '3')
        AND material ?& ARRAY['lineage', 'epoch', 'revision', 'intents']
        AND (material->>'revision')::bigint >= 0
        AND jsonb_typeof(material->'intents') = 'object'
    ) IS TRUE) NOT VALID;
ALTER TABLE hubuum_control.deletion_authority VALIDATE CONSTRAINT deletion_authority_material_check;
