SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

LOCK TABLE hubuum_control.deletion_authority IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM hubuum_control.deletion_authority WHERE material->>'format' = '3') THEN
        RAISE EXCEPTION 'Cannot downgrade retained protected deletion audit evidence';
    END IF;
END $$;
ALTER TABLE hubuum_control.deletion_authority DROP CONSTRAINT deletion_authority_material_check;
ALTER TABLE hubuum_control.deletion_authority
    ADD CONSTRAINT deletion_authority_material_check CHECK ((
        jsonb_typeof(material) = 'object'
        AND octet_length(material::text) <= 4194304
        AND material->>'format' IN ('1', '2')
        AND material ?& ARRAY['lineage', 'epoch', 'revision', 'intents']
        AND (material->>'revision')::bigint >= 0
        AND jsonb_typeof(material->'intents') = 'object'
    ) IS TRUE) NOT VALID;
ALTER TABLE hubuum_control.deletion_authority VALIDATE CONSTRAINT deletion_authority_material_check;
