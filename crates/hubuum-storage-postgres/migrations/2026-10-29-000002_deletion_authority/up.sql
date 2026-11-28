-- This control ledger is excluded from logical replacement. There are no
-- references into replaceable application tables and no runtime write grants.
CREATE TABLE hubuum_control.deletion_authority (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    material JSONB NOT NULL CHECK ((
        jsonb_typeof(material) = 'object'
        AND octet_length(material::text) <= 4194304
        AND material->>'format' = '1'
        AND material ?& ARRAY['lineage', 'epoch', 'revision', 'intents']
        AND (material->>'revision')::bigint >= 0
        AND jsonb_typeof(material->'intents') = 'object'
    ) IS TRUE)
);
INSERT INTO hubuum_control.deletion_authority (id, material)
VALUES (1, jsonb_build_object('format', 1, 'lineage', gen_random_uuid(),
    'epoch', gen_random_uuid(), 'revision', 0, 'intents', '{}'::jsonb));
REVOKE ALL ON hubuum_control.deletion_authority FROM PUBLIC;

CREATE FUNCTION public.hubuum_check_deletion_restore_gate() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    -- Lock against a concurrent authority transition until replacement commits.
    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id = 1 FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Deletion authority is unavailable; keep recovery quarantined';
    END IF;
    IF EXISTS (SELECT 1 FROM hubuum_control.deletion_authority
               WHERE material->'intents' <> '{}'::jsonb) THEN
        RAISE EXCEPTION 'Deletion authority requires reapplication before logical restore; keep recovery quarantined';
    END IF;
END $$;
REVOKE ALL ON FUNCTION public.hubuum_check_deletion_restore_gate() FROM PUBLIC;
