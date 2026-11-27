WITH candidates AS (
    SELECT h.*,
        logical_identity #>> '{subject,version}' = $4::text AS project_description,
        octet_length(name) + octet_length(description) <= 4194304 AS fits_raw
    FROM public.collections_history h
    WHERE logical_identity #>> '{subject,identity,identity}' = $1
      AND (
          ($2::text IS NOT NULL AND logical_identity #>> '{subject,version}' = $2)
          OR ($2::text IS NULL AND valid_from <= $3 AND (valid_to IS NULL OR $3 < valid_to))
      )
    ORDER BY valid_from DESC, history_id DESC
    LIMIT 2
), encoded AS MATERIALIZED (
    SELECT id, name, description, logical_identity, project_description,
        CASE WHEN fits_raw AND project_description THEN to_json(description)::text END AS quoted_description,
        CASE WHEN fits_raw THEN
            jsonb_build_object(
                'identity', jsonb_build_object(
                    'resource', logical_identity #> '{subject,identity,identity}',
                    'version', logical_identity #> '{subject,version}'
                ),
                'valid_from', valid_from, 'valid_to', valid_to,
                'payload', jsonb_build_object(
                    'id', id, 'name', name,
                    'description', CASE WHEN project_description THEN '' ELSE description END,
                    'created_at', created_at AT TIME ZONE 'UTC',
                    'updated_at', updated_at AT TIME ZONE 'UTC',
                    'parent_collection_id', parent_collection_id, 'revision', revision,
                    'logical_identity', logical_identity
                )
            )
        END AS row
    FROM candidates
), bounded AS MATERIALIZED (
    -- The projected row already includes the two quotes around the empty
    -- description. Restore the exact original JSON byte count for admission.
    SELECT *, row IS NULL OR
        octet_length(row::text) + COALESCE(octet_length(quoted_description) - 2, 0) > 4194304 AS oversized
    FROM encoded
)
SELECT COALESCE((
        SELECT jsonb_agg(CASE
            WHEN oversized THEN '__oversized__'
            ELSE row::text
        END) FROM bounded
    ), '[]'::jsonb) AS material,
    COALESCE((
        SELECT jsonb_agg(jsonb_build_object(
            'collection', id, 'name', name, 'identity', jsonb_build_object(
                'resource', logical_identity #> '{subject,identity,identity}',
                'version', logical_identity #> '{subject,version}'
            )
        )) FROM bounded
    ), '[]'::jsonb) AS authorization,
    (SELECT sha256(convert_to(description, 'UTF8')) FROM bounded
        WHERE project_description AND NOT oversized LIMIT 1) AS description_digest,
    (SELECT octet_length(convert_to(quoted_description, 'UTF8')) - 2 FROM bounded
        WHERE project_description AND NOT oversized LIMIT 1) AS encoded_description_bytes,
    EXISTS(SELECT 1 FROM public.collections
        WHERE logical_identity #>> '{subject,identity,identity}' = $1
          AND $3 < created_at AT TIME ZONE 'UTC') AS known_empty,
    EXISTS(SELECT 1 FROM public.collections
        WHERE logical_identity #>> '{subject,identity,identity}' = $1) AS live
