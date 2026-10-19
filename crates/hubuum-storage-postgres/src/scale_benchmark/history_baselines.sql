-- Bulk loading suppresses temporal triggers. Every live temporal resource still
-- needs one open history row, including resources outside the history overlay.
-- Preserve the root collection baseline and the explicit multi-revision overlay.
DO $baseline$
DECLARE
    resource_table text;
    history_table text;
    columns text;
    projection text;
BEGIN
    FOREACH resource_table IN ARRAY ARRAY[
        'collections', 'hubuumclass', 'hubuumclass_relation', 'hubuumobject',
        'hubuumobject_relation', 'export_templates', 'remote_targets'
    ] LOOP
        history_table := resource_table || '_history';
        SELECT string_agg(format('%I', attname), ', ' ORDER BY attnum),
               string_agg(format('resource.%I', attname), ', ' ORDER BY attnum)
          INTO columns, projection
          FROM pg_attribute
         WHERE attrelid = resource_table::regclass
           AND attnum > 0 AND NOT attisdropped;
        EXECUTE format(
            'INSERT INTO %I (%s, op, valid_from, valid_to, actor_id, history_id,
                            actor_kind, initiator_user_id, task_id)
             SELECT %s, ''I'', resource.updated_at AT TIME ZONE ''UTC'', NULL,
                    NULL, nextval(%L::regclass), ''system'', NULL, NULL
             FROM %I resource
             WHERE NOT EXISTS (
                 SELECT 1 FROM %I history WHERE history.id = resource.id
             )',
            history_table, columns, projection, resource_table || '_history_seq',
            resource_table, history_table
        );
    END LOOP;
END;
$baseline$;
-- Bulk loading bypasses triggers. Retain the complete synthetic shared set,
-- including empty classes, with stable identities and timestamps.
INSERT INTO computed_field_history (history_id, class_id, valid_from, evaluator_version, definitions)
SELECT class.id, class.id, class.updated_at AT TIME ZONE 'UTC', 1,
    COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'id', field.id, 'class_id', field.class_id, 'revision', field.revision,
        'definition', jsonb_build_object(
            'key', field.key, 'label', field.label, 'description', field.description,
            'operation', field.operation, 'result_type', field.result_type,
            'enabled', field.enabled, 'semantics_version', field.semantics_version
        )
    ) ORDER BY field.id) FROM computed_field_definitions field
        WHERE field.class_id = class.id AND field.visibility = 'shared'), '[]'::jsonb)
FROM hubuumclass class ORDER BY class.id;
