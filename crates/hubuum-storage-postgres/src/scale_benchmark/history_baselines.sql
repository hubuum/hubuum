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
