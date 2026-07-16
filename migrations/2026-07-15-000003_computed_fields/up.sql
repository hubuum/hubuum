CREATE TABLE computed_field_definitions (
    id SERIAL PRIMARY KEY,
    class_id INTEGER NOT NULL REFERENCES hubuumclass(id) ON DELETE CASCADE,
    visibility VARCHAR NOT NULL,
    owner_user_id INTEGER NULL REFERENCES users(id) ON DELETE CASCADE,
    key VARCHAR NOT NULL,
    label VARCHAR NOT NULL,
    description VARCHAR NOT NULL DEFAULT '',
    operation JSONB NOT NULL,
    result_type VARCHAR NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    revision BIGINT NOT NULL DEFAULT 1,
    semantics_version SMALLINT NOT NULL DEFAULT 1,
    created_by INTEGER NULL REFERENCES principals(id) ON DELETE SET NULL,
    updated_by INTEGER NULL REFERENCES principals(id) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_at TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    CONSTRAINT computed_field_visibility_check
        CHECK (visibility IN ('personal', 'shared')),
    CONSTRAINT computed_field_owner_check CHECK (
        (visibility = 'personal' AND owner_user_id IS NOT NULL)
        OR (visibility = 'shared' AND owner_user_id IS NULL)
    ),
    CONSTRAINT computed_field_key_check
        CHECK (key ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT computed_field_label_check
        CHECK (octet_length(label) BETWEEN 1 AND 128),
    CONSTRAINT computed_field_description_check
        CHECK (octet_length(description) <= 2048),
    CONSTRAINT computed_field_operation_check
        CHECK (jsonb_typeof(operation) = 'object'),
    CONSTRAINT computed_field_result_type_check
        CHECK (result_type IN ('string', 'number', 'integer', 'boolean', 'object', 'array')),
    CONSTRAINT computed_field_revision_check CHECK (revision > 0),
    CONSTRAINT computed_field_semantics_version_check CHECK (semantics_version = 1)
);

CREATE UNIQUE INDEX computed_field_shared_key
    ON computed_field_definitions (class_id, key)
    WHERE visibility = 'shared';

CREATE UNIQUE INDEX computed_field_personal_key
    ON computed_field_definitions (owner_user_id, class_id, key)
    WHERE visibility = 'personal';

CREATE INDEX computed_field_class_visibility
    ON computed_field_definitions (class_id, visibility, id);

CREATE INDEX computed_field_personal_owner
    ON computed_field_definitions (owner_user_id, class_id, id)
    WHERE visibility = 'personal';

CREATE TABLE class_computation_state (
    class_id INTEGER PRIMARY KEY REFERENCES hubuumclass(id) ON DELETE CASCADE,
    evaluation_revision BIGINT NOT NULL DEFAULT 0,
    rebuild_status VARCHAR NOT NULL DEFAULT 'ready',
    active_task_id INTEGER NULL REFERENCES tasks(id) ON DELETE SET NULL,
    last_error VARCHAR NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_at TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    CONSTRAINT class_computation_revision_check CHECK (evaluation_revision >= 0),
    CONSTRAINT class_computation_status_check
        CHECK (rebuild_status IN ('ready', 'rebuilding', 'failed'))
);

CREATE INDEX class_computation_active_task
    ON class_computation_state (active_task_id)
    WHERE active_task_id IS NOT NULL;

CREATE TABLE object_computed_data (
    object_id INTEGER PRIMARY KEY REFERENCES hubuumobject(id) ON DELETE CASCADE,
    class_id INTEGER NOT NULL REFERENCES hubuumclass(id) ON DELETE CASCADE,
    evaluation_revision BIGINT NOT NULL,
    source_data_sha256 VARCHAR(64) NOT NULL,
    values JSONB NOT NULL DEFAULT '{}'::jsonb,
    errors JSONB NOT NULL DEFAULT '{}'::jsonb,
    computed_at TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    CONSTRAINT object_computed_revision_check CHECK (evaluation_revision >= 0),
    CONSTRAINT object_computed_hash_check
        CHECK (source_data_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT object_computed_values_check CHECK (jsonb_typeof(values) = 'object'),
    CONSTRAINT object_computed_errors_check CHECK (jsonb_typeof(errors) = 'object')
);

CREATE INDEX object_computed_class_revision
    ON object_computed_data (class_id, evaluation_revision, object_id);
