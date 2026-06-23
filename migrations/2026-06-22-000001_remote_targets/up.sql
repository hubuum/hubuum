ALTER TABLE permissions
    ADD COLUMN has_read_remote_target BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN has_create_remote_target BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN has_update_remote_target BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN has_delete_remote_target BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN has_execute_remote_target BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE tasks
    DROP CONSTRAINT tasks_kind_check,
    ADD CONSTRAINT tasks_kind_check CHECK (kind IN ('import', 'report', 'export', 'reindex', 'remote_call'));

CREATE TABLE remote_targets (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    name VARCHAR NOT NULL,
    description VARCHAR NOT NULL DEFAULT '',
    method VARCHAR NOT NULL CHECK (method IN ('get', 'post', 'patch', 'delete')),
    url_template TEXT NOT NULL,
    headers_template JSONB NOT NULL DEFAULT '{}'::jsonb,
    body_template TEXT NULL,
    auth_config JSONB NOT NULL DEFAULT '{"type":"none"}'::jsonb,
    allowed_subject_types JSONB NOT NULL,
    timeout_ms INT NOT NULL DEFAULT 10000,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (namespace_id, name),
    CHECK (timeout_ms > 0),
    CHECK (jsonb_typeof(headers_template) = 'object'),
    CHECK (jsonb_typeof(auth_config) = 'object'),
    CHECK (
        jsonb_typeof(allowed_subject_types) = 'array'
        AND jsonb_array_length(allowed_subject_types) > 0
        AND allowed_subject_types <@ '["namespace", "class", "object", "class_relation", "object_relation"]'::jsonb
    )
);

CREATE TABLE remote_call_results (
    id SERIAL PRIMARY KEY,
    task_id INT REFERENCES tasks (id) ON DELETE CASCADE NOT NULL UNIQUE,
    target_id INT REFERENCES remote_targets (id) ON DELETE SET NULL,
    subject_type VARCHAR NOT NULL CHECK (subject_type IN ('namespace', 'class', 'object', 'class_relation', 'object_relation')),
    subject_id INT NOT NULL,
    method VARCHAR NOT NULL,
    rendered_url TEXT NOT NULL,
    response_status INT NULL,
    response_headers JSONB NULL,
    response_body_preview TEXT NULL,
    duration_ms INT NOT NULL DEFAULT 0,
    success BOOLEAN NOT NULL,
    error TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_remote_targets_namespace_id ON remote_targets(namespace_id);
CREATE INDEX idx_remote_targets_enabled ON remote_targets(enabled);
CREATE INDEX idx_remote_call_results_target_id ON remote_call_results(target_id);
CREATE INDEX idx_remote_call_results_subject ON remote_call_results(subject_type, subject_id);

CREATE TRIGGER update_remote_targets_updated_at
BEFORE UPDATE ON remote_targets
FOR EACH ROW EXECUTE FUNCTION update_modified_column();
