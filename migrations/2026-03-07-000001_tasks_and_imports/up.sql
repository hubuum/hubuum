CREATE TABLE tasks (
    id SERIAL PRIMARY KEY,
    kind VARCHAR NOT NULL CHECK (kind IN ('import', 'report', 'export', 'reindex')),
    status VARCHAR NOT NULL CHECK (
        status IN (
            'queued',
            'validating',
            'running',
            'succeeded',
            'failed',
            'partially_succeeded',
            'cancelled'
        )
    ),
    submitted_by INT REFERENCES users (id) ON DELETE SET NULL,
    idempotency_key VARCHAR NULL,
    request_hash VARCHAR NULL,
    request_payload JSONB NULL,
    summary TEXT NULL,
    total_items INT NOT NULL DEFAULT 0,
    processed_items INT NOT NULL DEFAULT 0,
    success_items INT NOT NULL DEFAULT 0,
    failed_items INT NOT NULL DEFAULT 0,
    request_redacted_at TIMESTAMP NULL,
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    deleted_at TIMESTAMP NULL,
    deleted_by INT NULL REFERENCES users (id) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (submitted_by, idempotency_key)
);

CREATE TABLE task_events (
    id SERIAL PRIMARY KEY,
    task_id INT REFERENCES tasks (id) ON DELETE CASCADE NOT NULL,
    event_type VARCHAR NOT NULL,
    message TEXT NOT NULL,
    data JSONB NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE import_task_results (
    id SERIAL PRIMARY KEY,
    task_id INT REFERENCES tasks (id) ON DELETE CASCADE NOT NULL,
    item_ref VARCHAR NULL,
    entity_kind VARCHAR NOT NULL,
    action VARCHAR NOT NULL,
    identifier TEXT NULL,
    outcome VARCHAR NOT NULL,
    error TEXT NULL,
    details JSONB NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_tasks_status_created_at ON tasks (status, created_at);
CREATE INDEX idx_tasks_submitted_by ON tasks (submitted_by);
CREATE INDEX idx_tasks_deleted_at ON tasks (deleted_at);
CREATE INDEX idx_tasks_active_status ON tasks (deleted_at, status);
CREATE INDEX idx_task_events_task_id_created_at ON task_events (task_id, created_at);
CREATE INDEX idx_import_task_results_task_id_created_at ON import_task_results (task_id, created_at);

CREATE TRIGGER update_tasks_updated_at
BEFORE UPDATE ON tasks
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();
