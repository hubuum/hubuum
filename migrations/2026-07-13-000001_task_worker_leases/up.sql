ALTER TABLE tasks
    ADD COLUMN lease_token UUID NULL,
    ADD COLUMN lease_expires_at TIMESTAMP NULL,
    ADD COLUMN attempt_count INT NOT NULL DEFAULT 0,
    ADD CONSTRAINT tasks_attempt_count_nonnegative CHECK (attempt_count >= 0),
    ADD CONSTRAINT tasks_lease_pair CHECK (
        (lease_token IS NULL AND lease_expires_at IS NULL)
        OR (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    );

CREATE INDEX idx_tasks_active_lease_expiry
    ON tasks (lease_expires_at, id)
    WHERE status IN ('validating', 'running');
