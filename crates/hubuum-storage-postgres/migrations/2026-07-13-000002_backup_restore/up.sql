ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check;
ALTER TABLE tasks
    ADD CONSTRAINT tasks_kind_check
    CHECK (kind IN ('import', 'export', 'backup', 'reindex', 'remote_call'));

CREATE TABLE backup_task_outputs (
    id SERIAL PRIMARY KEY,
    task_id INT REFERENCES tasks (id) ON DELETE CASCADE NOT NULL UNIQUE,
    document BYTEA NOT NULL,
    byte_size BIGINT NOT NULL CHECK (byte_size >= 0),
    sha256 VARCHAR(64) NOT NULL CHECK (length(sha256) = 64),
    output_expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_backup_task_outputs_output_expires_at
    ON backup_task_outputs (output_expires_at);

-- Restore control-plane state deliberately has no foreign keys to application
-- identity/task rows: a restore transaction replaces those rows while its job
-- is in flight. A successful restore deletes every staging job after recording
-- its immutable initiator snapshot in the restore provenance event.
CREATE TABLE restore_jobs (
    id BIGSERIAL PRIMARY KEY,
    status VARCHAR NOT NULL CHECK (
        status IN (
            'validated', 'confirmed', 'failed', 'expired'
        )
    ),
    requested_by INT NULL,
    requested_by_identity_scope VARCHAR NOT NULL,
    requested_by_name VARCHAR NOT NULL,
    document BYTEA NOT NULL,
    byte_size BIGINT NOT NULL CHECK (byte_size >= 0),
    sha256 VARCHAR(64) NOT NULL CHECK (length(sha256) = 64),
    capability_hash VARCHAR(64) NOT NULL CHECK (length(capability_hash) = 64),
    validation_summary JSONB NOT NULL,
    error TEXT NULL,
    expires_at TIMESTAMP NOT NULL,
    confirmed_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_restore_jobs_status_created_at
    ON restore_jobs (status, created_at);
CREATE INDEX idx_restore_jobs_expires_at
    ON restore_jobs (expires_at);

CREATE TABLE system_maintenance (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
    state VARCHAR NOT NULL DEFAULT 'normal'
        CHECK (state IN ('normal', 'draining')),
    restore_job_id BIGINT NULL REFERENCES restore_jobs (id) ON DELETE SET NULL,
    entered_at TIMESTAMP NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO system_maintenance (id) VALUES (1);

CREATE TABLE server_instances (
    instance_id UUID PRIMARY KEY,
    maintenance_generation BIGINT NOT NULL DEFAULT 0 CHECK (maintenance_generation >= 0),
    drained BOOLEAN NOT NULL DEFAULT FALSE,
    last_heartbeat_at TIMESTAMP NOT NULL DEFAULT now(),
    started_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_server_instances_last_heartbeat
    ON server_instances (last_heartbeat_at);

CREATE TRIGGER update_restore_jobs_updated_at
BEFORE UPDATE ON restore_jobs
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_system_maintenance_updated_at
BEFORE UPDATE ON system_maintenance
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- System restore inserts preserved temporal rows itself. This transaction-local
-- switch prevents the ordinary history triggers from manufacturing a second,
-- restore-time history timeline while current rows are reconstructed.
CREATE OR REPLACE FUNCTION hubuum_record_history() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  hist text := quote_ident(TG_TABLE_NAME || '_history');
  seq text := quote_literal(TG_TABLE_NAME || '_history_seq');
  ts timestamptz := clock_timestamp();
  actor int := nullif(current_setting('hubuum.actor_id', true), '')::int;
  base_cols text;
  hist_cols text;
BEGIN
  IF current_setting('hubuum.restore_history', true) = 'on' THEN
    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    END IF;
    RETURN NEW;
  END IF;

  SELECT string_agg(format('($1).%1$I', a.attname), ', ' ORDER BY a.attnum),
         string_agg(format('%1$I', a.attname), ', ' ORDER BY a.attnum)
    INTO base_cols, hist_cols
  FROM pg_attribute a
  WHERE a.attrelid = TG_RELID
    AND a.attnum > 0
    AND NOT a.attisdropped;

  IF TG_OP = 'INSERT' THEN
    EXECUTE format(
      'INSERT INTO %s (%s, op, valid_from, valid_to, actor_id, history_id)
       SELECT %s, %L, $2, NULL, $3, nextval(%s)',
      hist, hist_cols, base_cols, 'I', seq)
      USING NEW, ts, actor;
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    EXECUTE format('UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL', hist)
      USING ts, OLD.id;
    EXECUTE format(
      'INSERT INTO %s (%s, op, valid_from, valid_to, actor_id, history_id)
       SELECT %s, %L, $2, NULL, $3, nextval(%s)',
      hist, hist_cols, base_cols, 'U', seq)
      USING NEW, ts, actor;
    RETURN NEW;
  ELSE
    EXECUTE format('UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL', hist)
      USING ts, OLD.id;
    EXECUTE format(
      'INSERT INTO %s (%s, op, valid_from, valid_to, actor_id, history_id)
       SELECT %s, %L, $2, $2, $3, nextval(%s)',
      hist, hist_cols, base_cols, 'D', seq)
      USING OLD, ts, actor;
    RETURN OLD;
  END IF;
END; $$;

-- Restored audit events are already normalized as dispatched and must not wake
-- fanout workers. Ordinary event inserts retain the original notification.
CREATE OR REPLACE FUNCTION notify_events_fanout()
RETURNS TRIGGER AS $$
BEGIN
    IF current_setting('hubuum.restore_events', true) = 'on' THEN
        RETURN NEW;
    END IF;
    PERFORM pg_notify('hubuum_events_fanout', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
