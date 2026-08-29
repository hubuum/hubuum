DROP TABLE IF EXISTS server_instances;
DROP TABLE IF EXISTS system_maintenance;
DROP TABLE IF EXISTS restore_jobs;
DELETE FROM tasks WHERE kind = 'backup';
DROP TABLE IF EXISTS backup_task_outputs;

CREATE OR REPLACE FUNCTION notify_events_fanout()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('hubuum_events_fanout', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check;
ALTER TABLE tasks
    ADD CONSTRAINT tasks_kind_check
    CHECK (kind IN ('import', 'export', 'reindex', 'remote_call'));

CREATE OR REPLACE FUNCTION hubuum_record_history() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  hist text := quote_ident(TG_TABLE_NAME || '_history');
  seq text := quote_literal(TG_TABLE_NAME || '_history_seq');
  ts timestamptz := clock_timestamp();
  actor int := nullif(current_setting('hubuum.actor_id', true), '')::int;
  base_cols text;
  hist_cols text;
BEGIN
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
