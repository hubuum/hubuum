ALTER TABLE tasks
    ADD COLUMN initiator_user_id INTEGER NULL;

UPDATE tasks
SET initiator_user_id = submitted_by
WHERE submitted_by IS NOT NULL;

ALTER TABLE events
    ADD COLUMN initiator_user_id INTEGER NULL,
    ADD COLUMN task_id INTEGER NULL;

CREATE INDEX events_initiator_user_id_idx
    ON events (initiator_user_id, id)
    WHERE initiator_user_id IS NOT NULL;

CREATE INDEX events_task_queued_initiator_fallback_idx
    ON events (entity_id, actor_user_id)
    WHERE entity_type = 'task' AND action = 'queued';

DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'hubuumclass_history',
        'hubuumobject_history',
        'collections_history',
        'hubuumclass_relation_history',
        'hubuumobject_relation_history',
        'export_templates_history',
        'remote_targets_history'
    ]
    LOOP
        EXECUTE format(
            'ALTER TABLE %I
                ADD COLUMN actor_kind TEXT NULL,
                ADD COLUMN initiator_user_id INTEGER NULL,
                ADD COLUMN task_id INTEGER NULL',
            table_name
        );
        EXECUTE format(
            'UPDATE %I SET actor_kind = ''user'' WHERE actor_id IS NOT NULL',
            table_name
        );
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION hubuum_record_history() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  hist text := quote_ident(TG_TABLE_NAME || '_history');
  seq text := quote_literal(TG_TABLE_NAME || '_history_seq');
  ts timestamptz := clock_timestamp();
  actor int := nullif(current_setting('hubuum.actor_id', true), '')::int;
  actor_kind_value text := nullif(current_setting('hubuum.actor_kind', true), '');
  initiator int := nullif(current_setting('hubuum.initiator_user_id', true), '')::int;
  provenance_task_id int := nullif(current_setting('hubuum.task_id', true), '')::int;
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
      'INSERT INTO %s
         (%s, op, valid_from, valid_to, actor_id, history_id,
          actor_kind, initiator_user_id, task_id)
       SELECT %s, %L, $2, NULL, $3, nextval(%s), $4, $5, $6',
      hist, hist_cols, base_cols, 'I', seq)
      USING NEW, ts, actor, actor_kind_value, initiator, provenance_task_id;
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    EXECUTE format('UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL', hist)
      USING ts, OLD.id;
    EXECUTE format(
      'INSERT INTO %s
         (%s, op, valid_from, valid_to, actor_id, history_id,
          actor_kind, initiator_user_id, task_id)
       SELECT %s, %L, $2, NULL, $3, nextval(%s), $4, $5, $6',
      hist, hist_cols, base_cols, 'U', seq)
      USING NEW, ts, actor, actor_kind_value, initiator, provenance_task_id;
    RETURN NEW;
  ELSE
    EXECUTE format('UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL', hist)
      USING ts, OLD.id;
    EXECUTE format(
      'INSERT INTO %s
         (%s, op, valid_from, valid_to, actor_id, history_id,
          actor_kind, initiator_user_id, task_id)
       SELECT %s, %L, $2, $2, $3, nextval(%s), $4, $5, $6',
      hist, hist_cols, base_cols, 'D', seq)
      USING OLD, ts, actor, actor_kind_value, initiator, provenance_task_id;
    RETURN OLD;
  END IF;
END; $$;

CREATE OR REPLACE FUNCTION enforce_events_append_only()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF current_setting('events.allow_purge', true) IS DISTINCT FROM 'on' THEN
            RAISE EXCEPTION 'events table is append-only: DELETE is not permitted';
        END IF;
        RETURN OLD;
    END IF;

    IF NEW.id IS DISTINCT FROM OLD.id
       OR NEW.event_id IS DISTINCT FROM OLD.event_id
       OR NEW.occurred_at IS DISTINCT FROM OLD.occurred_at
       OR NEW.entity_type IS DISTINCT FROM OLD.entity_type
       OR NEW.entity_id IS DISTINCT FROM OLD.entity_id
       OR NEW.entity_name IS DISTINCT FROM OLD.entity_name
       OR NEW.collection_id IS DISTINCT FROM OLD.collection_id
       OR NEW.action IS DISTINCT FROM OLD.action
       OR NEW.actor_user_id IS DISTINCT FROM OLD.actor_user_id
       OR NEW.actor_kind IS DISTINCT FROM OLD.actor_kind
       OR NEW.initiator_user_id IS DISTINCT FROM OLD.initiator_user_id
       OR NEW.task_id IS DISTINCT FROM OLD.task_id
       OR NEW.request_id IS DISTINCT FROM OLD.request_id
       OR NEW.correlation_id IS DISTINCT FROM OLD.correlation_id
       OR NEW.summary IS DISTINCT FROM OLD.summary
       OR NEW.before IS DISTINCT FROM OLD.before
       OR NEW.after IS DISTINCT FROM OLD.after
       OR NEW.metadata IS DISTINCT FROM OLD.metadata
       OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
    THEN
        RAISE EXCEPTION 'events table is append-only: only fan-out claim fields and dispatched_at may be updated';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
