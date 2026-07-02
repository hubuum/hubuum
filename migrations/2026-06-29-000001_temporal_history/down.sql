DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'hubuumclass','hubuumobject','namespaces','hubuumclass_relation',
    'hubuumobject_relation','report_templates','remote_targets'
  ]
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %1$I_skip_unchanged_temporal_update_trg ON %1$I', t);
    EXECUTE format('DROP TRIGGER IF EXISTS %1$I_history_trg ON %1$I', t);
    EXECUTE format('DROP TABLE IF EXISTS %1$I_history', t);  -- drops OWNED sequence too
  END LOOP;
END $$;
DROP FUNCTION IF EXISTS hubuum_skip_unchanged_temporal_update();
DROP FUNCTION IF EXISTS hubuum_record_history();
