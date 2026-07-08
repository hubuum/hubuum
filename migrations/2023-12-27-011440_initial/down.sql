-- Drop everything created by up.sql, in reverse dependency order. CASCADE on
-- tables removes their triggers/constraints; functions are dropped explicitly.

-- Tables (reverse dependency order)
DROP TABLE IF EXISTS remote_targets_history CASCADE;
DROP TABLE IF EXISTS report_templates_history CASCADE;
DROP TABLE IF EXISTS hubuumobject_relation_history CASCADE;
DROP TABLE IF EXISTS hubuumclass_relation_history CASCADE;
DROP TABLE IF EXISTS collections_history CASCADE;
DROP TABLE IF EXISTS hubuumobject_history CASCADE;
DROP TABLE IF EXISTS hubuumclass_history CASCADE;
DROP TABLE IF EXISTS remote_call_results CASCADE;
DROP TABLE IF EXISTS report_task_outputs CASCADE;
DROP TABLE IF EXISTS import_task_results CASCADE;
DROP TABLE IF EXISTS event_deliveries CASCADE;
DROP TABLE IF EXISTS event_subscriptions CASCADE;
DROP TABLE IF EXISTS event_sinks CASCADE;
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS tasks CASCADE;
DROP TABLE IF EXISTS token_scopes CASCADE;
DROP TABLE IF EXISTS tokens CASCADE;
DROP TABLE IF EXISTS remote_targets CASCADE;
DROP TABLE IF EXISTS report_templates CASCADE;
DROP TABLE IF EXISTS hubuumobject_relation CASCADE;
DROP TABLE IF EXISTS hubuumclass_reachability CASCADE;
DROP TABLE IF EXISTS hubuumclass_relation CASCADE;
DROP TABLE IF EXISTS hubuumobject CASCADE;
DROP TABLE IF EXISTS hubuumclass CASCADE;
DROP TABLE IF EXISTS permissions CASCADE;
DROP TABLE IF EXISTS group_memberships CASCADE;
DROP TABLE IF EXISTS service_accounts CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS collection_closure CASCADE;
DROP TABLE IF EXISTS collections CASCADE;
DROP TABLE IF EXISTS groups CASCADE;
DROP TABLE IF EXISTS principals CASCADE;

-- Functions
DROP FUNCTION IF EXISTS get_bidirectionally_related_objects(INT, INT[], INT);
DROP FUNCTION IF EXISTS get_bidirectionally_related_classes(INT, INT[], INT, TEXT, INT[], BOOLEAN, TEXT, INT[], BOOLEAN);
DROP FUNCTION IF EXISTS get_transitively_linked_objects(INT, INT, INT[], INT);
DROP FUNCTION IF EXISTS hubuum_skip_unchanged_temporal_update();
DROP FUNCTION IF EXISTS hubuum_record_history();
DROP FUNCTION IF EXISTS notify_events_fanout();
DROP FUNCTION IF EXISTS enforce_events_append_only();
DROP FUNCTION IF EXISTS protect_root_collection();
DROP FUNCTION IF EXISTS refresh_class_reachability_cache();
DROP FUNCTION IF EXISTS rebuild_class_reachability_cache();
DROP FUNCTION IF EXISTS reverse_integer_array(INT[]);
DROP FUNCTION IF EXISTS cleanup_invalid_object_relations();
DROP FUNCTION IF EXISTS validate_object_relation();
DROP FUNCTION IF EXISTS enforce_object_relation_order();
DROP FUNCTION IF EXISTS enforce_class_relation_order();
DROP FUNCTION IF EXISTS jsonb_has_key(jsonb, text);
DROP FUNCTION IF EXISTS jsonb_contains_all(jsonb, text[]);
DROP FUNCTION IF EXISTS jsonb_contains_any(jsonb, text[]);
DROP FUNCTION IF EXISTS try_timestamp(TEXT);
DROP FUNCTION IF EXISTS try_boolean(TEXT);
DROP FUNCTION IF EXISTS try_numeric(TEXT);
DROP FUNCTION IF EXISTS try_inet(TEXT);
DROP FUNCTION IF EXISTS remote_target_subject_types_valid(JSONB);
DROP FUNCTION IF EXISTS update_modified_column();
