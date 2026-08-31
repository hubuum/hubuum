DROP FUNCTION IF EXISTS hubuum_complete_event_retention_purge(uuid);

-- Downgrades restore the previous invoker security mode. Function bodies are
-- intentionally retained because the next older migration owns their exact
-- definitions and a subsequent full downgrade will restore them.
ALTER FUNCTION hubuum_record_history() SECURITY INVOKER;
ALTER FUNCTION hubuum_record_history() RESET ALL;
ALTER FUNCTION hubuum_manage_resource_revision() RESET ALL;
ALTER FUNCTION hubuum_bump_revision_owner() SECURITY INVOKER;
ALTER FUNCTION hubuum_bump_revision_owner() RESET ALL;
ALTER FUNCTION hubuum_check_delete_revision() RESET ALL;
ALTER FUNCTION hubuum_fill_event_revisions() SECURITY INVOKER;
ALTER FUNCTION hubuum_fill_event_revisions() RESET ALL;
ALTER FUNCTION notify_events_fanout() SECURITY INVOKER;
ALTER FUNCTION notify_events_fanout() RESET ALL;
ALTER FUNCTION enforce_events_append_only() SECURITY INVOKER;
ALTER FUNCTION enforce_events_append_only() RESET ALL;

GRANT EXECUTE ON FUNCTION hubuum_record_history() TO PUBLIC;
GRANT EXECUTE ON FUNCTION hubuum_bump_revision_owner() TO PUBLIC;
GRANT EXECUTE ON FUNCTION hubuum_fill_event_revisions() TO PUBLIC;
GRANT EXECUTE ON FUNCTION notify_events_fanout() TO PUBLIC;
GRANT EXECUTE ON FUNCTION enforce_events_append_only() TO PUBLIC;
