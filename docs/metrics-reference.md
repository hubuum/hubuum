# Generated Metric Reference

This file is generated from the typed operational-contract registry. Do not edit it by hand.

| Metric | Type | Unit | Scope | Labels | Description |
| --- | --- | --- | --- | --- | --- |
| `hubuum_api_errors_total` | counter | none | process | `class` | API errors by public error class |
| `hubuum_build_info` | gauge | none | process | `version`, `git_sha` | Build identity for the process |
| `hubuum_client_allowlist_rejections_total` | counter | none | process | `reason` | Requests rejected by the client allowlist |
| `hubuum_computed_field_errors_total` | counter | none | process | `scope`, `code` | Computed-field runtime errors by stable code |
| `hubuum_computed_field_evaluations_total` | counter | none | process | `scope`, `outcome` | Computed-field evaluations |
| `hubuum_computed_field_live_fallbacks_total` | counter | none | process | none | Stale materializations evaluated live |
| `hubuum_computed_field_read_repairs_total` | counter | none | process | `outcome` | Guarded computed-field read repairs |
| `hubuum_computed_field_rebuild_batches_total` | counter | none | process | `items` | Computed-field rebuild batches |
| `hubuum_computed_field_rebuild_completions_total` | counter | none | process | `status` | Computed-field rebuild terminal outcomes |
| `hubuum_computed_field_rebuild_duration_seconds` | histogram | seconds | process | `status` | Computed-field rebuild duration |
| `hubuum_db_connection_acquire_duration_seconds` | histogram | seconds | process | `caller` | Database connection acquisition duration |
| `hubuum_db_connection_acquire_failures_total` | counter | none | process | `caller` | Database connection acquisition failures |
| `hubuum_db_operation_duration_seconds` | histogram | seconds | process | `caller`, `operation`, `result` | Database helper operation duration |
| `hubuum_db_operation_errors_total` | counter | none | process | `caller`, `operation`, `result` | Database helper operation failures |
| `hubuum_db_pool_connections` | gauge | none | process | `state` | Database pool connections by state |
| `hubuum_event_oldest_age_seconds` | gauge | seconds | database | `queue` | Oldest actionable event-queue item age |
| `hubuum_event_queue_items` | gauge | none | database | `queue`, `state` | Event queue items by state |
| `hubuum_event_stale_claims` | gauge | none | database | `queue` | Stale event worker claims |
| `hubuum_event_worker_batch_size` | gauge | none | process | `worker` | Configured event-worker batch size |
| `hubuum_event_worker_lock_timeout_seconds` | gauge | seconds | process | `worker` | Configured event-worker claim timeout |
| `hubuum_event_worker_poll_interval_seconds` | gauge | seconds | process | `worker` | Configured event-worker poll interval |
| `hubuum_event_worker_wakeups_total` | counter | none | process | `worker`, `kind` | Event-worker wakeups |
| `hubuum_event_workers_configured` | gauge | none | process | `worker` | Configured event workers |
| `hubuum_export_completions_total` | counter | none | process | `scope`, `content_type` | Persisted export completions |
| `hubuum_export_duration_seconds` | histogram | seconds | process | `template_id`, `outcome` | Total export duration |
| `hubuum_export_phase_duration_seconds` | histogram | seconds | process | `phase`, `outcome` | Export phase duration |
| `hubuum_export_template_info` | gauge | none | database | `template_id`, `template_name` | Current export-template identities |
| `hubuum_export_truncations_total` | counter | none | process | `scope`, `content_type` | Persisted truncated exports |
| `hubuum_export_warnings_total` | counter | none | process | `scope`, `content_type` | Warnings on persisted exports |
| `hubuum_extraction_failures_total` | counter | none | process | `kind` | Request extraction failures |
| `hubuum_http_request_duration_seconds` | histogram | seconds | process | `method`, `route`, `status_family` | HTTP request duration |
| `hubuum_http_requests_in_flight` | gauge | none | process | `route` | HTTP requests currently in flight |
| `hubuum_http_requests_total` | counter | none | process | `method`, `route`, `status_code`, `status_family` | HTTP requests handled |
| `hubuum_import_failed_items_total` | counter | none | process | none | Import items completed with failure |
| `hubuum_import_phase_duration_seconds` | histogram | seconds | process | `phase`, `outcome` | Import phase duration |
| `hubuum_import_processed_items_total` | counter | none | process | none | Items processed by terminal imports |
| `hubuum_import_succeeded_items_total` | counter | none | process | none | Import items completed successfully |
| `hubuum_inventory_entities` | gauge | none | database | `entity_type` | Current inventory entity counts |
| `hubuum_login_attempts_total` | counter | none | process | `outcome` | Login attempts |
| `hubuum_login_limiter_backend_failures_total` | counter | none | process | `backend`, `operation` | Shared login-limiter failures |
| `hubuum_login_limiter_entries` | gauge | none | process | `state` | Local login-limiter entries |
| `hubuum_login_lockouts_total` | counter | none | process | `scope` | Login limiter lockout transitions |
| `hubuum_metrics_refresh_duration_seconds` | gauge | seconds | process | `source` | Duration of the latest metrics refresh |
| `hubuum_metrics_refresh_failures_total` | counter | none | process | `source` | Best-effort metrics refresh failures |
| `hubuum_metrics_refresh_last_success_timestamp_seconds` | gauge | seconds | process | `source` | Latest successful metrics refresh timestamp |
| `hubuum_metrics_refresh_skipped_total` | counter | none | process | `source`, `reason` | Skipped concurrent metrics refreshes |
| `hubuum_process_start_time_seconds` | gauge | seconds | process | none | Time when Hubuum initialized metrics |
| `hubuum_remote_call_duration_seconds` | histogram | seconds | process | `method`, `status_family`, `outcome` | Remote HTTP execution duration |
| `hubuum_remote_call_results_total` | counter | none | process | `method`, `status_family`, `outcome` | Remote HTTP execution outcomes |
| `hubuum_revision_conditions_total` | counter | none | process | `outcome` | Conditional-write outcomes |
| `hubuum_runtime_info` | gauge | none | process | `role` | Runtime role for the process |
| `hubuum_secret_resolution_duration_seconds` | histogram | seconds | process | `provider`, `consumer`, `outcome` | Secret resolution duration |
| `hubuum_secret_resolutions_total` | counter | none | process | `provider`, `consumer`, `outcome` | Secret resolution outcomes |
| `hubuum_secret_source_info` | gauge | none | process | `provider` | Selected secret provider |
| `hubuum_storage_backend_info` | gauge | none | process | `backend` | Selected storage backend |
| `hubuum_storage_operation_duration_seconds` | histogram | seconds | process | `backend`, `capability`, `operation`, `result` | Logical storage operation duration |
| `hubuum_storage_operation_errors_total` | counter | none | process | `backend`, `capability`, `operation`, `result` | Logical storage operation failures |
| `hubuum_task_claims_total` | counter | none | process | `kind` | Tasks claimed by workers |
| `hubuum_task_completions_total` | counter | none | process | `kind`, `final_status` | Tasks reaching a terminal status |
| `hubuum_task_execution_duration_seconds` | histogram | seconds | process | `kind`, `final_status` | Task execution duration |
| `hubuum_task_last_terminal_timestamp_seconds` | gauge | seconds | database | `kind`, `status` | Most recent terminal-task timestamp |
| `hubuum_task_lease_recoveries_total` | counter | none | process | `kind` | Tasks failed after lease expiry |
| `hubuum_task_oldest_age_seconds` | gauge | seconds | database | `kind`, `state` | Oldest queued or active task age |
| `hubuum_task_output_cleanup_deleted_total` | counter | none | process | `kind` | Stored outputs deleted by cleanup |
| `hubuum_task_output_cleanup_failures_total` | counter | none | process | `kind` | Stored output cleanup failures |
| `hubuum_task_output_cleanup_runs_total` | counter | none | process | `kind` | Stored output cleanup runs |
| `hubuum_task_poll_interval_seconds` | gauge | seconds | process | none | Configured task-worker poll interval |
| `hubuum_task_queue_wait_duration_seconds` | histogram | seconds | process | `kind` | Task queue wait duration |
| `hubuum_task_worker_iterations_total` | counter | none | process | `outcome` | Task worker loop iterations |
| `hubuum_task_workers_configured` | gauge | none | process | none | Configured task workers |
| `hubuum_tasks` | gauge | none | database | `kind`, `status` | Current task counts |
| `hubuum_token_authentications_total` | counter | none | process | `format`, `key_state`, `outcome` | Bearer-token authentication outcomes |
| `hubuum_token_hash_key_info` | gauge | none | process | `mode`, `active_key_id`, `ring_identity` | Token hash key-ring identity |
| `hubuum_token_hash_keys` | gauge | none | process | `state` | Configured token hash keys |
| `hubuum_token_hash_stored` | gauge | none | database | `key_state`, `lifecycle` | Stored bearer tokens by key lifecycle |
| `process_cpu_seconds_total` | counter | seconds | process | none | Process user and system CPU time |
| `process_max_fds` | gauge | none | process | none | Process file-descriptor limit |
| `process_open_fds` | gauge | none | process | none | Open file descriptors or handles |
| `process_resident_memory_bytes` | gauge | bytes | process | none | Resident process memory |
| `process_start_time_seconds` | gauge | seconds | process | none | Operating-system process start time |
| `process_virtual_memory_bytes` | gauge | bytes | process | none | Virtual process memory |

## Histogram Buckets

- **latency:** 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30 seconds
- **outbound:** 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120 seconds
- **background:** 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600 seconds
