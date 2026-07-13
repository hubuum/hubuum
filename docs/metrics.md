# Runtime Metrics

Hubuum exposes low-cardinality runtime metrics through OpenTelemetry instruments and a Prometheus scrape endpoint. The endpoint is enabled by default at `/metrics`.

## Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_METRICS_ENABLED` | `true` | Enables the Prometheus metrics scrape endpoint |
| `HUBUUM_METRICS_PATH` | `/metrics` | Literal absolute non-root endpoint path. It must not contain route patterns or collide with API, probe, OpenAPI, or Swagger UI routes |

Metrics are ephemeral operational signals for alerting, dashboards, and capacity planning. Durable audit and business facts stay in the event stream.

Database-backed gauges are refreshed on a short in-process cache and are best-effort. If a refresh fails, `/metrics` still returns the runtime metrics it has and keeps the last successful database gauge values when available. Concurrent scrapes do not start duplicate database refreshes.

The metrics endpoint uses the main HTTP listener and is subject to `HUBUUM_CLIENT_ALLOWLIST`. Put it behind network-level access controls appropriate for operational data.

## Cardinality Rules

Metric labels must stay bounded. Hubuum metrics do not use usernames, user IDs, client IPs, raw URL paths, object IDs, class names, collection names, rendered remote URLs, template names, idempotency keys, or error messages.

Use admin JSON/API endpoints for detailed high-cardinality views.

## Metrics

| Metric | Labels | Description |
| ------ | ------ | ----------- |
| `hubuum_http_requests_total` | `method`, `route`, `status_code`, `status_family` | HTTP requests by stable route template or coarse route group |
| `hubuum_http_request_duration_seconds` | `method`, `route`, `status_family` | HTTP request duration histogram |
| `hubuum_http_requests_in_flight` | none | Requests currently being handled |
| `hubuum_api_errors_total` | `class` | API errors by public error class |
| `hubuum_extraction_failures_total` | `kind` | JSON and path extraction failures |
| `hubuum_db_pool_connections` | `state` | Database pool connections by configured, open, idle, and checked-out state |
| `hubuum_db_connection_acquire_duration_seconds` | none | Pool connection acquisition duration |
| `hubuum_db_connection_acquire_failures_total` | none | Pool connection acquisition failures |
| `hubuum_db_operation_duration_seconds` | `operation`, `result` | `with_connection` and `with_transaction` helper duration |
| `hubuum_db_operation_errors_total` | `operation`, `result` | Database helper failures by broad public error class |
| `hubuum_metrics_refresh_failures_total` | `source` | Best-effort scrape refresh failures by inventory, tasks, or events source |
| `hubuum_task_worker_iterations_total` | `outcome` | Worker iterations by claimed, idle, or error |
| `hubuum_task_claims_total` | `kind` | Tasks claimed by workers |
| `hubuum_task_lease_recoveries_total` | `kind` | Tasks failed after their owning worker lease expired |
| `hubuum_task_completions_total` | `kind`, `final_status` | Tasks reaching a terminal status |
| `hubuum_task_queue_wait_duration_seconds` | `kind` | Time from task creation to claim |
| `hubuum_task_execution_duration_seconds` | `kind`, `final_status` | Time from task start to finish |
| `hubuum_task_worker_config` | `setting` | Configured task worker count and poll interval |
| `hubuum_tasks` | `kind`, `status` | Current tasks by bounded kind and status |
| `hubuum_task_oldest_age_seconds` | `state` | Oldest queued and active task age |
| `hubuum_export_output_cleanup_runs_total` | none | Export output cleanup runs |
| `hubuum_export_output_cleanup_failures_total` | none | Export output cleanup failures |
| `hubuum_export_output_cleanup_deleted_total` | none | Export outputs deleted by cleanup |
| `hubuum_export_phase_duration_seconds` | `phase` | Export query, hydration, render, and total phase duration |
| `hubuum_export_completions_total` | `scope`, `content_type` | Successfully persisted export outputs |
| `hubuum_export_truncations_total` | `scope`, `content_type` | Successfully persisted truncated exports |
| `hubuum_export_warnings_total` | `scope`, `content_type` | Warning count on successfully persisted exports |
| `hubuum_import_phase_duration_seconds` | `phase` | Import planning, execution, and total phase duration |
| `hubuum_import_processed_items_total` | none | Items processed by terminal import tasks |
| `hubuum_import_succeeded_items_total` | none | Import items completed successfully |
| `hubuum_import_failed_items_total` | none | Import items completed with failure |
| `hubuum_remote_call_duration_seconds` | `method`, `status_family`, `outcome` | Remote call duration |
| `hubuum_remote_call_results_total` | `method`, `status_family`, `outcome` | Remote call outcomes: success, failure, timeout, validation rejection, or private-target rejection |
| `hubuum_login_attempts_total` | `outcome` | Login attempts by success, bad credentials, rate-limited, or internal error |
| `hubuum_login_lockouts_total` | `scope` | Login limiter lockout transitions by principal/IP, IP, or subnet scope |
| `hubuum_login_limiter_backend_failures_total` | `backend`, `operation` | Shared login limiter failures while local enforcement remains active |
| `hubuum_login_limiter_entries` | `state` | Active and locked login limiter entries |
| `hubuum_client_allowlist_rejections_total` | `reason` | Requests rejected for a disallowed or missing client IP |
| `hubuum_event_queue_items` | `queue`, `state` | Fan-out and delivery queue items by bounded state |
| `hubuum_event_stale_claims` | `queue` | Stale fan-out and delivery worker claims |
| `hubuum_event_oldest_age_seconds` | `queue` | Oldest actionable fan-out or delivery item age |
| `hubuum_event_worker_config` | `worker`, `setting` | Fan-out and delivery worker configuration |
| `hubuum_event_worker_wakeups` | `worker`, `kind` | Notification and polling wakeup counters reported as gauges |
| `hubuum_inventory_entities` | `entity_type` | Total collections, classes, objects, users, groups, service accounts, and remote targets |

## Alert Starting Points

These thresholds are deployment starting points, not universal defaults:

| Signal | Suggested alert |
| ------ | --------------- |
| DB acquisition failures | Any sustained non-zero `hubuum_db_connection_acquire_failures_total` rate |
| DB pool pressure | `checked_out / configured` above 0.8 for several minutes |
| HTTP 5xx rate | `5xx` status family above normal baseline |
| Task queue age | Oldest queued task age above expected worker latency |
| Task worker errors | Sustained non-zero `outcome="error"` worker iteration rate |
| Task lease recovery | Any unexpected `hubuum_task_lease_recoveries_total` increase |
| Login lockouts | Sudden increase in `hubuum_login_lockouts_total` or sustained locked entries |
| Shared limiter degradation | Any sustained non-zero `hubuum_login_limiter_backend_failures_total` rate |
| Remote call failures | Failure or timeout rate above target-specific baseline |
| Event backlog | Oldest fan-out or delivery age above the configured processing objective |
