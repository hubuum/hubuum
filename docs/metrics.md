# Runtime Metrics

Hubuum exposes low-cardinality runtime metrics through OpenTelemetry instruments and a Prometheus scrape endpoint. The endpoint is enabled by default at `/metrics`.

## Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_METRICS_ENABLED` | `true` | Enables the Prometheus metrics scrape endpoint |
| `HUBUUM_METRICS_PATH` | `/metrics` | Absolute non-root endpoint path. It must not collide with API, health, readiness, or OpenAPI routes |

Metrics are ephemeral operational signals for alerting, dashboards, and capacity planning. Durable audit and business facts stay in the event stream.

Database-backed gauges are refreshed on a short in-process cache and are best-effort. If a refresh fails, `/metrics` still returns the runtime metrics it has and keeps the last successful database gauge values when available.

## Cardinality Rules

Metric labels must stay bounded. Hubuum metrics do not use usernames, user IDs, client IPs, raw URL paths, object IDs, class names, namespace names, rendered remote URLs, template names, idempotency keys, or error messages.

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
| `hubuum_metrics_refresh_failures_total` | `source` | Best-effort scrape refresh failures by inventory or tasks source |
| `hubuum_task_worker_iterations_total` | `outcome` | Worker iterations by claimed, idle, or error |
| `hubuum_task_claims_total` | `kind` | Tasks claimed by workers |
| `hubuum_task_completions_total` | `kind`, `final_status` | Tasks reaching a terminal status |
| `hubuum_task_queue_wait_duration_seconds` | `kind` | Time from task creation to claim |
| `hubuum_task_execution_duration_seconds` | `kind`, `final_status` | Time from task start to finish |
| `hubuum_task_worker_config` | `setting` | Configured task worker count and poll interval |
| `hubuum_tasks` | `kind`, `status` | Current tasks by bounded kind and status |
| `hubuum_task_oldest_age_seconds` | `state` | Oldest queued and active task age |
| `hubuum_report_output_cleanup_runs_total` | none | Report output cleanup runs |
| `hubuum_report_output_cleanup_failures_total` | none | Report output cleanup failures |
| `hubuum_report_output_cleanup_deleted_total` | none | Report outputs deleted by cleanup |
| `hubuum_report_phase_duration_seconds` | `phase` | Report query, hydration, render, and total phase duration |
| `hubuum_report_results_total` | `scope`, `content_type`, `outcome` | Report success, truncation, and warning counters |
| `hubuum_remote_call_duration_seconds` | `method`, `status_family`, `outcome` | Remote call duration |
| `hubuum_remote_call_results_total` | `method`, `status_family`, `outcome` | Remote call result counters |
| `hubuum_login_attempts_total` | `outcome` | Login attempts by success, bad credentials, rate-limited, or internal error |
| `hubuum_login_limiter_entries` | `state` | Active and locked login limiter entries |
| `hubuum_inventory_entities` | `entity_type` | Total namespaces, classes, objects, users, groups, service accounts, and remote targets |

## Alert Starting Points

These thresholds are deployment starting points, not universal defaults:

| Signal | Suggested alert |
| ------ | --------------- |
| DB acquisition failures | Any sustained non-zero `hubuum_db_connection_acquire_failures_total` rate |
| DB pool pressure | `checked_out / configured` above 0.8 for several minutes |
| HTTP 5xx rate | `5xx` status family above normal baseline |
| Task queue age | Oldest queued task age above expected worker latency |
| Task worker errors | Sustained non-zero `outcome="error"` worker iteration rate |
| Login lockouts | Sudden increase in `hubuum_login_limiter_entries{state="locked"}` |
| Remote call failures | Failure or timeout rate above target-specific baseline |
