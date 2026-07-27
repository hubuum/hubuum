# Runtime Metrics

Hubuum exposes low-cardinality runtime metrics through a Prometheus scrape
endpoint. The endpoint is enabled by default at `/metrics`.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `HUBUUM_METRICS_ENABLED` | `true` | Enables the Prometheus metrics scrape endpoint |
| `HUBUUM_METRICS_PATH` | `/metrics` | Literal absolute non-root endpoint path; it must not contain route patterns or collide with API, probe, OpenAPI, or Swagger UI routes |

The endpoint is subject to `HUBUUM_CLIENT_ALLOWLIST` and uses the configured TLS
settings. Put it behind network-level access controls appropriate for
operational data.

Processes with the `all` or `api` runtime role serve metrics on the main HTTP
listener. A `worker` process serves only the configured metrics path on
`HUBUUM_BIND_IP:HUBUUM_PORT` when metrics are enabled; it does not expose the
application API or health probes. Give API and worker containers separate
network namespaces or ports if they run on the same host.

## Scrape Each Process Directly

Counters, histograms, connection-pool gauges, and worker configuration describe
one process. Configure Prometheus with a stable target for every API and worker
process. Do not scrape a load-balanced public URL: successive scrapes can land
on different processes, producing apparent counter resets and hiding
worker-only activity.

Use these metrics to identify a target:

- `hubuum_build_info{version,git_sha}`
- `hubuum_runtime_info{role}`
- `hubuum_process_start_time_seconds`

Inventory, task backlog, export-template identity, and event-queue gauges are
database-wide snapshots. Every replica connected to the same database reports
the same logical values. Aggregate those series with `max` or `avg`, not `sum`,
unless replica multiplication is intentional. Process-local gauges such as
database-pool state should normally be summed when calculating deployment-wide
capacity.

Database-backed gauges use a 30-second in-process cache and refresh on a
best-effort basis. If a refresh fails, `/metrics` still returns process metrics
and retains the last successful database snapshot when one exists. The refresh
duration, last-success timestamp, failures, and skipped concurrent refreshes
make stale values visible.

Single-host deployments provide deterministic convenience routes.
Shared-host `direct` routing sends `/metrics` to the worker-enabled primary and
`/metrics/standby` to the HTTP-only standby. With shared-host `prefixed`
routing, use `/hubuum-api/metrics` and
`/hubuum-api/metrics/standby`. Shared-host `bff` routing does not expose either
backend metrics endpoint publicly.

## Cardinality Rules

Metric labels must stay bounded. Hubuum metrics do not use usernames, user IDs,
client IPs, raw URL paths, object IDs, class names, collection names, rendered
remote URLs, task IDs, idempotency keys, or error messages.

HTTP metrics use Actix route templates such as
`/api/v1/classes/{class_id}/objects/{object_id}`. Requests that do not resolve
to a registered route use a coarse route group instead of their raw path.

Export phase timing is aggregated by phase and outcome. Only the total export
histogram carries `template_id`; this keeps template identity useful without
multiplying every query, hydration, and render bucket by every stored template.
`hubuum_export_template_info` maps current database IDs to mutable names and is
reset on every inventory snapshot, so renames and deletions do not leave stale
series in a process.

Use admin JSON/API endpoints and task logs for per-task detail.

## Duration Units And Histograms

Prometheus duration metric names and observations use seconds. A 4.7
millisecond request is recorded as `0.0047`; using seconds does not discard
millisecond or sub-millisecond precision.

Hubuum uses three explicit bucket profiles:

- HTTP and database latency: `0.0005` seconds through `30` seconds.
- Outbound remote calls: `0.01` seconds through `120` seconds.
- Queued and background work: `0.01` seconds through `3600` seconds.

The long background buckets are intentional: most buckets should be empty for
fast work, while imports, exports, backups, remote dependencies, or a
constrained worker can legitimately take seconds or minutes. Empty upper
buckets are cheap and preserve visibility when an exceptional slow operation
occurs.

Prometheus histogram buckets are cumulative. This query calculates HTTP p95
latency by route over five minutes:

```promql
histogram_quantile(
  0.95,
  sum by (le, route) (
    rate(hubuum_http_request_duration_seconds_bucket[5m])
  )
)
```

The histogram sum divided by its count gives the mean. Apply `rate` before
division for a time window:

```promql
sum by (route) (
  rate(hubuum_http_request_duration_seconds_sum[5m])
)
/
sum by (route) (
  rate(hubuum_http_request_duration_seconds_count[5m])
)
```

The exported `_sum` is therefore useful, but only together with `_count`.
Classic Prometheus histograms do not expose exact minimum or maximum
observations. `histogram_quantile` estimates percentiles from bucket boundaries
and remains aggregatable across processes.

Counter and histogram series appear only after a process observes a matching
event. Seeing only `/readyz` means that target handled readiness probes but not
application requests; it does not mean other routes are filtered out.

For stored-template exports, join the total-duration histogram to the current
template-info gauge:

```promql
histogram_quantile(
  0.95,
  sum by (le, template_id) (
    rate(
      hubuum_export_duration_seconds_bucket{
        template_id!="none"
      }[15m]
    )
  )
)
* on (template_id) group_left (template_name)
max by (template_id, template_name) (
  hubuum_export_template_info
)
```

## Metrics

### Process, HTTP, And Database

| Metric | Labels | Description |
| --- | --- | --- |
| `hubuum_build_info` | `version`, `git_sha` | Build identity for the scraped process |
| `hubuum_runtime_info` | `role` | Runtime role for the scraped process |
| `hubuum_process_start_time_seconds` | none | Unix timestamp when the process initialized metrics |
| `hubuum_http_requests_total` | `method`, `route`, `status_code`, `status_family` | HTTP requests by stable route template or coarse route group |
| `hubuum_http_request_duration_seconds` | `method`, `route`, `status_family` | HTTP request duration histogram |
| `hubuum_http_requests_in_flight` | `route` | Requests currently being handled by stable route |
| `hubuum_api_errors_total` | `class` | API errors by public error class |
| `hubuum_extraction_failures_total` | `kind` | JSON and path extraction failures |
| `hubuum_db_pool_connections` | `state` | Database pool connections by configured, open, idle, and checked-out state |
| `hubuum_db_connection_acquire_duration_seconds` | `caller` | Pool connection acquisition duration |
| `hubuum_db_connection_acquire_failures_total` | `caller` | Pool connection acquisition failures |
| `hubuum_db_operation_duration_seconds` | `caller`, `operation`, `result` | `with_connection` and `with_transaction` helper duration |
| `hubuum_db_operation_errors_total` | `caller`, `operation`, `result` | Database helper failures by broad public error class |
| `hubuum_metrics_refresh_duration_seconds` | `source` | Duration of the latest refresh attempt |
| `hubuum_metrics_refresh_last_success_timestamp_seconds` | `source` | Unix timestamp of the latest successful refresh |
| `hubuum_metrics_refresh_failures_total` | `source` | Best-effort refresh failures |
| `hubuum_metrics_refresh_skipped_total` | `source`, `reason` | Refreshes skipped because another scrape was already refreshing |

The bounded database `caller` values are `event_delivery`, `event_fanout`,
`event_retention`, `http_request`, `metrics_refresh`, `readiness`,
`request_maintenance`, `restore_coordinator`, `task_lease`, `task_worker`,
`token_retention`, and `unattributed`.

### Tasks, Exports, Imports, And Remote Calls

| Metric | Labels | Description |
| --- | --- | --- |
| `hubuum_task_worker_iterations_total` | `outcome` | Worker iterations by claimed, idle, or error outcome |
| `hubuum_task_claims_total` | `kind` | Tasks claimed by workers |
| `hubuum_task_lease_recoveries_total` | `kind` | Tasks failed after their owning worker lease expired |
| `hubuum_task_completions_total` | `kind`, `final_status` | Tasks reaching a terminal status |
| `hubuum_task_queue_wait_duration_seconds` | `kind` | Time from task creation to claim |
| `hubuum_task_execution_duration_seconds` | `kind`, `final_status` | Time from task start to finish |
| `hubuum_task_workers_configured` | none | Task workers configured in this process; zero on API-only processes |
| `hubuum_task_poll_interval_seconds` | none | Configured task-worker poll interval |
| `hubuum_tasks` | `kind`, `status` | Current database-wide task counts |
| `hubuum_task_oldest_age_seconds` | `kind`, `state` | Oldest queued and active task age per task kind |
| `hubuum_task_output_cleanup_runs_total` | `kind` | Stored output cleanup runs for export or backup artifacts |
| `hubuum_task_output_cleanup_failures_total` | `kind` | Stored output cleanup failures |
| `hubuum_task_output_cleanup_deleted_total` | `kind` | Stored outputs deleted by cleanup |
| `hubuum_export_template_info` | `template_id`, `template_name` | Current stored export-template identities from the shared database |
| `hubuum_export_phase_duration_seconds` | `phase`, `outcome` | Aggregate export query, hydration, render, and total phase duration |
| `hubuum_export_duration_seconds` | `template_id`, `outcome` | Total export duration by stored template ID; ad-hoc exports use `none` |
| `hubuum_export_completions_total` | `scope`, `content_type` | Successfully persisted export outputs |
| `hubuum_export_truncations_total` | `scope`, `content_type` | Successfully persisted truncated exports |
| `hubuum_export_warnings_total` | `scope`, `content_type` | Warning count on successfully persisted exports |
| `hubuum_import_phase_duration_seconds` | `phase`, `outcome` | Import planning, execution, and total phase duration, including failures |
| `hubuum_import_processed_items_total` | none | Items processed by terminal import tasks |
| `hubuum_import_succeeded_items_total` | none | Import items completed successfully |
| `hubuum_import_failed_items_total` | none | Import items completed with failure |
| `hubuum_remote_call_duration_seconds` | `method`, `status_family`, `outcome` | Remote HTTP execution duration |
| `hubuum_remote_call_results_total` | `method`, `status_family`, `outcome` | Remote outcomes such as success, failure, timeout, or validation rejection |

Export timer phases are limited to `total`, `query`, `hydration`, and `render`;
their outcomes are `success`, `error`, or `timeout`. Import timer phases are
limited to `total`, `planning`, and `execution`; their outcomes are `success`,
`failed`, `partially_succeeded`, or `error`.

### Computed Fields, Security, Events, And Inventory

| Metric | Labels | Description |
| --- | --- | --- |
| `hubuum_computed_field_evaluations_total` | `scope`, `outcome` | Computed-field evaluations by shared, personal, or preview scope and outcome |
| `hubuum_computed_field_errors_total` | `scope`, `code` | Computed-field runtime errors by stable bounded code |
| `hubuum_computed_field_live_fallbacks_total` | none | Stale shared materializations evaluated live during reads |
| `hubuum_computed_field_read_repairs_total` | `outcome` | Guarded stale-materialization repairs by success or failure |
| `hubuum_computed_field_rebuild_batches_total` | `items` | Computed-field rebuild batches classified as empty or non-empty |
| `hubuum_computed_field_rebuild_completions_total` | `status` | Computed-field rebuild terminal outcomes |
| `hubuum_computed_field_rebuild_duration_seconds` | `status` | Computed-field rebuild duration histogram |
| `hubuum_login_attempts_total` | `outcome` | Login attempts by success, bad credentials, rate-limited, or internal error |
| `hubuum_login_lockouts_total` | `scope` | Login limiter lockout transitions by principal/IP, IP, or subnet scope |
| `hubuum_login_limiter_backend_failures_total` | `backend`, `operation` | Shared login-limiter failures while local enforcement remains active |
| `hubuum_login_limiter_entries` | `state` | Active and locked login-limiter entries in this process |
| `hubuum_client_allowlist_rejections_total` | `reason` | Requests rejected for a disallowed or missing client IP |
| `hubuum_event_queue_items` | `queue`, `state` | Database-wide fan-out and delivery queue items by bounded state |
| `hubuum_event_stale_claims` | `queue` | Stale fan-out and delivery worker claims |
| `hubuum_event_oldest_age_seconds` | `queue` | Oldest actionable fan-out or delivery item age |
| `hubuum_event_workers_configured` | `worker` | Event workers configured in this process |
| `hubuum_event_worker_batch_size` | `worker` | Configured event-worker batch size |
| `hubuum_event_worker_poll_interval_seconds` | `worker` | Configured event-worker poll interval |
| `hubuum_event_worker_lock_timeout_seconds` | `worker` | Configured event-worker claim lock timeout |
| `hubuum_event_worker_wakeups_total` | `worker`, `kind` | Notification, poll, and notification-send wakeups observed by this process |
| `hubuum_inventory_entities` | `entity_type` | Database-wide collections, classes, objects, users, groups, service accounts, and remote targets |

## Alert Starting Points

These thresholds are deployment starting points, not universal defaults:

| Signal | Suggested alert |
| --- | --- |
| Missing target | `up == 0`, grouped by expected API and worker target |
| Counter reset | Unexpected `resets(hubuum_http_requests_total[15m])`, correlated with process start time |
| Stale snapshots | Current time minus `hubuum_metrics_refresh_last_success_timestamp_seconds` exceeds the cache and scrape tolerance |
| DB acquisition failures | Any sustained non-zero `hubuum_db_connection_acquire_failures_total` rate |
| DB pool pressure | Checked-out divided by configured connections above `0.8` for several minutes |
| HTTP 5xx rate | `5xx` status family above the normal route-specific baseline |
| Task queue age | Oldest queued task age above the expected latency for that task kind |
| Task worker errors | Sustained non-zero worker iteration `outcome="error"` rate |
| Task lease recovery | Any unexpected `hubuum_task_lease_recoveries_total` increase |
| Export or import failures | Failure or timeout outcomes above the task-kind baseline |
| Login lockouts | Sudden increase in lockouts or sustained locked entries |
| Shared limiter degradation | Sustained non-zero login-limiter backend failure rate |
| Remote call failures | Failure or timeout rate above the remote-call baseline |
| Event backlog | Oldest fan-out or delivery age above the processing objective |
