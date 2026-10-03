"""Build the shared operator package. Use --check to detect generated drift."""

import argparse
import json
import sys

from . import ROOT

ASSETS = ROOT / "observability"
RUNBOOK = "https://github.com/hubuum/hubuum/blob/main/observability/runbooks/"
SCOPE = 'deployment=~"$deployment"'
API = 'route=~"/api/.*",status_family=~"2xx|3xx|5xx"'


def record(name, expression):
    return {"record": name, "expr": expression}


def recordings():
    rules = []
    for window in ("5m", "30m", "1h", "6h", "30d"):
        total = f'sum by (deployment) (rate(hubuum_http_requests_total{{{API}}}[{window}]))'
        errors = f'sum by (deployment) (rate(hubuum_http_requests_total{{route=~"/api/.*",status_family="5xx"}}[{window}]))'
        # A zero-valued matching denominator fills absent error counters, but
        # leaves idle/missing deployments without a success ratio (0 / 0).
        rules += [
            record(f"hubuum:api_requests:rate{window}", total),
            record(f"hubuum:api_error_ratio:rate{window}", f"({errors} or ({total} * 0)) / ({total})"),
            record(f"hubuum:api_error_budget_burn:rate{window}", f"hubuum:api_error_ratio:rate{window} / 0.001"),
        ]
        successful = f'sum by (deployment) (rate(hubuum_http_request_duration_seconds_count{{route=~"/api/.*",status_family=~"2xx|3xx"}}[{window}]))'
        fast = f'sum by (deployment) (rate(hubuum_http_request_duration_seconds_bucket{{route=~"/api/.*",status_family=~"2xx|3xx",le="1"}}[{window}]))'
        rules += [record(f"hubuum:api_latency_bad_ratio:rate{window}", f"1 - ({fast}) / ({successful})"),
                  record(f"hubuum:api_latency_budget_burn:rate{window}", f"hubuum:api_latency_bad_ratio:rate{window} / 0.01")]
    for quantile in (.5, .95, .99):
        rules.append(record(
            f"hubuum:api_route_latency_p{int(quantile * 100)}:5m",
            f'histogram_quantile({quantile}, sum by (deployment, route, method, le) (rate(hubuum_http_request_duration_seconds_bucket{{route=~"/api/.*",status_family=~"2xx|3xx"}}[5m])))',
        ))
    rules += [
        record("hubuum:api_latency_p95:5m", 'histogram_quantile(0.95, sum by (deployment, le) (rate(hubuum_http_request_duration_seconds_bucket{route=~"/api/.*",status_family=~"2xx|3xx"}[5m])))'),
        record("hubuum:pool_utilization:ratio", 'max by (deployment, instance) (hubuum_db_pool_connections{state="checked_out"}) / max by (deployment, instance) (hubuum_db_pool_connections{state="configured"})'),
        record("hubuum:task_queue_age:seconds", 'max by (deployment, kind) (hubuum_task_oldest_age_seconds{state="queued"})'),
        record("hubuum:event_queue_age:seconds", 'max by (deployment, queue) (hubuum_event_oldest_age_seconds)'),
        record("hubuum:event_queue:items", 'max by (deployment, queue, state) (hubuum_event_queue_items)'),
        record("hubuum:api_replicas:available", 'sum by (deployment) ((up{job="hubuum"} == 1) * on (deployment, instance) group_left() (hubuum_runtime_info{role=~"all|api"} == 1))'),
        record("hubuum:worker_processes:available", 'sum by (deployment) ((up{job="hubuum"} == 1) * on (deployment, instance) group_left() (hubuum_runtime_info{role=~"all|worker"} == 1))'),
        record("hubuum:task_failure:recent", 'time() - max by (deployment, kind) (hubuum_task_last_terminal_timestamp_seconds{status="failed"}) < bool 3600'),
        record("hubuum:event_dead:increase1h", 'clamp_min(delta(hubuum:event_queue:items{queue="delivery",state="dead"}[1h]), 0)'),
        record("hubuum:scrape_age:seconds", 'time() - timestamp(up{job="hubuum"})'),
    ]
    return {"groups": [{"name": "hubuum.recording", "interval": "30s", "rules": rules}]}


def alert(name, expr, summary, book, component, duration="5m", severity="warning"):
    return {"alert": name, "expr": expr, "for": duration,
            "labels": {"severity": severity, "component": component},
            "annotations": {"summary": summary, "runbook_url": RUNBOOK + book + ".md"}}


def extended_alerts():
    return [
        alert("HubuumScrapeUnavailable", 'up{job="hubuum"} == 0', "A configured Hubuum process cannot be scraped", "availability", "process"),
        alert("HubuumNoReadyApi", 'max by (deployment) (probe_success{job="hubuum-readiness"}) == 0', "No probed API replica is ready", "availability", "api", "2m", "critical"),
        alert("HubuumApiAvailabilityBurn", 'hubuum:api_error_budget_burn:rate1h > 14.4 and hubuum:api_error_budget_burn:rate5m > 14.4', "API availability is consuming the monthly budget rapidly", "api-slo", "api", "2m", "critical"),
        alert("HubuumApiAvailabilitySlowBurn", 'hubuum:api_error_budget_burn:rate6h > 6 and hubuum:api_error_budget_burn:rate30m > 6', "API availability is consuming the monthly budget", "api-slo", "api", "5m"),
        alert("HubuumApiLatencyBurn", 'hubuum:api_latency_budget_burn:rate1h > 14.4 and hubuum:api_latency_budget_burn:rate5m > 14.4', "Successful API requests are exceeding the latency objective", "api-slo", "api"),
        alert("HubuumDatabaseAcquireFailures", 'sum by (deployment, instance) (increase(hubuum_db_connection_acquire_failures_total[10m])) > 0', "Database connection acquisition is failing", "database-pool", "database"),
        alert("HubuumNoTaskWorkers", 'max by (deployment) (hubuum_tasks{status="queued"}) > 0 unless on (deployment) (sum by (deployment) (hubuum_task_workers_configured * on (deployment, instance) group_left() (up{job="hubuum"} == 1)) > 0)', "Queued tasks have no reachable configured workers", "worker-errors", "tasks"),
        alert("HubuumEventDeadLetters", 'max by (deployment) (hubuum_event_queue_items{queue="delivery",state="dead"}) > 0', "Dead event deliveries require investigation", "event-backlog", "events", "10m"),
        alert("HubuumEventClaimsStale", 'max by (deployment, queue) (hubuum_event_stale_claims) > 0', "Event claims remain stale", "event-backlog", "events", "10m"),
        alert("HubuumIdentityBackendFailures", 'sum by (deployment) (increase(hubuum_login_limiter_backend_failures_total[10m])) > 0 or sum by (deployment) (increase(hubuum_api_errors_total{class="permission_backend_unavailable"}[10m])) > 0 or sum by (deployment) (increase(hubuum_login_attempts_total{outcome="internal_error"}[10m])) > 0', "Authentication or authorization dependencies are failing", "identity", "identity"),
        alert("HubuumEphemeralTokenKey", 'max by (deployment, instance) (hubuum_token_hash_key_info{mode="ephemeral"}) == 1', "A token key will change on process restart", "identity", "identity", "10m"),
        alert("HubuumOutputCleanupFailures", 'sum by (deployment, kind) (increase(hubuum_task_output_cleanup_failures_total[1h])) > 0', "Stored output retention cleanup is failing", "retention", "storage"),
        alert("HubuumEventRetentionFailures", 'sum by (deployment) (increase(hubuum_db_operation_errors_total{caller="event_retention"}[1h])) > 0', "Event retention database operations are failing", "retention", "storage"),
        alert("HubuumOperatorJobFailed", 'hubuum_operator_job_success == 0', "An externally supervised operator job failed", "operator-jobs", "recovery", "0m"),
        alert("HubuumOperatorJobOverdue", '(time() - hubuum_operator_job_last_success_timestamp_seconds > hubuum_operator_job_max_age_seconds) or (hubuum_operator_job_expected == 1 unless on (deployment, operation) hubuum_operator_job_last_success_timestamp_seconds)', "An expected operator job has no recent successful completion", "operator-jobs", "recovery", "5m"),
        alert("HubuumIntegrationProbeFailed", 'probe_success{job="hubuum-integrations"} == 0', "An external integration probe is failing", "integrations", "{{ $labels.component }}"),
        alert("HubuumFileDescriptorsHigh", '(process_open_fds{job="hubuum"} / process_max_fds{job="hubuum"} > 0.9) and (process_max_fds{job="hubuum"} > 0)', "A process is close to its file descriptor limit", "availability", "process", "10m"),
    ]


def dashboard(slug, title, panels):
    result = {"uid": "hubuum-" + slug, "title": "Hubuum " + title, "schemaVersion": 39,
              "version": 1, "tags": ["hubuum"], "editable": False, "timezone": "browser",
              "refresh": "30s", "time": {"from": "now-6h", "to": "now"},
              "links": [{"type": "dashboards", "tags": ["hubuum"], "asDropdown": True, "title": "Hubuum dashboards"}],
              "templating": {"list": [
                  {"name": "DS_PROMETHEUS", "type": "datasource", "query": "prometheus"},
                  {"name": "deployment", "type": "query", "datasource": {"type": "prometheus", "uid": "${DS_PROMETHEUS}"},
                   "query": "label_values(up{job=\"hubuum\"}, deployment)", "refresh": 1, "includeAll": True, "allValue": ".*", "multi": True}]},
              "panels": []}
    for index, (name, expression, unit, description) in enumerate(panels):
        result["panels"].append({"id": index + 1, "title": name, "type": "timeseries", "description": description,
                                 "datasource": {"type": "prometheus", "uid": "${DS_PROMETHEUS}"},
                                 "gridPos": {"x": (index % 2) * 12, "y": (index // 2) * 8, "w": 12, "h": 8},
                                 "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
                                 "targets": [{"refId": "A", "expr": expression, "legendFormat": "__auto"}]})
    return result


def dashboards():
    s = SCOPE
    shared = "Database-wide snapshot: max deduplicates replicas sharing a deployment label."
    return {
        "overview": dashboard("operations", "operations", [
            ("API availability (30 days)", f'1 - hubuum:api_error_ratio:rate30d{{{s}}}', "percentunit", "99.9% objective; API 2xx/3xx/5xx responses only. Missing or idle traffic has no ratio."),
            ("Availability budget burn", f'hubuum:api_error_budget_burn:rate1h{{{s}}}', "none", "1 consumes exactly one budget over 30 days; alerts use two windows."),
            ("API latency compliance (30 days)", f'1 - hubuum:api_latency_bad_ratio:rate30d{{{s}}}', "percentunit", "99% of successful API responses within one second; excludes asynchronous execution time."),
            ("Configured scrape targets", f'up{{job="hubuum",{s}}}', "none", "Each process must be scraped directly. A missing target is not healthy."),
            ("Active alerts", f'ALERTS{{alertstate="firing",{s}}}', "none", "Alertmanager destinations and paging policy are configured separately."),
            ("Build inventory", f'hubuum_build_info{{{s}}}', "none", "Version and revision labels identify the running build."),
            ("Runtime roles", f'hubuum_runtime_info{{{s}}}', "none", "Workers need independent scrape targets in distributed deployments."),
            ("API and worker scrape age", f'hubuum:scrape_age:seconds{{{s}}}', "s", "Age of the last scrape evaluation; inspect up for failures."),
            ("API request rate", f'hubuum:api_requests:rate5m{{{s}}}', "reqps", "Ordinary API SLO traffic, excluding client errors and probes."),
            ("Successful API latency p95", f'hubuum:api_latency_p95:5m{{{s}}}', "s", "Successful API requests only."),
            ("API readiness (external)", f'probe_success{{job="hubuum-readiness",{s}}}', "none", "Optional per-replica /readyz probes; scraping alone does not prove readiness."),
            ("Database pool utilization", f'hubuum:pool_utilization:ratio{{{s}}}', "percentunit", "Per-process checked-out/configured connections."),
            ("Database operation failures", f'sum by (deployment, caller) (rate(hubuum_db_operation_errors_total{{{s}}}[5m]))', "ops", "Bounded caller categories; investigate persistent errors."),
            ("Oldest queued task", f'hubuum:task_queue_age:seconds{{{s}}}', "s", "Initial queue objective: below 300 seconds."),
            ("Recent terminal task failures", f'hubuum:task_failure:recent{{{s}}}', "none", "One indicates a retained task failure within the last hour."),
            ("Event backlog", f'hubuum:event_queue:items{{{s}}}', "short", shared),
        ]),
        "api": dashboard("api", "API", [
            ("Application requests", f'hubuum:api_requests:rate5m{{{s}}}', "reqps", "SLO traffic excludes client errors, probes, metrics, Swagger and OpenAPI."),
            ("Status distribution", f'sum by (deployment, status_family) (rate(hubuum_http_requests_total{{{s},route=~"/api/.*"}}[5m]))', "reqps", "All application statuses, including client errors."),
            ("Requests by route and method", f'sum by (deployment, route, method) (rate(hubuum_http_requests_total{{{s},route=~"/api/.*"}}[5m]))', "reqps", "Bounded route templates and methods, never raw URL paths."),
            *[(f"Latency p{int(q*100)} by route and method", f'hubuum:api_route_latency_p{int(q*100)}:5m{{{s}}}', "s", "Stable route templates and methods only; successful API requests.") for q in (.5, .95, .99)],
            ("In-flight requests", f'sum by (deployment, route) (hubuum_http_requests_in_flight{{{s},route=~"/api/.*"}})', "short", "Process-local API requests summed across replicas; excludes probes and metrics."),
            ("Authorization and request errors", f'sum by (deployment, class) (rate(hubuum_api_errors_total{{{s}}}[5m]))', "ops", "Bounded public error classes include permission and input/resource limits."),
            ("Allowlist rejections", f'sum by (deployment, reason) (rate(hubuum_client_allowlist_rejections_total{{{s}}}[5m]))', "ops", "No source addresses or user identities are exposed."),
            ("Authentication failures", f'sum by (deployment, outcome) (rate(hubuum_login_attempts_total{{{s},outcome!="success"}}[5m]))', "ops", "Bounded outcomes without principals or addresses."),
            ("Login limiter failures and fallback", f'sum by (deployment, operation) (rate(hubuum_login_limiter_backend_failures_total{{{s}}}[5m]))', "ops", "Shared limiter errors trigger local enforcement fallback."),
            ("Process memory", f'process_resident_memory_bytes{{job="hubuum",{s}}}', "bytes", "Tune instance resource limits to observed workloads."),
            ("Process CPU", f'rate(process_cpu_seconds_total{{job="hubuum",{s}}}[5m])', "cores", "Per process CPU usage."),
        ]),
        "postgresql": dashboard("postgresql", "PostgreSQL", [
            ("Pool utilization", f'hubuum:pool_utilization:ratio{{{s}}}', "percentunit", "Checked-out/configured connections per process; these are pool checkouts, not network connections."),
            ("Pool connections", f'hubuum_db_pool_connections{{{s}}}', "short", "Configured, open, idle and checked-out connections per process."),
            ("Checkout p95", f'histogram_quantile(0.95, sum by (deployment, caller, le) (rate(hubuum_db_connection_acquire_duration_seconds_bucket{{{s}}}[5m])))', "s", "Includes readiness and background callers."),
            ("Acquisition failures", f'sum by (deployment, caller) (rate(hubuum_db_connection_acquire_failures_total{{{s}}}[5m]))', "ops", "Check saturation, reachability and PostgreSQL limits."),
            ("Pool checkout rate by caller", f'sum by (deployment, caller) (rate(hubuum_db_connection_acquire_duration_seconds_count{{{s}}}[5m]))', "ops", "Includes readiness and background checkouts, not new network connections."),
            ("Operation p95", f'histogram_quantile(0.95, sum by (deployment, caller, le) (rate(hubuum_db_operation_duration_seconds_bucket{{{s}}}[5m])))', "s", "Bounded database caller categories."),
            ("Database errors", f'sum by (deployment, caller, result) (rate(hubuum_db_operation_errors_total{{{s}}}[5m]))', "ops", "Use logs for SQLSTATE details and timeout causes."),
            ("Inventory", f'max by (deployment, entity_type) (hubuum_inventory_entities{{{s}}})', "short", shared),
            ("Readiness probes (external)", f'probe_success{{job="hubuum-readiness",{s}}}', "none", "Optional blackbox exporter checks each API /readyz; configure as described in the package."),
        ]),
        "tasks": dashboard("tasks", "tasks and workers", [
            ("Task states", f'max by (deployment, kind, status) (hubuum_tasks{{{s}}})', "short", shared),
            ("Oldest queued task", f'hubuum:task_queue_age:seconds{{{s}}}', "s", "Initial queue objective: age below 300 seconds; tune by task kind."),
            ("Configured workers", f'hubuum_task_workers_configured{{{s}}}', "short", "Zero on HTTP-only replicas is expected."),
            ("Active task counts", f'max by (deployment, kind) (hubuum_tasks{{{s},status="running"}})', "short", shared),
            ("Recent terminal failures", f'hubuum:task_failure:recent{{{s}}}', "none", "One indicates a retained failure within the last hour."),
            ("Queue wait p95", f'histogram_quantile(0.95, sum by (deployment, kind, le) (rate(hubuum_task_queue_wait_duration_seconds_bucket{{{s}}}[5m])))', "s", "Only claimed tasks contribute; oldest queued age also detects starvation."),
            ("Execution p95", f'histogram_quantile(0.95, sum by (deployment, kind, le) (rate(hubuum_task_execution_duration_seconds_bucket{{{s}}}[5m])))', "s", "Completion depends on workload size, not an API latency SLO."),
            ("Terminal outcomes", f'sum by (deployment, kind, final_status) (rate(hubuum_task_completions_total{{{s}}}[5m]))', "ops", "Process counters are summed after calculating their rate."),
            ("Lease recoveries", f'sum by (deployment, kind) (increase(hubuum_task_lease_recoveries_total{{{s}}}[1h]))', "short", "Investigate worker restarts or blocked lease renewal."),
            ("Worker iterations", f'sum by (deployment, outcome) (rate(hubuum_task_worker_iterations_total{{{s}}}[5m]))', "ops", "Claimed, idle and error outcomes."),
            ("Export render/query phases", f'histogram_quantile(0.95, sum by (deployment, phase, le) (rate(hubuum_export_phase_duration_seconds_bucket{{{s}}}[5m])))', "s", "Avoid unbounded per-task and template-name dimensions."),
            ("Stored-template export duration p95", f'histogram_quantile(0.95, sum by (deployment, template_id, le) (rate(hubuum_export_duration_seconds_bucket{{{s},template_id!="none"}}[15m]))) * on (deployment, template_id) group_left (template_name) max by (deployment, template_id, template_name) (hubuum_export_template_info{{{s}}})', "s", "Uses the existing bounded template-info join; identities never enter alert text or dashboard variables."),
            ("Import phases p95", f'histogram_quantile(0.95, sum by (deployment, phase, outcome, le) (rate(hubuum_import_phase_duration_seconds_bucket{{{s}}}[15m])))', "s", "Planning and execution duration by bounded result."),
            ("Output retention failures", f'sum by (deployment, kind) (increase(hubuum_task_output_cleanup_failures_total{{{s}}}[1h]))', "short", "Inspect storage/recovery for cleanup and artifact health."),
        ]),
        "events": dashboard("events", "events", [
            ("Event queue states", f'hubuum:event_queue:items{{{s}}}', "short", shared),
            ("Oldest due event", f'hubuum:event_queue_age:seconds{{{s}}}', "s", "Initial objective: fanout and delivery ages below 300 seconds."),
            ("Dead-letter growth (net)", f'hubuum:event_dead:increase1h{{{s}}}', "short", "Net change in retained dead rows, not a failure counter; retention can mask new dead letters."),
            ("Stale claims", f'max by (deployment, queue) (hubuum_event_stale_claims{{{s}}})', "short", shared),
            ("Worker wakeups", f'sum by (deployment, worker, kind) (rate(hubuum_event_worker_wakeups_total{{{s}}}[5m]))', "ops", "Notification and poll activity; transport outcomes require authorized delivery health reads."),
            ("Configured event workers", f'hubuum_event_workers_configured{{{s}}}', "short", "Inspect /api/v1/event-deliveries/health for per-sink status; never use sink IDs as metric labels."),
        ]),
        "integrations": dashboard("integrations", "identity and integrations", [
            ("Login outcomes", f'sum by (deployment, outcome) (rate(hubuum_login_attempts_total{{{s}}}[5m]))', "ops", "Local and external identity outcomes; use controlled logs for provider-specific diagnosis."),
            ("Shared limiter failures", f'sum by (deployment, operation) (rate(hubuum_login_limiter_backend_failures_total{{{s}}}[5m]))', "ops", "Valkey failures may cause local fallback."),
            ("Login lockouts", f'sum by (deployment, scope) (rate(hubuum_login_lockouts_total{{{s}}}[5m]))', "ops", "Bounded scope only; no principals or addresses."),
            ("Secret resolution failures", f'sum by (deployment, consumer, outcome) (rate(hubuum_secret_resolutions_total{{{s},outcome!="ok"}}[5m]))', "ops", "Includes LDAP, event sinks and remote targets without exposing secrets or endpoints."),
            ("Remote HTTP outcomes", f'sum by (deployment, outcome) (rate(hubuum_remote_call_results_total{{{s}}}[5m]))', "ops", "Task remote calls, not generic event-delivery transport metrics."),
            ("Authorization backend failures", f'sum by (deployment) (rate(hubuum_api_errors_total{{{s},class="permission_backend_unavailable"}}[5m]))', "ops", "Unavailable configured permission backend, including Treetop."),
            ("OTLP export outcomes", f'sum by (deployment, outcome) (rate(hubuum_trace_export_batches_total{{{s}}}[5m]))', "ops", "Applicable only when tracing is configured."),
            ("Integration probes (external)", f'probe_success{{job="hubuum-integrations",{s}}}', "none", "Optional probes for bounded components ldap, treetop, valkey, amqp, smtp and webhook; network probes do not prove delivery."),
        ]),
        "recovery": dashboard("recovery", "storage and recovery", [
            ("Backup outcomes", f'sum by (deployment, final_status) (increase(hubuum_task_completions_total{{{s},kind="backup"}}[1h]))', "short", "A successful backup is not evidence of restorability."),
            ("Backup duration p95", f'histogram_quantile(0.95, sum by (deployment, le) (rate(hubuum_task_execution_duration_seconds_bucket{{{s},kind="backup"}}[1h])))', "s", "Completed backup tasks only."),
            ("Output cleanup", f'sum by (deployment, kind) (increase(hubuum_task_output_cleanup_deleted_total{{{s}}}[1h]))', "short", "Export and backup artifact retention."),
            ("Cleanup failures", f'sum by (deployment, kind) (increase(hubuum_task_output_cleanup_failures_total{{{s}}}[1h]))', "short", "Investigate retention permissions and storage errors."),
            ("Storage errors", f'sum by (deployment, capability, result) (rate(hubuum_storage_operation_errors_total{{{s}}}[5m]))', "ops", "Backend-neutral aggregate diagnostics."),
            ("Operator job results (external)", f'hubuum_operator_job_success{{{s}}}', "none", "External restore verification, archive and retention jobs, recorded through the supplied textfile helper."),
            ("Last successful operator job age", f'time() - hubuum_operator_job_last_success_timestamp_seconds{{{s}}}', "s", "Missing series means unconfigured or never successful; the expected-job alert covers never-run jobs."),
            ("Operator job duration", f'hubuum_operator_job_duration_seconds{{{s}}}', "s", "Measured outside Hubuum; optional node exporter textfile input."),
        ]),
    }


def outputs():
    result = {f"dashboards/{name}.json": value for name, value in dashboards().items()}
    result["prometheus/recording-rules.json"] = recordings()
    # The initial six rules and their regression fixtures are retained verbatim.
    alerts = json.loads((ASSETS / "prometheus/alerts.json").read_text())
    alerts["groups"] = [alerts["groups"][0], {"name": "hubuum.extended", "interval": "1m", "rules": extended_alerts()}]
    result["prometheus/alerts.json"] = alerts
    result["prometheus/operator-rule.json"] = {
        "apiVersion": "monitoring.coreos.com/v1", "kind": "PrometheusRule",
        "metadata": {"name": "hubuum", "labels": {"app.kubernetes.io/name": "hubuum"}},
        "spec": {"groups": recordings()["groups"] + alerts["groups"]},
    }
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    stale = []
    for name, content in outputs().items():
        path = ASSETS / name
        rendered = json.dumps(content, indent=2) + "\n"
        if args.check:
            if not path.exists() or path.read_text() != rendered:
                stale.append(name)
        else:
            path.write_text(rendered)
    manifest = "\n".join([f"dashboards/{name}.json" for name in dashboards()] + ["prometheus/alerts.json", "prometheus/recording-rules.json"]) + "\n"
    path = ASSETS / "manifest.txt"
    if args.check:
        if not path.exists() or path.read_text() != manifest:
            stale.append("manifest.txt")
    else:
        path.write_text(manifest)
    if stale:
        sys.exit("Regenerate operator assets: " + ", ".join(stale))
