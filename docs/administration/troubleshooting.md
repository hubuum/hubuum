# Troubleshooting

Start with the affected process and the request's correlation fields in
[structured logs](../logging.md). Compare its version and effective
configuration with the rest of the deployment. Avoid copying tokens, passwords,
or unredacted configuration into an issue.

| Symptom | First checks | Detailed guidance |
| --- | --- | --- |
| `/healthz` fails | Process status, bind address, listener, and proxy routing. | [Configuration](../quick_start.md#health-probes) |
| `/healthz` works but `/readyz` fails | Database connectivity, migration state, and maintenance/restore status. | [Deployment sequencing](../distributed_deployment.md) |
| Clients cannot connect through a container or proxy | Client allowlist and trusted proxy settings; clients may not appear as loopback. | [Configuration](../quick_start.md) |
| Tokens fail after a restart | Stable token hash key and matching key-ring settings across replicas. | [Secrets and rotation](../secret_sources.md) |
| A request returns `403` | Principal's groups, inherited permission rows, token scope, and credential approval when applicable. | [Permissions](../permissions.md) and [approvals](../credential_approvals.md) |
| Login returns `429` | The throttled scope and correct client-IP resolution. | [Login rate limiting](../login_rate_limiting.md) |
| Tasks remain queued | A worker-enabled process is running against the same database; inspect queue and lease metrics. | [Worker lifecycle](../background_workers.md) and [task runbook](https://github.com/hubuum/hubuum/blob/main/observability/runbooks/task-queue.md) |
| Template rendering fails | Matching template-worker executable, resource limits, and template diagnostics. | [Template worker](../template_worker.md) |
| A confirmed restore does not run | The separate restore executor is supervised and has the required database credentials. | [Backup and restore](../backup-restore.md) |
| Pool acquisition becomes slow | Per-process pool saturation and the total database connection budget. | [Pool tuning](../performance.md) |
| Metrics look duplicated or stale | Per-process scrape targets, deployment labels, refresh errors, and gauge aggregation. | [Metrics](../metrics.md) and [operator package](https://github.com/hubuum/hubuum/tree/main/observability) |

## Reporting a problem

Include the server and client versions, deployment topology, redacted request
and response, relevant log correlation ID, and steps to reproduce. Report
server issues in [hubuum/hubuum](https://github.com/hubuum/hubuum/issues), and
interface-specific issues in the relevant [companion project](../ecosystem.md).
Use the [security policy](https://github.com/hubuum/hubuum/blob/main/SECURITY.md)
for vulnerability reports.
