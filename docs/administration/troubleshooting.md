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
| Tasks remain queued | A worker-enabled process is running against the same database; inspect queue and lease metrics. | [Worker lifecycle](../background_workers.md) and [task runbook](../../observability/runbooks/task-queue.md) |
| Template rendering fails | Matching template-worker executable, resource limits, and template diagnostics. | [Template worker](../template_worker.md) |
| A confirmed restore does not run | The separate restore executor is supervised and has the required database credentials. | [Backup and restore](../backup-restore.md) |
| Pool acquisition becomes slow | Per-process pool saturation and the total database connection budget. | [Pool tuning](../performance.md) |
| Metrics look duplicated or stale | Per-process scrape targets, deployment labels, refresh errors, and gauge aggregation. | [Metrics](../metrics.md) and [operator package](../../observability/README.md) |

## Podman cannot find the migration service

The v0.0.17 single-host scripts place `hubuum-migrate` in the `administration`
Compose profile. Some Podman Compose providers filter that service out unless
the profile is explicitly enabled, producing `missing services [hubuum-migrate]`.
When this happens during migration preflight, the rollout exits before stopping
application processes, although it has refreshed the deployment files.

Check the candidate's migration mode with the profile enabled:

```sh
cd /opt/hubuum
sudo podman compose --env-file .env -f compose.yml \
  --profile administration \
  run --rm --no-deps -T hubuum-migrate --migration-mode
```

This command inspects pending migrations without applying them. If it prints
`offline`, follow the [offline upgrade preparation](../deployment.md#updates)
before retrying. If it prints `rolling`, the updater can use its normal rolling
path. Other errors need diagnosis before starting an update.

For the affected v0.0.17 helpers, enable the profile for one updater invocation
with a temporary Compose provider wrapper. The example uses the usual provider
path `/usr/bin/podman-compose`; use the path reported by your `podman compose`
command if it differs.

```bash
sudo bash -c '
set -euo pipefail
provider="$(mktemp /opt/hubuum/.compose-provider.XXXXXX)"
trap "rm -f -- \"$provider\"" EXIT
printf "%s\n" "#!/bin/sh" "exec /usr/bin/podman-compose --profile administration \"\$@\"" > "$provider"
chmod 700 "$provider"
PODMAN_COMPOSE_PROVIDER="$provider" \
  /opt/hubuum/update-single-host.sh --engine podman --monitoring
'
```

The wrapper is removed when the updater exits. It preserves the updater's
migration preflight and health checks. It does not fix the saved helpers, so
later runs of the affected scripts still need the workaround. Hand edits to
`single-host-rollout.sh` are replaced during script refresh.

## Reporting a problem

Include the server and client versions, deployment topology, redacted request
and response, relevant log correlation ID, and steps to reproduce. Report
server issues in [hubuum/hubuum](https://github.com/hubuum/hubuum/issues), and
interface-specific issues in the relevant [companion project](../ecosystem.md).
Use the [security policy](https://github.com/hubuum/hubuum/blob/main/SECURITY.md)
for vulnerability reports.
