# Operate Hubuum

A production deployment uses PostgreSQL, the Hubuum server, and the matching
template-worker executable. Depending on your topology, background workers run
with the API or as separate processes. Web restores need a separately supervised
administrator restore executor.

## Install and prepare

| Decision | Guidance |
| --- | --- |
| One host, optionally including the frontend | [Single-host deployment](../deployment.md) |
| Multiple API or worker replicas | [Distributed deployment](../distributed_deployment.md) |
| Native binaries | [First server](../getting-started/first-server.md) and [configuration](../quick_start.md) |
| One database login or separate privilege roles | [PostgreSQL roles](../database_roles.md) |
| Passwords and stable token keys | [Secret sources and rotation](../secret_sources.md) |
| Local users, LDAP, or external policy | [Identity providers](../external_auth.md) and [Treetop](../treetop/README.md) |

Pin the application versions you deploy. Run migrations as a one-shot workload
before starting the new server. Follow the release's upgrade instructions and
the [distributed sequencing rules](../distributed_deployment.md) for mixed-version
rollouts. The single-host installer can follow moving images, so select explicit
server and frontend tags when you need a reproducible deployment.

## Secure access

Configure TLS or a trusted reverse proxy, client allowlists, and proxy trust.
Use [group permissions](../permissions.md) and scoped service-account tokens for
automation. Read [credential approvals](../credential_approvals.md) before
upgrading credential-management integrations. Check
[login rate limiting](../login_rate_limiting.md) when deploying multiple replicas.

## Monitor and recover

Scrape each process's [metrics](../metrics.md), preserve
[structured logs](../logging.md), and enable [tracing](../tracing.md) where needed.
The repository includes [Grafana dashboards, Prometheus alerts, and runbooks](https://github.com/hubuum/hubuum/tree/main/observability).

Establish a [backup and restore](../backup-restore.md) procedure and exercise it
before relying on it. Watch worker health and task retention, and size database
pools using the [capacity and tuning guidance](../performance.md).

For an incident, start with [troubleshooting](troubleshooting.md).
