# Hubuum operator package

The same seven Grafana dashboards and Prometheus rules serve single-host and
distributed installations: overview/SLO, API, PostgreSQL, tasks/workers, events,
identity/integrations, and storage/recovery. Use assets from your server's Git
tag; `main` describes development. Older releases contain the initial overview
and alerts. The complete manifest and installer integration first appear after
0.0.16. Thresholds are starting points for tuning to your workload.

## Single-host installation

Add `--monitoring` to `scripts/install-single-host.sh`, or enable it on an
existing installation:

```sh
sudo /opt/hubuum/update-single-host.sh --monitoring
```

Both `all` and `backend` modes support Docker Compose and rootful Podman Compose.
Monitoring stays disabled unless requested. In `all` mode the frontend domain
serves `/grafana/` and `/prometheus/`; in `backend` mode the API domain serves
both. Shared-host `bff`, `direct`, and `prefixed` modes reserve these same paths.
Caddy terminates TLS and preserves application prefixes, redirects, asset URLs
and Grafana Live connections.

Grafana uses its native login, with anonymous access and signup disabled.
Prometheus requires separate HTTP Basic credentials at Caddy. Both initially
use username `admin`, with independently generated passwords. These accounts
are separate from Hubuum authentication. No monitoring ports are published
directly. Retrieve initial credentials locally as root:

```sh
sudo awk -F= '/^(GRAFANA_ADMIN_PASSWORD|PROMETHEUS_PASSWORD)=/ {print}' /opt/hubuum/.env
```

Do not paste this output into logs or tickets. Change the Grafana password in
Grafana; its database is authoritative after first startup. The `.env` value
remains the bootstrap password. To change the Prometheus password, update
`PROMETHEUS_PASSWORD` in the root-readable `.env` and run the updater, which
regenerates the Caddy hash. Use a long random hexadecimal value. Grafana's
[authentication options](https://grafana.com/docs/grafana/latest/setup-grafana/configure-access/configure-authentication/)
can support separately managed SSO; the installer does not grant Hubuum users
Grafana access automatically.

Prometheus scrapes `hubuum-api` and `hubuum-api-standby` directly every 15 seconds.
The primary includes workers; the standby serves HTTP only. Both share a stable
`deployment` label, initially the API hostname, and distinct `instance` labels.
They never fail over to one another as scrape targets.

| Setting in `.env` | Default | Purpose |
| --- | --- | --- |
| `MONITORING_ENABLED` | `false` | Persisted opt-in; set by `--monitoring` |
| `MONITORING_DEPLOYMENT` | API hostname | Stable identifier for one database |
| `MONITORING_ASSETS_REF` | `auto` | Derive package from backend tag; `--monitoring-ref` overrides |
| `PROMETHEUS_IMAGE` | Prometheus 3.13.1, digest-pinned | Override with `--prometheus-image` |
| `GRAFANA_IMAGE` | Grafana 13.2.3, digest-pinned | Override with `--grafana-image` |
| `PROMETHEUS_RETENTION_TIME` | `31d` | Allows a full 30-day SLO window once enough data is collected |
| `PROMETHEUS_RETENTION_SIZE` | `5GB` | TSDB retention bound, not a filesystem quota; reserve WAL/head space |
| `PROMETHEUS_MEMORY_LIMIT` | `512m` | Container limit; tune for cardinality and queries |
| `GRAFANA_MEMORY_LIMIT` | `512m` | Container limit |

Each monitoring container also has a one-CPU limit. Reserve additional host disk
and memory before increasing workload or retention. The first retention limit
reached applies, so the configured time window is not guaranteed by time alone.

Updates retain image choices, credentials, labels, named data volumes and
operator-edited configuration. They refresh maintained dashboards and rules
from the selected backend release and recreate monitoring containers to load
them. Source builds use their checkout's package. `latest` resolves the latest
server release; custom images and non-release tags require `--monitoring-ref`.
An unavailable/incomplete package stops refresh instead of silently selecting
another version. Inspect `/opt/hubuum/monitoring/asset-source.txt` for its source.

The initial `monitoring/prometheus/prometheus.yml` and Grafana provisioning files
are retained on updates. Add custom rule files under `monitoring/prometheus/rules/`
and dashboards under `monitoring/grafana/dashboards/`; reserve supplied filenames
for the maintained package. `targets.json` is regenerated for the configured
port and deployment label. Copy a supplied dashboard to a new UID to customize it.

To disable monitoring, set `MONITORING_ENABLED=false` in `.env` and run the
updater. This removes the monitoring containers and Caddy routes while retaining
configuration, credentials, Grafana's encryption key, and data volumes. Re-enable
with `update-single-host.sh --monitoring` to reuse that data and authentication.
Re-running the installer, including `--recreate`, also preserves these secrets.

Stop and ordinary uninstall preserve data and configuration. Explicit
`uninstall-single-host.sh --purge` removes Compose volumes and the installation
directory, including monitoring data. Application backups do not contain these
volumes; back up Grafana and required time series separately.

## Direct Prometheus and Grafana installations

Copy `prometheus/alerts.json` and `prometheus/recording-rules.json` to any
Prometheus server. JSON is valid YAML. Load both through `rule_files` and
configure direct process targets as in
[the example configuration](prometheus/prometheus.example.yml). Use distinct
`deployment` labels for separate databases and stable unique `instance` labels
for every process. Do not scrape an application load balancer.

Import every `dashboards/*.json` into Grafana, then select a Prometheus
datasource and deployment. Datasource selection is a Grafana variable, so no
installer-specific UID or URL is embedded in the dashboards. Alternatively use
a file dashboard provider pointing to that directory. Dashboard UIDs and rule
group names remain stable across installations.

For Prometheus Operator, apply the equivalent resource:

```sh
kubectl --namespace monitoring apply -f observability/prometheus/operator-rule.json
```

Adjust its namespace and labels to match your Prometheus `ruleSelector` and
`ruleNamespaceSelector`. Configure `ServiceMonitor` or `PodMonitor` separately;
preserve `deployment`, `instance`, and the canonical `job="hubuum"` label.
Configure deduplication in the querying layer when using HA Prometheus replicas.
The resource contains exactly the groups in the directly consumed rule files.
An eventual Helm chart can package these files without another set of queries;
this package does not install Prometheus Operator or Helm.

## Local Compose example

For the repository's development stack, use the optional overlay. Set its
credentials in your shell; preserve them securely for later restarts:

```sh
export GRAFANA_ADMIN_PASSWORD="$(openssl rand -hex 24)"
export GRAFANA_SECRET_KEY="$(openssl rand -hex 32)"
export PROMETHEUS_PASSWORD="$(openssl rand -hex 24)"
export PROMETHEUS_PASSWORD_HASH="$(printf '%s\n' "$PROMETHEUS_PASSWORD" |
  docker run --rm -i caddy:2-alpine caddy hash-password)"
# These public configuration assets must be readable by the container users.
chmod -R a+rX observability
docker compose -f docker-compose.yml -f observability/compose.monitoring.yml \
  --profile monitoring up -d
```

First follow the [development database/migration setup](../docs/development.md).
The local overlay exposes `https://localhost:9443/grafana/` and
`https://localhost:9443/prometheus/` through Caddy with a local development CA.
Trust only that local CA for browser use; public single-host installations use
the installer's ACME certificates. This example scrapes the development stack's
single `hubuum` process and mounts the same committed rule and dashboard files.
Compose `down` retains its named volumes; explicit `down --volumes` removes them.

## Service objectives

| Objective | Initial SLI and target | Applicability and exclusions |
| --- | --- | --- |
| API availability | 99.9% non-5xx over 30 days | `/api/` route templates; denominator is 2xx, 3xx and 5xx. Excludes all 4xx, probes, metrics, Swagger, OpenAPI and unclassified routes. Review overload/authentication failures separately. |
| API latency | 99% of successful API responses within 1 second over 30 days | 2xx/3xx API responses only; HTTP time, not asynchronous execution. Split heavy routes into separate objectives when appropriate. |
| Task service | Oldest queued task below 300 seconds | Tune by kind. Histograms only observe claimed tasks, so oldest age and worker capacity also matter. This is an operational age objective, not a completion SLO. |
| Event delivery | Oldest actionable fanout/delivery below 300 seconds; no retained dead deliveries | Requires event processing. Net dead-letter growth is a gauge delta affected by retention, not a failure ratio. |
| Recovery readiness | Isolated restore verification succeeds within the configured maximum age | Requires the external job input below. Backup completion alone does not prove restorability. |

Availability alerts use two-window burn rates: 14.4 times budget over both
1 hour and 5 minutes, or 6 times over both 6 hours and 30 minutes. Latency uses
the 1-hour/5-minute pair against its 1% budget. Idle/absent traffic yields no
success ratio. Retain and collect 30 days before interpreting a monthly SLO.

Pool utilization and resources stay per instance. Counters are rated before
summing across processes. Database-wide task, inventory and event gauges use
`max`, never a replica sum. Missing values are not converted to healthy zeros.
Refresh-staleness checks cover failed inventory refreshes; target discovery and
Prometheus availability still require independent supervision.

## Optional external inputs

[external-metrics.json](external-metrics.json) records dependencies outside the
server metric contract. Missing optional inputs appear as no data, not success.
Configure them for single-host or distributed deployments when applicable.

For readiness, use a Prometheus blackbox exporter with the HTTP module in
[blackbox.example.yml](prometheus/blackbox.example.yml). Probe every API `/readyz`
with `job="hubuum-readiness"`, stable `instance`, and `deployment` labels. The
scrape example includes relabeling. HTTP status probes detect a reachable but
unready server; `up` alone cannot establish readiness.

For integration probes use `job="hubuum-integrations"` and bounded components
`ldap`, `treetop`, `valkey`, `amqp`, `smtp`, or `webhook`. Configure an appropriate
HTTP/TCP probe. Do not label by endpoint host, user, recipient, group or secret
reference. TCP reachability does not prove authentication or delivery. The
server exposes login/permission errors, secret resolution, remote HTTP and OTLP
results; provider-specific freshness and event-transport attempt/latency
counters are not currently emitted. Consult controlled logs and authorized
`GET /api/v1/event-deliveries/health` for that evidence.

The current contract also has no dedicated listener-health, lease-renewal,
worker-shutdown, schema-readiness, maintenance-state, artifact-size or integrity
gauges. Use readiness probes and administrator state for those checks. Task
counts show active work rather than an invented active-worker gauge, and
database error panels retain bounded caller/result categories rather than
claiming to distinguish statement timeouts. Restore duration and outcome come
from the external verification job, not from backup completion. Add native
panels and alerts alongside the corresponding metric-contract additions.

For recovery, archive and externally supervised retention jobs, configure node
exporter's textfile collector. Declare an expected job before its first run
using Python 3.11+ (standard library only):

```sh
python3 scripts/observability.py record-job \
  --directory /var/lib/node_exporter/textfile_collector \
  --deployment production --operation restore_verify \
  --max-age-seconds 86400 --init
```

Wrap your deployment's isolated verification command in its scheduler:

```sh
flock /run/hubuum-restore-verify.lock \
  python3 scripts/observability.py record-job \
  --directory /var/lib/node_exporter/textfile_collector \
  --deployment production --operation restore_verify \
  --max-age-seconds 86400 -- /usr/local/sbin/verify-hubuum-backup
```

The command must fail if restore or integrity checks fail. The helper preserves
its exit status, records duration/outcome atomically, and preserves the last
successful timestamp after failure. `--init` never claims a successful run.
Serialize runs per deployment/operation; the helper does not schedule jobs or
lock backup resources. Only `restore_verify`, `event_archive`, and `retention`
operations are accepted. Monitor the exporter's own scrape failures too;
removing its target is not evidence of health.

## Walkthrough and notifications

1. Open **Hubuum operations** in Grafana and choose the deployment. Check both
   targets at `/prometheus/targets` and the API/worker role inventory.
2. Sign into Hubuum and load the Atlas example inventory from the getting-started
   documentation, or read existing collections. Request rate and pool activity
   should increase within two scrape intervals. Submit an export and inspect
   task completions and export phases.
3. Open `/prometheus/alerts`. On a disposable test installation, stop only
   `hubuum-api-standby`, wait five minutes and observe `HubuumScrapeUnavailable`.
   Start it again and confirm recovery; keep the primary serving traffic.
4. If external job recording is configured, wrap a harmless failing test command
   under a disposable deployment label and inspect the recovery dashboard.
   Remove only that test collector file afterwards.

Prometheus alerts do not send messages without Alertmanager. Add your
Alertmanager targets to the preserved Prometheus configuration; configure
routing, receivers, credentials, grouping and inhibition under your own on-call
policy. Grafana contact points are separate. The installer does not choose
destinations or reuse Hubuum event-sink credentials.

Monitoring on the same host cannot independently report total host failure.
Use external probes and monitoring when that coverage matters.

## Maintain and validate

Models and additional rules live in `scripts/monitoring/generate.py`; the
initial rule group remains maintained in `prometheus/alerts.json`. Run:

```sh
python3 scripts/observability.py generate
python3 scripts/observability.py check --promtool
python3 tests/python/run.py unit monitoring deployment.test_monitoring
python3 tests/python/run.py integration monitoring-fixture --engine docker
```

Validation checks every dashboard/rule metric, label and enum against the
server contract and explicit external list, checks runbooks in both directions,
rejects direct sums of shared gauges, checks SLI exclusions, and compares
Operator/direct groups. Pinned Prometheus parses every query and evaluates
firing, recovery, deduplication and SLI-exclusion fixtures. CI exercises installer
configuration and lifecycle. The `integration monitoring-fixture` command checks Docker and Podman
transport/routing with two independent metrics fixtures. The production-container
CI job also runs the real-server acceptance test:

```sh
python3 tests/python/run.py integration monitoring --image hubuum-server:ci \
  --report target/monitoring-acceptance.json
```

Build the production image first. This test runs the actual single-host installer
in backend mode with PostgreSQL, both Hubuum processes, the restore executor,
Caddy, Prometheus and Grafana. Add `--mode all` to include the frontend and Valkey.
It verifies native Grafana login, all dashboard queries, Atlas import/export,
SQL versus metric counts, exact HTTP counter deltas, SLI exclusions, recording
rules, and the real five-minute scrape alert followed by recovery. It also creates
two webhook deliveries from one collection update through the API: one succeeds;
the other receives HTTP 503 responses, becomes retryable, and exhausts its two
configured attempts. Pending, failed, retryable, dead and recovered snapshots must
match exact SQL and health API counts on both Prometheus targets and the actual
Grafana event panel. The shared-database recording must deduplicate the targets.
Receiver access logs must show the same event UUID on every HTTPS attempt.

The production ten-minute dead-letter alert must become pending, fire once for
the deployment, and recover after the receiver accepts an administrator-triggered
retry. Queue rows and alert durations are never rewritten. A temporary worker
uses the installed image and runtime database credentials, a two-minute retry
backoff, the existing private-target setting and the installation's disposable
CA; certificate verification remains enabled. The fixture briefly pauses that
worker so the due retry remains observable. Database snapshots are allowed their
documented cache and scrape intervals to converge. The receiver's extra internal
Caddy host and the worker are removed before lifecycle checks. Allow approximately
25 minutes for the complete acceptance run (35-minute CI deadline).
Updates, disable/re-enable, uninstall/restart and purge exercise credentials,
application data, Grafana state and historical Prometheus samples.

Each run uses a unique Compose project and loopback port and purges its resources,
including on failure. Only the root guard, download sources, global container
names, published ports and bridge subnet are adapted in temporary copies; the
rollout, health checks, database setup and alert hold remain unchanged. The
provided server image and checkout assets are used throughout updates. Reports
contain non-secret results and the failing stage, never generated credentials.

The CLI groups generation, validation, tests and external job recording under
`python3 scripts/observability.py`; its implementation uses normal modules in
`scripts/monitoring/`, with Python 3.11+ and no third-party packages.

Every metric-contract change requires reviewing this package in the same pull
request. Update affected models, dependencies, fixtures and runbooks; do not
suppress missing metrics. Version files with the server release and pin runbook
links to that tag when immutable incident guidance is required.
