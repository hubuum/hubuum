# Distributed Deployment

Hubuum supports multiple API and background-worker replicas backed by one
PostgreSQL database. Valkey or Redis is optional and adds shared login-throttle
state; the default in-memory limiter remains fully functional for deployments
that do not configure it.

```text
clients -> load balancer -> API replicas ----+
                                             +-> PostgreSQL
                         worker replicas ----+
                                             +-> Valkey/Redis (optional)
```

## Runtime Roles

Set `HUBUUM_RUNTIME_ROLE` on each long-running process:

| Value | HTTP API | Background workers | Intended use |
| ----- | -------- | ------------------ | ------------ |
| `all` | Yes | Yes | Default; preserves the single-process behavior |
| `api` | Yes | No | Horizontally scaled stateless API replicas |
| `worker` | No | Yes | Independently scaled task and event workers |

An `api` process cannot start workers through a later queue notification. A
`worker` process does not bind an HTTP port. Use process/container liveness for
worker replicas rather than `/healthz`; the image's built-in Docker health check
accounts for the worker role. API replicas expose `/healthz` and `/readyz` as
documented in [Quick Start](quick_start.md#health-probes).

The `all` role is the default, so existing single-instance deployments continue
to behave as before.

## One-Shot Migrations

The `api` and `worker` container roles always skip migrations. Setting the
following value as well makes that ownership explicit in deployment manifests:

```env
HUBUUM_SKIP_MIGRATIONS=true
```

Run exactly one migration job before rolling out the new application version.
The production image contains `hubuum-admin` and embedded migrations; it does
not require the Diesel CLI or `psql`.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: hubuum-migrate
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: migrate
          image: ghcr.io/hubuum/hubuum-server:VERSION
          command: ["/usr/local/bin/hubuum-admin", "--migrate"]
          env:
            - name: HUBUUM_DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: hubuum
                  key: database-url
```

Wait for the job to complete successfully before updating API or worker
replicas. For the task-lease migration, use this upgrade order:

1. Stop old-version worker replicas or let their active tasks drain.
2. Run the one-shot migration.
3. Deploy new worker and API replicas.

The drain prevents a new worker from treating a task owned by an old,
lease-unaware worker as abandoned.

## Shared Configuration And Secrets

All replicas must use the same values for settings that define cluster-wide
identity or behavior. In particular:

- `HUBUUM_DATABASE_URL` must point to the same PostgreSQL database.
- `HUBUUM_TOKEN_HASH_KEY` must be one stable shared secret. Tokens are stored in
  PostgreSQL, but replicas cannot verify the same token hash with different
  keys.
- Authentication-provider configuration and referenced credentials must be
  equivalent on every API and worker replica.
- Task lease and event lock durations should be consistent across worker
  replicas.
- Reverse-proxy trust and client-allowlist settings should be consistent across
  API replicas.

The effective non-secret configuration is available through the existing
running-configuration endpoint. Secret fields report only whether they are
configured.

## Shared Login Throttling

The default backend is local memory:

```env
HUBUUM_LOGIN_RATE_LIMIT_BACKEND=memory
```

This has the same behavior as earlier releases and needs no external service.
Each API replica enforces its own limits.

To coordinate attempts and lockouts across API replicas, configure Valkey or
Redis:

```env
HUBUUM_LOGIN_RATE_LIMIT_BACKEND=valkey
HUBUUM_LOGIN_RATE_LIMIT_VALKEY_URL=rediss://valkey.example:6379/0
HUBUUM_LOGIN_RATE_LIMIT_VALKEY_PREFIX=hubuum:login-rate-limit
HUBUUM_LOGIN_RATE_LIMIT_VALKEY_IO_TIMEOUT_MS=1000
```

The local limiter remains active as a safety net. If the shared backend is
unavailable, password logins continue under per-instance limits, a warning is
logged on the transition, and
`hubuum_login_limiter_backend_failures_total{backend="valkey",operation="..."}`
increments. Shared enforcement resumes automatically after Valkey recovers.
Existing bearer-token authentication never depends on Valkey.

Limiter administration remains strict during an outage: shared list, release,
and clear operations return an error when they cannot operate on the canonical
shared state. See [Login Rate Limiting](login_rate_limiting.md).

## Task Ownership And Recovery

PostgreSQL remains the task queue. Claims use `FOR UPDATE SKIP LOCKED`, and each
claimed task now carries a durable random lease token and expiry. The owning
worker renews the lease while executing. State updates and terminal writes are
fenced by that token, so a stale worker cannot overwrite recovery performed by
another replica.

Configure the lease with:

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_TASK_LEASE_SECONDS` | `60` | Lease duration for an active task |
| `HUBUUM_TASK_HEARTBEAT_SECONDS` | `20` | Renewal interval; must be shorter than the lease |
| `HUBUUM_TASK_RECOVERY_INTERVAL_SECONDS` | `30` | Minimum interval between recovery scans in one process |

An expired task is failed, its request payload is redacted, and a system event
records the prior state and recovery reason. Hubuum does not automatically
replay abandoned tasks because imports and remote calls can have external side
effects. Inspect the task history and submit a new task only when replay is
known to be safe.

## Capacity Planning

Each process owns its own database pool. Start with this upper-bound budget:

```text
connections = (api replicas + worker replicas) * HUBUUM_DB_POOL_SIZE
              + migration/administration connections
              + operational headroom
```

Keep the result below PostgreSQL's connection limit and leave capacity for
maintenance. API and worker replicas may use different pool sizes even though
they share the same setting name in their respective process configuration.
See [Performance](performance.md) for measurement and pool metrics.

Scale task workers by increasing worker replicas or `HUBUUM_TASK_WORKERS`.
Correctness does not depend on optimizing the active-task capacity count query;
that query's higher-scale optimization remains follow-up work in
[issue #67](https://github.com/hubuum/hubuum/issues/67).

## Storage And Networking Checklist

- Put a load balancer only in front of `api` replicas.
- Configure `HUBUUM_TRUSTED_PROXIES` for the actual load-balancer networks
  before enabling forwarded-IP headers.
- Do not use local event archive files across replicas. Keep retention archive
  disabled or deliver audit events to shared external storage.
- Mount identical TLS, CA, and authentication configuration where those files
  are used.
- Keep clocks synchronized for logs and external integrations. Valkey supplies
  the shared limiter clock, and PostgreSQL supplies the task-lease clock.
- Use graceful termination and allow at least the worker shutdown timeout
  before forcibly killing a pod.

For a single-host installation, continue to use
[Single-Host Container Deployment](deployment.md).
