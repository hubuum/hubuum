# Quick Start Guide

## Environment Variables

Hubuum can be configured using environment variables or command-line arguments. All environment variables have the prefix `HUBUUM_`.

### Health Probes

Hubuum exposes unauthenticated probe endpoints for container schedulers and load balancers:

| Endpoint | Purpose |
| -------- | ------- |
| `/healthz` | Liveness probe. Returns `200 OK` when the process can serve HTTP. Does not touch the database. |
| `/readyz` | Readiness probe. Returns `200 OK` only after a simple database query succeeds. Returns `503 Service Unavailable` when the service should not receive traffic. |

Probe paths bypass the client IP allowlist so platform health checks are not rejected before reaching the handler.

### Server Configuration

| Variable | Default | Description |
| ------- | ------- | ----------- |
| `HUBUUM_BIND_IP` | `127.0.0.1` | IP address the server binds to |
| `HUBUUM_BIND_PORT` | `8080` | Port the server listens on |
| `HUBUUM_LOG_LEVEL` | `info` | Logging level (trace, debug, info, warn, error) |
| `HUBUUM_ACTIX_WORKERS` | Detected CPU count | Number of Actix worker threads |

### Access Control Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_CLIENT_ALLOWLIST` | `127.0.0.1,::1` | Comma-separated list of allowed client IPs or CIDRs (e.g., `10.0.0.0/24,2001:db8::/32`) |
| `HUBUUM_TRUST_IP_HEADERS` | `false` | Master switch for trusting the `X-Forwarded-For` header when resolving the client IP |
| `HUBUUM_TRUSTED_PROXIES` | *(empty)* | Comma-separated trusted reverse-proxy IPs/CIDRs. Skipped from the connection peer inward; the first untrusted hop is the client |
| `HUBUUM_TRUSTED_PROXY_HOPS` | `0` | Number of proxy hops to skip from the right of the chain; used only when `HUBUUM_TRUSTED_PROXIES` is empty |

**Container note**: The default `HUBUUM_CLIENT_ALLOWLIST=127.0.0.1,::1` is loopback-only. In containerized setups, clients commonly arrive from bridge/network IPs, not loopback. For local/dev containers, set `HUBUUM_CLIENT_ALLOWLIST=*`. For production, prefer explicit CIDRs/IP ranges.

**Proxy note**: The client IP is resolved from the right of the `[X-Forwarded-For..., peer]` hop chain, so spoofed `X-Forwarded-For` values cannot take effect. When `HUBUUM_TRUST_IP_HEADERS=true`, configure `HUBUUM_TRUSTED_PROXIES` (preferred) or `HUBUUM_TRUSTED_PROXY_HOPS`. If neither is set, forwarded headers are ignored and the peer address is used.

### Database Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_DATABASE_URL` | `postgres://localhost` | PostgreSQL connection URL |
| `HUBUUM_DB_POOL_SIZE` | `10` | Maximum number of database connections in the pool |
| `HUBUUM_SKIP_MIGRATIONS` | `false` | If true, the container waits for the database but does not run Diesel migrations on startup |
| `HUBUUM_DB_STATEMENT_TIMEOUT_MS` | `0` | Pool-global Postgres `statement_timeout` in ms (`0` disables). Cancels any query exceeding it server-side; applies to **all** DB work, not just exports |

### Task System Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_TASK_WORKERS` | About half the detected CPU count, minimum `1` | Number of background task workers |
| `HUBUUM_TASK_POLL_INTERVAL_MS` | `200` | Idle polling interval for background task workers |

### Event And Audit Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_EVENT_FANOUT_WORKERS` | `1` | Number of background workers that fan matching audit events out to delivery rows |
| `HUBUUM_EVENT_FANOUT_BATCH_SIZE` | `100` | Number of events a fan-out worker claims per batch |
| `HUBUUM_EVENT_FANOUT_POLL_INTERVAL_MS` | `250` | Idle polling interval for fan-out workers |
| `HUBUUM_EVENT_FANOUT_LOCK_TIMEOUT_MS` | `30000` | Fan-out claim lock timeout before another worker may retry |
| `HUBUUM_EVENT_DELIVERY_WORKERS` | `0` | Number of background workers that deliver rows to external sinks; `0` disables transport delivery |
| `HUBUUM_EVENT_DELIVERY_BATCH_SIZE` | `100` | Number of delivery rows a delivery worker claims per batch |
| `HUBUUM_EVENT_DELIVERY_POLL_INTERVAL_MS` | `500` | Idle polling interval for delivery workers |
| `HUBUUM_EVENT_DELIVERY_LOCK_TIMEOUT_MS` | `30000` | Delivery claim lock timeout before another worker may retry |
| `HUBUUM_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS` | `25000` | Wall-clock timeout for one external transport attempt; must be less than the delivery lock timeout |
| `HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS` | `1000` | Initial delivery retry backoff |
| `HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS` | `300000` | Maximum delivery retry backoff |
| `HUBUUM_EVENT_DELIVERY_MAX_ATTEMPTS` | `10` | Attempts before a delivery row moves to dead-letter status |
| `HUBUUM_EVENT_RETENTION_PURGE_ENABLED` | `false` | Enables destructive audit event retention purge |
| `HUBUUM_EVENT_RETENTION_DAYS` | `365` | Age threshold for purging eligible audit events |
| `HUBUUM_EVENT_DELIVERY_RETENTION_DAYS` | `30` | Age threshold for purging terminal delivery rows |
| `HUBUUM_EVENT_RETENTION_PURGE_INTERVAL_SECONDS` | `3600` | Retention worker interval |
| `HUBUUM_EVENT_RETENTION_PURGE_BATCH_SIZE` | `1000` | Maximum event rows selected per purge batch |
| `HUBUUM_EVENT_RETENTION_FILE_ARCHIVE_ENABLED` | `false` | Enables local JSON Lines archive writes before deleting eligible events |
| `HUBUUM_EVENT_RETENTION_ARCHIVE_PATH` | *(empty)* | Local JSON Lines archive path; required when file archive writes are enabled |

**Event note**: The canonical audit stream is always stored in the `events`
table. External delivery workers default to disabled, and retention purge
defaults to disabled because it deletes audit rows. See
[Event And Audit](events.md) for audit querying, sink/subscription setup,
delivery semantics, operational health, and retention behavior.

### Export and Template Execution Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_EXPORT_OUTPUT_RETENTION_HOURS` | `168` | How long successful async export outputs remain refetchable before cleanup |
| `HUBUUM_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS` | `300` | How often workers attempt cleanup of expired stored export outputs |
| `HUBUUM_EXPORT_MAX_ACTIVE_TASKS_PER_USER` | `100` | Maximum queued, validating, or running export tasks one user may have at once |
| `HUBUUM_EXPORT_TEMPLATE_RECURSION_LIMIT` | `64` | MiniJinja recursion and template composition depth limit |
| `HUBUUM_EXPORT_TEMPLATE_FUEL` | `50000` | MiniJinja fuel budget for one render |
| `HUBUUM_EXPORT_TEMPLATE_MAX_OBJECTS` | `2000` | Maximum hydrated relation-aware template objects per export |
| `HUBUUM_EXPORT_MAX_OUTPUT_BYTES` | `262144` | Server maximum for rendered export output size; request-level `limits.max_output_bytes` cannot exceed this |
| `HUBUUM_EXPORT_STAGE_TIMEOUT_MS` | `10000` | Post-completion rejection budget per export stage (ms). Rejects an export *after* a stage finishes if it exceeded this; it does not interrupt in-flight work. Use `HUBUUM_DB_STATEMENT_TIMEOUT_MS` to actually cancel slow queries |
| `HUBUUM_EXPORT_DB_STATEMENT_TIMEOUT_MS` | `0` | Export-scoped Postgres `statement_timeout` in ms (`0` disables). Cancels slow queries in-flight **only while executing exports** (applied as a transaction-local `SET LOCAL`), without affecting imports or other DB work. Typically set `<= HUBUUM_EXPORT_STAGE_TIMEOUT_MS` |

**Export/template note**: These settings control async export task behavior, including stored output retention, template execution limits, and relation hydration guardrails. See [Export API](export_api.md) and [Export Template Guide](export_template_guide.md) for the user-facing behavior these limits affect.

### Pagination Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_DEFAULT_PAGE_LIMIT` | `100` | Default number of items per page |
| `HUBUUM_MAX_PAGE_LIMIT` | `250` | Maximum number of items per page |
| `HUBUUM_MAX_TRANSITIVE_DEPTH` | `100` | Maximum recursion depth for transitive relation graph walks |

### Authentication & Authorization

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_ADMIN_GROUPNAME` | `admin` | Name of the admin group |
| `HUBUUM_TOKEN_LIFETIME_HOURS` | `24` | Token lifetime in hours |
| `HUBUUM_LOGIN_RATE_LIMIT_ENABLED` | `true` | Master switch for login throttling |
| `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS` | `5` | Max failed attempts per `(name, IP)` per window |
| `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP` | `20` | Max failed attempts per client IP per window (`0` disables) |
| `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET` | `100` | Max failed attempts per client subnet per window (`0` disables) |
| `HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS` | `300` | Login rate-limit sliding window in seconds |
| `HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS` | `300` | First lockout duration; doubles on repeated lockouts |
| `HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS` | `86400` | Maximum lockout duration for exponential backoff |
| `HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4` | `24` | IPv4 prefix length for per-subnet aggregation |
| `HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V6` | `64` | IPv6 prefix length for per-subnet aggregation |
| `HUBUUM_TOKEN_HASH_KEY` | *(generated per startup if unset)* | Key used for deterministic token hashing at rest |

**Login rate-limit note**: These settings throttle failed logins across layered scopes with exponential backoff. For the full model, client-IP resolution behind proxies, and the admin endpoints for inspecting and releasing throttled scopes, see [login_rate_limiting.md](login_rate_limiting.md).

**Token hash key note**: If `HUBUUM_TOKEN_HASH_KEY` is not set, Hubuum generates an ephemeral key on startup and logs a warning. Tokens issued before restart will be invalid after restart.

### TLS Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_TLS_CERT_PATH` | None | Path to TLS certificate chain file (PEM format) |
| `HUBUUM_TLS_KEY_PATH` | None | Path to TLS private key file (PEM format) |
| `HUBUUM_TLS_KEY_PASSPHRASE` | None | Passphrase for encrypted private key (OpenSSL only) |
| `HUBUUM_TLS_BACKEND` | Auto / unset | Preferred TLS backend when TLS is enabled (`rustls` or `openssl`) |

**Note**: TLS requires both certificate and key paths to be set. The rustls feature does not support encrypted keys with passphrases.

## Exit Codes

The application uses specific exit codes to indicate different failure modes, which helps with monitoring and automation:

| Exit Code | Constant | Description |
| --------- | ------- | ----------- |
| `0` | - | Successful execution |
| `1` | `EXIT_CODE_GENERIC_ERROR` | Generic/unclassified errors |
| `2` | `EXIT_CODE_CONFIG_ERROR` | Configuration validation |
| `3` | `EXIT_CODE_DATABASE_ERROR` | Database connection or pool initialization failures |
| `4` | `EXIT_CODE_INIT_ERROR` | Critical initialization errors (e.g., admin user/group creation) |
| `5` | `EXIT_CODE_TLS_ERROR` | TLS setup errors |

### Exit Code Usage Examples

```bash
# Check if server started successfully
./hubuum-server || echo "Server failed with exit code $?"
```

### First-Time Bootstrap

On first startup with an empty database, Hubuum automatically creates:

- A default admin user (name: `admin`) with a randomly generated password
- A default admin group (named as per `HUBUUM_ADMIN_GROUPNAME`, default: `admin`)
- The admin user is added to the admin group

**Important**: The generated password is not printed or logged. Reset the password immediately after startup:

```bash
hubuum-admin --reset-password admin
```

## Example Configurations

### Development (HTTP)

```bash
export HUBUUM_BIND_IP="127.0.0.1"
export HUBUUM_BIND_PORT="8080"
export HUBUUM_LOG_LEVEL="debug"
export HUBUUM_DATABASE_URL="postgres://user:pass@localhost/hubuum_dev"
./hubuum-server
```

### Production (HTTPS)

```bash
export HUBUUM_BIND_IP="0.0.0.0"
export HUBUUM_BIND_PORT="8443"
export HUBUUM_LOG_LEVEL="warn"
export HUBUUM_DATABASE_URL="postgres://hubuum:secure_password@db.example.com/hubuum_prod"
export HUBUUM_TLS_CERT_PATH="/etc/hubuum/certs/fullchain.pem"
export HUBUUM_TLS_KEY_PATH="/etc/hubuum/certs/privkey.pem"
export HUBUUM_ACTIX_WORKERS="8"
export HUBUUM_DB_POOL_SIZE="20"
./hubuum-server
```

### Docker Compose

```yaml
services:
  hubuum:
    image: hubuum:latest
    environment:
      HUBUUM_BIND_IP: "0.0.0.0"
      HUBUUM_DATABASE_URL: "postgres://hubuum:password@postgres:5432/hubuum"
      HUBUUM_LOG_LEVEL: "info"
      HUBUUM_CLIENT_ALLOWLIST: "*"
    ports:
      - "8080:8080"
    depends_on:
      - postgres
```

## See Also

- [Development Setup](development.md) - Git hooks, testing, and development workflow
- [Authentication & Authorization Model](auth_model.md) - Principals, service accounts, tokens, and scopes
- [Permissions](permissions.md) - Access control and authorization system
- [Collection Hierarchy](collection_hierarchy.md) - Recursive collections, inherited permissions, and move rules
- [Querying](querying.md) - API query syntax and filtering
- [Unified search](search_api.md) - grouped search across collections, classes, and objects
- [Query Support Matrix](query_support_matrix.md) - Endpoint-by-endpoint filter and sort support
- [Event And Audit](events.md) - Audit log, event delivery, sink subscriptions, retention, and operational health
- [Relationships](relationship_endpoints.md) - Working with object relationships
- [Task System](task_system.md) - Background workers, queue claiming, and task execution flow
- [Export API](export_api.md) - Server-side export execution and templated output
- [Remote Target API](remote_targets.md) - Collection-scoped outbound subject actions
- [Export Template Guide](export_template_guide.md) - Stored template syntax, context, and examples
