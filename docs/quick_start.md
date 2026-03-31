# Quick Start Guide

## Environment Variables

Hubuum can be configured using environment variables or command-line arguments. All environment variables have the prefix `HUBUUM_`.

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
| `HUBUUM_TRUST_IP_HEADERS` | `false` | Whether to trust `X-Forwarded-For` and `Forwarded-For` headers for client IP detection |

**Container note**: The default `HUBUUM_CLIENT_ALLOWLIST=127.0.0.1,::1` is loopback-only. In containerized setups, clients commonly arrive from bridge/network IPs, not loopback. For local/dev containers, set `HUBUUM_CLIENT_ALLOWLIST=*`. For production, prefer explicit CIDRs/IP ranges.

### Database Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_DATABASE_URL` | `postgres://localhost` | PostgreSQL connection URL |
| `HUBUUM_DB_POOL_SIZE` | `10` | Maximum number of database connections in the pool |

### Task System Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_TASK_WORKERS` | About half the detected CPU count, minimum `1` | Number of background task workers |
| `HUBUUM_TASK_POLL_INTERVAL_MS` | `200` | Idle polling interval for background task workers |

### Report and Template Execution Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_REPORT_OUTPUT_RETENTION_HOURS` | `168` | How long successful async report outputs remain refetchable before cleanup |
| `HUBUUM_REPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS` | `300` | How often workers attempt cleanup of expired stored report outputs |
| `HUBUUM_REPORT_TEMPLATE_RECURSION_LIMIT` | `64` | MiniJinja recursion and template composition depth limit |
| `HUBUUM_REPORT_TEMPLATE_FUEL` | `50000` | MiniJinja fuel budget for one render |
| `HUBUUM_REPORT_TEMPLATE_MAX_OBJECTS` | `2000` | Maximum hydrated relation-aware template objects per report root |
| `HUBUUM_REPORT_STAGE_TIMEOUT_MS` | `10000` | Maximum elapsed time allowed for each report execution stage |

**Report/template note**: These settings control async report task behavior, including stored output retention, template execution limits, and relation hydration guardrails. See [Report API](report_api.md) and [Template Guide](template_guide.md) for the user-facing behavior these limits affect.

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
| `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS` | `5` | Max failed login attempts per rate-limit window |
| `HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS` | `300` | Login rate-limit window in seconds |
| `HUBUUM_TOKEN_HASH_KEY` | _(generated per startup if unset)_ | Key used for deterministic token hashing at rest |

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

- A default admin user (username: `admin`) with a randomly generated password
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
- [Permissions](permissions.md) - Access control and authorization system
- [Querying](querying.md) - API query syntax and filtering
- [Unified search](search_api.md) - grouped search across namespaces, classes, and objects
- [Query Support Matrix](query_support_matrix.md) - Endpoint-by-endpoint filter and sort support
- [Relationships](relationship_endpoints.md) - Working with object relationships
- [Task System](task_system.md) - Background workers, queue claiming, and task execution flow
- [Report API](report_api.md) - Server-side report execution and templated output
- [Template Guide](template_guide.md) - Stored template syntax, context, and examples
