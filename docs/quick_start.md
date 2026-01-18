# Quick Start Guide

## Environment Variables

Hubuum can be configured using environment variables or command-line arguments. All environment variables have the prefix `HUBUUM_`.

### Server Configuration

| Variable | Default | Description |
| ------- | ------- | ----------- |
| `HUBUUM_BIND_IP` | `127.0.0.1` | IP address the server binds to |
| `HUBUUM_BIND_PORT` | `8080` | Port the server listens on |
| `HUBUUM_LOG_LEVEL` | `info` | Logging level (trace, debug, info, warn, error) |
| `HUBUUM_ACTIX_WORKERS` | `4` | Number of Actix worker threads |

### Database Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_DATABASE_URL` | `postgres://localhost` | PostgreSQL connection URL |
| `HUBUUM_DB_POOL_SIZE` | `10` | Maximum number of database connections in the pool |

### Authentication & Authorization

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_ADMIN_GROUPNAME` | `admin` | Name of the admin group |

### TLS Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_TLS_CERT_PATH` | None | Path to TLS certificate chain file (PEM format) |
| `HUBUUM_TLS_KEY_PATH` | None | Path to TLS private key file (PEM format) |
| `HUBUUM_TLS_KEY_PASSPHRASE` | None | Passphrase for encrypted private key (OpenSSL only) |

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

# Restart on config errors only
./hubuum-server
if [ $? -eq 2 ]; then
    echo "Configuration error - check environment variables"
fi

# Container health checks
if ! timeout 5 ./hubuum-server --help > /dev/null; then
    exit 1
fi
```

### First-Time Bootstrap

On first startup with an empty database, Hubuum automatically creates:

- A default admin user (username: `admin`) with a randomly generated password
- A default admin group (named as per `HUBUUM_ADMIN_GROUPNAME`, default: `admin`)
- The admin user is added to the admin group

**Important**: The generated password is logged once at startup (log level: WARN). Change this password immediately after first login.

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
    ports:
      - "8080:8080"
    depends_on:
      - postgres
```

## See Also

- [Development Setup](development.md) - Git hooks, testing, and development workflow
- [Permissions](permissions.md) - Access control and authorization system
- [Querying](querying.md) - API query syntax and filtering
- [Relationships](relationship_endpoints.md) - Working with object relationships
