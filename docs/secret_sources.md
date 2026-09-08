# Secret Sources

Hubuum defaults to environment-backed credentials. Both `hubuum-server` and
`hubuum-admin` can instead read supported secrets from mounted files. File mode
covers credential material; ordinary settings such as ports, database role
mode, token key IDs, and LDAP configuration still use their existing CLI,
environment, or TOML inputs.

API records and LDAP `bind_password_ref` contain validated aliases. An alias
cannot select a provider, arbitrary environment variable, or filesystem path.
Inline LDAP `bind_password` remains a separate compatibility option.

## Configuration Interfaces And Precedence

| CLI option (both binaries) | Environment variable | Default |
| --- | --- | --- |
| `--secret-source environment` or `--secret-source file` | `HUBUUM_SECRET_SOURCE` | `environment` |
| `--secret-file-root DIRECTORY` | `HUBUUM_SECRET_FILE_ROOT` | Unset; required for file mode |

CLI options override their corresponding environment variables. For ordinary
configuration, an unset option uses its documented default. The source and
root are selected once per process and require restart to change.

Database URL selection has these rules:

1. `--database-url` explicitly overrides the runtime URL from either source.
   `--migration-database-url` similarly overrides the administrator's privileged
   URL. The server has no migration-URL option.
2. Otherwise, `environment` mode uses the corresponding environment variable,
   and `file` mode reads the corresponding file in the table below.
3. File mode does not fall back to environment credentials when a file is
   missing, unreadable, empty, or invalid. An explicit URL argument is an
   intentional override, not an automatic fallback.

The default `single` database role mode uses the runtime URL for all workloads,
with an optional privileged URL for migrations and restores. That privileged
URL takes priority for those commands, including when `--database-url` is also
supplied. Only an absent privileged URL falls back to the runtime URL; an
invalid privileged file fails the command. In opt-in `split` mode, migration
and restore commands require the privileged URL and never use the runtime URL
as a fallback. See [PostgreSQL Database Roles](database_roles.md).

The server retains its `postgres://localhost` default in environment mode.
Administrator database commands require a configured URL. Offline
`hubuum-admin --verify-backup` and role-SQL generation need no database secret;
a restore drill with `--restore-test-database-url` checks mounted database URLs
as well as CLI/environment URLs to reject a production database as its target.

## Environment And File Mappings

File paths below are relative to `HUBUUM_SECRET_FILE_ROOT` (or
`--secret-file-root`). Configure only the secrets each workload needs.

| Consumer | Environment source | File source | Explicit URL option |
| --- | --- | --- | --- |
| Runtime/shared PostgreSQL URL | `HUBUUM_DATABASE_URL` | `database/url` | `--database-url` |
| Admin migration/restore PostgreSQL URL | `HUBUUM_MIGRATION_DATABASE_URL` | `database/migration-url` | `--migration-database-url` (admin only) |
| Compatible single token-hash key | `HUBUUM_TOKEN_HASH_KEY` | `token/key` | None |
| Token key-ring ID `primary` | `HUBUUM_TOKEN_HASH_KEY_PRIMARY` | `token/primary` | None |
| Event sink alias `inventory-api` | `HUBUUM_EVENT_SINK_SECRET_INVENTORY_API` | `event-sink/inventory-api` | None |
| Remote-target alias `inventory-api` | `HUBUUM_REMOTE_SECRET_INVENTORY_API` | `remote/inventory-api` | None |
| LDAP alias `readonly_password` | `HUBUUM_LDAP_SECRET_READONLY_PASSWORD` | `ldap/readonly_password` | None |

Aliases contain 1-128 ASCII letters, numbers, underscores, or hyphens. For
file lookup, spelling and case are preserved. For environment lookup, letters
are uppercased and hyphens become underscores. There is no general
`HUBUUM_*_FILE` convention and no CLI option for individual integration secrets.

Environment values are limited to 1 MiB and preserve raw bytes on Unix.
Missing and empty secrets are distinct; an empty secret value is rejected.
The optional administrator migration URL treats an empty environment or CLI
value as unset for compatibility with existing deployment automation. Empty
URL files are errors.

## Mounted File Source

Set the source and root in the environment:

```bash
export HUBUUM_SECRET_SOURCE=file
export HUBUUM_SECRET_FILE_ROOT=/run/secrets/hubuum
```

Or supply the equivalent options to either binary:

```bash
hubuum-server --secret-source file --secret-file-root /run/secrets/hubuum
hubuum-admin --secret-source file --secret-file-root /run/secrets/hubuum --database-ready
```

For example, a single-role workload's mounted directory can contain:

```text
/run/secrets/hubuum/
├── database/
│   └── url
├── event-sink/
│   └── inventory-api
├── ldap/
│   └── readonly_password
├── remote/
│   └── inventory-api
└── token/
    └── key
```

Each file contains the complete secret value. `database/url` contains a whole
PostgreSQL connection URL, including any password and query parameters; it is
not just a password file. `token/key` contains at least 32 bytes. File contents
are not trimmed: when transferring existing text secrets, use `printf '%s'`
rather than appending a newline. Existing environment-backed token keys are
trimmed, so preserve their effective bytes when moving them to files.

Mount the directory read-only and grant directory traversal and file read
access to the process user. The production image uses UID/GID `10001:10001`
unless changed at build time. The provider accepts binary values up to 1 MiB,
opens ordinary files only, bounds each read, detects concurrent file changes,
and rejects paths outside the configured root. Consumers that require text
reject values that are not UTF-8.

Kubernetes projected-secret symlinks are supported: resolved targets and
opened files must remain below the root. Project keys into the relative paths
in the mapping table, and mount the directory containing the projection.
Symlinks escaping that directory are rejected.

Changing an LDAP alias file affects `bind_password_ref`; it does not replace
an inline `bind_password`. Changing the token key source does not change the
active/previous key IDs. Keep `HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY=true` when
stable tokens are required: the legacy single-key mode otherwise retains its
ephemeral-key fallback if `token/key` is absent.

## Single-Role Deployment Example

Prepare `./secrets/database/url` for an existing PostgreSQL database and
`./secrets/token/key` with a stable key. Add integration files as needed.
For native binaries, export the source/root above, run
`hubuum-admin --migrate`, then supervise `hubuum-admin --restore-executor` and
`hubuum-server` as separate processes using those same settings. `single` is
the default; no migration URL or additional PostgreSQL roles are required.

This Compose example uses the same mounted files for all three workloads.
Set `HUBUUM_IMAGE` to the release image being deployed. The database URL must
be reachable from inside the containers.

```yaml
x-hubuum: &hubuum
  image: ${HUBUUM_IMAGE:?Set HUBUUM_IMAGE to the release image}
  environment: &secret-environment
    HUBUUM_SECRET_SOURCE: file
    HUBUUM_SECRET_FILE_ROOT: /run/secrets/hubuum
    HUBUUM_DATABASE_ROLE_MODE: single
    HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY: "true"
  volumes:
    - ./secrets:/run/secrets/hubuum:ro
  read_only: true
  tmpfs:
    - /tmp

services:
  migrate:
    <<: *hubuum
    profiles: [administration]
    entrypoint: /usr/local/bin/hubuum-admin
    command: [--migrate]
    healthcheck:
      disable: true

  restore-executor:
    <<: *hubuum
    entrypoint: /usr/local/bin/hubuum-admin
    command: [--restore-executor]
    restart: unless-stopped
    healthcheck:
      disable: true

  hubuum:
    <<: *hubuum
    environment:
      <<: *secret-environment
      HUBUUM_BIND_IP: 0.0.0.0
      HUBUUM_CLIENT_ALLOWLIST: "*"
    ports:
      - "127.0.0.1:8080:8080"
```

Run the migration before starting the server and web-restore executor:

```bash
docker compose --profile administration run --rm migrate
docker compose up -d restore-executor hubuum
```

The example publishes only on localhost. Configure the client allowlist and
proxy trust for the intended deployment before exposing the API more widely.
Container startup passes explicit secret-source and database URL options to
its administrator readiness probe, so CLI overrides agree with the server.

## Split-Role Mounts

For `HUBUUM_DATABASE_ROLE_MODE=split`, first provision and adopt the roles using
[the database role guide](database_roles.md#adopting-an-existing-single-role-database).
Set split mode consistently on the server, migration job, and restore executor.
Give API/worker workloads a directory containing `database/url` with only the
runtime login, the shared token keys, and their required integration secrets.
Give the migration job and restore executor a separate directory containing
`database/migration-url` with the migrator login. Each workload may mount its
own directory at the same `/run/secrets/hubuum` path; do not mount the migrator
credential into API or ordinary worker containers.

In the Compose example, change role mode to `split` and replace the shared
volume on each admin service with `./admin-secrets:/run/secrets/hubuum:ro`, while
the server keeps `./secrets:/run/secrets/hubuum:ro`. Configure any non-default
owner/migrator/runtime role names consistently. No database URL environment
variables are needed in either mode. One-shot `hubuum-admin --restore` uses the
same role-specific file selection as the restore executor.

## Reload And Rotation

Secret resolution is single-flight and cached separately for each bounded
consumer class. Each cache holds at most 128 aliases and 128 MiB for five
minutes. Failed resolutions are not cached. The application fails closed on
provider errors and does not return expired values; the internal resolver
exposes an explicit opt-in stale policy for consumers that define a different
availability contract.

LDAP service binds, event deliveries, and remote-target calls observe a rotated
value after the cache entry expires or is explicitly invalidated. AMQP, SMTP,
and Valkey sink connection pools key clients by the resolved URI, so a rotated
credential creates a new client and old idle clients leave through the existing
bounded LRU policy.

PostgreSQL URLs and token-hash keys are loaded at startup. Updating their
files does not change a running database pool or key ring. Restart affected
processes after a database credential change; rotate token keys using the
staged procedure below, which includes rolling process restarts.

Cache expiry is checked on use. After a mounted integration file changes, the
next operation after its five-minute cache entry expires reads the new value.
Changing a container's environment configuration also requires recreating or
restarting it; cache expiry cannot update an existing process's environment.

The login rate-limit Valkey URL, Treetop URL, TLS private-key passphrase, and
other certificate paths continue to use their existing configuration adapters
and require restart after changes. They are intentionally listed here so those
consumers are not mistaken for live-rotating integrations.

## Diagnostics

Administrator configuration reports the selected provider, whether a file root
is configured, the effective cache bounds, fail-closed stale policy, and
projected-symlink confinement without returning aliases, paths, versions, or
values. Prometheus exports `hubuum_secret_source_info`,
`hubuum_secret_resolutions_total`, and
`hubuum_secret_resolution_duration_seconds` with bounded provider, consumer,
and outcome labels. Secret values and alias names are never labels.

## Token Key-Ring Rotation

Hubuum accepts one active issuance key and at most seven previous verification
keys. Key IDs contain 1-32 lowercase ASCII letters, numbers, or interior
hyphens. Every key must contain at least 32 bytes. Startup rejects missing keys,
duplicate IDs or material, malformed IDs, short or empty material, and rings
larger than the bound. Error messages and logs never include key material.

The compatible configuration remains:

```text
HUBUUM_TOKEN_HASH_KEY=<stable-secret>
```

It is represented internally as the stable key ID `legacy`. Set
`HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY=true` in production to make a missing
stable key a startup error instead of creating an ephemeral process-local key.

For an environment-backed ring, key ID `old` maps to
`HUBUUM_TOKEN_HASH_KEY_OLD`; for a file-backed ring it maps to `token/old`
below `HUBUUM_SECRET_FILE_ROOT`. IDs are non-secret configuration:

```text
HUBUUM_TOKEN_HASH_ACTIVE_KEY_ID=old
HUBUUM_TOKEN_HASH_PREVIOUS_KEY_IDS=new
HUBUUM_TOKEN_HASH_KEY_OLD=<old-secret>
HUBUUM_TOKEN_HASH_KEY_NEW=<new-secret>
HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY=true
```

Use this staged multi-replica procedure:

1. Upgrade every replica while retaining the compatible
   `HUBUUM_TOKEN_HASH_KEY=<old-secret>` setting. During a rolling software
   upgrade, keep that setting even if ring variables are also staged, because
   a pre-key-ring binary only reads the compatible setting.
2. Generate the new independent key. Deploy `old` as active and `new` as a
   previous key to every replica. Do not advance until every replica's running
   configuration reports the same ring identity. The compatible old-key
   variable may be removed after no pre-key-ring binaries remain.
3. Deploy `new` as active and `old` as previous. During this configuration
   rollout, replicas still using the old-active ring can verify new tokens
   because step 2 taught them `new`, and new-active replicas can verify old
   tokens through `old`.
4. Wait for the previous-key active count to reach zero. Check
   `hubuum-admin --token-key-status` for active, revoked, and expired counts,
   latest validation, and expiry bounds. The runtime configuration exposes
   active and previous IDs plus a deterministic redacted ring identity.
   Prometheus exposes the active ID and redacted identity through
   `hubuum_token_hash_key_info`, plus `hubuum_token_hash_stored` with bounded
   key-state and lifecycle labels.
5. Remove `old` from the previous list and remove its secret, then restart all
   replicas. Confirm the final ring identity is consistent.

New bearer values have the opaque form `hbt1.<key-id>.<secret>`. Verification
uses only the embedded key ID; an unknown or malformed versioned token never
falls back across the ring. Unversioned legacy tokens are checked against the
bounded ring in one storage operation and, after a valid active authentication,
their unidentified stored digest is migrated atomically to the active key.
Revoked and expired tokens are never migrated. A versioned token issued under
a previous key keeps that key ID and ages out through expiry or revocation;
changing its stored digest would contradict the no-fallback format contract.

To roll back step 3, redeploy `old` as active with `new` previous while both
secrets are still retained. Tokens issued during the attempted rotation remain
valid. Never replace the material behind an existing ID in place: that creates
the same mixed-replica failure as the former single-key configuration.
