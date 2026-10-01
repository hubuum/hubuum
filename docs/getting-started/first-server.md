# Run your first server

This walkthrough runs a native release on loopback for evaluation. You need an
existing PostgreSQL database and a login that owns it. For a Linux installation
that provisions PostgreSQL and the frontend too, use the
[single-host installer](../deployment.md) instead.

## 1. Get matching binaries

Download an archive for your platform from
[Hubuum releases](https://github.com/hubuum/hubuum/releases).
[Verify the release artifacts](../supply-chain-security.md), extract them, and
open a terminal in that directory. Keep `hubuum-server`, `hubuum-admin`, and
`hubuum-template-worker` together and from the same release. The server and
administrator use the worker for template rendering and validation.

The following commands use a POSIX shell on Linux or macOS. Windows archives
contain the same executables with `.exe` suffixes; set equivalent environment
variables in your shell.

## 2. Configure the database and token key

Set the connection string for your evaluation database. The example values
below are placeholders; the database must already exist.

```sh
export HUBUUM_DATABASE_URL='postgres://hubuum:your-password@127.0.0.1:5432/hubuum'
export HUBUUM_BIND_IP='127.0.0.1'
export HUBUUM_BIND_PORT='8080'
export HUBUUM_TOKEN_HASH_KEY="$(openssl rand -hex 32)"
export HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY=true
```

Keep the same token hash key across restarts. Regenerating it invalidates existing
tokens. Store production credentials using the
[secret-source configuration](../secret_sources.md); the environment above is
only a local starting point. The [configuration reference](../quick_start.md)
lists all supported options.

## 3. Migrate, then start

```sh
./hubuum-admin --migrate
./hubuum-server
```

The server does not apply migrations on startup. Leave it running. In another
terminal, check the two unauthenticated probes:

```sh
curl --fail http://127.0.0.1:8080/healthz
curl --fail http://127.0.0.1:8080/readyz
```

Both should return HTTP `200`. Liveness checks the HTTP process; readiness also
checks database and migration state. See [troubleshooting](../administration/troubleshooting.md)
if readiness fails.

## 4. Set the initial administrator password

On first startup, the server creates the `admin` user with a random password
that is not printed. In a second terminal, set the same database configuration
and run the administrator command from the extracted directory:

```sh
./hubuum-admin --reset-password admin
```

Follow the command's password instructions. You can now
[make your first API requests](first-requests.md) or connect a
[compatible CLI or frontend](../integrations/clients.md).

## Before sharing the service

Use [the administration guide](../administration/index.md) to configure TLS,
client allowlists, groups, backups, and monitoring. A web restore additionally
requires `hubuum-admin --restore-executor` as a separate supervised process.
Follow [backup and restore](../backup-restore.md) before relying on recovery.

## Add an example inventory

[Load the Atlas dataset](example-dataset.md) for four classes and ten connected
objects you can explore through every Hubuum interface. Then follow
[your first API requests](first-requests.md).
