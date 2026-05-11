# Single-Host Container Deployment

Hubuum can be installed on one host with Docker Compose or rootful Podman Compose. The installer uses published container images by default and can deploy either:

- `all`: backend API, frontend, Caddy, Postgres, and Valkey.
- `backend`: backend API, Caddy, and Postgres.

Both modes can also use an existing Postgres server instead of creating a Postgres container.

## Requirements

- Linux host with inbound TCP `80` and `443` open.
- DNS for the selected hostnames pointing at the host.
- `openssl` and either Docker with the Docker Compose plugin or rootful Podman with `podman compose`.
- `git` is only required when using `--build-from-source`.
- Run the script as `root` or through `sudo`.
- Rootless Podman is not supported by this installer because Caddy binds privileged host ports `80` and `443`.

## Container Engine

By default, the installer auto-detects a compose engine:

- Docker Compose is preferred when both Docker and Podman are available.
- Podman Compose is used when Docker Compose is unavailable.
- Use `--engine docker` or `--engine podman` to force one engine.

Podman support is intended for rootful installs only.

## All-In-One

```bash
sudo ./scripts/install-single-host.sh \
  --web hubuum.example.com \
  --api hubuum-api.example.com \
  --email admin@example.com
```

This creates `/opt/hubuum` by default, generates `/opt/hubuum/.env`, writes `compose.yml` and `Caddyfile`, pulls published images, and starts the stack.

By default, the installer starts the stack directly with Compose. Pass `--systemd` to also write `/etc/systemd/system/hubuum.service`, enable it, and start the stack through that unit.

Default app images:

- Backend: `ghcr.io/hubuum/hubuum-server:main`
- Frontend: `ghcr.io/hubuum/hubuum-frontend:main`

## Frontend BFF And Public API

All-in-one installs expose both public hostnames:

- `--web` serves the frontend.
- `--api` serves the backend API directly for integrations and API users.

The frontend is still configured as a BFF-style service. Browser clients can call frontend-owned routes such as `/api/v0/auth/login`, `/api/v1/...`, and `/api/hubuum/...`; the frontend reaches the backend over the private compose network with:

```env
BACKEND_BASE_URL=http://hubuum-api:8080
```

That keeps backend bearer tokens server-side for frontend browser flows, while still leaving the backend API publicly available on the API hostname.

## Shared Host Routing

You can use the same DNS name and public `80`/`443` ports for both frontend and API by setting `--web` and `--api` to the same hostname. In that case, choose an explicit routing mode:

```bash
sudo ./scripts/install-single-host.sh \
  --web hubuum.example.com \
  --api hubuum.example.com \
  --shared-host-routing prefixed \
  --email admin@example.com
```

Modes:

- `prefixed`: exposes the backend under `/hubuum-api/` and sends everything else to the frontend. This avoids collisions with frontend-owned `/api/...` BFF routes and is the recommended shared-host mode.
- `direct`: sends backend-owned paths such as `/api/v0...`, `/api/v1...`, `/api-doc...`, and `/swagger-ui...` directly to the backend, with everything else going to the frontend. This makes the backend available at its normal paths, but those paths bypass the frontend BFF.
- `bff`: sends all traffic to the frontend. The backend is publicly available only through frontend-owned BFF/proxy routes. Use this only if the frontend intentionally proxies every backend API route you need to expose.

For the frontend to make shared-host deployments easier, it should keep its internal/BFF routes under a distinct prefix that will never collide with direct backend routes, for example `/_hubuum-bff/...` or `/api/frontend/...`. That would let Caddy route backend paths like `/api/v1/...` directly to the backend while reserving a separate namespace for frontend-only session and proxy behavior.

## Curl Install

The installer is self-contained enough to run directly from the repository:

```bash
curl -fsSL https://raw.githubusercontent.com/hubuum/hubuum/main/scripts/install-single-host.sh \
  | sudo bash -s -- \
      --web hubuum.example.com \
      --api hubuum-api.example.com \
      --email admin@example.com
```

Use a branch, tag, commit SHA, or PR ref by changing the raw GitHub URL. Pass the same ref with `--script-ref` so the installed management helpers come from the same source:

```bash
REF=main
curl -fsSL "https://raw.githubusercontent.com/hubuum/hubuum/${REF}/scripts/install-single-host.sh" \
  | sudo bash -s -- \
      --script-ref "${REF}" \
      --web hubuum.example.com \
      --api hubuum-api.example.com \
      --email admin@example.com
```

Examples:

- Branch: `REF=my-feature-branch`
- Tag: `REF=v0.1.0`
- Commit: `REF=1a2b3c4d5e6f...`
- Pull request head: `REF=refs/pull/123/head`

For pull requests from forks, GitHub may not expose package/image changes yet; use `--build-from-source` when you need to test code from that PR rather than the published `:main` images.

The script installs management helpers into the install directory:

- `install-single-host.sh`
- `update-single-host.sh`
- `stop-single-host.sh`
- `uninstall-single-host.sh`

## Backend Only

```bash
sudo ./scripts/install-backend.sh \
  --api hubuum-api.example.com \
  --email admin@example.com
```

Equivalent explicit form:

```bash
sudo ./scripts/install-single-host.sh \
  --mode backend \
  --api hubuum-api.example.com \
  --email admin@example.com
```

## Existing Postgres

Pass `--database-url` to skip the managed Postgres container:

```bash
sudo ./scripts/install-single-host.sh \
  --web hubuum.example.com \
  --api hubuum-api.example.com \
  --email admin@example.com \
  --database-url 'postgres://hubuum:secret@postgres.example.com:5432/hubuum?sslmode=require'
```

The database must already exist and be reachable from containers on the host. Avoid `localhost` in the URL unless Postgres is inside the same container; from a container, `localhost` means the API container itself.

## Source Builds

The installer does not clone repositories by default. Use `--build-from-source` only when you need to build local images from Git refs that are not available as published container images:

```bash
sudo ./scripts/install-single-host.sh \
  --web hubuum.example.com \
  --api hubuum-api.example.com \
  --email admin@example.com \
  --build-from-source \
  --backend-ref main \
  --frontend-ref main
```

In source-build mode, the installer clones the repositories under `/opt/hubuum/src` and builds `local/hubuum-api:single-host` and `local/hubuum-web:single-host`.

## Service Management

Systemd service management is opt-in. Install with `--systemd` to manage the stack with systemd:

```bash
sudo systemctl status hubuum
sudo systemctl restart hubuum
sudo systemctl stop hubuum
sudo systemctl start hubuum
```

The containers use `restart: unless-stopped`; a systemd unit adds an explicit host-boot contract. It runs `compose up -d` on start and `compose down` on stop from the install directory.

Use `--service-name NAME` to install the unit under a different name. Without `--systemd`, manage the stack directly with Compose.

## Updates

The installer copies `update-single-host.sh` into the install directory:

```bash
cd /opt/hubuum
sudo ./update-single-host.sh
```

For image-based installs, the update command pulls the latest configured images and restarts the stack. For source-build installs, it fetches the source checkouts, rebuilds the local app images, and restarts the stack.

If the systemd unit exists, updates restart through systemd. Otherwise, updates fall back to `compose up -d`.

## Stop And Uninstall

Stop the stack:

```bash
cd /opt/hubuum
sudo ./stop-single-host.sh
```

Equivalent direct form:

```bash
sudo ./install-single-host.sh --stop --dir /opt/hubuum
```

Uninstall stops the stack and removes the systemd unit if one exists. By default, it preserves `/opt/hubuum` and compose volumes:

```bash
cd /opt/hubuum
sudo ./uninstall-single-host.sh
```

Use `--purge` to also remove compose volumes and the install directory:

```bash
cd /opt/hubuum
sudo ./uninstall-single-host.sh --purge
```

## Parameters

Required:

- `--api`: public backend API hostname served by Caddy.
- `--api-port`: internal backend API listen port. Default: `8080`.
- `--email`: Let's Encrypt registration email.
- `--web`: public frontend hostname. Required only in `all` mode.

Common optional parameters:

- `--stop`: stop the installed stack and exit.
- `--uninstall`: stop the stack, remove the systemd unit if present, and exit.
- `--purge`: with `--uninstall`, also remove compose volumes and the install directory.
- `--dir`: install directory. Default: `/opt/hubuum`.
- `--mode`: `all` or `backend`. Default: `all`.
- `--database-url`: existing Postgres URL. If omitted, the installer creates a managed Postgres container.
- `--engine`: `auto`, `docker`, or `podman`. Default: `auto`.
- `--backend-image`: backend image. Default: `ghcr.io/hubuum/hubuum-server:main`.
- `--frontend-image`: frontend image. Default: `ghcr.io/hubuum/hubuum-frontend:main`.
- `--postgres-image`: managed Postgres image. Default: `docker.io/library/postgres:18-alpine`.
- `--valkey-image`: frontend session/cache Valkey image. Default: `docker.io/valkey/valkey:9-alpine`.
- `--caddy-image`: reverse proxy image. Default: `docker.io/library/caddy:2-alpine`.
- `--network-subnet`: container bridge subnet and backend client allowlist. Default: `172.30.42.0/24`.
- `--shared-host-routing`: required when `--web` and `--api` are the same in `all` mode. Accepted values: `bff`, `direct`, `prefixed`.
- `--systemd`: install and enable a systemd service.
- `--service-name`: systemd service name. Default: `hubuum`.
- `--no-systemd`: skip systemd unit installation. This is the default.
- `--script-base-url`: base URL for downloading management scripts during curl-based installs.
- `--script-ref`: Git ref used to derive raw GitHub management script URLs.
- `--build-from-source`: clone repositories and build app images locally.
- `--backend-ref`: source build backend Git ref. Default: `main`.
- `--frontend-ref`: source build frontend Git ref. Default: `main`.
- `--recreate`: regenerate generated secrets.
- `--no-pull`: skip pulling images before starting.

## Generated Environment

Backend:

- `HUBUUM_DATABASE_URL` and `DATABASE_URL`: Postgres connection URL.
- `HUBUUM_BIND_IP=0.0.0.0`
- `HUBUUM_BIND_PORT`: internal backend API listen port. Default: `8080`.
- `HUBUUM_LOG_LEVEL=info`
- `HUBUUM_TOKEN_HASH_KEY`: generated stable token hash key.
- `HUBUUM_CLIENT_ALLOWLIST`: defaults to the container bridge subnet.
- `HUBUUM_TRUST_IP_HEADERS=false`
- `HUBUUM_TOKEN_LIFETIME_HOURS=24`
- `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS=5`
- `HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS=300`

Frontend, all-in-one mode only:

- `BACKEND_BASE_URL`: internal backend URL, derived from `--api-port`.
- `VALKEY_URL=redis://valkey:6379/0`
- `SESSION_TTL_SECONDS=28800`
- `SESSION_PREFIX=hubuum:sess:`
- `NEXT_PUBLIC_APP_NAME="Hubuum Console"`

The backend creates the initial admin user and group on first startup. The generated admin password is not logged; reset it with `hubuum-admin` in the API container after installation.
