# Hubuum - A flexible asset management system

[![CI](https://github.com/hubuum/hubuum/actions/workflows/ci.yml/badge.svg)](https://github.com/hubuum/hubuum/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/hubuum/hubuum)](https://github.com/hubuum/hubuum/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

*Hubuum (𒄷𒁍𒌝) in Sumerian translates as “axle” or “wheel assembly”*[^1].

Hubuum is a REST service that provides a shared interface for your resources.

Hubuum `0.0.1` is the first public release. It is suitable for evaluation and early
deployments, but its API and configuration may change before `1.0.0`. Pin deployments to
an explicit version instead of using the moving `main` image tag.

## Getting Started

Hubuum requires PostgreSQL. The release is available as pre-built archives for Linux,
macOS, and Windows, and as a Linux container image for AMD64 and ARM64.

Linux archives contain stripped, statically linked executables and do not require a compatible
system glibc, libpq, or OpenSSL installation. macOS and Windows archives bundle libpq and OpenSSL
while retaining only their normal operating-system runtime dependencies.

```sh
docker pull ghcr.io/hubuum/hubuum-server:v0.0.1
```

- Follow the [quick start guide](docs/quick_start.md) for configuration and first-time
  administrator setup.
- Follow the [deployment guide](docs/deployment.md) for Docker or Podman Compose
  installation.
- Follow the [distributed deployment guide](docs/distributed_deployment.md) for
  multiple API/worker replicas, one-shot migrations, and optional shared login
  throttling.
- Download native binaries and checksums from
  [GitHub Releases](https://github.com/hubuum/hubuum/releases).
- Check a running instance at `/healthz` and `/readyz`.

The Alpine-based container image includes both the `rustls` and OpenSSL TLS backends. See the
[release guide](docs/releasing.md#container-images) for the complete tag scheme.

## Concept

Most content management systems (CMDBs) are strongly opinionated. They provide fairly strict models with user interfaces designed for those models and all their data. This design may not be ideal for every use case.

CMDBs also like to be authoritative for any data they possess. The problem with this in this day and age, very often other highly dedicated systems are the authoritative sources of lots and lots data, and these sources typically come with very domain specific scraping tools.

With Hubuum you can...

- define your own data structures and their relationships.
- populate your data structures as JSON, and enforce validation when required.
- draw in data from any source into any object, structuring it as your organization requires.
- look up and search within these JSON structures in an efficient way, via a REST interface.
- offload the work of searching and indexing to Hubuum, and focus on your data.
- control permissions to one object set in one application instead of having to do it in multiple places.
- know that REST is your interface, no matter what data you are accessing.

Once upon a time your data was everywhere, each in its own silo. Now you can have it all in one place, and access it all through a single REST interface.

## Design

Hubuum is designed around the idea of classes and objects, where the classes are user-defined and optionally constrained by a JSON schema[^2]. Objects are instances of these classes and these classes only. If the class defines a schema, and the class requires validation against the schema, you are guaranteed that objects within that class conform to said schema.

## API Documentation

- OpenAPI JSON is served at `/api-doc/openapi.json`.
- Swagger UI is served at `/swagger-ui/` when built with the `swagger-ui` feature.

### Authentication in OpenAPI/Swagger

Most endpoints require bearer authentication.

```http
Authorization: Bearer <token>
```

The identity model (human users and service-account principals), the token
lifecycle, token scopes, and the request-authority gates are documented in
[docs/auth_model.md](docs/auth_model.md).
External identity scopes are documented in
[docs/external_auth.md](docs/external_auth.md).

Quick example:

```sh
curl -H "Authorization: Bearer <token>" http://localhost:8080/api/v1/iam/users
```

### OpenAPI Versioning Policy

- The `openapi.info.version` value is tied to `Cargo.toml` package version (`CARGO_PKG_VERSION`).
- `docs/openapi.json` is the canonical committed spec for the current code.
- CI generates the spec and fails if it drifts from `docs/openapi.json`.
- The export endpoint is documented in [docs/export_api.md](docs/export_api.md).
- Stored template examples are documented in [docs/export_template_guide.md](docs/export_template_guide.md).
- Remote target actions are documented in [docs/remote_targets.md](docs/remote_targets.md).
- Event audit and delivery behavior is documented in [docs/events.md](docs/events.md).
- Temporal history, actor capture, and GDPR anonymization are documented in [docs/temporal_history.md](docs/temporal_history.md).
- Personal and shared computed object fields are documented in [docs/computed_fields.md](docs/computed_fields.md).
- Full-system disaster-recovery behavior is documented in [docs/backup-restore.md](docs/backup-restore.md).
- Database pool sizing, observability, and load testing are documented in [docs/performance.md](docs/performance.md).

### Production Behavior

- `swagger-ui` is enabled by default.
- To disable Swagger UI in production builds, build without default features (or without `swagger-ui`):
  - `cargo build --no-default-features`

### Container Networking Note

- The default client allowlist is loopback-only (`127.0.0.1,::1`).
- In containers, inbound clients usually do not appear as loopback, so requests may be rejected unless you set `HUBUUM_CLIENT_ALLOWLIST`.
- `HUBUUM_TRUST_IP_HEADERS` defaults to `false`; only enable it behind trusted reverse proxies.
- For local/dev container setups, `HUBUUM_CLIENT_ALLOWLIST=*` is common.
- For production, prefer explicit CIDRs/IPs instead of `*`.

### Resolving the Real Client IP Behind a Proxy

The client IP used for the allowlist, request logging, and login rate limiting is
resolved from the right of the `[X-Forwarded-For..., peer]` hop chain, so attacker-supplied
`X-Forwarded-For` values cannot be spoofed. Configure trust explicitly:

- `HUBUUM_TRUST_IP_HEADERS=true` is the master switch for honoring `X-Forwarded-For`.
- `HUBUUM_TRUSTED_PROXIES` (preferred): comma-separated proxy IPs/CIDRs. Hops in this set
  are skipped from the connection peer inward, and the first untrusted hop is taken as the
  client (e.g. `HUBUUM_TRUSTED_PROXIES=10.0.0.0/8,192.168.0.0/16`).
- `HUBUUM_TRUSTED_PROXY_HOPS` (fallback when no allowlist is set): the number of proxy
  hops in front of the server to skip from the right of the chain.
- If `HUBUUM_TRUST_IP_HEADERS=true` but neither of the above is set, forwarded headers are
  **ignored** and the connection peer address is used (forwarded values are never trusted
  blindly).

### Token Lifetime

- `HUBUUM_TOKEN_LIFETIME_HOURS` controls bearer token lifetime and defaults to `24`.

### Logging

Hubuum writes newline-delimited JSON logs. Set `HUBUUM_LOG_LEVEL` to control verbosity; see
[docs/logging.md](docs/logging.md) for fields, request correlation, authorization events, and
`jq` recipes.

### Login Rate Limiting

Login throttling is layered across three scopes - per `(username, IP)`, per IP, and per
subnet - so that single-account brute force, password spraying across many usernames from
one host, and distributed spraying from one network are all throttled. When a scope crosses
its threshold within the window it is locked out, and repeated lockouts back off
exponentially (doubling from the backoff base up to the backoff maximum).

- `HUBUUM_LOGIN_RATE_LIMIT_ENABLED` master switch for login throttling; defaults to `true`.
- `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS` max failed attempts per `(username, IP)` per window; defaults to `5`.
- `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP` max failed attempts per client IP per window; defaults to `20` (`0` disables this scope).
- `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET` max failed attempts per client subnet per window; defaults to `100` (`0` disables this scope).
- `HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS` sliding window in seconds; defaults to `300`.
- `HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS` first lockout duration in seconds; defaults to `300`.
- `HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS` maximum lockout duration in seconds; defaults to `86400`.
- `HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4` IPv4 prefix length for subnet aggregation; defaults to `24`.
- `HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V6` IPv6 prefix length for subnet aggregation; defaults to `64`.
- `HUBUUM_LOGIN_RATE_LIMIT_BACKEND` selects local `memory` (default) or shared `valkey` state.
- `HUBUUM_LOGIN_RATE_LIMIT_VALKEY_URL` configures the shared Valkey/Redis service when selected.

Accurate throttling behind a reverse proxy depends on correct client-IP resolution; see
[Resolving the Real Client IP Behind a Proxy](#resolving-the-real-client-ip-behind-a-proxy).

For the full model (scopes, backoff, client-IP resolution, and the admin endpoints for
inspecting and releasing throttled scopes), see [docs/login_rate_limiting.md](docs/login_rate_limiting.md).

### Token Hash Key

- `HUBUUM_TOKEN_HASH_KEY` sets the server-side key used for deterministic token hashing at rest.
- If unset, Hubuum generates an ephemeral in-memory key at startup and logs a warning.
- With an ephemeral key, all existing bearer tokens become invalid after each restart.

### Container Image

- The default container tags include both TLS backends and allow runtime selection with `HUBUUM_TLS_BACKEND`.
- The default image can also run without TLS if no certificate and key are configured.

### Configuration Reference

- The canonical environment-variable reference lives in [docs/quick_start.md](docs/quick_start.md).
- Task-worker and async export-template tuning settings are documented there alongside the core server, DB, auth, and TLS settings.

### Deployment

- Single-host Docker/Podman Compose deployment scripts are documented in [docs/deployment.md](docs/deployment.md).
- Multi-replica topology and upgrade sequencing are documented in [docs/distributed_deployment.md](docs/distributed_deployment.md).
- The scripts support all-in-one frontend/backend installs, backend-only installs, managed Postgres, and an existing external Postgres URL.
- All-in-one installs expose both frontend and backend API hostnames; browser frontend flows can still use the frontend BFF routes.
- Published container images are used by default; local repository cloning/building is opt-in for source builds.
- Curl-style install, update, stop, and uninstall flows are supported; systemd service installation is opt-in.

## Development

Build the workspace with Cargo:

```sh
cargo build --all-features --locked
```

The local test environment is configured through `.env`:

```sh
source .env && ./run_tests.sh
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

See [docs/development.md](docs/development.md) for database setup, Git hooks, and the full
development workflow.

## Releases

Release notes are maintained in [CHANGELOG.md](CHANGELOG.md). Pushing an annotated
`vX.Y.Z` tag for a commit that has passed CI on `main` publishes a GitHub Release with
native archives, SHA-256 checksums, and versioned multi-architecture container images.
Maintainer instructions are in [docs/releasing.md](docs/releasing.md).

## License

Hubuum is available under the [MIT License](LICENSE).

[^1]: Hubuum is probably a loanword from Akkadian.
[^2]: [JSON schema](https://json-schema.org) is a powerful tool for validating the structure of JSON data. It allows you to define the expected format of your data, including required fields, data types, and constraints on values.
