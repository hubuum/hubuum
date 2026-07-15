# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added a pluggable permission backend. Local SQL authorization remains the
  default and is available in every build, while opt-in Treetop support makes
  Cedar policies authoritative across point checks, list and search visibility,
  tasks, relations, templates, and reverse permission queries.
- Added a Prometheus-compatible runtime metrics endpoint, enabled by default at
  `/metrics`, with low-cardinality metrics for HTTP traffic, database activity,
  background tasks, imports, exports, remote calls, authentication, event
  processing, and inventory.
- Added the admin-only `GET /api/v1/admin/config` endpoint for inspecting a
  deny-by-default, redacted view of the effective runtime configuration.
- Added first-class distributed deployment support with `all`, `api`, and
  `worker` runtime roles, explicit one-shot migration ownership, supervised
  background workers, and a deployment guide for scaling API and worker
  replicas independently.
- Added durable PostgreSQL task leases with heartbeats, stale-worker fencing,
  terminal recovery without unsafe task replay, and lease recovery metrics.
- Added an optional Valkey/Redis login-rate-limit backend for sharing login
  attempts and lockouts across API replicas while retaining local enforcement
  during shared-backend outages.
- Added `include_total=false` to cursor-paginated API requests so
  latency-sensitive clients can skip the exact count query and omit the
  `X-Total-Count` response header.

### Changed

- **Breaking:** Import and export submission now requires an unscoped runtime
  administrator. Non-admin and scoped tokens now receive `403 Forbidden`.
  Automation should use dedicated service accounts in the configured admin
  group with unscoped tokens; service accounts remain excluded from human/IAM
  administration. Workers recheck runtime-admin authority before execution, so
  queued tasks fail closed if that authority is revoked.
- **Breaking:** Operational logs from the server and admin CLI are now
  newline-delimited JSON only. Update log collectors and parsers that expect the
  previous text format. Records now include request and correlation IDs,
  status-aware request completion, authenticated principal context, committed
  mutations, authorization decisions, and structured startup information.
- PostgreSQL access now uses an asynchronous connection pool with bounded
  acquisition waits. The admin database metadata endpoint exposes pool capacity,
  wait, timeout, and connection-lifecycle statistics.
- **Breaking:** The default global PostgreSQL statement timeout is now 30
  seconds. Set `HUBUUM_DB_STATEMENT_TIMEOUT_MS=0` to retain an unlimited timeout,
  or configure a deployment-appropriate bound.
- **Breaking:** Active imports are limited to 100 per user by default and unified
  search queries are limited to 256 characters. Adjust
  `HUBUUM_IMPORT_MAX_ACTIVE_TASKS_PER_USER` where needed and keep client search
  input within the new bound.
- **Breaking:** Published containers now use one unprivileged Alpine-based image
  with both TLS backends, embedded migrations, and a built-in health check. Move
  deployments using a `-rustls-only` tag to the default or `-full` tag.
- Linux, macOS, and Windows release archives are now self-contained with bundled
  PostgreSQL and TLS dependencies. The admin CLI can run embedded migrations and
  database readiness checks without external PostgreSQL or Diesel tools.
- **Breaking:** Local Docker Compose services now require an untracked `.env`
  file containing `POSTGRES_PASSWORD`; create it before starting the stack.
  Published ports now bind to loopback, root filesystems are read-only, Linux
  capabilities are dropped, and `no-new-privileges` is enabled.
- Server shutdown now cancels and joins task, event, retention, and PostgreSQL
  notification workers before dropping the database pool. Interrupted active
  tasks are marked failed instead of remaining active.
- **Breaking:** Before applying the task-lease migration, stop old-version
  worker replicas or let their active tasks drain. Then run the one-shot
  migration before starting the new workers; mixed old and new task workers are
  unsupported during this upgrade.

### Fixed

- Background task workers now use the configured permission backend for
  execution-time authorization, including worker-only replicas, rather than
  falling back to local SQL permissions when Treetop is authoritative.
- In-memory login limiting now rejects new high-cardinality scopes at its key
  cap instead of evicting active failures or lockouts, preserving both the
  default limiter and the local Valkey-outage safety state. CI now executes the
  Valkey limiter contract against a real service.
- Distributed API and worker startup, the admin database-readiness command, and
  `/readyz` now require the latest application migration instead of checking
  database connectivity alone.
- Task workers now renew leases through a dedicated database pool and runtime
  thread, stop side-effecting work when renewal failures outlive the confirmed
  lease, keep renewing through failure finalization, and reconstruct
  recovered-task progress from durable import results or terminal single-item
  failure accounting. Lease timestamps are anchored to UTC independently of
  the PostgreSQL session timezone.
- Initialized replicas now skip generating and hashing an unused default
  administrator password during startup.
- Shared login limiting now honors administrative releases across replicas,
  preserves active reservations and lockouts when its key index is full, and
  reuses an asynchronously multiplexed Valkey connection instead of opening a
  connection for every limiter operation.
- Container migration and health-check behavior now honors `--runtime-role`
  command-line overrides as well as `HUBUUM_RUNTIME_ROLE`, and worker processes
  exit when their supervised background workers stop unexpectedly.
- Audited mutations that leave domain state unchanged are now treated as
  no-ops, avoiding misleading lifecycle events and `updated_at` changes. This
  includes entity updates, principal settings, collection moves, permission
  grants and revocations, service-account disable, and group membership
  transitions.

### Security

- Hardened login handling against username enumeration and concurrent
  rate-limit bypasses, and stopped exposing internal database, hashing, and
  service details in public error responses.
- **Breaking:** Remote HTTP calls now enforce their configured timeout and do
  not follow redirects, preventing redirects from bypassing target validation.
  Configure the final validated destination URL directly instead of relying on
  redirects.

## [0.0.1] - 2026-07-11

### Added

- Initial release of Hubuum.
