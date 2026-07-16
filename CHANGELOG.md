# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added class-bound shared and human-owned personal computed object fields with
  a typed deterministic operation catalog, preview and management APIs,
  opt-in object enrichment, transactional shared materialization, stale-read
  fallback and repair, and bounded task-backed rebuilds.
- Added a pluggable permission backend. Local SQL authorization remains the
  default and is available in every build, while opt-in Treetop support makes
  Cedar policies authoritative across point checks, list and search visibility,
  tasks, relations, templates, and reverse permission queries.
- Added consistent, versioned full-system logical backups with durable history
  by default, expiring task outputs, and an administrator CLI path.
- Added staged full restore through the API and admin CLI with document digest
  verification, a hashed recovery capability, an exact destructive
  confirmation phrase, coordinated maintenance mode, and transactional
  rollback on failure. Successful restores leave exactly the restored
  application data plus one `restore.succeeded` provenance event containing the
  backup digest and initiating administrator snapshot. Extended imports can
  merge identity and integration data for administrators and seed deterministic
  benchmark datasets.
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

- Active task admission now uses a partial per-submitter and per-kind index so
  capacity checks remain bounded by queued, validating, and running work rather
  than scanning a submitter's completed task history.
- **Breaking:** Class JSON Schemas are now validated as schema documents before
  storage. Schemas used for object validation reject external HTTP, file,
  dynamic, or recursive references; inline those definitions and reference them
  with local `#...` fragments before enabling validation. Compiled local schemas
  are cached for object validation.
- Related-collection audit visibility now uses an indexed relational projection
  instead of generating JSON predicates for every collection visible to the
  caller.
- **Breaking:** Import and export submission now requires an unscoped runtime
  administrator. Non-admin and scoped tokens now receive `403 Forbidden`.
  Automation should use dedicated service accounts in the configured admin
  group with unscoped tokens; service accounts remain excluded from human/IAM
  administration. Workers recheck runtime-admin authority before execution, so
  queued tasks fail closed if that authority is revoked.
- **Breaking:** Backup documents are now version 3 and always represent a
  full-system disaster-recovery snapshot. Collection-scoped backup and embedded
  import representations were removed; computed-field definitions are included
  while their rebuildable state and materialization cache are excluded. Use
  export/import for selective or merge-oriented transfers, and create new
  version 3 backups before relying on the logical restore workflow. Backup
  creation and artifact access, plus restore staging and confirmation, now
  require an unscoped administrator token; history is included unless
  explicitly omitted.
- Expired export and backup artifacts now share one cleanup schedule and metric
  family. The existing export-prefixed environment variable and metric names
  are retained for compatibility.
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

- Password hashing and verification now run through a bounded blocking-work
  pool instead of blocking asynchronous API and task-worker runtimes.
- Object audit routes now reject class/object path mismatches, and all audit
  route identifiers validate through their domain ID types.
- Event fan-out now uses the transaction-aware PostgreSQL insert trigger as its
  single wakeup source, eliminating the duplicate notification on a mismatched
  channel.
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
- Restore coordination now registers API-only replicas in the drain barrier and
  gives live confirmations an ownership grace period before interrupted-restore
  reconciliation. Backup completion is fenced by the active task lease so a
  stale worker cannot overwrite recovered task state or publish an artifact.
- Extended imports now enforce collection ownership for class-scoped templates
  and remote targets, validate composed templates against existing and
  same-import dependencies, and apply collision policy and restored timestamps
  to group memberships and their sources. Restore uploads are also described as
  `BackupDocument` objects in OpenAPI, and the backup migration can be rolled
  back after backup tasks have been created.
- Interrupted restores are reconciled after restart, full snapshots missing the
  local identity scope or root collection are rejected before draining, import
  dry runs validate extended references and collisions, and generic task
  responses include backup output metadata.
- Restore drain coordination now keeps instance heartbeats fresh, confirmation
  cannot rewrite confirmed or terminal jobs as expired, and merge imports
  preserve restored timestamps while rejecting ambiguous extended references
  and timestamps where `updated_at` predates `created_at`.
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

- Backup files created by the admin CLI are restricted to the owning user on
  Unix and Windows, failed and expired restore jobs erase their staged
  documents, successful restores remove all staging records, and
  imported templates, remote targets, event sinks, and subscriptions receive
  the same validation as API-created rows. Downloaded backups are served as
  attachments, and backup artifacts plus restore responses use
  `Cache-Control: no-store`.
- Backup creation, output retrieval, deferred backup execution, and extended
  identity imports now authorize through the configured permission backend.
- Backup and restore dynamic SQL accepts only closed internal identifier lists;
  backup JSON and restore values remain bound parameters. Full backup artifacts
  contain password hashes, while authentication tokens and environment-backed
  secret values are intentionally excluded.
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
