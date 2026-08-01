# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Breaking (Rust API):** permission-controller group parameters and
  permission-backend collection and group parameters now use validated
  `CollectionID` and `GroupID` values instead of raw integers. Downstream
  callers and backend implementations must construct the newtypes at their
  input boundaries and update their method signatures.

### Security

- **Breaking (HTTP and Rust API):** Event-delivery API responses no longer
  expose the internal `claim_token` worker lease capability. API clients must
  stop deserializing or depending on this field. The persistence-only
  `EventDelivery` model no longer implements Serde or OpenAPI schema traits,
  and its claim token is crate-private.
- **Breaking:** LDAP provider URLs now reject embedded user information. Move
  service credentials into `bind_dn` and `bind_password`; configuration debug
  output now shows only the LDAP URL scheme, host, and port.
- Local password changes and administrative password resets now atomically
  revoke all active bearer tokens for the affected user.
- Redacted embedded URL credentials and common secret-header spellings from
  event sink configuration and subscription routing responses and audit
  snapshots.
- **Breaking (Rust API):** `OutboundHttpError::ClientBuild`,
  `OutboundHttpError::ResponseRead`, and `OutboundHttpError::Request` no longer
  carry low-level error strings, keeping endpoint details out of integration
  diagnostics and persisted event-delivery failures. Downstream matches and
  constructors must remove the former string payloads. Credential-resolved
  AMQP, email, and Valkey transport failures are likewise reported without
  transport-library detail. The underlying failures remain available to
  administrators in contextual server logs.
- Restore-status capability checks no longer disclose whether a restore stage
  ID exists when the supplied capability is invalid. Server logs distinguish
  missing stages from capability mismatches without recording the capability.
- Response debug logs no longer include custom HTTP header values, preventing
  pagination cursors and future secret-bearing headers from leaking into logs.
- Redacted passwords, raw logout bearer tokens, stored token digests, task
  request payloads, idempotency keys, and task lease tokens from debug output.

## [0.0.8] - 2026-08-01

### Fixed

- Resource-scoped administrator tokens now list in-scope collections, classes,
  objects, and relations without requiring duplicate collection permission
  grants.

## [0.0.7] - 2026-07-31

### Added

- Authorized imports can preserve `created_at` and `updated_at` for collections,
  classes, objects, class relations, and object relations without exposing
  timestamp mutation through standard resource endpoints. Unchanged import
  overwrites do not create redundant history revisions.
- Added optional per-class-side object relation limits, allowing class
  relations to enforce one-to-one, one-to-many, or bounded object
  cardinalities.

### Changed

- Migrated deterministic benchmarks from IAI-Callgrind to Gungraun and updated
  the reusable benchmark workflow to its v2.3 migration bridge.

## [0.0.6] - 2026-07-29

### Added

- Database acquisition, failure, operation-duration, and operation-error metrics
  now include a bounded `caller` label for readiness, maintenance, task, event,
  retention, restore-coordinator, lease, and metrics-refresh work.
- Queued task transactions now wake task workers across processes with
  transactional PostgreSQL notifications.
- Added build, runtime-role, and process-start metrics so every scrape target
  can be identified and counter resets can be correlated with restarts.
- Linux, macOS, and Windows scrape targets now expose standard Prometheus
  process metrics for CPU, resident and virtual memory, file descriptors or
  handles, and process start time, with process refresh health included in the
  existing scrape-refresh diagnostics.
- Worker-only processes now expose their own metrics-only HTTP listener at the
  configured bind address, port, and path, allowing Prometheus to scrape task
  and event workers directly.
- Added scrape-refresh duration and freshness metrics, route-labelled in-flight
  requests, and per-task-kind oldest queued and active ages.
- Added database-wide last-terminal task timestamps by kind and status so
  retained task outcome counts can be distinguished from recent failures.
- Export metrics now expose aggregate phase outcomes and a per-template total
  duration histogram. A resettable database snapshot maps template IDs to
  current names, while export task details persist total, query, hydration, and
  render timings.

### Changed

- **Breaking (metrics):** database metric series now include the `caller`
  label. Update recording rules or exact label-set matches that assume the old
  unlabeled acquisition series.
- Duration histograms now use workload-specific fractional-second buckets for
  useful HTTP, database, remote-call, and background-task percentiles instead
  of OpenTelemetry's unit-agnostic defaults.
- Task, event fan-out, and event delivery safety polling now default to five
  seconds. Transactional notifications preserve prompt normal wakeups, and
  delivery workers still wake at scheduled retry deadlines below that interval.
- Single-host `/metrics` now targets the worker-enabled primary deterministically,
  and `/metrics/standby` exposes the HTTP-only standby. Prefixed routing exposes
  the equivalent `/hubuum-api/metrics` and `/hubuum-api/metrics/standby` paths.
- **Breaking (Prometheus metric contract):** configuration and cleanup metrics
  now use dimensionally typed names, event wakeups are a counter, and the HTTP
  in-flight, task-age, export-phase, and import-phase families have
  new bounded labels. Dashboard and alert owners must migrate from
  `hubuum_task_worker_config`, `hubuum_event_worker_config`,
  `hubuum_event_worker_wakeups`, and `hubuum_export_output_cleanup_*` to the
  replacement families documented in `docs/metrics.md`, and update queries for
  the changed label sets before deploying this version.

### Fixed

- Single-host updates now refresh generated Compose and Caddy configuration
  before rolling application containers, while preserving existing `.env`
  values and adding only newly introduced defaults.
- Shared-host installations using direct routing now proxy `/metrics` to the
  backend instead of letting the request fall through to the frontend.
- Reduced idle database pool checkouts by combining maintenance and claim
  queries on one transaction, combining readiness queries on one checkout,
  collapsing task-kind claims to one query, coalescing restore-coordinator
  heartbeats, and preventing metrics scrapes from running request-maintenance
  checks.
- Direct shared-host routing now uses one health-checked public backend proxy
  instead of creating an independent active checker for every backend path
  group.
- API-only processes now report zero effective event workers instead of the
  worker count configured for worker-enabled roles.
- Restore coordinators now sample guarded local activity only after observing
  the draining generation, preventing stale idle heartbeats from crossing the
  restore drain barrier.
- Worker notification listeners now use dedicated connections so task-only or
  event-only workers with a one-connection execution pool can still claim work.
- Database-backed template identity no longer retains deleted or renamed
  templates in a process, and import and export phase timings record error
  outcomes.
- Process descriptor-pressure metrics now use the live macOS soft limit and
  report the incomparable Windows handle limit as unavailable. Principal
  cleanup cancellations now persist their terminal timestamps.

## [0.0.5] - 2026-07-26

### Added

- Expired bearer-token rows are now purged automatically after a configurable
  post-expiry retention period. Cleanup is coordinated across replicas and uses
  bounded, non-blocking batches.
- The public client configuration now exposes the default token lifetime at
  `authentication.default_token_lifetime_hours`.

### Changed

- **Breaking (Rust API):** `ExternalIdentityProvider::refresh_user` now accepts
  an `ExternalUserRefreshRequest` containing validated current-username and
  expected-subject values. Provider implementations must locate the user by
  username and reject a result whose subject does not match.
- Newly issued tokens now materialize an explicit expiry when the request omits
  one, so later configuration changes cannot alter their lifetime. Login and
  token-mint responses now return that authoritative `expires_at` alongside the
  raw token.
- Token-retention purge batch sizes below 10 are now rejected so both explicit
  and legacy expiry streams make progress in every cleanup transaction.

### Fixed

- Restored permission-aware import and export submission for ordinary users and
  scoped tokens. Workers now enforce the submitting token's persisted
  permission and resource boundary against live grants; stored-template exports
  require `ReadTemplate`, and identity, template, and integration imports remain
  restricted to unscoped administrators.
- LDAP identity refresh now reuses the configured username lookup and verifies
  the returned stable subject, avoiding directory-wide subject searches that
  can exceed provider administrative limits. Provider errors are also logged
  when cached memberships have exceeded the maximum stale window.

## [0.0.4] - 2026-07-25

### Added

- Token-list responses now include each visible token's exact permission and
  resource scope dimensions in a shared `scope` object. A `null` scope
  identifies an unscoped token.
- Added durable task provenance across audit events, task lifecycle history,
  temporal resource history, event subscriptions, and all event sinks. Worker
  and system actions now retain the root task initiator and task ID, API and
  delivery responses batch-resolve actor and initiator names, legacy task
  events use their queued event as a bounded fallback, and full backups and
  event archives retain the additive nullable provenance fields.
- Service-account and human bearer tokens can now be narrowed to specific
  collections, classes, and objects in addition to permission types. Resource
  scopes compose hierarchically, filter list totals and relation endpoints, are
  preserved for asynchronous remote calls, and always intersect with live group
  grants; naming an ungranted resource never grants access.
- Extended permission-aware object aggregation with up to four ordered
  `sum`, `average`, `min`, or `max` measures over nested JSON and numeric
  computed fields, optional global aggregation without `group_by`, explicit
  contributing/skipped value counts, and backend-consistent bounded merging.

### Changed

- **Breaking (HTTP and Rust API):** Token-mint and token-metadata payloads now
  use a singular `scope` object with optional `permissions` and `resources`
  fields. Token-mint clients must replace top-level `scopes` and
  `resource_scopes` with `scope.permissions` and `scope.resources`; legacy flat
  fields are rejected with `400` to prevent accidentally minting an unscoped
  token. Token metadata consumers must replace the flat `scopes` and
  `resource_scopes` fields with `scope`, and must replace the redundant
  `scoped` boolean with a null check on `scope`. Rust callers use
  `TokenScopeDetails::permissions()` or `TokenScopeDetails::resources()`.
- **Breaking (Rust API):** Token metadata identifiers now use `TokenID` and
  `PrincipalID`; call `.id()` where a raw integer is required.
  `TokenScope::resource_scopes()` is renamed to `TokenScope::resources()`.
  The long-positional `create_principal_token`,
  `create_principal_token_with_scope`, and corresponding `_db` helpers are
  removed in favor of `PrincipalTokenCreateRequest`, and token revocation now
  requires typed `TokenID` and `PrincipalID` arguments. Construct a request with
  `PrincipalTokenCreateRequest::new(principal_id)`, set optional values through
  its builder methods, and finish with `.create(&backend, context).await?`.
- **Breaking (Rust API):** `PrincipalTokenMetadata` no longer implements
  `From<PrincipalToken>` because exact scope metadata requires a database
  lookup. Downstream callers must replace `PrincipalTokenMetadata::from(token)`
  or `token.into()` with
  `PrincipalTokenMetadata::load_for_tokens(&backend, &[token]).await?`.
- **Breaking:** Before applying the task-provenance migration, stop
  old-version worker replicas and allow their bounded graceful shutdown to
  finish or fail active tasks. Old API replicas may stay online: legacy
  actor-only writes retain direct-user attribution, and task inserts derive the
  durable initiator from `submitted_by`. The single-host updater drains its
  all-role primary automatically when the API-only standby is available.
- **Breaking (Rust API):** `DatabaseUrlComponents` now parses through `FromStr`,
  exposes a typed `DatabaseVendor`, and keeps its representation private.
  Downstream Rust callers must replace `DatabaseUrlComponents::new(url)` with
  `url.parse::<DatabaseUrlComponents>()` and read components through the new
  accessor methods.
- **Breaking (Rust API):** Pagination limit helpers now use the validated
  `PageLimits` value object instead of `(usize, usize)` tuples. Downstream Rust
  callers must use `default_limit()`, `maximum_limit()`, `resolve()`, and
  `clamp()`, and pass `PageLimits` to the config-free unified-search parser.
- **Breaking:** Export scope `class_id` and `object_id` values must now be
  positive integers. Clients sending zero or negative export scope IDs must
  replace them with valid resource IDs.
- **Breaking (Rust API):** `ExportScope::validate` now returns a
  `ValidatedExportScope`, and the raw `class_id_required` and
  `object_id_required` helpers have been removed. Integrations should validate
  once and retain the returned typed scope during execution.
- JSON filters and object-aggregate dimensions now share one typed JSON-path
  parser. Empty segments, whitespace, and characters outside ASCII letters,
  digits, `_`, and `$` are rejected consistently before SQL generation.

### Fixed

- OpenSSL TLS startup now rejects a certificate and private key that do not
  match instead of binding a server that cannot complete TLS handshakes.
- Event-retention file archives are now durably synchronized before the
  database transaction deletes the archived events.
- Group deletion now checks service-account ownership while holding the group
  row lock, so concurrent account creation returns a stable `409 Conflict`;
  conflict diagnostics also cap the number of account names they include.
- Unix admin CLI backups now synchronize the destination directory after
  atomically replacing the output file.
- Single-host rolling updates now wait for Caddy's passive upstream failure
  marks to clear between replica replacements, preserving continuous routing
  without reprovisioning an unchanged proxy configuration.
- Single-host rolling updates no longer force Caddy to reprovision an unchanged
  configuration after every replica replacement, avoiding transient public
  request failures while still applying changed Caddyfiles.
- Event-retention workers now coordinate one transaction-scoped batch across
  replicas, keep selected event rows locked through archival and deletion, and
  limit terminal-delivery cleanup to the configured batch size. A partial
  retention index is added for old `succeeded` and `dead` deliveries.
- Unified search now uses the shared form-query decoder, so `+` and percent
  escapes are interpreted consistently with other list and search endpoints.

### Security

- **Breaking:** History endpoints now reauthorize stored versions against their
  historical collection, name, and class attributes, and deleted-history admin
  checks use the configured permission backend. History lists omit versions the
  caller cannot read, with pagination totals and cursors based only on visible
  versions; as-of reads authorize the selected version. Deleted-resource
  history continues to require an unscoped configured-backend administrator
  token. Before upgrading, audit history consumers for partial result sets and
  grant access to required historical collections; configure deleted-history
  principals as unscoped administrators in the active permission backend.
- **Breaking:** Remote target header templates and API-key authentication now
  reject HTTP routing, framing, connection-specific, and proxy-authentication
  fields. Existing targets using these transport-controlled headers must remove
  them and let Hubuum's HTTP client derive them from the target URL and body.
- **Breaking:** Async task submission endpoints now require `Idempotency-Key`
  values to contain between 1 and 255 bytes. Clients using empty or longer keys
  must replace them with bounded identifiers. Oversized client-controlled keys
  are rejected before they can fail in PostgreSQL's unique task index.
- Integer list and range filters are limited to 1,024 unique expanded values,
  and oversized ranges fail during bounded parsing instead of allocating an
  attacker-controlled number of integers.

## [0.0.3] - 2026-07-21

### Added

- Added permission-aware aggregated object queries at
  `GET /api/v1/classes/{class_id}/object-aggregates` and its numeric-safe
  `/api/v1/classes/by-name/{class_name}/object-aggregates` alias, with one to
  three scalar, nested JSON, shared computed, or owned personal computed
  dimensions, typed shared and owned personal computed source filters,
  deterministic cursor pagination, explicit
  null/missing/unavailable states, aggregate-cardinality totals, byte-bounded
  source snapshots and external aggregation, and replay-safe cursor transport
  budgets.
- Added explicit, numeric-safe `by-name` aliases for current class and object
  reads, updates, deletes, class-scoped object listing and creation,
  permissions, related-resource views, and object-data patching. Name-addressed
  writes recheck the resolved ID and original natural key under their row lock
  so concurrent renames fail instead of redirecting the operation.
- Added ID- and natural-key-addressed atomic RFC 6902 JSON Patch
  endpoints for raw object data, with row-locked concurrent composition,
  conditional `test` operations, class schema validation, transactional
  computed materialization and audit events, and bounded patch operation and
  pointer depth, result size, result nesting, PostgreSQL JSONB representability,
  and cumulative application work.
- Added unauthenticated `GET /api/v1/config` client capability discovery with
  the effective default and maximum pagination limits, including values
  overridden by server configuration.
- Added database-backed filtering and cursor sorting for shared and personal
  computed object fields, including public/private query aliases, typed filter
  operators, JSON containment, null-safe ordering,
  hash-verified stale shared-cache fallback, owner-only scope-consistent
  personal evaluation, full-list-visibility-safe definition resolution,
  policy-authorized ID pushdown, two-key computed-filter and sort bounds,
  depth-bounded and backend-consistent PostgreSQL-compatible 64 KiB cursor
  validation, read-only stale-cache fallback, and at-most-one-row raw cursor
  enrichment independent of page size.

### Changed

- Class-scoped object creation now infers `hubuum_class_id` and `collection_id`
  from the path. Existing clients may still send either field, but conflicting
  values are rejected.
- **Breaking:** LDAP `group_filters` now match group names produced by
  `group_rules` instead of raw LDAP attribute values. Replace filters containing
  raw directory structure, such as distinguished-name components, with patterns
  matching the configured `name` template.
- Successful `/healthz` and `/readyz` request-completion logs now use `DEBUG`
  severity, while failed probes retain their status-derived severity.
- Pagination now clamps positive client limits above the configured maximum
  instead of returning `400 Bad Request`, and paginated responses expose the
  effective value in `X-Page-Limit`.

### Fixed

- Fixed single-host installs and updates with older `podman-compose` providers:
  service discovery no longer passes unsupported names to `compose ps`,
  migrations do not consume piped installer input, and Caddy no longer holds
  hard Podman dependencies that block rolling replica replacement. Existing
  Caddy containers with legacy dependency metadata are recreated once. Routine
  external-provider notices and successful Caddy reload logs are suppressed,
  while reload failures retain their diagnostic output.
- Fixed an unbound `BASH_SOURCE` warning when running the installer through
  `curl | bash`.
- Generated Caddyfiles now use canonical formatting, no longer emit a
  formatting warning during reload, and use valid prefixed-route redirect
  syntax.

## [0.0.2] - 2026-07-17

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
- Added zero-HTTP-downtime single-host application updates with primary and
  standby API/frontend containers, readiness-aware Caddy load balancing,
  one-shot migrations, shared Valkey login throttling, and ordered rolling
  replacement. PostgreSQL, Valkey, and Caddy now remain running during ordinary
  backend and frontend updates. The distributed deployment guide now also
  defines the Kubernetes and Helm rollout contract for HTTP availability.
- Added durable PostgreSQL task leases with heartbeats, stale-worker fencing,
  terminal recovery without unsafe task replay, and lease recovery metrics.
- Added an optional Valkey/Redis login-rate-limit backend for sharing login
  attempts and lockouts across API replicas while retaining local enforcement
  during shared-backend outages.
- Added `include_total=false` to cursor-paginated API requests so
  latency-sensitive clients can skip the exact count query and omit the
  `X-Total-Count` response header.

### Changed

- **Breaking:** Existing single-host installations must rerun
  `install-single-host.sh` once to generate the redundant API/frontend topology
  before using `update-single-host.sh`; the updater now fails safely when those
  rolling-update services are absent. Ordinary application updates use the
  standby-first rollout helper, while explicit `systemctl restart` continues to
  stop and start the whole stack.
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
