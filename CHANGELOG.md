# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- The experimental public storage adapter SDK now has a closed six-crate
  crates.io graph, one lockstep `0.1.0` version, exact in-graph dependencies,
  Rust 1.88 MSRV and feature guarantees, explicit closed/non-exhaustive enum
  policy, deprecation rules, coordinated release order, and adapter upgrade
  process. `hubuum-storage-conformance` is the supported certification entry
  point; the root application and PostgreSQL adapter remain internal.

### Changed

- The workspace storage contract now exhaustively specifies every pageable,
  searchable, batched, and complete-collection method, including supported
  query keys, cursor/count/visibility behavior, ordering, multiplicity,
  completeness, bounds, snapshot consistency, and portable errors. Adapter
  changes that omit a method contract now fail the architecture suite.
- Storage semantic evidence is now method-aware: every contract method has one
  machine-checked effect classification and direct named evidence, and the
  context-free writer inventory is enforced exhaustively so new workflow
  writers cannot be omitted silently.
- **Breaking (workspace storage API):** authorization candidate reads now
  accept cursor-page query types and return `StorageCandidatePage<T>`, while
  unified-search reads return bounded pages of
  `StorageUnifiedSearchCandidate<T>` with adapter-owned rank cursors. Complete
  adapter implementations must apply the requested look-ahead limit, report
  `has_more`, preserve stable cursor order, and construct the rank cursor beside
  every unified-search row.
- Delegated collection lists, permission-grid group lists, and unified search
  now authorize bounded storage pages before public paging and retain only a
  response page plus look-ahead. Exact totals still scan every candidate page
  without retaining the complete set, while skipped totals stop as soon as the
  response can be finalized.
- Complete collection enumeration for delegated event and remote-target
  visibility now requires an explicit validated candidate ceiling and returns
  service unavailable above the current 10,000-candidate maximum instead of
  materializing an unbounded set.

## [0.0.10] - 2026-08-30

### Added

- Added a read-only, versioned `POST /api/v1/search` DSL for collections,
  classes, objects, audit events, users, groups, and service accounts. It
  supports typed `and`/`or`/`not`, target-specific fields, stable
  fingerprint-bound cursors, optional exact totals, audit redaction, and
  equivalent permission-aware behavior for SQL and external policy backends.
  Object searches additionally support an optional exact class selector and
  bounded existential related-object predicates.
- Added `POST /api/v1/search/stream`, which accepts the same structured-search
  DSL and emits `started`, ordered tagged `result`, and terminal `done` or
  `error` server-sent events without holding a database connection under client
  backpressure.
- `hubuum-domain`, `hubuum-events-core`, `hubuum-query`,
  `hubuum-task-core`, and `hubuum-storage-core` now have explicit compatibility,
  MSRV, error, runtime, cancellation, and security policies in preparation for
  a possible later crate split. They remain workspace-internal and unpublished
  in this release. An external-crate integration test exercises the future
  adapter-facing surface without making a crates.io support promise.
- The redacted administrator configuration now reports the selected complete
  storage backend and effective non-secret pool settings. Startup logs and
  Prometheus metrics expose the same backend identity, and storage calls have
  uniform bounded tracing plus duration and failure metrics. The server and
  administrator CLI accept the typed `HUBUUM_STORAGE_BACKEND` selector;
  `postgresql` is the sole registered value, empty selects that default, and
  other unknown values fail startup.
- Class object lists now accept up to four named `related.<alias>` filter
  groups. Each group selects one target class, normal target-object fields, and
  an optional bidirectional depth up to 10; groups are combined with `AND`, and
  graph paths remain permission-aware and resource-scoped with both SQL and
  external policy backends. External-policy graph expansion uses explicit
  target, object, and relation work limits.
- Added machine-enforced Rust package support classifications. CI prevents
  internal crates from becoming publishable and automatically adds rustdoc,
  clean-package, and crates.io-backed semantic compatibility checks when a
  crate is deliberately promoted to a supported public status.
- Storage compatibility now has a machine-checked trait-method and input-variant
  evidence inventory, runs representative service and authenticated HTTP paths
  for every registered backend, and uses adapter-private deterministic
  failpoints to verify rollback of compound collection and task-finalization
  writes.
- Breaking storage-adapter API: event envelopes now expose typed, validated
  accessors instead of public raw fields; event sink, subscription, and
  inventory and delivery-work projections now require fallible construction;
  subscription catalogs use `EntityType` and `Action`; event update requests
  use terminal builders; and
  `BackupSnapshotStorage::create_backup_snapshot` is renamed to
  `capture_backup_snapshot`. External adapter authors must migrate field reads
  to accessors, handle `try_build` or `try_new` failures as backend corruption
  when decoding persisted state, use typed subscription catalog values, and
  rename the backup snapshot method.
- Storage query documentation now defines common visibility, counting,
  snapshot, ordering, cursor, limit, and error semantics plus the exact
  identity-membership and collection-authorization filter/sort matrix. The
  remaining method-specific matrices are explicitly required before the
  boundary is promoted to a supported external adapter SDK.
- Storage backend certification now includes a reusable six-part audit
  conformance harness. Every selectable backend must prove that committed
  receipts match durable events, no-ops append nothing, failed mutations roll
  back state and events, durable outbox work reaches a recording sink, and
  logical, backend, and failure telemetry is reported, and stale writes return
  the exact current revision. Reusable retention and deterministic-fault
  runners additionally prove durable retry identity, idempotent completion,
  delivery recovery, restore-coordination rollback, and lease-loss
  finalization while adapters retain native fault injection.
- Storage backends now provide a mandatory backend-neutral unit of work for
  composing collection, class, object, and relation operations. Transactional
  mutations inherit one audit context, and shared PostgreSQL and memory-model
  tests verify whole-graph commit plus state-and-event rollback.

### Changed

- Unified search SSE responses now begin before database search work and emit
  collection, class, and object batches as each completes instead of buffering
  the entire event sequence before returning the response.
- Removed the legacy root PostgreSQL storage implementation tree and direct SQL
  from the storage compatibility harness. PostgreSQL composition is now
  confined to the storage factory, adapter-native fixtures are typed and
  feature-gated in `hubuum-storage-postgres`, and reusable
  application/service/HTTP expectations live in
  `hubuum-storage-conformance`. Deterministic storage fault coverage now
  includes delivery acknowledgement, restore coordination, lease loss, and
  connection loss; high-availability failover remains out of scope.
- Main-branch CI no longer updates a rolling GitHub Release. Native binaries
  remain available as per-run Actions artifacts, while multi-architecture GHCR
  images receive a commit-SHA tag before the movable `main` and `main-full`
  tags are advanced to the same manifest.
- Storage backends now implement one complete compile-time contract for
  catalog and lifecycle operations, authorization and identity, queries and
  history, workflows, operational behavior, and durable-worker notifications.
  Application code exchanges backend-neutral DTOs and errors through uniformly
  observed traits; PostgreSQL owns Diesel rows, SQL construction, transactions,
  native notifications, migrations, and adapter-specific failures. Opaque
  storage contexts preserve the configured backend instance. Resource services
  depend on their exact operation traits; focused adapters implement only the
  traits they support and cannot be selected as complete backends.
  PostgreSQL helpers accept concrete pools only inside the adapter tree.
  Storage-only contexts no longer carry authorization-provider selection;
  policy-aware workflows explicitly require the stronger application context,
  whose `AuthorizationContext::authorization_mode` implementation must choose
  `LocalStorage` or `Delegated` explicitly.
  PostgreSQL is the only selectable production backend, and compatibility tests
  exercise every semantic capability group for every selectable backend.
  Validated domain identifiers now live in the backend-neutral domain crate, and their OpenAPI
  schemas explicitly declare the existing positive-integer invariant. The
  deprecated administrator configuration field
  `exports.database_statement_timeout_ms` remains as an alias for
  `exports.storage_query_budget_ms`.
- **Breaking (workspace storage API):** complete storage adapters must now
  implement `TransactionStorage` in addition to the existing capability
  traits. Capability traits consistently use the `<Domain>Storage` suffix;
  operations use `get_*`, `list_*`, `resolve_*`, and `search_*`; observer hooks
  are `StorageObserver` and `PostgresObserver`; identity administration is
  split into focused bootstrap, scope, membership, service-account, and
  external-identity traits; `TransactionStorage::with_transaction` owns atomic
  callbacks; archive destinations implement async `EventArchiveSink`; and
  paginated operations use `StoragePage<T>` instead of domain-specific
  wrappers. `WorkerNotificationProvider` is now an
  optional application-composition provider rather than a required
  `StorageBackend` supertrait. Long positional parts aliases are replaced by
  named private-field types, token issuance policy construction validates its
  invariants, and retention orchestration can no longer be overridden by
  adapters. Adapter authors must update method names, await archive calls,
  provide atomic transaction-scoped lifecycle accessors, attach notification
  providers explicitly when supported, and use the named `StorageError`
  constructors. Named parts structs replace the most error-prone positional
  decompositions introduced or changed by this boundary work. Static
  application selection remains unchanged; there is no dynamic plugin
  interface.
- **Breaking (workspace storage API):** task execution now uses separate
  `StorageTaskActiveUpdate` and `StorageTaskTerminalUpdate` values. Both are
  fallible, claims require an active task with a projected lease expiry, and
  `StorageTaskCompletion::try_new` requires the task kind plus its matching
  export, backup, remote-call, or absent artifact. Task progress and task
  projections also use fallible construction. Adapter authors must replace
  `StorageTaskStateUpdate::new`, `StorageTaskCompletion::new`,
  `StorageTaskProgress::new`, and `StorageTaskBuilder::build`, map invalid
  persisted projections to `StorageErrorKind::Backend`, and verify that a
  completion's declared kind matches the claimed stored task. PostgreSQL now
  rejects negative task progress counters with database constraints. Operators
  upgrading a database containing negative values in `tasks.total_items`,
  `processed_items`, `success_items`, or `failed_items` must repair those rows
  before running migrations.
- Persisted reversed history intervals and negative computed-field revisions
  are now consistently reported as `StorageErrorKind::Backend` instead of
  `Internal`.
- **Breaking (workspace storage API):** ordinary storage mutations now require
  `EventContext`; resource lifecycle mutations return `StorageMutationOutcome` with a
  non-empty set of durable `StorageAuditReceipt` values for commits and no receipt for
  genuine no-ops. Import
  and restore implement `ImportStorage` and `RestoreStorage` directly, and
  logical/native observers are supplied explicitly by application composition.
  External
  adapter authors must remove optional audit contexts, return the new outcome
  type from resource methods, provide telemetry observers, implement the
  reusable audit fixture, and be added to the sealed certification registry.
  No database or HTTP migration is required.
- **Breaking (workspace storage API):** the backend-neutral domain, query, event,
  and storage contracts now keep database and application implementation
  details behind their crate boundaries. External adapter authors must replace
  raw lifecycle, event, principal, metadata, and revision integers with the
  matching `hubuum-domain` IDs and `ResourceRevision`; use
  `StorageRevisionTarget` instead of formatted revision-owner keys; construct
  and mutate `QueryOptions` through its bounded collection and cursor APIs;
  handle the now-fallible `QueryOptions::set_limit` method, which rejects zero
  and values outside the native signed limit range;
  replace `SQLMappedType` and the JSONB-named inference helpers with
  `QueryScalarType` and the `infer_*` helpers; use semantic event `parse`
  methods instead of `from_db`; and update matches to the semantic
  `StorageErrorKind` names. HTTP shapes, database schemas, and persisted event
  documents are unchanged by this Rust boundary cleanup.
- **Breaking (workspace storage API):** capability ownership and naming now
  align across traits, discovery modules, family bounds, and observer labels.
  Adapters must rename `AuthorizationStorage` to `AuthorizationDataStorage`,
  `CollectionAuthorizationStorage` to
  `CollectionAuthorizationQueryStorage`, `ComputedFieldLifecycleStorage` to
  `ComputedFieldStorage`, `EventSubscriptionStorage` to
  `EventConfigurationStorage`, `EventDeliveryStorage` to
  `EventDeliveryWorkerStorage`, and `BootstrapStorage` to
  `LocalIdentityCredentialStorage`. Group listing and lifecycle belong to
  `GroupStorage`; `GroupMembershipStorage` owns membership reads and mutations;
  and retained-token listing belongs to `TokenStorage`. Pageable operations use
  `list_*`, while complete policy projections use `load_*`; page-returning
  method names no longer carry a redundant `_page` suffix. Renamed operations
  otherwise use consistent `resolve_*`, `update_*`, and
  service-account-specific names. The coarse cross-cutting family is
  `OperationalStorage`, matching the other singular family bounds. Observer
  capabilities are typed `StorageCapability` values; storage metrics users
  must update label selectors such as `authorization` to
  `authorization_data`, `event_subscriptions` to `event_configuration`, and
  `event_delivery` to either `event_delivery_administration` or
  `event_delivery_worker`; the membership label is `group_membership`.
  Capability labels now uniformly use the singular trait stem, for example
  `collection`, `collection_authorization_query`, `computed_field`,
  `task_queue`, and `transaction`. Complete
  batch reads now use `load_token_metadata_by_ids` and
  `load_export_template_health` rather than `get_*` names. Authorization
  candidate enumeration is now named `load_authorization_*_candidates`, group
  candidate queries accept filters only, and the duplicate collection-grant
  methods on `CollectionAuthorizationQueryStorage` are removed in favor of
  `AuthorizationDataStorage::{list_local_collection_grants,
  get_local_collection_grant}`. Operational adapters must also rename
  `list_export_templates_for_audit` to `load_export_templates_for_audit`.
  `ExportTemplateStorage` no longer duplicates class ownership lookup; callers
  must use `ClassStorage::resolve_class`.
- **Breaking (workspace storage API):** object aggregation accepts one
  `StorageObjectAggregateAuthorization` strategy that carries a delegated authorizer
  when required. The query no longer stores a separate authorization mode, so
  callers cannot construct storage/delegated mode and callback mismatches.
  Delegated target and object authorization, computed-definition resolution,
  aggregation, and paging now share one repeatable-read snapshot. The native
  read transaction remains open while the application-owned policy backend
  evaluates bounded candidate batches.
- **Breaking (workspace storage API):** contract value names now consistently
  begin with `Storage`, while persistence ports retain the
  `<Capability>Storage` form. Adapter authors must rename authentication,
  authorization, computed-object, history, relation-query, unified-search,
  event-worker, metric, operational, mutation, and other previously unprefixed
  values. `ObjectAggregateStorageQuery` becomes
  `StorageObjectAggregateQuery`; the canonical flat class projection becomes
  `StorageClass`, while the projection embedding its collection becomes
  `StorageClassWithCollection`. `StorageTaskPageQuery` becomes
  `StorageTaskChildListQuery`, `StorageRemoteHttpMethod` becomes
  `StorageRemoteTargetHttpMethod`, `StorageObjectsByClassCount` becomes
  `StorageObjectCountByClass`, and `StorageRestoreTimestampParts` becomes
  `StorageRestoreTimestampsParts`. The fallible authentication-attempt and
  token-observation constructors now use `try_new` and return
  `StorageValidationError`; `StorageTokenIssuancePolicy::new` becomes
  `StorageTokenIssuancePolicy::try_new`; the task-create and object-aggregate
  query builders now terminate with `try_build`.
- **Breaking (workspace storage API):** storage pages now use one
  `StoragePage<T>` with an optional non-negative exact total instead of a
  negative sentinel or a second counted-page type. Persisted record metadata
  uses explicit UTC timestamps and rejects reversed creation/update order.
  Restore identities, artifact digests, lifecycle timestamps, remote-target
  transports and policies, event-delivery and computed-rebuild states, retained
  events, and backup rows validate their invariants at construction. History
  metadata, import DTOs, group membership, user details, remote transports, and
  remote-call artifact targets expose named parts where positional
  decomposition would be especially error-prone. Contract DTO timestamps use
  explicit UTC values; adapters must convert native timestamp representations
  at their private boundary. Import execution policy, identity-key scopes,
  class schema-validation state, and permission replacement behavior are now
  concrete before crossing into an adapter; adapter authors must remove native
  fallback defaults for those values.
  `StorageErrorKind::ValidationFailed` distinguishes semantically invalid
  content, and malformed persisted conflict metadata becomes an internal
  storage error instead of panicking. The complete storage contract no longer
  exposes a generic unsupported-operation result; all mandatory behavior must
  be implemented. `StorageTaskCreateRequestBuilder::try_build` now returns a
  result and rejects negative totals and zero active-task capacity; callers
  must handle that validation error. Ordinary `StorageTask` projections expose
  lease presence and expiry without exposing or prescribing a UUID claim-token
  representation. `StorageTaskClaim::try_new` rejects mismatched task and lease
  identifiers, and `StorageTaskResultCounts::try_new` centrally rejects negative
  task progress counts. Task status, lifecycle timestamp, and lease
  combinations are validated at projection construction. Restore lifecycle
  markers cannot follow the row's update timestamp, and synchronization success
  cannot follow the latest attempt. `StorageTaskGaugeLastTerminal` now requires
  a terminal timestamp instead of accepting `Option`; metric and operational
  snapshots correlate their counts and timestamps. Computed-object page
  projections must exactly match unique returned rows. Recorded-event audit
  receipt conversion is now infallible, so callers must remove result handling
  around `StorageRecordedEvent::into_audit_receipt`. Group pages now carry one
  query from which adapters
  derive an optional exact count, while computed-object requested/execution
  queries validate their shared predicate and count intent, normalized stable
  ordering, effective page limit, and one-row look-ahead. Export task artifacts
  and retained output projections require exactly one typed content variant
  and a non-negative warning count. Backup task artifacts derive their byte
  size and SHA-256 digest from their document, and retained backup projections
  validate those values. Token-scope permission lists now carry
  `StorageAuthorizationPermission` values instead of strings, and service-account
  disable outcomes carry `StorageTaskKind` values instead of task-kind strings;
  adapter authors must remove string parsing at those boundaries. Task-output
  durations reject negative phases. Shared
  projection validators now return an unclassified `StorageValidationError`:
  applications must map caller values with `into_request_error`, while adapters
  must map rejected persisted projections to `StorageErrorKind::Backend`.
  Fallible constructors consistently use `try_new` or `try_build`; adapter
  authors must update constructor names and handle these validation results.
- **Breaking (backup/restore):** full backups are version 5 and restore rejects
  version 4. Version 5 uses stable logical resource and history section names,
  semantic class/object/principal fields, permission-name arrays,
  `history_entry_id`, `create`/`update`/`delete` history operations, and
  explicit RFC 3339 UTC timestamps. PostgreSQL tables, legacy columns, trigger
  operation codes, credentials, and worker-claim fields are mapped or excluded
  privately by its adapter. Operators must create new backups after upgrading;
  adapters must project their persistence layout to the version 5 logical
  schema.
- Administrator configuration responses add `database.backend` and
  `exports.storage_query_budget_ms`; the deprecated
  `exports.database_statement_timeout_ms` alias remains with the same value.
  Database diagnostics document the backend-unavailable `404` response. The
  token-resource and remote-invocation `oneOf` variants remain JSON-compatible
  with v0.0.9, and optional provenance task IDs still accept the same
  integer-or-null values. No client migration is required because the
  token-resource and remote-invocation `oneOf` variants are schema-identical to
  v0.0.9 and optional provenance task IDs retain the same integer-or-null
  representation.
- **Breaking (storage operations):** event retention now uses durable
  claim/archive/complete batches. `HUBUUM_EVENT_RETENTION_ARCHIVE_PATH` names a
  directory, not an append-only JSONL file; operators using local archival must
  create or point to a durable directory and must not reuse the old file path.
  Each claim is atomically written as `<batch-uuid>.jsonl`. Failed archives
  retain the same claim and source events for retry, and completion deletes
  exactly the claimed IDs. The new retention-claim migration is applied
  automatically before the worker runs.
- Backend-neutral logical metrics traits and DTOs now live in
  `hubuum-storage-core`. PostgreSQL pool statistics remain adapter-owned and
  are projected by application composition into the root-owned compatibility
  shape used by the legacy database endpoint and gauges.
- Event worker and retention configuration validation now uses backend-neutral
  policy terminology, matching the storage traits and DTOs used by application
  workers instead of exposing database implementation language.
- **Breaking (Rust API support policy):** the root `hubuum` library and crates
  not explicitly classified as experimental public remain internal and
  non-publishable. Their Rust `pub` items are workspace construction details
  rather than supported third-party embedding APIs. Rust application clients
  must use `hubuum-client-rust` over the versioned HTTP/OpenAPI contract. The
  server, admin, and OpenAPI binaries call narrow internal library entrypoints,
  and the library owns API/worker/all runtime composition.

### Fixed

- Single-host rolling updates now require Caddy to report the API standby
  itself eligible before draining the primary for migrations. Unrelated web
  failure marks no longer block frontend recovery, custom API ports are
  respected, and expected replicas are checked after each rollout phase.
- `hubuum-admin --migrate` once again exits with the documented database error
  code `3` when it cannot connect or apply a migration.
- Structured search now accepts its documented dotted JSON paths, rejects
  unknown properties on every expression node, and never emits a wrapped
  cursor too large to reuse in the same compact request.
- External-policy structured class and object searches now require parent
  collection visibility in addition to the resource-specific read permission,
  matching SQL-backed search behavior. Related-object traversal applies the
  same conjunctive visibility rule to every object it examines.

### Security

- Production container builds now upgrade packages from the pinned Alpine 3.24
  repositories before installing runtime dependencies. This incorporates
  OpenSSL 3.5.8-r0, which fixes CVE-2026-14456 in the 3.24.1 base image.
- Mitigated RUSTSEC-2026-0258 by disabling Actix's optional HTTP/2 feature, so
  vulnerable `h2` 0.3 code is not compiled. HTTP/1.1, TLS, compression,
  cookies, and WebSockets remain enabled. Deployments requiring HTTP/2 must
  terminate it at a reverse proxy until Actix supports the fixed `h2` 0.4
  series; the owned advisory exception exists only because Cargo records the
  disabled optional package in `Cargo.lock`.
- Treetop authorization failures now redact transport details, response bodies,
  URL credentials, and failed batch-item diagnostics from public errors. A
  pinned, hermetic real-service conformance gate now exercises these failure
  paths and the complete shared authorization corpus before releases.

## [0.0.9] - 2026-08-07

### Added

- Principal settings PATCH endpoints now accept bounded RFC 6902 JSON Patch via
  `application/json-patch+json`, including atomic `test` with numeric-value
  equality, precise array updates, literal JSON `null`, and rollback-safe `add`,
  `remove`, `replace`, `move`, and `copy` operations. Existing `application/json`
  JSON Merge Patch behavior is unchanged, with `application/merge-patch+json`
  also accepted as an alias.
- OpenAPI validation now blocks pull requests, `main`, and tagged releases on
  generated-document drift or unaccepted compatibility breaks from the latest
  stable release. CI publishes the generated document, exact drift diff,
  baseline tag and digest, structured compatibility data, and a grouped
  Markdown report for review.
- `HUBUUM_TREETOP_CA_CERT` now loads PEM CA certificate bundles from regular
  files up to 4 MiB into the Treetop client's trust store, enabling private PKI
  without disabling TLS certificate validation.
- Token managers can inspect retained expired and revoked credentials through
  lifecycle-filtered lists and point lookups, and can renew an active or expired
  token into a fresh secret with the same descriptive metadata and exact scope.
  Token audit snapshots now retain that hash-free scope and link renewals to
  their source token.
- Token retention now writes a system-authored `token.purged` audit event with
  the exact final hash-free token and scope snapshot before deleting each token,
  including legacy rows whose earlier events predate complete scope snapshots.
- Release CI now certifies the immediately previous stable image by immutable
  digest through migration, a live mixed-version API interval, application
  rollback, and candidate restoration. Reports include migration timing and API
  availability measurements while the fixed earliest-release test remains.

### Changed

- **Breaking (Rust API):** `hubuum_events_core::Action` now includes `Purged`.
  Downstream exhaustive matches must handle the new variant. Token purge events
  use this action and identify their retention basis in event metadata.
- **Breaking (Rust API):** `PrincipalTokenMetadata` now includes the
  server-derived `active` and `expired` lifecycle fields. Downstream struct
  literals must initialize both fields; token-management clients should prefer
  them over reproducing expiry rules locally.
- Token retention now starts at the earlier of revocation and effective expiry,
  so explicitly revoked long-lived credentials are purged after the configured
  retention window instead of remaining until their original expiry.
- Added database-owned positive 64-bit revisions to authoritative resources,
  aggregate permission, membership, principal, and token state, temporal
  history, and revision-aware audit events. Revision filtering and sorting are
  available through the shared list-query contract.
- Added strong opaque ETags to canonical entity reads and mutation responses,
  with bounded `If-Match` parsing and lock-protected conditional updates and
  deletes. Stale validators return `412 Precondition Failed` with the stable
  `stale_resource` reason.
- Import v2 collection, class, and object items support `create_only`,
  unconditional `overwrite`, and numeric `if_revision` write conditions that
  are rechecked by queued workers under row locks.
- Revision indexes are built concurrently in a separate non-transactional
  migration so an adjacent-release migration does not take blocking index-build
  locks on live tables.

- **Breaking (HTTP API):** entity JSON now contains `revision`; principal
  settings return `{revision, settings}`, SQL permission reads return
  `{collection_id, revision, permissions}`, and group member lists return
  membership entities with an optional nested principal. Clients must update
  deserializers for these response shapes.
- **Breaking (HTTP API):** class point routes return the class entity by
  default. Use `include=collection` for the expanded, untagged representation.
  Raw object points are tagged; `include=computed` remains expanded and
  untagged.
- **Breaking (HTTP API):** canonical tagged group points omit directory-sync
  timestamps, and canonical tagged token points omit `last_used_at`, because
  those operational fields do not advance resource revisions. The fields
  remain available in untagged group and token lists.
- **Breaking (HTTP API):** canonical tagged user and service-account points use
  stable `identity_scope_id` values and omit independently mutable provider and
  synchronization metadata. Tagged group-membership points omit the expanded
  principal, and SQL permission-set entries expose permission rows with stable
  `group_id` values instead of expanded groups. Rich metadata remains available
  from the corresponding untagged list responses.
- Actor-expanded historical snapshots and shared computed-field mutation
  responses are intentionally untagged because their composite fields do not
  share one revision owner. Fetch the canonical point resource before a later
  conditional mutation.
- **Breaking (computed fields):** mutation bodies and delete queries no longer
  accept `expected_revision`. Obtain the opaque ETag from a computed-field
  point route and send it in `If-Match` instead.
- **Breaking (backup/restore):** full backups are version 4 and restore rejects
  version 3. Version 4 preserves authoritative, authorization-set, event, and
  temporal revisions. Operators must create a new backup before restore and
  quiesce backup, restore, and import operations during the certified adjacent
  API overlap.
- **Breaking (imports):** imports are version 2 and version 1 is rejected.
  Producers must emit the v2 version number and may attach per-item write
  conditions where supported.

- **Breaking (Rust API):** permission-controller group parameters and
  permission-backend collection and group parameters now use validated
  `CollectionID` and `GroupID` values instead of raw integers. Downstream
  callers and backend implementations must construct the newtypes at their
  input boundaries and update their method signatures.
- **Breaking (HTTP API):** token mint requests now reject expirations that are
  not in the future or exceed the new `HUBUUM_MAX_TOKEN_LIFETIME_HOURS` policy
  (default 8760 hours). The client-safe configuration endpoint exposes the
  effective maximum alongside the default lifetime. Before upgrading, clients
  that request longer expirations must shorten them, or operators must raise
  the new limit; no database migration is required.
- The public `CursorPaginated` trait no longer requires row types to implement
  `Clone`; pagination borrows or moves rows and accepts non-clone domain types.
- External identity refresh removes stale group memberships with one set-based
  database write instead of issuing one delete per stale membership.
- **Breaking (Rust API):** Encapsulated event-sink delivery and webhook settings
  fields. Workspace-crate consumers must use `SinkDelivery` accessors and the
  validating `WebhookSinkSettings` constructor and configuration methods.
- **Breaking (Rust API):** `PermissionsList` is now a permission-specific,
  non-generic domain type that canonicalizes duplicate values. Rust callers
  should replace `PermissionsList<Permissions>` with `PermissionsList`.
- **Breaking (Rust API):** event delivery, fan-out, and retention worker
  settings and low-level claim helpers are now crate-private, validated event
  subsystem APIs instead of public database-layer field bags. Downstream
  callers must migrate to the validated APIs and stop invoking the private
  claim helpers. Operators must correct zero, inconsistent, or unrepresentable
  worker-duration values before upgrading; those values now fail startup.
- **Breaking (Rust API):** task-worker settings now use a validating builder,
  and low-level task claim and lease-renewal helpers are crate-private.
  Downstream callers must migrate to the builder and stop invoking the private
  helpers. Operators must correct zero, inconsistent, or unrepresentable
  worker-duration values before upgrading; those values now fail startup.
- **Breaking (Rust API):** `UpdateUser::save`,
  `UpdateUser::save_without_events`, `UpdateGroup::save`, and
  `UpdateGroup::save_without_events` now accept validated `UserID` or
  `GroupID` values instead of raw `i32` identifiers. Library callers must
  construct the matching ID newtype before invoking these domain update
  methods.
- **Breaking Rust API:** `CanUpdate` and its adapter now declare an associated
  `Identifier` type. Built-in collection, class, object, export-template, and
  service-account updates require their validated ID newtypes instead of raw
  `i32` values. Downstream update adapters must declare the associated type,
  and callers must construct the matching ID before invoking `update` or
  `update_without_events`.
- **Breaking (Rust API):** restore confirmation and status functions now
  accept a validated `RestoreJobID` instead of a raw `i64`. Downstream callers
  must construct `RestoreJobID` at their input boundary. Versioned restore
  paths reject non-positive stage IDs at the request boundary.
- **Breaking (Rust API):** `NewCollectionWithAssignee::group_id` and
  `NewServiceAccount::owner_group_id` now use the validated `GroupID` newtype.
  Library callers must construct `GroupID` values for those fields. Collection
  and service-account creation reject non-positive group IDs at the request
  boundary.
- **Breaking (Rust API):** replaced the partial `FilterField::table_field`
  helper, which panicked for non-JSON fields, with the total
  `FilterField::json_column` mapping. Callers must handle its optional result.
- **Breaking (Rust API):** raw dynamic SQL components, bind values, and JSON SQL
  generation helpers are now crate-private. External query integrations should
  use the validated `JsonPredicateExt::as_json_predicate` API instead.
- External permission-backed list filtering now bounds in-process
  authorization working sets and avoids retaining authorized rows outside the
  requested page, reducing peak memory use for large candidate sets.
- Outbound HTTP client construction no longer holds the process-wide client
  cache lock, allowing unrelated first-time integration destinations to
  initialize concurrently.

### Security

- Stable release archives now include CycloneDX SBOMs, signed authoritative
  checksums, and SLSA provenance. Published container platforms include Rust
  and Alpine SBOM data, signed provenance, keyless manifest signatures, and
  blocking scans for fixed HIGH or CRITICAL vulnerabilities. CI also enforces
  Rust advisory, license, immutable-input, and time-bounded exception policies.
- **Breaking (Rust API):** persisted task, event, import-result, and
  export-output rows no longer implement broad Serde or `Debug` traits that
  could expose raw stored content. `TaskRecord` retains redacted debug output
  that hides request, idempotency, token-scope, and worker-lease material.
  Library callers must use the corresponding task response types for public
  payloads.
- **Breaking:** TLS certificate chains, private keys, and PostgreSQL root CA
  bundles must now be regular files and are read with explicit 4 MiB
  certificate and 1 MiB key limits; malformed rustls certificate entries are
  rejected instead of skipped. Before upgrading, replace non-regular TLS files,
  reduce oversized PEM material, and correct malformed certificate entries.
- **Breaking (HTTP API):** Login identity scopes and names are now limited to
  255 characters, and login passwords to 4096 characters. Clients must keep
  those fields within the new limits before upgrading. Oversized requests are
  rejected before rate-limit state or password workers are used, bounding
  unauthenticated limiter-key memory and password-processing work.
- **Breaking (HTTP API):** Common resource queries now reject more than 128
  parameters, 64 filters, or 8 distinct sort fields before building database
  queries. Duplicate sort fields and ambiguous repeated history `at` parameters
  are also rejected. Clients must keep queries within those limits and send one
  value for each sort field and history `at` parameter before upgrading. This
  bounds query-parser allocation and quadratic cursor-predicate growth.
- **Breaking (Unix deployments):** event retention archives now restrict
  existing files to owner-only permissions before appending and reject archive
  paths whose final component is a symbolic link or another non-regular file.
  Operators relying on group-readable archives must move that access behind an
  owner-controlled collector; no database migration is required.
- **Breaking (configuration):** auth provider configuration loading now accepts
  only regular UTF-8 files up to 1 MiB and reports TOML error locations without
  echoing credential-bearing source lines. Deployments must replace larger or
  non-regular sources with a bounded regular file; no database migration is
  required.
- **Breaking:** External identity refresh policy now rejects unrepresentable
  durations and stale windows shorter than the refresh TTL. Before upgrading,
  raise `max_stale_seconds` or lower `refresh_ttl_seconds` wherever the stale
  window is shorter. Future-dated provider sync timestamps also fail closed
  instead of extending cached authorization state.
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
- Debug formatting now redacts authentication credentials, including raw
  logout bearer tokens and stored token digests, along with task request and
  lease data, worker claim tokens, audit-event payloads, outbound HTTP headers,
  URLs and response previews, remote-target configuration and results,
  event-sink configuration and routing, full backup contents, restore
  capabilities and errors, login-limiter connection URLs, and
  credential-bearing connection cache keys.
- Failed restore status responses now retain only public-safe error messages;
  detailed database, connection, hashing, and internal failures remain in
  server logs with the restore job identifier instead of being persisted for
  capability-authenticated clients.
- **Breaking (OpenAPI):** removed the internal `PrincipalToken` storage schema,
  which contains the persisted bearer-token HMAC, from OpenAPI and made the
  storage model non-serializable. Consumers that referenced that unbound schema
  should use the hash-free `PrincipalTokenMetadata` schema returned by token
  listing endpoints.

### Fixed

- Tagged release validation and every downstream publication job now check
  their direct prerequisites explicitly, so skipped non-release jobs do not
  suppress binaries, SBOMs, signed checksums, provenance attestations, or
  multi-architecture container publication. GitHub Release publication waits
  for the successfully attested multi-architecture manifest, and generated
  CycloneDX release SBOMs include the required RFC 4122 document serial. Release
  attestors can also persist GitHub artifact-metadata storage records.
- Conditional collection, class, object, and membership mutations now return
  `412 Precondition Failed` when their selected target vanishes, changes before
  delete validation, or produces a membership-source no-op.
- Import v2 now matches shared computed fields with their null owner, reports
  vanished `if_revision` execution targets as `stale_revision`, and exposes
  observed shared-field revisions during dry runs. Permission-scoped group
  lists now honor the documented `revision` filter.
- In-memory cursor pagination now computes every row's sort values before
  ordering and extracts each value once, so failures cannot silently leave rows
  unordered and fallible work is not repeated inside the sort comparator.
- Export, backup, and staged-restore retention horizons now reject durations or
  expiry timestamps outside the supported range instead of panicking during
  startup or artifact creation.
- Concurrent first deliveries to the same AMQP, email, or Valkey endpoint now
  share one connection initializer instead of opening duplicate connections
  and discarding all but one.
- Event-sink connection caches now retain at most 64 recently used endpoint
  configurations, bounding stale connections and credential-resolved URI keys
  after sink configuration or secret rotation.

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
