# Application and Storage Boundary

Hubuum has one application-facing storage boundary. A storage backend is a
complete implementation of that boundary, not a collection of optional
features. PostgreSQL is currently the only selectable backend.

The in-memory implementation used by lifecycle tests is deliberately named a
contract model. It is not selectable, is not advertised to administrators, and
does not satisfy the complete `StorageBackend` trait. It may become a backend
only after it implements every required capability family and passes the full
compatibility suite.

The immediate goal is a hard dependency boundary around PostgreSQL and Diesel,
not support for additional production databases.

## Dependency Direction

```text
HTTP / workers / administration
              |
              v
     application use cases
              |
              v
      opaque StorageContext
              |
              v
 complete StorageBackend contract
              |
              v
      PostgreSQL adapter
              |
              v
 Diesel, SQL, pools, transactions
```

Lifecycle services use a narrower internal `LifecycleStorage` view of the
selected complete backend:

```text
CollectionService / ClassService / ObjectService
       / ClassRelationService / ObjectRelationService
                           |
                           v
                    LifecycleStorage
                      /          \
                     v            v
          selected PostgreSQL   memory contract model
                backend             (tests only)
```

This narrower view exists so domain behavior can be tested without pretending
that the test model is a complete application backend. Production composition
uses `DynLifecycleStorage::from_backend`, which requires `StorageBackend`.
`DynLifecycleStorage::new` is reserved for focused contract harnesses.

## Complete Backend Contract

`StorageBackend` is a sealed aggregate trait. A selectable implementation must
satisfy every capability family below before `StorageHandle` can compose it:

| Required family | Contract responsibility |
| --- | --- |
| Domain lifecycle | Collection, class, object, class-relation, and object-relation resolution and lifecycle behavior |
| Catalog queries | Permission- and resource-scoped collection, class, and object filtering, cursor paging, and optional exact counts |
| Computed object queries | Computed filtering and sorting, exact counts, cursor-boundary snapshots, and computed-value enrichment |
| Computed-field lifecycle | Shared and personal definition CRUD, class computation state, rebuild scheduling and claimed execution, and atomic audit behavior |
| Object aggregates | Permission-scoped grouping, numeric measures, stable aggregate cursors, and bounded delegated-policy batching |
| Relation queries | Relation filtering and paging, endpoint-set queries, graph traversal, and export-oriented multi-root expansion |
| Identity and authorization data | Principals, credentials, memberships, single- and multi-collection grant decisions, and class/object projections needed by configured authorization providers |
| Temporal history | Revision-filtered pages, stable cursors, point-in-time reads, visibility pushdown, and provenance-name resolution |
| Unified search | Ranked collection, class, and object search with stable per-kind cursors and token visibility pushdown |
| Remote targets | Point and list reads, atomic audited lifecycle mutations, redacted transport policy, and invocation provenance |
| Task queue | Idempotent task submission, access facts, task/event/result paging, and retained export and backup output reads |
| Task execution | Opaque claims, lease renewal and recovery, claim-checked events and state changes, atomic terminal artifacts, failure accounting, and output retention |
| Backup snapshots | Canonical state and optional history sections read from one consistent backend snapshot |
| Restores | Durable artifact staging, lifecycle transitions, global drain coordination, rollback-safe snapshot replacement, provenance, and recovery |
| Imports | Planning lookups, rollback-only preflight, strict and best-effort application, and durable item results |
| Export queries | Backend-enforced per-read budgets around export selection, includes, and relation hydration |
| Operations | Probes, metrics snapshots, retention, event delivery, locking, and worker coordination |

The families are not feature flags and the admin configuration does not report
optional support. Every selectable backend implements the entire list. The
sealed composition in `src/storage/contract.rs` makes adding a backend an
explicit architecture change rather than an incidental trait implementation.
During extraction, the remaining operational family retains a temporary central
migration gate. That gate prevents another backend from becoming selectable,
but it is not behavioral proof and must be replaced by mandatory operation-shaped
traits and shared tests. The former
identity, catalog-query, relation-query, history, and unified-search gates have
now been replaced by the real `AuthenticationStorage`,
`AuthorizationStorage`, `HistoryStorage`, `CatalogStorage`,
`ComputedObjectStorage`, `ComputedFieldLifecycleStorage`,
`ObjectAggregateStorage`, `RelationQueryStorage`, and `UnifiedSearchStorage`
contracts. `TaskQueueStorage` replaces task submission and reads, while
`TaskExecutionStorage` owns the complete worker claim and state machine.
`BackupSnapshotStorage` owns consistent full-system reads, and computed rebuild
execution is part of `ComputedFieldLifecycleStorage`. `RemoteTargetStorage`
owns target reads, lifecycle mutations, and invocation provenance.
`RestoreStorage` owns the complete staged-restore lifecycle and coordinator.
`ImportStorage` owns the complete import workflow. `ExportQueryStorage` owns the
backend read-budget scope used by selection, includes, and hydration. No family
is considered complete merely because a marker exists.

PostgreSQL query implementations live in
`src/storage/postgres/operations/*`. Separating their persistence rows from
root domain models is the next extraction layer; the current location is an
implementation detail, not partial backend support. `StorageHandle` selects
one certified PostgreSQL adapter, and only the storage implementation can
recover its pool. Application consumers use `StorageContext`, lifecycle
traits, mandatory capability traits, or application services. No second backend can be added to
composition without implementing every operation behind those contracts.

The storage contract version changes when a required family is added or when
observable semantics change. The selected backend and contract version are
reported in startup logs, process metrics, and the redacted admin configuration.

## Export Query Semantics

Every selectable backend implements `ExportQueryStorage`. The application
supplies an optional non-zero `StorageQueryBudget` around each export read stage:
scope selection, related-object includes, and template hydration. It does not
select a database timeout primitive. The adapter evaluates the stage exactly
once and enforces the budget using its native cancellation mechanism; `None`
explicitly disables the export-specific limit.

PostgreSQL implements this contract with a task-local adapter scope that applies
transaction-local `statement_timeout` to every connection or transaction opened
by the stage. That detail is private to the adapter and cannot leak back through
the pool. The shared backend suite verifies the mandatory scope behavior, while
PostgreSQL unit tests verify cancellation and timeout reset on connection reuse.

## Authorization Data Semantics

Every selectable backend implements the complete `AuthorizationStorage` trait.
The application owns token-scope checks, administrator policy, authorization
logging, resource construction, and conversion to `ApiError`. The storage
backend supplies identity facts, local grant mutations and decisions, and the
minimal class/object projections required to construct policy resources.

Multi-collection decisions are one mandatory operation: the backend returns
`true` only when every normalized permission is available on every normalized
collection, including inherited collection grants. This preserves the
all-permissions-on-all-collections rule without exposing joins, closure tables,
or query-builder types to application traits. Resource identifiers and
permission sets are deduplicated at the contract boundary, and DTO debug output
reports bounded counts while redacting identifiers and object names.

`StorageHandle` observes every authorization entry point under bounded
`authorization/*` labels. The shared available-backend suite exercises identity,
membership, resource projections, single and batch decisions, grant lifecycle,
candidate listing, and policy snapshots. Adapter tests remain responsible for
native query and inheritance mechanics.

## Import Semantics

Every selectable backend implements the complete `ImportStorage` trait. Import
support is indivisible: a backend must provide planning lookups for collections,
classes, objects, relations, and groups; rollback-only dry-run preflight; strict
atomic application; best-effort per-item transactions; and durable result
recording. A lookup-only or write-only adapter cannot satisfy `StorageBackend`.

The application owns request validation, authorization decisions, collision
policy, result presentation, and task lifecycle. It produces an exhaustive,
typed `StorageImportOperation` plan and receives indexed preflight or apply
outcomes containing only `StorageError`. The adapter owns runtime reference
resolution, row locking, optimistic-revision rechecks, savepoints, and commit or
rollback behavior. Consequently task planning and execution never receive a
connection or transaction object and cannot select PostgreSQL operations.

The opaque handle observes every import entry point under bounded `imports/*`
labels. Labels contain operation names only; imported identifiers, names,
payloads, and errors are excluded. PostgreSQL-specific tests retain responsibility
for transaction isolation, history triggers, and timestamp behavior, while the
available-backend compatibility registry exercises the mandatory contract.

## Restore Semantics

Every selectable backend implements `RestoreStorage`; restore support cannot be
omitted or advertised as partial. The backend must provide all of these
behaviors:

- durably stage validated artifact bytes, a document digest, redacted initiator
  identity, validation results, an expiry, and a capability proof;
- expose a complete artifact only to confirmation and recovery paths while
  providing a document-free status projection for ordinary status reads;
- use compare-and-set lifecycle transitions so expiration and confirmation
  cannot both win;
- atomically couple confirmation to global draining maintenance ownership;
- publish coordinator heartbeats against the maintenance generation and mark an
  instance drained only after the backend has observed non-normal maintenance
  and application-local work is idle;
- re-check drain ownership before destructive work, then replace canonical
  state, reset backend-owned identifiers and derived state, emit restore
  provenance, resume operation, and clear staging in one rollback-safe apply;
- erase artifact bytes when jobs fail or expire; and
- recover orphaned, terminal, and interrupted maintenance states idempotently.

The application validates and decodes Hubuum's backup document. It passes
backend-neutral `StorageRestore*` DTOs through `RestoreStorage`; it does not
construct SQL, handle Diesel records, manage a PostgreSQL transaction, or emit
the backend-owned success event. `StorageHandle` observes every contract entry
under the bounded `restores/*` capability labels. The PostgreSQL adapter keeps
the narrower database-operation attribution used by the restore coordinator.

The shared available-backend suite verifies staging, safe projections,
expiration, failure cleanup, recovery idempotence, heartbeat publication, live
instance filtering, and membership removal for every selectable backend. The
isolated destructive restore round-trip additionally proves drain, atomic
apply, provenance, state replacement, and restart recovery. Adapter unit tests
cover PostgreSQL transaction/query mechanics.

## Lifecycle Semantics

The currently shared lifecycle contract covers the following behavior.

Collections provide:

- point reads;
- create with an initial assignee grant and atomic lifecycle event;
- update and revision-preserving no-op behavior;
- delete constraints;
- direct children and ordered ancestors; and
- hierarchy moves with sibling-scoped name uniqueness.

Classes provide:

- point resolution by ID or name;
- create with schema validation and an atomic lifecycle event;
- update, no-op behavior, and collection moves;
- stale selector rejection; and
- delete with lifecycle and cascade semantics.

Objects provide:

- point resolution by ID or name within a class;
- create and update with JSON Schema validation;
- bounded, atomic JSON Patch and revision-preserving no-ops;
- stale selector rejection; and
- delete with lifecycle and cascade semantics.

Class and object relations provide:

- endpoint preparation before authorization;
- resolution with the endpoint aggregates required by policy checks;
- stale endpoint rejection between authorization and mutation;
- normalized direction, aliases, duplicates, and cardinality semantics;
- atomic lifecycle events; and
- the documented object, class, collection, and relation cascades.

Capability methods are use-case or aggregate shaped. Implementations own
transactions, batching, locking, hierarchy maintenance, initial grants,
cardinality enforcement, and atomic event persistence. The contract must not
devolve into table repositories or expose query builders.

Inputs and results crossing the boundary are storage-owned, backend-neutral
DTOs. They may contain domain types, but they do not derive Diesel traits or
expose SQL rows, query builders, connections, pools, or driver errors. Each
adapter keeps its persistence rows private and explicitly converts them into
the contract DTOs. Metrics use this pattern today: `MetricsStorage` returns
neutral inventory, task, event, and pool snapshots while PostgreSQL keeps its
queryable row structs inside the adapter. `OperationalStateStorage` does the
same for readiness and maintenance state, and `TokenRetentionStorage` accepts
validated retention settings without exposing the transaction, advisory lock,
or SQL cutoffs. `EventHealthStorage` returns only persisted queue and claim
state. The application adds worker configuration and in-process wake-up
counters when projecting that snapshot into its API response; those values are
not a storage backend responsibility. `EventFanoutStorage` owns the complete
claim, subscription-match, delivery insertion, claim release, and notification
operation. Its caller supplies a validated `EventFanoutSettings` policy and
receives only the processed count. `EventDeliveryStorage` owns due-work
selection, locking, legacy provenance enrichment, principal resolution, and
claim-checked acknowledgements. The delivery worker receives only a complete
`EventDeliveryWorkItem`: an event envelope, transport routing, redacted-debug
sink settings, and an opaque claim to return on success or failure.
`EventRetentionStorage` similarly owns worker coordination, selection, and the
purge transaction. It passes redacted-debug, serialized `RetainedEvent` DTOs to
an application-owned `EventArchive`; an archive failure rolls back deletion.
Neither side sees the PostgreSQL event row, connection, or claim state.

`AuthenticationStorage` owns the consistent principal/human join and token
scope reads used by bearer-token extractors. It returns a minimal
`AuthenticationPrincipal`, an optional password-free `AuthenticationHuman`,
and scope DTOs that distinguish a disabled dimension from an enabled empty
deny-all dimension. PostgreSQL kind strings, credential hashes, Diesel rows,
and scope-table layouts never cross into request handling. The opaque
`StorageHandle` applies the common `authentication` tracing and metric labels
before dispatching to the selected adapter.

`AuthorizationStorage` supplies neutral principal membership facts and the
complete local-policy-store surface: collection decisions, reverse collection
queries, paginated grant reads, and grant mutations. The application-facing
`AuthzSubject`, `PrincipalRef`, and `LocalPermissionBackend` consume that
contract; they do not construct Diesel queries, import schema modules, or
recover a PostgreSQL pool. Local grant rows cross the boundary as compact
permission-set DTOs and are converted into API models only in the permission
application layer. The opaque handle observes every entry point with bounded
`authorization/*` operation labels.

External policy evaluation uses the same boundary. Candidate collection and
group reads, along with complete local-policy snapshots used for Cedar export,
are mandatory authorization operations rather than connection recovery paths.
Consequently the complete `src/permissions` application tree is independent of
PostgreSQL, Diesel, generated schema modules, and connection pools; a source
guard enforces that boundary.

`HistoryStorage` owns every temporal-history read exposed by the HTTP API:
collection, class, object, export-template, and remote-target pages and
point-in-time lookups, plus batched principal-name resolution for provenance.
Visibility is an owned backend-neutral request, counting happens under the same
visibility predicate as page selection, and adapter rows are converted to
private-field DTOs before crossing the boundary. `StorageHandle` observes these
calls with bounded `history/*` labels. The application history service converts
DTOs into response models and is the only layer that maps `StorageError` into
`ApiError`.

`UnifiedSearchStorage` owns all three ranked search projections as one complete
capability: collections, classes with their collection projection, and objects.
The request owns decoded cursor state, query options, administrator status, and
independent token permission/resource dimensions. The PostgreSQL adapter maps
those DTOs to its private query implementation and returns private-field DTOs;
the application service alone reconstructs API/domain models. Each entry point
is observed with a bounded `unified_search/{collections,classes,objects}` label,
and the available-backend compatibility test exercises every operation with
real matching rows.

`RemoteTargetStorage` owns remote-target point and list reads, atomic audited
create/update/delete behavior, and invocation provenance. Transport templates,
authentication configuration, and subject policy cross the boundary only in
private-field, redacted-debug storage DTOs; Diesel rows stay in the PostgreSQL
adapter. The application service performs public-model validation and converts
between API/domain models and storage DTOs. Handlers and remote-call workers do
not import adapter operations or recover a pool. Every entry point is observed
under bounded `remote_targets/*` labels, and the available-backend compatibility
test exercises all six operations.

`CatalogStorage` owns the ordinary collection, class, and object query surface.
Each operation applies backend-neutral filters, stable cursor state, local
permission visibility, token permission/resource dimensions, page selection,
and an optional exact total. The application prepares public cursor semantics
and converts the private-field storage projections into domain/API models. For
external policy backends it requests an unpaged candidate set through the same
contract, performs policy authorization in the application, and only then
paginates. HTTP handlers, computed-list visibility checks, and export hydration
therefore share one boundary instead of calling PostgreSQL query traits. The
opaque handle observes the three operations with bounded
`catalog/{collections,classes,objects}` labels.

`ComputedObjectStorage` owns computed object filtering, sorting, optional exact
counts, and enrichment. Query snapshots, materialization rows, generated SQL,
and Diesel models remain adapter-private. The request carries either ordinary
storage visibility or object identifiers already authorized by an external
policy backend. The result returns private-field object and computed-scope DTOs
plus resolved backend-neutral query metadata needed to encode stable computed
cursors. The application converts those DTOs into API responses. The opaque
handle observes the complete capability with bounded
`computed_objects/{list,enrich}` labels, and the available-backend test invokes
both operations.

`ComputedFieldLifecycleStorage` owns the complete application-facing
definition lifecycle: class state, shared and personal listing, point lookup,
shared and personal create/update/delete, explicit rebuild scheduling, and
execution of the backend-owned rebuild workflow under an opaque task lease.
Requests and results cross the boundary as private-field DTOs; persistence
rows, visibility strings, revisions, Diesel transactions, audit inserts, task
cancellation, and rebuild enqueueing remain PostgreSQL adapter details. The
application service validates API models, converts DTOs into response models,
and owns the pure preview evaluator. Every opaque-handle entry point uses a
bounded `computed_fields/*` observation label. Each rebuild batch validates and
locks the live task lease in the same transaction as its materialization
writes, so a stale worker cannot commit computed data after losing its claim.

`TaskExecutionStorage` owns every persistence transition needed by task
workers. Claims cross the boundary as a task DTO plus an opaque, redacted token;
the application can only return that token for renewal, events, state updates,
completion, or failure. PostgreSQL's UUID representation, lease SQL, durable
timestamps, row locks, task/output insert records, and workflow result counting
remain adapter-private. Completion stores an optional export, backup, or
remote-call artifact and the terminal lifecycle event in the same backend
operation. Failure counts
are derived by the backend so the application never queries workflow result
tables. The opaque handle observes all nine entry points with bounded
`task_execution/*` labels, and every selectable backend must pass the shared
state-machine compatibility test.

`BackupSnapshotStorage` projects live backend state into Hubuum's canonical,
versioned backup document sections. The application owns document metadata,
serialization, hashing, and retention artifacts; adapters own consistent reads
and the mapping from their private representation into those logical sections.
Both state-only and history-inclusive snapshots cross private-field DTOs and
are observed with the bounded `backup_snapshots/snapshot` label.

`ObjectAggregateStorage` owns filtered grouping, numeric measures, computed
aggregate snapshots, exact group counts, and stable aggregate cursors. The
request uses private-field target and specification DTOs plus the same neutral
visibility descriptor used by other reads. When authorization can be pushed
into storage, the backend performs the complete operation directly. For an
external policy engine, the application supplies an `ObjectAggregateAuthorizer`
implementation that receives only storage-owned target and candidate DTOs;
the backend retains bounded candidate paging, computed evaluation, and
accumulation while the application retains authorization decisions. This
avoids collecting every authorized object identifier in memory and keeps the
concrete permission and PostgreSQL implementations on their respective sides
of the boundary. The opaque handle observes the operation with the bounded
`object_aggregates/aggregate` label, and the available-backend compatibility
test exercises both storage-pushdown and delegated authorization modes.

`RelationQueryStorage` owns the complete relation read surface rather than only
ordinary relation lists. Its mandatory operations cover class and object
relation filtering and counts, direct relations touching one endpoint,
relations touching or contained within endpoint sets, bounded object-relation
frontier reads with explicit exclusions, related-class and related-object graph
pages, and both directional and bidirectional multi-root
expansion used by exports. Graph DTOs are composed from storage-owned class and
object projections, so Diesel query rows and SQL traversal functions remain
adapter-private. Alternative-path preservation is an explicit backend-neutral
request semantic used when external policy authorization must remove graph
edges before canonical paths and limits are selected. The opaque handle
observes every operation under bounded `relations/*` labels, and the shared
available-backend compatibility test exercises all twelve operations.

`TaskQueueStorage` owns the complete application-facing task queue surface:
idempotent submission under active-task limits, task access facts, filtered
task pages, lifecycle events, import item results, and retained export and
backup outputs. The PostgreSQL adapter converts persistence records and legacy
event provenance into private-field storage DTOs. The application task service
owns authorization decisions, principal-name resolution, and API/domain model
conversion; handlers do not call task query traits or receive Diesel records.
All eleven entry points are observed under bounded `tasks/*` labels, and the
available-backend compatibility test invokes every operation. Claiming,
leasing, terminal transitions, and workflow-specific atomic writes remain a
separate mandatory extraction rather than optional backend behavior.

## Error Direction

Errors cross the boundary in one direction:

```text
PostgresStorageError / contract-model error
                    |
                    v
              StorageError
                    |
                    v
                 ApiError
```

Each adapter owns its implementation error and converts it to the bounded
`StorageErrorKind` taxonomy at the adapter edge. Backend-neutral storage code
does not import `ApiError`. The application error layer alone converts
`StorageError` into the public `ApiError` response surface. There is no reverse
application-error-to-storage-error conversion.

Expected domain outcomes such as not found, conflict, validation, and stale
preconditions retain their useful public classification. Database, unavailable,
and internal failures retain diagnostic detail for logs while public HTTP
responses use the existing safe generic messages.

## Observability Contract

Every migrated storage call is wrapped outside the implementation, so backends
cannot silently omit common diagnostics. Lifecycle services use the observed
storage wrapper; opaque-handle metrics and operational calls use the same
observation function. It provides:

- a `storage_operation` tracing span with bounded `backend`, `capability`, and
  `operation` fields;
- a `storage operation complete` debug event on success;
- a debug rejection event for expected domain failures;
- a warning event for database, unavailable, and internal failures; and
- backend-neutral duration and error metrics with the same bounded labels.

The PostgreSQL connection and transaction helpers provide the equivalent
completion and failure logs for capability families still backed by legacy
operation-shaped adapters. Their existing low-cardinality database metrics
remain the implementation-level view. Storage metrics describe logical calls;
database metrics describe pool and transaction behavior. Operators should use
both rather than deriving one from the other.

Logs and metric labels must never contain entity IDs, user-controlled names,
queries, URLs, credentials, or payloads. Detailed mutation audit logs remain
commit-aware and separate from diagnostic backend events.

## Administrator Configuration

`GET /api/v1/admin/config` reports the effective non-secret storage selection:

- backend name;
- storage contract version;
- the complete required capability-family list;
- whether a connection URL is configured;
- pool size;
- pool acquisition timeout; and
- statement timeout.

The connection URL and credentials are never returned. Startup metadata and
`hubuum_storage_backend_info` report the same backend identity and contract
version so configuration, logs, and metrics cannot disagree about composition.

## Compatibility and Backend-Specific Tests

There are two complementary test layers:

1. Shared lifecycle contract tests run the same focused behaviors against the
   PostgreSQL adapter and the in-memory contract model.
2. The available-backend compatibility registry iterates every selectable
   `StorageBackendKind`, composes it through `StorageHandle`, verifies its
   contract descriptor, and exercises every mandatory operation family. The
   temporal-history contract test covers all list, point-in-time, visibility,
   and provenance-resolution entry points; the computed-object contract covers
   query and enrichment entry points; the computed-field lifecycle contract
   covers every shared and personal definition, state, and rebuild entry point;
   and the object-aggregate contract covers both storage-pushdown and delegated
   policy execution.

PostgreSQL-specific tests remain responsible for behavior a logical model
cannot reproduce: transactions, rollbacks, isolation, row locks, trigger
serialization, migrations, temporal trigger semantics, recovery, concurrency,
query budgets, and production feature combinations. The complete repository test
suite is therefore part of PostgreSQL backend certification, not a substitute
for the shared logical contracts.

Adding another selectable backend requires all of the following in one change:

1. Implement every capability family in `StorageBackend`.
2. Add the backend to the sealed certification module and
   `StorageBackendKind::ALL`.
3. Add an exhaustive `StorageHandle` composition variant without a fallback.
4. Run every shared compatibility contract against the implementation.
5. Add backend-specific unit and integration coverage for its native failure,
   consistency, concurrency, and recovery behavior.
6. Provide adapter-owned error conversion into `StorageError`.
7. Verify the common tracing and metric wrapper labels.
8. Add a redacted administrator configuration projection for its settings.
9. Preserve the PostgreSQL integration and query-budget suite unless
   PostgreSQL is deliberately removed as an available backend.

If any item is absent, the implementation is a test model or internal adapter,
not an available storage backend.

## Workspace Crates and Extraction Path

The first workspace boundaries are now in place:

- `hubuum-domain` owns maintenance, token-policy, and event-worker policy values
  and their validation errors without application or persistence dependencies.
  More domain DTOs move here as mixed Diesel/domain models are separated.

- `hubuum-storage-core` owns backend-neutral descriptors, the contract version,
  capability identities, `StorageError`, authentication and authorization
  DTOs, operational snapshot DTOs, and the extracted authentication,
  authorization, catalog-query, temporal-history, unified-search, operational
  state, computed-object, computed-field lifecycle, object-aggregate, task-queue,
  task-execution, backup-snapshot, remote-target, relation-query, event-health,
  event-fan-out, event-retention, and token-retention traits without application,
  transport, or driver dependencies.
- `hubuum-storage-postgres` owns PostgreSQL pool construction, TLS connection
  setup, safe endpoint diagnostics, JSONB validation, query capture, and its
  crate-owned pool-construction error.

The remaining target dependency graph is:

```text
root hubuum application
|-- depends on --> hubuum-domain
|-- depends on --> hubuum-storage-core
|                    `-- depends on --> hubuum-domain
`-- depends on --> hubuum-storage-postgres
                     |-- depends on --> hubuum-storage-core
                     `-- depends on --> hubuum-domain

hubuum-storage-contract-tests
|-- depends on --> hubuum-storage-core
`-- exercised with --> each backend adapter
```

No workspace crate depends on the root application package.

The crates have deliberately different responsibilities:

- `hubuum-domain` owns validated identifiers, commands, aggregates, and result
  types without Diesel, Actix, global configuration, or `ApiError`. It starts
  with maintenance, token-policy, and event-worker policy values; remaining
  mixed models are an incremental extraction.
- `hubuum-storage-core` ultimately owns the complete traits in addition to its
  current errors, descriptors, capability metadata, operational traits, and
  storage DTOs. Its authentication and authorization contracts use only
  backend-neutral DTOs and are mandatory in the root aggregate trait.
  Behavioral traits that still name root domain types remain in `src/storage`
  until those types move to `hubuum-domain`; the root aggregate trait enforces
  completeness meanwhile.
- `hubuum-storage-postgres` currently owns pool and TLS setup, JSONB helpers,
  and query capture. It ultimately owns generated schema, migrations,
  transaction helpers, persistence rows, PostgreSQL queries, and
  `PostgresStorageError` as domain types are separated.
- `hubuum-storage-contract-tests` supplies the reusable compatibility suite and
  focused logical model. Every available adapter runs that suite; each adapter
  also retains native unit and integration tests.
- The root package owns application services and composition, administrator
  configuration projection, Actix/OpenAPI types, and `StorageError` to
  `ApiError` conversion.

Moving Diesel first without separating types would either make the adapter
depend on the root package or move application and HTTP concerns into the
adapter. Both create the wrong dependency direction. Existing structs that
combine domain behavior with `Queryable`, `Insertable`, schema references, or
SQL methods therefore need to split into backend-neutral types and private
PostgreSQL row types at the adapter edge.

A safe continuation stack is:

1. Continue extracting backend-neutral domain command/result types and add
   explicit conversions to private PostgreSQL rows.
2. Move domain-typed behavioral contracts and the compatibility harness into
   `hubuum-storage-core` and `hubuum-storage-contract-tests`.
3. Move the remaining transaction boundary, queries, and adapter errors into
   `hubuum-storage-postgres`.
4. Move `src/schema.rs` and `migrations/` together so generated schema,
   embedded migrations, and production queries stay owned by one crate.
5. Forward root package features such as embedded migrations and TLS to the
   adapter crate, and update the CLI migration entrypoint to call its narrow
   migration API.
6. Update the Docker manifest-copy stage, CI change classifier, Rust API policy,
   and production container build in the same stack layers that add the crates.

The pool and TLS runtime is already extracted behind crate-owned settings and
errors. Its concrete pool and connection types are an adapter integration
surface used by composition and `src/storage/postgres`; application consumers
must not import them. Subsequent query extraction replaces that transitional
integration surface with adapter-owned operation APIs.

## Boundary Enforcement

Architecture tests enforce that:

- handlers, services, extractors, middleware, and metrics scraping do not
  import Diesel, `PostgresPool`, transaction helpers, or database
  implementation modules;
- backend-neutral services and storage contracts do not import PostgreSQL or
  application API errors;
- storage contract DTOs do not derive Diesel traits or expose adapter types;
- only adapter modules translate implementation errors to `StorageError`;
- the application error layer owns `StorageError` to `ApiError` conversion;
- `AppContext` owns an opaque `StorageHandle` and composes services from a
  complete backend;
- the complete backend trait contains every required capability family; and
- the memory contract model cannot be certified or selected as a full backend.

Direct persistence APIs remain internal implementation machinery for adapters,
administrative workflows, and fixtures. They are not an application-facing
escape hatch.
