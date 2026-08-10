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
| Identity and authorization data | Principals, credentials, memberships, grants, and data needed by configured authorization providers |
| Queries and history | Lists, filtering, stable pagination, search, aggregates, computed enrichment, graphs, and temporal history |
| Workflows | Imports, restores, tasks, backups, exports, remote calls, and their atomic state transitions |
| Operations | Probes, metrics snapshots, retention, event delivery, leases, locking, and worker coordination |

The families are not feature flags and the admin configuration does not report
optional support. Every selectable backend implements the entire list. The
central certification implementations in `src/storage/contract.rs` make adding
a backend an explicit architecture change rather than an incidental trait impl.

PostgreSQL query implementations live in
`src/storage/postgres/operations/*`. Separating their persistence rows from
root domain models is the next extraction layer; the current location is an
implementation detail, not partial backend support. `StorageHandle` selects
one certified PostgreSQL adapter, and only the storage implementation can
recover its pool. Application consumers use `StorageContext`, lifecycle
traits, or the explicit capability facade. No second backend can be added to
composition without implementing every operation behind those contracts.

The storage contract version changes when a required family is added or when
observable semantics change. The selected backend and contract version are
reported in startup logs, process metrics, and the redacted admin configuration.

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
not a storage backend responsibility.

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
   contract descriptor, and exercises the service boundary.

PostgreSQL-specific tests remain responsible for behavior a logical model
cannot reproduce: transactions, rollbacks, isolation, row locks, trigger
serialization, migrations, temporal history, recovery, concurrency, query
budgets, and production feature combinations. The complete repository test
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

- `hubuum-domain` owns maintenance and token-policy values and their validation
  errors without application or persistence dependencies. More domain DTOs
  move here as mixed Diesel/domain models are separated.

- `hubuum-storage-core` owns backend-neutral descriptors, the contract version,
  capability identities, `StorageError`, operational snapshot DTOs, and the
  extracted operational state, event-health, and token-retention traits without
  application, transport, or driver dependencies.
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
  with maintenance and token-policy values; remaining mixed models are an
  incremental extraction.
- `hubuum-storage-core` ultimately owns the complete traits in addition to its
  current errors, descriptors, capability metadata, operational traits, and
  storage DTOs. Behavioral traits that still name root domain types remain in
  `src/storage` until those types move to `hubuum-domain`; the root aggregate
  trait enforces completeness meanwhile.
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
