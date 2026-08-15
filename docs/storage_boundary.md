# Application and Storage Boundary

Hubuum has one application-facing storage boundary. A selectable storage backend implements that boundary in full; it is not a collection of optional features.

PostgreSQL is currently the only selectable backend. The in-memory resource model is a focused test tool, not a partially implemented backend.

## Choose a Reading Path

- Start here for the invariants and the overall shape.
- Use the [capability family map](storage_boundary/capability-families.md) to find the trait that owns an operation and the families it collaborates with.
- Use the [backend author guide](storage_boundary/backend-author-guide.md) to implement or evaluate a backend.
- Use the [maintainer guide](storage_boundary/maintainer-guide.md) to trace a call, locate its implementation, and change the boundary safely.
- Use [testing and compatibility](storage_boundary/testing.md) to understand what the test layers prove and where confidence remains limited.
- Inspect the machine-checked [semantic coverage inventory](storage_boundary/semantic-coverage.toml) for the exact methods, tracked input variants, and test evidence.

## The Boundary in One Page

```text
HTTP handlers / workers / administration
                    |
                    v
        application services and policy
                    |
                    v
            opaque StorageContext
                    |
                    v
             StorageHandle
        common dispatch + observation
                    |
                    v
          complete StorageBackend
                    |
                    v
           PostgreSQL adapter
                    |
                    v
     Diesel / SQL / pools / transactions
```

Dependencies point down the diagram. Calls and errors return upward through the same layers.

Values crossing the boundary are backend-neutral DTOs. An application consumer must never recover a pool, receive a Diesel row, build SQL, or handle a driver error.

The boundary has four responsibilities:

1. **The application** owns use cases, public API models, authorization-policy selection, persistence-independent validation, and conversion from `StorageError` to `ApiError`.
2. **The storage contract** owns operation-shaped traits, backend-neutral requests and results, and the bounded storage error taxonomy.
3. **The opaque handle** owns exhaustive backend dispatch plus common tracing and metrics. Callers do not select an adapter for each operation.
4. **Each adapter** owns persistence rows, queries, transactions, locking, driver errors, native notifications, and explicit conversion to contract DTOs and `StorageError`.

## Complete Means Complete

`StorageBackend` is the aggregate trait in `crates/hubuum-storage-core/src/backend.rs`. Its supertraits are the complete compile-time contract.

An adapter opts in explicitly only after it implements every required family. Rust rejects the implementation if any method or family is missing.

The documentation groups those traits into 20 capability families:

- lifecycle and identity foundations;
- permission-aware reads and computed data;
- relations, history, inventory, and search;
- tasks and long-running workflows;
- event administration and event workers; and
- operational state, retention, notifications, and execution context.

These families are a documentation map over one indivisible contract. They are neither a second runtime contract nor feature flags. The [capability family map](storage_boundary/capability-families.md) names every family, maps it to its required traits, and explains its relationships.

Adapters are statically linked Rust crates. Trait checking and crate versions are therefore the compatibility mechanism; Hubuum has no duplicate runtime contract version or dynamic capability negotiation.

Startup logs, metrics, and the administrator configuration endpoint report the selected backend and the same non-sensitive effective settings.

## Services Depend on Exact Operation Families

Collection, class, object, class-relation, and object-relation services use the specific traits that own their operations:

```text
CollectionService -------> CollectionStore
ClassService ------------> ClassStore
ObjectService -----------> ObjectStore
ClassRelationService ----> ClassRelationStore
ObjectRelationService ---> ObjectRelationStore
                                ^
                                |
                     PostgreSQL or focused model
```

There is no aggregate lifecycle trait and no default "unsupported" behavior. A focused model implements only the family traits it can perform. Tests may inject it through those traits, but it cannot satisfy `StorageBackend` and cannot be selected for the application.

Production composition projects exact observed trait objects from a complete `StorageHandle`.

## Boundary Rules

The following rules are architectural invariants:

- Application code accepts `StorageContext`, `AuthorizationContext`, an
  application service, or a backend-neutral capability trait.
- Only an authorization-aware context may select the configured permission
  backend. A storage handle cannot silently substitute local authorization for
  an external policy backend.
- Adapter inputs and results are crate-owned or storage-owned DTOs with private
  representation where practical.
- Multi-step atomic behavior crosses the boundary as one operation. The
  adapter, not the caller, owns its transaction.
- Native mechanisms such as SQL cursors, statement timeouts, advisory locks,
  task-local database settings, and notification listeners remain private to
  the adapter.
- Backend errors move upward exactly once:

  ```text
  adapter error -> StorageError -> ApiError
  ```

- Every storage call uses bounded, static capability and operation labels.
  Entity IDs, names, queries, URLs, credentials, and payloads never become log
  or metric labels.
- A backend is selectable only after it passes the shared compatibility suite
  and its own native consistency, concurrency, recovery, and failure tests.

Architecture tests enforce these rules for the current source tree. The [maintainer guide](storage_boundary/maintainer-guide.md) locates those checks; the [testing guide](storage_boundary/testing.md) explains what they do and do not prove.

## Current Workspace Shape

Three layers are already distinct:

```text
hubuum application
|-- hubuum-domain
|-- hubuum-storage-core
`-- hubuum-storage-postgres
```

- `hubuum-domain` owns backend-independent validated domain values extracted from the root application.
- `hubuum-storage-core` owns the complete storage contract, DTOs, errors, cursors, and backend identity. It has no Actix, Diesel, global configuration, or `ApiError` dependency.
- `hubuum-storage-postgres` owns the native pool, TLS, endpoint diagnostics, generated schema, migrations, JSONB validation, query instrumentation, and all production PostgreSQL operations.
- The root crate owns application services and static composition. It constructs the PostgreSQL adapter with telemetry and dedicated operational pools, then places it behind the opaque handle.
- The root crate retains a legacy PostgreSQL row-and-SQL harness for old tests. It is compiled only for unit tests or the explicit `integration-test-support` feature and is not part of a production build.

The backend-neutral contracts needed by an out-of-tree adapter are publishable crates. Backend registration remains explicit, exhaustive, and application-owned. An adapter may therefore be supplied by a crates.io, Git, or path dependency. Hubuum does not load storage plugins dynamically.

Moving a file does not by itself improve the boundary. Dependencies must continue to point from the application to contracts and from adapters to contracts, never from a contract or adapter back into the application.

## Current Confidence

The PostgreSQL path is exercised against a real migrated database by shared backend contracts, PostgreSQL-specific tests, service tests, HTTP integration tests, destructive restore tests, query-budget tests, platform and feature builds, production-container tests, and benchmarks. CI also migrates representative data from the adjacent stable release, starts the new application, and restarts the previous application against the migrated schema.

The contract's methods and selected input variants are inventoried mechanically. Every registered backend runs compact service, readiness, and authenticated HTTP point/list scenarios. Adapter-private deterministic failpoints prove rollback at representative compound-write and task-state-machine seams.

This is strong practical coverage, not a formal proof of portability. PostgreSQL is the only complete production adapter, and the compatibility suite still lives in the root test module instead of an independently consumable contract-test crate.

The [testing guide](storage_boundary/testing.md) gives the detailed assessment and the highest-value remaining improvements.
