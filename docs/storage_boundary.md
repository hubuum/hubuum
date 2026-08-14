# Application and Storage Boundary

Hubuum has one application-facing storage boundary. A selectable storage
backend implements that boundary in full; it is not a collection of optional
features.

PostgreSQL is currently the only selectable backend. The in-memory resource
model is a focused test tool, not a partially implemented backend.

## Choose a Reading Path

- Start with this page for the architectural rules and the shape of the system.
- Read the [capability family map](storage_boundary/capability-families.md) to
  understand what the contract contains and how its parts collaborate.
- Read the [backend author guide](storage_boundary/backend-author-guide.md) when
  implementing or evaluating another backend.
- Read the [maintainer guide](storage_boundary/maintainer-guide.md) to find the
  code that owns a behavior and to change the boundary safely.
- Read [testing and compatibility](storage_boundary/testing.md) for the current
  test layers, their strengths, and their known limitations.
- Inspect the machine-checked
  [semantic coverage inventory](storage_boundary/semantic-coverage.toml) for
  the exact trait methods, tracked input variants, and their test evidence.

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

Dependencies point down this diagram. Values returned across the storage
boundary are backend-neutral DTOs. An application consumer must never recover
a pool, receive a Diesel row, build SQL, or handle a driver error.

The boundary has four responsibilities:

1. **The application** owns use cases, public API models, authorization-policy
   selection, validation that is independent of persistence, and conversion
   from `StorageError` to `ApiError`.
2. **The storage contract** owns operation-shaped traits, backend-neutral
   requests and results, and the bounded storage error taxonomy.
3. **The opaque handle** owns exhaustive backend dispatch plus common tracing
   and metrics. Callers do not select an adapter for each operation.
4. **Each adapter** owns persistence rows, queries, transactions, locking,
   driver errors, native notifications, and explicit conversion to contract
   DTOs and `StorageError`.

## Complete Means Complete

`StorageBackend` is an aggregate trait in
`crates/hubuum-storage-core/src/backend.rs`. Its
supertraits are the complete compile-time contract. An adapter opts in with an
explicit implementation only after it implements every required family; Rust
rejects that implementation if any requirement is missing.

The documentation groups those traits into 20 capability families:

- lifecycle and identity foundations;
- permission-aware reads and computed data;
- relations, history, inventory, and search;
- tasks and long-running workflows;
- event administration and event workers; and
- operational state, retention, notifications, and execution context.

These families are a documentation map over one indivisible contract. They are
not a second runtime contract and they are not feature flags.
The [capability family map](storage_boundary/capability-families.md) names every
family, maps it to its required traits, and explains its relationships.

Because adapters are statically linked Rust crates, trait checking and crate
versions are the compatibility mechanism. Hubuum does not maintain a duplicate
runtime contract version or advertise a universal capability list. The
selected backend and its non-sensitive settings are reported through startup
logs, metrics, and the administrator configuration endpoint.

## Services Depend on Exact Operation Families

Collection, class, object, class-relation, and object-relation services use the
specific traits that own their operations:

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

There is no aggregate lifecycle trait and no default "unsupported" behavior.
A focused model implements only the family traits it can perform. It can be
injected into the matching tests, but it cannot satisfy `StorageBackend` and
therefore cannot be selected for the application. Production composition
projects exact observed trait objects from a complete `StorageHandle`.

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

Architecture tests enforce these rules for the current source tree. See the
[maintainer guide](storage_boundary/maintainer-guide.md) for the enforcement
locations and the [testing guide](storage_boundary/testing.md) for what those
tests do and do not prove.

## Current Workspace Shape

Three layers are already distinct:

```text
hubuum application
|-- hubuum-domain
|-- hubuum-storage-core
`-- hubuum-storage-postgres
```

- `hubuum-domain` owns backend-independent validated domain values that have
  been extracted from the root application.
- `hubuum-storage-core` owns extracted storage traits, DTOs, errors, and
  backend identity. It has no Actix, Diesel, global
  configuration, or `ApiError` dependency.
- `hubuum-storage-postgres` owns PostgreSQL pool construction, TLS setup,
  endpoint diagnostics, generated schema, migrations, JSONB validation, and
  query instrumentation.
- The root crate still owns application services, composition, several traits
  whose domain values have not yet been extracted, and most PostgreSQL query
  implementations.

The boundary is enforced today even where physical crate extraction is not yet
complete. Moving code between crates must preserve the dependency direction;
crate placement alone does not define the boundary.

## Current Confidence

The PostgreSQL path is exercised through a real migrated database by shared
backend contracts, PostgreSQL-specific tests, service tests, HTTP integration
tests, destructive restore tests, query-budget tests, platform and feature
builds, production-container tests, and benchmarks.

The contract's methods and selected input variants are inventoried
mechanically, and every registered backend runs compact service, readiness,
and authenticated HTTP point/list scenarios. Adapter-private deterministic
failpoints additionally prove rollback at representative compound-write and
task-state-machine seams.

That is strong practical coverage, but it is not a formal proof of portability:
PostgreSQL is the only complete production adapter, and the compatibility suite
is currently maintained in the root test module rather than consumed as an
independent workspace crate. The [testing guide](storage_boundary/testing.md)
gives the detailed assessment and the most valuable remaining improvements.
