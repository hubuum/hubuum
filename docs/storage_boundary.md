# Application and Storage Boundary

Hubuum has one application-facing storage boundary. A selectable storage backend implements that boundary in full; it is not a collection of optional features.

PostgreSQL is currently the only selectable backend. The in-memory resource model is a focused test tool, not a partially implemented backend.

## Choose a Reading Path

- Start here for the invariants and the overall shape.
- Read the normative [storage contract](storage_boundary/contract.md) for the
  guarantees every selectable backend and application caller must preserve.
- Use the [capability family map](storage_boundary/capability-families.md) to find the trait that owns an operation and the families it collaborates with.
- Use the [backend author guide](storage_boundary/backend-author-guide.md) to implement or evaluate a backend.
- Use the [maintainer guide](storage_boundary/maintainer-guide.md) to trace a call, locate its implementation, and change the boundary safely.
- Use [transactions and side effects](storage_boundary/transactions-and-events.md) when a use case spans several resource operations or must define audit behavior.
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
       | operation traits         |
       | audited StorageTransaction
                    |
                    v
           PostgreSQL adapter
                    |
                    v
     Diesel / SQL / pools / transactions
```

Dependencies point down the diagram. Calls and errors return upward through the same layers.

Values crossing the boundary are backend-neutral DTOs. An application consumer must never recover a pool, receive a Diesel row, build SQL, or handle a driver error.

The boundary has five responsibilities:

1. **The application** owns use cases, public API models, authorization-policy selection, persistence-independent validation, and conversion from `StorageError` to `ApiError`.
2. **The storage contract** owns operation-shaped traits, composable transaction-scoped resource APIs, backend-neutral requests and results, and the bounded storage error taxonomy.
3. **The opaque handle** owns exhaustive backend dispatch plus common tracing and metrics. Callers do not select an adapter for each operation.
4. **Each adapter** owns persistence rows, queries, native transactions, locking, driver errors, notifications, and explicit conversion to contract DTOs and `StorageError`.
5. **Certification** combines reusable semantic conformance, complete-backend
   compatibility, native failure tests, and a sealed application registry.
   Implementing the Rust trait shapes alone does not make an adapter selectable.

## Complete Means Complete

`StorageBackend` is the aggregate trait in `crates/hubuum-storage-core/src/backend.rs`. Its supertraits are the complete compile-time contract, including the mandatory `TransactionStorage` unit-of-work capability.

An adapter opts in structurally only after it implements every required family.
Rust rejects the implementation if any method or family is missing. The
application then admits it through the sealed `CertifiedStorageBackend`
registry only after its shared and native behavioral evidence passes.

The documentation groups those traits into 20 capability families:

- lifecycle and identity foundations;
- permission-aware reads and computed data;
- relations, history, inventory, and search;
- tasks and long-running workflows;
- event administration and event workers; and
- operational state, retention, notifications, and execution context.

These families are a documentation map over one indivisible contract. They are neither a second runtime contract nor feature flags. The [capability family map](storage_boundary/capability-families.md) names every family, maps it to its required traits, and explains its relationships.

Adapters are statically linked Rust crates. Trait checking and crate versions are therefore the compatibility mechanism; Hubuum has no duplicate runtime contract version or dynamic capability negotiation.

The server and administrator CLI select one registered adapter through the
typed `HUBUUM_STORAGE_BACKEND` setting. The only current value is
`postgresql`; an empty value selects that default, while every other unknown
value fails configuration parsing. Startup logs, metrics, and the administrator
configuration endpoint report the selected backend and the same non-sensitive
effective settings.

## Services Depend on Exact Operation Families

Collection, class, object, class-relation, and object-relation services use the specific traits that own their operations:

```text
CollectionService -------> CollectionStorage
ClassService ------------> ClassStorage
ObjectService -----------> ObjectStorage
ClassRelationService ----> ClassRelationStorage
ObjectRelationService ---> ObjectRelationStorage
                                ^
                                |
                     PostgreSQL or focused model
```

There is no aggregate lifecycle trait and no default "unsupported" behavior. A focused model implements only the family traits it can perform. Tests may inject it through those traits, but it cannot satisfy `StorageBackend` and cannot be selected for the application.

Production composition projects exact observed trait objects from a complete `StorageHandle`.

Application workflows that must compose several resource mutations use
`TransactionStorage`. The callback receives an opaque `StorageTransaction`
whose `collections()`, `classes()`, `class_relations()`, `objects()`, and
`object_relations()` accessors return discoverable operation types. Every
transactional mutation inherits one required `EventContext`.

## Boundary Rules

The following rules are architectural invariants:

- Application code accepts `StorageContext`, `AuthorizationContext`, an
  application service, or a backend-neutral capability trait.
- Only an authorization-aware context may select the configured permission
  backend. A storage handle cannot silently substitute local authorization for
  an external policy backend.
- Adapter inputs and results are crate-owned or storage-owned DTOs with private
  representation where practical.
- A use case may compose safe resource primitives through
  `TransactionStorage`. The adapter owns the native transaction and exposes
  neither a connection nor a query language.
- Invariant-heavy state machines remain one operation-shaped trait method.
  Task completion, restore application, retention, permission mutation, and
  similar workflows must not be reconstructed from lower-level calls.
- Transaction-scoped mutations always inherit the transaction's
  `EventContext`. State and durable audit events commit or roll back together.
- Ordinary audited mutations require `EventContext` and return
  `MutationOutcome`: committed changes carry a durable `AuditReceipt`, while
  genuine no-ops carry no receipt and append no event.
- Imports and restores are restricted to the explicit `ImportStorage` and
  `RestoreStorage` capabilities. Those typed surfaces preserve or reconstruct
  history and are not unaudited shortcuts for ordinary writes.
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
  and six-part conformance verifier plus its own native consistency,
  concurrency, recovery, and failure tests.

Architecture tests enforce these rules for the current source tree. The [maintainer guide](storage_boundary/maintainer-guide.md) locates those checks; the [testing guide](storage_boundary/testing.md) explains what they do and do not prove.

The [transactions and side-effects guide](storage_boundary/transactions-and-events.md)
defines the decision rule, event guarantee, cancellation semantics, and
adapter obligations in detail.

## Current Workspace Shape

The application, reusable contracts, and native adapter are distinct:

```text
hubuum application
|-- hubuum-domain
|-- hubuum-query
|-- hubuum-events-core
|-- hubuum-task-core
|-- hubuum-storage-core
|-- hubuum-storage-conformance
`-- hubuum-storage-postgres
```

- `hubuum-domain` owns backend-independent validated identifiers, revisions,
  patches, and policy values extracted from the root application.
- `hubuum-query` owns bounded query options, filters, sorts, cursors, scalar
  inference, and parsing. It describes query intent and does not expose SQL or
  database type names.
- `hubuum-events-core` owns typed event identity, envelopes, mutation
  provenance, and event integration traits. Its public identifiers reuse
  `hubuum-domain` newtypes instead of raw database integers.
- `hubuum-task-core` owns task values shared by storage and worker code.
- `hubuum-storage-core` owns the complete storage contract, private-field DTOs,
  and semantic errors. It has no Actix, Diesel, global
  configuration, or `ApiError` dependency. Resource lifecycle, revision,
  metadata, and principal boundaries use domain IDs and revisions rather than
  persistence-shaped strings and integers.
- `hubuum-storage-conformance` owns the reusable six-part behavioral verifier
  for receipts, no-ops, rollback, fan-out to a recording sink, telemetry, and
  exact revision-conflict propagation. It also owns the retention retry
  verifier for durable claim identity and idempotent completion, deterministic
  delivery, restore-coordination, and lease-loss protocol expectations, and
  the common application, service, readiness, and authenticated HTTP
  expectations.
  It is workspace-internal and used only as a development dependency.
- `hubuum-storage-postgres` owns the native pool, TLS, endpoint diagnostics, generated schema, migrations, JSONB validation, query instrumentation, and all production PostgreSQL operations.
- The root crate owns application services and static composition. It constructs the PostgreSQL adapter with telemetry and dedicated operational pools, then places it behind the opaque handle.
- The root crate has no PostgreSQL module tree. Adapter-specific integration
  fixtures are typed, feature-gated APIs owned by `hubuum-storage-postgres`.

The backend-neutral contracts needed by an out-of-tree adapter remain
workspace-internal in this pull request; they are not being published yet. An
external-crate integration test nevertheless compiles the transaction ports
and representative typed DTO/query APIs without crate-private access so a
later crate split does not require an interface redesign. Backend registration
remains explicit, exhaustive, and application-owned. Hubuum does not load
storage plugins dynamically.

Moving a file does not by itself improve the boundary. Dependencies must continue to point from the application to contracts and from adapters to contracts, never from a contract or adapter back into the application.

## Current Confidence

The PostgreSQL path is exercised against a real migrated database by shared backend contracts, PostgreSQL-specific tests, service tests, HTTP integration tests, destructive restore tests, query-budget tests, platform and feature builds, production-container tests, and benchmarks. CI also migrates representative data from the adjacent stable release, starts the new application, and restarts the previous application against the migrated schema.

The contract's methods and selected input variants are inventoried mechanically.
Every registered backend runs the reusable six-part audit verifier plus
compact service, readiness, and authenticated HTTP point/list scenarios.
Adapter-private deterministic failpoints prove rollback at representative
compound-write and task-state-machine seams.

This is strong practical coverage, not a formal proof of portability.
PostgreSQL is the only complete production adapter. The portable six-part,
retention-retry, delivery-fault, restore-coordination, and lease-loss
expectations are extracted, while backend provisioning and the broader
application compatibility fixtures remain application-owned.

The [testing guide](storage_boundary/testing.md) gives the detailed assessment and the highest-value remaining improvements.
