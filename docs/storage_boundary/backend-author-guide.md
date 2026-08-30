# Storage Backend Author Guide

This guide describes how to implement a selectable backend. Read the normative
[storage contract](contract.md) and the
[semantic capability group map](capability-families.md) first; they define the
guarantees, responsibilities, and relationships summarized here.

## Definition of a Backend

A selectable backend is a complete implementation of `StorageBackend`. It is
not selectable when it supports only lifecycle operations, only HTTP-facing
queries, or only the operations needed by one deployment.

Application registration also requires `Clone + 'static`. Cloning must be
cheap: put native pools, clients, caches, and other shared adapter state behind
reference-counted handles so `clone()` duplicates only handles. Composition
clones the adapter for observation, diagnostics, notifications, and opaque
dispatch; it must not create a new pool, connection manager, worker, or cache.

A partial implementation can be useful as a focused model or test double. Name
and compose it through only the narrow traits it implements. Do not provide
dummy methods, silent no-ops, or generic "unsupported" defaults. Do not add it
to `StorageBackendKind::ALL`, advertise it to administrators, or implement
`StorageBackend` for it.

### Why unsupported defaults are forbidden

A mandatory write that logs and returns success can lose data or skip an
invariant while its caller proceeds as though the operation committed. A
mandatory read that returns an empty value can produce incorrect authorization,
pagination, retention, or recovery decisions. Returning a generic unsupported
error is safer than a no-op, but still defers a composition mistake from the
compiler to a production request.

Partiality is therefore structural:

- a collection-only model implements `CollectionStorage`;
- a service accepts `Arc<dyn CollectionStorage>`, not a complete backend; and
- application composition accepts only `StorageBackend`, whose supertraits
  require every operation trait and family bound.

An operation may have a real optional or best-effort semantic only when the
contract explicitly defines it. For example, a wake-up hint may be optional if
durable polling remains the correctness path. That semantic belongs on the
narrow operation itself; it is not a blanket escape hatch for an incomplete
backend.

## Implementation Order

The grouped imports under `hubuum_storage_core::capabilities` provide the
canonical discovery map. Crate-root reexports retain the same type names as
convenience paths; adapters must not expect shorter aliases in capability
modules.

Read [storage query semantics](query-semantics.md) before implementing any
collection-shaped method. It defines the common page contract, while the
[method contract registry](method-contracts.toml) defines every operation's
exact carrier, supported query keys, cursor/count/visibility behavior, and
batch or complete-read semantics.

The following order minimizes rework:

1. Define adapter settings, safe diagnostics, and a private native client or
   pool.
2. Define the adapter error and its conversion to `StorageError`.
3. Implement foundational lifecycle, identity, and authorization facts.
4. Implement `TransactionStorage` over the lifecycle operations and prove
   state-plus-event commit and rollback.
5. Implement permission-aware read models.
6. Implement task execution and the remaining workflow groups.
7. Implement the event and operational groups.
8. Add exhaustive dispatch, common observation, administrator projection, and
   the explicit `StorageBackend` implementation.
9. Implement a `BackendAuditFixture` and pass the reusable six-part audit
   conformance verifier.
10. Add the adapter to the sealed application certification registry, then run
    shared compatibility and backend-native verification tests.

Later groups depend conceptually on the earlier facts, but this order does
not authorize direct trait-to-trait backend recovery. Prefer private adapter
helpers that share native transactions and queries.

## Boundary Values

Use the request and result DTOs owned by `hubuum-storage-core` and the validated
values owned by its backend-neutral workspace dependencies. They are designed
for later publication but remain unpublished in this change. Root application
models may be converted into those DTOs, but they are not part of the adapter
contract.

Boundary values describe application intent and observable results, not the
adapter's schema.

### Naming policy

Public contract value/DTO structs, enums, and type aliases owned by
`hubuum-storage-core` use the `Storage` prefix. Their remaining terms follow
subject, operation, then role or shape: for example,
`StorageComputedObjectListQuery`,
`StorageObjectAggregateQuery`, and `StorageTaskChildListQuery`. A builder uses
the exact value name followed by `Builder`. Fallible construction uses
`try_new` or `try_build`; `new` and `build` are infallible.

The unqualified resource names `StorageCollection`, `StorageClass`, and
`StorageObject` are the canonical flat projections. An alternative projection
states its expansion explicitly, as `StorageClassWithCollection` does. Other
result-shape suffixes retain their ordinary meanings: `ListItem` is a reduced
list projection, `Details` is an expanded point projection, `Summary` is a
reduced retained result, `Row` is one batch or aggregate row, `Page` is a paged
result, `Snapshot` is one consistent observation, and `Outcome` is a mutation
result.

Persistence ports keep the `<Capability>Storage` form, such as
`CollectionStorage` and `TaskQueueStorage`; they do not become
`StorageCollectionStorage`. Non-DTO collaborators use their semantic role,
such as `EventArchiveSink`, `ObjectAggregateAuthorizer`, and
`WorkerNotificationProvider`. The `Transactional*` operation handles likewise
describe their bound role and are not DTO naming exceptions.

Adapter boundary types must not expose:

- a native connection, pool, transaction, row, or query builder;
- a Diesel or driver error;
- SQL cursor values or database type identifiers;
- application configuration or `ApiError`; or
- unredacted credentials, payloads, URLs, or personally identifying debug
  output.

Keep persistence rows private to the adapter and write explicit conversions.
Use private fields, validating constructors, and typed builders for contract
requests with several settings or meaningful invalid combinations.
Use the `hubuum-domain` identifier and revision newtypes carried by the
contract. Convert them to native keys only inside the adapter. Treat
`QueryOptions`, `QueryFilters`, `QuerySort`, and `QueryCursor` as validated
query intent; do not reinterpret their private representation or add SQL
concepts to the shared query crate.

### Projection validation

Fallible value constructors and projection builders use `try_new` and
`try_build`. They return `StorageValidationError`, which deliberately carries
no request or backend classification. The application maps caller-supplied
values with `StorageValidationError::into_request_error`; an adapter maps a
rejected native or persisted projection to `StorageErrorKind::Backend`. Do not
add a blanket `From<StorageValidationError>` conversion, because it would make
one of those two boundaries classify the same value incorrectly.

An infallible `new` or `build` is intentional only when the value has no
additional invalid combination beyond its already validated components, or
when it is a provenance-only projection. The adapter-returned value inventory
in `external_adapter_values.rs` uses these classifications:

| Classification | Families and examples | Constructor rule |
| --- | --- | --- |
| Invariant-bearing projections | Record metadata; pages and counts; authorization, identity, resource, relation, and history aggregates; task and output state; restore and backup artifacts; event delivery state; remote-target policy; aggregate rows; operational and metric snapshots | Reject invalid chronology, count ranges, document shapes, digests, state combinations, canonical ordering, and cross-component identifiers through `try_new` or `try_build`. |
| Validated-component wrappers | Computed objects, event-health composites, service-account detail/list rows, task access, and mutation outcomes | Infallible construction is permitted when every component is already valid and the wrapper introduces no new cross-field invariant. |
| Provenance-only projections | Names, descriptions, audit payloads, task-event and import-result details, source labels, and versioned definition documents | Preserve the backend-neutral value without inventing adapter-specific content rules. Validate only its typed identifiers or enclosing invariant-bearing aggregate. |

Request and query DTOs are not projection values. Their terminal validation may
return a request-classified `StorageError` when the value can only originate
from application intent. Shared values used on both sides of the boundary must
use `StorageValidationError` instead.

Do not mirror tables with one repository trait per table. Capability methods
are shaped around application operations and consistency boundaries. A method
may legitimately touch many native tables.

## Atomicity and Consistency

Every selectable backend implements `TransactionStorage`. It provides an
opaque unit of work for composing the safe resource operations exposed by
`StorageTransaction`. The callback sees crate-owned operation types, never a
native connection, driver session, or query interface.

Use transaction composition when an application workflow combines existing
safe resource semantics. For example, an application may create two objects
and the relation between them in one unit of work. The adapter must use one
native atomic mechanism and reuse its ordinary validation, revision, and event
semantics.

If an invariant depends on a hidden state machine, lock protocol, or
backend-specific consistency rule, expose one operation-shaped method instead
of rebuilding it through the generic unit of work. Examples include:

- a lifecycle mutation plus its audit event;
- a grant mutation plus owner-revision advancement;
- task completion plus its output artifact and terminal event;
- a computed rebuild batch plus validation of its live task claim; and
- restore application plus state replacement, provenance, resume, and staging
  cleanup.

The backend also owns native locking, isolation, optimistic revision checks,
identifier allocation, uniqueness enforcement, and rollback behavior. The
application may hold an opaque `StorageTransaction` across several safe
resource calls; it must never hold or recover the native transaction.

Ordinary mutations require an `EventContext`; optional audit provenance is not
part of the contract. Resource mutations return `StorageMutationOutcome`, with a
durable `StorageAuditReceipt` for commits and no receipt for genuine no-ops.
Transaction-scoped mutations inherit one required context. The adapter must
commit or roll back state, audit events, and transactional notifications
together. It must serialize access when its native transaction cannot safely
execute concurrent operations.

Import and restore implement explicit `ImportStorage` and `RestoreStorage`
capabilities. Do not use those surfaces as unaudited shortcuts for ordinary
application writes.

Read contracts state whether paging, totals, visibility, and projections must
come from one snapshot. Implement those semantics even when a native store
would make several unrelated reads easier.

## Authorization Boundaries

Storage supplies facts and enforces local permission queries. The application
selects and evaluates the configured authorization policy.

Read operations therefore receive one of these forms:

- backend-neutral visibility that the backend can push down;
- token permission and resource dimensions;
- identifiers already authorized by an external policy backend; or
- a narrow callback such as `ObjectAggregateAuthorizer` for bounded delegated
  decisions.

External-policy candidate enumeration must use `StorageCandidatePageLimit` and
`StorageCandidatePage<T>` (or an operation-specific page with the same bound).
Return one stable cursor page, including only the requested number of rows and a
`has_more` signal. For ranked search, return the adapter-owned cursor beside the
row. Complete policy inputs are permitted only when the caller supplies a
validated total bound and the operation rejects overflow.

Do not import a concrete permission backend into a storage adapter. Do not
silently choose local policy when the application configured an external
backend.

## Errors

Define an adapter-owned error, for example `ExampleStorageError`. Record native
context in adapter-owned structured logs and convert it once at the adapter
edge:

```text
ExampleStorageError -> StorageError -> ApiError
```

Map expected outcomes to the narrowest `StorageErrorKind`, including not found,
conflict, validation, and stale precondition. Follow the normative error matrix
in the [storage contract](contract.md#5-portable-error-classification-is-consistent):
pool, connection-establishment, and temporary service reachability failures are
`Unavailable`; failures executing against a reached backend and corrupt
persisted values are `Backend`. Keep diagnostic detail in adapter-owned logs;
the portable error retains only a safe classification, message, and optional
current revision.

Treat the kind and structured fields as the portable contract. The message is
safe human-readable diagnostic prose, not a stable code, and callers must not
branch on it. Map known native constraints to deliberately selected portable
messages; never forward arbitrary driver or server text. The complete contract
has no generic unsupported-operation error: every method and documented input
variant is mandatory. Model genuinely optional behavior outside
`StorageBackend`, as `WorkerNotificationProvider` does.

Backend-neutral storage crates must not import `ApiError`. Application code
must not convert `ApiError` back into `StorageError`. An adapter maps
`StorageValidationError` from a native projection into its private backend
error while it is assembling or decoding that DTO; that is not an application
dependency reversal.

## Import Plans and References

`StorageImportPlan` is validated before an adapter begins execution. Backends
may rely on its strictly increasing item indexes, valid positive update
identifiers, non-empty names, and unambiguous selectors.

The application also resolves import defaults before constructing the plan.
`StorageImportMode` always contains concrete atomicity, collision, and
permission policies; group and principal keys always name an identity scope;
class inputs contain the final schema and schema-validation state; and
collection-permission inputs contain a concrete replacement choice. An adapter
must apply these values rather than choosing its own defaults. Remaining
optional values represent nullable persisted state, selector alternatives,
write conditions, or timestamp behavior documented by the import contract.

Task completion commands also arrive with their storage invariants resolved.
An export artifact contains one typed JSON or text content variant and
non-negative report values. A backup artifact derives its byte size and SHA-256
digest from the document it carries. Shared task-output durations are
non-negative. Adapters should persist these values directly and must map any
corrupt native projections encountered on reads to `Backend`.

An import `ref` is local to one plan. It allows a later item in that plan to address a value created or updated by an earlier item without knowing the backend-assigned identifier. A backend must maintain this plan-local reference map during preflight and application.

A key is durable. Use a key when an item addresses state that existed before the plan or when a later, separate plan addresses state produced by an earlier plan.

```text
same StorageImportPlan
create class ref "class:room"
          |
          `----> create object using class_ref "class:room"

later StorageImportPlan
update relation using class_key / object_key
```

Do not persist plan-local refs as backend identity unless a separate contract explicitly introduces that behavior.

## Observability

`StorageHandle` applies common tracing and metrics before dispatch. Each
logical storage operation needs a unique pair of static capability and
operation labels. The maintainer guide documents the small set of metadata and
execution helpers that are intentionally not logical observed operations.

The common observer records:

- backend, capability, and operation on a storage span;
- duration and error counters using the same bounded labels;
- debug completion events;
- debug rejection events for expected domain failures; and
- warning events for database, unavailable, and internal failures.

An adapter may add native pool, transaction, or query diagnostics. Those are a
second implementation-level view, not a replacement for common storage
observation. Its production constructor must accept an application-owned
telemetry implementation. Any no-op observer must be an explicit opt-out for
tests, benchmarks, or one-shot tools.

`TransactionStorage::with_transaction` is one logical observed entrypoint. Calls
made through its operation accessors are constituent steps rather than new
composition entrypoints. Native transaction and query instrumentation supplies
the implementation-level detail without multiplying logical metrics.

Never use an entity ID, name, query, URL, credential, error message, or payload
as a metric label. Redact adapter settings and DTO `Debug` output by
construction.

## Execution Context

Implement both `ExecutionStorage::run_in_scope` and `run_in_scope_send` using
the backend's native context mechanism. They must evaluate each wrapped future
exactly once and preserve every override in `StorageExecutionScope`:

- a bounded `StorageCallSite`;
- typed mutation provenance; and
- a validated revision precondition;
- an optional non-zero query budget.

Context must not leak between requests, worker tasks, reused connections, or
transactions. An absent field inherits its surrounding scope; a present `None`
explicitly clears it. The local and `Send` forms must have identical semantics.

## Configuration and Composition

Provide a validating settings type with private fields. Process composition
must be able to create the adapter without exposing its native client to the
rest of the application.

The administrator projection must include:

- stable backend name;
- effective non-sensitive settings useful for diagnosis.

Report whether a sensitive setting is configured when useful, but never return
its value. Startup logs, backend-info metrics, and administrator configuration
must agree on backend identity. Statically linked adapters use trait checking
and crate versions; do not introduce duplicate runtime contract metadata.

An adapter owns its schema migrations. Keep them inside the adapter crate and
expose only the narrow runner needed by application or deployment composition.
Do not add migration paths, schema rows, or a migration framework to
`hubuum-storage-core`. Diesel adapters may use crate-relative
`embed_migrations!` and pass the same directory explicitly to Diesel CLI
workflows.

## Registration

Registration is deliberately static and explicit. There is no dynamic loader,
string fallback, or partially supported backend. Adding an adapter requires
these edits:

1. Add the adapter crate as a static application dependency.
2. Add its stable `StorageBackendKind` variant and include it in the test-only
   `StorageBackendKind::ALL` registry. Clap and Serde then accept that exact
   value through `HUBUUM_STORAGE_BACKEND`; empty selects the default, while
   other unknown values remain errors.
3. Implement every trait aggregated by `StorageBackend`, then explicitly
   implement `StorageBackend` beside the complete adapter type.
4. Add the adapter type to the sealed `CertifiedStorageBackend` registry only
   after its shared and native behavioral evidence passes.
5. Add one `BackendImplementation` variant and one arm to the exhaustive
   dispatch macro, then implement the root-local `RegisteredStorageBackend` for
   the certified adapter. Compose it through `from_registered_backend`, which
   fixes its descriptor and common observed resource ports once.
6. Add one application-local adapter factory in `src/storage/factory.rs`. It
   owns settings translation, native observer wiring, initialization errors,
   operational resources and migrations. If the adapter is database-backed
   and can support the legacy database endpoint, attach the optional root-owned
   `DatabaseDiagnosticsProvider` projection. If it supports native wake-ups,
   attach its `WorkerNotificationProvider` separately as a latency
   optimization. Registration itself must require neither provider. Native
   diagnostics and notifications do not become required storage traits.
7. Add one `BackendTestEnvironment` variant. Keep its native client or pool
   inside that variant while provisioning the reusable audit, service, and
   HTTP fixtures.
8. Run the reusable six-part audit verifier and every shared compatibility
   scenario. Also implement and run the applicable retention-retry,
   delivery-fault, restore-coordination, and lease-loss fixture contracts.
9. Add native consistency, failure, concurrency, recovery, migration, and
   performance coverage appropriate to the adapter.

Compilation should fail when any trait or exhaustive match arm is missing.

## Compatibility and Native Tests

A new backend must pass four distinct kinds of checks.

### Shared behavior

Run every test in `src/tests/storage_contract.rs` through the backend returned
by `available_backend_environments()`. Do not copy and edit the tests for the
new adapter. The point of the registry is identical observable behavior while
adapter-native fixture resources remain contained in one exhaustive enum.

Extend the backend application fixture in the same exhaustive match. It must
provision an administrator and bearer token using the backend being certified.
The shared harness runs application services, readiness, and authenticated
point and list HTTP requests without registering a native client in Actix.

Update `semantic-coverage.toml` when the contract or a tracked input enum
changes. Its architecture test requires exact trait-method and variant lists
plus effect classification and method-specific shared or native evidence. Also
update `method-contracts.toml` whenever a page, candidate, batch, or complete
read is added or reclassified. Native evidence is appropriate for transaction,
notification, and driver mechanics; it must not hide a portable behavior that
every backend should share.

### Native behavior

Add adapter-specific tests for mechanics the shared contract cannot express:

- transaction commit and rollback;
- rollback of lifecycle state, audit events, and transactional notifications
  when a transaction callback returns an application error;
- isolation and lock contention;
- uniqueness and constraint mapping;
- claim and lease concurrency;
- trigger, revision, and provenance behavior;
- notification commit visibility and reconnect behavior;
- query-budget enforcement and reset on client reuse;
- retention and recovery after interruption; and
- migrations from supported releases.

### Application and API behavior

Run the existing service and HTTP integration suites unchanged against the new
composition. If the test harness cannot select the adapter without exposing a
native client, fix the harness boundary instead of adding an application
escape hatch.

### Operational behavior

Exercise startup, readiness, administrator configuration, workers, metrics,
feature combinations, production packaging, and representative performance.

See [testing and compatibility](testing.md) for the current suite and commands.

## Completion Checklist

A backend is selectable only when all of the following are true:

- [ ] Every `StorageBackend` supertrait has a real implementation.
- [ ] All 20 semantic capability groups preserve their documented semantics.
- [ ] DTOs and errors contain no native implementation types.
- [ ] Safe lifecycle compositions use one native unit of work.
- [ ] Hidden state-machine invariants remain native atomic operations.
- [ ] Ordinary audited mutations require `EventContext`.
- [ ] Committed and unchanged mutations return correct `StorageMutationOutcome`
      variants and receipts.
- [ ] Maintenance operations cannot serve as an ordinary eventless write path.
- [ ] Transaction rollback removes both state and audit side effects.
- [ ] Dispatch is exhaustive and has no fallback backend.
- [ ] Common observation covers every entry point with bounded labels.
- [ ] Production constructors require application-owned logical and native
      telemetry; explicit no-op telemetry remains an opt-out.
- [ ] Native diagnostics contain no sensitive data.
- [ ] Administrator settings are useful and redacted.
- [ ] Shared compatibility tests pass through
      `available_backend_environments()`.
- [ ] The six-part conformance verifier passes and the sealed certification
      registry includes the adapter only afterward.
- [ ] The service and HTTP smoke contract passes through the backend fixture registry.
- [ ] `semantic-coverage.toml` exactly inventories methods, variants, and evidence.
- [ ] `method-contracts.toml` exactly specifies every collection-shaped method.
- [ ] Native failure, consistency, concurrency, and recovery tests pass.
- [ ] Service, API, CLI, worker, feature, and packaging tests pass.
- [ ] Representative database round trips show no unexplained regression.
- [ ] Trait, compatibility, and boundary documentation changes remain aligned.

If any item is missing, keep the implementation internal and non-selectable.
