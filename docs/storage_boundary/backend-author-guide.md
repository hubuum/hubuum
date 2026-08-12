# Storage Backend Author Guide

This guide describes the contract a new selectable backend must satisfy. Read
the [capability family map](capability-families.md) first; it defines the
responsibilities and relationships summarized here.

## Definition of a Backend

A selectable backend is a complete implementation of `StorageBackend`. It is
not selectable when it supports only lifecycle operations, only HTTP-facing
queries, or only the operations needed by one deployment.

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

- a collection-only model implements `StorageIdentity` and `CollectionStore`;
- a service accepts `Arc<dyn CollectionStore>`, not a complete backend; and
- application composition accepts only `StorageBackend`, whose supertraits
  require every family.

An operation may have a real optional or best-effort semantic only when the
contract explicitly defines it. For example, a wake-up hint may be optional if
durable polling remains the correctness path. That semantic belongs on the
narrow operation itself; it is not a blanket escape hatch for an incomplete
backend.

## Implementation Order

The following order minimizes rework:

1. Define adapter settings, safe diagnostics, and a private native client or
   pool.
2. Define the adapter error and its conversion to `StorageError`.
3. Implement foundational lifecycle, identity, and authorization facts.
4. Implement permission-aware read models.
5. Implement task execution and workflow families.
6. Implement event and operational families.
7. Add exhaustive dispatch, common observation, administrator projection, and
   the explicit `StorageBackend` implementation.
8. Run shared compatibility and backend-native certification tests.

Later families depend conceptually on the earlier facts, but this order does
not authorize direct trait-to-trait backend recovery. Prefer private adapter
helpers that share native transactions and queries.

## Boundary Values

Use the request and result DTOs owned by `hubuum-storage-core` or the root
storage contract. These values describe application intent and observable
results, not the adapter's schema.

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

Do not mirror tables with one repository trait per table. Capability methods
are shaped around application operations and consistency boundaries. A method
may legitimately touch many native tables.

## Atomicity and Consistency

If an application invariant requires several writes to succeed or fail
together, expose one storage operation and implement one native transaction.
Examples include:

- a lifecycle mutation plus its audit event;
- a grant mutation plus owner-revision advancement;
- task completion plus its output artifact and terminal event;
- a computed rebuild batch plus validation of its live task claim; and
- restore application plus state replacement, provenance, resume, and staging
  cleanup.

The backend also owns native locking, isolation, optimistic revision checks,
identifier allocation, uniqueness enforcement, and rollback behavior. Do not
ask the application to hold a transaction open across multiple trait calls.

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

Do not import a concrete permission backend into a storage adapter. Do not
silently choose local policy when the application configured an external
backend.

## Errors

Define an adapter-owned error, for example `ExampleStorageError`. Preserve
native context inside that layer and convert it once at the adapter edge:

```text
ExampleStorageError -> StorageError -> ApiError
```

Map expected outcomes to the narrowest `StorageErrorKind`, including not found,
conflict, validation, and stale precondition. Map connectivity and native
execution failures to database or unavailable classifications as appropriate.
Retain diagnostic detail for logs while keeping public responses safe.

Backend-neutral storage crates must not import `ApiError`. Application code
must not convert `ApiError` back into `StorageError`. Transitional PostgreSQL
helpers that still return `ApiError` are classified immediately inside the
adapter and are not a pattern for a new backend.

## Observability

`StorageHandle` applies common tracing and metrics before dispatch. Each entry
point needs a unique pair of static capability and operation labels.

The common observer records:

- backend, capability, and operation on a storage span;
- duration and error counters using the same bounded labels;
- debug completion events;
- debug rejection events for expected domain failures; and
- warning events for database, unavailable, and internal failures.

An adapter may add native pool, transaction, or query diagnostics. Those are a
second implementation-level view, not a replacement for common storage
observation.

Never use an entity ID, name, query, URL, credential, error message, or payload
as a metric label. Redact adapter settings and DTO `Debug` output by
construction.

## Execution Context

Implement `StorageExecution` using the backend's native context mechanism. It
must evaluate each wrapped future exactly once and preserve:

- a bounded `StorageCallSite`;
- typed mutation provenance; and
- a validated revision precondition.

Context must not leak between requests, worker tasks, reused connections, or
transactions. Both local and `Send` call-site forms are required.

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

## Registration

Adding an adapter requires explicit edits; there is deliberately no dynamic
fallback:

1. Add its stable variant to `StorageBackendKind` and `StorageBackendKind::ALL`.
2. Implement every trait aggregated by `StorageBackend`.
3. Explicitly implement `StorageBackend` in `src/storage/contract.rs`.
4. Add one exhaustive `StorageHandle` composition and dispatch variant.
5. Add factory construction and redacted settings projection.
6. Add it to the `available_backends()` test factory.

Compilation should fail when any trait or exhaustive match arm is missing.

## Certification Tests

A new backend must pass four distinct kinds of checks.

### Shared behavior

Run every test in `src/tests/storage_contract.rs` through the backend returned
by `available_backends()`. Do not copy and edit the tests for the new adapter.
The point of the registry is identical observable behavior.

### Native behavior

Add adapter-specific tests for mechanics the shared contract cannot express:

- transaction commit and rollback;
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

See [testing and certification](testing.md) for the current suite and commands.

## Completion Checklist

A backend is selectable only when all of the following are true:

- [ ] Every `StorageBackend` supertrait has a real implementation.
- [ ] All 20 capability families preserve their documented semantics.
- [ ] DTOs and errors contain no native implementation types.
- [ ] Atomic application invariants are native atomic operations.
- [ ] Dispatch is exhaustive and has no fallback backend.
- [ ] Common observation covers every entry point with bounded labels.
- [ ] Native diagnostics contain no sensitive data.
- [ ] Administrator settings are useful and redacted.
- [ ] Shared compatibility tests pass through `available_backends()`.
- [ ] Native failure, consistency, concurrency, and recovery tests pass.
- [ ] Service, API, CLI, worker, feature, and packaging tests pass.
- [ ] Representative database round trips show no unexplained regression.
- [ ] Trait, compatibility, and boundary documentation changes remain aligned.

If any item is missing, keep the implementation internal and non-selectable.
