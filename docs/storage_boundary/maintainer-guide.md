# Storage Boundary Maintainer Guide

This guide explains where storage-boundary code lives, how a request moves
through it, and how to decide which layer owns a change.

For normative guarantees, see the [storage contract](contract.md). For trait
responsibilities, see the [capability family map](capability-families.md). For
adapter acceptance, see [testing and compatibility](testing.md).

## Request Flow

A normal storage-backed request follows this direction:

```text
handler
  |
  v
application service or permission service
  |
  v
StorageContext / AuthorizationContext
  |
  v
StorageHandle trait implementation
  |  common span, logging, metrics, exhaustive dispatch
  v
PostgresStorage trait implementation
  |
  v
adapter operation and private row conversion
  |
  v
with_connection or with_transaction
```

A composed resource workflow follows the same path, but
`TransactionStorage` creates one opaque `StorageTransaction` and all of its
resource operation accessors reuse the same native unit of work.

Results travel back as storage DTOs, then domain or API values. Errors travel
back as `PostgresStorageError`, `StorageError`, and finally `ApiError`.

## Source Map

| Concern | Primary location |
| --- | --- |
| Extracted traits, DTOs, errors, descriptors | `crates/hubuum-storage-core/src/*` |
| PostgreSQL adapter operations, private rows, pool, TLS, schema, migrations, JSONB, query capture | `crates/hubuum-storage-postgres/*` |
| Complete aggregate | `crates/hubuum-storage-core/src/backend.rs` |
| Complete PostgreSQL backend type and aggregate opt-in | `crates/hubuum-storage-postgres/src/backend/mod.rs` |
| Opaque context, dispatch, common observation | `src/storage/context/*` |
| Application context, DTO conversion, and execution facade | `src/storage/*.rs` |
| Thin PostgreSQL capability delegates | `crates/hubuum-storage-postgres/src/backend/capabilities/*` |
| Application-owned PostgreSQL construction and telemetry wiring | `src/storage/factory.rs` |
| Typed adapter-native integration fixtures | `crates/hubuum-storage-postgres/src/test_support.rs` |
| Native connection and transaction helpers | `crates/hubuum-storage-postgres/src/runtime.rs` |
| Backend-neutral unit of work and operation accessors | `crates/hubuum-storage-core/src/transaction.rs` |
| PostgreSQL transaction-scoped capability implementation | `crates/hubuum-storage-postgres/src/backend/transaction.rs` |
| Adapter error conversion | `crates/hubuum-storage-postgres/src/error.rs` |
| Storage construction and settings | `src/storage/factory.rs` |
| Application use cases and DTO conversion | `src/services/*` |
| Authorization-policy selection | `src/permissions/*` |
| HTTP presentation and `ApiError` | `src/api/*`, `src/errors/*` |
| Shared selectable-backend behavior | `src/tests/storage_contract.rs` |
| Reusable six-part behavioral verifier | `crates/hubuum-storage-conformance/src/lib.rs` |
| Sealed selectable-backend certification | `src/storage/contract.rs` |
| Method, variant, and scenario inventory | `docs/storage_boundary/semantic-coverage.toml` |
| Boundary architecture guards | `src/tests/application_boundary.rs`, `src/tests/workspace_boundaries.rs` |
| PostgreSQL query budgets | `src/tests/storage_performance.rs` |
| HTTP integration suites | `tests/api_*_suite/*` |

`hubuum-storage-postgres` owns `PostgresStorage`, its complete `StorageBackend` implementation, and every production PostgreSQL operation, row, query, migration, and driver concern. The root application selects that statically linked adapter, supplies effective settings, attaches application telemetry and dedicated operational pools, and places it behind `StorageHandle`.

The root crate has no PostgreSQL module tree. New adapter-specific tests belong
beside `hubuum-storage-postgres` or use its typed `integration-test-support`
surface. Do not recreate Diesel rows or SQL helpers in application tests.

The composition modules are grouped by capability family. In
`src/storage/context`, `mod.rs` owns the opaque handle, backend enum, resource
ports, and one exhaustive dispatch macro. Sibling modules own forwarding
implementations for identity, identity queries, general queries, relations,
computed fields, tasks, workflows, events, transactions, and operational
capabilities.

`crates/hubuum-storage-postgres/src/backend` implements every contract trait for
`PostgresStorage`. The capability modules are thin delegation boundaries; the
workflow modules own adapter-specific coordination. Native execution belongs
under `crates/hubuum-storage-postgres/src/operations`; do not recreate an
adapter-specific implementation or SQL tree in the root application.

This split is structural, not semantic. A selectable backend still implements
the single complete `StorageBackend` aggregate, and every application call
still crosses the same observer and exhaustive dispatch point.

## Where a Change Belongs

### New storage operation

Ask whether the operation represents application intent and one consistency
boundary.

If yes:

1. Add a typed request and result to `hubuum-storage-core` when they can be
   independent of root application types. Otherwise keep the temporary
   contract in `src/storage`.
2. Add the operation to the narrow owning trait.
3. Add observed exhaustive dispatch to `StorageHandle`.
4. Implement it in every selectable backend.
5. Convert adapter-private rows explicitly.
6. Add shared behavior and backend-native tests.

If the proposed method is merely "return this table row" or "give me a
connection so I can query," redesign it around the use case.

Before adding a method, ask whether the use case only composes existing safe
resource semantics. If it does, implement the orchestration in the application
through `TransactionStorage`. Add a trait method when storage needs a new
query semantic, a hidden lock or state machine, or one invariant that callers
cannot safely reconstruct.

```text
existing safe primitives + application orchestration -> StorageTransaction
new persistence semantic or hidden invariant          -> owning trait method
```

### New public API behavior

Keep parsing, public validation, permission checks, response models, and
`ApiError` at the API or application layer. Add storage behavior only for the
facts, queries, or atomic writes the use case requires.

Regenerate OpenAPI when endpoint or schema shapes change.

### New native PostgreSQL optimization

Keep SQL, query builders, row types, cursor mappings, locks, and transaction
settings in the PostgreSQL adapter. Preserve the existing contract request,
result, errors, and semantics unless the application requirement itself
changed.

### New capability family

Adding a family changes the compile-time backend contract. Update:

- the `StorageBackend` aggregate;
- exhaustive dispatch and observation;
- every selectable adapter;
- sanitized administrator settings when applicable;
- shared compatibility tests; and
- the capability and backend-author documentation.

Do not add optional markers, no-op implementations, or generic unsupported
defaults to make one backend compile. Focused adapters implement only the
narrow traits they support.

## Contexts and Authorization

`StorageContext` provides access to the already configured opaque
`StorageHandle`. It does not expose a pool and it does not choose an
authorization policy.

Use `AuthorizationContext` for use cases that must evaluate the configured
permission backend. Production implementations require `AppContext`. Bare
storage-handle and concrete-pool compatibility implementations exist only for
tests or explicitly gated benchmark support.

This separation prevents a subtle failure mode: reconstructing a storage
backend from a pool and accidentally replacing an externally configured policy
backend with local PostgreSQL authorization.

## DTO Design

Boundary DTOs should make invalid or ambiguous calls difficult:

- use validated newtypes rather than unchecked primitives;
- keep representation private and expose explicit accessors;
- use a builder when several optional settings interact;
- use typestate only when call order or missing required data is a meaningful
  hazard;
- provide redacted `Debug` for sensitive requests; and
- omit `Debug` when even a redacted representation would add little value.

Do not derive Diesel traits on a storage DTO. Define a private adapter row and
write the conversion at the adapter edge.

Shared resource projections such as `StorageCollection`, `StorageClass`, and
`StorageObject` are logical views. They are not promises that all native stores
must share PostgreSQL's schema.

## Transactions

Use `TransactionStorage` for application-owned composition of existing safe
collection, class, object, and relation semantics. The transaction requires an
`EventContext`; its operation types attach that context to every mutation.

Keep a single operation-shaped trait method for task, restore, import,
permission, retention, delivery, and other state machines whose invariants the
backend must own.

Inside PostgreSQL:

- implement transaction-scoped resource calls with private connection-level
  helpers that never start a nested transaction;
- use `with_transaction` for multi-step writes that must roll back together;
- use `with_connection` for a single read, a single write, or intentionally
  non-atomic work; and
- keep connection and transaction parameters out of public or application
  signatures.

Revision checks, audit events, task claims, and destructive restore ownership
must be verified inside the same native transaction as the protected write.
The transaction compatibility test must prove rollback of both state and audit
events for every composable lifecycle family.

Ordinary mutation APIs require `EventContext` and return `MutationOutcome`.
Do not add an optional context, a boolean event switch, or an eventless helper.
Compatibility code without a user actor uses explicit system attribution.
Committed outcomes carry the receipt produced by the durable event write;
genuine no-ops return `Unchanged` and append no event.

`ImportStorage` and `RestoreStorage` are explicit, typed workflow capabilities.
They are not a general way to bypass the ordinary audit contract.

## Errors

The allowed direction is:

```text
PostgresStorageError -> StorageError -> ApiError
```

The PostgreSQL adapter classifies driver and native operation errors. The
storage contract exposes only `StorageError`. The application error layer owns
the public mapping.

When adding an error path:

1. Preserve the expected domain classification when possible.
2. Record native diagnostic context in adapter-owned structured logs before
   constructing the portable error; `StorageError` deliberately retains only
   its safe classification, message, and optional current revision.
3. Avoid sensitive values in portable display text and tracing fields.
4. Do not make the storage crate depend on an HTTP error.

## Logging and Metrics

Every logical storage operation dispatches through the common observer. Give
new operations one unique pair of static labels:

```text
(capability, operation)
```

The pair must be bounded and must not contain data. Architecture tests derive
the expected operation count from the semantic contract inventory, require
exactly one observer per method, and reject duplicate labels.

Execution-scope helpers are not logical storage operations. PostgreSQL pool
metrics use the adapter's diagnostic surface rather than a logical storage
capability, so collecting them cannot recursively instrument metric collection.

PostgreSQL connection and transaction metrics answer different questions from
storage-operation metrics:

- storage metrics measure logical application calls; and
- database metrics measure pool, checkout, transaction, and native execution.

Keep both. Do not infer one from the other or silently omit the common wrapper
because native instrumentation exists.

The complete transaction callback is one logical operation labeled
`transactions/with_transaction`. Its constituent resource calls are steps inside that
entrypoint. PostgreSQL pool, transaction, and query diagnostics provide the
lower-level view without double-counting each step as an independent
application call.

## Administrator Configuration

The administrator endpoint, startup logs, and backend-info metric must agree on
backend identity. The application registry is the single authority for the
selected `StorageBackendKind` and its diagnostic name. Add diagnostic settings
only when they are non-sensitive or can be represented as a safe boolean.

Never expose a connection URL, password, token, certificate contents, remote
authentication configuration, or raw driver option string.

## Workspace Ownership

- `hubuum-domain` owns extracted backend-independent domain values.
- `hubuum-storage-core` owns the complete contract traits and portable DTOs,
  including backend-neutral logical metrics snapshots. Native pool diagnostics
  stay in the adapter and are projected into the root-owned legacy database
  endpoint shape during composition.
- `hubuum-storage-conformance` owns reusable audit, retention, delivery-fault,
  restore-coordination, lease-loss, and application/service/HTTP compatibility
  expectations. It is a
  workspace-internal development dependency, not production code.
- `hubuum-storage-postgres` owns pool construction, schema, migrations, native helpers, and production operation implementations.
- The root crate owns application services, authorization-policy selection,
  adapter registration, observer wiring, settings and legacy-diagnostics
  projection, and exhaustive composition.

The backend-independent audit, retention, and application/service/HTTP
expectations live in `hubuum-storage-conformance`. Root code implements the
fixture hooks because authentication, administrator provisioning, and HTTP
composition remain application responsibilities.

The remaining extraction sequence is for broader reusable compatibility
testing, not production PostgreSQL behavior:

1. Continue replacing direct SQL in application integration tests with public
   contracts or typed adapter test support.
2. Add backend-neutral fixture hooks for more capability families where the
   assertions are reusable.
3. Move those family expectations into `hubuum-storage-conformance` without
   making the workspace crates publishable in this change.

When workspace membership or manifests change, update the Docker manifest-copy
stage and run the container parity and production build required by
`AGENTS.md`.

## Architecture Enforcement

The current source guards verify that:

- handlers, services, permissions, workers, middleware, and metrics consumers
  do not import Diesel, PostgreSQL adapter modules, pools, or transactions;
- process entry points compose storage through neutral settings and an opaque
  handle;
- contract DTOs contain no adapter or HTTP types;
- only adapters convert implementation errors into `StorageError`;
- only the application converts `StorageError` into `ApiError`;
- all required capability traits, including `TransactionStorage`, remain in
  the aggregate;
- the semantic inventory exactly matches aggregate traits, trait methods,
  tracked input variants, and existing evidence functions;
- PostgreSQL explicitly implements the aggregate and the memory model does not;
- dispatch labels remain complete, bounded, and unique; and
- workspace dependencies continue to point from adapters toward neutral
  crates, never toward the application.

These checks are compile-time-adjacent source guards, not a replacement for
code review. A cleverly indirect leak can still be architecturally wrong even
when it does not match a forbidden source pattern.

## Maintainer Checklist

Before considering a boundary change complete:

- [ ] The operation has one clear owning family and trait.
- [ ] No application consumer imports adapter details.
- [ ] DTOs express intent without mirroring a native schema.
- [ ] Existing safe semantics are composed through the opaque unit of work.
- [ ] Hidden invariants remain inside one adapter operation.
- [ ] Ordinary mutations require attribution and return the correct outcome.
- [ ] Maintenance APIs remain narrow and cannot bypass ordinary auditing.
- [ ] Audited state and event side effects share one commit boundary.
- [ ] Errors cross outward through the one-way adapter-to-contract-to-application
  path; contract validation errors may still be classified inside an adapter.
- [ ] Dispatch is exhaustive and commonly observed.
- [ ] Production composition supplies logical and adapter-native telemetry.
- [ ] Labels and debug output are bounded and non-sensitive.
- [ ] Shared and PostgreSQL-native tests cover the changed semantics.
- [ ] The six-part conformance fixture and sealed certification remain aligned.
- [ ] API, worker, administration, and configuration callers remain neutral.
- [ ] Trait-family docs, OpenAPI, and changelog are updated where applicable.
- [ ] All verification required by `AGENTS.md` passes.
