# Storage Boundary Maintainer Guide

This guide explains where storage-boundary code lives, how a request moves
through it, and how to decide which layer owns a change.

For contract responsibilities, see the
[capability family map](capability-families.md). For adapter acceptance, see
[testing and compatibility](testing.md).

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

Results travel back as storage DTOs, then domain or API values. Errors travel
back as `PostgresStorageError`, `StorageError`, and finally `ApiError`.

## Source Map

| Concern | Primary location |
| --- | --- |
| Extracted traits, DTOs, errors, descriptors | `crates/hubuum-storage-core/src/*` |
| Extracted PostgreSQL pool, TLS, schema, migrations, JSONB, query capture | `crates/hubuum-storage-postgres/*` |
| Complete aggregate | `crates/hubuum-storage-core/src/backend.rs` |
| Application-owned backend opt-in | `src/storage/contract.rs` |
| Opaque context, dispatch, common observation | `src/storage/context/*` |
| Resource-family and root-domain contracts | `src/storage/*.rs` |
| PostgreSQL adapter trait implementations | `src/storage/postgres/capabilities/*` |
| PostgreSQL operations, rows, and query mapping | `src/storage/postgres/operations/*` |
| Connection and transaction helpers | `src/storage/postgres/runtime.rs` |
| Adapter error conversion | `src/storage/postgres/error.rs` |
| Storage construction and settings | `src/storage/factory.rs` |
| Application use cases and DTO conversion | `src/services/*` |
| Authorization-policy selection | `src/permissions/*` |
| HTTP presentation and `ApiError` | `src/api/*`, `src/errors/*` |
| Shared selectable-backend behavior | `src/tests/storage_contract.rs` |
| Method, variant, and scenario inventory | `docs/storage_boundary/semantic-coverage.toml` |
| Boundary architecture guards | `src/tests/application_boundary.rs`, `src/tests/workspace_boundaries.rs` |
| PostgreSQL query budgets | `src/tests/storage_performance.rs` |
| HTTP integration suites | `tests/api_*_suite/*` |

`src/storage/postgres` is an adapter tree even though most of it still resides
in the root crate. Do not treat root-crate placement as permission for an
application consumer to import it.

The composition modules are grouped by capability family. In
`src/storage/context`, `mod.rs` owns the opaque handle, backend enum, resource
ports, and one exhaustive dispatch macro. The sibling modules own forwarding
implementations for identity, queries, computed fields, tasks, workflows,
resources, diagnostics, and operations. In
`src/storage/postgres/capabilities`, the same broad groups hold the PostgreSQL
trait implementations. SQL and row mapping remain below them in the adapter
operation modules.

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

Use one trait call for one required atomic outcome. Inside PostgreSQL:

- use `with_transaction` for multi-step writes that must roll back together;
- use `with_connection` for a single read, a single write, or intentionally
  non-atomic work; and
- keep connection and transaction parameters out of public or application
  signatures.

Revision checks, audit events, task claims, and destructive restore ownership
must be verified inside the same native transaction as the protected write.

## Errors

The allowed direction is:

```text
PostgresStorageError -> StorageError -> ApiError
```

The PostgreSQL adapter classifies driver and transitional helper errors. The
storage contract exposes only `StorageError`. The application error layer owns
the public mapping.

When adding an error path:

1. Preserve the expected domain classification when possible.
2. Include diagnostic source context inside the adapter error.
3. Avoid sensitive values in display text and tracing fields.
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

Identity metadata and execution-scope helpers are not logical storage
operations. `metrics_pool_state` is also intentionally unobserved so collecting
metrics cannot recursively instrument metric collection. Keep such exceptions
explicit in the architecture guard rather than adding an unlabeled bypass.

PostgreSQL connection and transaction metrics answer different questions from
storage-operation metrics:

- storage metrics measure logical application calls; and
- database metrics measure pool, checkout, transaction, and native execution.

Keep both. Do not infer one from the other or silently omit the common wrapper
because native instrumentation exists.

## Administrator Configuration

The administrator endpoint, startup logs, and backend-info metric must agree on
backend identity. Composition asserts that the adapter's `StorageIdentity`
matches its selectable `StorageBackendKind`, so resource-family and aggregate
operation labels cannot silently diverge. Add diagnostic settings only when
they are non-sensitive or can be represented as a safe boolean.

Never expose a connection URL, password, token, certificate contents, remote
authentication configuration, or raw driver option string.

## Workspace Ownership

The current workspace extraction is intentionally incremental:

- `hubuum-domain` owns extracted backend-independent domain values.
- `hubuum-storage-core` owns extracted contract traits and DTOs, including
  backend-neutral metrics snapshots and pool diagnostics.
- `hubuum-storage-postgres` owns pool construction, schema, migrations, and
  selected native helpers.
- The root crate owns composition and still contains traits and PostgreSQL
  queries that depend on root domain types.

There is not yet a `hubuum-storage-contract-tests` workspace crate. The shared
suite currently lives in `src/tests/storage_contract.rs`. Extract it only after
its inputs no longer require root application fixtures or root-owned domain
types.

A safe continuation is:

1. Split mixed domain and Diesel types into neutral values plus private rows.
2. Move the corresponding trait and DTO into `hubuum-storage-core`.
3. Move its implementation, rows, and queries into
   `hubuum-storage-postgres`.
4. Extract the reusable test harness after it depends only on public contract
   values and a backend fixture interface.

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
- all required capability traits remain in the aggregate;
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
- [ ] Required atomicity is implemented inside one adapter operation.
- [ ] Errors follow the one-way conversion path.
- [ ] Dispatch is exhaustive and commonly observed.
- [ ] Labels and debug output are bounded and non-sensitive.
- [ ] Shared and PostgreSQL-native tests cover the changed semantics.
- [ ] API, worker, administration, and configuration callers remain neutral.
- [ ] Trait-family docs, OpenAPI, and changelog are updated where applicable.
- [ ] All verification required by `AGENTS.md` passes.
