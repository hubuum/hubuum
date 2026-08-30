# Storage Contract

This document is the normative application-to-storage contract for Hubuum. It
defines what a selectable backend must provide, what application callers may
assume, and which obligations are verified structurally or behaviorally.

The exact Rust surface is `StorageBackend` in
`crates/hubuum-storage-core/src/backend.rs`. The
[semantic capability group map](capability-families.md) maps every required
trait, and
`semantic-coverage.toml` inventories every method, effect, tracked input
variant, and evidence scenario. `method-contracts.toml` exhaustively specifies
every page, candidate, batch, and complete-collection method. Those
machine-checked sources and this semantic contract must change together.

## Scope

The contract applies to every backend in `StorageBackendKind::ALL`. Focused
models and test doubles may implement narrow capability traits, but they are
not selectable backends and must not claim the guarantees in this document.

The contract is a statically linked Rust interface, not a dynamic plugin ABI.
An external adapter can implement the capability traits from
`hubuum-storage-core`, but Hubuum must still add it explicitly to exhaustive
dispatch and the sealed application certification registry before operators
can select it.

## Contract Layers

The complete guarantee has three layers:

| Layer | Mechanism | What it proves |
| --- | --- | --- |
| Structural | `StorageBackend` supertraits, private-field DTOs, `StorageError`, and architecture guards | Every required operation exists and native or application types do not cross the boundary. |
| Semantic | This document, capability documentation, and the semantic coverage inventory | The observable behavior required from those operations. |
| Certification | Shared compatibility tests, `hubuum-storage-conformance`, and backend-native tests | A registered implementation exhibits the portable semantics and its native consistency and failure guarantees. |

Rust trait bounds alone cannot prove that a method wrote an audit event, used
one commit boundary, delivered an event, or emitted telemetry. Certification
exists to test those obligations, and the sealed registry prevents structural
conformance alone from making a backend selectable.

## Boundary and Ownership

Values crossing the boundary are owned by backend-neutral crates. A storage
API must not expose:

- Actix request, response, extractor, or application-state types;
- `ApiError` or public HTTP status decisions;
- Diesel traits, rows, schema modules, queries, connections, or errors;
- a native pool, client, transaction, cursor, or database type identifier;
- global configuration, metrics registries, or exporters; or
- unredacted credentials, payloads, filters, URLs, or claim tokens in
  diagnostics.

The application owns use-case orchestration, public validation, authorization
policy selection, HTTP projection, and `StorageError` to `ApiError` conversion.
The contract owns operation-shaped traits, validated requests, observable
results, and bounded error classifications. Each adapter owns persistence
layout, native transactions, locking, queries, migrations, driver errors, and
conversion into contract values.

Errors move in one direction:

```text
adapter error -> StorageError -> application or API error
```

## Complete Backend Contract

`StorageBackend` is indivisible for application selection. Its traits are
organized into 20 documented semantic capability groups covering:

- resource lifecycle and atomic transaction composition;
- identity, authentication, groups, principals, and authorization facts;
- catalog, computed, aggregate, relation, history, inventory, and search
  reads;
- tasks, imports, exports, backups, restores, and remote targets;
- audit reads, subscriptions, fan-out, delivery, and retention; and
- readiness, metrics snapshots, execution context, and other operational
  state.

A missing method is a compile error. Dummy success, empty-result, and generic
unsupported implementations do not satisfy the semantic contract. Truly
optional behavior is composed outside the aggregate. In particular,
`WorkerNotificationProvider` may be attached for low-latency wake-ups because
durable polling remains the correctness path.

## Six-Part Audited Storage Contract

The following six semantic guarantees are mandatory and are tested together
for every registered backend. The behavioral certification gate that follows
controls whether an implementation may be selected by the application.

### 1. Attribution Is Mandatory

Every ordinary audited mutation requires an `EventContext`. The context names
the immediate actor and carries request, correlation, task, and initiator
provenance where applicable. It is not optional.

Code without a user actor must choose explicit system or worker attribution.
Compatibility helpers whose historical names contain `without_events` do not
disable auditing; they use system attribution. There is no ordinary
event-suppression escape hatch.

### 2. The Result Proves the Audit Write

An ordinary audited mutation returns `StorageMutationOutcome<T>`:

- `Committed { value, audits }` means state changed and the non-empty `audits`
  set identifies every durable event written for that atomic change.
- `Unchanged(value)` means the requested operation was a semantic no-op. It
  carries no receipt, writes no lifecycle event, and must not advance revision
  or modification time merely to manufacture a change.

`StorageAuditReceipt` contains the stable event sequence, event UUID, entity type,
action, and before and after revisions. It deliberately omits snapshots,
metadata, actor details, and other permission-scoped audit content. Authorized
callers retrieve those through `AuditEventStorage`.

Returning a receipt is not permission to synthesize evidence after commit. It
must be derived from the event persisted by the same atomic operation.

### 3. State and Durable Side Effects Are Atomic

The state mutation and canonical audit append share one backend-native commit
boundary. On failure or rollback, neither may remain visible. Transactional
notifications may become visible only after the same commit.

`TransactionStorage::with_transaction` accepts one required `EventContext` and
passes it to every transaction-scoped lifecycle mutation. A callback returning
`Err`, a native operation failure, or a failed commit rolls back the complete
unit of work, including its audit events.

The external sink call is intentionally not part of the domain transaction.
After commit, `EventFanoutStorage` atomically claims the canonical event,
matches subscriptions, creates durable delivery rows, and releases the claim.
Delivery workers then use opaque claims and acknowledgements. Delivery is at
least once, may be unordered across events, and consumers deduplicate by event
UUID. Worker notification is a latency optimization; durable polling remains
the correctness path.

### 4. Observation Is Application-Owned and Mandatory

The application supplies two complementary observers in production:

- `StorageObserver` receives bounded logical operation observations at the
  opaque storage handle; and
- an adapter-native observer, currently `PostgresObserver`, reports pool,
  transaction, query, failure, and other implementation-level signals.

Adapters and wrappers do not choose a global metrics registry or exporter.
Production construction requires injected observers. Explicit unobserved or
no-op constructors exist only for tests, benchmarks, and deliberate one-shot
maintenance tools.

Capability, operation, backend, result, and other metric dimensions are static
and bounded. IDs, names, queries, URLs, credentials, payloads, and error text
must never become metric labels. Failed and rolled-back operations must still
produce failure observation.

### 5. Portable Error Classification Is Consistent

Adapters must classify equivalent failures identically because the application
uses the kind for HTTP status and retry behavior. This matrix is normative:

| Condition | `StorageErrorKind` | Application behavior |
| --- | --- | --- |
| Pool exhaustion or timeout, connection establishment failure, temporarily unreachable storage service, or an explicit maintenance outage | `Unavailable` | Service unavailable; retry may succeed without changing the request. |
| Query execution, transaction, driver, protocol, serialization, or persisted-value corruption after storage was reached | `Backend` | Backend failure; do not assume that an unchanged retry is safe. |
| Adapter or Hubuum invariant violation unrelated to native persistence execution | `Internal` | Internal failure; operator or code correction is required. |
| Configured authorization provider cannot answer safely | `AuthorizationUnavailable` | Permission service unavailable; never downgrade to a denial or local-policy fallback. |
| Malformed caller input, oversized input, authentication, permission, rate, or semantic validation failure | The matching specific input or policy kind | Preserve the specific client-facing outcome; do not collapse it into a backend failure. |
| Missing state, conflicting state, or a failed precondition | `NotFound`, `Conflict`, `RevisionConflict`, or `PreconditionFailed` | Preserve the expected domain outcome and any required current revision. |

Native error text remains adapter-private. In particular, a pool checkout
failure is `Unavailable`, while a failed query on an acquired connection is
`Backend`. Corrupt persisted values are also `Backend`. Shared DTO validators
return an unclassified `StorageValidationError`; applications map caller values
to request errors, while adapters map rejected native projections to `Backend`.

### 6. Revision Conflicts Preserve the Current Revision

Optimistic-concurrency failures use `StorageErrorKind::RevisionConflict` and
must carry the positive current `ResourceRevision`. Adapters must not discard,
guess, or stringify this value while translating native errors. The
application projects the same value to its API error so a caller can refresh
or retry against an authoritative revision.

## Selection Requires Behavioral Certification

`hubuum-storage-conformance` supplies the reusable `BackendAuditFixture` and
`verify_backend_audit_contract` runner. For each `StorageBackendKind::ALL`
entry, it verifies:

1. a committed receipt matches the durable event;
2. a no-op returns no receipt and appends no event;
3. an injected failure persists neither state nor event;
4. the committed event creates durable fan-out and reaches a recording sink;
   and
5. logical, native-backend, and failure telemetry are observed; and
6. a stale precondition returns the exact current revision without persisting
   the attempted mutation.

The root compatibility registry additionally exercises every semantic
capability group plus service, readiness, and authenticated HTTP behavior. Backend-native
tests remain mandatory for isolation, lock behavior, cancellation, connection
loss, claims, leases, recovery, migrations, and database-specific failure
mechanics.

Only after these checks pass may the application implement its sealed
`CertifiedStorageBackend` marker for the adapter. The marker records a reviewed
certification decision; it is not a substitute for running the tests.

## Maintenance Writes

Imports and restores implement the explicit `ImportStorage` and
`RestoreStorage` capabilities. These workflows preserve or reconstruct durable
state and history, so pretending each imported row is an ordinary user mutation
would be incorrect.

Maintenance is not a general unaudited write API. It exposes only the typed
import and restore state machines, including their validation, provenance,
coordination, durable results, and rollback requirements. Ordinary request,
service, worker, and fixture writes must use audited mutation APIs. Adding a
new maintenance operation requires an explicit contract and review of how its
history is preserved or recorded.

## Event Retention and External Archives

Retention separates durable database coordination from external archival:

1. `claim_event_retention_batch` durably records one immutable batch ID and
   its exact event documents without deleting them;
2. `EventArchiveSink::archive` runs outside the database transaction and must be
   idempotent for that batch ID; and
3. `complete_event_retention_batch` deletes exactly the claimed events and
   records completion atomically.

The core `execute_event_retention_batch` helper owns this sequence. Adapters
implement only claim and completion and cannot replace the archive-before-purge
ordering.

An archive failure leaves the claim and events intact. Retrying must return the
same batch ID and documents. Completion is idempotent, but it is valid only
after the caller has durably archived that batch (or deliberately selected a
discard archive). A count mismatch, malformed claimed document, concurrent
maintenance state, or unavailable coordinator is an error; it must never be
reported as a successful empty purge.

## Transactions, Concurrency, and Cancellation

The application may compose existing safe resource primitives through the
opaque `StorageTransaction`. It never receives the native transaction or
connection. Hidden state machines, lock protocols, and consistency rules stay
behind one operation-shaped backend method.

The adapter owns isolation, locking, uniqueness, optimistic revision checks,
claim validation, and serialization when its native transaction cannot safely
run operations concurrently. Snapshot reads must provide the consistency,
visibility, filtering, counting, and paging semantics documented by their
capability rather than combining unrelated observations.

Dropping a future requests cancellation. A backend must not report a successful
commit after its callback returned an error. If native work can continue after
cancellation, the adapter must preserve the externally visible atomicity
contract and document and test that behavior.

## Authorization

Storage supplies neutral facts and enforces local permission queries. The
application selects and evaluates the configured authorization policy. Storage
operations therefore accept neutral visibility, token scope, pre-authorized
identifiers, or narrow bounded authorization callbacks. An adapter must not
import a concrete external policy engine or silently replace it with local
authorization.

Visibility is applied before filtering, counting, paging, aggregation, and
audit projection wherever the owning capability requires it. Authorization
inputs are enforced constraints, not hints.

## Migrations and Schema Ownership

Migrations belong to the adapter that owns the schema. PostgreSQL migrations
live in `crates/hubuum-storage-postgres/migrations`; neither
`hubuum-storage-core` nor the root application owns them.

Diesel supports crate-local migrations. The PostgreSQL adapter embeds them with
`embed_migrations!("migrations")`, resolved relative to that crate, while test
and deployment tooling passes the same directory explicitly with
`--migration-dir`. The adapter build script tracks the directory so embedded
migration inputs rebuild correctly.

Schema changes must preserve the repository's adjacent-release compatibility
policy and update generated schema, migration checks, change classification,
container inputs, and native migration tests as required.

## Packaging and Public API

`hubuum-storage-core` is the experimental public extension surface. It, its
backend-neutral dependencies, and `hubuum-storage-conformance` form the
exact-version [storage adapter SDK](../storage_adapter_sdk.md). They must remain
usable by an out-of-tree adapter without root or adapter-private access.

An external crate can consume the types, implement the traits, and use the
published conformance harness. Method-specific query and collection behavior
is normative and exhaustive. Neutral audit-document construction and
validation by a second complete adapter remain follow-up work. See
[storage query semantics](query-semantics.md) and the
[method contract registry](method-contracts.toml).

`hubuum-storage-postgres` and the root `hubuum` crate are workspace-internal.
The conformance crate is a public development dependency and must not enter
production binaries. PostgreSQL schema, migrations, native clients, and
telemetry types remain in the PostgreSQL adapter rather than leaking into
`hubuum-storage-core`.

## Performance Contract

The semantic guarantees do not prescribe SQL, but they must not conceal
unbounded queries, repeated pool checkout, or accidental per-row work.
Query-budget tests protect representative database shapes. Benchmarks measure
the read boundary, audited mutation dispatch, and construction and consumption
of committed mutation outcomes separately from database round trips.

Returning an audit receipt does not require a follow-up audit read. An adapter
should derive it from the event write in the atomic mutation. Transactional
composition reuses one native connection and unit of work rather than opening
nested transactions for each constituent operation.

## Change Rule

Any storage-boundary change must update, as applicable:

1. the owning trait and DTOs;
2. the `StorageBackend` aggregate and exhaustive application dispatch;
3. every selectable adapter;
4. `semantic-coverage.toml`, `method-contracts.toml`, and shared conformance or
   compatibility scenarios;
5. backend-native consistency and failure tests;
6. this contract and the affected capability or maintainer guide; and
7. changelog, OpenAPI, migrations, CI classification, and container inputs
   when their external contracts are affected.

If these artifacts disagree, the implementation is not complete.
