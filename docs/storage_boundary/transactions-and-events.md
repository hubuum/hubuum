# Transactions and Side Effects

Hubuum exposes a backend-neutral unit of work for application workflows that
need to compose resource operations atomically. It does not expose a database
connection, a Diesel transaction, or a general query interface.

This is a mandatory part of every selectable `StorageBackend`.
The normative six-part guarantee is defined in the
[storage contract](contract.md); this guide expands its transaction and event
semantics.

## The Shape

```text
TransactionalStorage::transaction(EventContext, callback)
                            |
                            v
                  opaque StorageTransaction
             /          /       |       \          \
  collections()  classes()  objects()  class_relations()  object_relations()
             \          \       |       /          /
                            v
              one backend-native transaction
                            |
                  commit `Ok` / roll back `Err`
```

The accessors return crate-owned operation types with inherent methods. This
makes operations discoverable in an editor without creating a mirrored set of
`TxCollectionStorage`, `TxClassStorage`, and similar traits.

A callback must compose through the transaction passed to it. Starting another
unit of work through the outer storage handle creates an independent native
transaction and is unsupported; with a bounded pool it can also deadlock while
waiting for a second connection.

The ordinary lifecycle traits remain the implementation contract underneath
those types. An adapter therefore implements each resource semantic once and
provides a connection-scoped execution path for its transaction object.

## When to Compose and When to Add an Operation

Use a unit of work when application logic needs to combine already-safe
resource primitives and the combination has no hidden backend invariant. For
example, creating two objects and a relation between them is a natural
composition.

Add or retain one operation-shaped trait method when the backend must own a
state machine, lock protocol, specialized query, or consistency rule. Examples
include:

- task claims, completion artifacts, and lease-checked transitions;
- restore application and recovery;
- strict and best-effort import execution;
- computed rebuilds guarded by a live task claim;
- permission mutation with owner-revision advancement; and
- event delivery, retention, and worker coordination.

The decision is semantic, not based on the number of SQL statements.

```text
Can the use case be expressed entirely with existing safe resource operations?
            |                                      |
           yes                                    no
            |                                      |
 use StorageTransaction             add an operation-shaped capability

Does the backend need to keep a hidden invariant across the whole operation?
            |                                      |
           no                                     yes
            |                                      |
 composition remains valid           keep the invariant in one backend call
```

This keeps the trait boundary from growing for every orchestration while
preventing application code from rebuilding backend state machines.

## Audit and Event Guarantee

`TransactionalStorage::transaction` requires one `EventContext`. Callers do
not pass an optional event context to individual transactional mutations.
`TransactionalCollections`, `TransactionalClasses`,
`TransactionalClassRelations`, `TransactionalObjects`, and
`TransactionalObjectRelations` forward the required context automatically.

For every successful transactional mutation:

1. The adapter performs the state mutation.
2. The adapter appends the mutation's durable audit event using the inherited
   context.
3. Both changes are part of the same native transaction.
4. The backend publishes follow-on transactional notifications, if any, only
   as part of that commit.

If the callback returns `Err`, state, audit events, and transactional
notifications roll back together. A backend that commits the state but loses
the audit event does not satisfy the contract.

External transport is not executed inside this transaction. After commit, the
fan-out capability creates durable delivery rows, and delivery workers call the
configured sinks with at-least-once semantics. Notifications reduce latency;
polling the durable event and delivery state remains the recovery path.

The lower-level lifecycle traits also require an `EventContext`; there is no
optional-context escape hatch in the ordinary storage contract. A committed
resource mutation returns `MutationOutcome::Committed` with an `AuditReceipt`
identifying the durable event written in the same atomic operation. A genuine
no-op returns `MutationOutcome::Unchanged` and appends no event.

Fixture compatibility helpers use explicit system attribution. Import and
restore behavior belongs to `MaintenanceStorage`, which is a separate surface
because those workflows preserve or reconstruct durable history rather than
masquerading as ordinary user mutations.

## Example

```rust
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    StorageObjectCreate, StorageObjectRelationCreate,
    StorageObjectRelationCreateSelector, TransactionalStorage,
};

storage
    .transaction(EventContext::system(), |transaction| {
        Box::pin(async move {
            let left = transaction
                .objects()
                .create(&left_class, left_command)
                .await?
                .into_value();
            let right = transaction
                .objects()
                .create(&right_class, right_command)
                .await?
                .into_value();
            let prepared = transaction
                .object_relations()
                .prepare(StorageObjectRelationCreateSelector::Explicit(
                    StorageObjectRelationCreate::new(
                        left.id(),
                        right.id(),
                        class_relation_id,
                    ),
                ))
                .await?;
            transaction.object_relations().create(&prepared).await
        })
    })
    .await?;
```

The boxed callback future is the only allocation introduced by the contract.
It makes the callback lifetime expressible on the workspace MSRV while keeping
the result strongly typed. Individual operations remain ordinary static async
trait calls.

## Backend Requirements

A complete adapter must:

- implement `TransactionalStorage` and every trait aggregated by
  `StorageBackend`;
- provide one native atomic unit for the complete callback;
- implement every exposed transaction accessor;
- reuse the same validation, resolution, revision, relation-cardinality, and
  event semantics as calls outside a transaction;
- serialize access when its native transaction cannot execute concurrent
  operations safely;
- commit only when the callback returns `Ok`;
- roll back state and side effects when it returns `Err` or native commit
  fails; and
- convert its native error once into `StorageError` at the adapter boundary.

An adapter may use a connection, session, batch, compare-and-swap log, or other
native mechanism internally. Those types never cross the contract.

## Cancellation

Dropping the future requests cancellation. An adapter must not report a
successful commit after the callback returned an error. If its native system
cannot cancel work immediately, it must still preserve the externally visible
commit-or-rollback contract and document any background completion behavior.

The PostgreSQL adapter holds one checked-out connection for the unit of work.
Dropping or failing the native transaction rolls it back through the adapter's
runtime transaction mechanism.

## Observability and Performance

The unit of work is one logical storage entrypoint with the bounded labels
`transactions` and `run`. The PostgreSQL runtime separately measures pool
checkout and native transaction duration. Constituent calls do not create a
second set of logical entrypoint metrics; query-level diagnostics remain an
adapter concern.

The transaction reuses one native connection. It does not add a database
round trip to each constituent operation. Compared with calling several
independent audited operations, it removes repeated pool checkouts and nested
`BEGIN`/`COMMIT` pairs. The contract adds one boxed callback future and a small
in-process serialization guard per constituent call.

Benchmarks and query-budget tests must protect both single-operation latency
and composed transaction behavior. A backend should optimize its private
execution without changing the contract.

## Compatibility Evidence

The shared transaction scenario runs against PostgreSQL and the deterministic
memory model. It:

- commits a collection, two classes, a class relation, two objects, and an
  object relation through one callback;
- verifies committed state through ordinary capability reads and confirms one
  durable audit trail for every resource family;
- repeats all five mutation families and returns an application error; and
- verifies that neither state nor any family-specific audit event survives.

Backend-native tests remain responsible for isolation, connection loss,
driver cancellation, commit failure, notification visibility, and native
locking behavior.
