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
TransactionStorage::with_transaction(EventContext, callback)
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

`TransactionStorage::with_transaction` requires one `EventContext`. Callers do
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
restore behavior belongs to explicit workflow capabilities because those
operations preserve or reconstruct durable history rather than masquerading as
ordinary user mutations.

## Context-Free Mutation Inventory

The following review-maintained list is intended to cover every
`StorageBackend` method that may durably write without accepting an
`EventContext`. They are deliberately outside the ordinary audited-mutation
category. The current guard proves that each listed name is a real trait method;
it does not yet infer write effects from method bodies or mechanically prove
the reverse direction. That inference is deferred. Until it exists, reviewers
must add every new context-free writer to `semantic-coverage.toml`, place it in
one of these semantic categories, and review it here. An ordinary resource or
configuration mutation is not eligible for an exception.

- Best-effort observation: `AuthenticationStorage::authenticate_bearer_token`
  may throttle-write token use time. This does not change authorization or
  lifecycle state, and a valid authentication does not fail solely because the
  timestamp could not be refreshed.
- Bootstrap and provider reconciliation:
  `LocalIdentityCredentialStorage::bootstrap_default_admin` runs only while no
  actor can yet exist; `IdentityScopeStorage::ensure_identity_scope` is
  idempotent provider configuration reconciliation;
  `ExternalIdentityStorage::mark_external_sync_attempted` is synchronization
  telemetry; and `ExternalIdentityStorage::sync_external_user` is an
  identity-provider reconciliation operation that emits a system-attributed
  audit event and returns its `MutationOutcome`.
- Workflow state machines:
  `ComputedFieldStorage::request_computed_field_rebuild`,
  `ComputedFieldStorage::execute_computed_field_rebuild`, and
  `TaskQueueStorage::create_task` create or advance durable work whose task row,
  submitter, claim, and task-event stream carry its provenance. Worker-only
  transitions are `TaskExecutionStorage::claim_next_task`,
  `TaskExecutionStorage::renew_task_lease`,
  `TaskExecutionStorage::recover_expired_task_leases`,
  `TaskExecutionStorage::append_task_event`,
  `TaskExecutionStorage::update_task_state`,
  `TaskExecutionStorage::complete_task`, `TaskExecutionStorage::fail_task`,
  `TaskExecutionStorage::purge_expired_export_outputs`, and
  `TaskExecutionStorage::purge_expired_backup_outputs`.
- Event coordination:
  `EventDeliveryAdministrationStorage::release_event_delivery_for_retry` and
  `EventDeliveryAdministrationStorage::mark_event_delivery_dead` are explicit
  administrator interventions on derived delivery state. Worker protocols use
  `EventDeliveryWorkerStorage::claim_event_delivery_batch`,
  `EventDeliveryWorkerStorage::mark_event_delivery_succeeded`,
  `EventDeliveryWorkerStorage::mark_event_delivery_failed`,
  `EventFanoutStorage::process_event_fanout_batch`,
  `EventRetentionStorage::claim_event_retention_batch`, and
  `EventRetentionStorage::complete_event_retention_batch`. Claims and task or
  delivery state provide the concurrency evidence; none of these methods is an
  ordinary domain lifecycle shortcut.
- Retention: `TokenRetentionStorage::purge_expired_tokens` applies configured
  expiry and emits the token-purge semantics owned by the retention worker.
- Maintenance: `RestoreStorage::stage_restore`,
  `RestoreStorage::expire_restore_stage`,
  `RestoreStorage::start_restore_draining`, `RestoreStorage::apply_restore`,
  `RestoreStorage::fail_restore_and_resume`,
  `RestoreStorage::resume_maintenance_without_restore`,
  `RestoreStorage::resume_terminal_restore`,
  `RestoreStorage::tick_restore_coordinator`, and
  `RestoreStorage::remove_restore_instance` form the capability-authenticated
  restore and coordinator protocol. `ImportStorage::apply_import_strict`,
  `ImportStorage::apply_import_best_effort`, and
  `ImportStorage::record_import_results` preserve or reconstruct imported
  state and results under the typed import workflow.

`BackupSnapshotStorage::capture_backup_snapshot` is not listed because it only
reads a consistent projection. `ImportStorage::preflight_import` executes
against rollback-only state and does not durably write. `ExecutionStorage`
scope changes are native execution context, not durable storage mutations.
Personal and shared computed-field definition lifecycle, administrator local
password reset, and every other ordinary mutation require `EventContext` and
return `MutationOutcome`.

## Example

```rust
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    StorageObjectCreate, StorageObjectRelationCreate,
    StorageObjectRelationCreateSelector, TransactionStorage,
};

storage
    .with_transaction(EventContext::system(), |transaction| {
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

- implement `TransactionStorage` and every trait aggregated by
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
- log native diagnostic context inside the adapter and convert the failure once
  into a safe `StorageError` at the adapter boundary.

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
`transaction` and `with_transaction`. The PostgreSQL runtime separately
measures pool checkout and native transaction duration. Constituent calls do
not create a second set of logical entrypoint metrics; query-level diagnostics
remain an adapter concern.

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
