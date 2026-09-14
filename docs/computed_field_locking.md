# Computed-field transaction lock protocol

The PostgreSQL adapter protects backfill and read-repair object batches with
transaction-bound capabilities in
`crates/hubuum-storage-postgres/src/operations/computed_fields/locking.rs`.
The invariant is: before explicitly locking any batch object, the same
transaction holds the class's shared definition advisory lock and its row's
`FOR KEY SHARE` lock. Materialization uses only objects reloaded under those
locks, filtered to the capability's class, and the current definition revision.

This prevents the class/object lock inversion identified in issue #374.
An object writer can hold a class row while waiting for an object row. A batch
must therefore acquire the class lock before its object locks, including the
class key-share lock that a later materialization foreign-key check needs.
Read repair must also acquire the definition advisory lock before object rows:
a definition writer can hold the advisory lock while waiting for the class.

## Capability boundary

`with_computed_transaction` starts a transaction through the existing runtime's
`with_transaction`. Its callback receives a `ComputedTransaction`, which owns
the exclusive borrow of the connection. It exposes no raw connection,
arbitrary SQL callback, transaction controls, or savepoints.

The entry capability is consumed when acquiring `LockedComputedClass`. Only
that class capability can acquire `LockedComputedObjects`; locking objects
consumes the class capability too. Fields and constructors are private. The
caller cannot reacquire a different class after taking object locks, change a
capability's identity, or substitute source objects from another transaction.
The higher-ranked callback prevents returning either capability beyond the
transaction. Test-only reexports under `integration-test-support` allow
compile-fail doctests to exercise this actual API, alongside a compiling
positive example. These exports are absent from production builds.

Dropping a Rust value does **not** release a PostgreSQL transaction lock.
Success commits; errors roll back the transaction and its writes together.
There is no rollback-to-savepoint operation while a capability exists, so a
token cannot survive a rollback that released its database locks. Nested
transactions and raw-connection constructors are deliberately absent from this
protocol. No Rust `unsafe` is involved.

## Operation ordering

The table describes explicit acquisitions and relevant subsequent writes.
Foreign keys and triggers can acquire additional database locks.

| Operation | Acquisition and write order | Enforcement |
| --- | --- | --- |
| Canonical object create | Shared definition advisory lock; resolved class `FOR UPDATE`; object insert; optional schema evidence; materialization; audit/event writes | Existing operation implementation and runtime revision checks |
| Canonical object update or patch | Shared definition advisory lock; resolved class `FOR UPDATE`; resolved object `FOR UPDATE`; revision and schema checks; object write; schema evidence; materialization; audit/event writes | Existing operation implementation and runtime identity/revision checks |
| Canonical object delete | Shared definition advisory lock; class `FOR UPDATE`; object `FOR UPDATE`; revision check; delete with cascades; audit/event writes | Existing operation implementation |
| Computed backfill batch | Shared definition advisory lock; runnable task claim `FOR UPDATE`; class `FOR KEY SHARE`; ascending object IDs `FOR UPDATE`; computation-state/revision check; materialization writes; runnable claim recheck | Capabilities for class/object order; database claim fencing and revision checks |
| Ordinary enriched-read snapshot | Shared definition advisory locks in ascending class ID order; definitions, state and cache reads; end read-only snapshot | Existing snapshot implementation; no object row locks or materialization writes |
| Read-repair batch | Shared definition advisory lock; class `FOR KEY SHARE`; ascending object IDs `FOR UPDATE`; current computation state and definitions; materialization writes | Capabilities for class/object order and source ownership |
| Shared definition create/update/delete or manual rebuild request | Exclusive definition advisory lock; class `FOR SHARE`; definition row lock where applicable; computation-state changes; active-task inspection/enqueue; audit/event writes where applicable | Existing operation implementation plus database revision/uniqueness constraints |
| Backfill completion | Shared definition advisory lock; runnable task claim `FOR UPDATE`; fenced computation-state update; terminal task/event writes | Existing claim and target-revision fencing; no object locks |

Personal definition creation uses an advisory lock scoped to the class and
owner; updates and deletes lock the applicable definition row. These operations
read the class without an explicit row lock and do not maintain shared
materializations or acquire batch object locks.
Import object writes acquire the shared definition advisory lock, validate
against a locked class, then insert or lock/update the object and materialize
it. Imports can also hold task claims and locks from earlier commands in their
transaction. Restore, schema-evolution and class-level orchestration retain
their existing transaction protocols. These paths are inspected here, but are
not converted into the batch capability API.

## Runtime checks and limits

Backfills retain the configured batch size, original object upper bound,
ascending ID cursor, shared advisory lock, and class key-share lock strength.
Task lease/token, cancellation/deadline, active-task identity and evaluation
revision checks remain in the transaction; the runnable claim is checked again
after materialization. Types do not replace this fencing.

Read repair groups stale IDs by class and splits them using the same configured
batch size. Each batch gets its own transaction and releases all its locks
before the next one. Its query filters by both object IDs and the locked class;
deleted objects or objects moved by outside writers are skipped. Definitions
and state are loaded once per batch, and source data is reloaded from locked
rows. The original response retains its consistent read snapshot and live
fallback; repair failures remain best-effort and observable. Earlier successful
repair batches remain committed if a later batch fails.

The protocol adds no global lock or stronger class row lock. Different classes
remain independent. Same-class batches retain compatible shared advisory and
key-share locks, while overlapping object writes serialize at their row locks.
Query-budget tests cover batched enrichment, and deterministic concurrency
tests cover backfill/update, read repair versus class and definition locks,
and definitions changing between the read snapshot and repair.

## Escape hatches and database limits

Canonical object writes, imports, definition mutations, and other adapter
operations still use raw PostgreSQL connections internally. Their materializer
and advisory-lock helpers are implementation escape hatches, not proofs of
prior acquisitions. The batch protocol encapsulates its connection throughout
its operation; an unrestricted marker passed alongside a connection would not
provide this guarantee. Extending the protected scope requires bringing the
whole operation under that boundary, including transaction control.

PostgreSQL clients outside this API, migrations, triggers, foreign keys, lock
upgrades, and earlier locks in composed transactions are not statically
controlled. Dynamic ordering across multiple classes and objects remains an
explicit runtime/design responsibility. This protocol enforces the covered
explicit acquisitions; it does not prove that the database is deadlock-free.
