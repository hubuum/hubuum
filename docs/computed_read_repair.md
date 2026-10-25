# Computed read repair

A stale computed read evaluates shared fields from its source snapshot and returns
those values, errors, evaluation revision, and `materialization_stale: true`.
Persistence happens afterward through the existing durable task workers. The
response never substitutes an old cached value for live evaluation. A later read
is fresh only when a materialization matches its source and definition revision.
Personal computed fields remain request-local and are never queued.

## Bounds and admission

PostgreSQL stores at most one active internal `reindex` task with a
`computed_read_repairs` payload across all replicas. Its payload contains only
validated class/object identities, with at most 4,096 distinct object IDs. Repeated
readers coalesce; a newer observed class replaces the same object's queued class.
An admission never evicts existing work to make room for a new identity.

Admission tries a transaction advisory lock without waiting and uses
`FOR UPDATE SKIP LOCKED` for an existing task. The complete admission, including
pool acquisition, has a 25 ms deadline. Queue contention, capacity, database
failure, or timeout leaves the evaluated response unchanged; another stale read
can retry. A timeout can lose the acknowledgement of a successful commit, so
coalescing also handles that uncertainty. There is no detached per-request task.

## Execution and consistency

The normal task lease selects one driver. A database transaction advisory lock
also serializes repair transactions across replicas, including an old driver
whose lease expired. Each batch contains one class and at most
`computed_reindex_batch_size` identities. The driver takes the class definition
lock, checks its task claim, locks the class row, then locks objects in ascending
ID order. It reloads authoritative source data and definitions inside that
transaction. The class capability cannot be reused for another class.

Materializations, removal of completed identities, and progress commit together.
The claim and execution deadline are checked again after evaluation. A lost or
expired claim rolls the entire batch back. Deleted classes/objects and objects
moved out of the queued class are skipped. A subsequent read in a new class can
queue its current identity. Concurrent definition changes use the existing class
lock protocol and cannot make an old evaluation current.

## Upgrade, failure, restart, and shutdown

Expired repair leases are requeued with their remaining identities and committed
progress. Recovery assigns a new token; the previous token cannot commit. A
worker error fails the task through the normal worker machinery and redacts its
remaining payload. Later stale reads can request repair again; there is no
unbounded immediate retry of a permanently failing identity. Cancellation and
execution deadlines likewise stop derived work without changing source objects.

Graceful shutdown follows the existing task worker drain policy. Forced shutdown
or process loss leaves a durable queue for lease recovery. This is an operational upgrade requirement: API-only deployments
need a worker-role process to drain it; when workers are unavailable, reads still
evaluate correctly and admission remains bounded.

Full backups retain terminal task history, not active work or computed caches.
Restore rebuilds derived computed state from restored definitions and objects;
it does not replay this queue. The single migration adds the queue bound and
active-task uniqueness constraints without backfilling objects. Stop workers
before rolling this migration back: rollback removes outstanding internal repair
tasks, and the previous synchronous read path can recreate their derived values.

## Diagnostics

`computed.repair.enqueue` measures bounded admission; `computed.repair` and the
existing lock/hash/evaluation spans describe worker work. Diagnostics contain
counts and bounded outcome labels, never source documents. The existing repair
counter distinguishes `queued`, `coalesced`, `queue_full`, `queue_busy`,
`enqueue_failure`, `enqueue_timeout`, `success`, and `failure`. Task progress and
batch diagnostics expose remaining backlog (`total_items - processed_items`).
Success counts committed batches; skipped deleted/moved identities still advance
task progress. A completed repair is not a promise that a concurrent later write
will leave the next snapshot fresh.
