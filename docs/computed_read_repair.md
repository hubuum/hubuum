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

## Pinned read and worker measurements

Measured on 2026-10-25 on a shared Linux host with the optimized release profile.
The synchronous baseline is `2742327aef454004d8aef33bbf52bde48cc78692` from #491;
queue/evaluation code is `84d7f9755b92dbd1301e305101604d003aedd9c9`.
The revised benchmark in this change has SHA-256
`7675a3257e5f21b45f086cb3c084aba6dd858c387830a6bedf44f09705a3c84b`.
Later migration-checkpoint, metric-contract, and resumed-task validation fixes do
not change these successful read/worker measurements. #492's streaming hash is not
included in either binary.

Both fixtures use PostgreSQL
`18.6-alpine3.24@sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873`,
20 stored objects, eight shared first-non-null definitions, and 1 KiB or 256 KiB
of unrelated string padding per object. A Docker CLI wrapper changes only the
fixture's container arguments: `--network host`, a free loopback-only port, and
`--tmpfs /var/lib/postgresql:rw,size=512m`; the default benchmark instead publishes
a container port and uses the image's normal data volume. PostgreSQL fsync remains
enabled. This controlled fixture removes shared-host disk and rootless port-forwarding
stalls; it is not a production durability, throughput, or multi-replica load test.

Each row uses 100 separate calls; input construction, cache invalidation, worker
execution, and response serialization are outside the read timer. CPU is process
CPU excluding PostgreSQL, aggregated from a 10 ms clock. Times are microseconds.

| Mode | Objects | Padding/object | Read p50 before → after | Read p95 before → after | Process CPU before → after |
| --- | --- | --- | --- | --- | --- |
| fresh | 1 | 1,024 B | 762 → 709 | 947 → 732 | 400 → 400 |
| fresh | 20 | 1,024 B | 1,081 → 1,168 | 1,293 → 1,400 | 800 → 800 |
| fresh | 1 | 262,144 B | 2,530 → 2,826 | 2,711 → 3,594 | 2,300 → 2,600 |
| fresh | 20 | 262,144 B | 37,004 → 36,991 | 38,477 → 43,714 | 36,200 → 37,600 |
| stale | 1 | 1,024 B | 2,029 → 3,317 | 2,476 → 3,995 | 800 → 500 |
| stale | 20 | 1,024 B | 10,344 → 3,664 | 11,190 → 4,530 | 3,700 → 1,500 |
| stale | 1 | 262,144 B | 7,160 → 3,967 | 7,800 → 4,964 | 5,500 → 1,400 |
| stale | 20 | 262,144 B | 108,401 → 9,607 | 114,238 → 10,196 | 92,100 → 7,600 |

The large 20-object stale-page median falls from about 108 ms to 9.6 ms because
repair runs afterward. The smallest one-object stale case is slower (2.0 to
3.3 ms): durable task admission has bookkeeping cost, so this is not a universal
latency improvement. Fresh reads perform the same freshness work; the large-page
median remains about 37 ms. Small-case and tail timing vary on this shared host;
these samples do not establish a precise fresh-read percentage change.

Worker drain measurements include task claim, locked repair transactions, task
completion, and confirmation that the active queue is empty. Only samples with
observed admitted identities contribute to these worker percentiles. An empty
`SKIP LOCKED` claim alone is not evidence of an empty queue; the harness checks
active tasks and retries. Freshness is asserted for the identities actually
observed in the queue, because a best-effort admission can leave part of a page
unadmitted. The check itself does not queue the other objects.

| Objects | Padding/object | Admitted samples | Worker p50 | Worker p95 | Maximum observed queued identities |
| --- | --- | --- | --- | --- | --- |
| 1 | 1,024 B | 100 | 11,769 µs | 14,073 µs | 1 |
| 20 | 1,024 B | 100 | 19,874 µs | 23,563 µs | 20 |
| 1 | 262,144 B | 100 | 16,660 µs | 19,783 µs | 1 |
| 20 | 262,144 B | 100 | 73,372 µs | 78,132 µs | 20 |

Every controlled-fixture stale sample admitted its complete page; observed misses
and partial admissions were zero, and every drain ended with zero active tasks.
Fresh reads use seven queries and one checkout. Stale reads use 14 queries and two
checkouts for both page sizes, versus 16/35 queries in the synchronous one/20-object
baseline. Worker queries are separate. Response sizes and snapshot staleness
metadata are unchanged.

A separate run with the same localhost networking but the normal disk-backed
container volume observed no queued identity immediately after 74–96 of 100
stale reads, depending on the case. The 25 ms admission deadline intentionally
bounds those waits; lost acknowledgements mean this observation is not proof
that every such admission rolled back. Later reads retry. Those disk-backed
results must not be presented as successful repair speedups. Use the admission
outcome counters and backlog to evaluate a deployment's database latency.
