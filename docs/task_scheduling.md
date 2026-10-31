# Shared task start scheduling

Every successful claim commits weighted class rotation and principal service
history in the same transaction as its fenced task lease. These deployment
records survive logical restore. Worker availability is discarded during restore;
new claims register against the reconciled control generation.

## Selection and guarantees

Ordinary selection visits task kinds using positive integer weights, then chooses
the eligible principal least recently served in that class. Ties use authoritative
creation time and task ID. Retries keep their original creation time. Empty polls,
failed transactions and unavailable capacity do not advance either fairness lane.

A separate promotion lane selects the oldest eligible overdue task. Its proposed
default is one promotion per eight successful claims, after five minutes of age.
Promotions obey every execution cap, worker restriction and reservation. They
update principal service history but do not advance ordinary rotation. A promotion
requires at least the current interval since the last promotion, including after
policy changes. When no overdue task is eligible, ordinary selection proceeds.
An entirely overdue backlog therefore leaves at least seven of eight starts to
ordinary class/principal selection with the default interval.

These are start opportunities, not CPU-time shares. With a finite eligible backlog,
positive weights, recurring supporting workers and releasing capacity, each
eligible principal and task receives opportunities. Permanently occupied caps,
locked tasks or unsupported classes do not satisfy those assumptions. Long tasks
can delay starts; the scheduler neither estimates their cost nor preempts them.
Class weights describe ordinary starts. Promotions can change total start shares
by at most their bounded fraction. Equal duration and equal CPU consumption are
not assumed.

PostgreSQL orders principal service history before reading task queue heads, so
ordinary selection can stop once the first eligible service-history tie is
resolved. New principals with equal history still compare creation time and task
ID. A due promotion compares overdue heads across eligible groups; an indexed
check skips that comparison when no queued task is old enough. Both paths retain
`SKIP LOCKED`, capacity checks and the same transactional scheduling counters.

## Worker availability and reservations

Each worker loop generates one `TaskWorkerId` for its single execution slot and
reuses it on every claim request. A request advertises a validated nonempty set
of supported kinds. Built-in workers support every kind. Registration and successful
lease heartbeats renew availability using the backend clock and task lease duration.
A worker must finish or lose its fenced claim before claiming again. Process
restarts create fresh identities; expired registrations stop contributing capacity.

Eligible capacity is the smaller of live registered slots and the configured
execution-slot ceiling. A hard reservation is active only when that capacity can
hold all reserved slots plus one ordinary slot, enough live workers support internal
repair, and at least one supports ordinary work. Ordinary tasks cannot borrow idle
reserved slots, including the last reserved internal-capable worker. Per-kind
execution caps also apply to internal repair. Existing principal, collection and
ordinary deployment caps remain authoritative.

A single configured slot shares execution between ordinary and internal work.
It cannot guarantee prompt repair during a nonpreemptible task. The same fallback
applies when the eligible pool shrinks below the reservation. Existing claims stay
occupied; no task is preempted. Returning workers restore hard reservations on
the next registration. Administrator snapshots derive this consistently from
shared worker leases and report `single_slot`, `insufficient_workers`,
`insufficient_internal_workers` or `no_ordinary_workers`. `unsupported_kinds`
separately identifies classes with no live supporting worker.

## Configuration and upgrade

The administrator task-capacity policy now requires a `scheduling` object:

```json
{
  "execution_slots": 64,
  "internal_reserved_slots": 1,
  "promotion_interval": 8,
  "overdue_seconds": 300,
  "weights": [
    {"kind": "import", "weight": 1},
    {"kind": "export", "weight": 1},
    {"kind": "backup", "weight": 1},
    {"kind": "reindex", "weight": 1},
    {"kind": "remote_call", "weight": 1},
    {"kind": "schema_validation", "weight": 1},
    {"kind": "schema_profiling", "weight": 1}
  ]
}
```

Slots range from 1–1,024; reservations are positive and must leave ordinary
capacity when more than one slot is configured. Promotion intervals range from
2–1,024, overdue age from 1–604,800 seconds, and weights from 1–32 with each kind
present exactly once. Updates use the existing revision-checked policy endpoint.
Weight changes interpret the durable rotation position modulo the new rotation;
service history remains intact. Reduced capacity drains existing claims first.

This is an offline upgrade. Stop API and worker replicas, take a database backup,
run `hubuum-admin --migrate` and reconcile split-role grants, then start matching
binaries. The migration creates the queued-task index and protected scheduler
state and initializes the scheduling policy. Older workers cannot enforce these
rules and must not coexist with upgraded workers. Retain the pre-upgrade database
snapshot for rollback. Logical restore preserves fairness history but resets
worker leases. Upgrade all ten storage SDK crates to 0.15 together and migrate
adapters to `StorageTaskClaimRequest`; see the
[SDK guide](storage_adapter_sdk.md#upgrading-from-014-to-015).

## Measurement

The acceptance profiles and P3/P4 targets are defined in
[lifecycle budgets](performance/lifecycle_budgets.md). The eight-claim default
remains subject to those workload results. A warm SQL selection probe alone does
not establish end-to-end claim latency, utilization or constrained-profile acceptance.

The benchmark separates fixture preparation from measured workers. Preparation
loads the checked-in lifecycle dataset, initializes immutable identity and capacity
evidence, adds the requested queued backlog, and records a protected fixture marker.
Worker processes reject mismatched dataset, workload, cap, reservation or overdue
settings. Each process has an independent eight-connection pool and stable slot
identities. The shared profile runs two processes with two slots each.

The local harness supports PostgreSQL limits of 2 CPUs/2 GiB or 4 CPUs/4 GiB
and worker limits of 2 CPUs/512 MiB per process when the container runtime can
enforce them. Container inspection verifies the applied limits. Five fresh database copies are
measured for each of 1k, 10k and 100k queued tasks, a saturated overdue backlog,
and the timed skewed-duration workload with a per-principal execution cap of one.
The 10k-backlog immediate runs measure 12k successful claims, leaving at least
10k ordinary selections after bounded promotions for the start-share comparison.
Queued timestamps reset to the declared initial age for each fresh copy; tasks
continue aging normally during the run. Immediate runs exclude 100 warm-up claims per process; timed runs first complete
a separate 30-second warm-up window. Every sample, preparation inventory, container
configuration and candidate-selection plan is retained. Budget failures remain in
the report. A completed measurement run does not by itself certify P3/P4.

Run the harness locally; use `--unlimited-cpu` when the runtime cannot enforce
CPU limits. Database and worker memory limits remain enforced and inspected:

```bash
cargo build --locked --release -p hubuum-scale-benchmark
cargo build --locked --release --features embedded-migrations --bin hubuum-admin
python3 -I -S tests/python/run.py integration scheduling-benchmark \
  --profile shared --unlimited-cpu --output target/scheduling-shared
```

`--unlimited-cpu` produces diagnostic evidence without CPU controls.

## Recorded local diagnostics

The [2026-10-28 diagnostic summary](performance/scheduling_diagnostics.json)
retains all five repeats of both profiles, including failures, per-principal
starts, cap-blocked time, container limits and raw-artifact hashes. Release
binaries use the complete lifecycle fixture. Worker processes call production
storage claim and terminalization operations and replenish the queue; timed
task bodies sleep for the prescribed 100 ms or 30 s. Empty polls retry after
5 ms. This measures the claim path and prescribed occupancy workload.
These original results precede the ordered queue-head selection optimization
measured in the [follow-up comparison](#queue-head-selection-follow-up).

The table gives the median of five per-run percentiles in milliseconds. The
pass count checks each repeat separately against claim p95 ≤25 ms, p99 ≤100 ms,
lock-wait p95 ≤10 ms and the round-trip limit. It does not certify full P3.

| Profile | Workload | Claim p95 ms | Claim p99 ms | Lock p95 ms | Latency passes |
| --- | --- | --- | --- | --- | --- |
| local | 1k queued | 5.945 | 7.248 | 0.066 | 5/5 |
| local | 10k queued | 6.258 | 7.346 | 0.066 | 5/5 |
| local | 100k queued | 6.299 | 25.482 | 0.066 | 5/5 |
| local | 10k overdue | 6.276 | 7.409 | 0.067 | 5/5 |
| local | Timed, principal cap 1 | 8.720 | 26.490 | 0.118 | 4/5 |
| shared | 1k queued | 17.492 | 21.890 | 10.777 | 2/5 |
| shared | 10k queued | 17.830 | 20.053 | 10.960 | 0/5 |
| shared | 100k queued | 18.400 | 120.957 | 11.371 | 0/5 |
| shared | 10k overdue | 17.699 | 20.044 | 10.887 | 0/5 |
| shared | Timed, principal cap 1 | 27.207 | 56.189 | 2.559 | 2/5 |

- local: 24/25 repeats pass the numeric latency checks.
- local timed utilization: 99.40%–99.46% (median 99.45%).
- shared: 4/25 repeats pass the numeric latency checks.
- shared timed utilization: 99.41%–99.54% (median 99.52%).

Ordinary-share checks passed 20/20; promotion-spacing checks passed 50/50; timed utilization checks passed 10/10.

The local timed repeat with claim p95 of 26.733 ms remains a failure, and
shared-worker claim and lock-wait misses remain visible in the pass counts and JSON.
Utilization uses one ordinary slot locally and three non-reserved slots across
the shared workers. Starts and cap-blocked time are reported separately from
occupied execution time; they do not establish CPU-time fairness.

The earlier join-based query visited all 10,000 queued tasks and measured
claim p95/p99 of 91.218/224.665 ms in its completed 10k run. Those results are
retained. Correlated accounting eligibility lets the ordered index stop at queue
heads: the corrected fixture probes read 64 task candidates. The native row-read
regression guards this case, not a general bound including locking and retries.

P3/P4 acceptance remains open. This host cannot enforce rootless CPU quotas;
database and worker memory caps were enforced and inspected. Other development
work shares the host, storage medium and RTT were not independently established,
and shrinking-pool utilization has correctness coverage but no timed measurement.
The recorded latency misses are not waived by these limitations. The eight-claim
promotion interval remains a proposed default pending acceptance review.

## Queue-head selection follow-up

The [paired comparison](performance/scheduling_head_selection.json) retains five
fresh database copies per variant, alternating the original and optimized SQL
with the same release worker binary and complete lifecycle fixture. Each repeat
uses two worker processes, four registered slots, one internal reservation and
10,000 queued tasks. It excludes 200 warm-up claims and measures 2,000 successful
claims. This task's builds and correctness tests were stopped during measurement;
independent compilation in another worktree was observed on the shared host.

Values below are medians of five per-run percentiles, in milliseconds. Every
repeat, including failures, remains in the report.

| Selection | Claim p95 ms | Claim p99 ms | Lock p95 ms | Numeric latency passes |
| --- | --- | --- | --- | --- |
| Original | 21.670 | 181.499 | 13.077 | 0/5 |
| Ordered queue heads | 13.791 | 36.127 | 8.003 | 4/5 |

All five optimized repeats met the 10 ms lock-wait p95 target. One still missed
the 100 ms claim p99 target at 112.556 ms. Five database round trips remain
necessary; there were no failed polls. This is local diagnostic evidence, not
constrained-profile acceptance or a replacement for the original workload matrix.

The warm generic-plan probe returned two ordinary queue heads instead of 64.
The earliest queued timestamp uses the existing status/time index: a parameterized
`EXISTS` age check was rejected because its generic plan scanned the complete
20,000-row fixture. Native regressions exercise generic plans with new principals,
existing service history, a due promotion with no overdue work, and overdue work.
The live-row checks do not bound dead index entries or retries; the retained
timestamp-reset probe demonstrates that limitation. General candidate-work bounds,
timed shrinking-pool utilization and CPU-constrained profiles remain open. These
2,000-claim runs also do not meet P4's 10,000 ordinary-start sample requirement.

### PostgreSQL version compatibility

The latency comparison above measures commit `1350a86a` before the PostgreSQL 16
compatibility fix. CI found that PostgreSQL 16 inspected 64 ordinary queue heads
because it could not propagate ordering through the materialized group CTE.
The group query now explicitly remains inline, preserving its ordering for the
outer selection. The existing native row-read limits remain unchanged.

The [compatibility plan report](performance/scheduling_postgres_compatibility.json)
retains both versions of the query on PostgreSQL 16 and 18. Ordinary selection
reads two task rows; a due promotion with no overdue work reads three, including
the minimum-timestamp probe. All four plan scenarios pass on both versions after
the fix. This verifies query plans and live-row counts; it adds no latency
measurement or general physical-work bound to the earlier comparison.
