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

A separate promotion lane selects the oldest eligible overdue task. Its default
is one promotion per eight successful claims, after five minutes of age.
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

## Public diagnostics contract

The scheduler extends `scheduling` in `GET /api/v1/admin/task-capacity` and
`task_capacity.scheduling` in `GET /api/v1/admin/config`. Both routes require
an unscoped human administrator through the configured authorization backend.
Ordinary users, scoped tokens and service accounts cannot inspect deployment
scheduling state. The projection contains aggregate counters and task kinds,
without worker identities, principal identities or other users' queued tasks.

`mode` is either `{"mode":"reserved"}` or `{"mode":"shared","reason":...}`.
The shared reason is one of the four bounded values documented above;
`unsupported_kinds` independently identifies kinds with no live supporting worker.
Fallback does not make an unsupported kind executable. `ordinary_position` is a
position in the scheduler's weighted rotation, not a task's queue position.
Counters describe successful starts, not completed work or CPU consumption.

These diagnostics do not change the existing [task response](task_api.md), invent
per-task wait reasons, or promise queue position or ETA. A queued task remains
inspectable by its authorized durable reference. Ending a client's local wait
neither cancels that task nor changes its execution deadline or output lifetime.

This is the applicable #545 contract review for #507. Backend scheduling contracts
cover fallback/unsupported-kind behavior; the administrator endpoint tests cover
policy projection and denial to ordinary users. Broader phase/progress models,
consumer helpers and provider-wait contracts remain separate #545/#516 work.

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
[lifecycle budgets](performance/lifecycle_budgets.md). The constrained matrix below
supports the eight-claim default for those workloads. A warm SQL selection probe alone does
not establish end-to-end claim latency, utilization or constrained-profile acceptance.

The benchmark separates fixture preparation from measured workers. Preparation
loads the checked-in lifecycle dataset, initializes immutable identity and capacity
evidence, adds the requested queued backlog, and records a protected fixture marker.
Worker processes reject mismatched dataset, workload, cap, reservation or overdue
settings. Each process has an independent eight-connection pool and stable slot
identities. The shared profile runs two processes with two slots each.

The local harness supports PostgreSQL limits of 2 CPUs/2 GiB or 4 CPUs/4 GiB
and worker limits of 2 CPUs/512 MiB per process when the container runtime can
enforce them. The harness verifies kernel CPU quotas and memory ceilings, PostgreSQL thread
affinity when requested, and worker process RSS and high-water memory at 20 ms intervals. Five fresh database copies are
measured for each of 1k, 10k and 100k queued tasks, a saturated overdue backlog,
and the timed skewed-duration workload with a per-principal execution cap of one.
The shared profile also runs a 90-second capacity cycle, shrinking the production
policy from four execution slots to one after 30 seconds and restoring four after
60 seconds. Existing claims drain normally. Its utilization calculation clips
occupancy to eligible capacity, brackets policy-commit and clock uncertainty,
and prevents temporary overcommit from concealing idle time after growth.
The 10k-backlog immediate runs measure 12k successful claims, leaving at least
10k ordinary selections after bounded promotions for the start-share comparison.
Queued timestamps reset to the declared initial age for each fresh copy; tasks
continue aging normally during the run. Immediate runs exclude a global prefix of 100 committed claims locally or 200
across the two shared processes. Timed runs complete 100 real claims per process
on the same connection pools and worker identities before measuring. These
warm-up claims preserve the configured caps and immediately release capacity,
without simulated work delays. Their samples are retained separately, including
cold-start latency; promotion spacing, query and candidate bounds still include
them. The measured window uses the declared task durations and capacity cycle.
Every sample, preparation inventory, container
configuration and candidate-selection plan is retained. Budget failures remain in
the report. A completed measurement run does not by itself certify P3/P4.

A disposable database wrapper records task index entries returned and sequential
rows visited around every production scheduler call, including empty searches.
Repeated visits count again; failed transactions discard their recorder rows and
fail the latency check. The original function is renamed without changing its
body, and its definition hash is retained. The wrapper and recorder insert remain
inside measured claim latency and lock occupancy. The report requires the recorder
and worker successful-claim counts to agree before checking the 1,024-entry limit.
Task table and index block requests, including buffer hits, are reported separately.

PostgreSQL resource evidence includes the enforced `memory.max`, observed peak,
and reclaim/OOM event counts. Linux permits temporary overshoot while reclaiming
pages, so the peak is retained even when it exceeds `memory.max`; any OOM event
fails verification. See the [kernel memory controller contract](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html).
The worker's separate 512 MiB RSS/high-water requirement remains a strict check.

These counters include index entries returned for dead or uncommitted rows, unlike
live-row plan counts; see [PostgreSQL index statistics](https://www.postgresql.org/docs/18/monitoring-stats.html#MONITORING-PG-STAT-ALL-INDEXES-VIEW).
They do not count internal B-tree comparisons, skipped entries that the index never
returns, or every tuple traversed in a heap update chain. Prepared and post-workload
generic plans cover all eight promotion turns. The retained counters and plans
qualify the measured workload; they do not prove a universal physical-work bound
for arbitrary database states.

The full scheduler acceptance matrix has 25 local and 30 shared repetitions.
Run it explicitly, add the `ci:scheduler` label to a PR, or manually dispatch the
**Scheduler acceptance** workflow. The label also enables reruns on subsequent
pushes; removing it cancels the active run. Other labels, including `ci:full` and
`ci:benchmarks`, do not enable this matrix. Normal PR CI keeps the scheduler
correctness tests and harness unit tests without these timed measurements.

The opt-in workflow builds once, measures the profiles in separate jobs, and
retains raw reports and build/runner metadata for 30 days even when budgets fail.
It uses `--check-budgets` to fail after all repetitions have been recorded when
P3, applicable P4 checks, or the shared database RTT limit fail. Hosted-runner
hardware and contention remain part of the evidence; a green numeric check alone
does not establish an equivalent dedicated deployment or a universal work bound.

Run the harness on Linux with access to the Docker host PID namespace; the
20 ms sampler reads worker processes directly from `/proc`. It needs permission
to inspect the container processes; the CI job uses `sudo` for this step. On macOS, run the
harness inside the Docker Linux environment with that PID namespace and use
Linux release binaries. When CPU quotas are unavailable, `--cpu-affinity` assigns
disjoint CPU masks to PostgreSQL and each worker and verifies their threads.
Database and worker memory limits remain enforced and inspected:

```bash
cargo build --locked --release -p hubuum-scale-benchmark
cargo build --locked --release --features embedded-migrations --bin hubuum-admin
python3 -I -S tests/python/run.py integration scheduling-benchmark \
  --profile shared --cpu-affinity --output target/scheduling-shared
```

`--unlimited-cpu` remains available for diagnostic evidence without CPU controls.
Resource enforcement does not establish SSD, RTT or candidate-work qualification.
The retained plan probes and all five repetitions still require acceptance review.

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
timestamp-reset probe demonstrates that limitation. Those diagnostic runs left general candidate-work bounds,
timed shrinking-pool utilization and CPU-constrained profiles unverified. These
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

## Handoff acceptance, 2026-10-31

All 25 local and 30 shared repetitions passed the applicable P3/P4 and resource
profile checks. The [complete report](performance/scheduling_acceptance.json)
retains every run, workload summaries, binary and fixture fingerprints, database
settings, resource evidence and SHA-256 digests for the raw artifacts. This
qualifies the declared workloads on the recorded host; the candidate-counter
limitations above still apply to arbitrary database states.

This historical matrix used a separate 30-second worker process for timed
warm-up. It therefore does not establish that each measured process had warm
connections. The integrated qualification below uses the corrected same-process
protocol.

The release binaries were built from `cd2f0389` with Rust 1.99.0 and embedded
migrations. PostgreSQL 18.6 ran in the pinned Debian container on Docker Desktop's
Linux arm64 VM, backed by the host's SSD. The host was an Apple M4 Pro with
14 logical CPUs and 48 GiB RAM; the VM had 14 CPUs and about 23.4 GiB RAM.
Kernel CPU quotas and memory limits enforced each profile. No task-owned builds
or correctness tests ran during measurement; normal desktop activity continued.
The database used 128 MiB shared buffers, 4 MiB work memory, synchronous commits
and fsync. Full settings and image identities are in the report.

Each table value is the worst corresponding per-run percentile or candidate count
across all five repetitions, rather than a pooled percentile. All runs used at
most five database round trips per claim, below the limit of twelve.

| Profile | Workload | Claim p95 ms | Claim p99 ms | Lock p95 ms | Max task entries | P3 passes |
| --- | --- | --- | --- | --- | --- | --- |
| local | 1k queued | 1.849 | 4.184 | 0.019 | 34 | 5/5 |
| local | 10k queued | 1.911 | 2.333 | 0.018 | 34 | 5/5 |
| local | 100k queued | 2.757 | 4.404 | 0.029 | 34 | 5/5 |
| local | 10k overdue | 2.005 | 2.276 | 0.018 | 118 | 5/5 |
| local | Timed, principal cap 1 | 7.258 | 13.237 | 0.102 | 74 | 5/5 |
| shared | 1k queued | 3.370 | 3.819 | 1.342 | 35 | 5/5 |
| shared | 10k queued | 3.537 | 4.506 | 1.408 | 35 | 5/5 |
| shared | 100k queued | 3.408 | 5.110 | 1.353 | 34 | 5/5 |
| shared | 10k overdue | 4.932 | 5.617 | 2.051 | 184 | 5/5 |
| shared | Timed, principal cap 1 | 16.448 | 36.621 | 6.166 | 75 | 5/5 |
| shared | Timed, capacity 4-1-4 | 9.072 | 38.618 | 3.621 | 75 | 5/5 |

Every applicable 10k-backlog run passed the ordinary 75%/25% start-share check
over at least 10k ordinary selections. All repeats preserved promotion spacing.
The conservative utilization bounds were 99.29–99.35% locally, 99.52–99.60% with
steady shared capacity, and 99.52–99.64% during shared 4→1→4 capacity cycles,
against the 85% target. Per-principal starts and cap-blocked times remain separate
in each run's report; these results do not promise CPU-time fairness.

The slowest worker's warm database RTT p95 never exceeded 0.759 ms in the shared
profile, below its 2 ms limit. Peak worker RSS/high-water memory was 10.19 MiB
locally and 7.81/8.58 MiB shared, below 512 MiB. The local database recorded a
24 KiB transient peak above its enforced 2 GiB memory limit during reclamation;
there were no OOM events or OOM kills in either profile. The shared database
stayed below its 4 GiB limit.

The complete matrix took 23.0 minutes locally and 28.7 minutes shared, excluding
compilation and correctness tests. An earlier local attempt stopped after six
complete repeats because its verifier incorrectly rejected any cgroup peak above
`memory.max`; its partial evidence is retained separately. The corrected matrix
used fresh fixtures for all 55 repeats. The opt-in CI workflow measures the two
profiles in separate jobs; ordinary PR CI does not run this matrix.

## Integrated acceptance, 2026-10-31

The [integrated report](performance/scheduling_integration.json) records all
55 fresh repetitions against source `50ba73cf`, combining the audit/recovery
changes with SDK 0.15 scheduling. All 25 local and 30 shared repetitions passed
P3, applicable P4 checks and resource verification. The report retains source,
fixture, harness and binary fingerprints, every run and raw-artifact hashes.
The full source snapshot and raw reports are retained with the integration
verification evidence. This is qualification on the recorded host and workloads;
it does not establish performance on arbitrary hardware or database states.

Timed runs now perform 100 real warm-up claims in each measured process using
the same pools and worker identities. Cold samples remain in the raw reports
and still count toward promotion, query and candidate bounds. The sampler also
recognizes a process exiting between reads of its status and memory evidence;
a live process with missing evidence still fails. These changes neither raise
budgets nor discard completed measured samples.

The earlier [hosted run](https://github.com/hubuum/hubuum/actions/runs/41188038909)
at `26a7c097` completed 48 repetitions, with nine latency failures, before a
shared worker's exit triggered the memory-sampling race. Its failures and raw
evidence remain retained. The new qualification also includes audit/recovery
changes, so it is not an isolated causal comparison of the warm-up correction.
Hosted-runner contention remains a separate qualification concern.

The host, CPU/memory profiles and database settings match the handoff matrix
above. All claims used at most six database round trips, including the audit
execution fence, below the limit of twelve. Each table value is the worst
corresponding per-run percentile or candidate count across five repetitions.

| Profile | Workload | Claim p95 ms | Claim p99 ms | Lock p95 ms | Max task entries | P3 passes |
| --- | --- | --- | --- | --- | --- | --- |
| local | 1k queued | 2.358 | 3.545 | 0.018 | 35 | 5/5 |
| local | 10k queued | 2.226 | 2.833 | 0.017 | 34 | 5/5 |
| local | 100k queued | 2.016 | 2.389 | 0.017 | 34 | 5/5 |
| local | 10k overdue | 2.319 | 2.623 | 0.017 | 117 | 5/5 |
| local | Timed, principal cap 1 | 6.203 | 6.203 | 0.072 | 74 | 5/5 |
| shared | 1k queued | 5.759 | 7.990 | 2.618 | 34 | 5/5 |
| shared | 10k queued | 4.723 | 5.721 | 1.997 | 35 | 5/5 |
| shared | 100k queued | 4.268 | 6.929 | 1.758 | 34 | 5/5 |
| shared | 10k overdue | 5.232 | 5.939 | 2.816 | 176 | 5/5 |
| shared | Timed, principal cap 1 | 9.660 | 10.875 | 5.685 | 77 | 5/5 |
| shared | Timed, capacity 4-1-4 | 7.117 | 11.732 | 2.941 | 77 | 5/5 |

All 20 applicable start-share checks and all 15 timed utilization checks passed;
promotion spacing passed in every repetition. Conservative utilization was
99.47–99.53% locally, 99.36–99.80% with steady shared capacity and
99.53–99.71% during shared 4→1→4 capacity cycles, against the 85% target.
The timed runs measured 19 successful claims per local repeat, 32–104 per
steady shared repeat and 52–83 per shared capacity cycle. These small timed
samples qualify the prescribed occupancy workload, not the 10k-start fairness
criterion; that criterion uses the separate immediate runs.

The largest shared database RTT p95 was 0.666 ms, below 2 ms. Peak worker
RSS/high-water memory was 10.32 MiB locally and 7.93 MiB shared, below 512 MiB.
The local database's transient reclaim peak exceeded its enforced 2 GiB limit
by 84 KiB; neither profile recorded OOM events or OOM kills. The shared
database remained below its 4 GiB limit.

The matrix took 20.3 minutes locally and 23.7 minutes shared, excluding builds
and correctness tests. No task-owned compilation, correctness suite or disposable
test database ran during measurement. The full matrix remains opt-in through
`ci:scheduler` or manual dispatch; ordinary PR CI runs correctness and harness
regressions without these timed profiles.
