# Task retention measurement

The task retention driver exercises production HTTP admission, authenticated
checkpoint publication, real worker completion, and the administrator retention
executor. It uses database timestamps without modifying them. The checked-in
driver supports an initial protected cohort, bounded steady refill, and cleanup,
with a separate remote-effect recovery probe. Running it does not by itself
establish that the P6 target in [lifecycle budgets](lifecycle_budgets.md) passed;
the raw denominators, occupancy, resource ceilings and recovery results matter.

## Reproduce the diagnostic

Use Python 3.11 or newer, Docker, Linux `/proc`, and binaries built from the same
revision as the application image. The output directory must not exist. The
driver creates a fresh disposable database and destroys its containers and
network on completion or failure. It retains an owner-only checkpoint directory
and synthetic measurement artifacts. Never point this driver at production.
Fixture containers use the inspected address of their own PostgreSQL container
instead of depending on rootless bridge DNS. Owned control/key mounts receive
shared SELinux labels so multiple replicas can access the same protected files;
their 0700/0600 modes remain enforced.

When the driver runs inside the container engine's Linux host (for example, a
Linux helper on Docker Desktop), use `--direct-container-network` if published
loopback ports are reachable only from the desktop host. This explicitly selects
the inspected addresses on the disposable fixture's own network for API and
native database connections, including after a recovery restart. It does not
select an existing database. Remote-effect probes reach the helper through that
same network's inspected gateway. Reports record the connection mode; database RTT
must still satisfy the selected profile. The default uses published ports for
ordinary Linux and rootless deployments.

```bash
source .env
cargo build --locked --release -p hubuum-scale-benchmark
cargo build --locked --release --features embedded-migrations --bin hubuum-admin
docker build \
  --build-arg 'CARGO_BUILD_FLAGS=-F tls-rustls -F tls-openssl --locked --release' \
  --tag hubuum-server:retention-measure .
python3 -I -S tests/python/run.py integration task-retention \
  --image hubuum-server:retention-measure \
  --scale-binary target/release/hubuum-scale-benchmark \
  --admin-binary target/release/hubuum-admin \
  --output-directory /tmp/hubuum-retention-diagnostic \
  --runs 1
```

Use `--count 100` for a smoke run, or `--admission-only` to isolate the initial
protected admission phase. These options are recorded through the exact workload
JSON and its SHA-256 digest. The default
[versioned workload](../../scale-benchmarks/lifecycle/task-retention-v1.json)
admits 10,000 distinct keyed dry-run import tasks at concurrency 32, with identical
and conflicting retries every 100 submissions. These requests exercise real
task creation and import results without persistent graph changes.

The fixture explicitly configures a 120-second terminal-anchored receipt window,
30-second task detail age, and capacity for 20,000 outstanding tasks. It starts
the API before the worker so that the initial receipt cohort exists before
terminal clocks begin. Both application containers receive 2 CPUs and a 512 MiB
memory limit, with pool size 8; PostgreSQL receives 2 CPUs and 2 GiB. PostgreSQL
uses the pinned image shared with the corpus driver and quiet disposable-fixture
logging. This topology is named `task-retention-diagnostic-v1`; it is not the
local or shared acceptance profile.

For the declared local or shared topology, select `--profile local` or
`--profile shared`. Both provision 32 distinct authenticated principals and use
80% traffic skew toward the first principal. Local replaces its admission-only
API gracefully with one API/worker process and one execution slot. Shared uses
two API processes, two workers with two slots each, and a 4 CPU/4 GiB database.
`--steady-seconds 360` refills up to the initial receipt occupancy for three
120-second windows, in batches of at most 256 actual submissions by default, then drains.
Use `--refill-max-batch 1000` when the smaller batch cannot replenish the target
between cleanup observations. This bounds the number of arrivals per refill;
the recipe's concurrency 32 and all application resource limits stay unchanged.
Reports retain the selected bound. Refill and observation take real time, so
neither setting establishes sustained occupancy without reviewing the raw samples.
Every occupancy observation and eligible deadline remains in the report.
Occupancy separates executable, terminal and recovery-unknown receipt outcomes,
unexpired terminal receipts and eligible receipts. An executable receipt can
still await terminal settlement; it does not identify queued versus running
work. Required refill follows observed receipt retirement, since the reuse
window starts at completion and does not age queued work.

```bash
python3 -I -S tests/python/run.py integration task-retention \
  --image hubuum-server:retention-measure \
  --scale-binary target/release/hubuum-scale-benchmark \
  --admin-binary target/release/hubuum-admin \
  --output-directory /tmp/hubuum-retention-local-five \
  --profile local --steady-seconds 360 --refill-max-batch 1000 --runs 5
```

Repeat with `--profile shared` and a fresh output directory. These commands still
need actual execution and acceptance review. The fixture records its readiness
and authentication warm-up; it does not fabricate completed warm-up tasks.
Only HTTP 503 receives a bounded same-key retry. Actual unavailable attempts and
total elapsed time remain visible, including repository lock contention.

Administrator setup retries only the exact repository-lock refusal for a bounded
30-second retry budget, retaining attempt counts and total elapsed time. Native
cleanup observers retain every refused process attempt and retry within their
original overall timeout/pass limit. Contention delays remain part of observed
expiry lag; no source deadline or measurement window is renewed. Key mismatches,
recovery-required outcomes and other failures still stop the run.

Application startup also retries only an exited process whose exact fatal
diagnostic identifies repository-lock contention, for at most 30 seconds from
the original startup attempt. Every restart and total elapsed time is retained
in `startup-*.json`. APIs must answer readiness and workers must serve their
metrics listener before their process resource limits are verified. Other
startup failures remain fatal; these retries never bypass the recovery gate or
renew task deadlines.

Some rootless engines expose a memory controller but no CPU quota controller.
The driver refuses to treat requested `--cpus` flags as enforcement on those
engines. Build a measurement-only image with `taskset`, then add `--cpu-affinity`
to the driver command:

```bash
docker build -f scale-benchmarks/lifecycle/affinity.Dockerfile \
  --build-arg HUBUUM_IMAGE=hubuum-server:retention-measure \
  --tag hubuum-server:retention-affinity .
```

Use that derivative as `--image`. Production binaries and the production
entrypoint are unchanged. Affinity assigns disjoint fixed logical CPU sets to
the database, each application process, and native administrator/load-generator
children. Two fixed logical CPUs impose a maximum concurrency of two; unlike a
two-CPU quota, they do not permit short bursts across more CPUs. Record this
stricter concurrency limit as the profile equivalence, together with actual
hardware and storage. The driver verifies all observed threads and descendants,
reads actual cgroup `memory.max`, and records `memory.peak`; native bounded child
runners verify inherited affinity too. No affinity result is accepted solely
because the launch command requested a mask. Formal runs require other builds
and measurements to be stopped.

For a separately identified recovery run, `--recovery-probes` provisions a
verified HTTPS counter and invokes it through
the real remote task worker. Rust checks concurrent duplicate recognition,
changed-body conflicts, and exactly one effect before and after detail purge.
The fixture holds that task while taking a real history-enabled backup, releases
the hold through a protected checkpoint, and later stops runtimes, restores the
older backup, and restarts a worker. The restored retry must report the same
expired task identity with the effect count unchanged. Its receipt uses a
separate prospective 7,200-second window so the recovery assertion remains
within the original protection period.

`--physical-recovery-probes` additionally captures and restores a real PostgreSQL
physical base backup of the disposable primary, including protected database
tables, while preserving the independent checkpoint directory and HTTPS effect
counter. It restores only the fixture's validated managed PostgreSQL volume
while PostgreSQL is stopped, preserving its data ownership and system identity.
The helper restarts PostgreSQL after restoring the stopped data directory,
recovers the latest independent authority under the repository lifetime lock,
interrupts actual protected reapplication under a controlled row
lock, and retries cleanup before restarting a runtime. The separate report must
identify the interruption and observed physical state; the remote effect count
and expired receipt identity are checked again afterward.

Add `--output-probe` to one separately identified run to create a real export
whose task detail retention is disabled and whose output expires after one hour.
The observer retains the database's original output deadline, waits for actual
expiry, then requires both physical output absence and the protected
`ExpiredOutputs` removal decision while the task row survives. This additional
probe can age while the steady workload runs; it need not be repeated in each
of the five receipt/detail timing runs. Its wall-clock delay is intentional.

`--safety-probes` adds a separate retention and interrupted-effect drill. It
creates two real dry-run imports with 1,201 results each, protects their holds,
and creates a retained schema repair report through the API. A gated HTTPS
response keeps an unrelated remote task running after the fixture has counted
its effect. Rust then observes bounded cleanup of the released import while the
held import and report dependency survive past their original deadlines. The
fixture kills its producing runtimes, performs protected recovery and
reapplication, and requires the interrupted task's retry to preserve one effect.
The drill explicitly raises remote transport and task timeouts to 900 seconds;
fixture shutdown releases all response gates. It does not modify stored clocks.
Source support alone is not evidence that this drill passed.

## Evidence and interpretation

| Artifact | Meaning |
| --- | --- |
| `workload.json`, `capacity-policy.json`, `activation.json` | Exact recipe, admission limits, and protected activation result. |
| `admission.json` | Every HTTP timing and original task UUID; duplicate responses must identify the same task and changed requests must conflict. |
| `metrics/*.prom` | Production publisher counters before and after admission. |
| `resources.json` | Sampled RSS, Linux process high-water marks, sampled repository occupancy, effective limits, and measurement errors. |
| `cleanup.json` | Original database anchors, verified physical removal observations, raw task lifetimes, censored work, and lag distributions. |
| `cleanup-processes/pass-*.json` | Actual administrator exit status, bounded output, process resource measurements, and publisher counters. |
| `cleanup-processes/latest-lifetimes.json` | Most recent original anchors and observations, retained even if a later cleanup invocation fails. |
| `cleanup-processes/refill-*.json` | Actual bounded refill admissions and their child-process resource reports. |
| `probe-*.json`, `logical-recovery.json` | Rust-verified external effect counts, retry results, and actual logical restore duration. |
| `safety-*.json` | Held and dependent detail observations, large-child cleanup, and interrupted-effect retry evidence from the separate safety drill. |

Application RSS and repository occupancy are sampled every 20 ms. Kernel
`VmHWM` is reported separately from sampled RSS. The administrator process tree
uses the existing bounded process runner, now with 20 ms sampling and Linux
`wait4` RSS high-water accounting. Kernel and sampled values are separate;
neither is an allocator-specific measurement. A failed sampler makes the
resource report invalid; a failed workload remains a failed run even when
diagnostic artifacts were retained.

The database resource report retains database allocation and WAL observations.
Its generic host-wide PostgreSQL CPU/RSS fields are unset; container process and
cgroup measurements identify the disposable database being measured. The
separate relation inventory includes TOAST and protected control allocation.
The observer also records the actual PostgreSQL version/settings and ten
`SELECT 1` round trips on one acquired connection after an excluded warm-up.
Those timings include transport and server execution; they exclude pool
acquisition and are not a claim of network-only latency.

Publisher `read_bytes` and `write_bytes` count successful file read/write return
values, including reads served from the page cache. They do not measure block
device traffic. Operation counters include encoding/signing, verification,
file and directory fsync, publication, garbage collection, and asynchronous
coalescing wait. File lock durations measure nonblocking `flock` attempts;
contention counts do not represent mutex wait. End-to-end HTTP and administrator
timings include publication waits. Sampled directory size measures resident
occupancy, not cumulative I/O. Runtime-isolation nonce-file I/O is outside the
publisher counters.

Cleanup requires both exact-UUID physical absence and the independently verified
checkpoint's matching removal decision. Receipt retirement requires primary and
checkpoint absence plus a protected expiry floor covering the original deadline.
The observation interval and administrator publication delay remain in the
reported lag; samples are conservative upper bounds. A timeout retains eligible
unfinished items instead of dropping them from the denominator.
Database deadlines round down to milliseconds, and removal observations use the
query completion time with an upper rounding margin. Nonterminal evidence uses
the query start time and an explicit active status, so a read crossing the steady
window boundary cannot extend that evidence to its later completion. Lags end
at the first verified removal observation.
The report distinguishes freshly observed nonterminal work from missing deadline
evidence. Censored samples contribute lower bounds and an explicit incomplete
or breached percentile status. Full cohort drain has its own outcome: a drain
timeout does not by itself establish an eligible-expiry percentile breach.

## Encoding proofs and remaining acceptance work

The Rust tests
`ten_thousand_receipts_and_157_manifests_fit_the_declared_encoding_budgets` and
`production_composite_publisher_round_trips_ten_thousand_receipts_and_bounds_live_files`
construct validated bounded records, serialize them, and exercise the actual
signed publisher. They test 10,000 receipts, 157 task manifests, receipt and
retention byte caps, authenticated readback, and retained repository files.
Their synthetic timestamps are suitable for encoding tests only. Run with
`--nocapture` to retain actual byte counts; successful test results are distinct
from runtime throughput or expiry measurements.

The first constrained local attempt on 2026-10-29 did not complete admission.
It used production revision `95afd176`, measurement client `1ec1cb80`, the local
CPU-affinity profile, and a 10,000-task target. The 1,200-second admission timeout
left 8,741 receipts in the independently protected checkpoint. This count does
not prove that every receipt's task activated. Workers and expiry observation
had not started, so the attempt provides no expiry or recovery timing result.

| Observation | Recorded value |
| --- | --- |
| Resource samples / monitor errors | 52,028 / 0 |
| API sampled RSS and kernel high-water | 229,249,024 bytes (218.63 MiB) |
| Sampled repository peak | 18,043,280 bytes / 7 files |
| Diagnostic checkpoint reads / writes | 26,539,271,742 / 738,003,752 bytes |
| Diagnostic verification / publication time | 760.467 / 104.942 seconds |
| Diagnostic publication count | 260 |

The diagnostic counters are deltas from the initial metrics snapshot to one
separately identified scrape before the timeout; they are not final totals.
Verification includes typed decoding, validation and canonical reserialization.
Publication includes nested operations, so these times must not be added.
Writes already coalesced roughly 32 protected records per publication, while
each admission reread and verified the growing checkpoint. This evidence motivates
bounded reuse of authenticated immutable contents with fresh independent file,
digest, reference, key and durability checks. It does not establish acceptance.
The host exposes a MegaRAID controller with rotational-device metadata. The
operator confirmed on 2026-10-30 that its backing drives are SSDs; this is
operator evidence rather than direct media identification by the operating
system. Resource enforcement and the remaining profile equivalence still need
to be recorded with each acceptance run.

P6 acceptance still requires five fresh runs with declared warm-up, demonstrated steady occupancy
near 10,000 receipts and continuous capacity release/refill for at least three windows.
The acceptance topology and workload must include the declared principal skew,
concurrent replicas, output expiry, held and dependent tasks, large child sets,
maximum chunk budgets, and an unrelated long-running task. The logical restore
and whole-primary rollback helpers require successful real executions; adding
their source does not establish recovery performance. Report outages, holds, recovery drain,
oldest remaining obligations, database allocation/WAL, and repository storage
properties separately. Production output retention currently has a minimum of
one hour; an honest output-expiry run must allow that real deadline to elapse.
Neither a diagnostic cohort drain nor unexecuted sustained-run support
establishes the 60-second expiry p95 or 300-second recovery cleanup target.

The SDK 0.16 integration adds receipt eligibility to the durable scheduler's
candidate selection. The retained SDK 0.15 scheduler qualification predates those
checks. Rerun the opt-in P3/P4 scheduler matrix on the final retention source as
well as the P6 profiles before accepting its performance; passing correctness
regressions does not establish the added checks' latency or utilization cost.
