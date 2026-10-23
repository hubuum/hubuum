# Operation budgets

## Decision

Share byte counters and bounded writers in `hubuum-budget`. Keep validated
operation policies, error mapping, and termination in their existing owners.
Backup logical-row accounting and final serialization, plus template/export
output, use the shared writer. This is the smallest common mechanism with two
real consumers. `StorageBackupBudget` and `TemplateLimits` remain the boundary
types; a byte limit cannot replace row, fuel, deadline, or authorization policy.

Keeping separate writers duplicates overflow and partial-write semantics.
A universal keyed quota framework would erase unit distinctions and introduce
locking without making database or subprocess work stoppable. Template batches
provide the composition pilot: each render borrows one shared parent budget and
has its own output cap. The child is clamped to the parent's remaining capacity
at construction; the exclusive borrow preserves that fact until it ends. Every
accepted write charges both budgets, including partial destination writes.
Dropping a child releases its output buffer without refunding accepted work.
Siblings run sequentially; the type system prevents concurrent borrowing of the
same parent. Concurrent capacity reservations and live policy replacement are
not included because neither pilot needs them. Durable retry accounting remains
with task/storage owners.

`hubuum-storage-core` is publishable, so its new prerequisite is also an
experimental public member of the storage SDK, at the same version and Rust
1.88 MSRV. The API is additive; existing backup/template signatures and public
errors remain unchanged. Publish `hubuum-budget` before storage-core. There are
no new runtime dependencies or configuration settings.

## Inventory and enforcement

| Owner | Unit and policy source | Enforcement and work before rejection | Zero, ownership, and termination |
| --- | --- | --- | --- |
| Backup capture (`StorageBackupBudget`) | Compact logical bytes and enumerated rows; validated deployment/admin settings | Charge each scanned row before retaining it; serialize one decoded row to a counting sink; final framing is checked again during artifact serialization | Byte and row limits must be positive. One immutable capture policy; PostgreSQL stream/transaction is dropped on error. One decoded row exists before its byte check. |
| Template output (`TemplateLimits`, `SizeLimitedWriter`) | UTF-8 output bytes; caller policy capped by isolation limits | Reject each excess output fragment before appending to its buffer | Zero allows empty output. One writer per render; render returns on I/O failure. Intermediate values are governed separately by worker heap/fuel. |
| Template batch/isolation (`hubuum-templates`) | Aggregate output bytes, input bytes, live Rust heap, interpreter fuel, recursion, worker concurrency and wall time | Shared parent/child writer charges, batch admission, framed transport checks, allocator, interpreter, supervisor deadline | Each render has an exclusive child budget under one batch ceiling. Cancellation/deadline kills and reaps the process; byte counters cannot provide that guarantee. |
| Export hydration (`src/services/exports`) | Hydrated objects, related depth, candidates, output bytes; admitted request limits constrained by deployment policy | Root/related capacity checks, paginated fetches, stage checks, bounded output serialization | Counts are cumulative work, not memory reservations. Fetches can retain a bounded page before checks. Storage query deadlines remain adapter-owned. |
| Grouped object queries | Candidate rows, group cardinality, aggregate work and query time; validated query/deployment limits | Query planning and backend execution budgets | Truncatable queries report their own completeness. SQL statement timeout and query cancellation are separate from counters. |
| JSON Schema (`hubuum-domain`) | Schema nodes/depth, instance bytes and estimated validation work; explicit domain policy | Structure/admission checks before schema compilation and validation; bounded diagnostics afterward | Fixed domain acceptance semantics and conservative estimates, not heap or CPU preemption. Compilation cache is separately bounded. |
| Authorization traversal/pagination | Inspected candidates and page fetches; operation-specific traversal policy | Bounded batches and decision checks before returning authorized rows | Candidate work is not refunded when rows are hidden. Preserve each operation's exhaustion response and pagination contract. |
| Tasks (`hubuum-task-core`, task/storage layers) | Queue/active work, wall deadlines and leases; validated deployment/execution policy | Admission transaction, cooperative checkpoints, database statement limits, fenced terminal writes | Durable cluster accounting belongs to PostgreSQL. Local counters neither enforce cluster quotas nor reset durable work on retry. |
| Regex (`hubuum-regex`, adapters) | Fixed source bytes, nesting, repetition, expanded complexity; adapter compilation/matching work and statement time | `Pattern::new` before storage; native compilation/matching or SQL execution afterward | Portable language limits are fixed semantics. See the [full regex inventory](regex-testing.md) for defaults, zero behavior and work before rejection. |

The shared writer measures bytes accepted by its destination. It does not bound
`Vec` capacity: growth may reserve more than logical length, as before. Backup
buffers start empty; template buffers reserve `min(limit, 1024)` and grow using
the same standard `Vec` algorithm. Counting sinks retain no output. Neither
pilot promises constant-memory backup materialization.

Configuration and detailed limits stay with the existing references:
[backup/restore](backup-restore.md), [template isolation](template_worker.md),
[querying](querying.md), [schema evolution](schema_evolution.md),
[tasks](task_system.md), and [regex](regex-testing.md).
`JsonSchemaLimits::DEFAULT` selects 65,536 schema bytes, 16,384 expanded-work
units, 2 MiB instance bytes, and 256 MiB instance-work units; its builder rejects
zero. `TraversalBudget` accepts depths 1–512 and work rows 1–50,000 and narrows
the depth for each request. These different units and validation rules remain
in their domain types rather than becoming interchangeable byte counters.

## Policy selection and lifetime

Dynamic means operation-selected limits, constrained by validated deployment
settings. Backup admission/worker configuration creates a `StorageBackupBudget`;
template preparation selects `TemplateLimits` and the isolated request snapshots
its effective cap. `ByteLimit` pins the resulting finite value for a writer's
lifetime. Changing settings elsewhere cannot expand an already-created writer.
Zero is valid only at the shared byte layer; the backup constructor continues
rejecting it. No unlimited sentinel or unchecked arithmetic is introduced.

Queued backup work uses the execution worker's effective validated settings;
there is no new persisted policy version. Export requests already persist their
resolved request limits. A fresh execution starts fresh local counters, while
task admission, lease ownership, and persisted state keep their existing durable
semantics. These counters must not be used to claim a job-wide quota spanning
retries. No live reconfiguration endpoint is added.

Usage, limit, remaining bytes, and rejection are available as content-free
observations. They add no clocks, atomics, metrics labels, or backend-specific
instrumentation. Existing operation tracing and metrics retain ownership of
reporting. A rejected fragment leaves usage unchanged, sets `exceeded`, and
propagates the caller's existing error. Both pilots abort at the first failure.

## Verification and performance

The shared tests exercise zero/exact/excess limits, `usize::MAX`, failed charges,
partial writes, underlying I/O failures, flush, early producer termination,
parent/child exhaustion and destination release without refunds on drop. A
compile-fail example proves children cannot concurrently borrow their parent.
Backup capture tests exercise real streamed rows,
rejected large JSON rows and database-side early termination.
Template isolation tests exercise oversized rendering and bounded worker heap;
the existing supervisor retains cancellation/kill/reap ownership. These tests establish
actual producer and backend behavior beyond arithmetic alone.

Run the existing full test runner and the focused benchmark:

```sh
source .env && ./run_tests.sh
cargo bench --bench operation_budgets_criterion
```

The benchmark compares identical fragments and preallocation with the previous
backup and template algorithms, including four sequential batch children, and
checks identical output and final capacity.
The shared counter uses fixed-size stack state and no heap allocation or
instrumentation calls. Rejection constructs a typed `io::Error`; the public
template wrapper remaps it to preserve its existing message, adding an error
construction on that failure path. Record compiler, machine, features and
measured latency alongside results when evaluating overhead.

### Measured overhead

The [raw report](performance/operation_budgets.json) records two sequential
repeats, confidence intervals, medians, process resources, compiler information,
source hashes, and the exact benchmark executable hash. These runs used Rust
1.98.0 / LLVM 22.1.8, default features, and the existing `opt-level = z` release
profile with fat LTO and one codegen unit. The host was an Intel Xeon Silver 4216
(16 cores, 32 logical CPUs), approximately 62 GiB RAM, running Linux x86-64.
The executable was pinned to CPU 15 after this experiment's builds and database
tests finished; existing host services remained running, and load averages are
recorded in the report. Each case used 50 samples, two seconds of warm-up, and
five seconds of measurement.

Each operation writes 128 fragments of 21 bytes, or 2,688 bytes total. The batch
case divides them among four sequential children. The second repeat supplies
the absolute means below; both repeats' changes are shown to expose variation.

| Workload | Previous mean, µs | Shared mean, µs | Second repeat change | First repeat change |
| --- | --- | --- | --- | --- |
| Backup buffer | 1.873 | 2.568 | +37.1% | +30.8% |
| Template buffer | 1.759 | 2.097 | +19.2% | +11.8% |
| Four sequential batch children | 1.688 | 1.559 | -7.7% | +4.3% |

The backup writer costs approximately 0.6–0.7 µs more per 128-fragment operation
across these repeats; template output costs approximately 0.2–0.3 µs more.
Batch changes span both signs, so these measurements do not establish a batch
speedup. The shared counting sink takes approximately 0.85–0.90 µs for the same
fragments and retains no output buffer; it has no previous-writer comparison.
These are writer microbenchmarks, not whole backup/export or template latencies.
The pilot accepts the measured overhead for common partial-write semantics and
typed parent/child enforcement; it does not claim zero cost.

Successful writes retain the same `Vec` preallocation and growth behavior as the
previous implementation; benchmark assertions compare output and final capacity.
The shared counter itself adds fixed-size stack state, without a new allocation
or instrumentation call. Rejection/error construction is outside these timed
success cases. This is a code-level allocation assessment, not an allocator
profile. End-to-end latency and allocation behavior still depend on each caller.

To retain the measured optimization profile when reproducing after a release
profile change, run:

```sh
CARGO_PROFILE_RELEASE_OPT_LEVEL=z cargo bench --bench operation_budgets_criterion \
  -- --sample-size 50 --warm-up-time 2 --measurement-time 5 --noplot
```
