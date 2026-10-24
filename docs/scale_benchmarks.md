# Scale Operational Benchmarks

The scale suite generates deterministic, skewed data and drives it through a
production `hubuum-server` process with API and worker roles enabled. It is
designed for operational validation, not microbenchmarking. Timing and resource
measurements are evidence; correctness failures fail the run. PostgreSQL is the
first implemented scale adapter.

The loader replaces data in a freshly migrated, disposable database. Never point
it at a development, staging, or production database.

## Architecture and Backend Comparisons

The suite is split along the storage boundary:

- `hubuum-scale-core` owns backend-neutral profiles, logical manifests,
  workloads, reports, scale-impact analysis, and backend comparison output.
- `hubuum-scale-benchmark` is the shared command-line and HTTP workload
  frontend. It does not depend on the root `hubuum` library.
- Each storage adapter implements `ScaleBenchmarkBackend`. The adapter owns
  fixture loading, physical validation, readiness hooks, backend identity and
  effective settings, and resource probes that only make sense for that
  backend. PostgreSQL's implementation is opt-in under
  `scale-benchmark-support`.

Every selected backend runs the same logical manifest and versioned HTTP
workload. Common measurements use neutral names such as storage bytes, while
optional data, index, write-ahead, resident-memory, CPU, and named custom
metrics are reported only when an adapter can measure them honestly. This lets
the suite measure one-axis corpus growth and placement skew within a backend,
then compare how different backends react to the same logical corpus shapes.
Ordinary `rust-pr-bench` jobs own base-versus-pull-request code-regression
comparisons at a fixed corpus.

## Dataset Profiles

The committed profiles are compact TOML specifications. Generated rows, backup
artifacts, and raw reports are CI artifacts and must not be committed. Reviewed,
sanitized measurement summaries may accompany performance documentation.

| Profile | Collections | Classes |   Objects | Class relations | Object relations |
| ------- | ----------: | ------: | --------: | --------------: | ---------------: |
| large   |       1,000 |   4,000 |   250,000 |           8,000 |        1,000,000 |
| huge    |       4,000 |  16,000 | 1,000,000 |          32,000 |        4,000,000 |

Each profile contains named object-heavy, class-heavy, balanced,
history-heavy, and authorization-adversarial regions. The generator also
creates sparse and dense classes, empty classes, a high-degree object, bounded
graph components, concentrated relations, varied JSON payloads, history skew,
computed fields, templates, disabled remote targets and subscriptions,
terminal tasks, audit events, and terminal event-delivery history.

The seed, profile version, exact totals, distribution summaries, stable anchor
IDs, and logical corpus SHA-256 are written to the manifest. A run stops before
measurement if the loaded rows or required skews do not match that manifest.
Generation is set-based and deterministic; increasing the seed changes the
logical corpus while preserving the declared shape.

The bundled large and huge specifications now use profile version 2. This is a
breaking fixture-identity change: version-1 manifests do not match those bundled
specifications. Recreate the disposable dataset and manifest with the matching
version-2 profile, or continue using an archived version-1 specification through
`--profile-spec`. Older custom profile files remain readable; omitted lifecycle
settings receive their documented defaults.

## Build and Prepare

Build the shared runner and the real production binaries:

```bash
cargo build --locked --release --package hubuum-scale-benchmark
cargo build --locked --release --features embedded-migrations \
  --bin hubuum-server --bin hubuum-admin
```

Create two empty disposable databases: one for the benchmark and one for the
isolated restore drill. Migrate only the benchmark database. This example uses
local non-secret credentials; adapt it to an isolated PostgreSQL instance.

```bash
createdb hubuum_scale_local
createdb hubuum_scale_restore
target/release/hubuum-admin \
  --migrate --legacy-single-role-migration \
  --database-url postgres://localhost/hubuum_scale_local
```

## Run Locally

Run the large profile under standard limits:

```bash
target/release/hubuum-scale-benchmark run \
  --backend postgres \
  --profile large \
  --limit-mode standard \
  --server-binary target/release/hubuum-server \
  --admin-binary target/release/hubuum-admin \
  --database-url postgres://localhost/hubuum_scale_local \
  --restore-test-database-url postgres://localhost/hubuum_scale_restore \
  --label local-large-standard \
  --manifest-output target/scale/local/manifest.json \
  --load-report-output target/scale/local/load.json \
  --output target/scale/local/report.json \
  --artifact-directory target/scale/local/artifacts
```

Use `--profile huge` for the huge tier. Use `--limit-mode extended` to exercise
the larger page and traversal settings. The profile specification and workload
can be overridden with `--profile-spec` and `--workload-spec`; record those
files with the report when doing so. Use `--seed` for a deliberate corpus
variant.

The two workload modes apply and report these server limits:

| Mode     | Default page | Maximum page | Graph depth | Related include | Export output |
| -------- | -----------: | -----------: | ----------: | --------------: | ------------: |
| standard |          100 |          250 |         100 |              50 |       256 KiB |
| extended |          250 |        1,500 |         200 |              50 |         4 MiB |

The related-object include ceiling is a fixed application safety limit and is
therefore unchanged. Unified search is also capped at the backend-neutral
512-candidate page ceiling even in extended mode. Dataset backup/restore
ceilings are separate provisioning limits declared by each profile.

The `manifest`, `load`, `measure`, and `assess` subcommands allow the phases to
be run separately. `load`, `measure`, and `run` accept `--backend`; PostgreSQL is
the current default. `measure` requires a manifest from the loaded database.
The following comparison fails on corpus drift, backend drift, or correctness
drift while keeping latency changes informational:

```bash
target/release/hubuum-scale-benchmark assess \
  --base target/scale/base.json \
  --head target/scale/head.json \
  --markdown-output target/scale/summary.md
```

When more than one adapter is available, run each against the same logical
profile and workload, then render raw side-by-side results:

```bash
target/release/hubuum-scale-benchmark compare-backends \
  --reports target/scale/postgres/report.json target/scale/other/report.json \
  --output target/scale/backend-comparison.json \
  --markdown-output target/scale/backend-comparison.md
```

The command rejects mismatched corpora, workloads, limits, scenario sets, and
failed correctness checks. It does not normalize unlike physical storage
models or imply that a single run establishes a winner.

## Measure Scale Sensitivity

Base/head assessment asks whether code changed at one fixed corpus size. It is
available as a local diagnostic, but ordinary `rust-pr-bench` CI owns that
regression question. Scale CI instead asks for the marginal cost of more data.
Comparing `large` with `huge` is not causal because every dataset dimension
changes together.

For a controlled sensitivity experiment, run the same binary, workload, seed,
limit mode, PostgreSQL settings, and fresh-database procedure twice. Leave the
first profile unchanged and add an arbitrary positive amount along exactly one
axis in the second:

```bash
: "${OBJECT_STEP:?set a pilot-derived object increment}"

target/release/hubuum-scale-benchmark run \
  --profile large \
  --add-objects "$OBJECT_STEP" \
  --limit-mode standard \
  --server-binary target/release/hubuum-server \
  --database-url postgres://localhost/hubuum_scale_objects \
  --workload-spec scale-benchmarks/workloads/v1.toml \
  --label objects-step \
  --manifest-output target/scale/objects/manifest.json \
  --load-report-output target/scale/objects/load.json \
  --output target/scale/objects/report.json \
  --artifact-directory target/scale/objects/artifacts
```

The baseline uses the same command without an increment. The available
controlled axes are:

| CLI option | Placement | Other domain totals |
| ---------- | --------- | ------------------- |
| `--add-classes` | Existing balanced collections | Objects and relations remain fixed |
| `--add-objects` | Existing balanced classes | Unchanged |
| `--add-object-heavy-objects` | Existing object-heavy classes, strengthening the hot-class skew | Unchanged |
| `--add-object-relations` | Existing balanced class relations | Unchanged |
| `--add-concentrated-object-relations` | The object-heavy region's concentrated class relation | Unchanged |
| `--add-dense-object-relations` | Small class-heavy pairs, increasing unique-pair saturation | Unchanged |

Balanced relation increments use source and target classes with enough pair
capacity for useful raw-volume experiments. Concentrated increments exercise a
large local relation bucket; they do not claim to be a near-capacity saturation
test. Dense increments distribute edges through small class-heavy pairs and
preflight every pair's finite unique source/target capacity. Every increment
has no default and only one may be supplied per run.

Compare the two sanitized reports:

```bash
target/release/hubuum-scale-benchmark impact \
  --baseline target/scale/baseline/report.json \
  --comparison target/scale/objects/report.json \
  --axis objects \
  --output target/scale/objects/impact.json \
  --markdown-output target/scale/objects/impact.md
```

The command rejects failed correctness checks, mismatched backends, runtimes or
workloads, and changes outside the declared axis and region. Its JSON and
Markdown report matching-scenario latency and throughput deltas together with
storage, data, index, write-ahead, CPU, memory, generation, loading, backup,
restore, and rebuild deltas when those measurements exist. With no
`--normalization-unit`, the report describes the actual step. Supply a common
unit only when several points need directly comparable slopes.

Choose steps empirically. Run at least three unmodified baselines to establish
the local noise envelope, then probe logarithmic relative steps such as small,
medium, and large percentages of the starting axis. Refine around the first
repeatable change outside that envelope and around operational boundaries such
as an extra pagination page, cache or memory capacity, timeout, or uniqueness
saturation. Alternate baseline and variant order, keep the hardware quiet, and
repeat each candidate point. One paired run discovers signal; it is not a
performance conclusion.

The first calibration of this runner found that small count changes could be
invisible in endpoint latency even though database and index growth remained
measurable. Larger object steps first appeared clearly in global search rather
than point or bounded class queries. Relation traversal changed sharply when a
representative class relation crossed the 250-row page boundary. It also found
that adding edges to small class pairs can exhaust the finite set of unique
source/target pairs. That is a density or saturation experiment, not the same
question as raw relation-table volume.

The versioned CI experiment in `scale-benchmarks/sensitivity-v1.toml` uses
independent +20%, +50%, and +100% points. In the volume pilot, +1% and +5%
object changes stayed inside repeated-baseline endpoint variance; +20% was the
first useful object-volume signal, +50% captured the observed
relation-pagination boundary, and +100% tested whether the trend continued.
The initial class-count, object-heavy, and concentrated-relation curves use the
same exact global increments so placement effects can be compared at matching
corpus sizes. The separately calibrated dense-pair curve uses +10%, +30%, and
+55% total relations. In the large profile those points move the measured small
class pair from 23.0% baseline saturation to 34.2%, 56.6%, and 85.2%; a +60%
point is impossible because at least one generated pair would exceed its
unique-edge capacity. These percentages are calibration choices, not permanent
thresholds. Revise the versioned experiment after repeated measurements show
that different points carry more information.

Relation-growth points also run object-graph searches from one bounded,
eight-class chain. Spread and concentrated growth place new edges outside that
chain, so maximum depths 1, 2, and 4 isolate the effect of a larger global
relation corpus while the reachable graph stays fixed. Dense-pair growth places
new edges inside the chain and reports depths 1 and 2 together with returned
node and edge counts, separating corpus-size cost from added fan-out. Calibration
also tested depth 7 for fixed-local growth, but the hosted +100% spread point
crossed the 30-second request timeout. Depth 4 already showed the deep-query
effect while retaining useful headroom. A focused dense pilot found that depth
4 at the +55% point likewise exceeded 30 seconds and left recursive database
work running. Deeper recurring probes would therefore produce timeout backlog
rather than a useful curve. The diagnostic depth scenarios use one warm-up and
three measured requests and are excluded from the mixed workload.

Page limit 250 runs all six curves. Page limit 1,500 repeats only balanced
object and spread-relation growth, where it provides a useful pagination
control. This avoids multiplying every shape by every operational setting.

Use `sensitivity-plan --limit-mode standard` (or `extended`) to turn the
relative matrix into exact increments for a profile, then produce one `impact`
report per fresh-database point. After all points complete,
`summarize-sensitivity` validates that the expected mode-specific matrix is
complete. Its short signal list makes relative p95 curves, pagination knees,
relation saturation, and deepest graph-search changes easy to scan. Exact
corpus, performance, and depth-by-growth matrices are collapsed by default
under each axis. The machine-readable report retains every absolute
measurement, relative cost, and returned graph shape. Once repeated trials are
available, a line chart with uncertainty bands will communicate trends better
than a single-run line. Until then, connecting three informational points would
imply more certainty than the measurements support.

Treat a repeatable corpus-size slope in a bounded, fixed-result operation as
an optimization target unless the operation intentionally examines a
proportional number of rows. Diagnose that slope with query plans, buffer and
row-flow evidence, and application profiling. Keep it separate from costs
caused by increasing the requested result set, local relation fan-out, or the
number of pagination pages; those experiments change the amount of work the
operation is asked to return.

High-value questions include:

| Dimension | Hold fixed | Vary and ask |
| --------- | ---------- | ------------ |
| Object volume | Classes, relations, authorization, payload shape | Which search, count, filter, aggregation, and pagination paths grow with object count? |
| Relation volume | Objects, classes, graph topology | What are the latency, throughput, index, WAL, and traversal slopes for spread edges? |
| Relation density | Object and class counts | Where do class pairs approach unique-edge capacity, and how does high density affect writes and reads? |
| Graph shape | Total edge count | How do hub degree and component width change cost beyond the bounded depth curves? |
| Tenant skew | Global totals | How do hot classes, empty classes, sparse visibility, and authorization backfill affect tail latency? |
| Authorization size | Domain corpus | How do principals, groups, memberships, grants, and candidate rejection affect each principal shape? |
| History and operations | Live domain rows | How do revision depth, audit events, tasks, and delivery history affect current reads and retention work? |
| Payload and computed work | Row counts | Where do JSON size, filtering selectivity, computed materialization, and rebuild time become dominant? |
| Concurrency | Corpus and request mix | At what client count do pool waits, timeouts, CPU, memory, and throughput become nonlinear? |
| Lifecycle | Logical corpus | How do loading, backup size, verification, restore, and computed rebuilding scale, and where do supported ceilings fail? |

For every curve, report absolute values, percentage changes, normalized slopes,
correctness, timeouts, database and index growth, WAL, CPU and memory, and the
exact topology. Averages alone can hide the relevant decline; retain p95/p99,
first/middle/final traversal pages, and principal-specific results.

## Measurement Phases

The versioned workload includes collection, class, object, relation, graph,
history, aggregation, computed-field, unified-search, task, event, and mutation
operations. It runs these phases in order:

1. Cold first-touch requests on a fresh production process and freshly loaded
   database.
2. Deliberate warmup, followed by warm single-client samples.
3. Complete cursor traversals with duplicate, missing-row, total, and sparse
   authorization checks.
4. Deterministic mixed traffic at moderate and higher concurrency.
5. A deterministic weighted interactive mix.
6. Create, conditional update, and conditional delete operations last, so
   mutations cannot alter earlier read samples.

Reports contain per-scenario request counts, p50/p95/p99/max latency,
throughput, bytes, returned graph nodes and edges, pages, traversal time,
timeouts, failures, resource use, first/middle/final traversal-page latency,
authorization candidate and returned counts, storage and index size,
write-ahead growth, storage and pool metric deltas, backend identity and
effective settings, and lifecycle durations. The runner starts and stops the
server itself, and its artifacts never contain bearer tokens, password hashes,
database URLs, or environment dumps.

## Backup and Restore Lifecycle

When `--admin-binary` is supplied, the runner first streams temporal state and
history rows to count a conservative lower bound on compact backup bytes.
The preflight retains one row at a time and stops above the profile's declared
document-size ceiling. Reports distinguish this lower bound from a generated
artifact's measured size and record preflight time separately. An already
over-limit corpus is reported as `backup_exceeds_supported_ceiling` without
materializing the full document. Fixture revision/history invariants are checked
independently before workloads, including when backup generation is over its ceiling.
Otherwise the runner creates a backup and invokes `hubuum-admin --verify-backup`. Supplying `--restore-test-database-url` additionally restores
into a separately created empty database and performs semantic verification.
Profile version 2 declares a 1 GiB backup/restore document ceiling for both
profiles. The large profile allows 4,000,000 enumerated rows and huge allows
16,000,000, including live rows and history. These explicit provisioning policies
are recorded in the manifest and passed to the server and administrator.
Production defaults remain 256 MiB and 1,000,000 rows. Custom profile files can
select a different envelope; omitted row limits retain the production default. Bulk loading seeds
one open temporal baseline for every live resource and preserves the additional
revisions in the history overlay. Audit revision fields retain matching snapshots.
Administrator failures report the failing stage, exit status, and diagnostic
output with configured connection strings and token keys redacted.

Lifecycle results distinguish a successful offline verification, a successful
isolated restore, an unsupported backup, an artifact that exceeds the ordinary
256 MiB ceiling, and a verification or restore failure. The measured size is
recorded and every generated document is removed after the drill, including failures;
the runner does not silently exempt scale artifacts from production limits.
The preflight is a size check, not proof of backup validity; generation and
verification still run for every corpus admitted by the preflight.
Use `--skip-lifecycle` only for focused local iteration.

Backup generation also enforces the production byte and row-work budgets during
capture, even when the preflight's lower bound fits. The preflight omits
operational sections and framing, so it cannot guarantee admission. A capture
limit failure produces no artifact and is reported as a backup-stage failure
with the resource-limit diagnostic; it must not be claimed as successful
verification or restore. The default work ceiling is 1,000,000 enumerated rows.
A corpus may still exceed either explicit ceiling; preflight reports that limitation
without claiming verification or restore.

### Resource envelope and lifecycle-only runs

Each administrator stage has a 1,800-second deadline and a sampled 6 GiB total
resident-memory limit for the process and its descendants. Reports record
elapsed milliseconds, sampled peak resident bytes, and the termination reason.
The supervisor checks every 25 ms, kills the administrator process group on Unix
when a bound is exceeded, and reaps the administrator. It also terminates remaining
descendants when the administrator exits normally or fails, before reaping its
leader so the process-group ID cannot be reused during cleanup. Sampling permits overshoot and can miss short
peaks; use an OS/container memory limit for a hard host-resource boundary. A
memory termination is reported as `<stage>_exceeds_memory_envelope`, an explicit
unsupported-envelope result like the document-size ceiling. It does not count
as a data-correctness failure in comparative assessment and never claims a
successful verification or restore. Deadlines, diagnostic overflow, and other
execution/verification failures still fail correctness assessment. Windows can
terminate the direct child but does not provide the Unix process-group guarantee.

Only one backup document is staged at a time in an exclusively created directory.
Its logical bytes are bounded during capture and serialization; atomic rename
uses one temporary file rather than a second full copy. Provision at least the
configured document ceiling plus 2 MiB for diagnostics, headroom for sampling
overshoot, filesystem overhead,
and free space for the database and its WAL separately. Diagnostic files have a
sampled 1 MiB cap each and bounded reads. The runner removes its private backup
directory and diagnostic captures on both success and handled failure. Abrupt
termination of the runner itself can leave its administrator and directory;
stop the administrator process group before removing the directory and retrying.
CI uploads only report JSON/Markdown, excludes artifact directories,
and retains reports for 14 days. It never uploads generated backup contents.

After `load`, run only the fixture validation and backup lifecycle:

```bash
target/release/hubuum-scale-benchmark lifecycle \
  --profile large \
  --database-url postgres://localhost/hubuum_scale_local \
  --admin-binary target/release/hubuum-admin \
  --restore-test-database-url postgres://localhost/hubuum_scale_restore \
  --output target/scale/lifecycle.json \
  --artifact-directory target/scale/lifecycle-artifacts
```

The source must be the disposable loaded fixture; the restore database must be
empty. The command writes the manifest, provisioning policy and lifecycle report
and exits unsuccessfully unless offline verification (and the requested restore)
completed. It does not silently accept a size limitation as a completed drill.

### Superseded large-profile memory measurements

On 23 October 2026, the version-2 large fixture (seed `36520260901`, 250,000
objects and 1,000,000 object relations, plus temporal histories and operational
overlays) was loaded and verified against PostgreSQL 18.6. Its temporal-state
lower bound was **837,811,676 bytes**, already above the ordinary 256 MiB ceiling.
Both runs admitted 1 GiB of document bytes and 4,000,000 scanned rows:

| Configured ceiling | Preflight | Backup before termination | Reported aggregate | Result |
| --- | --- | --- | --- | --- |
| 6 GiB (bundled profile) | 232.772 s | 28.711 s | 6,487,523,328 bytes | Memory envelope exceeded |
| 12 GiB (explicit custom profile) | 204.672 s | 57.656 s | 12,920,004,608 bytes | Memory envelope exceeded |

These reports **do not measure actual process RSS**. The supervisor used
`sysinfo`'s default process refresh, which also lists Linux threads as tasks with
their process as parent. It added each thread's shared RSS to the process RSS.
Tokio worker threads therefore multiplied the reported total. The corrected
supervisor disables task enumeration while retaining real child processes.

The administrator was killed and reaped and the private artifact directories
were removed in both runs. Neither produced a completed backup document;
verification and restore were not attempted. The fixture ran on Linux x86-64,
an Intel Xeon Silver 4216 (16 cores,
32 threads), 62 GiB RAM, with a Rust 1.98.0 release administrator using `opt-level=z`,
fat LTO, both TLS features and embedded migrations. Other validation builds were
running, so timings are diagnostic observations, not performance baselines.

The earlier conclusion that the large corpus exhausted 6 GiB and 12 GiB is
withdrawn. A fresh run on `283ce8ce` reproduced the incorrect rejection at a
reported 6,454,394,880 bytes after 14.748 seconds; a bounded 100,000-row capture
measured independently with the system allocator profiler and `/usr/bin/time`
used 147,304,414 bytes of peak heap and 222,976 KiB peak RSS while retaining
34,187,173 compact logical bytes. Limiting allocator arenas did not correct the
supervisor's accounting error.

Use corrected process-only measurements to decide whether the materialized
representation fits a provisioned envelope. The huge profile has no successful
measured lifecycle here. To select a different envelope, copy the TOML profile, edit
`lifecycle_max_resident_bytes`, and pass `--profile large --profile-spec <file>`.
The harness enforces and records that choice; it does not certify it in advance.

### Format decision

Keep the existing compact versioned JSON format and production limits. Measure
each phase before selecting a larger deployment envelope. Compression could
reduce artifact and disk bytes but would not reduce the decoded row graph.
The initial memory changes retained complete decoded snapshots. The streaming
implementation below replaces those graphs with replayable spools while retaining
the same JSON format.

### Initial memory-phase measurements

On 24 October 2026, `/usr/bin/time -v` measured the same version-2 large corpus
against baseline `283ce8ce`. The completed artifact contains 2,589,029 logical
rows in 857,313,491 bytes. Both binaries used Rust 1.98.0, release `opt-level=3`,
fat LTO, one codegen unit, embedded migrations, and both TLS features.

| Operation | Baseline peak RSS | Changed peak RSS |
| --- | --- | --- |
| Capture | 6.35 GiB | 5.62 GiB (sampled supervisor) |
| Offline verification | 11.85 GiB | 6.38 GiB |
| Isolated restore and comparison | Not measured | 10.53 GiB |

Offline verification used about 46% less peak resident memory and produced the
same report except for its timestamp, including the canonical state digest.
It still exceeded the bundled 6 GiB envelope. These successful probes used a
16 GiB virtual-address-space ceiling and a 1,800-second timeout. Builds ran
concurrently, so elapsed times are diagnostic rather than speed comparisons.
Exact bytes, commands' resource bounds, and fixture details are recorded in the
[sanitized measurement summary](performance/backup_memory.json).

A full lifecycle attempt with an explicit 8 GiB resident-memory envelope
completed capture and offline verification. The corrected supervisor measured
6,034,833,408 bytes during capture and 6,853,775,360 during offline verification.
It stopped isolated restore after 56.899 seconds at 8,658,653,184 bytes. This is
an actual envelope failure, unlike the superseded thread-counting reports above.
It does not certify an 8 GiB restore deployment. The capture result uses sampled
RSS rather than the independent runs' operating-system high-water mark.

Reusing the verified artifact, an independent restore-and-compare run passed
under the diagnostic 16 GiB address-space ceiling. It peaked at 11,302,916,096
resident bytes and took 1,218.19 seconds, including 707.633 seconds for restore
staging, confirmation, and application. The verifier matched both authoritative
state and retained history, preserved the canonical state digest, and reset the
disposable schema. Normal computed-data reindex jobs were queued; their worker
completion was not measured. This run establishes correctness at a larger
diagnostic bound, not a production memory-sizing recommendation. The bundled
6 GiB envelope and production document limits remain unchanged.

### Initial reductions in retained memory

The administrator serializes backups directly through a bounded buffered writer
to its owner-only temporary file. A limit or write failure removes the partial
file and leaves an existing destination intact. Task-backed backup output still
uses an in-memory byte buffer for its database artifact, but releases the decoded
document before persisting that buffer.

Restore validation transfers the decoded sections into the validated snapshot
instead of cloning them. Offline verification hashes canonical JSON directly into
the digest. Isolated restore verification keeps the source in compact byte form
during restore. Before capturing the destination, it fingerprints and releases
the large object, object-relation, schema-evidence, and corresponding history
sections. Fingerprints cover every value and row order. Sources needing legacy
computed-history baselines retain the full source for that reconstruction.
These initial measurements predate streaming: capture and decoding retained
complete row trees. The following implementation replaces that retention.

PostgreSQL restore consumes each validated section in batches of at most 256 rows
and a target of 1 MiB of encoded JSON. A single larger row is admitted alone and
remains bounded by the document limit. Collection rows retain one statement,
because their immediate parent foreign key permits forward references within
that statement. All batches remain inside the existing fenced transaction;
failure rolls back the complete restore. The batch target does not bound total
process memory or PostgreSQL transaction memory.

### Streaming measurements

The streaming implementation completed the full large lifecycle on 24 October
2026 under the same explicit 8 GiB administrator RSS envelope that stopped the
initial implementation. This reloaded version-2 fixture has 250,000 objects,
1,000,000 object relations, 2,589,029 logical rows, and an 857,313,491-byte backup.
The restore applied 74 migrations, matched authoritative state and retained
history, and reset the disposable target schema.

| Stage | Sampled administrator peak RSS | Elapsed time |
| --- | --- | --- |
| Capture | 115 MiB | 469.855 s |
| Offline verification | 116 MiB | 76.152 s |
| Isolated restore and comparison | 148 MiB | 1,227.648 s |

An independent `/usr/bin/time -v` comparison used the exact same artifact with
the initial materialized binary and the streaming binary. Offline verification
fell from 6,852,456,448 resident bytes (6.38 GiB) to 120,356,864 bytes (115 MiB), a
98.24% reduction. Reports matched except for the verification timestamp. The
state digest differs from the earlier fixture load because generated credential
salts vary; the two verifiers agree on this artifact's digest.

That independent verification took 26.66 s with materialization and 77.09 s with
streaming. Replay trades additional parsing and temporary-disk I/O for memory.
Builds ran concurrently, so these timings are diagnostic rather than controlled
speed comparisons. The supervisor measures administrator process RSS, excluding
PostgreSQL server memory and filesystem cache; the run does not establish a
whole-container limit or a production sizing recommendation. The bundled 6 GiB
envelope, production document limits, and unmeasured huge-profile status remain
unchanged.

The recorded runner lost the restore sub-duration when a structured setup log
preceded its JSON report. Its semantic timing therefore contains the combined
restore/comparison stage, and its zero computed-rebuild field is an old
placeholder, not a worker measurement. The runner now reads the JSON report
after structured logs and leaves unmeasured rebuild timing unset. The final
code also requires the new migration in its schema-readiness checkpoint; these
post-measurement corrections do not change the streaming algorithm. Exact
figures and qualifications are in the
[sanitized measurement summary](performance/backup_memory.json).

### Streaming implementation: indexed JSON replay

The existing JSON format supports row streaming without a format migration.
PostgreSQL capture writes one row at a time from its repeatable-read cursor to a
private spool. It enforces the existing capture byte and row limits, including
excluded history rows inspected during enumeration. The artifact is published
only after validation; administrator file output remains an atomic replacement.

Restore uploads and administrator inputs are copied to an owner-only temporary
file while computing the exact-byte SHA-256. PostgreSQL commits the stage and
chunks of at most 1 MiB in one transaction. A separately deployed executor can
recover those bytes after a restart. Reads check chunk order, total length, and
SHA-256. Old BYTEA stages are read through bounded substrings. Expiry, failure,
and successful restore erase staged chunks through the same lifecycle as the
control record.

The parser uses Serde visitors to consume individual rows. It accepts arbitrary
member and section order and rejects duplicate envelope fields and sections.
Parsing records section offsets and row counts in a second private spool;
normalization and semantic validation then replay those sections. The shared
validators retain identity, revision, relationship, and schema indexes while
object and historical payloads stay on disk. These are multiple logical passes,
not exactly two scans: individual validators replay the sections they need.
The private validated row source owns its file handle through application, so
restore does not reopen an input path after validation. Read and insertion errors
propagate through the existing fenced transaction and roll back all changes.

Memory still scales with validation indexes, schema/definition metadata, the
largest individual row, and the insertion batch. Collection rows use one SQL
statement to preserve forward parent references. PostgreSQL transaction memory
is independent of the application spool. The in-memory backend implements the
same mandatory replay capabilities, materializing rows only to build its own
in-memory state. The former materialized SDK methods have been removed.
Backup task artifact storage/download still uses a byte buffer; capture no
longer keeps the decoded graph alongside it.

Input bytes use the configured upload limit. Parsing admits no more rows than
half that byte limit (the minimum encoded row is `{}`). Serde's recursion limit
remains enabled. Canonical row spools admit up to four times the input byte limit
to allow legacy defaults; each row also consumes one newline on disk. Preparing
history may use a further bounded expansion for new baseline fields. These are
per-spool limits, not a total filesystem quota. Raw, canonical, and prepared
spools can coexist, so provision temporary storage accordingly. Files are private
and automatically removed when dropped, including on failed validation. A single
large row or schema still needs to fit in memory; this is not a constant-memory
parser or a substitute for realistic upload limits.

Apply the chunk-storage migration and upgrade all restore executors before
accepting new stages. Old executors cannot read new chunked stages. The rollback
migration refuses to discard retained chunks: complete or expire those stages
first. CLI restore and backup verification now enforce `--restore-max-upload-bytes`.
The limit also applies to piped restore input; verification requires a regular file.

A section-framed NDJSON or archive format could simplify record boundaries and
per-section compression, but would require another format version and two
readers. Cross-section validation would still need indexes and replay. Larger
corpora may eventually justify disk-backed indexes. External artifact storage
in [#252](https://github.com/hubuum/hubuum/issues/252) remains a separate decision.

## CI Policy

Scale jobs are expensive and never run because `ci:full`, `ci:benchmarks`, or
an ordinary benchmark-path change requested them. Pull requests opt in with
these labels:

- `ci:scale-large` runs the large profile under standard and extended limits.
- `ci:scale-huge` runs the huge profile under standard and extended limits.

Applying both labels runs both profiles. The workflow can also be dispatched
manually with explicit profile and limit selections. A weekly schedule runs the
large profile and a monthly schedule runs the huge profile. Superseded runs are
cancelled. Each profile and limit combination has a distinct job summary and
artifact name. It holds one binary, workload, backend, seed, and limit mode
fixed; measures a fresh baseline; then measures each calibrated object and
object-relation growth point against its own fresh database. A final job
consolidates exact corpus sizes, search and relation latency/throughput,
traversal pagination, and database/index costs into one updatable `Scale Growth
Report` comment with a link to the workflow and its sanitized 14-day artifacts.
The report explicitly leaves code-regression detection to `rust-pr-bench`.

Generator code, profile and workload specifications, and the scale workflow
have an explicit `scale_benchmark` classification in
`scripts/classify-ci-changes.sh`. Update its regression test when moving or
adding inputs.
