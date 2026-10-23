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
artifacts, and reports are CI artifacts and must not be committed.

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
The supervisor checks every 25 ms and kills/reaps the administrator process group
on Unix when a bound is exceeded. Sampling permits overshoot and can miss short
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

### Measured large-profile limitation

On 23 October 2026, the version-2 large fixture (seed `36520260901`, 250,000
objects and 1,000,000 object relations, plus temporal histories and operational
overlays) was loaded and verified against PostgreSQL 18.6. Its temporal-state
lower bound was **837,811,676 bytes**, already above the ordinary 256 MiB ceiling.
Both runs admitted 1 GiB of document bytes and 4,000,000 scanned rows:

| Process memory ceiling | Preflight | Backup before termination | Sampled peak RSS | Result |
| --- | --- | --- | --- | --- |
| 6 GiB (bundled profile) | 232.772 s | 28.711 s | 6,487,523,328 bytes | Memory envelope exceeded |
| 12 GiB (explicit custom profile) | 204.672 s | 57.656 s | 12,920,004,608 bytes | Memory envelope exceeded |

The administrator was killed and reaped and the private artifact directories
were removed in both runs. Neither produced a completed backup document;
verification and restore were not attempted. These peaks include sampling
overshoot. The fixture ran on Linux x86-64, an Intel Xeon Silver 4216 (16 cores,
32 threads), 62 GiB RAM, with a Rust 1.98.0 release administrator using `opt-level=z`,
fat LTO, both TLS features and embedded migrations. Other validation builds were
running, so timings are diagnostic observations, not performance baselines.

The default large corpus therefore **does not have a completed lifecycle within
the measured 6 GiB or 12 GiB process envelopes**. Keep that explicit limitation;
raising the byte limit is not evidence of memory safety or restore support.
The existing materialized backup representation needs further retained-memory
reduction or a streaming/versioned design before claiming this corpus works on
ordinary CI runners. The huge profile has no successful measured lifecycle here.
To select a different provisioned envelope, copy the TOML profile, edit
`lifecycle_max_resident_bytes`, and pass `--profile large --profile-spec <file>`.
The harness enforces and records that choice; it does not certify it in advance.

### Format decision

Use the existing compact versioned JSON backup and an explicitly provisioned
extended ceiling. Compression could reduce artifact/disk bytes but would not
reduce the decoded row graph retained by verification/restore. A streaming format
would require incremental validation, transaction-safe restoration, bounded
cross-section reference checks and new compatibility/version handling. Neither
is necessary to measure the current envelope, and neither is implied by a larger
byte setting. Preserve the production capture limits and report measurements
before selecting a format change. Backup capture already streams rows from
PostgreSQL, but the admitted document and restore graph are still materialized.
