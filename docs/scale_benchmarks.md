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

Page limit 250 runs all six curves. Page limit 1,500 repeats only balanced
object and spread-relation growth, where it provides a useful pagination
control. This avoids multiplying every shape by every operational setting.

Use `sensitivity-plan --limit-mode standard` (or `extended`) to turn the
relative matrix into exact increments for a profile, then produce one `impact`
report per fresh-database point. After all points complete,
`summarize-sensitivity` validates that the expected mode-specific matrix is
complete. Its first table makes relative p95 curves and pagination knees easy
to scan; the detailed tables retain exact baseline and expanded corpus sizes,
absolute measurements, and relative costs. Once repeated trials are available,
a line chart with uncertainty bands will communicate trends better than a
single-run line. Until then, connecting three informational points would imply
more certainty than the measurements support.

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
| Graph shape | Total edge count | How do hub degree, component width, and traversal depth change cost independently of volume? |
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
throughput, bytes, pages, traversal time, timeouts, failures, resource use,
first/middle/final traversal-page latency, authorization candidate and returned
counts, storage and index size, write-ahead growth, storage and pool metric
deltas, backend identity and effective settings, and lifecycle durations. The
runner starts and stops the server itself, and its artifacts never contain
bearer tokens, password hashes, database URLs, or environment dumps.

## Backup and Restore Lifecycle

When `--admin-binary` is supplied, the runner creates a backup under the
profile's declared document-size limit and invokes `hubuum-admin
--verify-backup`. Supplying `--restore-test-database-url` additionally restores
into a separately created empty database and performs semantic verification.
The large profile retains the ordinary 256 MiB ceiling; the huge profile
declares an elevated 1 GiB provisioning envelope. Both values are recorded in
the manifest and applied to the server and administrator processes.

Lifecycle results distinguish a successful offline verification, a successful
isolated restore, an unsupported backup, an artifact that exceeds the ordinary
256 MiB ceiling, and a verification or restore failure. The measured size is
recorded and an over-limit generated document is removed instead of uploaded;
the runner does not silently exempt scale artifacts from production limits.
Use `--skip-lifecycle` only for focused local iteration.

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
