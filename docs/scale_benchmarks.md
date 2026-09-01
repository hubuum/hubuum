# Scale Operational Benchmarks

The scale suite generates deterministic, skewed PostgreSQL data and drives it
through a production `hubuum-server` process with API and worker roles enabled.
It is designed for operational validation, not microbenchmarking. Timing and
resource measurements are evidence; correctness failures fail the run.

The loader replaces data in a freshly migrated, disposable database. Never point
it at a development, staging, or production database.

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

Build the feature-gated runner and the real production binaries:

```bash
cargo build --locked --features scale-benchmark \
  --release \
  --bin hubuum-scale-benchmark --bin hubuum-server --bin hubuum-admin
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
be run separately. `measure` requires a manifest from the loaded database. The
following comparison fails on corpus drift or correctness drift while keeping
latency changes informational:

```bash
target/release/hubuum-scale-benchmark assess \
  --base target/scale/base.json \
  --head target/scale/head.json \
  --markdown-output target/scale/summary.md
```

## Measure Scale Sensitivity

Base/head assessment asks whether code changed at one fixed corpus size. It
does not answer the marginal cost of more data, and comparing `large` with
`huge` is not causal because every dataset dimension changes together.

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

The baseline uses the same command without `--add-objects`. Use
`--add-object-relations` for the relation-volume axis. Object increments are
distributed through existing balanced classes. Relation increments are spread
through existing balanced class relations, whose source and target classes
have enough pair capacity for useful volume experiments. The increment has no
default; it is an experimental input, not a benchmark constant.

Compare the two sanitized reports:

```bash
target/release/hubuum-scale-benchmark impact \
  --baseline target/scale/baseline/report.json \
  --comparison target/scale/objects/report.json \
  --axis objects \
  --output target/scale/objects/impact.json \
  --markdown-output target/scale/objects/impact.md
```

The command rejects failed correctness checks, mismatched runtimes or
workloads, and changes outside the declared axis and region. Its JSON and
Markdown report matching-scenario latency and throughput deltas together with
database, table, index, WAL, CPU, memory, generation, loading, backup, restore,
and rebuild deltas when those measurements exist. With no
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
counts, database and index size, WAL growth, storage and pool metric deltas,
PostgreSQL identity and settings, and lifecycle durations. The runner starts
and stops the server itself, and its artifacts never contain bearer tokens,
password hashes, database URLs, or environment dumps.

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
artifact name, and pull-request jobs use the head runner to generate equivalent
base and head corpora before assessing them.

Generator code, profile and workload specifications, and the scale workflow
have an explicit `scale_benchmark` classification in
`scripts/classify-ci-changes.sh`. Update its regression test when moving or
adding inputs.
