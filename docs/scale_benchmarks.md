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
