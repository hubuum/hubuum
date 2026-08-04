# Development Guide

## Git Hooks Setup

This project includes git hooks to maintain code quality standards. The hooks are stored in the `hooks/` directory and can be shared across the team.

## Setup

After cloning the repository, configure git to use the hooks directory:

```bash
git config core.hooksPath hooks
```

That's it! Git will automatically run hooks from the `hooks/` directory from now on.

## Pre-commit Hook

The `hooks/pre-commit` hook automatically runs `cargo clippy` and rebuilds `docs/openapi.json` before each commit. If clippy fails or the OpenAPI document cannot be regenerated, the commit is prevented.

### Features

- ✅ Runs clippy with `-D warnings` to treat all warnings as errors
- ✅ Rebuilds and stages `docs/openapi.json` from the current code
- ✅ Prevents commits that fail clippy checks
- ✅ Prevents commits if OpenAPI generation fails
- ✅ Clear error messages guide developers on how to fix issues
- ✅ Stored in version control and shared with the team
- ✅ No installation script needed - git handles it automatically via `core.hooksPath`

### Manual Checks

You can also manually run clippy at any time:

```bash
# Check for clippy issues
cargo clippy --all-targets

# Fix clippy issues with automatic suggestions
cargo clippy --all-targets --fix

# Rebuild the committed OpenAPI spec
cargo run --quiet --bin hubuum-openapi > docs/openapi.json
```

## Architecture Overview

The codebase is intentionally split into model-facing APIs and database-facing implementations.

- `src/models/*`:
  Public domain models and high-level operations.
  These should not contain Diesel query construction for non-trivial backend logic.
- `src/traits/*`:
  Public behavioral interfaces used by handlers and models.
  `BackendContext` is the boundary type that allows these APIs to accept either `DbPool` or wrappers (for example `web::Data<DbPool>`).
- `src/db/traits/*`:
  Diesel/Postgres-backed implementations behind the public traits.
  This is where query details, joins, filters, and transactions belong.

### Practical layering rule

When adding a feature:

1. Extend or add a trait in `src/traits` (or `src/models/traits`) that expresses the behavior.
2. Implement database details in `src/db/traits`.
3. Keep model methods thin by delegating to backend traits.

### Module layout notes

To keep backend code navigable, large trait backends are split into focused modules:

- `src/db/traits/user/`:
  `auth.rs`, `membership.rs`, `permissions.rs`, `search.rs`
- `src/db/traits/collection/`:
  `relations.rs`, `records.rs`, `permissions.rs`

The `mod.rs` files in these folders re-export the public backend traits so existing imports (`crate::db::traits::user::*`, `crate::db::traits::collection::*`) keep working.

### Collection hierarchy implementation

Recursive collections are implemented in the database layer, not in a workspace
crate. The implementation is coupled to Diesel schema modules, PostgreSQL
closure-table SQL, temporal history, `ApiError`, and Hubuum's permission
semantics. Keep hierarchy writes in `src/db/traits/collection/records.rs` and
permission reads in `src/db/traits/collection/permissions.rs` or
`src/db/traits/user/*`.

When adding a collection creation path, use the shared collection insert helper
from the collection backend so `collections` and `collection_closure` stay in
sync. Do not insert directly into `collections` unless the closure rows are
created in the same transaction. When changing permission checks, preserve the
combined-permission rule: a single permission row on the target collection or an
ancestor must satisfy all requested flags.

See [Collection Hierarchy](collection_hierarchy.md) for user-facing behavior,
move constraints, indexes, and the rationale for keeping this logic app-local.

## Pull request CI tiers

Pull request validation is selected from the complete base-to-head diff:

- Draft documentation-only pull requests run Markdown lint when Markdown files
  changed.
- Draft code pull requests run Rust formatting and the complete default-feature
  test suite on Linux with PostgreSQL. They do not run the other feature
  combinations, cross-platform tests, production container build, release
  build, or benchmarks.
- Ready-for-review code pull requests run the complete CI suite. The
  `ready_for_review` event starts that suite immediately, and later pushes keep
  running it.
- Ready-for-review documentation-only pull requests keep the smaller relevant
  checks. `docs/openapi.json` receives the OpenAPI contract check, while
  `docs/export_template_guide.md` is treated as code because it is embedded in
  a binary. `docs/querying.md` is treated as a test input because the API test
  suite compiles and validates its documented operator lists.

The OpenAPI contract check is blocking at every CI tier where API inputs
change. It separately verifies exact generated-document drift and semantic
compatibility with the latest stable release, then uploads generated, baseline,
diff, JSON, and Markdown evidence. The policy scripts, severity configuration,
and breaking-change exception file are themselves classified as OpenAPI inputs.
See [the release guide](releasing.md#openapi-compatibility-gate) for the baseline
and intentional-break rules.

The `ci:full` pull request label forces the complete CI and benchmark suites,
including on a draft or documentation-only pull request. The `ci:benchmarks`
label forces all benchmark jobs without expanding the main CI tier. Adding or
removing either label takes effect immediately; converting a pull request back
to draft cancels superseded work unless a forcing label remains.

The lightweight change classifier is implemented by
`scripts/classify-ci-changes.sh`. Unknown file types are classified
conservatively as code, container, artifact, and benchmark inputs. Keep its
tests in `scripts/test-classify-ci-changes.sh` synchronized with any new build
inputs. Direct literal `include_str!` and `include_bytes!` inputs are discovered
by that test and must be classified as code automatically. Renames are evaluated
as a deletion plus an addition so both the old and new paths affect the selected
tier. The stable `CI gate` job reports the combined result of every applicable
PR or `main` validation job, is the check intended for branch protection, and
must pass before `main-latest` artifacts or container images are published.

## Benchmarks

Benchmarking runs in a separate GitHub workflow, `.github/workflows/benchmarks.yml`, via `terjekv/github-action-iai-callgrind`.

### Local execution

The benchmark targets are split one benchmark binary per file so CI can fan them out independently:

```bash
cargo bench -p hubuum-query --bench parse_query_parameter_callgrind
cargo bench -p hubuum-query --bench parse_integer_list_callgrind
cargo bench --bench json_sql_filters_callgrind
cargo bench -p hubuum-query --bench search_operator_parsing_callgrind
cargo bench --bench permissions_parsing_callgrind
cargo bench -p hubuum-query --bench jsonb_type_inference_callgrind
cargo bench -p hubuum-templates --bench size_limited_writer_callgrind
cargo bench --bench token_storage_hash_callgrind
cargo bench --bench request_hash_callgrind
cargo bench --bench unified_search_query_parsing_callgrind
cargo bench --bench unified_search_cursor_callgrind
cargo bench --bench object_validation_geo_callgrind
cargo bench --bench object_validation_nested_callgrind
cargo bench --bench database_url_parsing_criterion -- --noplot
cargo bench --bench password_hashing_criterion -- --noplot
```

The self-contained CI job auto-discovers `benches/*.rs`. Feature-gated
database benchmarks live in nested benchmark directories with explicit Cargo
paths so they remain in their dedicated jobs without disabling autodiscovery.
The container-build tests enforce this separation.

Gungraun requires `valgrind` and the matching benchmark runner to be installed
locally:

```bash
cargo install --locked --version 0.19.4 gungraun-runner
```

The PostgreSQL storage benchmark is opt-in and requires an empty, migrated,
disposable benchmark database. Fixture creation, cleanup, and warmup happen
outside the timed regions. The create scenario intentionally leaves its
append-only audit events behind:

```bash
export HUBUUM_BENCH_DATABASE_URL=postgres://postgres:postgres@localhost/hubuum_bench
cargo run --features embedded-migrations --bin hubuum-admin -- \
  --migrate --database-url "$HUBUUM_BENCH_DATABASE_URL"
cargo bench --features postgres-bench \
  --bench storage_postgres_criterion -- --noplot
```

The runtime behavior benchmark is also opt-in and requires a migrated,
disposable database. It starts an all-role primary and an API-only standby,
measures idle Prometheus counter deltas, sends fixed readiness traffic, and
inserts one intentionally invalid export task to measure PostgreSQL
notification-to-claim latency. Build the server before running the benchmark;
the development profile avoids irrelevant release-link overhead:

```bash
export HUBUUM_BENCH_DATABASE_URL=postgres://postgres:postgres@localhost/hubuum_runtime
cargo run --features embedded-migrations --bin hubuum-admin -- \
  --migrate --database-url "$HUBUUM_BENCH_DATABASE_URL"
cargo build --features runtime-behavior-bench --bin hubuum-server
cargo bench --profile dev --features runtime-behavior-bench \
  --bench runtime_behavior -- measure \
  --server-binary target/debug/hubuum-server \
  --database-url "$HUBUUM_BENCH_DATABASE_URL" \
  --sample-seconds 60 \
  --label local \
  --output target/runtime-behavior/local.json
cargo bench --profile dev --features runtime-behavior-bench \
  --bench runtime_behavior -- assess \
  --head target/runtime-behavior/local.json
```

The JSON report separates connection-pool acquisitions by caller, records
task/fan-out timer rates and per-iteration checkout ratios, verifies one
readiness checkout per request, and records notification wake-up and task-claim
latency. Connection acquisitions are pooled checkouts, not new PostgreSQL
network connections.

The deterministic PostgreSQL query budgets use the normal isolated test
database runner. The central storage suite covers point reads, hierarchy and
permission traversal, paginated object and history reads, and event-producing
writes:

```bash
source .env && ./run_tests.sh storage_performance
```

Import planning, export hydration, and event fan-out budgets live beside those
private execution paths and run as part of the full test suite. Fixed-size
operations pin exact domain, transaction-control, query-fingerprint, and
connection-checkout counts. Cardinality tests compare small and large inputs to
pin either constant query shapes or an explicit bounded-linear slope.

The capture excludes the pool's internal `SELECT $1` checkout-validation probe
from application query totals. Each checkout is counted separately, which
keeps pool-use regressions visible without attributing a connection-health
query nondeterministically to the next operation.

### CI behavior

- Draft pull requests skip benchmarks unless `ci:full` or `ci:benchmarks` is
  present. Ready pull requests run only the benchmark jobs affected by their
  base-to-head diff.
- Change classification happens before PostgreSQL services or benchmark
  runners are allocated. Superseded benchmark workflow runs are cancelled.
- The self-contained benchmark job runs both backends in one combined
  `backend: all` job, so PRs get a single consolidated benchmark export.
- Gungraun's Callgrind measurements remain the practical gating signal with a
  low regression threshold.
- Criterion still runs in the same combined job, but uses a very high regression threshold so it exports timing changes without acting as a meaningful gate.
- A separate PostgreSQL job runs storage Criterion benchmarks against
  isolated base and pull-request databases. It warns above a 10% median change
  and fails above 20% only when the 95% confidence interval also indicates a
  regression.
- A two-process runtime behavior job records base/head Prometheus counter
  deltas and publishes both JSON reports plus a Markdown comparison. Absolute
  budgets guard idle polling, database checkout ratios, readiness behavior, and
  notification-driven task claims; a 25% base/head threshold catches larger
  behavioral regressions while tolerating timer-boundary jitter.
- The PostgreSQL query-budget tests are the stricter gate: fixed operation
  totals, control/domain splits, query fingerprints, connection checkouts, and
  declared scaling slopes must remain stable.
- On the harness's first pull request there is no base target to execute, so CI
  records the initial baseline. Later pull requests compare base and head.

### Adding or modifying benchmarks

- Put new benchmark entrypoints in `benches/`.
- Keep each benchmark target in its own file so the benchmark workflow can fan out per bench binary.
- Add a matching `[[bench]]` stanza in `Cargo.toml` with `harness = false`.
- Include `callgrind` in the benchmark filename when it should be auto-discovered by the CI workflow.
- Include `criterion` in the benchmark filename when it should be Criterion-only in CI autodiscovery.
- Prefer deterministic library-level code paths such as parsers, query builders, and serialization helpers over handlers that require network or database setup.
- Put database-backed targets behind the `postgres-bench` feature so the
  self-contained benchmark fan-out does not try to execute them without a
  database.
- Seed, migrate, warm, and clean PostgreSQL fixtures outside measured regions.
  Mutation benchmarks run last against fresh isolated base/head databases;
  emitted audit events remain append-only, as they do in production.
- Avoid code paths that read the global `CONFIG` (the clap-backed application configuration). Initialising it inside a benchmark binary panics on the harness's own CLI arguments (for example `--iai-run`). Where a function needs configuration values such as page limits, prefer a config-free entry point that takes them as parameters (see `parse_unified_search_query_with_limits` and `validate_page_limit_with_max`).
