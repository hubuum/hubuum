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

## Benchmarks

Benchmarking runs in a separate GitHub workflow, `.github/workflows/benchmarks.yml`, via `terjekv/github-action-iai-callgrind`.

### Local execution

The benchmark targets are split one benchmark binary per file so CI can fan them out independently:

```bash
cargo bench --bench parse_query_parameter_callgrind
cargo bench --bench parse_integer_list_callgrind
cargo bench --bench json_sql_filters_callgrind
cargo bench --bench search_operator_parsing_callgrind
cargo bench --bench permissions_parsing_callgrind
cargo bench --bench jsonb_type_inference_callgrind
cargo bench --bench token_storage_hash_callgrind
cargo bench --bench request_hash_callgrind
cargo bench --bench unified_search_query_parsing_callgrind
cargo bench --bench unified_search_cursor_callgrind
cargo bench --bench object_validation_geo_callgrind
cargo bench --bench object_validation_nested_callgrind
cargo bench --bench database_url_parsing_criterion -- --noplot
cargo bench --bench password_hashing_criterion -- --noplot
```

`iai-callgrind` requires `valgrind` to be installed locally.

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

- The self-contained benchmark job runs both backends in one combined
  `backend: all` job, so PRs get a single consolidated benchmark export.
- `iai-callgrind` remains the practical gating signal with a low regression threshold.
- Criterion still runs in the same combined job, but uses a very high regression threshold so it exports timing changes without acting as a meaningful gate.
- A separate PostgreSQL 17 job runs storage Criterion benchmarks against
  isolated base and pull-request databases. It warns above a 10% median change
  and fails above 20% only when the 95% confidence interval also indicates a
  regression.
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
