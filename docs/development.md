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

The `hooks/pre-commit` hook automatically runs `cargo clippy` before each commit. If clippy finds any warnings or errors, the commit will be prevented until they are fixed.

### Features

- ✅ Runs clippy with `-D warnings` to treat all warnings as errors
- ✅ Prevents commits that fail clippy checks
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
```

## Benchmarks

Benchmarking runs in a separate GitHub workflow, `.github/workflows/benchmarks.yml`, via `terjekv/github-action-iai-callgrind`.

### Local execution

The current benchmark targets are split one benchmark binary per file so CI can fan them out independently:

```bash
cargo bench --bench parse_query_parameter_callgrind
cargo bench --bench parse_integer_list_callgrind
cargo bench --bench json_sql_filters_callgrind
cargo bench --bench object_validation_geo_callgrind
cargo bench --bench object_validation_nested_callgrind
cargo bench --bench database_url_parsing_criterion -- --noplot
```

`iai-callgrind` requires `valgrind` to be installed locally.

### CI behavior

- The benchmark workflow runs both backends in one combined `backend: all` job, so PRs get a single consolidated benchmark report.
- `iai-callgrind` remains the practical gating signal with a low regression threshold.
- Criterion still runs in the same combined job, but uses a very high regression threshold so it reports timing changes without acting as a meaningful gate.
- The current benchmark set is fully self-contained and does not require a database in CI.

### Adding or modifying benchmarks

- Put new benchmark entrypoints in `benches/`.
- Keep each benchmark target in its own file so the benchmark workflow can fan out per bench binary.
- Add a matching `[[bench]]` stanza in `Cargo.toml` with `harness = false`.
- Include `callgrind` in the benchmark filename when it should be auto-discovered by the CI workflow.
- Include `criterion` in the benchmark filename when it should be Criterion-only in CI autodiscovery.
- Prefer deterministic library-level code paths such as parsers, query builders, and serialization helpers over handlers that require network or database setup.
