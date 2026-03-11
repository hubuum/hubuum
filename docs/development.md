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
- `src/db/traits/namespace/`:
  `relations.rs`, `records.rs`, `permissions.rs`

The `mod.rs` files in these folders re-export the public backend traits so existing imports (`crate::db::traits::user::*`, `crate::db::traits::namespace::*`) keep working.
