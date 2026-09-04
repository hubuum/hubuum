# Repository Guidelines

## Verification

- Run the test suite with `source .env && ./run_tests.sh`.
- The test runner runs tests in parallel. If you need to run more than a few targeted tests, run the full suite instead of trying to manually select a large subset.
- `cargo clippy` should pass for all code before changes are considered complete.
- Run clippy as `cargo clippy --all-targets -- -D warnings`.
- `rustfmt` should pass for all Rust code. Keep formatting mechanical and avoid hand-formatting that fights `rustfmt`.
- Regenerate OpenAPI after endpoint or schema changes before considering the change complete.
- Markdown lint must pass for all `*.md` files. Run it locally with `npx markdownlint-cli2 --config .markdownlint.json "**/*.md" "!target"` before considering documentation changes complete. Every fenced code block must declare a language (use `text` for plain ASCII/diagrams), and tables must use a single, consistent column style (MD060).
- When adding or moving build, test, lint, benchmark, or embedded-file inputs, verify that `scripts/classify-ci-changes.sh` selects the required CI targets and update `scripts/test-classify-ci-changes.sh` when needed. Unknown paths intentionally receive conservative validation. Direct literal `include_str!` and `include_bytes!` inputs are checked automatically; dynamically constructed paths still require manual review.

## Container Builds

- The Docker dependency-cache stage copies workspace manifests explicitly. Whenever `[workspace].members`, a workspace crate manifest, `Cargo.lock`, Docker build features, `Dockerfile`, or `entrypoint.sh` changes, treat the production container as an affected build target.
- When adding or removing a workspace member, update the manifest-only `COPY` entries in `Dockerfile` in the same change. A normal host `cargo build` is not a substitute because it can see files that are absent from Docker's dependency-cache stage.
- Run the fast parity regression test first: `cargo test --bin hubuum-server dockerfile_copies_every_workspace_manifest --locked`. It requires the Dockerfile's workspace-manifest `COPY` set to exactly match Cargo's workspace member set.
- Then build the real production image with the feature combination used for the full published container: `docker build --build-arg 'CARGO_BUILD_FLAGS=-F tls-rustls -F tls-openssl --locked --release' --tag hubuum-server:verify .`.
- A container-affecting change is not complete until both the parity test and the real Docker build pass. Keep the pull-request container-build CI check enabled so the restricted Docker build context is exercised before merge.

## Architecture

- Preserve validated facts as types across architectural boundaries. Convert raw
  API and database representations once with a fallible constructor, keep the
  resulting proof type private-fielded, and make downstream services and storage
  operations accept that type instead of reconstructing or rechecking the fact.
  Use newtypes for scalar invariants, enums for mutually exclusive or correlated
  states, and capability/proof wrappers for validated, resolved, authorized, or
  claimed state. Keep database constraints, transactions, and fencing for
  concurrent or cross-row invariants; types complement rather than replace them.
- Treat the root `hubuum` library as an internal application composition crate,
  not a supported third-party embedding API. Its `pub` visibility may exist for
  sibling binaries, integration tests, or benchmarks without creating a SemVer
  promise. Keep package classifications consistent with
  `docs/rust_api_boundary.md` and the Rust API policy checker.
- Keep public domain behavior in `src/models/*` and `src/traits/*`.
- Put migrated application use-case orchestration in `src/services/*` and
  backend-neutral persistence capabilities in `src/storage/*`.
- Keep Diesel/Postgres query construction and backend details in
  `src/storage/postgres/*` and `crates/hubuum-storage-postgres`.
- Model methods should stay thin and delegate persistence-heavy work to storage capabilities.
- Route high-level callers through services or operation-shaped backend
  capabilities. Application consumers may pass `AppContext`, `StorageHandle`,
  or a `StorageContext`, but must not acquire, name, or select a
  `PostgresPool`.
  A `StorageContext` must preserve and return its already configured opaque
  handle; it must not expose a pool accessor, reconstruct a backend from one,
  or carry permission-backend selection. Permission-aware use cases require the
  stronger `AuthorizationContext`; storage-only code should not depend on it.
  PostgreSQL adapter helpers accept `PostgresPool` explicitly inside
  `src/storage/postgres/*`; direct `PostgresPool` context compatibility exists
  only for focused tests. Application code must not rely on it, and production
  composition always uses the opaque handle. Likewise, only `AppContext`
  provides production authorization selection; a bare storage handle must not
  silently bypass a configured external policy backend.
- Keep storage capabilities aggregate- or query-shaped rather than table-shaped.
  Every selectable backend must implement the complete storage contract; focused
  in-memory models may exercise shared logical contracts but must not be exposed
  as partially supported backends. PostgreSQL-only tests must retain transaction,
  locking, trigger, concurrency, migration, recovery, and query-budget coverage.
- Give each storage adapter an implementation-owned error type and convert it to
  the backend-neutral `StorageError` at the adapter boundary. Only the application
  error layer converts `StorageError` to `ApiError`.
- Apply backend-neutral tracing and metrics outside individual adapters so a new
  backend cannot silently omit common diagnostics. Report the selected backend
  and non-secret effective settings through the administrator configuration.
- Put multi-step database writes in `with_transaction`; use `with_connection` for single reads, single writes, and non-atomic database work.
- Workspace crates should expose small, explicit interfaces with private fields. Prefer typed request/builder APIs over long positional argument lists when callers must provide several settings.
- Keep workspace crate boundaries clean of app-specific errors, global config, Actix, Diesel, and task persistence unless the crate explicitly owns that layer. A dedicated PostgreSQL storage crate may own Diesel, its generated schema, migrations, pool/TLS setup, transaction helpers, and adapter errors; those types must not leak through the backend-neutral storage contract.
- Avoid leaking third-party implementation types from workspace crate APIs unless they are the intentional integration surface. Use crate-owned structs, builders, traits, and errors at boundaries where practical.
- Use typestate builders when they prevent meaningful invalid call order or missing required data; otherwise prefer a simpler builder with validating terminal methods.

## API Conventions

- New API work should live under the versioned `src/api/v1` routes and handlers unless intentionally changing legacy API behavior.
- Handlers should return `Result<impl Responder, ApiError>` and use the shared response helpers for JSON and pagination.
- Use `ApiError` as the public API error surface. Prefer specific variants and clear messages over generic internal errors.
- Add or update `utoipa` annotations when changing endpoint request/response shapes.
- Keep permission checks close to the handler boundary, using the existing `can!` pattern where applicable.

## Rust Standards

- Follow Rust best practices and the conventions already present in this repository.
- Prefer designs built around newtypes instead of passing primitive values through the domain unchecked.
- Newtypes should usually have validating constructors, private fields, and explicit accessors or setters where mutation is part of the model.
- Endpoints should accept newtypes whenever possible so validation happens at the boundary, as early as possible, with clear and actionable error messages.
- Put behavior on types with `impl` blocks when it naturally belongs to the type. Prefer this over collections of bare functions that operate on loosely related data.
- Keep invariants close to the data they protect. Constructors and setters should reject invalid states rather than relying on callers to remember preconditions.
- Use small, explicit APIs. Expose only what callers need, and keep representation details private unless there is a strong reason not to.
- Prefer `use` imports over inline fully-qualified paths for functions, types, and macros. Only fully-qualify a path inline when needed to resolve a genuine name ambiguity (or for a one-off reference where a `use` would mislead).
- Use Rust's conventional module discovery (`foo.rs` or `foo/mod.rs`) and organize files accordingly. Do not use `#[path = "..."]` module overrides.

## OpenAPI

- `docs/openapi.json` is committed and should stay in sync with code.
- Regenerate it with `cargo run --quiet --bin hubuum-openapi > docs/openapi.json` after API schema changes.
- The pre-commit hook also checks clippy and regenerates OpenAPI; enable it with `git config core.hooksPath hooks`.

## Database And Migrations

- Schema changes should go through Diesel migrations in
  `crates/hubuum-storage-postgres/migrations/`.
- Keep `crates/hubuum-storage-postgres/src/schema.rs` generated by Diesel
  rather than hand-editing it.
- Preserve database invariants in migrations and mirror them in Rust validation where practical, especially at API boundaries.

## Tests

- Prefer the shared test utilities in `src/tests/*` for API requests, fixtures, scoped names, and assertions.
- Use `TestScope` or `TestContext` for database-backed tests so test data is isolated and names do not collide under parallel execution.
- Clean up fixtures where tests create persistent domain objects outside existing fixture helpers.
- Keep each test focused on a single behavior; avoid asserting several unrelated outcomes in one test body. When a behavior varies by input, drive the variants with `#[rstest]` `#[case(...)]` parameterization rather than stacking multiple assertions in one test. (A small amount of arrange/precondition checking in service of the one behavior under test is fine.)
- Do not add dead code (unused fields, functions, imports, or `#[allow(dead_code)]`) to make a test or build pass; remove what is unused instead.

## Benchmarks

- Put benchmark entrypoints in `benches/` and add matching `[[bench]]` entries in `Cargo.toml` with `harness = false`.
- Keep benchmark targets one per file so CI can fan them out independently.
- Prefer deterministic library-level benchmarks over handlers or database-backed flows.
- Avoid reading global `CONFIG` from benchmarks; provide config-free helper functions that accept limits or options explicitly.

## Pull Requests And Merges

- Treat the changelog review as required for every pull request. Before merge, add relevant user-facing additions, changes, fixes, and security notes to the `[Unreleased]` section of `CHANGELOG.md`. If a pull request has no changelog-worthy impact, state that explicitly in its description; do not add empty or internal-only changelog entries.
- Call out every breaking change explicitly in both the pull request description and its `[Unreleased]` changelog entry, including the upgrade or migration action users must take.
- When squash-merging a pull request, use its detailed PR description as the squash commit body. Preserve the substantive summary, rationale, behavior notes, and issue references, but remove verification-only sections such as test commands, checklists, and `## Verification` before merging.

### Stacked Pull Requests

- Use GitHub stacked pull requests when two or more changes in this repository form a strict linear dependency and each layer remains a focused, independently reviewable change. Keep foundational schema, types, and shared interfaces lower in the stack, with dependent behavior above them.
- Keep unrelated, merely sequential, fork-based, or branching work as standalone pull requests targeting `main`. Do not create a stack just to group a campaign of independent changes or reduce the CI queue.
- Treat stacks as a public-preview GitHub feature. Use the official `github/gh-stack` extension (`gh extension install github/gh-stack`) and check the current GitHub documentation before scripting stack operations.
- For a new stack, use `gh stack init`, add layers with `gh stack add`, publish with `gh stack submit`, and maintain it with `gh stack view`, `gh stack sync`, and `gh stack rebase`. To adopt an existing correctly ordered chain, use `gh stack link <bottom-pr> <next-pr> ... <top-pr>`.
- Expect required reviews and pull-request CI to run for every layer. Only optimize duplicate expensive jobs with `github.event.pull_request.stack` after confirming that every required check still resolves correctly.
- Merge from the bottom upward, or use `gh stack merge` or the stack-aware asynchronous merge API for partial or whole-stack merges. Do not use the legacy pull-request merge API for a stack. Preserve the repository's required squash commit body; when automation needs exact text, pass an explicit commit title and message through the stack-aware asynchronous merge API.

## Change Discipline

- Keep edits scoped to the task at hand.
- Add or update tests when behavior changes or when a bug fix would otherwise be easy to regress.
- Prefer clear, idiomatic code over cleverness.
