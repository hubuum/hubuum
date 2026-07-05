# Repository Guidelines

## Verification

- Run the test suite with `source .env && ./run_tests.sh`.
- The test runner runs tests in parallel. If you need to run more than a few targeted tests, run the full suite instead of trying to manually select a large subset.
- `cargo clippy` should pass for all code before changes are considered complete.
- Run clippy as `cargo clippy --all-targets -- -D warnings`.
- `rustfmt` should pass for all Rust code. Keep formatting mechanical and avoid hand-formatting that fights `rustfmt`.
- When workspace crate membership, crate manifests, `Cargo.lock`, `Dockerfile`, or `.dockerignore` changes, verify the production container build with `scripts/check-container-build.sh` before shipping a PR. Use `scripts/check-container-build.sh --variant full` when changes affect TLS/OpenSSL feature composition.
- Regenerate OpenAPI after endpoint or schema changes before considering the change complete.
- Markdown lint must pass for all `*.md` files. Run it locally with `npx markdownlint-cli2 --config .markdownlint.json "**/*.md" "!target"` before considering documentation changes complete. Every fenced code block must declare a language (use `text` for plain ASCII/diagrams), and tables must use a single, consistent column style (MD060).

## Container Builds

- The Docker dependency-cache stage copies workspace manifests explicitly. Whenever `[workspace].members`, a workspace crate manifest, `Cargo.lock`, Docker build features, `Dockerfile`, or `entrypoint.sh` changes, treat the production container as an affected build target.
- When adding or removing a workspace member, update the manifest-only `COPY` entries in `Dockerfile` in the same change. A normal host `cargo build` is not a substitute because it can see files that are absent from Docker's dependency-cache stage.
- Run the fast parity regression test first: `cargo test --bin hubuum-server dockerfile_copies_every_workspace_manifest --locked`. It requires the Dockerfile's workspace-manifest `COPY` set to exactly match Cargo's workspace member set.
- Then build the real production image with the feature combination used for the full published container: `docker build --build-arg 'CARGO_BUILD_FLAGS=-F tls-rustls -F tls-openssl --locked --release' --tag hubuum-server:verify .`.
- A container-affecting change is not complete until both the parity test and the real Docker build pass. Keep the pull-request container-build CI check enabled so the restricted Docker build context is exercised before merge.

## Architecture

- Keep public domain behavior in `src/models/*` and `src/traits/*`.
- Keep Diesel/Postgres query construction and backend details in `src/db/traits/*`.
- Model methods should stay thin and delegate persistence-heavy work to backend traits.
- Use `BackendContext` for APIs that should accept either `DbPool` or wrappers such as `web::Data<DbPool>`.
- Put multi-step database writes in `with_transaction`; use `with_connection` for single reads, single writes, and non-atomic database work.
- Workspace crates should expose small, explicit interfaces with private fields. Prefer typed request/builder APIs over long positional argument lists when callers must provide several settings.
- Keep workspace crate boundaries clean of app-specific errors, global config, Actix, Diesel, and task persistence unless the crate explicitly owns that layer.
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

## OpenAPI

- `docs/openapi.json` is committed and should stay in sync with code.
- Regenerate it with `cargo run --quiet --bin hubuum-openapi > docs/openapi.json` after API schema changes.
- The pre-commit hook also checks clippy and regenerates OpenAPI; enable it with `git config core.hooksPath hooks`.

## Database And Migrations

- Schema changes should go through Diesel migrations in `migrations/`.
- Keep `src/schema.rs` generated by Diesel rather than hand-editing it.
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

- When squash-merging a pull request, use its detailed PR description as the squash commit body. Preserve the substantive summary, rationale, behavior notes, and issue references, but remove verification-only sections such as test commands, checklists, and `## Verification` before merging.

## Change Discipline

- Keep edits scoped to the task at hand.
- Add or update tests when behavior changes or when a bug fix would otherwise be easy to regress.
- Prefer clear, idiomatic code over cleverness.
