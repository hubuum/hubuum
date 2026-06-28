# Claude Instructions

This repository already defines the primary contributor guidance in [AGENTS.md](AGENTS.md).

If you are an AI assistant or automated contributor, treat [AGENTS.md](AGENTS.md) as the source of truth for:

- verification commands
- architecture boundaries
- API conventions
- Rust standards
- OpenAPI/migration discipline
- testing and benchmark expectations

## Additional Conventions Not Explicitly Centralized In AGENTS.md

These conventions are present in the codebase and docs and should be followed along with [AGENTS.md](AGENTS.md):

1. Identity model terminology is principal-first.
   Use "principal" for shared identity concepts, with two principal kinds: `human` and `service_account`.
   Keep IAM changes aligned with [docs/auth_model.md](docs/auth_model.md).

2. Service-account safety rules are strict.
   Service accounts do not password-login, cannot self-manage credentials, and scoped tokens are fail-closed.
   Preserve these properties when editing IAM handlers, extractors, and token logic.

3. Test execution has required environment prerequisites.
   `./run_tests.sh` requires `HUBUUM_TEST_DB_PASSWORD` and creates/drops an isolated temporary database.
   Prefer `source .env && ./run_tests.sh` for full-suite validation.

4. Git hooks are part of expected workflow.
   Enable shared hooks with `git config core.hooksPath hooks` so pre-commit runs clippy and OpenAPI regeneration.
   See [docs/development.md](docs/development.md).

5. Benchmark naming/discovery conventions matter.
   Keep one benchmark target per file in `benches/`, with matching `[[bench]]` entries in `Cargo.toml`.
   Use `callgrind` or `criterion` naming patterns consistently so CI discovers the correct targets.

6. Generated artifacts should remain generated.
   Do not hand-edit `src/schema.rs`; use Diesel migrations and regeneration flow.
   Keep `docs/openapi.json` synchronized with code changes.

7. Markdown must pass markdownlint.
   CI runs `markdownlint-cli2` over `**/*.md` with `.markdownlint.json`
   (via `DavidAnson/markdownlint-cli2-action`). Run
   `npx markdownlint-cli2 --config .markdownlint.json "**/*.md" "!target"`
   locally before committing docs (the `!target` glob skips generated
   `target/doc` output). Fenced code blocks must specify a language
   (use `text` for plain ASCII/diagrams), and tables must use one
   consistent column style (MD060).

When guidance in this file and [AGENTS.md](AGENTS.md) overlap, follow [AGENTS.md](AGENTS.md).
