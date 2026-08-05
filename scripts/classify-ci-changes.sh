#!/usr/bin/env bash
set -euo pipefail

any=false
markdown=false
code=false
openapi=false
container=false
artifacts=false
benchmarks=false
postgres_benchmark=false
runtime_benchmark=false

for path in "$@"; do
  any=true

  if [[ "$path" == *.md ]]; then
    markdown=true
  fi

  case "$path" in
    docs/openapi.json)
      openapi=true
      ;;
    docs/export_template_guide.md)
      code=true
      container=true
      artifacts=true
      ;;
    docs/querying.md)
      code=true
      ;;
    .markdownlint.json)
      markdown=true
      ;;
    *.md | docs/* | LICENSE | .gitattributes | .gitignore | \
      .env.example | .env.*.example | .agents/* | .codex/* | \
      .github/ISSUE_TEMPLATE/* | .github/PULL_REQUEST_TEMPLATE*)
      ;;
    .github/workflows/benchmarks.yml)
      code=true
      benchmarks=true
      postgres_benchmark=true
      runtime_benchmark=true
      ;;
    .github/workflows/ci.yml)
      code=true
      openapi=true
      container=true
      artifacts=true
      ;;
    src/tests/* | tests/*)
      code=true
      ;;
    src/*)
      code=true
      container=true
      artifacts=true
      benchmarks=true
      if [[ "$path" != src/tests/* ]]; then
        postgres_benchmark=true
        runtime_benchmark=true
      fi
      ;;
    crates/*/benches/*)
      code=true
      benchmarks=true
      ;;
    crates/*)
      code=true
      container=true
      artifacts=true
      benchmarks=true
      postgres_benchmark=true
      ;;
    benches/postgres/*)
      code=true
      benchmarks=true
      postgres_benchmark=true
      ;;
    benches/runtime_behavior.rs)
      code=true
      benchmarks=true
      runtime_benchmark=true
      ;;
    benches/*)
      code=true
      benchmarks=true
      ;;
    migrations/*)
      code=true
      container=true
      artifacts=true
      postgres_benchmark=true
      runtime_benchmark=true
      ;;
    Cargo.toml | Cargo.lock)
      code=true
      openapi=true
      container=true
      artifacts=true
      benchmarks=true
      postgres_benchmark=true
      runtime_benchmark=true
      ;;
    Cross.toml | diesel.toml | build.rs)
      code=true
      container=true
      artifacts=true
      ;;
    Dockerfile | entrypoint.sh | .dockerignore)
      code=true
      container=true
      artifacts=true
      ;;
    docker/* | docker-compose.yml | .env | .env.docker.local)
      code=true
      container=true
      ;;
    scripts/classify-ci-changes.sh | scripts/test-classify-ci-changes.sh)
      code=true
      benchmarks=true
      postgres_benchmark=true
      runtime_benchmark=true
      ;;
    scripts/check-criterion-regressions.sh | scripts/check-criterion-stability.sh)
      code=true
      benchmarks=true
      postgres_benchmark=true
      ;;
    scripts/install-single-host.sh | scripts/single-host-rollout.sh | \
      scripts/check-migration-compatibility.sh | scripts/resolve-adjacent-release.sh | \
      scripts/test-adjacent-release-upgrade.sh | scripts/test-migration-compatibility.sh | \
      scripts/test-install-script-refresh.sh | scripts/test-single-host-rollout.sh | \
      scripts/test-single-host-zero-downtime.sh | scripts/update-single-host.sh | \
      scripts/uninstall-single-host.sh | scripts/stop-single-host.sh)
      code=true
      container=true
      ;;
    scripts/* | run_tests.sh | cleanup_test_databases.sh)
      code=true
      ;;
    *)
      # Unknown inputs are treated conservatively so new build inputs do not
      # silently bypass validation or main artifact publication.
      code=true
      container=true
      artifacts=true
      benchmarks=true
      postgres_benchmark=true
      runtime_benchmark=true
      ;;
  esac
done

outputs=(
  "any=$any"
  "markdown=$markdown"
  "code=$code"
  "openapi=$openapi"
  "container=$container"
  "artifacts=$artifacts"
  "benchmarks=$benchmarks"
  "postgres_benchmark=$postgres_benchmark"
  "runtime_benchmark=$runtime_benchmark"
)

printf '%s\n' "${outputs[@]}"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  printf '%s\n' "${outputs[@]}" >> "$GITHUB_OUTPUT"
fi
