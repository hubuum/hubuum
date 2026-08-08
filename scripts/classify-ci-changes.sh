#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
policy_root="${RUST_API_POLICY_ROOT:-$repo_root}"
declared_policy_documents="$(
  python3 "$repo_root/scripts/check-rust-api-policy.py" \
    --root "$policy_root" \
    --declared-policy-documents
)"

any=false
markdown=false
code=false
rust_api_policy=false
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

  while IFS= read -r policy_document; do
    if [[ -n "$policy_document" && "$path" == "$policy_document" ]]; then
      rust_api_policy=true
      break
    fi
  done <<< "$declared_policy_documents"

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
    SECURITY.md | docs/supply-chain-security.md)
      code=true
      container=true
      artifacts=true
      ;;
    .github/workflows/supply-chain.yml | .github/supply-chain-* | \
      deny.toml | .trivyignore | scripts/check-supply-chain-policy.py | \
      scripts/generate-container-evidence.sh | scripts/generate-release-sbom.py | \
      scripts/install-cargo-semver-checks.sh | scripts/run-cargo-deny.sh | \
      scripts/test-generate-release-sbom.sh | \
      scripts/test-generate-container-evidence.sh | scripts/test-supply-chain-policy.py)
      code=true
      container=true
      artifacts=true
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
    .github/oasdiff-severity-levels.txt | \
      .github/openapi-breaking-exceptions.json | \
      scripts/check-openapi-compatibility.sh | \
      scripts/install-oasdiff.sh | \
      scripts/resolve-openapi-baseline.sh | \
      scripts/test-openapi-compatibility.sh)
      code=true
      openapi=true
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
  "rust_api_policy=$rust_api_policy"
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
