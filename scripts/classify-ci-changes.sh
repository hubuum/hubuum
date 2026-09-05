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
operational_contract=false
container=false
artifacts=false
benchmarks=false
runtime_benchmark=false
scale_benchmark=false
treetop_conformance=false

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
    .github/treetop-conformance.env | .github/workflows/ci.yml | \
      Cargo.toml | Cargo.lock | docs/treetop/* | \
      scripts/run-treetop-conformance.sh | scripts/serve-treetop-fixture.py | \
      scripts/test-serve-treetop-fixture.py | \
      src/config.rs | src/config/* | src/db/traits/authz.rs | src/permissions/* | \
      src/models/permissions.rs | src/models/token.rs | \
      src/models/token_scope.rs | src/models/unified_search.rs | \
      src/api/v1/handlers/search.rs | src/tests/permissions/* | \
      tests/api_core_data_suite/object_aggregates/external_authorization.rs)
      treetop_conformance=true
      ;;
  esac

  case "$path" in
    CHANGELOG.md | docs/operational-contract.json | docs/metrics-reference.md | \
      .github/operational-contract-breaking-exceptions.json | \
      scripts/check-operational-contract-compatibility.py | \
      scripts/resolve-operational-contract-baseline.sh | \
      scripts/test-operational-contract-github-api.sh | \
      scripts/test-operational-contract-compatibility.py | \
      scripts/test-operational-contract-compatibility.sh)
      code=true
      operational_contract=true
      ;;
    docs/openapi.json)
      openapi=true
      ;;
    docs/export_template_guide.md)
      code=true
      container=true
      artifacts=true
      ;;
    docs/treetop/schema.cedarschema | docs/treetop/schema.json | \
      docs/treetop/test-fixture.cedar)
      code=true
      ;;
    docs/querying.md)
      code=true
      ;;
    docs/storage_boundary.md | docs/storage_boundary/*)
      # These files are dynamic inputs to the storage architecture and
      # semantic-documentation tests in src/tests/application_boundary.rs.
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
      runtime_benchmark=true
      ;;
    .github/workflows/scale-benchmarks.yml | scale-benchmarks/* | \
      crates/hubuum-scale-benchmark/* | crates/hubuum-scale-core/* | \
      crates/hubuum-storage-postgres/src/scale_benchmark.rs)
      code=true
      benchmarks=true
      scale_benchmark=true
      ;;
    .github/workflows/restore-drill.yml)
      code=true
      container=true
      ;;
    .github/workflows/ci.yml)
      code=true
      openapi=true
      operational_contract=true
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
      operational_contract=true
      container=true
      artifacts=true
      benchmarks=true
      if [[ "$path" != src/tests/* ]]; then
        runtime_benchmark=true
      fi
      ;;
    crates/*/benches/*)
      code=true
      benchmarks=true
      ;;
    crates/hubuum-storage-postgres/migrations/*)
      code=true
      container=true
      artifacts=true
      benchmarks=true
      runtime_benchmark=true
      ;;
    crates/*)
      code=true
      operational_contract=true
      container=true
      artifacts=true
      benchmarks=true
      ;;
    benches/*)
      code=true
      benchmarks=true
      ;;
    Cargo.toml | Cargo.lock)
      code=true
      openapi=true
      operational_contract=true
      container=true
      artifacts=true
      benchmarks=true
      runtime_benchmark=true
      scale_benchmark=true
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
      runtime_benchmark=true
      scale_benchmark=true
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
  "operational_contract=$operational_contract"
  "container=$container"
  "artifacts=$artifacts"
  "benchmarks=$benchmarks"
  "runtime_benchmark=$runtime_benchmark"
  "scale_benchmark=$scale_benchmark"
  "treetop_conformance=$treetop_conformance"
)

printf '%s\n' "${outputs[@]}"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  printf '%s\n' "${outputs[@]}" >> "$GITHUB_OUTPUT"
fi
