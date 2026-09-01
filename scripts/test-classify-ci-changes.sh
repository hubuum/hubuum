#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
classifier="$repo_root/scripts/classify-ci-changes.sh"

assert_flag() {
  local output="$1"
  local flag="$2"
  local expected="$3"

  if ! grep --fixed-strings --line-regexp --quiet "$flag=$expected" <<< "$output"; then
    echo "Expected $flag=$expected, got:" >&2
    echo "$output" >&2
    exit 1
  fi
}

assert_literal_include_is_code() {
  local source_path="$1"
  local invocation="$2"
  local relative_path="${invocation#*\"}"
  relative_path="${relative_path%%\"*}"

  local source_dir
  source_dir="$repo_root/$(dirname "$source_path")"
  local include_dir
  include_dir="$(cd "$source_dir/$(dirname "$relative_path")" && pwd -P)"
  local include_path
  include_path="$include_dir/$(basename "$relative_path")"

  case "$include_path" in
    "$repo_root"/*)
      include_path="${include_path#"$repo_root"/}"
      ;;
    *)
      return
      ;;
  esac

  local output
  output="$(bash "$classifier" "$include_path")"
  assert_flag "$output" code true
}

docs_output="$(bash "$classifier" README.md AGENTS.md docs/development.md)"
assert_flag "$docs_output" markdown true
assert_flag "$docs_output" code false
assert_flag "$docs_output" rust_api_policy false
assert_flag "$docs_output" artifacts false
assert_flag "$docs_output" treetop_conformance false

policy_fixture_root="$(mktemp -d)"
trap 'rm -rf "$policy_fixture_root"' EXIT
mkdir -p "$policy_fixture_root/src"
cat > "$policy_fixture_root/Cargo.toml" <<'EOF'
[package]
name = "policy-fixture"
version = "0.0.1"
edition = "2024"
publish = true

[package.metadata.hubuum]
rust-api = "experimental-public"
policy-document = "docs/public-api.md"

[workspace]
EOF
: > "$policy_fixture_root/src/lib.rs"

deleted_policy_document_output="$(
  RUST_API_POLICY_ROOT="$policy_fixture_root" \
    bash "$classifier" docs/public-api.md
)"
assert_flag "$deleted_policy_document_output" markdown true
assert_flag "$deleted_policy_document_output" code false
assert_flag "$deleted_policy_document_output" rust_api_policy true

moved_policy_document_output="$(
  RUST_API_POLICY_ROOT="$policy_fixture_root" \
    bash "$classifier" docs/public-api.md docs/moved-public-api.md
)"
assert_flag "$moved_policy_document_output" markdown true
assert_flag "$moved_policy_document_output" code false
assert_flag "$moved_policy_document_output" rust_api_policy true

python3 - "$repo_root/.github/workflows/ci.yml" <<'PY'
import sys
from pathlib import Path

workflow = Path(sys.argv[1]).read_text(encoding="utf-8")
start = workflow.index("\n  rust-api-policy:\n")
end = workflow.index("\n  lint:\n", start)
job = workflow[start:end]
condition = "needs.changes.outputs.rust_api_policy == 'true'"
if job.count(condition) != 1:
    raise SystemExit("Rust API policy job must consume rust_api_policy exactly once")
PY

querying_docs_output="$(bash "$classifier" docs/querying.md)"
assert_flag "$querying_docs_output" markdown true
assert_flag "$querying_docs_output" code true
assert_flag "$querying_docs_output" artifacts false

storage_contract_docs_output="$(bash "$classifier" \
  docs/storage_boundary.md \
  docs/storage_boundary/contract.md \
  docs/storage_boundary/semantic-coverage.toml)"
assert_flag "$storage_contract_docs_output" markdown true
assert_flag "$storage_contract_docs_output" code true
assert_flag "$storage_contract_docs_output" container false
assert_flag "$storage_contract_docs_output" artifacts false
assert_flag "$storage_contract_docs_output" benchmarks false

markdown_config_output="$(bash "$classifier" .markdownlint.json)"
assert_flag "$markdown_config_output" markdown true
assert_flag "$markdown_config_output" code false

supply_chain_output="$(bash "$classifier" \
  deny.toml \
  .trivyignore \
  .github/supply-chain-tools.env \
  scripts/generate-container-evidence.sh \
  scripts/install-cargo-semver-checks.sh \
  scripts/test-generate-container-evidence.sh \
  scripts/test-supply-chain-policy.py)"
assert_flag "$supply_chain_output" code true
assert_flag "$supply_chain_output" container true
assert_flag "$supply_chain_output" artifacts true
assert_flag "$supply_chain_output" benchmarks false

supply_chain_docs_output="$(bash "$classifier" SECURITY.md docs/supply-chain-security.md)"
assert_flag "$supply_chain_docs_output" markdown true
assert_flag "$supply_chain_docs_output" code true
assert_flag "$supply_chain_docs_output" container true
assert_flag "$supply_chain_docs_output" artifacts true

rust_api_policy_output="$(bash "$classifier" \
  scripts/check-rust-api-policy.py \
  scripts/test-rust-api-policy.py \
  scripts/check-crates-io-baseline.py \
  scripts/test-crates-io-baseline.py)"
assert_flag "$rust_api_policy_output" code true
assert_flag "$rust_api_policy_output" container false
assert_flag "$rust_api_policy_output" artifacts false
assert_flag "$rust_api_policy_output" benchmarks false

openapi_output="$(bash "$classifier" docs/openapi.json)"
assert_flag "$openapi_output" openapi true
assert_flag "$openapi_output" code false

openapi_policy_output="$(bash "$classifier" \
  .github/openapi-breaking-exceptions.json \
  .github/oasdiff-severity-levels.txt \
  scripts/check-openapi-compatibility.sh \
  scripts/install-oasdiff.sh \
  scripts/resolve-openapi-baseline.sh \
  scripts/test-openapi-compatibility.sh)"
assert_flag "$openapi_policy_output" openapi true
assert_flag "$openapi_policy_output" code true
assert_flag "$openapi_policy_output" container false
assert_flag "$openapi_policy_output" artifacts false

embedded_doc_output="$(bash "$classifier" docs/export_template_guide.md)"
assert_flag "$embedded_doc_output" markdown true
assert_flag "$embedded_doc_output" code true
assert_flag "$embedded_doc_output" container true
assert_flag "$embedded_doc_output" artifacts true

test_output="$(bash "$classifier" tests/api_core_data_suite/querying.rs)"
assert_flag "$test_output" code true
assert_flag "$test_output" container false
assert_flag "$test_output" artifacts false
assert_flag "$test_output" benchmarks false

source_output="$(bash "$classifier" src/api/v1/mod.rs)"
assert_flag "$source_output" code true
assert_flag "$source_output" container true
assert_flag "$source_output" artifacts true
assert_flag "$source_output" benchmarks true
assert_flag "$source_output" runtime_benchmark true
assert_flag "$source_output" treetop_conformance false

treetop_output="$(bash "$classifier" \
  .github/treetop-conformance.env \
  docs/treetop/schema.json \
  src/config/environment.rs \
  src/permissions/treetop/mod.rs \
  src/models/token_scope.rs \
  scripts/run-treetop-conformance.sh \
  scripts/serve-treetop-fixture.py \
  scripts/test-serve-treetop-fixture.py)"
assert_flag "$treetop_output" code true
assert_flag "$treetop_output" markdown false
assert_flag "$treetop_output" treetop_conformance true

treetop_fixture_output="$(bash "$classifier" \
  docs/treetop/schema.cedarschema \
  docs/treetop/schema.json \
  docs/treetop/test-fixture.cedar)"
assert_flag "$treetop_fixture_output" code true
assert_flag "$treetop_fixture_output" treetop_conformance true

treetop_docs_output="$(bash "$classifier" docs/treetop/README.md)"
assert_flag "$treetop_docs_output" code false
assert_flag "$treetop_docs_output" markdown true
assert_flag "$treetop_docs_output" treetop_conformance true

migration_output="$(bash "$classifier" \
  crates/hubuum-storage-postgres/migrations/2026-08-03-000001_resource_revisions/up.sql)"
assert_flag "$migration_output" code true
assert_flag "$migration_output" container true
assert_flag "$migration_output" artifacts true
assert_flag "$migration_output" benchmarks true
assert_flag "$migration_output" runtime_benchmark true

benchmark_output="$(bash "$classifier" .github/workflows/benchmarks.yml)"
assert_flag "$benchmark_output" benchmarks true
assert_flag "$benchmark_output" runtime_benchmark true
assert_flag "$benchmark_output" artifacts false
assert_flag "$benchmark_output" scale_benchmark false

scale_benchmark_output="$(bash "$classifier" \
  .github/workflows/scale-benchmarks.yml \
  scale-benchmarks/profiles/large.toml \
  scale-benchmarks/profiles/huge.toml \
  scale-benchmarks/workloads/v1.toml \
  src/bin/scale_benchmark.rs \
  src/observability/scale_benchmark/loader.rs)"
assert_flag "$scale_benchmark_output" code true
assert_flag "$scale_benchmark_output" benchmarks true
assert_flag "$scale_benchmark_output" runtime_benchmark false
assert_flag "$scale_benchmark_output" scale_benchmark true
assert_flag "$scale_benchmark_output" container false
assert_flag "$scale_benchmark_output" artifacts false

python3 - "$repo_root/.github/workflows/scale-benchmarks.yml" <<'PY'
import sys
from pathlib import Path

workflow = Path(sys.argv[1]).read_text(encoding="utf-8")
required = [
    "ci:scale-large",
    "ci:scale-huge",
    "workflow_dispatch:",
    "schedule:",
    "17 3 * * 2",
    "43 3 1 * *",
    "base.json",
    "head.json",
    "Scale operational benchmark:",
    "cancel-in-progress: true",
]
missing = [token for token in required if token not in workflow]
if missing:
    raise SystemExit(f"scale workflow is missing required controls: {missing}")
for generic_label in ("ci:full", "ci:benchmarks"):
    if generic_label in workflow:
        raise SystemExit(f"{generic_label} must not enable the scale workflow")
PY

storage_benchmark_output="$(
  bash "$classifier" benches/storage_postgres_criterion.rs
)"
assert_flag "$storage_benchmark_output" code true
assert_flag "$storage_benchmark_output" benchmarks true
assert_flag "$storage_benchmark_output" runtime_benchmark false
assert_flag "$storage_benchmark_output" artifacts false

runtime_check_output="$(
  bash "$classifier" src/bin/runtime_behavior_check.rs
)"
assert_flag "$runtime_check_output" code true
assert_flag "$runtime_check_output" benchmarks true
assert_flag "$runtime_check_output" runtime_benchmark true
assert_flag "$runtime_check_output" scale_benchmark false
assert_flag "$runtime_check_output" artifacts true

classifier_output="$(bash "$classifier" scripts/classify-ci-changes.sh)"
assert_flag "$classifier_output" code true
assert_flag "$classifier_output" benchmarks true
assert_flag "$classifier_output" runtime_benchmark true
assert_flag "$classifier_output" scale_benchmark true
assert_flag "$classifier_output" artifacts false

docker_output="$(bash "$classifier" Dockerfile)"
assert_flag "$docker_output" code true
assert_flag "$docker_output" container true
assert_flag "$docker_output" artifacts true

compatibility_output="$(bash "$classifier" scripts/test-adjacent-release-upgrade.sh)"
assert_flag "$compatibility_output" code true
assert_flag "$compatibility_output" container true
assert_flag "$compatibility_output" artifacts false

restore_drill_output="$(bash "$classifier" .github/workflows/restore-drill.yml)"
assert_flag "$restore_drill_output" code true
assert_flag "$restore_drill_output" container true
assert_flag "$restore_drill_output" artifacts false
assert_flag "$restore_drill_output" benchmarks false

unknown_output="$(bash "$classifier" future-build-input.conf)"
assert_flag "$unknown_output" code true
assert_flag "$unknown_output" container true
assert_flag "$unknown_output" artifacts true
assert_flag "$unknown_output" benchmarks true

literal_include_count=0
while IFS=: read -r source_path invocation; do
  assert_literal_include_is_code "$source_path" "$invocation"
  ((literal_include_count += 1))
done < <(
  cd "$repo_root"
  git grep --only-matching --extended-regexp \
    'include_(str|bytes)![[:space:]]*\([[:space:]]*"[^"]+"[[:space:]]*\)' \
    -- '*.rs'
)

if ((literal_include_count == 0)); then
  echo "Expected to find at least one direct include_str! or include_bytes! input." >&2
  exit 1
fi

echo "CI change classifier tests passed."
