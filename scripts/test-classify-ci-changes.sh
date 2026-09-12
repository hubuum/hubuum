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

python_version_output="$(bash "$classifier" .python-version)"
for flag in code openapi operational_contract container artifacts benchmarks runtime_benchmark treetop_conformance; do
  assert_flag "$python_version_output" "$flag" true
done

for python_tool in scripts/check-python-version.py scripts/test-python-version.py; do
  python_tool_output="$(bash "$classifier" "$python_tool")"
  assert_flag "$python_tool_output" code true
done

for probe_path in scripts/single-host-health-probe.py scripts/test-single-host-health-probe.py \
  scripts/install-single-host.sh scripts/update-single-host.sh scripts/test-single-host-tags.py; do
  probe_output="$(bash "$classifier" "$probe_path")"
  assert_flag "$probe_output" code true
  assert_flag "$probe_output" container true
done

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
assert_flag "$openapi_output" operational_contract false
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

operational_contract_output="$(bash "$classifier" \
  CHANGELOG.md \
  docs/operational-contract.json \
  docs/metrics-reference.md \
  .github/operational-contract-breaking-exceptions.json \
  scripts/check-operational-contract-compatibility.py \
  scripts/resolve-operational-contract-baseline.sh \
  scripts/test-operational-contract-github-api.sh \
  scripts/test-operational-contract-compatibility.py \
  scripts/test-operational-contract-compatibility.sh)"
assert_flag "$operational_contract_output" operational_contract true
assert_flag "$operational_contract_output" code true
assert_flag "$operational_contract_output" container false
assert_flag "$operational_contract_output" artifacts false

changelog_output="$(bash "$classifier" CHANGELOG.md)"
assert_flag "$changelog_output" markdown true
assert_flag "$changelog_output" code true
assert_flag "$changelog_output" operational_contract true

ci_workflow_output="$(bash "$classifier" .github/workflows/ci.yml)"
assert_flag "$ci_workflow_output" code true
assert_flag "$ci_workflow_output" openapi true
assert_flag "$ci_workflow_output" operational_contract true
assert_flag "$ci_workflow_output" container true
assert_flag "$ci_workflow_output" artifacts true

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
assert_flag "$source_output" operational_contract true
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

for probe_path in src/extractors/mod.rs src/api/v1/handlers/principals.rs \
  src/api/v1/handlers/service_accounts.rs \
  tests/api_identity_suite/administrative_authorization.rs; do
  probe_output="$(bash "$classifier" "$probe_path")"
  assert_flag "$probe_output" code true
  assert_flag "$probe_output" treetop_conformance true
done

for traversal_path in src/services/authorized_traversal.rs src/services/authorization_resources.rs src/models/traits/user.rs src/tests/search/related_objects.rs; do
  traversal_output="$(bash "$classifier" "$traversal_path")"
  assert_flag "$traversal_output" code true
  assert_flag "$traversal_output" treetop_conformance true
done

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

python3 - "$repo_root/.github/workflows/benchmarks.yml" <<'PY'
import re
import sys
from pathlib import Path

workflow = Path(sys.argv[1]).read_text(encoding="utf-8")


def job(name):
    start = workflow.index(f"\n  {name}:\n")
    return re.split(r"\n  [\w-]+:\n", workflow[start + 1:], maxsplit=1)[0]


def build_inputs(name):
    block = job(name)
    implementation = re.search(r"^    uses: (\S+)", block, re.MULTILINE).group(1)
    inputs = {
        key: value.strip()
        for key, value in re.findall(
            r"^      (\w+):([\s\S]*?)(?=^      \w+:|\Z)", block, re.MULTILINE
        )
    }
    # Measurement/reporting controls can differ; all build inputs must match,
    # including future toolchain, feature, and external-input declarations.
    for key in tuple(inputs):
        if key.startswith(("criterion_", "regression_")) or key in {
            "fail_on_regression", "compile_only", "comment_mode", "cache_save"
        }:
            del inputs[key]
    return implementation, inputs


if build_inputs("benchmarks") != build_inputs("warm-cache"):
    raise SystemExit("PR benchmark and main cache-warming build inputs must match")
if "github.event_name == 'pull_request' &&" not in job("changes"):
    raise SystemExit("PR change classification must not run during cache warming")
for required in (
    "github.ref == 'refs/heads/main'",
    "github.event_name == 'push'",
    "github.event_name == 'workflow_dispatch'",
    "compile_only: true",
    "comment_mode: never",
):
    if required not in job("warm-cache"):
        raise SystemExit(f"Benchmark cache warming is missing: {required}")
PY

scale_benchmark_output="$(bash "$classifier" \
  .github/workflows/scale-benchmarks.yml \
  scale-benchmarks/profiles/large.toml \
  scale-benchmarks/profiles/huge.toml \
  scale-benchmarks/sensitivity-v1.toml \
  scale-benchmarks/workloads/v1.toml \
  crates/hubuum-scale-benchmark/src/runner.rs \
  crates/hubuum-scale-core/src/lib.rs \
  crates/hubuum-storage-postgres/src/scale_benchmark.rs \
  crates/hubuum-storage-postgres/src/scale_benchmark/history_baselines.sql)"
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
    "sensitivity-plan",
    "summarize-sensitivity",
    "--add-classes",
    "--add-object-heavy-objects",
    "--add-concentrated-object-relations",
    "--add-dense-object-relations",
    "baseline.json",
    "Scale Growth Report",
    "rust-pr-bench",
    "hubuum-scale-pr-bench",
    "pull-requests: write",
    "cancel-in-progress: true",
]
missing = [token for token in required if token not in workflow]
if missing:
    raise SystemExit(f"scale workflow is missing required controls: {missing}")
for generic_label in ("ci:full", "ci:benchmarks"):
    if generic_label in workflow:
        raise SystemExit(f"{generic_label} must not enable the scale workflow")
for regression_control in ("BASE_SHA", "target/scale-base"):
    if regression_control in workflow:
        raise SystemExit(
            f"{regression_control} belongs to code-regression CI, not scale-growth CI"
        )
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

for windows_setup_path in scripts/install-windows-postgresql.py scripts/test-install-windows-postgresql.py; do
  windows_setup_output="$(bash "$classifier" "$windows_setup_path")"
  assert_flag "$windows_setup_output" code true
  assert_flag "$windows_setup_output" benchmarks false
done

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

# Runtime hardening adds a compiled documentation input. Keep its validation.
hardening_docs="$(bash "$classifier" docs/runtime_hardening.md)"
assert_flag "$hardening_docs" code false
inventory_output="$(bash "$classifier" docs/generated/project_inventory.json)"
assert_flag "$inventory_output" code true
# The isolated worker, its tests, telemetry, and benchmark retain CI coverage.
worker_output="$(bash "$classifier" crates/hubuum-templates/src/bin/hubuum-template-worker.rs)"
assert_flag "$worker_output" container true
assert_flag "$worker_output" artifacts true
performance_output="$(bash "$classifier" benches/template_schema_concurrency/main.rs)"
assert_flag "$performance_output" code true
assert_flag "$performance_output" benchmarks true

async_worker_tests="$(bash "$classifier" crates/hubuum-templates/src/isolation/tests.rs)"
assert_flag "$async_worker_tests" code true
assert_flag "$async_worker_tests" container true
async_worker_metrics="$(bash "$classifier" src/observability/metrics/template.rs)"
assert_flag "$async_worker_metrics" code true
async_worker_telemetry="$(bash "$classifier" crates/hubuum-templates/tests/telemetry.rs)"
assert_flag "$async_worker_telemetry" code true

# Batch protocol regressions and the shared-context benchmark must stay selected.
batch_tests="$(bash "$classifier" crates/hubuum-templates/tests/batching.rs)"
assert_flag "$batch_tests" code true
assert_flag "$batch_tests" container true
batch_benchmark="$(bash "$classifier" benches/template_schema_concurrency/main.rs)"
assert_flag "$batch_benchmark" code true
assert_flag "$batch_benchmark" benchmarks true

for schema_input in scripts/check-json-schema-budget.py \
  crates/hubuum-domain/src/json_schema.rs \
  crates/hubuum-domain/src/json_schema/budget.rs \
  benches/template_schema_concurrency/main.rs; do
  schema_budget_output="$(bash "$classifier" "$schema_input")"
  assert_flag "$schema_budget_output" code true
  assert_flag "$schema_budget_output" benchmarks true
  assert_flag "$schema_budget_output" runtime_benchmark true
done
