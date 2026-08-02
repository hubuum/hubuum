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

docs_output="$(bash "$classifier" README.md AGENTS.md docs/development.md)"
assert_flag "$docs_output" markdown true
assert_flag "$docs_output" code false
assert_flag "$docs_output" artifacts false

openapi_output="$(bash "$classifier" docs/openapi.json)"
assert_flag "$openapi_output" openapi true
assert_flag "$openapi_output" code false

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
assert_flag "$source_output" postgres_benchmark true
assert_flag "$source_output" runtime_benchmark true

benchmark_output="$(bash "$classifier" .github/workflows/benchmarks.yml)"
assert_flag "$benchmark_output" benchmarks true
assert_flag "$benchmark_output" postgres_benchmark true
assert_flag "$benchmark_output" runtime_benchmark true
assert_flag "$benchmark_output" artifacts false

classifier_output="$(bash "$classifier" scripts/classify-ci-changes.sh)"
assert_flag "$classifier_output" code true
assert_flag "$classifier_output" benchmarks true
assert_flag "$classifier_output" postgres_benchmark true
assert_flag "$classifier_output" runtime_benchmark true
assert_flag "$classifier_output" artifacts false

docker_output="$(bash "$classifier" Dockerfile)"
assert_flag "$docker_output" code true
assert_flag "$docker_output" container true
assert_flag "$docker_output" artifacts true

unknown_output="$(bash "$classifier" future-build-input.conf)"
assert_flag "$unknown_output" code true
assert_flag "$unknown_output" container true
assert_flag "$unknown_output" artifacts true
assert_flag "$unknown_output" benchmarks true

echo "CI change classifier tests passed."
