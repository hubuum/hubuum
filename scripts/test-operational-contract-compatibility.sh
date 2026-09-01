#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT

python3 "$repo_root/scripts/test-operational-contract-compatibility.py"

cargo run --quiet --features embedded-migrations \
  --bin hubuum-operational-contracts -- json > "$temporary_dir/baseline.json"
HUBUUM_OPERATIONAL_CONTRACT_BASELINE_FILE="$temporary_dir/baseline.json" \
HUBUUM_OPERATIONAL_CONTRACT_BASELINE_TAG="v$(jq --raw-output '.release' "$temporary_dir/baseline.json")" \
  "$repo_root/scripts/resolve-operational-contract-baseline.sh" "$temporary_dir/report"
jq --exit-status '.status == "available" and (.sha256 | test("^[0-9a-f]{64}$"))' \
  "$temporary_dir/report/baseline-metadata.json" >/dev/null

if HUBUUM_OPERATIONAL_CONTRACT_BASELINE_FILE="$temporary_dir/baseline.json" \
  HUBUUM_OPERATIONAL_CONTRACT_BASELINE_TAG="v9.9.9" \
  "$repo_root/scripts/resolve-operational-contract-baseline.sh" "$temporary_dir/wrong"; then
  echo "Expected the resolver to reject a baseline whose release does not match its tag" >&2
  exit 1
fi
