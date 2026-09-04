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

mock_bin="$temporary_dir/mock-bin"
mkdir -p "$mock_bin"
ln -s "$repo_root/scripts/test-operational-contract-github-api.sh" "$mock_bin/curl"

write_release_fixture() {
  local tag="$1"
  jq --null-input --arg tag "$tag" \
    '[{tag_name: $tag, draft: false, prerelease: false}]' \
    > "$temporary_dir/releases.json"
}

write_release_fixture "v0.0.11"
PATH="$mock_bin:$PATH" \
HUBUUM_TEST_RELEASES_FILE="$temporary_dir/releases.json" \
HUBUUM_TEST_CONTENTS_STATUS="404" \
GITHUB_API_URL="https://github.invalid" \
GITHUB_REPOSITORY="hubuum/hubuum" \
GITHUB_REF="" \
GITHUB_REF_NAME="" \
  "$repo_root/scripts/resolve-operational-contract-baseline.sh" \
  "$temporary_dir/bootstrap-missing"
jq --exit-status \
  '.status == "unavailable" and .tag == "v0.0.11" and (.reason | contains("bootstrap boundary"))' \
  "$temporary_dir/bootstrap-missing/baseline-metadata.json" >/dev/null

write_release_fixture "v0.0.12"
if PATH="$mock_bin:$PATH" \
  HUBUUM_TEST_RELEASES_FILE="$temporary_dir/releases.json" \
  HUBUUM_TEST_CONTENTS_STATUS="404" \
  GITHUB_API_URL="https://github.invalid" \
  GITHUB_REPOSITORY="hubuum/hubuum" \
  GITHUB_REF="" \
  GITHUB_REF_NAME="" \
  "$repo_root/scripts/resolve-operational-contract-baseline.sh" \
  "$temporary_dir/post-bootstrap-missing"; then
  echo "Expected a post-bootstrap release without a snapshot to fail" >&2
  exit 1
fi
