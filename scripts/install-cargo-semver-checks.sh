#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_version="$(
  python3 "$repo_root/scripts/check-supply-chain-policy.py" \
    --tool-value CARGO_SEMVER_CHECKS_VERSION
)"
cargo_bin="${CARGO_HOME:-$HOME/.cargo}/bin"
semver_checks_bin="$cargo_bin/cargo-semver-checks"
expected_version="cargo-semver-checks $target_version"
installed_version=""

if [[ -x "$semver_checks_bin" ]]; then
  installed_version="$($semver_checks_bin --version 2>/dev/null || true)"
fi
if [[ "$installed_version" != "$expected_version" ]]; then
  cargo install \
    --locked \
    --force \
    --version "=$target_version" \
    cargo-semver-checks
fi

if [[ ! -x "$semver_checks_bin" ]]; then
  echo "cargo-semver-checks binary not found at $semver_checks_bin" >&2
  exit 1
fi

installed_version="$($semver_checks_bin --version)"
if [[ "$installed_version" != "$expected_version" ]]; then
  printf 'unexpected cargo-semver-checks version after install: %s (expected %s)\n' \
    "$installed_version" "$expected_version" >&2
  exit 1
fi
if command -v sha256sum >/dev/null 2>&1; then
  digest="$(sha256sum "$semver_checks_bin" | awk '{print $1}')"
else
  digest="$(shasum -a 256 "$semver_checks_bin" | awk '{print $1}')"
fi

echo "$installed_version"
echo "cargo-semver-checks sha256:$digest"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "- cargo-semver-checks: \`$installed_version\`"
    echo "- cargo-semver-checks executable: \`sha256:$digest\`"
  } >> "$GITHUB_STEP_SUMMARY"
fi
