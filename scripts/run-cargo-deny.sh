#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CARGO_DENY_VERSION="$(
  python3 "$repo_root/scripts/check-supply-chain-policy.py" \
    --tool-value CARGO_DENY_VERSION
)"
report_path="${1:-cargo-deny.log}"

installed_version="$(cargo deny --version 2>/dev/null || true)"
if [[ "$installed_version" != "cargo-deny ${CARGO_DENY_VERSION}" ]]; then
  cargo install --locked --version "=${CARGO_DENY_VERSION}" cargo-deny
fi

cargo_deny_bin="$(command -v cargo-deny)"
version="$(cargo deny --version)"
if [[ "$version" != "cargo-deny ${CARGO_DENY_VERSION}" ]]; then
  echo "unexpected cargo-deny version after install: $version" >&2
  exit 1
fi
if command -v sha256sum >/dev/null 2>&1; then
  digest="$(sha256sum "$cargo_deny_bin" | awk '{print $1}')"
else
  digest="$(shasum -a 256 "$cargo_deny_bin" | awk '{print $1}')"
fi

echo "$version"
echo "cargo-deny sha256:$digest"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "- cargo-deny: \`$version\`"
    echo "- cargo-deny executable: \`sha256:$digest\`"
  } >> "$GITHUB_STEP_SUMMARY"
fi

set +e
cargo deny --all-features --locked check 2>&1 | tee "$report_path"
status="${PIPESTATUS[0]}"
set -e
exit "$status"
