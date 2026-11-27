#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
CARGO_DENY_VERSION="$(
  python3 "$repo_root/scripts/check-supply-chain-policy.py" \
    --tool-value CARGO_DENY_VERSION
)"
report_path="${1:-cargo-deny.log}"
manifest_path="${2:-$repo_root/Cargo.toml}"

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

# Root-only exceptions need not match the separate fuzz workspace. The root
# audit still rejects unused exceptions; matching advisory errors stay fatal.
unused_exception_lint="$(
  python3 - "$manifest_path" "$repo_root/fuzz/Cargo.toml" <<'PY'
from pathlib import Path
import sys

print("--warn" if Path(sys.argv[1]).resolve() == Path(sys.argv[2]).resolve() else "--deny")
PY
)"

set +e
cargo deny --manifest-path "$manifest_path" --config "$repo_root/deny.toml" \
  --all-features --locked check "$unused_exception_lint" advisory-not-detected 2>&1 | tee "$report_path"
status="${PIPESTATUS[0]}"
set -e
exit "$status"
