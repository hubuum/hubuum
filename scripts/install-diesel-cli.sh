#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$repo_root/.github/supply-chain-tools.env"

cargo_bin="${CARGO_HOME:-$HOME/.cargo}/bin"
diesel_bin="$cargo_bin/diesel"

installed_version=""
if [[ -x "$diesel_bin" ]]; then
  installed_version="$($diesel_bin --version 2>/dev/null || true)"
fi

if [[ "$installed_version" != "diesel ${DIESEL_CLI_VERSION}" ]]; then
  cargo install \
    --locked \
    --force \
    --version "=${DIESEL_CLI_VERSION}" \
    --no-default-features \
    --features postgres \
    diesel_cli
fi

if [[ ! -x "$diesel_bin" ]]; then
  echo "diesel binary not found at $diesel_bin" >&2
  exit 1
fi

version="$($diesel_bin --version)"
if [[ "$version" != "diesel ${DIESEL_CLI_VERSION}" ]]; then
  echo "unexpected Diesel CLI version after install: $version" >&2
  exit 1
fi
if command -v sha256sum >/dev/null 2>&1; then
  digest="$(sha256sum "$diesel_bin" | awk '{print $1}')"
else
  digest="$(shasum -a 256 "$diesel_bin" | awk '{print $1}')"
fi

echo "$version"
echo "diesel sha256:$digest"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "- Diesel CLI: \`$version\`"
    echo "- Diesel CLI executable: \`sha256:$digest\`"
  } >> "$GITHUB_STEP_SUMMARY"
fi
