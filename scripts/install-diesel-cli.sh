#!/usr/bin/env bash
set -euo pipefail

parse_diesel_cli_version() {
  awk '
    ($1 == "diesel" || $1 == "Version:") && NF == 2 {
      version = $2
      candidates++
    }
    END {
      if (candidates != 1) {
        exit 1
      }
      print version
    }
  '
}

main() {
  local repo_root
  local target_version
  local cargo_bin
  local diesel_bin
  local installed_description=""
  local installed_version=""
  local version_description
  local version
  local version_label
  local digest

  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  target_version="$(
    python3 "$repo_root/scripts/check-supply-chain-policy.py" \
      --tool-value DIESEL_CLI_VERSION
  )"

  cargo_bin="${CARGO_HOME:-$HOME/.cargo}/bin"
  diesel_bin="$cargo_bin/diesel"

  if [[ -x "$diesel_bin" ]]; then
    installed_description="$($diesel_bin --version 2>/dev/null || true)"
    installed_version="$(
      parse_diesel_cli_version <<< "$installed_description" 2>/dev/null || true
    )"
  fi

  if [[ "$installed_version" != "$target_version" ]]; then
    cargo install \
      --locked \
      --force \
      --version "=${target_version}" \
      --no-default-features \
      --features postgres \
      diesel_cli
  fi

  if [[ ! -x "$diesel_bin" ]]; then
    echo "diesel binary not found at $diesel_bin" >&2
    exit 1
  fi

  version_description="$($diesel_bin --version)"
  if ! version="$(parse_diesel_cli_version <<< "$version_description")"; then
    printf 'could not parse Diesel CLI version after install:\n%s\n' \
      "$version_description" >&2
    exit 1
  fi
  if [[ "$version" != "$target_version" ]]; then
    printf 'unexpected Diesel CLI version after install: %s (expected %s)\n' \
      "$version" "$target_version" >&2
    exit 1
  fi
  version_label="diesel $version"
  if command -v sha256sum >/dev/null 2>&1; then
    digest="$(sha256sum "$diesel_bin" | awk '{print $1}')"
  else
    digest="$(shasum -a 256 "$diesel_bin" | awk '{print $1}')"
  fi

  echo "$version_label"
  echo "diesel sha256:$digest"

  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
      echo "- Diesel CLI: \`$version_label\`"
      echo "- Diesel CLI executable: \`sha256:$digest\`"
    } >> "$GITHUB_STEP_SUMMARY"
  fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
