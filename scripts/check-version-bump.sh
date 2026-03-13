#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

extract_version() {
  awk '
    $0 == "[package]" { in_package = 1; next }
    /^\[/ { in_package = 0 }
    in_package && $1 == "version" {
      if (match($0, /"([^"]+)"/)) {
        print substr($0, RSTART + 1, RLENGTH - 2)
        exit
      }
    }
  '
}

base_ref=""
range=""

if [[ -n "${GITHUB_BASE_REF:-}" ]]; then
  base_ref="origin/${GITHUB_BASE_REF}"
  range="${base_ref}...HEAD"
elif [[ -n "${GITHUB_EVENT_BEFORE:-}" && "${GITHUB_EVENT_BEFORE}" != "0000000000000000000000000000000000000000" ]]; then
  base_ref="${GITHUB_EVENT_BEFORE}"
  range="${base_ref}...HEAD"
elif git rev-parse --verify HEAD^ >/dev/null 2>&1; then
  base_ref="HEAD^"
  range="${base_ref}...HEAD"
else
  echo "Skipping version bump checks because there is no base commit to compare against."
  exit 0
fi

head_version="$(extract_version < Cargo.toml)"
base_version="$(git show "${base_ref}:Cargo.toml" | extract_version || true)"

if [[ -z "$base_version" ]]; then
  echo "Skipping version bump checks because the base Cargo.toml could not be read."
  exit 0
fi

if [[ "$head_version" == "$base_version" ]]; then
  echo "Cargo.toml version unchanged (${head_version}); no release note gating needed."
  exit 0
fi

changed_files="$(git diff --name-only "$range")"

file_changed() {
  local path="$1"

  if grep -Fxq "$path" <<<"$changed_files"; then
    return 0
  fi

  if git ls-files --others --exclude-standard -- "$path" | grep -Fxq "$path"; then
    return 0
  fi

  return 1
}

if ! file_changed "CHANGELOG.md"; then
  echo "Cargo.toml version changed from $base_version to $head_version, but CHANGELOG.md was not updated." >&2
  exit 1
fi

if ! file_changed "docs/openapi.json"; then
  echo "Cargo.toml version changed from $base_version to $head_version, but docs/openapi.json was not updated." >&2
  exit 1
fi

"$repo_root/scripts/check-release-readiness.sh"

echo "Version bump checks passed for $base_version -> $head_version"
