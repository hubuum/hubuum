#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

package_version="$(
  awk '
    $0 == "[package]" { in_package = 1; next }
    /^\[/ { in_package = 0 }
    in_package && $1 == "version" {
      if (match($0, /"([^"]+)"/)) {
        print substr($0, RSTART + 1, RLENGTH - 2)
        exit
      }
    }
  ' Cargo.toml
)"

if [[ -z "$package_version" ]]; then
  echo "Could not determine package version from Cargo.toml" >&2
  exit 1
fi

if ! grep -Eq "^## \\[$package_version\\]" CHANGELOG.md; then
  echo "CHANGELOG.md is missing a section for version $package_version" >&2
  exit 1
fi

if ! grep -Eq "\"version\": \"$package_version\"" docs/openapi.json; then
  echo "docs/openapi.json does not match Cargo.toml version $package_version" >&2
  exit 1
fi

release_ref="${1:-${GITHUB_REF_NAME:-}}"
if [[ -n "$release_ref" && "$release_ref" == v* ]]; then
  expected_tag="v$package_version"
  if [[ "$release_ref" != "$expected_tag" ]]; then
    echo "Release tag $release_ref does not match Cargo.toml version $package_version ($expected_tag)" >&2
    exit 1
  fi
fi

echo "Release readiness checks passed for version $package_version"
