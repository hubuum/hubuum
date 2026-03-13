#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <version>" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

version="$1"

awk -v version="$version" '
  $0 == "## [" version "]" || index($0, "## [" version "] - ") == 1 {
    in_section = 1
  }
  /^## \[/ && in_section && $0 != "## [" version "]" && index($0, "## [" version "] - ") != 1 {
    exit
  }
  in_section {
    print
  }
' CHANGELOG.md
