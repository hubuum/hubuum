#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
generator="$repo_root/scripts/generate-container-evidence.sh"
digest="$(printf 'a%.0s' {1..64})"
revision="$(printf 'b%.0s' {1..40})"

assert_rejected() {
  if HUBUUM_EVIDENCE_DIR="${TMPDIR:-/tmp}/hubuum-invalid-evidence" \
    bash "$generator" "$@" >/dev/null 2>&1; then
    echo "container evidence input was unexpectedly accepted: $*" >&2
    exit 1
  fi
}

assert_rejected "example.invalid/hubuum:latest" output "$revision" main linux-amd64
assert_rejected "example.invalid/hubuum@sha256:$digest" ../escape "$revision" main linux-amd64
assert_rejected "example.invalid/hubuum@sha256:$digest" output short main linux-amd64
assert_rejected "example.invalid/hubuum@sha256:$digest" output "$revision" latest linux-amd64

echo "Container evidence input validation tests passed."
