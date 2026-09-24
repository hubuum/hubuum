#!/usr/bin/env bash
# Copy this small launcher to a project's scripts/docs.sh.
set -euo pipefail
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
source .github/docs-tools.env
if [[ ! "$DOCS_TOOLING_REVISION" =~ ^[0-9a-f]{40}$ ]]; then
  echo 'DOCS_TOOLING_REVISION must pin a full commit SHA.' >&2
  exit 2
fi
tooling_checkout="${DOCS_TOOLING_ROOT:-$repo_root/target/docs-tooling}"
if [[ -z "${DOCS_TOOLING_ROOT:-}" ]]; then
  if [[ ! -d "$tooling_checkout/.git" ]]; then
    git init --quiet "$tooling_checkout"
    git -C "$tooling_checkout" remote add origin https://github.com/hubuum/.github.git
  fi
  if [[ "$(git -C "$tooling_checkout" rev-parse HEAD 2>/dev/null || true)" != "$DOCS_TOOLING_REVISION" ]]; then
    git -C "$tooling_checkout" fetch --depth 1 origin "$DOCS_TOOLING_REVISION"
    git -C "$tooling_checkout" checkout --detach "$DOCS_TOOLING_REVISION"
  fi
fi
exec bash "$tooling_checkout/docs-tooling/build.sh" "$@"
