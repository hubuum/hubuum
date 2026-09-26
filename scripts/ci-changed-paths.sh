#!/usr/bin/env bash
set -euo pipefail

# Print NUL-delimited paths. Callers must capture this command's exit status
# before reading the output; a failed diff must never look like an empty diff.
base="${BASE_SHA:-}"
head="${HEAD_SHA:-HEAD}"
if [[ -z "$base" || "$base" =~ ^0+$ ]]; then
  git ls-files --cached --others --exclude-standard -z
else
  if [[ "${IS_PULL_REQUEST:-false}" == true ]]; then
    base="$(git merge-base "$base" "$head")"
  fi
  git diff --no-renames --name-only -z "$base" "$head" --
fi
