#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
base_ref="${1:-}"

if [[ -z "$base_ref" ]]; then
  while IFS= read -r candidate_ref; do
    if [[ "${GITHUB_REF_TYPE:-}" == "tag" && "$candidate_ref" == "${GITHUB_REF_NAME:-}" ]]; then
      continue
    fi
    base_ref="$candidate_ref"
    break
  done < <(git -C "$repository_root" tag --list 'v[0-9]*' --sort=-v:refname)
fi
[[ -n "$base_ref" ]] || {
  echo "ERROR: no stable release tag is available for migration review" >&2
  exit 1
}
git -C "$repository_root" rev-parse --verify --quiet "${base_ref}^{commit}" >/dev/null || {
  echo "ERROR: migration compatibility baseline '$base_ref' does not exist" >&2
  exit 1
}

failures=0
checked=0

report_failure() {
  local file="$1"
  local line="$2"
  local reason="$3"

  printf '%s:%s: %s\n' "$file" "$line" "$reason" >&2
  failures=$((failures + 1))
}

sql_statements() {
  local file="$1"

  awk '
    /^[[:space:]]*(--|$)/ { next }
    statement == "" { first_line = NR }
    {
      statement = statement " " $0
      if ($0 ~ /;/) {
        sub(/^[[:space:]]+/, "", statement)
        print first_line ":" statement
        statement = ""
      }
    }
    END {
      if (statement != "") {
        sub(/^[[:space:]]+/, "", statement)
        print first_line ":" statement
      }
    }
  ' "$file"
}

while IFS= read -r file; do
  [[ -n "$file" ]] || continue
  checked=$((checked + 1))

  while IFS=: read -r line_number statement; do
    [[ -n "$line_number" ]] || continue
    upper_statement="$(printf '%s' "$statement" | tr '[:lower:]' '[:upper:]')"

    if [[ "$upper_statement" =~ DROP[[:space:]]+(TABLE|COLUMN|CONSTRAINT|INDEX) ]]; then
      report_failure "$file" "$line_number" "dropping schema objects is not adjacent-release compatible"
    elif [[ "$upper_statement" =~ RENAME[[:space:]]+(TO|COLUMN|CONSTRAINT) ]]; then
      report_failure "$file" "$line_number" "renaming schema objects is not adjacent-release compatible"
    elif [[ "$upper_statement" =~ ALTER[[:space:]]+COLUMN.*[[:space:]]TYPE[[:space:]] ]]; then
      report_failure "$file" "$line_number" "changing a column type in place is not adjacent-release compatible"
    elif [[ "$upper_statement" =~ ADD[[:space:]]+COLUMN.*NOT[[:space:]]+NULL ]] \
      && [[ ! "$upper_statement" =~ DEFAULT[[:space:]] ]]; then
      report_failure "$file" "$line_number" "a new NOT NULL column must have an old-writer-compatible default"
    elif [[ "$upper_statement" =~ ALTER[[:space:]]+COLUMN.*SET[[:space:]]+NOT[[:space:]]+NULL ]]; then
      if [[ "$upper_statement" != *"HUBUUM-COMPAT: SET-NOT-NULL-AFTER-BACKFILL"* ]]; then
        report_failure "$file" "$line_number" "setting NOT NULL requires a separately reviewed backfill phase"
      fi
    elif [[ "$upper_statement" =~ ADD[[:space:]]+CONSTRAINT ]] \
      && [[ ! "$upper_statement" =~ NOT[[:space:]]+VALID ]]; then
      report_failure "$file" "$line_number" "new constraints must use NOT VALID before separate validation"
    elif [[ "$upper_statement" =~ CREATE([[:space:]]+UNIQUE)?[[:space:]]+INDEX[[:space:]] ]] \
      && [[ ! "$upper_statement" =~ INDEX[[:space:]]+CONCURRENTLY[[:space:]] ]]; then
      report_failure "$file" "$line_number" "indexes on an adjacent-release path must be created concurrently"
    fi
  done < <(sql_statements "$repository_root/$file")
done < <(
  git -C "$repository_root" diff --diff-filter=AM --name-only "$base_ref"...HEAD -- 'migrations/*/up.sql'
)

if ((failures > 0)); then
  echo "Migration compatibility review failed with $failures unsafe pattern(s)." >&2
  exit 1
fi

echo "Migration compatibility review passed for $checked migration file(s) since $base_ref."
