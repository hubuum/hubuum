#!/usr/bin/env bash
set -euo pipefail

if (( $# != 6 )); then
  echo "Usage: $0 BASELINE CANDIDATE METADATA EXCEPTIONS CHANGELOG REPORT_DIRECTORY" >&2
  exit 2
fi

baseline_path="$1"
candidate_path="$2"
metadata_path="$3"
exceptions_path="$4"
changelog_path="$5"
report_dir="$6"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
oasdiff_bin="${OASDIFF_BIN:-oasdiff}"
severity_levels="$repo_root/.github/oasdiff-severity-levels.txt"
raw_changes="$report_dir/oasdiff-changes.json"
raw_diff="$report_dir/oasdiff-diff.json"
synthetic_changes="$report_dir/repository-checks.json"
all_changes="$report_dir/all-changes.json"
report_path="$report_dir/compatibility.json"
summary_path="$report_dir/summary.md"
mkdir -p "$report_dir"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

for json_path in "$candidate_path" "$metadata_path" "$exceptions_path"; do
  jq --exit-status . "$json_path" >/dev/null
done

if ! jq --exit-status '
  def nonblank_string:
    type == "string" and test("\\S");
  type == "object" and
  (.openapi | nonblank_string) and
  (.info | type == "object") and
  (.info.version | nonblank_string) and
  (.paths | type == "object")
' "$candidate_path" >/dev/null; then
  echo "Invalid candidate OpenAPI document: $candidate_path" >&2
  exit 1
fi
candidate_version="$(jq --raw-output '.info.version' "$candidate_path")"

if ! jq --exit-status '
  def nonblank_string:
    type == "string" and test("\\S");
  def stable_tag:
    nonblank_string and test("^v[0-9]+\\.[0-9]+\\.[0-9]+$");
  def sha256_digest:
    type == "string" and test("^[0-9a-f]{64}$");
  type == "object" and
  (.status == "available" or .status == "unavailable") and
  (.source | nonblank_string) and
  (.candidate_ref | nonblank_string) and
  (.candidate_sha | nonblank_string) and
  if .status == "available" then
    (.tag | stable_tag) and
    (.sha256 | sha256_digest) and
    .reason == null
  else
    .tag == null and
    .sha256 == null and
    (.reason | nonblank_string)
  end
' "$metadata_path" >/dev/null; then
  echo "Invalid OpenAPI baseline metadata: $metadata_path" >&2
  exit 1
fi

today="${HUBUUM_OPENAPI_TODAY:-$(date -u +%F)}"
if ! jq --null-input --exit-status --arg date "$today" '
  try (
    ($date | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")) and
    (($date + "T00:00:00Z" | fromdateiso8601 | strftime("%Y-%m-%d")) == $date)
  ) catch false
' >/dev/null; then
  echo "Invalid OpenAPI policy date: $today" >&2
  exit 1
fi

if ! jq --exit-status '
  def nonblank_string:
    type == "string" and test("\\S");
  def stable_tag:
    nonblank_string and test("^v[0-9]+\\.[0-9]+\\.[0-9]+$");
  def valid_date:
    try (
      (type == "string") and
      test("^[0-9]{4}-[0-9]{2}-[0-9]{2}$") and
      (((. + "T00:00:00Z") | fromdateiso8601 | strftime("%Y-%m-%d")) == .)
    ) catch false;
  .schema_version == 1 and
  (.exceptions | type == "array") and
  all(.exceptions[];
    (.id | type == "string" and test("^[a-z0-9]+([.-][a-z0-9]+)*$")) and
    (.baseline | stable_tag) and
    (.expires | valid_date) and
    (.reason | nonblank_string) and
    (.migration | nonblank_string) and
    (.changelog_entry | nonblank_string) and
    (.fingerprints | type == "array" and length > 0) and
    all(.fingerprints[]; nonblank_string)
  )
' "$exceptions_path" >/dev/null; then
  echo "Invalid OpenAPI compatibility exception file: $exceptions_path" >&2
  exit 1
fi

candidate_sha256="$(sha256_file "$candidate_path")"
metadata_status="$(jq --raw-output '.status' "$metadata_path")"
if [[ "$metadata_status" == "unavailable" ]]; then
  jq --null-input \
    --slurpfile baseline "$metadata_path" \
    --arg candidate_sha256 "$candidate_sha256" \
    '{
      status: "skipped",
      reason: $baseline[0].reason,
      baseline: $baseline[0],
      candidate: {sha256: $candidate_sha256},
      counts: {additive: 0, behavioral: 0, breaking: 0, accepted: 0, unaccepted: 0},
      additive: [],
      behavioral: [],
      breaking: [],
      exception_errors: []
    }' > "$report_path"
  {
    echo "## OpenAPI compatibility"
    echo
    echo "No stable compatibility baseline is available. The check was explicitly skipped."
    echo
    echo "Reason: $(jq --raw-output '.reason' "$metadata_path")"
  } > "$summary_path"
  exit 0
fi

jq --exit-status . "$baseline_path" >/dev/null
baseline_sha256="$(sha256_file "$baseline_path")"
expected_baseline_sha256="$(jq --raw-output '.sha256' "$metadata_path")"
if [[ "$baseline_sha256" != "$expected_baseline_sha256" ]]; then
  echo "OpenAPI baseline digest mismatch: expected $expected_baseline_sha256, got $baseline_sha256" >&2
  exit 1
fi

baseline_tag="$(jq --raw-output '.tag' "$metadata_path")"
baseline_version="${baseline_tag#v}"
if ! jq --exit-status --arg expected_version "$baseline_version" '
  type == "object" and
  (.openapi | type == "string" and test("\\S")) and
  (.info | type == "object") and
  .info.version == $expected_version and
  (.paths | type == "object")
' "$baseline_path" >/dev/null; then
  echo "OpenAPI baseline document does not match metadata tag $baseline_tag" >&2
  exit 1
fi

if ! "$oasdiff_bin" changelog \
  "$baseline_path" \
  "$candidate_path" \
  --allow-external-refs=false \
  --severity-levels "$severity_levels" \
  --format json > "$raw_changes"; then
  echo "oasdiff changelog failed" >&2
  exit 1
fi

if ! "$oasdiff_bin" diff \
  "$baseline_path" \
  "$candidate_path" \
  --allow-external-refs=false \
  --format json > "$raw_diff"; then
  echo "oasdiff structural diff failed" >&2
  exit 1
fi

# Oasdiff 1.16 does not emit changelog findings for parameter serialization.
# Promote those structural changes, and parameter moves, to explicit breaks.
jq '[
  (.paths.modified // {} | to_entries[] as $path
    | ($path.value.operations.modified // {} | to_entries[] as $operation
      | ($operation.value.parameters.modified // {} | to_entries[] as $location
        | ($location.value | to_entries[] as $parameter
          | ($parameter.value | to_entries[]
            | select(.key == "style" or .key == "explode" or .key == "allowReserved")
            | {
                id: "request-parameter-serialization-changed",
                text: ("the `" + $location.key + "` request parameter `" + $parameter.key + "` changed `" + .key + "` from `" + (.value.from | tostring) + "` to `" + (.value.to | tostring) + "`"),
                level: 3,
                operation: $operation.key,
                path: $path.key,
                section: "paths",
                fingerprint: ("serialization:" + $operation.key + ":" + $path.key + ":" + $location.key + ":" + $parameter.key + ":" + .key)
              }
          )
        )
      )
    )
  ),
  (.paths.modified // {} | to_entries[] as $path
    | ($path.value.operations.modified // {} | to_entries[] as $operation
      | ($operation.value.parameters.added // {} | to_entries[] as $added_location
        | $added_location.value[] as $name
        | ($operation.value.parameters.deleted // {} | to_entries[]
          | select(.key != $added_location.key and (.value | index($name)))
          | {
              id: "request-parameter-location-changed",
              text: ("the request parameter `" + $name + "` moved from `" + .key + "` to `" + $added_location.key + "`"),
              level: 3,
              operation: $operation.key,
              path: $path.key,
              section: "paths",
              fingerprint: ("location:" + $operation.key + ":" + $path.key + ":" + $name + ":" + .key + ":" + $added_location.key)
            }
        )
      )
    )
  )
]' "$raw_diff" > "$synthetic_changes"

jq --slurp 'add | unique_by(.fingerprint)' \
  "$raw_changes" \
  "$synthetic_changes" > "$all_changes"

checked_changelog_path="$report_dir/checked-changelog.md"
awk '
  /^## \[Unreleased\]/ { in_unreleased = 1; next }
  in_unreleased && /^## \[/ { exit }
  in_unreleased { print }
' "$changelog_path" > "$checked_changelog_path"
if [[ "$candidate_version" != "$baseline_version" ]]; then
  awk -v heading="## [$candidate_version]" '
    index($0, heading) == 1 &&
      (length($0) == length(heading) || substr($0, length(heading) + 1, 1) == " ") {
      in_release = 1
      next
    }
    in_release && /^## \[/ { exit }
    in_release { print }
  ' "$changelog_path" >> "$checked_changelog_path"
fi

jq --null-input \
  --slurpfile changes "$all_changes" \
  --slurpfile baseline "$metadata_path" \
  --slurpfile policy "$exceptions_path" \
  --rawfile checked_changelog "$checked_changelog_path" \
  --arg baseline_tag "$baseline_tag" \
  --arg baseline_version "$baseline_version" \
  --arg candidate_version "$candidate_version" \
  --arg today "$today" \
  --arg candidate_sha256 "$candidate_sha256" '
  def normalize_whitespace:
    gsub("\\s+"; " ");
  def matching_exception($valid; $fingerprint):
    first(
      $valid[] as $exception
      | select($exception.fingerprints | index($fingerprint))
      | $exception
    ) // null;
  def duplicate_id_errors:
    [
      $policy[0].exceptions
      | group_by(.id)[]
      | select(length > 1)
      | "duplicate exception id `\(.[0].id)`"
    ];
  def duplicate_fingerprint_errors:
    [
      $policy[0].exceptions
      | group_by(.baseline)[]
      | . as $same_baseline
      | ([.[] | .fingerprints[]] | group_by(.)[] | select(length > 1) | .[0]) as $fingerprint
      | "duplicate fingerprint `\($fingerprint)` for baseline `\($same_baseline[0].baseline)`"
    ];
  ($checked_changelog | normalize_whitespace) as $document
  | ($policy[0].exceptions | map(select(.baseline == $baseline_tag))) as $current_exceptions
  | ($current_exceptions | map(
      . as $exception
      | select(
          $exception.expires >= $today and
          ($document | contains($exception.changelog_entry | normalize_whitespace)) and
          ($document | contains($exception.migration | normalize_whitespace))
        )
    )) as $valid_exceptions
  | ($changes[0] | map(select(.level == 3) | .fingerprint)) as $breaking_fingerprints
  | ([
      $current_exceptions[] as $exception
      | if $exception.expires < $today then
          "exception `\($exception.id)` expired on \($exception.expires)"
        elif ($document | contains($exception.changelog_entry | normalize_whitespace) | not) then
          "exception `\($exception.id)` has no matching candidate changelog entry"
        elif ($document | contains($exception.migration | normalize_whitespace) | not) then
          "exception `\($exception.id)` migration guidance is not present in the candidate changelog"
        else
          empty
        end
    ]) as $documentation_errors
  | ([
      $valid_exceptions[] as $exception
      | $exception.fingerprints[] as $fingerprint
      | select(($breaking_fingerprints | index($fingerprint)) == null)
      | "exception `\($exception.id)` contains unused fingerprint `\($fingerprint)`"
    ]) as $unused_errors
  | ($changes[0] | map(select(.level == 1))) as $additive
  | ($changes[0] | map(select(.level == 2))) as $behavioral
  | ($changes[0] | map(
      select(.level == 3)
      | . as $change
      | matching_exception($valid_exceptions; $change.fingerprint) as $exception
      | $change + {
          accepted: ($exception != null),
          exception_id: ($exception.id // null)
        }
    )) as $breaking
  | (
      duplicate_id_errors +
      duplicate_fingerprint_errors +
      $documentation_errors +
      $unused_errors
    ) as $exception_errors
  | ($breaking | map(select(.accepted))) as $accepted
  | ($breaking | map(select(.accepted | not))) as $unaccepted
  | {
      status: (if (($unaccepted | length) == 0 and ($exception_errors | length) == 0) then "pass" else "fail" end),
      baseline: $baseline[0],
      candidate: {sha256: $candidate_sha256},
      policy: {
        checked_on: $today,
        exception_file_schema: $policy[0].schema_version,
        changelog_sections: (
          if $candidate_version == $baseline_version
          then ["Unreleased"]
          else ["Unreleased", $candidate_version]
          end
        ),
        inactive_exception_ids: ($policy[0].exceptions | map(select(.baseline != $baseline_tag) | .id))
      },
      counts: {
        additive: ($additive | length),
        behavioral: ($behavioral | length),
        breaking: ($breaking | length),
        accepted: ($accepted | length),
        unaccepted: ($unaccepted | length)
      },
      additive: $additive,
      behavioral: $behavioral,
      breaking: $breaking,
      exception_errors: $exception_errors
    }
' > "$report_path"

render_group() {
  local heading="$1"
  local group="$2"
  local count
  count="$(jq --arg group "$group" '.[$group] | length' "$report_path")"
  echo "### $heading ($count)"
  echo
  if (( count == 0 )); then
    echo "None."
  else
    jq --raw-output --arg group "$group" '
      .[$group][]
      | "- [" + .fingerprint + "] " + (.operation // "component") +
        " `" + (.path // .section) + "` — " + .text +
        (if has("accepted") then
           if .accepted then " (accepted by `" + .exception_id + "`)"
           else " (unaccepted)"
           end
         else ""
         end)
    ' "$report_path"
  fi
  echo
}

{
  echo "## OpenAPI compatibility"
  echo
  echo "Baseline: \`$baseline_tag\` (SHA-256 \`$(jq --raw-output '.baseline.sha256' "$report_path")\`)"
  echo
  echo "Candidate SHA-256: \`$candidate_sha256\`"
  echo
  echo "Result: **$(jq --raw-output '.status' "$report_path")**"
  echo
  render_group "Additive" 'additive'
  render_group "Behavioral" 'behavioral'
  render_group "Breaking" 'breaking'
  exception_error_count="$(jq '.exception_errors | length' "$report_path")"
  if (( exception_error_count > 0 )); then
    echo "### Exception policy errors ($exception_error_count)"
    echo
    jq --raw-output '.exception_errors[] | "- " + .' "$report_path"
    echo
  fi
} > "$summary_path"

if [[ "$(jq --raw-output '.status' "$report_path")" != "pass" ]]; then
  echo "OpenAPI compatibility check failed; see $summary_path" >&2
  exit 1
fi
