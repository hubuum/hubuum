#!/usr/bin/env bash
set -euo pipefail

if (( $# != 1 )); then
  echo "Usage: $0 REPORT_DIRECTORY" >&2
  exit 2
fi

report_dir="$1"
baseline_path="$report_dir/baseline.json"
metadata_path="$report_dir/baseline-metadata.json"
bootstrap_last_tag="v0.0.11"
mkdir -p "$report_dir"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

write_metadata() {
  local status="$1"
  local tag="$2"
  local source="$3"
  local digest="$4"
  local reason="$5"

  jq --null-input \
    --arg status "$status" \
    --arg tag "$tag" \
    --arg source "$source" \
    --arg sha256 "$digest" \
    --arg reason "$reason" \
    --arg candidate_ref "${GITHUB_REF_NAME:-local}" \
    --arg candidate_sha "${GITHUB_SHA:-local}" \
    '{
      status: $status,
      tag: (if $tag == "" then null else $tag end),
      source: $source,
      sha256: (if $sha256 == "" then null else $sha256 end),
      reason: (if $reason == "" then null else $reason end),
      candidate_ref: $candidate_ref,
      candidate_sha: $candidate_sha
    }' > "$metadata_path"
}

validate_baseline_document() {
  local tag="$1"
  local expected_version="${tag#v}"

  if ! jq --exit-status --arg expected_version "$expected_version" '
    type == "object" and
    .schema_version == 1 and
    .release == $expected_version and
    (.metrics | type == "array") and
    (.configuration | type == "array") and
    (.events | type == "object") and
    (.documents | type == "object") and
    (.cli | type == "array")
  ' "$baseline_path" >/dev/null; then
    echo "Operational-contract baseline document does not match tag $tag" >&2
    exit 1
  fi
}

is_bootstrap_release() {
  local tag="$1"

  jq --null-input --exit-status \
    --arg tag "$tag" \
    --arg bootstrap_last_tag "$bootstrap_last_tag" '
      def version_parts: ltrimstr("v") | split(".") | map(tonumber);
      ($tag | version_parts) <= ($bootstrap_last_tag | version_parts)
    ' >/dev/null
}

if [[ -n "${HUBUUM_OPERATIONAL_CONTRACT_BASELINE_FILE:-}" ]]; then
  if [[ -z "${HUBUUM_OPERATIONAL_CONTRACT_BASELINE_TAG:-}" ]]; then
    echo "HUBUUM_OPERATIONAL_CONTRACT_BASELINE_TAG is required with HUBUUM_OPERATIONAL_CONTRACT_BASELINE_FILE" >&2
    exit 1
  fi
  cp "$HUBUUM_OPERATIONAL_CONTRACT_BASELINE_FILE" "$baseline_path"
  validate_baseline_document "$HUBUUM_OPERATIONAL_CONTRACT_BASELINE_TAG"
  digest="$(sha256_file "$baseline_path")"
  write_metadata \
    "available" \
    "$HUBUUM_OPERATIONAL_CONTRACT_BASELINE_TAG" \
    "file:$HUBUUM_OPERATIONAL_CONTRACT_BASELINE_FILE" \
    "$digest" \
    ""
  exit 0
fi

repository="${GITHUB_REPOSITORY:-hubuum/hubuum}"
api_url="${GITHUB_API_URL:-https://api.github.com}"
token="${GITHUB_TOKEN:-}"
current_tag=""
if [[ "${GITHUB_REF:-}" == refs/tags/v* ]]; then
  current_tag="${GITHUB_REF_NAME:-${GITHUB_REF#refs/tags/}}"
fi

github_get() {
  local accept="$1"
  local url="$2"
  local output="$3"
  local curl_args=(
    --location
    --silent
    --show-error
    --header "Accept: $accept"
    --header "X-GitHub-Api-Version: 2022-11-28"
    --output "$output"
    --write-out '%{http_code}'
  )
  if [[ -n "$token" ]]; then
    curl_args+=(--header "Authorization: Bearer $token")
  fi
  curl "${curl_args[@]}" "$url"
}

releases="$report_dir/releases.json"
releases_status="$(github_get \
  "application/vnd.github+json" \
  "$api_url/repos/$repository/releases?per_page=100" \
  "$releases")"
if [[ "$releases_status" != "200" ]]; then
  echo "GitHub releases request returned HTTP $releases_status" >&2
  exit 1
fi

baseline_tag="$(jq --raw-output \
  --arg current "$current_tag" \
  'def version_parts:
     ltrimstr("v") | split(".") | map(tonumber);
   ($current | if test("^v[0-9]+\\.[0-9]+\\.[0-9]+$") then version_parts else null end) as $current_version
   | [
       .[]
       | select(.draft == false and .prerelease == false)
       | select(.tag_name | test("^v[0-9]+\\.[0-9]+\\.[0-9]+$"))
       | {tag: .tag_name, version: (.tag_name | version_parts)}
       | select($current_version == null or .version < $current_version)
     ]
   | sort_by(.version)
   | last.tag // empty' \
  "$releases")"

if [[ -z "$baseline_tag" ]]; then
  write_metadata \
    "unavailable" \
    "" \
    "$api_url/repos/$repository/releases?per_page=100" \
    "" \
    "No stable semantic-versioned release exists before the candidate."
  exit 0
fi

encoded_tag="$(jq --null-input --raw-output --arg value "$baseline_tag" '$value | @uri')"
contents_url="$api_url/repos/$repository/contents/docs/operational-contract.json?ref=$encoded_tag"
contents_status="$(github_get "application/vnd.github.raw+json" "$contents_url" "$baseline_path")"
if [[ "$contents_status" == "404" ]]; then
  rm -f "$baseline_path"
  if is_bootstrap_release "$baseline_tag"; then
    write_metadata \
      "unavailable" \
      "$baseline_tag" \
      "$contents_url" \
      "" \
      "Stable release $baseline_tag is at or before the known pre-snapshot bootstrap boundary $bootstrap_last_tag."
    exit 0
  fi
  echo "Stable release $baseline_tag is newer than the operational-contract bootstrap boundary but has no snapshot" >&2
  exit 1
fi
if [[ "$contents_status" != "200" ]]; then
  echo "Operational-contract baseline request returned HTTP $contents_status" >&2
  exit 1
fi

validate_baseline_document "$baseline_tag"
digest="$(sha256_file "$baseline_path")"
write_metadata "available" "$baseline_tag" "$contents_url" "$digest" ""
