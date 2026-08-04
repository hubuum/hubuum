#!/usr/bin/env bash
set -euo pipefail

if (( $# != 1 )); then
  echo "Usage: $0 REPORT_DIRECTORY" >&2
  exit 2
fi

report_dir="$1"
baseline_path="$report_dir/baseline.json"
metadata_path="$report_dir/baseline-metadata.json"
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
      source: (if $source == "" then null else $source end),
      sha256: (if $sha256 == "" then null else $sha256 end),
      reason: (if $reason == "" then null else $reason end),
      candidate_ref: $candidate_ref,
      candidate_sha: $candidate_sha
    }' > "$metadata_path"
}

if [[ -n "${HUBUUM_OPENAPI_BASELINE_FILE:-}" ]]; then
  if [[ -z "${HUBUUM_OPENAPI_BASELINE_TAG:-}" ]]; then
    echo "HUBUUM_OPENAPI_BASELINE_TAG is required with HUBUUM_OPENAPI_BASELINE_FILE" >&2
    exit 1
  fi
  cp "$HUBUUM_OPENAPI_BASELINE_FILE" "$baseline_path"
  jq --exit-status . "$baseline_path" >/dev/null
  digest="$(sha256_file "$baseline_path")"
  write_metadata \
    "available" \
    "$HUBUUM_OPENAPI_BASELINE_TAG" \
    "file:$HUBUUM_OPENAPI_BASELINE_FILE" \
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
    --fail
    --location
    --silent
    --show-error
    --header "Accept: $accept"
    --header "X-GitHub-Api-Version: 2022-11-28"
    --output "$output"
  )
  if [[ -n "$token" ]]; then
    curl_args+=(--header "Authorization: Bearer $token")
  fi
  curl "${curl_args[@]}" "$url"
}

releases="$report_dir/releases.json"
github_get \
  "application/vnd.github+json" \
  "$api_url/repos/$repository/releases?per_page=100" \
  "$releases"

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
contents_url="$api_url/repos/$repository/contents/docs/openapi.json?ref=$encoded_tag"
github_get "application/vnd.github.raw+json" "$contents_url" "$baseline_path"
jq --exit-status . "$baseline_path" >/dev/null

digest="$(sha256_file "$baseline_path")"
write_metadata "available" "$baseline_tag" "$contents_url" "$digest" ""
