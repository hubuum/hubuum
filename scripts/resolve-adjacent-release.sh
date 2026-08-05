#!/usr/bin/env bash
set -euo pipefail

repository="${GITHUB_REPOSITORY:-hubuum/hubuum}"
image_repository="${HUBUUM_RELEASE_IMAGE_REPOSITORY:-ghcr.io/hubuum/hubuum-server}"
api_url="${GITHUB_API_URL:-https://api.github.com}"
report_path="${HUBUUM_COMPATIBILITY_REPORT:-adjacent-release-metadata.json}"
mkdir -p "$(dirname -- "$report_path")"

headers=(
  --header "Accept: application/vnd.github+json"
  --header "X-GitHub-Api-Version: 2022-11-28"
)
if [[ -n "${GH_TOKEN:-}" ]]; then
  headers+=(--header "Authorization: Bearer ${GH_TOKEN}")
fi

if [[ -n "${HUBUUM_PREVIOUS_RELEASE_TAG:-}" ]]; then
  previous_tag="$HUBUUM_PREVIOUS_RELEASE_TAG"
else
  previous_tag="$(
    curl --fail --silent --show-error "${headers[@]}" \
      "$api_url/repos/$repository/releases/latest" | jq --raw-output '.tag_name'
  )"
fi
[[ "$previous_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
  echo "ERROR: latest stable release tag '$previous_tag' is not semantic version vMAJOR.MINOR.PATCH" >&2
  exit 1
}

tagged_image="$image_repository:$previous_tag"
docker pull "$tagged_image" >/dev/null
previous_image="$(
  docker image inspect "$tagged_image" \
    --format '{{range .RepoDigests}}{{println .}}{{end}}' \
    | grep -E "^${image_repository//./\.}@sha256:[0-9a-f]{64}$" \
    | head -n 1
)"
[[ "$previous_image" =~ @sha256:[0-9a-f]{64}$ ]] || {
  echo "ERROR: could not resolve an immutable digest for $tagged_image" >&2
  exit 1
}

jq --null-input \
  --arg previous_tag "$previous_tag" \
  --arg previous_image "$previous_image" \
  --arg candidate_sha "${GITHUB_SHA:-$(git rev-parse HEAD)}" \
  '{previous_tag: $previous_tag, previous_image: $previous_image, candidate_sha: $candidate_sha}' \
  > "$report_path"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  {
    echo "previous_tag=$previous_tag"
    echo "previous_image=$previous_image"
    echo "report_path=$report_path"
  } >> "$GITHUB_OUTPUT"
fi

echo "Resolved adjacent release $previous_tag as $previous_image"
