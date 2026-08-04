#!/usr/bin/env bash
set -euo pipefail

if (($# != 5)); then
  echo "usage: $0 <image@digest> <output-prefix> <source-revision> <source-tag> <target>" >&2
  exit 2
fi

image_ref="$1"
output_prefix="$2"
source_revision="$3"
source_tag="$4"
target="$5"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$repo_root/.github/supply-chain-tools.env"

if [[ "$image_ref" != *@sha256:* ]]; then
  echo "container evidence requires an immutable image digest: $image_ref" >&2
  exit 1
fi

evidence_dir="${HUBUUM_EVIDENCE_DIR:-$repo_root/supply-chain-evidence}"
mkdir -p "$evidence_dir"
raw_sbom="$evidence_dir/${output_prefix}.syft.cdx.json"
merged_sbom="$evidence_dir/${output_prefix}.cdx.json"
sbom_report="$evidence_dir/${output_prefix}.sbom-trivy.json"
policy_report="$evidence_dir/${output_prefix}.trivy-policy.json"
scan_status="$evidence_dir/${output_prefix}.scan-status"

docker run --rm \
  --env SYFT_REGISTRY_AUTH_USERNAME="${GITHUB_ACTOR:-}" \
  --env SYFT_REGISTRY_AUTH_PASSWORD="${GITHUB_TOKEN:-}" \
  "$SYFT_IMAGE" \
  "registry:${image_ref}" \
  --output cyclonedx-json > "$raw_sbom"

cd "$repo_root"
python3 scripts/generate-release-sbom.py \
  --base-sbom "$raw_sbom" \
  --output "$merged_sbom" \
  --subject-name "${image_ref%@*}" \
  --subject-digest "${image_ref##*@}" \
  --subject-type container \
  --source-revision "$source_revision" \
  --source-tag "$source_tag" \
  --target "$target"

trivy_cache="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/hubuum-trivy-cache"
mkdir -p "$trivy_cache"
docker run --rm \
  --env TRIVY_USERNAME="${GITHUB_ACTOR:-}" \
  --env TRIVY_PASSWORD="${GITHUB_TOKEN:-}" \
  --volume "$trivy_cache:/root/.cache" \
  --volume "$evidence_dir:/evidence" \
  --volume "$repo_root:/workspace:ro" \
  "$TRIVY_IMAGE" \
  image \
  --scanners vuln \
  --pkg-types os,library \
  --severity HIGH,CRITICAL \
  --ignorefile /workspace/.trivyignore \
  --format json \
  --output "/evidence/${output_prefix}.trivy.json" \
  "$image_ref"

set +e
docker run --rm \
  --env TRIVY_USERNAME="${GITHUB_ACTOR:-}" \
  --env TRIVY_PASSWORD="${GITHUB_TOKEN:-}" \
  --volume "$trivy_cache:/root/.cache" \
  --volume "$evidence_dir:/evidence" \
  --volume "$repo_root:/workspace:ro" \
  "$TRIVY_IMAGE" \
  image \
  --scanners vuln \
  --pkg-types os,library \
  --severity HIGH,CRITICAL \
  --ignore-unfixed \
  --ignorefile /workspace/.trivyignore \
  --format json \
  --output "/evidence/${output_prefix}.trivy-policy.json" \
  --exit-code 1 \
  "$image_ref"
image_scan_status="$?"
set -e
printf '%s\n' "$image_scan_status" > "$scan_status"

# Scan the Syft-derived SBOM independently and retain Trivy's JSON result.
docker run --rm \
  --volume "$trivy_cache:/root/.cache" \
  --volume "$evidence_dir:/evidence:ro" \
  "$TRIVY_IMAGE" \
  sbom \
  --severity HIGH,CRITICAL \
  --format json \
  "/evidence/${output_prefix}.cdx.json" > "$sbom_report"

test -s "$policy_report"

syft_version="$(docker run --rm "$SYFT_IMAGE" version)"
trivy_version="$(docker run --rm "$TRIVY_IMAGE" --version)"
syft_platform_digest="$(docker image inspect "$SYFT_IMAGE" --format '{{.Id}}')"
trivy_platform_digest="$(docker image inspect "$TRIVY_IMAGE" --format '{{.Id}}')"
{
  echo "$syft_version"
  echo "Syft image manifest: ${SYFT_IMAGE##*@}"
  echo "Syft platform image: $syft_platform_digest"
  echo "$trivy_version"
  echo "Trivy image manifest: ${TRIVY_IMAGE##*@}"
  echo "Trivy platform image: $trivy_platform_digest"
}

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "- Syft image: \`$SYFT_IMAGE\`"
    echo "- Syft platform image: \`$syft_platform_digest\`"
    echo "- Trivy image: \`$TRIVY_IMAGE\`"
    echo "- Trivy platform image: \`$trivy_platform_digest\`"
    echo "- Final-image scan policy: fixed HIGH/CRITICAL vulnerabilities fail publication"
  } >> "$GITHUB_STEP_SUMMARY"
fi
