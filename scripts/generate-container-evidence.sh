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

image_pattern='^[a-z0-9][a-z0-9._-]*(/[a-z0-9][a-z0-9._-]*)*(:[A-Za-z0-9_][A-Za-z0-9._-]*)?@sha256:[0-9a-f]{64}$'
if [[ ! "$image_ref" =~ $image_pattern ]]; then
  echo "container evidence requires an immutable image digest: $image_ref" >&2
  exit 1
fi
if [[ ! "$output_prefix" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  echo "container evidence output prefix contains unsupported characters: $output_prefix" >&2
  exit 1
fi
if [[ ! "$source_revision" =~ ^[0-9a-f]{40}$ ]]; then
  echo "container evidence source revision must be a full lowercase commit SHA" >&2
  exit 1
fi
if [[ ! "$source_tag" =~ ^(main|v[0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
  echo "container evidence source tag must be main or a stable release tag" >&2
  exit 1
fi
if [[ ! "$target" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  echo "container evidence target contains unsupported characters: $target" >&2
  exit 1
fi
case "$target" in
  linux-amd64)
    cargo_target=x86_64-unknown-linux-musl
    ;;
  linux-arm64)
    cargo_target=aarch64-unknown-linux-musl
    ;;
  *)
    echo "container evidence target is unsupported: $target" >&2
    exit 1
    ;;
esac

SYFT_IMAGE="$(python3 "$repo_root/scripts/check-supply-chain-policy.py" --tool-value SYFT_IMAGE)"
TRIVY_IMAGE="$(python3 "$repo_root/scripts/check-supply-chain-policy.py" --tool-value TRIVY_IMAGE)"

evidence_dir="${HUBUUM_EVIDENCE_DIR:-$repo_root/supply-chain-evidence}"
mkdir -p "$evidence_dir"
evidence_dir="$(cd "$evidence_dir" && pwd -P)"
raw_sbom="$evidence_dir/${output_prefix}.syft.cdx.json"
merged_sbom="$evidence_dir/${output_prefix}.cdx.json"
full_report="$evidence_dir/${output_prefix}.trivy.json"
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
  --cargo-features tls-rustls,tls-openssl,embedded-migrations \
  --cargo-target "$cargo_target" \
  --output "$merged_sbom" \
  --subject-name "${image_ref%@*}" \
  --subject-digest "${image_ref##*@}" \
  --subject-type container \
  --source-revision "$source_revision" \
  --source-tag "$source_tag" \
  --target "$target"

trivy_cache="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/hubuum-trivy-cache"
mkdir -p "$trivy_cache"
trivy_cache="$(cd "$trivy_cache" && pwd -P)"
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

for evidence in \
  "$raw_sbom" \
  "$merged_sbom" \
  "$full_report" \
  "$policy_report" \
  "$sbom_report" \
  "$scan_status"; do
  if [[ ! -s "$evidence" ]]; then
    echo "container evidence output is missing or empty: $evidence" >&2
    exit 1
  fi
done

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
