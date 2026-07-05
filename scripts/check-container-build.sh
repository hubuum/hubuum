#!/usr/bin/env bash
set -euo pipefail

variant="rustls-only"
tag="hubuum:local-container-check"
platform=""
load_flag=()

usage() {
  cat <<'USAGE'
Usage: scripts/check-container-build.sh [--variant rustls-only|full] [--platform linux/amd64|linux/arm64] [--tag TAG] [--no-load]

Builds the production Dockerfile locally with the same feature variants used by CI.
Defaults to the rustls-only variant because it is the cheaper container smoke test.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --variant)
      variant="${2:-}"
      shift 2
      ;;
    --platform)
      platform="${2:-}"
      shift 2
      ;;
    --tag)
      tag="${2:-}"
      shift 2
      ;;
    --no-load)
      load_flag=(--output type=cacheonly)
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$variant" in
  rustls-only)
    cargo_build_flags="-F tls-rustls --locked --release"
    ;;
  full)
    cargo_build_flags="-F tls-rustls -F tls-openssl --locked --release"
    ;;
  *)
    echo "--variant must be 'rustls-only' or 'full'" >&2
    exit 2
    ;;
esac

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the container build check" >&2
  exit 1
fi

build_args=(
  buildx build
  --file Dockerfile
  --build-arg "CARGO_BUILD_FLAGS=${cargo_build_flags}"
  --tag "$tag"
)

if [[ -n "$platform" ]]; then
  build_args+=(--platform "$platform")
fi

if [[ ${#load_flag[@]} -gt 0 ]]; then
  build_args+=("${load_flag[@]}")
else
  build_args+=(--load)
fi

build_args+=(.)

docker "${build_args[@]}"
