#!/usr/bin/env bash
set -euo pipefail

version="1.33.0"
install_dir="${1:-${RUNNER_TEMP:-/tmp}/oasdiff}"

case "$(uname -s):$(uname -m)" in
  Darwin:arm64 | Darwin:x86_64)
    archive="oasdiff_${version}_darwin_all.tar.gz"
    expected_sha256="2a479337c15afdcbf0b1e89c0b4d0cf0176472dbc11483358fed1f831d1e46c5"
    ;;
  Linux:x86_64)
    archive="oasdiff_${version}_linux_amd64.tar.gz"
    expected_sha256="43a4e328e2d13ba1552d760aa68d2485c75c5621f309f6ff64ae895188345247"
    ;;
  Linux:aarch64 | Linux:arm64)
    archive="oasdiff_${version}_linux_arm64.tar.gz"
    expected_sha256="4ae3c362d6074d919aada2dea82d0ee84366591600384455d0bdc658ddf8f7ae"
    ;;
  *)
    echo "Unsupported oasdiff platform: $(uname -s) $(uname -m)" >&2
    exit 1
    ;;
esac

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT

download_url="https://github.com/oasdiff/oasdiff/releases/download/v${version}/${archive}"
download_path="$temporary_dir/$archive"
curl --fail --location --proto '=https' --tlsv1.2 --silent --show-error \
  "$download_url" \
  --output "$download_path"

actual_sha256="$(sha256_file "$download_path")"
if [[ "$actual_sha256" != "$expected_sha256" ]]; then
  echo "oasdiff checksum mismatch: expected $expected_sha256, got $actual_sha256" >&2
  exit 1
fi

mkdir -p "$install_dir"
tar -xzf "$download_path" -C "$temporary_dir"
install -m 0755 "$temporary_dir/oasdiff" "$install_dir/oasdiff"

installed_version="$("$install_dir/oasdiff" --version)"
if [[ "$installed_version" != "oasdiff version $version" ]]; then
  echo "Unexpected installed oasdiff version: $installed_version" >&2
  exit 1
fi

printf '%s\n' "$install_dir/oasdiff"
