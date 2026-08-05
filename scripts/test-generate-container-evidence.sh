#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
generator="$repo_root/scripts/generate-container-evidence.sh"
digest="$(printf 'a%.0s' {1..64})"
revision="$(printf 'b%.0s' {1..40})"

assert_rejected() {
  if HUBUUM_EVIDENCE_DIR="${TMPDIR:-/tmp}/hubuum-invalid-evidence" \
    bash "$generator" "$@" >/dev/null 2>&1; then
    echo "container evidence input was unexpectedly accepted: $*" >&2
    exit 1
  fi
}

assert_rejected "example.invalid/hubuum:latest" output "$revision" main linux-amd64
assert_rejected "example.invalid/hubuum@sha256:$digest" ../escape "$revision" main linux-amd64
assert_rejected "example.invalid/hubuum@sha256:$digest" output short main linux-amd64
assert_rejected "example.invalid/hubuum@sha256:$digest" output "$revision" latest linux-amd64
assert_rejected "example.invalid/hubuum@sha256:$digest" output "$revision" main linux-s390x

test_tmp="$(mktemp -d "${TMPDIR:-/tmp}/hubuum-container-evidence.XXXXXX")"
trap 'rm -rf "$test_tmp"' EXIT
mock_bin="$test_tmp/bin"
evidence_dir="$test_tmp/evidence"
docker_log="$test_tmp/docker.log"
mkdir -p "$mock_bin" "$evidence_dir"

cat > "$mock_bin/python3" <<'PYTHON'
#!/usr/bin/env bash
set -euo pipefail

case "$*" in
  *"--tool-value SYFT_IMAGE"*)
    echo mock-syft
    ;;
  *"--tool-value TRIVY_IMAGE"*)
    echo mock-trivy
    ;;
  *"generate-release-sbom.py"*)
    output=""
    while (($# > 0)); do
      if [[ "$1" == "--output" ]]; then
        output="$2"
        break
      fi
      shift
    done
    test -n "$output"
    printf '{}\n' > "$output"
    ;;
  *)
    echo "unexpected python3 invocation: $*" >&2
    exit 1
    ;;
esac
PYTHON

cat > "$mock_bin/docker" <<'DOCKER'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "$MOCK_DOCKER_LOG"

if [[ "$1 $2" == "image inspect" ]]; then
  echo sha256:mock-platform-image
  exit 0
fi

if [[ " $* " == *" mock-trivy image "* ]]; then
  if [[ " $* " != *" --platform linux/arm64 "* ]]; then
    echo "Trivy image scan omitted the requested platform: $*" >&2
    exit 1
  fi

  output=""
  while (($# > 0)); do
    if [[ "$1" == "--output" ]]; then
      output="$2"
      break
    fi
    shift
  done
  test -n "$output"
  printf '{}\n' > "$MOCK_EVIDENCE_DIR/${output##*/}"
  exit 0
fi

case " $* " in
  *" mock-syft registry:"*)
    printf '{}\n'
    ;;
  *" mock-trivy sbom "*)
    printf '{}\n'
    ;;
  *" mock-syft version "*)
    echo "syft mock"
    ;;
  *" mock-trivy --version "*)
    echo "trivy mock"
    ;;
  *)
    echo "unexpected docker invocation: $*" >&2
    exit 1
    ;;
esac
DOCKER

chmod +x "$mock_bin/python3" "$mock_bin/docker"
PATH="$mock_bin:$PATH" \
  MOCK_DOCKER_LOG="$docker_log" \
  MOCK_EVIDENCE_DIR="$evidence_dir" \
  HUBUUM_EVIDENCE_DIR="$evidence_dir" \
  bash "$generator" \
  "example.invalid/hubuum@sha256:$digest" \
  output \
  "$revision" \
  main \
  linux-arm64 >/dev/null

if [[ "$(grep -c -- '--platform linux/arm64' "$docker_log")" != 2 ]]; then
  echo "both Trivy image scans must select the requested image platform" >&2
  exit 1
fi

echo "Container evidence input validation tests passed."
