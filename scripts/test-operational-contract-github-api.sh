#!/usr/bin/env bash
set -euo pipefail

output=""
url=""
while (( $# > 0 )); do
  case "$1" in
    --output)
      output="$2"
      shift 2
      ;;
    http://* | https://*)
      url="$1"
      shift
      ;;
    *)
      shift
      ;;
  esac
done

if [[ -z "$output" || -z "$url" ]]; then
  echo "mock GitHub API requires an output path and URL" >&2
  exit 2
fi

case "$url" in
  */releases\?*)
    cp "$HUBUUM_TEST_RELEASES_FILE" "$output"
    printf '200'
    ;;
  */contents/docs/operational-contract.json\?*)
    status="${HUBUUM_TEST_CONTENTS_STATUS:-404}"
    if [[ "$status" == "200" ]]; then
      cp "$HUBUUM_TEST_BASELINE_FILE" "$output"
    else
      : > "$output"
    fi
    printf '%s' "$status"
    ;;
  *)
    echo "unexpected mock GitHub API URL: $url" >&2
    exit 2
    ;;
esac
