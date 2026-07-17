#!/usr/bin/env bash
set -euo pipefail

REPOSITORY_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf "$TEST_ROOT"' EXIT

SCRIPT_DIR="$TEST_ROOT/installed"
INSTALL_DIR="$SCRIPT_DIR"
REMOTE_DIR="$TEST_ROOT/remote"
export SCRIPT_BASE_URL="https://example.invalid/scripts"
mkdir -p "$INSTALL_DIR" "$REMOTE_DIR"

curl() {
  local output_path=""
  local url=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      -o)
        output_path="$2"
        shift 2
        ;;
      -*)
        shift
        ;;
      *)
        url="$1"
        shift
        ;;
    esac
  done

  [[ -n "$output_path" && -n "$url" ]]
  cp "$REMOTE_DIR/${url##*/}" "$output_path"
}

function_source="$(
  sed -n '/^install_management_script() {$/,/^}$/p' \
    "$REPOSITORY_ROOT/scripts/install-single-host.sh"
)"
[[ -n "$function_source" ]]
eval "$function_source"

management_scripts=(
  install-single-host.sh
  update-single-host.sh
  single-host-rollout.sh
  stop-single-host.sh
  uninstall-single-host.sh
)

for script_name in "${management_scripts[@]}"; do
  printf '#!/usr/bin/env bash\nprintf old\n' > "$INSTALL_DIR/$script_name"
  printf '#!/usr/bin/env bash\nprintf refreshed\n' > "$REMOTE_DIR/$script_name"
done

for script_name in "${management_scripts[@]}"; do
  install_management_script "$script_name"
  cmp "$REMOTE_DIR/$script_name" "$INSTALL_DIR/$script_name"
  [[ -x "$INSTALL_DIR/$script_name" ]]
done

echo "Management script refresh test passed"
