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

installer_preamble="$(
  sed -n '1,/^INSTALL_DIR=/p' "$REPOSITORY_ROOT/scripts/install-single-host.sh"
)"
piped_output="$(printf '%s\n' "$installer_preamble" | bash 2>&1)"
[[ -z "$piped_output" ]] || {
  printf 'installer preamble emitted output under curl-style execution:\n%s\n' "$piped_output" >&2
  exit 1
}

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

merge_function_source="$(
  sed -n '/^merge_missing_env_values() {$/,/^}$/p' \
    "$REPOSITORY_ROOT/scripts/install-single-host.sh"
)"
[[ -n "$merge_function_source" ]]
eval "$merge_function_source"

ENV_FILE="$INSTALL_DIR/.env"
GENERATED_ENV="$TEST_ROOT/generated.env"
cat > "$ENV_FILE" <<'EOF'
INSTALL_MODE=all
HUBUUM_LOG_LEVEL=debug
OPERATOR_CUSTOM_SETTING=preserved
EOF
cat > "$GENERATED_ENV" <<'EOF'
INSTALL_MODE=backend
HUBUUM_LOG_LEVEL=info
MANAGEMENT_SCRIPT_BASE_URL=https://example.invalid/scripts
NEW_GENERATED_DEFAULT=present
EOF

merge_missing_env_values "$GENERATED_ENV"

[[ "$(grep -c '^INSTALL_MODE=' "$ENV_FILE")" -eq 1 ]]
[[ "$(grep -c '^HUBUUM_LOG_LEVEL=' "$ENV_FILE")" -eq 1 ]]
grep -qx 'INSTALL_MODE=all' "$ENV_FILE"
grep -qx 'HUBUUM_LOG_LEVEL=debug' "$ENV_FILE"
grep -qx 'OPERATOR_CUSTOM_SETTING=preserved' "$ENV_FILE"
grep -qx 'MANAGEMENT_SCRIPT_BASE_URL=https://example.invalid/scripts' "$ENV_FILE"
grep -qx 'NEW_GENERATED_DEFAULT=present' "$ENV_FILE"

refresh_function_source="$(
  sed -n '/^refresh_deployment_files() {$/,/^}$/p' \
    "$REPOSITORY_ROOT/scripts/update-single-host.sh"
)"
[[ -n "$refresh_function_source" ]]
eval "$refresh_function_source"

BASH_LOG="$TEST_ROOT/bash.log"
BUILD_FROM_SOURCE="false"
ENGINE_BIN="docker"
MANAGEMENT_SCRIPT_BASE_URL="$SCRIPT_BASE_URL"
bash() {
  if [[ "${1:-}" == "-n" ]]; then
    return 0
  fi
  printf '%s\n' "$*" >> "$BASH_LOG"
}

refresh_deployment_files

grep -q -- "--refresh-config --dir $INSTALL_DIR --engine docker" "$BASH_LOG"
grep -q -- "--script-base-url $SCRIPT_BASE_URL" "$BASH_LOG"
if find "$INSTALL_DIR" -maxdepth 1 -name '.install-single-host.*' | grep -q .; then
  echo "temporary refreshed installer was not removed" >&2
  exit 1
fi

echo "Management script refresh test passed"
