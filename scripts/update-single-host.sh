#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="/opt/hubuum"
ENGINE="auto"
SERVICE_NAME=""
USE_SYSTEMD="true"
AUTH_CONFIG_HOST_PATH=""

usage() {
  cat <<'EOF'
Usage:
  update-single-host.sh [--dir /opt/hubuum] [--engine auto|docker|podman]

Options:
  --dir PATH              Install directory. Default: /opt/hubuum
  --engine ENGINE         Container engine: auto, docker, or podman. Default: auto
  --auth-config PATH      Replace the host auth-provider TOML path before rolling the replicas
  --service-name NAME     systemd service name. Defaults to value in .env or hubuum
  --no-systemd            Retained for compatibility; rolling updates always use Compose directly
  -h, --help              Show this help
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

read_env_value() {
  local key="$1"
  local line

  [[ -f "$ENV_FILE" ]] || return 1
  line="$(grep -E "^${key}=" "$ENV_FILE" | tail -n 1 || true)"
  [[ -n "$line" ]] || return 1
  line="${line#*=}"
  line="${line%\"}"
  line="${line#\"}"
  printf '%s' "$line"
}

quote_env() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "$value"
}

absolute_config_path() {
  local path="$1"
  local directory

  [[ -f "$path" ]] || die "auth config is not a regular file: $path"
  [[ -r "$path" ]] || die "auth config is not readable: $path"
  directory="$(cd -- "$(dirname -- "$path")" && pwd -P)" || die "cannot resolve auth config path: $path"
  printf '%s/%s' "$directory" "$(basename -- "$path")"
}

set_env_value() {
  local key="$1"
  local value="$2"
  local temporary
  local found="false"
  local line

  temporary="$(mktemp "${ENV_FILE}.XXXXXX")"
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" == "${key}="* ]]; then
      if [[ "$found" == "false" ]]; then
        printf '%s=%s\n' "$key" "$(quote_env "$value")"
        found="true"
      fi
    else
      printf '%s\n' "$line"
    fi
  done < "$ENV_FILE" > "$temporary"
  if [[ "$found" == "false" ]]; then
    printf '%s=%s\n' "$key" "$(quote_env "$value")" >> "$temporary"
  fi
  chmod 0600 "$temporary"
  mv "$temporary" "$ENV_FILE"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir) INSTALL_DIR="$2"; shift 2 ;;
    --engine) ENGINE="$2"; shift 2 ;;
    --auth-config) AUTH_CONFIG_HOST_PATH="$2"; shift 2 ;;
    --service-name) SERVICE_NAME="$2"; shift 2 ;;
    --no-systemd) USE_SYSTEMD="false"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

[[ "$ENGINE" == "auto" || "$ENGINE" == "docker" || "$ENGINE" == "podman" ]] || die "--engine must be auto, docker, or podman"

if [[ "$EUID" -ne 0 ]]; then
  die "run as root, or via sudo"
fi

ENV_FILE="$INSTALL_DIR/.env"
[[ -f "$ENV_FILE" ]] || die "missing $ENV_FILE; run install-single-host.sh first"
[[ -f "$INSTALL_DIR/compose.yml" ]] || die "missing $INSTALL_DIR/compose.yml; run install-single-host.sh first"
[[ -f "$SCRIPT_DIR/single-host-rollout.sh" ]] || die "missing $SCRIPT_DIR/single-host-rollout.sh; re-run install-single-host.sh first"
grep -q '^  hubuum-api-standby:' "$INSTALL_DIR/compose.yml" || die "installed compose.yml does not support rolling updates; re-run install-single-host.sh first"

if [[ -n "$AUTH_CONFIG_HOST_PATH" ]]; then
  grep -q 'HUBUUM_AUTH_CONFIG_PATH' "$INSTALL_DIR/compose.yml" || die "installed compose.yml does not support auth configuration; re-run install-single-host.sh first"
  AUTH_CONFIG_HOST_PATH="$(absolute_config_path "$AUTH_CONFIG_HOST_PATH")"
  set_env_value HUBUUM_AUTH_CONFIG_HOST_PATH "$AUTH_CONFIG_HOST_PATH"
fi

if grep -q 'HUBUUM_AUTH_CONFIG_PATH' "$INSTALL_DIR/compose.yml"; then
  AUTH_CONFIG_HOST_PATH="$(read_env_value HUBUUM_AUTH_CONFIG_HOST_PATH || true)"
  [[ -n "$AUTH_CONFIG_HOST_PATH" ]] || die "HUBUUM_AUTH_CONFIG_HOST_PATH is missing from $ENV_FILE"
  absolute_config_path "$AUTH_CONFIG_HOST_PATH" >/dev/null
fi

BUILD_FROM_SOURCE="$(read_env_value BUILD_FROM_SOURCE || printf 'false')"
INSTALL_MODE="$(read_env_value INSTALL_MODE || printf 'all')"
DATABASE_MANAGED="$(read_env_value DATABASE_MANAGED || printf 'true')"
if [[ -z "$SERVICE_NAME" ]]; then
  SERVICE_NAME="$(read_env_value SYSTEMD_SERVICE_NAME || printf 'hubuum')"
fi
SERVICE_NAME="${SERVICE_NAME%.service}"
if [[ "$ENGINE" == "auto" ]]; then
  ENGINE="$(read_env_value CONTAINER_ENGINE || printf 'auto')"
fi

detect_engine() {
  if [[ "$ENGINE" == "docker" || "$ENGINE" == "auto" ]]; then
    if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
      ENGINE_BIN="docker"
      return 0
    fi

    [[ "$ENGINE" == "auto" ]] || die "docker compose plugin is required"
  fi

  if [[ "$ENGINE" == "podman" || "$ENGINE" == "auto" ]]; then
    if command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
      ENGINE_BIN="podman"
      return 0
    fi

    [[ "$ENGINE" == "auto" ]] || die "podman compose is required"
  fi

  die "no supported compose engine found; install docker compose or podman compose"
}

update_source_checkout() {
  local dest="$1"

  [[ -d "$dest/.git" ]] || return 0
  git -C "$dest" fetch --tags --prune origin
  git -C "$dest" pull --ff-only || true
}

detect_engine
ENGINE_PATH="$(command -v "$ENGINE_BIN")"
COMPOSE_CMD=("$ENGINE_PATH" compose --env-file .env -f compose.yml)

cd "$INSTALL_DIR"

if [[ "$BUILD_FROM_SOURCE" == "true" ]]; then
  command -v git >/dev/null 2>&1 || die "git is required for source-build updates"
  update_source_checkout "$INSTALL_DIR/src/hubuum"
  if [[ "$INSTALL_MODE" == "all" ]]; then
    update_source_checkout "$INSTALL_DIR/src/hubuum-frontend"
  fi

  PULL_SERVICES=(caddy)
  [[ "$DATABASE_MANAGED" == "true" ]] && PULL_SERVICES+=(postgres)
  [[ "$INSTALL_MODE" == "all" ]] && PULL_SERVICES+=(valkey)
  "${COMPOSE_CMD[@]}" pull "${PULL_SERVICES[@]}"
  "${COMPOSE_CMD[@]}" build --pull
else
  "${COMPOSE_CMD[@]}" pull
fi

# shellcheck source=scripts/single-host-rollout.sh
source "$SCRIPT_DIR/single-host-rollout.sh"
hubuum_rollout false

if [[ "$USE_SYSTEMD" == "true" && -d /run/systemd/system && "$(command -v systemctl || true)" ]] && systemctl cat "${SERVICE_NAME}.service" >/dev/null 2>&1; then
  echo "Hubuum rolled via ${ENGINE_BIN} compose; ${SERVICE_NAME}.service remained active"
else
  echo "Hubuum rolled via ${ENGINE_BIN} compose"
fi
