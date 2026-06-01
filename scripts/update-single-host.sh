#!/usr/bin/env bash
set -Eeuo pipefail

INSTALL_DIR="/opt/hubuum"
ENGINE="auto"
SERVICE_NAME=""
USE_SYSTEMD="true"

usage() {
  cat <<'EOF'
Usage:
  update-single-host.sh [--dir /opt/hubuum] [--engine auto|docker|podman]

Options:
  --dir PATH              Install directory. Default: /opt/hubuum
  --engine ENGINE         Container engine: auto, docker, or podman. Default: auto
  --service-name NAME     systemd service name. Defaults to value in .env or hubuum
  --no-systemd            Restart with compose directly even if a systemd service exists
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

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir) INSTALL_DIR="$2"; shift 2 ;;
    --engine) ENGINE="$2"; shift 2 ;;
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

if [[ "$USE_SYSTEMD" == "true" && -d /run/systemd/system && "$(command -v systemctl || true)" ]] && systemctl cat "${SERVICE_NAME}.service" >/dev/null 2>&1; then
  systemctl restart "$SERVICE_NAME"
  echo "Hubuum updated and restarted via ${SERVICE_NAME}.service"
else
  "${COMPOSE_CMD[@]}" up -d
  echo "Hubuum updated and restarted via ${ENGINE_BIN} compose"
fi
