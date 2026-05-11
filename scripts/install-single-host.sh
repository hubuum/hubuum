#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="/opt/hubuum"
MODE="all"
WEB_FQDN=""
API_FQDN=""
LETSENCRYPT_EMAIL=""
BACKEND_REF="main"
FRONTEND_REF="main"
BACKEND_REPO="https://github.com/hubuum/hubuum.git"
FRONTEND_REPO="https://github.com/hubuum/hubuum-frontend.git"
BACKEND_IMAGE="ghcr.io/hubuum/hubuum-server:main"
FRONTEND_IMAGE="ghcr.io/hubuum/hubuum-frontend:main"
POSTGRES_IMAGE="docker.io/library/postgres:18-alpine"
VALKEY_IMAGE="docker.io/valkey/valkey:9-alpine"
CADDY_IMAGE="docker.io/library/caddy:2-alpine"
EXTERNAL_DATABASE_URL=""
NETWORK_SUBNET="172.30.42.0/24"
RECREATE="false"
PULL="true"
ENGINE="auto"
ENGINE_BIN=""
ENGINE_PATH=""
BUILD_FROM_SOURCE="false"
INSTALL_SYSTEMD="false"
SERVICE_NAME="hubuum"
ACTION="install"
PURGE="false"
SERVICE_NAME_SET="false"
SCRIPT_BASE_URL="https://raw.githubusercontent.com/hubuum/hubuum/main/scripts"
SCRIPT_REF=""

usage() {
  cat <<'EOF'
Usage:
  install-single-host.sh --web hubuum.example.com --api hubuum-api.example.com --email admin@example.com
  install-single-host.sh --mode backend --api hubuum-api.example.com --email admin@example.com
  curl -fsSL https://raw.githubusercontent.com/hubuum/hubuum/main/scripts/install-single-host.sh | sudo bash -s -- --web hubuum.example.com --api hubuum-api.example.com --email admin@example.com

Options:
  --stop                  Stop the installed stack and exit
  --uninstall             Stop the stack, remove systemd unit if present, and exit
  --purge                 With --uninstall, also remove compose volumes and install directory
  --mode MODE             Install mode: all or backend. Default: all
  --dir PATH              Install directory. Default: /opt/hubuum
  --web FQDN              Public frontend hostname. Required in all mode
  --api FQDN              Public backend API hostname. Required
  --email EMAIL           Let's Encrypt registration email. Required
  --backend-image IMAGE   Backend image. Default: ghcr.io/hubuum/hubuum-server:main
  --frontend-image IMAGE  Frontend image. Default: ghcr.io/hubuum/hubuum-frontend:main
  --database-url URL      Existing Postgres URL. If set, no Postgres container is created
  --engine ENGINE         Container engine: auto, docker, or podman. Default: auto
  --postgres-image IMAGE  Postgres image. Default: docker.io/library/postgres:18-alpine
  --valkey-image IMAGE    Valkey image. Default: docker.io/valkey/valkey:9-alpine
  --caddy-image IMAGE     Caddy image. Default: docker.io/library/caddy:2-alpine
  --network-subnet CIDR   Container bridge subnet. Default: 172.30.42.0/24
  --systemd               Install and enable a systemd service
  --service-name NAME     systemd service name. Default: hubuum
  --no-systemd            Do not install or enable a systemd service. Default
  --script-base-url URL   Base URL for management scripts when installing via curl
  --script-ref REF        Git ref used to derive raw GitHub management script URLs
  --build-from-source     Clone repositories and build app images locally
  --backend-ref REF       Source build backend Git ref. Default: main
  --frontend-ref REF      Source build frontend Git ref. Default: main
  --backend-repo URL      Source build backend Git repository
  --frontend-repo URL     Source build frontend Git repository
  --no-pull               Do not pull dependency/base images before starting
  --recreate              Regenerate .env secrets even if they exist
  -h, --help              Show this help
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

quote_env() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "$value"
}

random_hex() {
  openssl rand -hex "$1" | tr -d '\n'
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
    --mode) MODE="$2"; shift 2 ;;
    --stop) ACTION="stop"; shift ;;
    --uninstall) ACTION="uninstall"; shift ;;
    --purge) PURGE="true"; shift ;;
    --dir) INSTALL_DIR="$2"; shift 2 ;;
    --web) WEB_FQDN="$2"; shift 2 ;;
    --api) API_FQDN="$2"; shift 2 ;;
    --email) LETSENCRYPT_EMAIL="$2"; shift 2 ;;
    --backend-image) BACKEND_IMAGE="$2"; shift 2 ;;
    --frontend-image) FRONTEND_IMAGE="$2"; shift 2 ;;
    --backend-ref) BACKEND_REF="$2"; shift 2 ;;
    --frontend-ref) FRONTEND_REF="$2"; shift 2 ;;
    --backend-repo) BACKEND_REPO="$2"; shift 2 ;;
    --frontend-repo) FRONTEND_REPO="$2"; shift 2 ;;
    --database-url) EXTERNAL_DATABASE_URL="$2"; shift 2 ;;
    --engine) ENGINE="$2"; shift 2 ;;
    --postgres-image) POSTGRES_IMAGE="$2"; shift 2 ;;
    --valkey-image) VALKEY_IMAGE="$2"; shift 2 ;;
    --caddy-image) CADDY_IMAGE="$2"; shift 2 ;;
    --network-subnet) NETWORK_SUBNET="$2"; shift 2 ;;
    --systemd) INSTALL_SYSTEMD="true"; shift ;;
    --service-name) SERVICE_NAME="$2"; SERVICE_NAME_SET="true"; shift 2 ;;
    --no-systemd) INSTALL_SYSTEMD="false"; shift ;;
    --script-base-url) SCRIPT_BASE_URL="${2%/}"; shift 2 ;;
    --script-ref) SCRIPT_REF="$2"; shift 2 ;;
    --build-from-source) BUILD_FROM_SOURCE="true"; shift ;;
    --no-pull) PULL="false"; shift ;;
    --recreate) RECREATE="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

[[ "$MODE" == "all" || "$MODE" == "backend" ]] || die "--mode must be all or backend"
[[ "$ENGINE" == "auto" || "$ENGINE" == "docker" || "$ENGINE" == "podman" ]] || die "--engine must be auto, docker, or podman"
SERVICE_NAME="${SERVICE_NAME%.service}"
if [[ "$PURGE" == "true" && "$ACTION" != "uninstall" ]]; then
  die "--purge can only be used with --uninstall"
fi
if [[ -n "$SCRIPT_REF" ]]; then
  SCRIPT_BASE_URL="https://raw.githubusercontent.com/hubuum/hubuum/${SCRIPT_REF}/scripts"
fi
if [[ "$ACTION" == "install" && -z "$API_FQDN" ]]; then
  usage
  exit 2
fi
if [[ "$ACTION" == "install" && -z "$LETSENCRYPT_EMAIL" ]]; then
  usage
  exit 2
fi
if [[ "$ACTION" == "install" && "$MODE" == "all" && -z "$WEB_FQDN" ]]; then
  usage
  exit 2
fi
if [[ "$EUID" -ne 0 ]]; then
  die "run as root, or via sudo"
fi

ENV_FILE="$INSTALL_DIR/.env"

detect_engine() {
  if [[ "$ENGINE" == "docker" || "$ENGINE" == "auto" ]]; then
    if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
      ENGINE="docker"
      ENGINE_BIN="docker"
      return 0
    fi

    [[ "$ENGINE" == "auto" ]] || die "docker compose plugin is required"
  fi

  if [[ "$ENGINE" == "podman" || "$ENGINE" == "auto" ]]; then
    if command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
      ENGINE="podman"
      ENGINE_BIN="podman"
      return 0
    fi

    [[ "$ENGINE" == "auto" ]] || die "podman compose is required"
  fi

  die "no supported compose engine found; install docker compose or podman compose"
}

load_existing_runtime() {
  [[ -f "$ENV_FILE" ]] || return 0

  if [[ "$ENGINE" == "auto" ]]; then
    ENGINE="$(read_env_value CONTAINER_ENGINE || printf 'auto')"
  fi
  if [[ "$SERVICE_NAME_SET" != "true" ]]; then
    SERVICE_NAME="$(read_env_value SYSTEMD_SERVICE_NAME || printf '%s' "$SERVICE_NAME")"
    SERVICE_NAME="${SERVICE_NAME%.service}"
  fi
}

systemd_available() {
  [[ -d /run/systemd/system ]] && command -v systemctl >/dev/null 2>&1
}

systemd_unit_exists() {
  systemd_available && systemctl cat "${SERVICE_NAME}.service" >/dev/null 2>&1
}

compose_down() {
  local purge_flag="${1:-false}"

  [[ -f "$INSTALL_DIR/compose.yml" ]] || return 0
  cd "$INSTALL_DIR"
  if [[ "$purge_flag" == "true" ]]; then
    "${COMPOSE_CMD[@]}" down -v --remove-orphans
  else
    "${COMPOSE_CMD[@]}" down --remove-orphans
  fi
}

stop_stack() {
  if systemd_unit_exists; then
    systemctl stop "$SERVICE_NAME"
  else
    compose_down false
  fi
}

uninstall_stack() {
  if systemd_unit_exists; then
    systemctl stop "$SERVICE_NAME" || true
    systemctl disable "$SERVICE_NAME" || true
    rm -f "/etc/systemd/system/${SERVICE_NAME}.service"
    systemctl daemon-reload
  fi

  compose_down "$PURGE"

  if [[ "$PURGE" == "true" ]]; then
    rm -rf "$INSTALL_DIR"
  fi
}

load_existing_runtime
detect_engine
ENGINE_PATH="$(command -v "$ENGINE_BIN")"
COMPOSE_CMD=("$ENGINE_PATH" compose --env-file .env -f compose.yml)

if [[ "$ACTION" == "stop" ]]; then
  stop_stack
  echo "Hubuum stack stopped."
  exit 0
fi

if [[ "$ACTION" == "uninstall" ]]; then
  uninstall_stack
  if [[ "$PURGE" == "true" ]]; then
    echo "Hubuum stack uninstalled and install directory removed."
  else
    echo "Hubuum stack uninstalled. Install files and volumes were preserved."
  fi
  exit 0
fi

need_cmd openssl
if [[ "$BUILD_FROM_SOURCE" == "true" ]]; then
  need_cmd git
fi

mkdir -p "$INSTALL_DIR"
if [[ "$BUILD_FROM_SOURCE" == "true" ]]; then
  mkdir -p "$INSTALL_DIR/src"
fi

install_management_script() {
  local script_name="$1"
  local local_path="$SCRIPT_DIR/$script_name"
  local dest_path="$INSTALL_DIR/$script_name"

  if [[ -f "$local_path" && "$local_path" != "$dest_path" ]]; then
    install -m 0755 "$local_path" "$dest_path"
    return 0
  elif [[ -f "$dest_path" ]]; then
    chmod 0755 "$dest_path"
    return 0
  fi

  if command -v curl >/dev/null 2>&1; then
    if curl -fsSL "${SCRIPT_BASE_URL}/${script_name}" -o "$dest_path"; then
      chmod 0755 "$dest_path"
      return 0
    fi
  fi

  echo "WARNING: could not install $script_name; curl is unavailable and no local script was found" >&2
}

install_management_script install-single-host.sh
install_management_script update-single-host.sh
install_management_script stop-single-host.sh
install_management_script uninstall-single-host.sh

clone_or_update() {
  local repo_url="$1"
  local dest="$2"
  local ref="$3"

  if [[ -e "$dest" && ! -d "$dest/.git" ]]; then
    die "$dest exists but is not a Git checkout"
  fi

  if [[ ! -d "$dest/.git" ]]; then
    git clone "$repo_url" "$dest"
  fi

  git -C "$dest" remote set-url origin "$repo_url"
  git -C "$dest" fetch --tags --prune origin

  if git -C "$dest" show-ref --verify --quiet "refs/remotes/origin/$ref"; then
    git -C "$dest" checkout -B "$ref" "origin/$ref"
  else
    git -C "$dest" checkout "$ref"
  fi
}

if [[ "$BUILD_FROM_SOURCE" == "true" ]]; then
  clone_or_update "$BACKEND_REPO" "$INSTALL_DIR/src/hubuum" "$BACKEND_REF"
  if [[ "$MODE" == "all" ]]; then
    clone_or_update "$FRONTEND_REPO" "$INSTALL_DIR/src/hubuum-frontend" "$FRONTEND_REF"
  fi
fi

ENV_FILE="$INSTALL_DIR/.env"

POSTGRES_PASSWORD=""
HUBUUM_TOKEN_HASH_KEY=""
if [[ "$RECREATE" != "true" ]]; then
  if [[ -z "$EXTERNAL_DATABASE_URL" && "$(read_env_value DATABASE_MANAGED || true)" == "false" ]]; then
    EXTERNAL_DATABASE_URL="$(read_env_value HUBUUM_DATABASE_URL || true)"
  fi
  POSTGRES_PASSWORD="$(read_env_value POSTGRES_PASSWORD || true)"
  HUBUUM_TOKEN_HASH_KEY="$(read_env_value HUBUUM_TOKEN_HASH_KEY || true)"
fi

[[ -n "$POSTGRES_PASSWORD" ]] || POSTGRES_PASSWORD="$(random_hex 32)"
[[ -n "$HUBUUM_TOKEN_HASH_KEY" ]] || HUBUUM_TOKEN_HASH_KEY="$(random_hex 32)"

DATABASE_MANAGED="true"
HUBUUM_DATABASE_URL="postgres://hubuum:${POSTGRES_PASSWORD}@postgres:5432/hubuum"
if [[ -n "$EXTERNAL_DATABASE_URL" ]]; then
  DATABASE_MANAGED="false"
  HUBUUM_DATABASE_URL="$EXTERNAL_DATABASE_URL"
fi

{
  printf 'INSTALL_MODE=%s\n' "$MODE"
  printf 'WEB_FQDN=%s\n' "$WEB_FQDN"
  printf 'API_FQDN=%s\n' "$API_FQDN"
  printf 'LETSENCRYPT_EMAIL=%s\n' "$LETSENCRYPT_EMAIL"
  printf 'BUILD_FROM_SOURCE=%s\n' "$BUILD_FROM_SOURCE"
  printf 'CONTAINER_ENGINE=%s\n' "$ENGINE_BIN"
  printf 'SYSTEMD_SERVICE_NAME=%s\n' "$SERVICE_NAME"
  printf '\n'
  printf 'BACKEND_IMAGE=%s\n' "$BACKEND_IMAGE"
  printf 'FRONTEND_IMAGE=%s\n' "$FRONTEND_IMAGE"
  printf 'BACKEND_REF=%s\n' "$BACKEND_REF"
  printf 'FRONTEND_REF=%s\n' "$FRONTEND_REF"
  printf 'BACKEND_REPO=%s\n' "$BACKEND_REPO"
  printf 'FRONTEND_REPO=%s\n' "$FRONTEND_REPO"
  printf 'POSTGRES_IMAGE=%s\n' "$POSTGRES_IMAGE"
  printf 'VALKEY_IMAGE=%s\n' "$VALKEY_IMAGE"
  printf 'CADDY_IMAGE=%s\n' "$CADDY_IMAGE"
  printf 'DATABASE_MANAGED=%s\n' "$DATABASE_MANAGED"
  printf 'POSTGRES_DB=hubuum\n'
  printf 'POSTGRES_USER=hubuum\n'
  printf 'POSTGRES_PASSWORD=%s\n' "$POSTGRES_PASSWORD"
  printf '\n'
  printf 'HUBUUM_DATABASE_URL=%s\n' "$(quote_env "$HUBUUM_DATABASE_URL")"
  printf 'DATABASE_URL=%s\n' "$(quote_env "$HUBUUM_DATABASE_URL")"
  printf 'HUBUUM_BIND_IP=0.0.0.0\n'
  printf 'HUBUUM_BIND_PORT=8080\n'
  printf 'HUBUUM_LOG_LEVEL=info\n'
  printf 'HUBUUM_TOKEN_HASH_KEY=%s\n' "$HUBUUM_TOKEN_HASH_KEY"
  printf 'HUBUUM_CLIENT_ALLOWLIST=%s\n' "$NETWORK_SUBNET"
  printf 'HUBUUM_TRUST_IP_HEADERS=false\n'
  printf 'HUBUUM_TOKEN_LIFETIME_HOURS=24\n'
  printf 'HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS=5\n'
  printf 'HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS=300\n'
  printf '\n'
  printf 'BACKEND_BASE_URL=http://hubuum-api:8080\n'
  printf 'VALKEY_URL=redis://valkey:6379/0\n'
  printf 'SESSION_TTL_SECONDS=28800\n'
  printf 'SESSION_PREFIX=hubuum:sess:\n'
  printf 'NEXT_PUBLIC_APP_NAME=%s\n' "$(quote_env "Hubuum Console")"
} > "$ENV_FILE"

chmod 0600 "$ENV_FILE"

if [[ "$MODE" == "all" ]]; then
  cat > "$INSTALL_DIR/Caddyfile" <<'EOF'
{
    email {$LETSENCRYPT_EMAIL}
}

{$WEB_FQDN} {
    encode zstd gzip
    reverse_proxy hubuum-web:3000
}

{$API_FQDN} {
    encode zstd gzip
    reverse_proxy hubuum-api:8080
}
EOF
else
  cat > "$INSTALL_DIR/Caddyfile" <<'EOF'
{
    email {$LETSENCRYPT_EMAIL}
}

{$API_FQDN} {
    encode zstd gzip
    reverse_proxy hubuum-api:8080
}
EOF
fi

cat > "$INSTALL_DIR/compose.yml" <<'EOF'
services:
EOF

if [[ "$DATABASE_MANAGED" == "true" ]]; then
  cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
  postgres:
    image: ${POSTGRES_IMAGE}
    container_name: hubuum-postgres
    restart: unless-stopped
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      PGUSER: ${POSTGRES_USER}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - hubuum_net
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 10s
      timeout: 5s
      retries: 10

EOF
fi

cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
  hubuum-api:
EOF

if [[ "$BUILD_FROM_SOURCE" == "true" ]]; then
  cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
    build:
      context: ./src/hubuum
    image: local/hubuum-api:single-host
EOF
else
  cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
    image: ${BACKEND_IMAGE}
EOF
fi

cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
    container_name: hubuum-api
    restart: unless-stopped
    environment:
      HUBUUM_BIND_IP: ${HUBUUM_BIND_IP}
      HUBUUM_BIND_PORT: ${HUBUUM_BIND_PORT}
      HUBUUM_DATABASE_URL: ${HUBUUM_DATABASE_URL}
      DATABASE_URL: ${DATABASE_URL}
      HUBUUM_LOG_LEVEL: ${HUBUUM_LOG_LEVEL}
      HUBUUM_TOKEN_HASH_KEY: ${HUBUUM_TOKEN_HASH_KEY}
      HUBUUM_CLIENT_ALLOWLIST: ${HUBUUM_CLIENT_ALLOWLIST}
      HUBUUM_TRUST_IP_HEADERS: ${HUBUUM_TRUST_IP_HEADERS}
      HUBUUM_TOKEN_LIFETIME_HOURS: ${HUBUUM_TOKEN_LIFETIME_HOURS}
      HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS: ${HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS}
      HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS: ${HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS}
EOF

if [[ "$DATABASE_MANAGED" == "true" ]]; then
  cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
    depends_on:
      - postgres
EOF
fi

cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
    expose:
      - "8080"
    networks:
      - hubuum_net
EOF

if [[ "$MODE" == "all" ]]; then
  cat >> "$INSTALL_DIR/compose.yml" <<'EOF'

  valkey:
    image: ${VALKEY_IMAGE}
    container_name: hubuum-valkey
    restart: unless-stopped
    command: ["valkey-server", "--appendonly", "yes"]
    volumes:
      - valkey_data:/data
    networks:
      - hubuum_net
    healthcheck:
      test: ["CMD", "valkey-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 10

  hubuum-web:
EOF

  if [[ "$BUILD_FROM_SOURCE" == "true" ]]; then
    cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
    build:
      context: ./src/hubuum-frontend
    image: local/hubuum-web:single-host
EOF
  else
    cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
    image: ${FRONTEND_IMAGE}
EOF
  fi

  cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
    container_name: hubuum-web
    restart: unless-stopped
    environment:
      NODE_ENV: production
      PORT: 3000
      HOSTNAME: 0.0.0.0
      BACKEND_BASE_URL: ${BACKEND_BASE_URL}
      VALKEY_URL: ${VALKEY_URL}
      SESSION_TTL_SECONDS: ${SESSION_TTL_SECONDS}
      SESSION_PREFIX: ${SESSION_PREFIX}
      NEXT_PUBLIC_APP_NAME: ${NEXT_PUBLIC_APP_NAME}
    depends_on:
      - hubuum-api
      - valkey
    expose:
      - "3000"
    networks:
      - hubuum_net
EOF
fi

cat >> "$INSTALL_DIR/compose.yml" <<'EOF'

  caddy:
    image: ${CADDY_IMAGE}
    container_name: hubuum-caddy
    restart: unless-stopped
    env_file:
      - .env
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy_data:/data
      - caddy_config:/config
    depends_on:
      - hubuum-api
EOF

if [[ "$MODE" == "all" ]]; then
  cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
      - hubuum-web
EOF
fi

cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
    networks:
      - hubuum_net

volumes:
EOF

if [[ "$DATABASE_MANAGED" == "true" ]]; then
  cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
  postgres_data:
EOF
fi

if [[ "$MODE" == "all" ]]; then
  cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
  valkey_data:
EOF
fi

cat >> "$INSTALL_DIR/compose.yml" <<EOF
  caddy_data:
  caddy_config:

networks:
  hubuum_net:
    driver: bridge
    ipam:
      config:
        - subnet: ${NETWORK_SUBNET}
EOF

cd "$INSTALL_DIR"

if [[ "$PULL" == "true" ]]; then
  PULL_SERVICES=(caddy)
  [[ "$BUILD_FROM_SOURCE" != "true" ]] && PULL_SERVICES+=(hubuum-api)
  [[ "$BUILD_FROM_SOURCE" != "true" && "$MODE" == "all" ]] && PULL_SERVICES+=(hubuum-web)
  [[ "$DATABASE_MANAGED" == "true" ]] && PULL_SERVICES+=(postgres)
  [[ "$MODE" == "all" ]] && PULL_SERVICES+=(valkey)
  "${COMPOSE_CMD[@]}" pull "${PULL_SERVICES[@]}"
fi

if [[ "$BUILD_FROM_SOURCE" == "true" ]]; then
  "${COMPOSE_CMD[@]}" build --pull
fi

install_systemd_unit() {
  local unit_path="/etc/systemd/system/${SERVICE_NAME}.service"

  cat > "$unit_path" <<EOF
[Unit]
Description=Hubuum container stack
Wants=network-online.target
After=network-online.target docker.service podman.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=${INSTALL_DIR}
ExecStart=${ENGINE_PATH} compose --env-file .env -f compose.yml up -d
ExecStop=${ENGINE_PATH} compose --env-file .env -f compose.yml down
TimeoutStartSec=0
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable "$SERVICE_NAME"
  systemctl restart "$SERVICE_NAME"
}

SYSTEMD_STATUS="not installed"
if [[ "$INSTALL_SYSTEMD" == "true" && -d /run/systemd/system && "$(command -v systemctl || true)" ]]; then
  install_systemd_unit
  SYSTEMD_STATUS="enabled as ${SERVICE_NAME}.service"
else
  "${COMPOSE_CMD[@]}" up -d
  if [[ "$INSTALL_SYSTEMD" == "true" ]]; then
    SYSTEMD_STATUS="skipped; systemd is not available"
  fi
fi

cat <<EOF

Hubuum ${MODE} stack started.

Container engine:
  ${ENGINE_BIN} compose

Boot service:
  ${SYSTEMD_STATUS}

Image source:
  $([[ "$BUILD_FROM_SOURCE" == "true" ]] && printf 'local source builds' || printf 'published container images')

Backend API:
  https://${API_FQDN}
EOF

if [[ "$MODE" == "all" ]]; then
  cat <<EOF

Frontend:
  https://${WEB_FQDN}
EOF
fi

cat <<EOF

Useful commands:
  cd ${INSTALL_DIR}
  ./update-single-host.sh
  ${ENGINE_BIN} compose --env-file .env -f compose.yml ps
  ${ENGINE_BIN} compose --env-file .env -f compose.yml logs -f hubuum-api
EOF

if [[ "$MODE" == "all" ]]; then
  cat <<EOF
  ${ENGINE_BIN} compose --env-file .env -f compose.yml logs -f hubuum-web
EOF
fi

PULL_SERVICES_DISPLAY="caddy"
[[ "$BUILD_FROM_SOURCE" != "true" ]] && PULL_SERVICES_DISPLAY="${PULL_SERVICES_DISPLAY} hubuum-api"
[[ "$BUILD_FROM_SOURCE" != "true" && "$MODE" == "all" ]] && PULL_SERVICES_DISPLAY="${PULL_SERVICES_DISPLAY} hubuum-web"
[[ "$DATABASE_MANAGED" == "true" ]] && PULL_SERVICES_DISPLAY="${PULL_SERVICES_DISPLAY} postgres"
[[ "$MODE" == "all" ]] && PULL_SERVICES_DISPLAY="${PULL_SERVICES_DISPLAY} valkey"

UP_COMMAND="up -d"
[[ "$BUILD_FROM_SOURCE" == "true" ]] && UP_COMMAND="up -d --build"

cat <<EOF
  ${ENGINE_BIN} compose --env-file .env -f compose.yml pull ${PULL_SERVICES_DISPLAY}
  ${ENGINE_BIN} compose --env-file .env -f compose.yml ${UP_COMMAND}

Important:
  Make sure DNS for ${API_FQDN} points to this host.
EOF

if [[ "$MODE" == "all" ]]; then
  cat <<EOF
  Make sure DNS for ${WEB_FQDN} points to this host.
EOF
fi

cat <<'EOF'
  Make sure inbound TCP 80 and 443 are open.
  The first admin password is not logged; use hubuum-admin in the API container to reset it.
EOF
