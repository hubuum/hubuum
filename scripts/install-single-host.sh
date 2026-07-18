#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="/opt/hubuum"
MODE="all"
WEB_FQDN=""
API_FQDN=""
API_PORT="8080"
SHARED_HOST_ROUTING=""
LETSENCRYPT_EMAIL=""
BACKEND_REF="main"
FRONTEND_REF="main"
BACKEND_REPO="https://github.com/hubuum/hubuum.git"
FRONTEND_REPO="https://github.com/hubuum/hubuum-frontend.git"
BACKEND_IMAGE="ghcr.io/hubuum/hubuum-server:main"
FRONTEND_IMAGE="ghcr.io/hubuum/hubuum-frontend:main"
POSTGRES_IMAGE="docker.io/library/postgres:18.4-alpine3.24@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
VALKEY_IMAGE="docker.io/valkey/valkey:9-alpine"
CADDY_IMAGE="docker.io/library/caddy:2-alpine"
EXTERNAL_DATABASE_URL=""
AUTH_CONFIG_HOST_PATH=""
AUTH_CONFIG_CONTAINER_PATH="/etc/hubuum/auth.toml"
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
# Space-delimited list of config variables the caller set explicitly on the
# command line. Used to decide which values may be reused from an existing
# installation's .env on a re-run.
ARG_SET=""

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
  --api-port PORT         Internal backend API listen port. Default: 8080
  --shared-host-routing MODE
                          Required when --web and --api are the same in all mode: bff, direct, or prefixed
  --email EMAIL           Let's Encrypt registration email. Required
  --backend-image IMAGE   Backend image. Default: ghcr.io/hubuum/hubuum-server:main
  --frontend-image IMAGE  Frontend image. Default: ghcr.io/hubuum/hubuum-frontend:main
  --database-url URL      Existing Postgres URL. If set, no Postgres container is created
  --auth-config PATH      Host auth-provider TOML file to mount read-only in the API container
  --engine ENGINE         Container engine: auto, docker, or podman. Default: auto
  --postgres-image IMAGE  Postgres image. Default: PostgreSQL 18.4 on Alpine 3.24 (digest-pinned)
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

absolute_config_path() {
  local path="$1"
  local directory

  [[ -f "$path" ]] || die "auth config is not a regular file: $path"
  [[ -r "$path" ]] || die "auth config is not readable: $path"
  directory="$(cd -- "$(dirname -- "$path")" && pwd -P)" || die "cannot resolve auth config path: $path"
  printf '%s/%s' "$directory" "$(basename -- "$path")"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; ARG_SET+=" MODE"; shift 2 ;;
    --stop) ACTION="stop"; shift ;;
    --uninstall) ACTION="uninstall"; shift ;;
    --purge) PURGE="true"; shift ;;
    --dir) INSTALL_DIR="$2"; shift 2 ;;
    --web) WEB_FQDN="$2"; ARG_SET+=" WEB_FQDN"; shift 2 ;;
    --api) API_FQDN="$2"; ARG_SET+=" API_FQDN"; shift 2 ;;
    --api-port) API_PORT="$2"; ARG_SET+=" API_PORT"; shift 2 ;;
    --shared-host-routing) SHARED_HOST_ROUTING="$2"; ARG_SET+=" SHARED_HOST_ROUTING"; shift 2 ;;
    --email) LETSENCRYPT_EMAIL="$2"; ARG_SET+=" LETSENCRYPT_EMAIL"; shift 2 ;;
    --backend-image) BACKEND_IMAGE="$2"; ARG_SET+=" BACKEND_IMAGE"; shift 2 ;;
    --frontend-image) FRONTEND_IMAGE="$2"; ARG_SET+=" FRONTEND_IMAGE"; shift 2 ;;
    --backend-ref) BACKEND_REF="$2"; ARG_SET+=" BACKEND_REF"; shift 2 ;;
    --frontend-ref) FRONTEND_REF="$2"; ARG_SET+=" FRONTEND_REF"; shift 2 ;;
    --backend-repo) BACKEND_REPO="$2"; ARG_SET+=" BACKEND_REPO"; shift 2 ;;
    --frontend-repo) FRONTEND_REPO="$2"; ARG_SET+=" FRONTEND_REPO"; shift 2 ;;
    --database-url) EXTERNAL_DATABASE_URL="$2"; shift 2 ;;
    --auth-config) AUTH_CONFIG_HOST_PATH="$2"; ARG_SET+=" AUTH_CONFIG_HOST_PATH"; shift 2 ;;
    --engine) ENGINE="$2"; shift 2 ;;
    --postgres-image) POSTGRES_IMAGE="$2"; ARG_SET+=" POSTGRES_IMAGE"; shift 2 ;;
    --valkey-image) VALKEY_IMAGE="$2"; ARG_SET+=" VALKEY_IMAGE"; shift 2 ;;
    --caddy-image) CADDY_IMAGE="$2"; ARG_SET+=" CADDY_IMAGE"; shift 2 ;;
    --network-subnet) NETWORK_SUBNET="$2"; ARG_SET+=" NETWORK_SUBNET"; shift 2 ;;
    --systemd) INSTALL_SYSTEMD="true"; shift ;;
    --service-name) SERVICE_NAME="$2"; SERVICE_NAME_SET="true"; shift 2 ;;
    --no-systemd) INSTALL_SYSTEMD="false"; shift ;;
    --script-base-url) SCRIPT_BASE_URL="${2%/}"; shift 2 ;;
    --script-ref) SCRIPT_REF="$2"; shift 2 ;;
    --build-from-source) BUILD_FROM_SOURCE="true"; ARG_SET+=" BUILD_FROM_SOURCE"; shift ;;
    --no-pull) PULL="false"; shift ;;
    --recreate) RECREATE="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

ENV_FILE="$INSTALL_DIR/.env"

arg_was_set() {
  [[ " $ARG_SET " == *" $1 "* ]]
}

# Reuse a value from an existing installation's .env when the caller did not
# pass it explicitly, leaving the hardcoded default in place only for a fresh
# install. This is what lets a bare re-run (including curl | bash) update an
# existing deployment in place without re-specifying every argument.
reuse_from_env() {
  local var="$1" key="$2" val
  arg_was_set "$var" && return 0
  [[ -f "$ENV_FILE" ]] || return 0
  val="$(read_env_value "$key" || true)"
  [[ -n "$val" ]] || return 0
  printf -v "$var" '%s' "$val"
}

if [[ "$ACTION" == "install" && -f "$ENV_FILE" ]]; then
  reuse_from_env MODE INSTALL_MODE
  reuse_from_env WEB_FQDN WEB_FQDN
  reuse_from_env API_FQDN API_FQDN
  reuse_from_env API_PORT HUBUUM_BIND_PORT
  reuse_from_env SHARED_HOST_ROUTING SHARED_HOST_ROUTING
  reuse_from_env LETSENCRYPT_EMAIL LETSENCRYPT_EMAIL
  reuse_from_env BACKEND_IMAGE BACKEND_IMAGE
  reuse_from_env FRONTEND_IMAGE FRONTEND_IMAGE
  reuse_from_env POSTGRES_IMAGE POSTGRES_IMAGE
  reuse_from_env VALKEY_IMAGE VALKEY_IMAGE
  reuse_from_env CADDY_IMAGE CADDY_IMAGE
  reuse_from_env BACKEND_REF BACKEND_REF
  reuse_from_env FRONTEND_REF FRONTEND_REF
  reuse_from_env BACKEND_REPO BACKEND_REPO
  reuse_from_env FRONTEND_REPO FRONTEND_REPO
  reuse_from_env BUILD_FROM_SOURCE BUILD_FROM_SOURCE
  reuse_from_env NETWORK_SUBNET HUBUUM_CLIENT_ALLOWLIST
  reuse_from_env AUTH_CONFIG_HOST_PATH HUBUUM_AUTH_CONFIG_HOST_PATH
fi

[[ "$MODE" == "all" || "$MODE" == "backend" ]] || die "--mode must be all or backend"
[[ "$ENGINE" == "auto" || "$ENGINE" == "docker" || "$ENGINE" == "podman" ]] || die "--engine must be auto, docker, or podman"
[[ "$API_PORT" =~ ^[0-9]+$ && "$API_PORT" -ge 1 && "$API_PORT" -le 65535 ]] || die "--api-port must be an integer between 1 and 65535"
if [[ -n "$SHARED_HOST_ROUTING" && "$SHARED_HOST_ROUTING" != "bff" && "$SHARED_HOST_ROUTING" != "direct" && "$SHARED_HOST_ROUTING" != "prefixed" ]]; then
  die "--shared-host-routing must be bff, direct, or prefixed"
fi
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
if [[ "$ACTION" == "install" && "$MODE" == "all" && "$WEB_FQDN" == "$API_FQDN" && -z "$SHARED_HOST_ROUTING" ]]; then
  die "--shared-host-routing is required when --web and --api use the same hostname"
fi
if [[ "$ACTION" == "install" && "$MODE" == "all" && "$WEB_FQDN" != "$API_FQDN" && -n "$SHARED_HOST_ROUTING" ]]; then
  die "--shared-host-routing only applies when --web and --api use the same hostname"
fi
if [[ "$ACTION" == "install" && "$MODE" == "backend" && -n "$SHARED_HOST_ROUTING" ]]; then
  die "--shared-host-routing only applies in all mode"
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

if [[ -z "$AUTH_CONFIG_HOST_PATH" ]]; then
  AUTH_CONFIG_HOST_PATH="$INSTALL_DIR/auth.toml"
  if [[ ! -e "$AUTH_CONFIG_HOST_PATH" ]]; then
    install -m 0600 /dev/null "$AUTH_CONFIG_HOST_PATH"
  fi
fi
AUTH_CONFIG_HOST_PATH="$(absolute_config_path "$AUTH_CONFIG_HOST_PATH")"

install_management_script() {
  local script_name="$1"
  local local_path="$SCRIPT_DIR/$script_name"
  local dest_path="$INSTALL_DIR/$script_name"
  local temp_path

  if [[ -f "$local_path" && "$local_path" != "$dest_path" ]]; then
    install -m 0755 "$local_path" "$dest_path"
    return 0
  fi

  if command -v curl >/dev/null 2>&1; then
    temp_path="$(mktemp "$INSTALL_DIR/.${script_name}.XXXXXX")"
    if curl -fsSL "${SCRIPT_BASE_URL}/${script_name}" -o "$temp_path"; then
      install -m 0755 "$temp_path" "$dest_path"
      rm -f "$temp_path"
      return 0
    fi
    rm -f "$temp_path"
  fi

  if [[ -f "$dest_path" ]]; then
    chmod 0755 "$dest_path"
    echo "WARNING: could not refresh $script_name; keeping the installed copy" >&2
    return 0
  fi

  echo "WARNING: could not install $script_name; curl is unavailable and no local script was found" >&2
}

install_management_script install-single-host.sh
install_management_script update-single-host.sh
install_management_script single-host-rollout.sh
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
EXISTING_POSTGRES_PASSWORD="$(read_env_value POSTGRES_PASSWORD || true)"
if [[ "$RECREATE" != "true" ]]; then
  if [[ -z "$EXTERNAL_DATABASE_URL" && "$(read_env_value DATABASE_MANAGED || true)" == "false" ]]; then
    EXTERNAL_DATABASE_URL="$(read_env_value HUBUUM_DATABASE_URL || true)"
  fi
  POSTGRES_PASSWORD="$EXISTING_POSTGRES_PASSWORD"
  HUBUUM_TOKEN_HASH_KEY="$(read_env_value HUBUUM_TOKEN_HASH_KEY || true)"
fi

# A managed Postgres data volume is initialized with the password only on first
# boot; rotating it afterwards leaves the stored credential out of sync and
# breaks authentication. Preserve the existing password on --recreate and tell
# the operator how to perform a real reset.
if [[ "$RECREATE" == "true" && -z "$EXTERNAL_DATABASE_URL" && -n "$EXISTING_POSTGRES_PASSWORD" ]]; then
  echo "WARNING: --recreate does not rotate the managed Postgres password, because the existing database volume was initialized with it. Rotating it would break authentication. To reset the database, uninstall with --purge first, then reinstall." >&2
  POSTGRES_PASSWORD="$EXISTING_POSTGRES_PASSWORD"
fi

[[ -n "$POSTGRES_PASSWORD" ]] || POSTGRES_PASSWORD="$(random_hex 32)"
[[ -n "$HUBUUM_TOKEN_HASH_KEY" ]] || HUBUUM_TOKEN_HASH_KEY="$(random_hex 32)"

DATABASE_MANAGED="true"
HUBUUM_DATABASE_URL="postgres://hubuum:${POSTGRES_PASSWORD}@postgres:5432/hubuum"
if [[ -n "$EXTERNAL_DATABASE_URL" ]]; then
  DATABASE_MANAGED="false"
  HUBUUM_DATABASE_URL="$EXTERNAL_DATABASE_URL"
fi

LOGIN_RATE_LIMIT_BACKEND="memory"
if [[ "$MODE" == "all" ]]; then
  LOGIN_RATE_LIMIT_BACKEND="valkey"
fi

{
  printf 'INSTALL_MODE=%s\n' "$MODE"
  printf 'WEB_FQDN=%s\n' "$WEB_FQDN"
  printf 'API_FQDN=%s\n' "$API_FQDN"
  printf 'SHARED_HOST_ROUTING=%s\n' "$SHARED_HOST_ROUTING"
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
  printf 'HUBUUM_BIND_PORT=%s\n' "$API_PORT"
  printf 'HUBUUM_LOG_LEVEL=info\n'
  printf 'HUBUUM_TOKEN_HASH_KEY=%s\n' "$HUBUUM_TOKEN_HASH_KEY"
  printf 'HUBUUM_CLIENT_ALLOWLIST=%s\n' "$NETWORK_SUBNET"
  printf 'HUBUUM_TRUST_IP_HEADERS=false\n'
  printf 'HUBUUM_TOKEN_LIFETIME_HOURS=24\n'
  printf 'HUBUUM_DEFAULT_PAGE_LIMIT=100\n'
  printf 'HUBUUM_MAX_PAGE_LIMIT=250\n'
  printf 'HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS=5\n'
  printf 'HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS=300\n'
  printf 'HUBUUM_LOGIN_RATE_LIMIT_BACKEND=%s\n' "$LOGIN_RATE_LIMIT_BACKEND"
  printf 'HUBUUM_LOGIN_RATE_LIMIT_VALKEY_URL=redis://valkey:6379/1\n'
  printf 'HUBUUM_AUTH_CONFIG_HOST_PATH=%s\n' "$(quote_env "$AUTH_CONFIG_HOST_PATH")"
  printf 'HUBUUM_AUTH_CONFIG_PATH=%s\n' "$AUTH_CONFIG_CONTAINER_PATH"
  printf '\n'
  printf 'BACKEND_BASE_URL=http://caddy:8081\n'
  printf 'VALKEY_URL=redis://valkey:6379/0\n'
  printf 'SESSION_TTL_SECONDS=28800\n'
  printf 'SESSION_PREFIX=hubuum:sess:\n'
  printf 'NEXT_PUBLIC_APP_NAME=%s\n' "$(quote_env "Hubuum Console")"
} > "$ENV_FILE"

chmod 0600 "$ENV_FILE"

CADDYFILE_TEMP="$(mktemp "$INSTALL_DIR/.Caddyfile.XXXXXX")"
cat > "$CADDYFILE_TEMP" <<EOF
{
    email ${LETSENCRYPT_EMAIL}
}

(api_proxy) {
    reverse_proxy hubuum-api:${API_PORT} hubuum-api-standby:${API_PORT} {
        health_uri /readyz
        health_interval 5s
        health_timeout 3s
        fail_duration 30s
        max_fails 1
        lb_try_duration 5s
        lb_try_interval 250ms
        stream_close_delay 5m
    }
}
EOF

if [[ "$MODE" == "all" ]]; then
  cat >> "$CADDYFILE_TEMP" <<'EOF'
(web_proxy) {
    reverse_proxy hubuum-web:3000 hubuum-web-standby:3000 {
        health_uri /
        health_status 2xx
        health_follow_redirects
        health_interval 5s
        health_timeout 3s
        fail_duration 30s
        max_fails 1
        lb_try_duration 5s
        lb_try_interval 250ms
        stream_close_delay 5m
    }
}

:8081 {
    import api_proxy
}
EOF
fi

if [[ "$MODE" == "all" && -z "$SHARED_HOST_ROUTING" ]]; then
  cat >> "$CADDYFILE_TEMP" <<EOF
${WEB_FQDN} {
    encode zstd gzip
    import web_proxy
}

${API_FQDN} {
    encode zstd gzip
    import api_proxy
}
EOF
elif [[ "$MODE" == "all" && "$SHARED_HOST_ROUTING" == "bff" ]]; then
  cat >> "$CADDYFILE_TEMP" <<EOF
${WEB_FQDN} {
    encode zstd gzip
    import web_proxy
}
EOF
elif [[ "$MODE" == "all" && "$SHARED_HOST_ROUTING" == "direct" ]]; then
  cat >> "$CADDYFILE_TEMP" <<EOF
${WEB_FQDN} {
    encode zstd gzip

    handle /api/v0* {
        import api_proxy
    }

    handle /api/v1* {
        import api_proxy
    }

    handle /api-doc* {
        import api_proxy
    }

    handle /swagger-ui* {
        import api_proxy
    }

    handle {
        import web_proxy
    }
}
EOF
elif [[ "$MODE" == "all" && "$SHARED_HOST_ROUTING" == "prefixed" ]]; then
  cat >> "$CADDYFILE_TEMP" <<EOF
${WEB_FQDN} {
    encode zstd gzip

    handle /hubuum-api {
        redir /hubuum-api/
    }

    handle_path /hubuum-api/* {
        import api_proxy
    }

    handle {
        import web_proxy
    }
}
EOF
else
  cat >> "$CADDYFILE_TEMP" <<EOF
${API_FQDN} {
    encode zstd gzip
    import api_proxy
}
EOF
fi

# Caddy bind-mounts this file directly. Preserve an existing destination inode
# so a running container sees the new contents when hubuum_reload_caddy reads
# /etc/caddy/Caddyfile on Linux.
cp "$CADDYFILE_TEMP" "$INSTALL_DIR/Caddyfile"
rm -f "$CADDYFILE_TEMP"

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
  hubuum-api: &hubuum-api
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
    read_only: true
    tmpfs:
      - /tmp:size=16m,mode=1777
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    # Allow Actix request draining and the separately bounded worker shutdown.
    stop_grace_period: 75s
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
      HUBUUM_DEFAULT_PAGE_LIMIT: ${HUBUUM_DEFAULT_PAGE_LIMIT}
      HUBUUM_MAX_PAGE_LIMIT: ${HUBUUM_MAX_PAGE_LIMIT}
      HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS: ${HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS}
      HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS: ${HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS}
      HUBUUM_LOGIN_RATE_LIMIT_BACKEND: ${HUBUUM_LOGIN_RATE_LIMIT_BACKEND}
      HUBUUM_LOGIN_RATE_LIMIT_VALKEY_URL: ${HUBUUM_LOGIN_RATE_LIMIT_VALKEY_URL}
      HUBUUM_AUTH_CONFIG_PATH: ${HUBUUM_AUTH_CONFIG_PATH}
    volumes:
      - type: bind
        source: "${HUBUUM_AUTH_CONFIG_HOST_PATH}"
        target: "${HUBUUM_AUTH_CONFIG_PATH}"
        read_only: true
EOF

if [[ "$DATABASE_MANAGED" == "true" ]]; then
  cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
    depends_on:
      postgres:
        condition: service_healthy
EOF
fi

cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
    expose:
      - "${HUBUUM_BIND_PORT}"
    networks:
      - hubuum_net
    healthcheck:
      test: ["CMD-SHELL", "wget --quiet --output-document=/dev/null http://127.0.0.1:${HUBUUM_BIND_PORT}/readyz"]
      interval: 5s
      timeout: 3s
      retries: 36

  hubuum-api-standby:
    <<: *hubuum-api
    container_name: hubuum-api-standby
    command: ["--runtime-role", "api"]
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

  hubuum-web: &hubuum-web
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
    stop_grace_period: 30s
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
      valkey:
        condition: service_healthy
    expose:
      - "3000"
    networks:
      - hubuum_net
    healthcheck:
      test: ["CMD", "node", "-e", "fetch('http://127.0.0.1:3000/').then(response => { if (!response.ok) process.exit(1) }).catch(() => process.exit(1))"]
      interval: 5s
      timeout: 3s
      retries: 36

  hubuum-web-standby:
    <<: *hubuum-web
    container_name: hubuum-web-standby
EOF
fi

cat >> "$INSTALL_DIR/compose.yml" <<'EOF'

  caddy:
    image: ${CADDY_IMAGE}
    container_name: hubuum-caddy
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy_data:/data
      - caddy_config:/config
    depends_on:
      - hubuum-api
      - hubuum-api-standby
EOF

if [[ "$MODE" == "all" ]]; then
  cat >> "$INSTALL_DIR/compose.yml" <<'EOF'
      - hubuum-web
      - hubuum-web-standby
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
}

SYSTEMD_STATUS="not installed"
if [[ "$INSTALL_SYSTEMD" == "true" && -d /run/systemd/system && "$(command -v systemctl || true)" ]]; then
  install_systemd_unit
  SYSTEMD_STATUS="enabled as ${SERVICE_NAME}.service"
elif [[ "$INSTALL_SYSTEMD" == "true" ]]; then
  SYSTEMD_STATUS="skipped; systemd is not available"
fi

# shellcheck source=scripts/single-host-rollout.sh
source "$INSTALL_DIR/single-host-rollout.sh"
INSTALL_MODE="$MODE"
hubuum_rollout

if [[ "$SYSTEMD_STATUS" == enabled* ]]; then
  # Mark the oneshot unit active without restarting the already-rolled stack.
  systemctl start "$SERVICE_NAME"
fi

cat <<EOF

Hubuum ${MODE} stack started.

Container engine:
  ${ENGINE_BIN} compose

Boot service:
  ${SYSTEMD_STATUS}

Image source:
  $([[ "$BUILD_FROM_SOURCE" == "true" ]] && printf 'local source builds' || printf 'published container images')

Authentication config:
  ${AUTH_CONFIG_HOST_PATH} (mounted read-only at ${AUTH_CONFIG_CONTAINER_PATH})

Backend API:
EOF

if [[ "$MODE" == "all" && "$SHARED_HOST_ROUTING" == "prefixed" ]]; then
  cat <<EOF
  https://${API_FQDN}/hubuum-api/
EOF
elif [[ "$MODE" == "all" && "$SHARED_HOST_ROUTING" == "bff" ]]; then
  cat <<EOF
  https://${API_FQDN} via frontend BFF routes
EOF
else
  cat <<EOF
  https://${API_FQDN}
EOF
fi

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

cat <<EOF

Important:
  Make sure DNS for ${API_FQDN} points to this host.
EOF

if [[ "$MODE" == "all" && "$WEB_FQDN" != "$API_FQDN" ]]; then
  cat <<EOF
  Make sure DNS for ${WEB_FQDN} points to this host.
EOF
fi

cat <<'EOF'
  Make sure inbound TCP 80 and 443 are open.
  The first admin password is not logged; use hubuum-admin in the API container to reset it.
EOF
