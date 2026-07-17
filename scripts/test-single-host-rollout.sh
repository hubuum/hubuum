#!/usr/bin/env bash
set -euo pipefail

REPOSITORY_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf "$TEST_ROOT"' EXIT

COMMAND_LOG="$TEST_ROOT/commands.log"
FAKE_ENGINE="$TEST_ROOT/engine"

cat > "$FAKE_ENGINE" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "$COMMAND_LOG"

if [[ "${1:-}" == "inspect" ]]; then
  service="${*: -1}"
  service="${service#container-}"
  if [[ " ${FAKE_UNHEALTHY_SERVICES:-} " == *" $service "* && ! -e "$TEST_ROOT/started-$service" ]]; then
    printf 'unhealthy\n'
  else
    printf 'healthy\n'
  fi
  exit 0
fi

if [[ "$*" == *" ps -q "* ]]; then
  service="${*: -1}"
  if [[ "$service" == "caddy" && "$FAKE_CADDY_RUNNING" != "true" && ! -e "$TEST_ROOT/started-caddy" ]]; then
    exit 0
  fi
  if [[ " ${FAKE_MISSING_SERVICES:-} " != *" $service "* || -e "$TEST_ROOT/started-$service" ]]; then
    printf 'container-%s\n' "$service"
  fi
fi

if [[ "$*" == *" up "* ]]; then
  service="${*: -1}"
  touch "$TEST_ROOT/started-$service"
fi
EOF
chmod +x "$FAKE_ENGINE"

export COMMAND_LOG FAKE_CADDY_RUNNING TEST_ROOT
export FAKE_MISSING_SERVICES=""
export FAKE_UNHEALTHY_SERVICES=""
ENGINE_PATH="$FAKE_ENGINE"
COMPOSE_CMD=("$FAKE_ENGINE" compose --env-file .env -f compose.yml)

# shellcheck source=scripts/single-host-rollout.sh
source "$REPOSITORY_ROOT/scripts/single-host-rollout.sh"

assert_commands() {
  local expected="$1"
  local actual="$TEST_ROOT/actual.log"

  grep -E '(^| )(run|up|exec) ' "$COMMAND_LOG" > "$actual"
  diff -u "$expected" "$actual"
}

FAKE_CADDY_RUNNING="true"
INSTALL_MODE="all"
: > "$COMMAND_LOG"
hubuum_rollout
cat > "$TEST_ROOT/expected-rolling.log" <<EOF
compose --env-file .env -f compose.yml run --rm --no-deps --entrypoint /usr/local/bin/hubuum-admin hubuum-api --migrate
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api-standby
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web-standby
compose --env-file .env -f compose.yml exec -T caddy caddy reload --force --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web
compose --env-file .env -f compose.yml exec -T caddy caddy reload --force --config /etc/caddy/Caddyfile --adapter caddyfile
EOF
assert_commands "$TEST_ROOT/expected-rolling.log"

INSTALL_MODE="backend"
: > "$COMMAND_LOG"
hubuum_rollout
cat > "$TEST_ROOT/expected-reload.log" <<EOF
compose --env-file .env -f compose.yml run --rm --no-deps --entrypoint /usr/local/bin/hubuum-admin hubuum-api --migrate
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api-standby
compose --env-file .env -f compose.yml exec -T caddy caddy reload --force --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api
compose --env-file .env -f compose.yml exec -T caddy caddy reload --force --config /etc/caddy/Caddyfile --adapter caddyfile
EOF
assert_commands "$TEST_ROOT/expected-reload.log"

FAKE_CADDY_RUNNING="true"
FAKE_UNHEALTHY_SERVICES="hubuum-api"
INSTALL_MODE="all"
rm -f "$TEST_ROOT/started-hubuum-api"
: > "$COMMAND_LOG"
hubuum_rollout
cat > "$TEST_ROOT/expected-recovery.log" <<EOF
compose --env-file .env -f compose.yml run --rm --no-deps --entrypoint /usr/local/bin/hubuum-admin hubuum-api --migrate
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api
compose --env-file .env -f compose.yml exec -T caddy caddy reload --force --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api-standby
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web-standby
compose --env-file .env -f compose.yml exec -T caddy caddy reload --force --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web
compose --env-file .env -f compose.yml exec -T caddy caddy reload --force --config /etc/caddy/Caddyfile --adapter caddyfile
EOF
assert_commands "$TEST_ROOT/expected-recovery.log"

FAKE_UNHEALTHY_SERVICES=""
FAKE_MISSING_SERVICES="valkey"
rm -f "$TEST_ROOT/started-valkey"
: > "$COMMAND_LOG"
hubuum_rollout
cat > "$TEST_ROOT/expected-missing-infrastructure.log" <<EOF
compose --env-file .env -f compose.yml up -d --no-deps --no-recreate valkey
compose --env-file .env -f compose.yml run --rm --no-deps --entrypoint /usr/local/bin/hubuum-admin hubuum-api --migrate
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api-standby
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web-standby
compose --env-file .env -f compose.yml exec -T caddy caddy reload --force --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web
compose --env-file .env -f compose.yml exec -T caddy caddy reload --force --config /etc/caddy/Caddyfile --adapter caddyfile
EOF
assert_commands "$TEST_ROOT/expected-missing-infrastructure.log"

FAKE_CADDY_RUNNING="false"
FAKE_MISSING_SERVICES=""
INSTALL_MODE="backend"
: > "$COMMAND_LOG"
hubuum_rollout
cat > "$TEST_ROOT/expected-initial.log" <<EOF
compose --env-file .env -f compose.yml up -d hubuum-api
compose --env-file .env -f compose.yml up -d --no-deps hubuum-api-standby
compose --env-file .env -f compose.yml up -d --no-deps caddy
EOF
assert_commands "$TEST_ROOT/expected-initial.log"

echo "Single-host rolling update test passed"
