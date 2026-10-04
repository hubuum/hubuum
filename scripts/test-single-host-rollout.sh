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

if [[ "$*" == *" hubuum-migrate --migration-mode" ]]; then
  printf '%s\n' "${FAKE_MIGRATION_MODE:-rolling}"
  exit "${FAKE_PREFLIGHT_STATUS:-0}"
fi

if [[ "$*" == *" run "* && "$*" == *" hubuum-migrate --migrate" &&
  "${FAKE_MIGRATION_FAIL:-false}" == "true" ]]; then
  exit 1
fi

if [[ "$*" == *" exec -T caddy caddy reload "* ]]; then
  printf '{"level":"info","msg":"fake reload diagnostic"}\n' >&2
  [[ "$FAKE_CADDY_RELOAD_FAIL" != "true" ]]
  exit
fi

if [[ "$*" == *" exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams"* ]]; then
  failure_polls_file="$TEST_ROOT/caddy-failure-polls"
  failure_polls=0
  if [[ -f "$failure_polls_file" ]]; then
    read -r failure_polls < "$failure_polls_file"
  fi
  if (( failure_polls > 0 )); then
    standby_fails=1
    printf '%s\n' "$((failure_polls - 1))" > "$failure_polls_file"
  else
    standby_fails=0
  fi
  printf '[{"address":"hubuum-api:8080","num_requests":0,"fails":0},'
  printf '{"address":"hubuum-api-standby:8080","num_requests":0,"fails":%s},' "$standby_fails"
  printf '{"address":"hubuum-web:3000","num_requests":0,"fails":0},'
  printf '{"address":"hubuum-web-standby:3000","num_requests":0,"fails":0}]\n'
  exit 0
fi

if [[ "${1:-}" == "inspect" && "$*" == *".Dependencies"* ]]; then
  if [[ "$FAKE_CADDY_DEPENDENCIES" == "true" && "${*: -1}" == "container-caddy" ]]; then
    printf 'container-hubuum-api\n'
  fi
  exit 0
fi

if [[ "${1:-}" == "inspect" && "$*" == *"Config.Labels"* ]]; then
  service="${*: -1}"
  printf '%s\n' "${service#container-}"
  exit 0
fi

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

if [[ "$*" == *" ps --help" ]]; then
  if [[ "$ENGINE_BIN" == "docker" ]]; then
    printf '%s\n' '  -a, --all  Show all stopped containers'
  else
    printf '%s\n' '  -q, --quiet  Only display container IDs'
  fi
  exit 0
fi

if [[ "$*" == *" ps "* ]]; then
  [[ "$*" == *" ps -q" || ( "$ENGINE_BIN" == "docker" && "$*" == *" ps -a -q" ) ]] || {
    echo "service arguments to compose ps are unsupported" >&2
    exit 2
  }

  for service in caddy postgres valkey hubuum-api hubuum-api-standby hubuum-restore-executor hubuum-web hubuum-web-standby prometheus grafana; do
    if [[ -e "$TEST_ROOT/removed-$service" ]]; then
      continue
    fi
    if [[ ( "$service" == "prometheus" || "$service" == "grafana" ) && "$FAKE_MONITORING_PRESENT" != "true" ]]; then
      continue
    fi
    if [[ -e "$TEST_ROOT/stopped-$service" && "$ENGINE_BIN" != "podman" && "$*" != *" ps -a -q" ]]; then
      continue
    fi
    if [[ "$service" == "caddy" && "$FAKE_CADDY_RUNNING" != "true" && ! -e "$TEST_ROOT/started-caddy" ]]; then
      continue
    fi
    if [[ " ${FAKE_MISSING_SERVICES:-} " != *" $service "* || -e "$TEST_ROOT/started-$service" ]]; then
      printf 'container-%s\n' "$service"
    fi
  done
fi

if [[ "${1:-}" == "stop" || "${1:-}" == "rm" ]]; then
  action="$1"
  shift
  for container in "$@"; do
    service="${container#container-}"
    [[ "$action" == "stop" ]] && touch "$TEST_ROOT/stopped-$service"
    [[ "$action" == "rm" ]] && touch "$TEST_ROOT/removed-$service"
  done
  exit 0
fi

if [[ "$*" == *" up "* ]]; then
  service="${*: -1}"
  rm -f "$TEST_ROOT/stopped-$service"
  touch "$TEST_ROOT/started-$service"
fi

if [[ "$*" == *" stop "* ]]; then
  service="${*: -1}"
  touch "$TEST_ROOT/stopped-$service"
fi

if [[ "$*" == *" start "* ]]; then
  service="${*: -1}"
  rm -f "$TEST_ROOT/stopped-$service"
fi
EOF
chmod +x "$FAKE_ENGINE"

FAKE_CADDY_DEPENDENCIES="false"
FAKE_CADDY_RELOAD_FAIL="false"
FAKE_MIGRATION_FAIL="false"
export COMMAND_LOG FAKE_CADDY_DEPENDENCIES FAKE_CADDY_RELOAD_FAIL
export FAKE_CADDY_RUNNING FAKE_MIGRATION_FAIL TEST_ROOT
export FAKE_MISSING_SERVICES=""
export FAKE_UNHEALTHY_SERVICES=""
export FAKE_MONITORING_PRESENT="false"
export ENGINE_BIN="docker"
ENGINE_PATH="$FAKE_ENGINE"
COMPOSE_CMD=("$FAKE_ENGINE" compose --env-file .env -f compose.yml)
API_PORT=8080
DATABASE_MANAGED="false"

# shellcheck source=scripts/single-host-rollout.sh
source "$REPOSITORY_ROOT/scripts/single-host-rollout.sh"

assert_commands() {
  local expected="$1"
  local actual="$TEST_ROOT/actual.log"

  grep -E '(^| )(run|up|exec|start|stop|rm) ' "$COMMAND_LOG" | grep -v -- '--migration-mode' > "$actual"
  diff -u "$expected" "$actual"
}

assert_commands_with_unordered_prefix() {
  local expected="$1"
  local unordered_count="$2"
  local actual="$TEST_ROOT/actual.log"
  local ordered_start=$((unordered_count + 1))

  grep -E '(^| )(run|up|exec|start|stop|rm) ' "$COMMAND_LOG" | grep -v -- '--migration-mode' > "$actual"
  diff -u \
    <(head -n "$unordered_count" "$expected" | sort) \
    <(head -n "$unordered_count" "$actual" | sort)
  diff -u \
    <(tail -n +"$ordered_start" "$expected") \
    <(tail -n +"$ordered_start" "$actual")
}

assert_caddy_upstream_status_eligibility() {
  local expected="$1"
  local upstreams="$2"
  local actual="false"
  shift 2

  if hubuum_caddy_upstream_status_is_eligible "$upstreams" "$@"; then
    actual="true"
  fi
  [[ "$actual" == "$expected" ]] || {
    printf 'unexpected Caddy upstream eligibility for %s (required: %s)\n' \
      "$upstreams" "$*" >&2
    exit 1
  }
}

assert_rollout_rejects_timeout_setting() {
  local setting_name="$1"
  local value="$2"
  local output

  printf -v "$setting_name" '%s' "$value"
  : > "$COMMAND_LOG"
  if output="$(hubuum_rollout 2>&1)"; then
    echo "rollout with invalid $setting_name unexpectedly succeeded" >&2
    exit 1
  fi
  [[ "$output" == "ERROR: $setting_name must be a positive integer; got '$value'" ]]
  [[ ! -s "$COMMAND_LOG" ]] || {
    echo "rollout changed state before validating $setting_name" >&2
    exit 1
  }
  unset "$setting_name"
}

assert_caddy_upstream_status_eligibility \
  "false" '[]' "hubuum-api-standby:8080"
assert_caddy_upstream_status_eligibility \
  "false" '[{"address":"hubuum-api:8080","fails":0}]' \
  "hubuum-api-standby:8080"
assert_caddy_upstream_status_eligibility \
  "false" '[{"address":"hubuum-api-standby:8080"}]' \
  "hubuum-api-standby:8080"
assert_caddy_upstream_status_eligibility \
  "true" \
  '[{"address":"hubuum-api-standby:8080","fails":0},{"address":"hubuum-web:3000","fails":2}]' \
  "hubuum-api-standby:8080"
assert_caddy_upstream_status_eligibility \
  "false" \
  '[{"address":"hubuum-api:8080","fails":0},{"address":"hubuum-api-standby:8080","fails":2}]' \
  "hubuum-api-standby:8080"
assert_caddy_upstream_status_eligibility \
  "false" \
  '[{"address":"hubuum-api-standby:8080","fails":0},{"address":"hubuum-api-standby:8080","fails":1}]' \
  "hubuum-api-standby:8080"
assert_caddy_upstream_status_eligibility \
  "true" \
  '[{"address":"hubuum-api-standby:8080","fails":0},{"address":"hubuum-web-standby:3000","fails":0},{"address":"hubuum-web:3000","fails":4}]' \
  "hubuum-api-standby:8080" "hubuum-web-standby:3000"

FAKE_CADDY_RUNNING="true"
FAKE_CADDY_DEPENDENCIES="true"
INSTALL_MODE="all"
: > "$COMMAND_LOG"
printf '2\n' > "$TEST_ROOT/caddy-failure-polls"
hubuum_rollout
cat > "$TEST_ROOT/expected-rolling.log" <<EOF
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate caddy
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml stop hubuum-api
compose --env-file .env -f compose.yml run --rm --no-deps -T hubuum-migrate --migrate
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-restore-executor
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api-standby
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web-standby
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
EOF
assert_commands "$TEST_ROOT/expected-rolling.log"

FAKE_CADDY_DEPENDENCIES="false"
INSTALL_MODE="backend"
: > "$COMMAND_LOG"
hubuum_rollout
cat > "$TEST_ROOT/expected-reload.log" <<EOF
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml stop hubuum-api
compose --env-file .env -f compose.yml run --rm --no-deps -T hubuum-migrate --migrate
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-restore-executor
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api-standby
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
EOF
assert_commands "$TEST_ROOT/expected-reload.log"

FAKE_MIGRATION_FAIL="true"
: > "$COMMAND_LOG"
if hubuum_rollout; then
  echo "rollout with a failed migration unexpectedly succeeded" >&2
  exit 1
fi
cat > "$TEST_ROOT/expected-migration-failure.log" <<EOF
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml stop hubuum-api
compose --env-file .env -f compose.yml run --rm --no-deps -T hubuum-migrate --migrate
compose --env-file .env -f compose.yml start hubuum-api
EOF
assert_commands "$TEST_ROOT/expected-migration-failure.log"
FAKE_MIGRATION_FAIL="false"

FAKE_UNHEALTHY_SERVICES="hubuum-api-standby"
rm -f "$TEST_ROOT/started-hubuum-api-standby"
: > "$COMMAND_LOG"
if unhealthy_standby_output="$(hubuum_rollout 2>&1)"; then
  echo "rollout with an unhealthy API standby unexpectedly succeeded" >&2
  exit 1
fi
[[ "$unhealthy_standby_output" == *"refusing to migrate while old-version workers remain online"* ]]
if grep -v -- '--migration-mode' "$COMMAND_LOG" | grep -Eq '(^| )(run|start|stop) '; then
  echo "rollout changed application state with an unhealthy API standby" >&2
  exit 1
fi

FAKE_CADDY_RUNNING="true"
FAKE_UNHEALTHY_SERVICES="hubuum-api"
INSTALL_MODE="all"
rm -f "$TEST_ROOT/started-hubuum-api"
: > "$COMMAND_LOG"
hubuum_rollout
cat > "$TEST_ROOT/expected-recovery.log" <<EOF
compose --env-file .env -f compose.yml run --rm --no-deps -T hubuum-migrate --migrate
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-restore-executor
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api-standby
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web-standby
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
EOF
assert_commands "$TEST_ROOT/expected-recovery.log"

FAKE_UNHEALTHY_SERVICES=""
FAKE_MISSING_SERVICES="valkey"
rm -f "$TEST_ROOT/started-valkey"
: > "$COMMAND_LOG"
hubuum_rollout
cat > "$TEST_ROOT/expected-missing-infrastructure.log" <<EOF
compose --env-file .env -f compose.yml up -d --no-deps --no-recreate valkey
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml stop hubuum-api
compose --env-file .env -f compose.yml run --rm --no-deps -T hubuum-migrate --migrate
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-restore-executor
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api-standby
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web-standby
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
EOF
assert_commands "$TEST_ROOT/expected-missing-infrastructure.log"

FAKE_CADDY_RUNNING="false"
FAKE_MISSING_SERVICES=""
INSTALL_MODE="backend"
rm -f "$TEST_ROOT/started-caddy"
: > "$COMMAND_LOG"
hubuum_rollout
cat > "$TEST_ROOT/expected-initial.log" <<EOF
compose --env-file .env -f compose.yml run --rm --no-deps -T hubuum-migrate --migrate
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-restore-executor
compose --env-file .env -f compose.yml up -d hubuum-api
compose --env-file .env -f compose.yml up -d --no-deps hubuum-api-standby
compose --env-file .env -f compose.yml up -d --no-deps caddy
EOF
assert_commands "$TEST_ROOT/expected-initial.log"

printf '2\n' > "$TEST_ROOT/caddy-failure-polls"
: > "$COMMAND_LOG"
sleep() { :; }
# Bash's integer SECONDS clock can cross a boundary immediately after the
# deadline is calculated. Keep enough synthetic time for all no-sleep polls.
HUBUUM_ROLLOUT_CADDY_TIMEOUT_SECONDS=5
hubuum_wait_for_caddy_upstreams "hubuum-api-standby:8080"
unset HUBUUM_ROLLOUT_CADDY_TIMEOUT_SECONDS
unset -f sleep
[[ "$(grep -c 'reverse_proxy/upstreams' "$COMMAND_LOG")" -eq 3 ]] || {
  echo "Caddy upstream wait did not poll until passive failures cleared" >&2
  exit 1
}
rm -f "$TEST_ROOT/caddy-failure-polls"

HUBUUM_ROLLOUT_CADDY_TIMEOUT_SECONDS=invalid
if timeout_output="$(hubuum_wait_for_caddy_upstreams "hubuum-api-standby:8080" 2>&1)"; then
  echo "invalid Caddy upstream timeout unexpectedly succeeded" >&2
  exit 1
fi
[[ "$timeout_output" == "ERROR: Caddy upstream timeout must be a positive integer; got 'invalid'" ]]
unset HUBUUM_ROLLOUT_CADDY_TIMEOUT_SECONDS

if timeout_output="$(hubuum_wait_for_healthy hubuum-api 0 2>&1)"; then
  echo "zero health timeout unexpectedly succeeded" >&2
  exit 1
fi
[[ "$timeout_output" == "ERROR: health timeout must be a positive integer; got '0'" ]]

assert_rollout_rejects_timeout_setting HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS invalid
assert_rollout_rejects_timeout_setting HUBUUM_ROLLOUT_CADDY_TIMEOUT_SECONDS 0

reload_output="$(hubuum_reload_caddy 2>&1)"
[[ "$reload_output" == "Reloading Caddy if its configuration changed..." ]] || {
  printf 'successful Caddy reload emitted unexpected output:\n%s\n' "$reload_output" >&2
  exit 1
}

FAKE_CADDY_RELOAD_FAIL="true"
if reload_output="$(hubuum_reload_caddy 2>&1)"; then
  echo "failed Caddy reload unexpectedly succeeded" >&2
  exit 1
fi
[[ "$reload_output" == *"ERROR: Caddy reload failed"* ]]
[[ "$reload_output" == *'fake reload diagnostic'* ]]

DATABASE_MANAGED="true"
DATABASE_ROLE_MODE="split"
: > "$COMMAND_LOG"
hubuum_run_migrations
cat > "$TEST_ROOT/expected-managed-migration.log" <<EOF
compose --env-file .env -f compose.yml run --rm --no-deps -T hubuum-migrate --database-role-setup-sql
compose --env-file .env -f compose.yml exec -T postgres psql --set ON_ERROR_STOP=1 --username hubuum --dbname hubuum
compose --env-file .env -f compose.yml run --rm --no-deps -T --entrypoint /usr/local/bin/hubuum-set-database-role-passwords postgres
compose --env-file .env -f compose.yml run --rm --no-deps -T hubuum-migrate --migrate
EOF
# Bash starts both sides of a pipeline concurrently, so either Compose process
# may reach the fake engine first. The password update and migration remain
# ordered after both pipeline processes finish.
assert_commands_with_unordered_prefix "$TEST_ROOT/expected-managed-migration.log" 2

DATABASE_ROLE_MODE="single"
: > "$COMMAND_LOG"
hubuum_run_migrations
cat > "$TEST_ROOT/expected-managed-single-role-migration.log" <<EOF
compose --env-file .env -f compose.yml run --rm --no-deps -T hubuum-migrate --migrate
EOF
assert_commands "$TEST_ROOT/expected-managed-single-role-migration.log"

MONITORING_ENABLED="false"
: > "$COMMAND_LOG"
hubuum_roll_monitoring
! grep -Eq '(^| )(run|up|exec|start|stop|rm) ' "$COMMAND_LOG"

for ENGINE_BIN in docker podman; do
  # One service is running and the other stopped; both must be removed after
  # configuration refresh has made them orphans. Other project services stay.
  FAKE_MONITORING_PRESENT="true"
  rm -f "$TEST_ROOT"/{stopped,removed}-{prometheus,grafana}
  touch "$TEST_ROOT/stopped-grafana"
  : > "$COMMAND_LOG"
  hubuum_roll_monitoring
  cat > "$TEST_ROOT/expected-monitoring-disabled.log" <<EOF
stop container-prometheus container-grafana
rm container-prometheus container-grafana
EOF
  assert_commands "$TEST_ROOT/expected-monitoring-disabled.log"
  [[ -e "$TEST_ROOT/removed-prometheus" && -e "$TEST_ROOT/removed-grafana" ]]
  : > "$COMMAND_LOG"
  hubuum_roll_monitoring
  ! grep -Eq '(^| )(run|up|exec|start|stop|rm) ' "$COMMAND_LOG"
done

MONITORING_ENABLED="true"
ENGINE_BIN="docker"
rm -f "$TEST_ROOT"/{stopped,removed}-{prometheus,grafana}
: > "$COMMAND_LOG"
hubuum_roll_monitoring
cat > "$TEST_ROOT/expected-monitoring.log" <<EOF
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate prometheus grafana
EOF
assert_commands "$TEST_ROOT/expected-monitoring.log"
grep -q 'inspect .*container-prometheus' "$COMMAND_LOG"
grep -q 'inspect .*container-grafana' "$COMMAND_LOG"

MONITORING_ENABLED="false"
FAKE_MONITORING_PRESENT="false"
DATABASE_MANAGED="false"
FAKE_CADDY_RELOAD_FAIL="false"
FAKE_CADDY_RUNNING="true"
INSTALL_MODE="backend"
export FAKE_MIGRATION_MODE="offline"
: > "$COMMAND_LOG"
hubuum_rollout
cat > "$TEST_ROOT/expected-offline.log" <<EOF
compose --env-file .env -f compose.yml stop --timeout 75 hubuum-api hubuum-api-standby hubuum-restore-executor
compose --env-file .env -f compose.yml run --rm --no-deps -T hubuum-migrate --migrate
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-restore-executor
compose --env-file .env -f compose.yml up -d hubuum-api
compose --env-file .env -f compose.yml up -d --no-deps hubuum-api-standby
compose --env-file .env -f compose.yml up -d --no-deps caddy
EOF
cat >> "$TEST_ROOT/expected-offline.log" <<EOF
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
EOF
assert_commands "$TEST_ROOT/expected-offline.log"

# An interrupted offline upgrade may have completed migration but left both
# API replicas stopped. A rolling preflight must still recover the stack.
export FAKE_MIGRATION_MODE="rolling"
FAKE_UNHEALTHY_SERVICES="hubuum-api hubuum-api-standby"
rm -f "$TEST_ROOT/started-hubuum-api" "$TEST_ROOT/started-hubuum-api-standby"
: > "$COMMAND_LOG"
hubuum_rollout
assert_commands "$TEST_ROOT/expected-offline.log"
FAKE_UNHEALTHY_SERVICES=""
export FAKE_MIGRATION_MODE="offline"

FAKE_MIGRATION_FAIL="true"
: > "$COMMAND_LOG"
if hubuum_rollout; then
  echo "failed offline migration unexpectedly succeeded" >&2
  exit 1
fi
cat > "$TEST_ROOT/expected-offline-failure.log" <<EOF
compose --env-file .env -f compose.yml stop --timeout 75 hubuum-api hubuum-api-standby hubuum-restore-executor
compose --env-file .env -f compose.yml run --rm --no-deps -T hubuum-migrate --migrate
EOF
assert_commands "$TEST_ROOT/expected-offline-failure.log"
FAKE_MIGRATION_FAIL="false"

for FAKE_MIGRATION_MODE in invalid offline; do
  export FAKE_PREFLIGHT_STATUS=0
  [[ "$FAKE_MIGRATION_MODE" != "offline" ]] || FAKE_PREFLIGHT_STATUS=1
  : > "$COMMAND_LOG"
  if hubuum_rollout; then
    echo "invalid or failed preflight unexpectedly succeeded" >&2
    exit 1
  fi
  ! grep -v -- '--migration-mode' "$COMMAND_LOG" | grep -Eq '(^| )(run|up|exec|start|stop|rm) '
done

echo "Single-host rolling and offline update tests passed"
