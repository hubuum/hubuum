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
    printf '[{"address":"hubuum-api:8080","num_requests":0,"fails":1}]\n'
    printf '%s\n' "$((failure_polls - 1))" > "$failure_polls_file"
  else
    printf '[{"address":"hubuum-api:8080","num_requests":0,"fails":0}]\n'
  fi
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

if [[ "$*" == *" ps -q"* ]]; then
  [[ "$*" == *" ps -q" ]] || {
    echo "service arguments to compose ps are unsupported" >&2
    exit 2
  }

  for service in caddy postgres valkey hubuum-api hubuum-api-standby hubuum-web hubuum-web-standby; do
    if [[ "$service" == "caddy" && "$FAKE_CADDY_RUNNING" != "true" && ! -e "$TEST_ROOT/started-caddy" ]]; then
      continue
    fi
    if [[ " ${FAKE_MISSING_SERVICES:-} " != *" $service "* || -e "$TEST_ROOT/started-$service" ]]; then
      printf 'container-%s\n' "$service"
    fi
  done
fi

if [[ "$*" == *" up "* ]]; then
  service="${*: -1}"
  touch "$TEST_ROOT/started-$service"
fi
EOF
chmod +x "$FAKE_ENGINE"

FAKE_CADDY_DEPENDENCIES="false"
FAKE_CADDY_RELOAD_FAIL="false"
export COMMAND_LOG FAKE_CADDY_DEPENDENCIES FAKE_CADDY_RELOAD_FAIL
export FAKE_CADDY_RUNNING TEST_ROOT
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

assert_caddy_upstream_status_eligibility() {
  local expected="$1"
  local upstreams="$2"
  local actual="false"

  if hubuum_caddy_upstream_status_is_eligible "$upstreams"; then
    actual="true"
  fi
  [[ "$actual" == "$expected" ]] || {
    printf 'unexpected Caddy upstream eligibility for %s\n' "$upstreams" >&2
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

assert_caddy_upstream_status_eligibility "false" '[]'
assert_caddy_upstream_status_eligibility "false" '[{"address":"hubuum-api:8080"}]'
assert_caddy_upstream_status_eligibility "true" '[{"fails": 0}, {"fails":0}]'
assert_caddy_upstream_status_eligibility "false" '[{"fails":0}, {"fails":2}]'

FAKE_CADDY_RUNNING="true"
FAKE_CADDY_DEPENDENCIES="true"
INSTALL_MODE="all"
: > "$COMMAND_LOG"
hubuum_rollout
cat > "$TEST_ROOT/expected-rolling.log" <<EOF
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate caddy
compose --env-file .env -f compose.yml run --rm --no-deps -T --entrypoint /usr/local/bin/hubuum-admin hubuum-api --migrate
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api-standby
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web-standby
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api
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
compose --env-file .env -f compose.yml run --rm --no-deps -T --entrypoint /usr/local/bin/hubuum-admin hubuum-api --migrate
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api-standby
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
EOF
assert_commands "$TEST_ROOT/expected-reload.log"

FAKE_CADDY_RUNNING="true"
FAKE_UNHEALTHY_SERVICES="hubuum-api"
INSTALL_MODE="all"
rm -f "$TEST_ROOT/started-hubuum-api"
: > "$COMMAND_LOG"
hubuum_rollout
cat > "$TEST_ROOT/expected-recovery.log" <<EOF
compose --env-file .env -f compose.yml run --rm --no-deps -T --entrypoint /usr/local/bin/hubuum-admin hubuum-api --migrate
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
compose --env-file .env -f compose.yml run --rm --no-deps -T --entrypoint /usr/local/bin/hubuum-admin hubuum-api --migrate
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api-standby
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-web-standby
compose --env-file .env -f compose.yml exec -T caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
compose --env-file .env -f compose.yml exec -T caddy wget -qO- http://127.0.0.1:2019/reverse_proxy/upstreams
compose --env-file .env -f compose.yml up -d --no-deps --force-recreate hubuum-api
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
compose --env-file .env -f compose.yml up -d hubuum-api
compose --env-file .env -f compose.yml up -d --no-deps hubuum-api-standby
compose --env-file .env -f compose.yml up -d --no-deps caddy
EOF
assert_commands "$TEST_ROOT/expected-initial.log"

printf '2\n' > "$TEST_ROOT/caddy-failure-polls"
: > "$COMMAND_LOG"
sleep() { :; }
hubuum_wait_for_caddy_upstreams 1
unset -f sleep
[[ "$(grep -c 'reverse_proxy/upstreams' "$COMMAND_LOG")" -eq 3 ]] || {
  echo "Caddy upstream wait did not poll until passive failures cleared" >&2
  exit 1
}
rm -f "$TEST_ROOT/caddy-failure-polls"

if timeout_output="$(hubuum_wait_for_caddy_upstreams invalid 2>&1)"; then
  echo "invalid Caddy upstream timeout unexpectedly succeeded" >&2
  exit 1
fi
[[ "$timeout_output" == "ERROR: Caddy upstream timeout must be a positive integer; got 'invalid'" ]]

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

echo "Single-host rolling update test passed"
