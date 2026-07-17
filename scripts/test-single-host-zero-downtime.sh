#!/usr/bin/env bash
set -Eeuo pipefail

REPOSITORY_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENGINE_PATH="$(command -v docker)"
HUBUUM_TEST_IMAGE="${HUBUUM_TEST_IMAGE:-hubuum-server:ci}"
HUBUUM_OLD_TEST_IMAGE="${HUBUUM_OLD_TEST_IMAGE:-ghcr.io/hubuum/hubuum-server:v0.0.1@sha256:061fc4cb3af57e2db06c33012db93e60834d4618815e30af4ebb4aa05a21f445}"
POSTGRES_TEST_IMAGE="${POSTGRES_TEST_IMAGE:-postgres:18.4@sha256:22c89fe0d0f507606260237fd55e51f6137f58b2d5bcf6152242b96d9fe8f9a4}"
CADDY_TEST_IMAGE="${CADDY_TEST_IMAGE:-caddy:2-alpine}"
TEST_ROOT="$(mktemp -d)"
PROJECT_NAME="hubuum-zero-downtime-${RANDOM}-${RANDOM}"
PROBE_STOP_FILE="$TEST_ROOT/stop-probes"
READY_PROBES_FILE="$TEST_ROOT/enable-ready-probes"
PROBE_PIDS=""
HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS="${HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS:-12}"
INSTALL_MODE="backend"

docker image inspect "$HUBUUM_TEST_IMAGE" >/dev/null 2>&1 || {
  echo "ERROR: missing test image $HUBUUM_TEST_IMAGE" >&2
  echo "Build it first or set HUBUUM_TEST_IMAGE to an existing Hubuum image." >&2
  exit 1
}
docker image inspect "$HUBUUM_OLD_TEST_IMAGE" >/dev/null 2>&1 || docker pull "$HUBUUM_OLD_TEST_IMAGE"

COMPOSE_CMD=(
  "$ENGINE_PATH" compose
  --project-name "$PROJECT_NAME"
  --env-file "$TEST_ROOT/.env"
  --file "$TEST_ROOT/compose.yml"
)
BASE_COMPOSE_CMD=("${COMPOSE_CMD[@]}")

stop_probes() {
  touch "$PROBE_STOP_FILE"
  local pid
  for pid in $PROBE_PIDS; do
    wait "$pid" 2>/dev/null || true
  done
  PROBE_PIDS=""
}

cleanup() {
  local status=$?
  trap - EXIT
  set +e
  stop_probes
  if [[ "$status" -ne 0 ]]; then
    echo "Live zero-downtime test failed; container state and logs follow." >&2
    "${BASE_COMPOSE_CMD[@]}" ps >&2
    "${BASE_COMPOSE_CMD[@]}" logs --no-color --tail 200 >&2
  fi
  "${BASE_COMPOSE_CMD[@]}" down --volumes --remove-orphans >/dev/null 2>&1
  if [[ "${HUBUUM_KEEP_ZERO_DOWNTIME_TEST_ROOT:-false}" == "true" ]]; then
    echo "Preserved test files in $TEST_ROOT" >&2
  else
    rm -rf "$TEST_ROOT"
  fi
  exit "$status"
}
trap cleanup EXIT

write_environment() {
  local database_url="$1"

  cat > "$TEST_ROOT/.env" <<EOF
HUBUUM_TEST_IMAGE=$HUBUUM_TEST_IMAGE
HUBUUM_OLD_TEST_IMAGE=$HUBUUM_OLD_TEST_IMAGE
POSTGRES_TEST_IMAGE=$POSTGRES_TEST_IMAGE
CADDY_TEST_IMAGE=$CADDY_TEST_IMAGE
HUBUUM_ZERO_DOWNTIME_DATABASE_URL=$database_url
EOF
}

DATABASE_URL="postgres://hubuum:zero-downtime-test@postgres/hubuum?sslmode=disable"
write_environment "$DATABASE_URL"

write_old_caddyfile() {
  cat > "$TEST_ROOT/Caddyfile" <<'EOF'
{
    auto_https off
}

:8080 {
    reverse_proxy hubuum-api:8080 {
        health_uri /healthz
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
}

write_candidate_caddyfile() {
  local caddyfile_temp

  caddyfile_temp="$(mktemp "$TEST_ROOT/.Caddyfile.XXXXXX")"
  cat > "$caddyfile_temp" <<'EOF'
{
    auto_https off
}

:8080 {
    reverse_proxy hubuum-api:8080 hubuum-api-standby:8080 {
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

  # Match the installer: update the contents without replacing the inode that
  # the already-running Caddy container bind-mounted from the legacy fixture.
  cp "$caddyfile_temp" "$TEST_ROOT/Caddyfile"
  rm -f "$caddyfile_temp"
}

write_old_caddyfile

cat > "$TEST_ROOT/compose.yml" <<'EOF'
services:
  postgres:
    image: ${POSTGRES_TEST_IMAGE}
    environment:
      POSTGRES_DB: hubuum
      POSTGRES_USER: hubuum
      POSTGRES_PASSWORD: zero-downtime-test
      PGUSER: hubuum
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U hubuum -d hubuum"]
      interval: 2s
      timeout: 2s
      retries: 30

  hubuum-api: &hubuum-api
    image: ${HUBUUM_TEST_IMAGE}
    stop_grace_period: 75s
    read_only: true
    tmpfs:
      - /tmp:size=16m,mode=1777
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    environment:
      HUBUUM_BIND_IP: 0.0.0.0
      HUBUUM_BIND_PORT: 8080
      HUBUUM_DATABASE_URL: ${HUBUUM_ZERO_DOWNTIME_DATABASE_URL}
      HUBUUM_CLIENT_ALLOWLIST: "*"
      HUBUUM_LOG_LEVEL: info
    depends_on:
      postgres:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "wget --quiet --output-document=/dev/null http://127.0.0.1:8080/readyz"]
      interval: 2s
      timeout: 2s
      retries: 30

  hubuum-api-standby:
    <<: *hubuum-api
    command: ["--runtime-role", "api"]

  caddy:
    image: ${CADDY_TEST_IMAGE}
    ports:
      - "127.0.0.1::8080"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
    depends_on:
      - hubuum-api
      - hubuum-api-standby
EOF

cat > "$TEST_ROOT/unhealthy-standby.yml" <<'EOF'
services:
  hubuum-api-standby:
    stop_grace_period: 2s
    environment:
      HUBUUM_DATABASE_URL: postgres://hubuum:zero-downtime-test@127.0.0.1:1/hubuum
EOF

cat > "$TEST_ROOT/unhealthy-primary.yml" <<'EOF'
services:
  hubuum-api:
    stop_grace_period: 2s
    environment:
      HUBUUM_BIND_PORT: 18081
EOF

cat > "$TEST_ROOT/old-version.yml" <<'EOF'
services:
  hubuum-api:
    image: ${HUBUUM_OLD_TEST_IMAGE}
    healthcheck:
      # v0.0.1 exposes /healthz but does not contain wget or curl. The external
      # Caddy probe below is the HTTP readiness gate for this historical image.
      test: ["CMD-SHELL", "kill -0 1"]
      interval: 2s
      timeout: 2s
      retries: 30
EOF

service_id() {
  "${BASE_COMPOSE_CMD[@]}" ps -q "$1"
}

assert_same_id() {
  local description="$1"
  local before="$2"
  local after="$3"
  [[ -n "$before" && "$before" == "$after" ]] || {
    echo "ERROR: $description was unexpectedly replaced" >&2
    return 1
  }
}

assert_changed_id() {
  local description="$1"
  local before="$2"
  local after="$3"
  [[ -n "$before" && -n "$after" && "$before" != "$after" ]] || {
    echo "ERROR: $description was not replaced" >&2
    return 1
  }
}

wait_for_public_endpoint() {
  local path="$1"
  local attempt
  for (( attempt = 1; attempt <= 60; attempt++ )); do
    if curl --fail --silent --show-error --max-time 2 "$PUBLIC_URL/$path" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: Caddy did not expose $path within 60 seconds" >&2
  return 1
}

probe_worker() {
  local worker="$1"
  local path
  local status

  while [[ ! -e "$PROBE_STOP_FILE" ]]; do
    path="healthz"
    if (( worker % 2 == 0 )) && [[ -e "$READY_PROBES_FILE" ]]; then
      path="readyz"
    fi

    if status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
      --connect-timeout 1 --max-time 2 "$PUBLIC_URL/$path" \
      2>> "$TEST_ROOT/probe-${worker}.stderr")"; then
      printf '%s %s\n' "$path" "$status" >> "$TEST_ROOT/probe-${worker}.log"
    else
      printf '%s curl-error\n' "$path" >> "$TEST_ROOT/probe-${worker}.log"
    fi
    sleep 0.05
  done
}

start_probes() {
  rm -f "$PROBE_STOP_FILE"
  local worker
  for worker in 1 2 3 4; do
    : > "$TEST_ROOT/probe-${worker}.log"
    : > "$TEST_ROOT/probe-${worker}.stderr"
    probe_worker "$worker" &
    PROBE_PIDS="$PROBE_PIDS $!"
  done
}

assert_probes_succeeded() {
  local health_request_count
  local ready_request_count
  local request_count
  local failures="$TEST_ROOT/probe-failures.log"

  request_count="$(awk 'END { print NR }' "$TEST_ROOT"/probe-*.log)"
  health_request_count="$(awk '$1 == "healthz" && $2 == "200" { count++ } END { print count + 0 }' "$TEST_ROOT"/probe-*.log)"
  ready_request_count="$(awk '$1 == "readyz" && $2 == "200" { count++ } END { print count + 0 }' "$TEST_ROOT"/probe-*.log)"
  grep -HnvE '^(healthz|readyz) 200$' "$TEST_ROOT"/probe-*.log > "$failures" || true

  if [[ -s "$failures" ]]; then
    echo "ERROR: public HTTP requests failed during the rollout:" >&2
    cat "$failures" >&2
    return 1
  fi
  if (( request_count < 100 )); then
    echo "ERROR: expected at least 100 probe requests, observed $request_count" >&2
    return 1
  fi
  if (( health_request_count == 0 || ready_request_count == 0 )); then
    echo "ERROR: expected successful health and readiness probes, observed healthz=$health_request_count readyz=$ready_request_count" >&2
    return 1
  fi
  echo "Observed $request_count successful HTTP requests with no failures (healthz=$health_request_count, readyz=$ready_request_count)."
}

expect_rollout_failure() {
  local description="$1"
  local output="$TEST_ROOT/${description}.log"
  local status

  set +e
  (
    set -Eeuo pipefail
    hubuum_rollout
  ) > "$output" 2>&1
  status=$?
  set -e

  if [[ "$status" -eq 0 ]]; then
    echo "ERROR: $description rollout unexpectedly succeeded" >&2
    cat "$output" >&2
    return 1
  fi
}

migration_count() {
  "${BASE_COMPOSE_CMD[@]}" exec -T postgres \
    psql --username hubuum --dbname hubuum --tuples-only --no-align \
    --command 'SELECT count(*) FROM __diesel_schema_migrations'
}

migration_is_applied() {
  local version="$1"
  "${BASE_COMPOSE_CMD[@]}" exec -T postgres \
    psql --username hubuum --dbname hubuum --tuples-only --no-align \
    --command "SELECT EXISTS (SELECT 1 FROM __diesel_schema_migrations WHERE version = '$version')"
}

container_uses_candidate_image() {
  local service="$1"
  local container_id
  local candidate_image_id

  container_id="$(service_id "$service")"
  candidate_image_id="$(docker image inspect "$HUBUUM_TEST_IMAGE" --format '{{.Id}}')"
  [[ "$(docker inspect "$container_id" --format '{{.Image}}')" == "$candidate_image_id" ]]
}

# shellcheck source=scripts/single-host-rollout.sh
source "$REPOSITORY_ROOT/scripts/single-host-rollout.sh"

echo "Starting the live single-host fixture on Hubuum v0.0.1..."
COMPOSE_CMD=("${BASE_COMPOSE_CMD[@]}" --file "$TEST_ROOT/old-version.yml")
"${COMPOSE_CMD[@]}" up -d hubuum-api
hubuum_wait_for_healthy hubuum-api "$HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS"
"${COMPOSE_CMD[@]}" up -d --no-deps caddy
COMPOSE_CMD=("${BASE_COMPOSE_CMD[@]}")
PUBLIC_ADDRESS="$("${BASE_COMPOSE_CMD[@]}" port caddy 8080)"
PUBLIC_URL="http://${PUBLIC_ADDRESS}"
wait_for_public_endpoint healthz

old_primary_id="$(service_id hubuum-api)"
initial_caddy_id="$(service_id caddy)"
initial_postgres_id="$(service_id postgres)"
old_migration_count="$(migration_count)"
expected_migration_version="$(find "$REPOSITORY_ROOT/migrations" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | sort | tail -n 1 | cut -d_ -f1 | tr -d '-')"
[[ "$(migration_is_applied "$expected_migration_version")" == "f" ]] || {
  echo "ERROR: v0.0.1 unexpectedly applied candidate migration $expected_migration_version" >&2
  exit 1
}

start_probes

echo "Migrating the v0.0.1 database and rolling to the candidate image..."
write_candidate_caddyfile
hubuum_rollout
touch "$READY_PROBES_FILE"
wait_for_public_endpoint readyz

candidate_migration_count="$(migration_count)"
(( candidate_migration_count > old_migration_count )) || {
  echo "ERROR: candidate did not apply migrations after v0.0.1" >&2
  exit 1
}
echo "Candidate applied $((candidate_migration_count - old_migration_count)) migration(s) after v0.0.1."
[[ "$(migration_is_applied "$expected_migration_version")" == "t" ]] || {
  echo "ERROR: candidate migration $expected_migration_version was not applied" >&2
  exit 1
}
assert_changed_id "v0.0.1 primary API" "$old_primary_id" "$(service_id hubuum-api)"
container_uses_candidate_image hubuum-api || {
  echo "ERROR: primary API does not use the candidate image" >&2
  exit 1
}
container_uses_candidate_image hubuum-api-standby || {
  echo "ERROR: standby API does not use the candidate image" >&2
  exit 1
}
assert_same_id "Caddy during version upgrade" "$initial_caddy_id" "$(service_id caddy)"
assert_same_id "PostgreSQL during version upgrade" "$initial_postgres_id" "$(service_id postgres)"

initial_primary_id="$(service_id hubuum-api)"
initial_standby_id="$(service_id hubuum-api-standby)"

echo "Verifying that a failed migration leaves both API replicas untouched..."
write_environment "postgres://hubuum:zero-downtime-test@127.0.0.1:1/hubuum"
expect_rollout_failure migration-failure
write_environment "$DATABASE_URL"
assert_same_id "primary API after migration failure" "$initial_primary_id" "$(service_id hubuum-api)"
assert_same_id "standby API after migration failure" "$initial_standby_id" "$(service_id hubuum-api-standby)"

echo "Verifying that an unhealthy standby aborts before replacing the primary..."
COMPOSE_CMD=("${BASE_COMPOSE_CMD[@]}" --file "$TEST_ROOT/unhealthy-standby.yml")
expect_rollout_failure standby-failure
COMPOSE_CMD=("${BASE_COMPOSE_CMD[@]}")
assert_same_id "primary API after standby failure" "$initial_primary_id" "$(service_id hubuum-api)"
assert_same_id "Caddy after standby failure" "$initial_caddy_id" "$(service_id caddy)"
assert_same_id "PostgreSQL after standby failure" "$initial_postgres_id" "$(service_id postgres)"

echo "Recovering the standby and performing a successful live rollout..."
hubuum_roll_service hubuum-api-standby
before_rollout_primary_id="$(service_id hubuum-api)"
before_rollout_standby_id="$(service_id hubuum-api-standby)"
hubuum_rollout

echo "Verifying that retrying a failed primary replacement preserves the healthy standby..."
COMPOSE_CMD=("${BASE_COMPOSE_CMD[@]}" --file "$TEST_ROOT/unhealthy-primary.yml")
expect_rollout_failure primary-failure
COMPOSE_CMD=("${BASE_COMPOSE_CMD[@]}")
hubuum_service_is_healthy hubuum-api-standby || {
  echo "ERROR: standby API was not healthy after the primary replacement failed" >&2
  exit 1
}
hubuum_rollout

sleep 2
stop_probes
assert_probes_succeeded
wait_for_public_endpoint readyz

assert_changed_id "primary API" "$before_rollout_primary_id" "$(service_id hubuum-api)"
assert_changed_id "standby API" "$before_rollout_standby_id" "$(service_id hubuum-api-standby)"
assert_same_id "Caddy" "$initial_caddy_id" "$(service_id caddy)"
assert_same_id "PostgreSQL" "$initial_postgres_id" "$(service_id postgres)"

echo "Live single-host zero-downtime test passed."
