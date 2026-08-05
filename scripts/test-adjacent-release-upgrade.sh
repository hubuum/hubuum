#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
candidate_image="${HUBUUM_TEST_IMAGE:-hubuum-server:ci}"
previous_image="${HUBUUM_PREVIOUS_IMAGE:?HUBUUM_PREVIOUS_IMAGE must be an immutable release image digest}"
previous_tag="${HUBUUM_PREVIOUS_RELEASE_TAG:?HUBUUM_PREVIOUS_RELEASE_TAG must identify the adjacent stable release}"
postgres_image="${POSTGRES_TEST_IMAGE:-postgres:18.4@sha256:22c89fe0d0f507606260237fd55e51f6137f58b2d5bcf6152242b96d9fe8f9a4}"
report_root="${HUBUUM_COMPATIBILITY_REPORT_DIR:-$repository_root/compatibility-report}"
test_root="$(mktemp -d)"
project_name="hubuum-adjacent-${RANDOM}-${RANDOM}"
phase="initialize"
probe_pid=""
probe_stop_file="$test_root/stop-probe"
probe_log="$report_root/api-probes.tsv"
migration_log="$report_root/migration.log"
compose_log="$report_root/compose.log"
report_file="$report_root/report.json"
database_url="postgres://hubuum:adjacent-release@postgres/hubuum?sslmode=disable"
admin_token=""
collection_id=""
class_id=""
object_id=""
probe_task_id=""
migration_seconds=0
max_probe_latency_seconds=0
max_probe_outage_seconds=0
api_body=""
api_headers=""

[[ "$previous_image" =~ @sha256:[0-9a-f]{64}$ ]] || {
  echo "ERROR: HUBUUM_PREVIOUS_IMAGE must be pinned by sha256 digest" >&2
  exit 1
}
docker image inspect "$candidate_image" >/dev/null
docker image inspect "$previous_image" >/dev/null

mkdir -p "$report_root"
: > "$probe_log"
: > "$migration_log"
: > "$compose_log"

compose=(
  docker compose
  --project-name "$project_name"
  --env-file "$test_root/.env"
  --file "$test_root/compose.yml"
)

applied_migrations() {
  local migration_table
  local versions

  if ! "${compose[@]}" ps postgres >/dev/null 2>&1; then
    printf '[]'
    return
  fi
  migration_table="$(
    "${compose[@]}" exec -T postgres psql \
      --username hubuum --dbname hubuum --tuples-only --no-align \
      --command "SELECT to_regclass('public.__diesel_schema_migrations')" \
      2>/dev/null || true
  )"
  if [[ "$migration_table" != "__diesel_schema_migrations" ]]; then
    printf '[]'
    return
  fi
  versions="$(
    "${compose[@]}" exec -T postgres psql \
      --username hubuum --dbname hubuum --tuples-only --no-align \
      --command 'SELECT version FROM __diesel_schema_migrations ORDER BY version' \
      2>/dev/null || true
  )"
  jq --null-input --arg versions "$versions" \
    '$versions | split("\n") | map(select(length > 0))'
}

write_report() {
  local result="$1"
  local previous_digest
  local candidate_id
  local migrations

  previous_digest="${previous_image##*@}"
  candidate_id="$(docker image inspect "$candidate_image" --format '{{.Id}}' 2>/dev/null || true)"
  migrations="$(applied_migrations)"

  jq --null-input \
    --arg result "$result" \
    --arg phase "$phase" \
    --arg previous_tag "$previous_tag" \
    --arg previous_image "$previous_image" \
    --arg previous_digest "$previous_digest" \
    --arg candidate_image "$candidate_image" \
    --arg candidate_image_id "$candidate_id" \
    --arg candidate_sha "${GITHUB_SHA:-$(git -C "$repository_root" rev-parse HEAD)}" \
    --argjson migration_seconds "$migration_seconds" \
    --argjson max_probe_latency_seconds "$max_probe_latency_seconds" \
    --argjson max_probe_outage_seconds "$max_probe_outage_seconds" \
    --argjson migrations "$migrations" \
    '{
      result: $result,
      phase: $phase,
      previous_release: {tag: $previous_tag, image: $previous_image, digest: $previous_digest},
      candidate: {sha: $candidate_sha, image: $candidate_image, image_id: $candidate_image_id},
      migration: {
        seconds: $migration_seconds,
        max_api_latency_seconds: $max_probe_latency_seconds,
        max_observed_api_outage_seconds: $max_probe_outage_seconds,
        applied_versions: $migrations
      }
    }' > "$report_file"
}

stop_probe() {
  touch "$probe_stop_file"
  if [[ -n "$probe_pid" ]]; then
    wait "$probe_pid" 2>/dev/null || true
    probe_pid=""
  fi
}

cleanup() {
  local status=$?
  trap - EXIT
  set +e
  stop_probe
  "${compose[@]}" logs --no-color --timestamps > "$compose_log" 2>&1
  if [[ "$status" -eq 0 ]]; then
    write_report passed
  else
    write_report failed
    echo "Adjacent-release compatibility failed during phase '$phase'." >&2
    echo "Report: $report_file" >&2
    tail -n 200 "$compose_log" >&2
  fi
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1
  rm -rf "$test_root"
  exit "$status"
}
trap cleanup EXIT

cat > "$test_root/.env" <<EOF
HUBUUM_CANDIDATE_IMAGE=$candidate_image
HUBUUM_PREVIOUS_IMAGE=$previous_image
POSTGRES_TEST_IMAGE=$postgres_image
HUBUUM_DATABASE_URL=$database_url
EOF

cat > "$test_root/compose.yml" <<'EOF'
services:
  postgres:
    image: ${POSTGRES_TEST_IMAGE}
    environment:
      POSTGRES_DB: hubuum
      POSTGRES_USER: hubuum
      POSTGRES_PASSWORD: adjacent-release
      PGUSER: hubuum
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U hubuum -d hubuum"]
      interval: 1s
      timeout: 2s
      retries: 60

  previous-api:
    image: ${HUBUUM_PREVIOUS_IMAGE}
    command: ["--runtime-role", "api"]
    ports:
      - "127.0.0.1::8080"
    environment: &hubuum-environment
      HUBUUM_BIND_IP: 0.0.0.0
      HUBUUM_BIND_PORT: 8080
      HUBUUM_DATABASE_URL: ${HUBUUM_DATABASE_URL}
      HUBUUM_CLIENT_ALLOWLIST: "*"
      HUBUUM_LOG_LEVEL: info
      HUBUUM_TOKEN_HASH_KEY: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
    depends_on:
      postgres:
        condition: service_healthy
    healthcheck: &hubuum-healthcheck
      test: ["CMD-SHELL", "wget --quiet --output-document=/dev/null http://127.0.0.1:8080/readyz"]
      interval: 1s
      timeout: 2s
      retries: 60

  previous-worker:
    image: ${HUBUUM_PREVIOUS_IMAGE}
    command: ["--runtime-role", "worker"]
    environment: *hubuum-environment
    depends_on:
      postgres:
        condition: service_healthy

  candidate-api:
    image: ${HUBUUM_CANDIDATE_IMAGE}
    command: ["--runtime-role", "api"]
    ports:
      - "127.0.0.1::8080"
    environment: *hubuum-environment
    depends_on:
      postgres:
        condition: service_healthy
    healthcheck: *hubuum-healthcheck

  candidate-worker:
    image: ${HUBUUM_CANDIDATE_IMAGE}
    command: ["--runtime-role", "worker"]
    environment: *hubuum-environment
    depends_on:
      postgres:
        condition: service_healthy
EOF

service_url() {
  local service="$1"
  local address

  address="$("${compose[@]}" port "$service" 8080)"
  printf 'http://%s' "$address"
}

wait_for_ready() {
  local service="$1"
  local url

  url="$(service_url "$service")"
  for _ in $(seq 1 90); do
    if curl --fail --silent --show-error --max-time 2 "$url/readyz" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: $service did not become ready" >&2
  return 1
}

api_request() {
  local service="$1"
  local method="$2"
  local path="$3"
  local payload="${4:-}"
  local etag="${5:-}"
  local status
  local url
  local body_file="$test_root/api-body.json"
  local headers_file="$test_root/api-headers.txt"
  local args=(
    --silent --show-error
    --request "$method"
    --output "$body_file"
    --dump-header "$headers_file"
    --write-out '%{http_code}'
    --header 'Accept: application/json'
  )

  url="$(service_url "$service")"
  if [[ -n "$admin_token" ]]; then
    args+=(--header "Authorization: Bearer $admin_token")
  fi
  if [[ -n "$payload" ]]; then
    args+=(--header 'Content-Type: application/json' --data "$payload")
  fi
  if [[ -n "$etag" ]]; then
    args+=(--header "If-Match: $etag")
  fi

  status="$(curl "${args[@]}" "$url$path")"
  api_body="$(cat "$body_file")"
  api_headers="$(cat "$headers_file")"
  if [[ ! "$status" =~ ^2[0-9][0-9]$ ]]; then
    echo "ERROR: $service $method $path returned HTTP $status" >&2
    printf '%s\n' "$api_body" >&2
    return 1
  fi
}

json_id() {
  jq --exit-status --raw-output '.id' <<< "$api_body"
}

seed_previous_release() {
  local group_id
  local user_id
  local service_account_id
  local child_collection_id
  local related_class_id
  local related_object_id
  local class_relation_id
  local sink_id
  local template_id
  local task_id

  api_request previous-api POST /api/v1/iam/groups \
    '{"groupname":"compat-operators","description":"adjacent release operators"}'
  group_id="$(json_id)"

  api_request previous-api POST /api/v1/iam/users \
    '{"name":"compat-user","password":"compatibility-password","proper_name":"Compatibility User","email":"compat@example.com"}'
  user_id="$(json_id)"
  api_request previous-api POST "/api/v1/iam/groups/$group_id/members/$user_id"

  api_request previous-api POST /api/v1/iam/service-accounts \
    "{\"name\":\"compat-agent\",\"description\":\"adjacent release fixture\",\"owner_group_id\":$group_id}"
  service_account_id="$(json_id)"
  api_request previous-api POST "/api/v1/iam/principals/$service_account_id/tokens" \
    '{"name":"compat-agent-token","description":"adjacent release fixture","scope":{"permissions":["ReadCollection","ReadClass","ReadObject"]}}'
  jq --exit-status --raw-output '.token' <<< "$api_body" >/dev/null

  api_request previous-api POST /api/v1/collections \
    "{\"name\":\"compat-root\",\"description\":\"adjacent release root\",\"group_id\":$group_id}"
  collection_id="$(json_id)"
  api_request previous-api POST /api/v1/collections \
    "{\"name\":\"compat-child\",\"description\":\"adjacent release child\",\"group_id\":$group_id,\"parent_collection_id\":$collection_id}"
  child_collection_id="$(json_id)"

  api_request previous-api POST /api/v1/classes \
    "{\"name\":\"compat-host\",\"description\":\"host fixture\",\"collection_id\":$collection_id,\"json_schema\":{\"type\":\"object\",\"properties\":{\"owner\":{\"type\":\"string\"}},\"required\":[\"owner\"]},\"validate_schema\":true}"
  class_id="$(json_id)"
  api_request previous-api POST /api/v1/classes \
    "{\"name\":\"compat-room\",\"description\":\"room fixture\",\"collection_id\":$child_collection_id,\"json_schema\":null,\"validate_schema\":false}"
  related_class_id="$(json_id)"

  api_request previous-api POST "/api/v1/classes/$class_id/" \
    "{\"name\":\"compat-host-01\",\"description\":\"host fixture\",\"collection_id\":$collection_id,\"hubuum_class_id\":$class_id,\"data\":{\"owner\":\"previous\"}}"
  object_id="$(json_id)"
  api_request previous-api POST "/api/v1/classes/$related_class_id/" \
    "{\"name\":\"compat-room-01\",\"description\":\"room fixture\",\"collection_id\":$child_collection_id,\"hubuum_class_id\":$related_class_id,\"data\":{}}"
  related_object_id="$(json_id)"

  api_request previous-api POST /api/v1/relations/classes \
    "{\"from_hubuum_class_id\":$class_id,\"to_hubuum_class_id\":$related_class_id,\"forward_template_alias\":\"rooms\",\"reverse_template_alias\":\"hosts\"}"
  class_relation_id="$(json_id)"
  api_request previous-api POST /api/v1/relations/objects \
    "{\"from_hubuum_object_id\":$object_id,\"to_hubuum_object_id\":$related_object_id,\"class_relation_id\":$class_relation_id}"

  api_request previous-api POST "/api/v1/classes/$class_id/computed-fields" \
    '{"key":"owner_copy","label":"Owner copy","description":"compatibility fixture","operation":{"type":"first_non_null","paths":["/owner"]},"result_type":"string","enabled":true}'

  api_request previous-api POST /api/v1/event-sinks \
    '{"name":"compat-sink","kind":"webhook","config":{},"enabled":false}'
  sink_id="$(json_id)"
  api_request previous-api POST "/api/v1/collections/$collection_id/event-subscriptions" \
    "{\"sink_id\":$sink_id,\"name\":\"compat-events\",\"description\":\"adjacent release fixture\",\"entity_types\":[\"object\"],\"actions\":[\"created\",\"updated\"],\"filter\":{},\"routing\":{},\"enabled\":false}"

  api_request previous-api POST /api/v1/remote-targets \
    "{\"collection_id\":$collection_id,\"class_id\":$class_id,\"name\":\"compat-target\",\"description\":\"adjacent release fixture\",\"method\":\"post\",\"url_template\":\"https://example.invalid/{{ object.id }}\",\"headers_template\":{},\"body_template\":null,\"auth_config\":{\"type\":\"none\"},\"timeout_ms\":1000,\"allowed_subject_types\":[\"object\"],\"enabled\":false}"

  api_request previous-api POST /api/v1/export-templates \
    "{\"collection_id\":$collection_id,\"class_id\":$class_id,\"name\":\"compat-export\",\"description\":\"adjacent release fixture\",\"content_type\":\"text/plain\",\"template\":\"{% for item in items %}{{ item.name }}\\n{% endfor %}\",\"kind\":\"export\",\"scope_kind\":\"objects_in_class\"}"
  template_id="$(json_id)"
  api_request previous-api POST "/api/v1/export-templates/$template_id/exports" '{}'
  task_id="$(json_id)"
  probe_task_id="$task_id"
  for _ in $(seq 1 100); do
    api_request previous-api GET "/api/v1/tasks/$task_id"
    case "$(jq --raw-output '.status' <<< "$api_body")" in
      succeeded)
        break
        ;;
      failed|cancelled)
        echo "ERROR: previous-release export task failed" >&2
        return 1
        ;;
    esac
    sleep 0.1
  done
  [[ "$(jq --raw-output '.status' <<< "$api_body")" == "succeeded" ]] || {
    echo "ERROR: previous-release export task did not complete" >&2
    return 1
  }

  api_request previous-api GET "/api/v1/collections/$collection_id/events?limit=50"
  [[ "$(jq 'length' <<< "$api_body")" -gt 0 ]] || {
    echo "ERROR: representative fixture did not create audit events" >&2
    return 1
  }
}

probe_previous_api() {
  local url
  local status
  local duration
  local result

  url="$(service_url previous-api)/api/v1/tasks/$probe_task_id"
  while [[ ! -e "$probe_stop_file" ]]; do
    if result="$(
      curl --silent --show-error --output /dev/null \
        --write-out '%{http_code}\t%{time_total}' \
        --connect-timeout 1 --max-time 3 \
        --header "Authorization: Bearer $admin_token" \
        "$url" 2>/dev/null
    )"; then
      status="${result%%$'\t'*}"
      duration="${result#*$'\t'}"
    else
      status="curl-error"
      duration="${result#*$'\t'}"
      if [[ "$result" != *$'\t'* || ! "$duration" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
        duration="3"
      fi
    fi
    printf '%s\t%s\n' "$status" "$duration" >> "$probe_log"
    sleep 0.1
  done
}

analyze_probes() {
  local failures

  failures="$(awk '$1 != 200 {count++} END {print count + 0}' "$probe_log")"
  max_probe_latency_seconds="$(awk 'BEGIN {max=0} $2 > max {max=$2} END {printf "%.6f", max}' "$probe_log")"
  max_probe_outage_seconds="$(
    awk '
      $1 == 200 {current=0; next}
      {current += $2 + 0.1; if (current > max) max=current}
      END {printf "%.6f", max + 0}
    ' "$probe_log"
  )"
  if ((failures > 0)); then
    echo "ERROR: previous API had $failures failed request(s) during candidate migration" >&2
    return 1
  fi
}

phase="start-previous-release"
"${compose[@]}" up -d postgres
for _ in $(seq 1 60); do
  if "${compose[@]}" exec -T postgres pg_isready --username hubuum --dbname hubuum >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
"${compose[@]}" exec -T postgres pg_isready --username hubuum --dbname hubuum >/dev/null
"${compose[@]}" run --rm --no-deps --entrypoint /usr/local/bin/hubuum-admin previous-api --migrate
"${compose[@]}" up -d previous-api previous-worker
wait_for_ready previous-api

phase="authenticate-previous-release"
password_output="$(
  "${compose[@]}" run --rm --no-deps --entrypoint /usr/local/bin/hubuum-admin \
    previous-api --reset-password admin
)"
admin_password="${password_output##*reset to: }"
[[ -n "$admin_password" && "$admin_password" != "$password_output" ]]
api_request previous-api POST /api/v0/auth/login \
  "{\"name\":\"admin\",\"password\":\"$admin_password\"}"
admin_token="$(jq --exit-status --raw-output '.token' <<< "$api_body")"

phase="seed-previous-release"
seed_previous_release

phase="drain-previous-worker"
"${compose[@]}" stop --timeout 75 previous-worker
[[ -z "$("${compose[@]}" ps --status running -q previous-worker)" ]]

phase="candidate-migration"
rm -f "$probe_stop_file"
probe_previous_api &
probe_pid=$!
migration_started=$SECONDS
"${compose[@]}" run --rm --no-deps --entrypoint /usr/local/bin/hubuum-admin \
  candidate-api --migrate > "$migration_log" 2>&1
migration_seconds=$((SECONDS - migration_started))
stop_probe
analyze_probes

phase="mixed-version"
"${compose[@]}" up -d candidate-api
wait_for_ready candidate-api
api_request previous-api PATCH "/api/v1/collections/$collection_id" \
  '{"description":"written by previous release after migration"}'
api_request candidate-api GET "/api/v1/collections/$collection_id"
[[ "$(jq --raw-output '.description' <<< "$api_body")" == "written by previous release after migration" ]]
etag="$(awk 'BEGIN {IGNORECASE=1} /^etag:/ {sub(/\r$/, "", $2); print $2}' <<< "$api_headers")"
[[ -n "$etag" ]]
api_request candidate-api PATCH "/api/v1/collections/$collection_id" \
  '{"description":"written by candidate during overlap"}' "$etag"
api_request previous-api GET "/api/v1/collections/$collection_id"
[[ "$(jq --raw-output '.description' <<< "$api_body")" == "written by candidate during overlap" ]]
api_request candidate-api GET "/api/v1/classes/$class_id/$object_id"
[[ "$(jq --raw-output '.data.owner' <<< "$api_body")" == "previous" ]]

phase="candidate-rollout"
"${compose[@]}" stop --timeout 75 previous-api
"${compose[@]}" up -d candidate-worker
sleep 2
[[ -n "$("${compose[@]}" ps --status running -q candidate-worker)" ]]

phase="application-rollback"
"${compose[@]}" stop --timeout 75 candidate-api candidate-worker
"${compose[@]}" up -d previous-api
wait_for_ready previous-api
api_request previous-api GET "/api/v1/collections/$collection_id"
[[ "$(jq --raw-output '.description' <<< "$api_body")" == "written by candidate during overlap" ]]
api_request previous-api PATCH "/api/v1/collections/$collection_id" \
  '{"description":"verified by previous release rollback"}'

phase="restore-candidate"
"${compose[@]}" stop --timeout 75 previous-api
"${compose[@]}" up -d candidate-api candidate-worker
wait_for_ready candidate-api
api_request candidate-api GET "/api/v1/collections/$collection_id"
[[ "$(jq --raw-output '.description' <<< "$api_body")" == "verified by previous release rollback" ]]
api_request candidate-api GET /readyz

phase="complete"
echo "Adjacent release compatibility passed: $previous_tag -> ${GITHUB_SHA:-local candidate}."
