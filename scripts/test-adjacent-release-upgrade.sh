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
restore_database_url="postgres://hubuum:adjacent-release@postgres/hubuum_restore?sslmode=disable"
history_free_restore_database_url="postgres://hubuum:adjacent-release@postgres/hubuum_restore_no_history?sslmode=disable"
current_restore_database_url="postgres://hubuum:adjacent-release@postgres/hubuum_restore_current?sslmode=disable"
adjacent_backup_file="$test_root/adjacent-backup.json"
history_free_backup_file="$test_root/adjacent-backup-no-history.json"
current_backup_file="$test_root/current-backup.json"
adjacent_restore_report="$report_root/adjacent-backup-restore.json"
history_free_restore_report="$report_root/history-free-backup-restore.json"
current_restore_report="$report_root/current-backup-restore.json"
admin_token=""
collection_id=""
class_id=""
object_id=""
probe_task_id=""
group_id=""
user_id=""
related_class_id=""
related_object_id=""
class_relation_id=""
template_id=""
migration_seconds=0
max_probe_latency_seconds=0
max_probe_outage_seconds=0
adjacent_backup_generation_ms=0
history_free_backup_generation_ms=0
current_backup_generation_ms=0
rebuild_duration_ms=0
restored_readiness=false
restored_authentication=false
restored_state_reads=false
restored_history_read=false
restored_task_read=false
restored_computed_rebuild=false
restored_token_exclusion=false
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

  if ! "${compose[@]}" exec -T postgres pg_isready \
    --host 127.0.0.1 --username hubuum --dbname hubuum >/dev/null 2>&1; then
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
  local adjacent_restore
  local history_free_restore
  local current_restore

  previous_digest="${previous_image##*@}"
  candidate_id="$(docker image inspect "$candidate_image" --format '{{.Id}}' 2>/dev/null || true)"
  migrations="$(applied_migrations)"
  adjacent_restore="null"
  history_free_restore="null"
  current_restore="null"
  if [[ -s "$adjacent_restore_report" ]] && \
    jq --exit-status '.' "$adjacent_restore_report" >/dev/null 2>&1; then
    adjacent_restore="$(jq '.' "$adjacent_restore_report")"
  fi
  if [[ -s "$history_free_restore_report" ]] && \
    jq --exit-status '.' "$history_free_restore_report" >/dev/null 2>&1; then
    history_free_restore="$(jq '.' "$history_free_restore_report")"
  fi
  if [[ -s "$current_restore_report" ]] && \
    jq --exit-status '.' "$current_restore_report" >/dev/null 2>&1; then
    current_restore="$(jq '.' "$current_restore_report")"
  fi

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
    --argjson adjacent_restore "$adjacent_restore" \
    --argjson history_free_restore "$history_free_restore" \
    --argjson current_restore "$current_restore" \
    --argjson adjacent_backup_generation_ms "$adjacent_backup_generation_ms" \
    --argjson history_free_backup_generation_ms "$history_free_backup_generation_ms" \
    --argjson current_backup_generation_ms "$current_backup_generation_ms" \
    --argjson rebuild_duration_ms "$rebuild_duration_ms" \
    --argjson restored_readiness "$restored_readiness" \
    --argjson restored_authentication "$restored_authentication" \
    --argjson restored_state_reads "$restored_state_reads" \
    --argjson restored_history_read "$restored_history_read" \
    --argjson restored_task_read "$restored_task_read" \
    --argjson restored_computed_rebuild "$restored_computed_rebuild" \
    --argjson restored_token_exclusion "$restored_token_exclusion" \
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
      },
      backup_recovery: {
        adjacent_release_with_history: (
          if $adjacent_restore == null then null else $adjacent_restore + {
            generation_duration_ms: $adjacent_backup_generation_ms
          } end
        ),
        adjacent_release_without_history: (
          if $history_free_restore == null then null else $history_free_restore + {
            generation_duration_ms: $history_free_backup_generation_ms
          } end
        ),
        current_release_with_history: (
          if $current_restore == null then null else $current_restore + {
            generation_duration_ms: $current_backup_generation_ms
          } end
        ),
        restored_application_smoke: {
          readiness: $restored_readiness,
          authentication_reestablished: $restored_authentication,
          authoritative_state_reads: $restored_state_reads,
          history_read: $restored_history_read,
          terminal_task_read: $restored_task_read,
          computed_rebuild: $restored_computed_rebuild,
          excluded_token_rejected: $restored_token_exclusion,
          rebuild_duration_ms: $rebuild_duration_ms
        }
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
  if ! "${compose[@]}" logs --no-color --timestamps > "$compose_log" 2>&1; then
    "${compose[@]}" logs --timestamps > "$compose_log" 2>&1
  fi
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
HUBUUM_COMPATIBILITY_DATABASE_URL=$database_url
HUBUUM_RESTORE_DATABASE_URL=$restore_database_url
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
      test: ["CMD-SHELL", "pg_isready -h 127.0.0.1 -U hubuum -d hubuum"]
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
      # v0.0.8 and earlier read the unprefixed name. Keep both variables so
      # this harness can exercise the actual adjacent-version boundary.
      DATABASE_URL: ${HUBUUM_COMPATIBILITY_DATABASE_URL}
      HUBUUM_DATABASE_URL: ${HUBUUM_COMPATIBILITY_DATABASE_URL}
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

  restored-api:
    image: ${HUBUUM_CANDIDATE_IMAGE}
    command: ["--runtime-role", "api"]
    ports:
      - "127.0.0.1::8080"
    environment:
      <<: *hubuum-environment
      DATABASE_URL: ${HUBUUM_RESTORE_DATABASE_URL}
      HUBUUM_DATABASE_URL: ${HUBUUM_RESTORE_DATABASE_URL}
    depends_on:
      postgres:
        condition: service_healthy
    healthcheck: *hubuum-healthcheck

  restored-worker:
    image: ${HUBUUM_CANDIDATE_IMAGE}
    command: ["--runtime-role", "worker"]
    environment:
      <<: *hubuum-environment
      DATABASE_URL: ${HUBUUM_RESTORE_DATABASE_URL}
      HUBUUM_DATABASE_URL: ${HUBUUM_RESTORE_DATABASE_URL}
    depends_on:
      postgres:
        condition: service_healthy
EOF

service_url() {
  local service="$1"
  local address

  address="$("${compose[@]}" port "$service" 8080)"
  if [[ "$address" =~ ^[0-9]+$ ]]; then
    address="127.0.0.1:$address"
  elif [[ "$address" == 0.0.0.0:* || "$address" == \[::\]:* ]]; then
    address="127.0.0.1:${address##*:}"
  fi
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

create_backup_artifact() {
  local service="$1"
  local include_history="$2"
  local output_path="$3"
  local duration_variable="$4"
  local task_id
  local status
  local headers_path="$output_path.headers"
  local expected_digest
  local actual_digest
  local started_ms
  local finished_ms

  started_ms="$(date +%s%3N)"
  api_request "$service" POST /api/v1/backups \
    "{\"include_history\":$include_history}"
  task_id="$(json_id)"
  for _ in $(seq 1 200); do
    api_request "$service" GET "/api/v1/tasks/$task_id"
    status="$(jq --raw-output '.status' <<< "$api_body")"
    case "$status" in
      succeeded)
        break
        ;;
      failed|cancelled)
        echo "ERROR: backup task $task_id ended as $status" >&2
        return 1
        ;;
    esac
    sleep 0.1
  done
  [[ "$status" == "succeeded" ]] || {
    echo "ERROR: backup task $task_id did not complete" >&2
    return 1
  }

  status="$(
    curl --silent --show-error \
      --output "$output_path" \
      --dump-header "$headers_path" \
      --write-out '%{http_code}' \
      --header "Authorization: Bearer $admin_token" \
      "$(service_url "$service")/api/v1/backups/$task_id/output"
  )"
  [[ "$status" == "200" ]] || {
    echo "ERROR: backup output $task_id returned HTTP $status" >&2
    return 1
  }
  expected_digest="$(
    awk 'BEGIN {IGNORECASE=1} /^x-hubuum-backup-sha256:/ {sub(/\r$/, "", $2); print $2}' \
      "$headers_path"
  )"
  actual_digest="$(sha256sum "$output_path" | awk '{print $1}')"
  [[ -n "$expected_digest" && "$expected_digest" == "$actual_digest" ]] || {
    echo "ERROR: downloaded backup digest does not match its response header" >&2
    return 1
  }
  finished_ms="$(date +%s%3N)"
  printf -v "$duration_variable" '%d' "$((finished_ms - started_ms))"
  rm -f "$headers_path"
}

verify_restore_artifact() {
  local backup_path="$1"
  local target_database_url="$2"
  local report_path="$3"
  local retention="$4"
  local backup_name
  local args

  backup_name="$(basename -- "$backup_path")"
  args=(
    --verify-backup "/verification/$backup_name"
    --restore-test-database-url "$target_database_url"
    --log-level error
    --json
  )
  if [[ "$retention" == "keep" ]]; then
    args+=(--keep-restore-test-database)
  fi
  "${compose[@]}" run --rm --no-deps -T \
    --user 0:0 \
    --volume "$test_root:/verification:ro,Z" \
    --entrypoint /usr/local/bin/hubuum-admin \
    candidate-api "${args[@]}" > "$report_path"
  jq --exit-status \
    '.result == "passed" and .mode == "isolated_restore" and .restore_test.storage_ready == true' \
    "$report_path" >/dev/null
}

seed_previous_release() {
  local service_account_id
  local child_collection_id
  local sink_id
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
  if "${compose[@]}" exec -T postgres pg_isready \
    --host 127.0.0.1 --username hubuum --dbname hubuum >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
"${compose[@]}" exec -T postgres pg_isready \
  --host 127.0.0.1 --username hubuum --dbname hubuum >/dev/null
"${compose[@]}" run --rm --no-deps -T \
  --entrypoint /usr/local/bin/hubuum-admin previous-api --migrate
"${compose[@]}" up -d previous-api previous-worker
wait_for_ready previous-api

phase="authenticate-previous-release"
password_output="$(
  "${compose[@]}" run --rm --no-deps -T --entrypoint /usr/local/bin/hubuum-admin \
    previous-api --reset-password admin
)"
admin_password="${password_output##*reset to: }"
[[ -n "$admin_password" && "$admin_password" != "$password_output" ]]
api_request previous-api POST /api/v0/auth/login \
  "{\"name\":\"admin\",\"password\":\"$admin_password\"}"
admin_token="$(jq --exit-status --raw-output '.token' <<< "$api_body")"

phase="seed-previous-release"
seed_previous_release

phase="backup-previous-release"
create_backup_artifact \
  previous-api true "$adjacent_backup_file" adjacent_backup_generation_ms
create_backup_artifact \
  previous-api false "$history_free_backup_file" history_free_backup_generation_ms

phase="drain-previous-worker"
"${compose[@]}" stop --timeout 75 previous-worker
if "${compose[@]}" exec -T previous-worker true >/dev/null 2>&1; then
  echo "ERROR: previous worker remained available after drain" >&2
  exit 1
fi

phase="candidate-migration"
rm -f "$probe_stop_file"
probe_previous_api &
probe_pid=$!
migration_started=$SECONDS
"${compose[@]}" run --rm --no-deps -T --entrypoint /usr/local/bin/hubuum-admin \
  candidate-api --migrate --legacy-single-role-migration > "$migration_log" 2>&1
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
"${compose[@]}" exec -T candidate-worker true

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

phase="backup-current-release"
create_backup_artifact candidate-api true "$current_backup_file" current_backup_generation_ms

phase="isolated-backup-restores"
"${compose[@]}" exec -T postgres createdb --username hubuum hubuum_restore
"${compose[@]}" exec -T postgres createdb --username hubuum hubuum_restore_no_history
"${compose[@]}" exec -T postgres createdb --username hubuum hubuum_restore_current
verify_restore_artifact \
  "$adjacent_backup_file" "$restore_database_url" "$adjacent_restore_report" keep
verify_restore_artifact \
  "$history_free_backup_file" "$history_free_restore_database_url" \
  "$history_free_restore_report" reset
verify_restore_artifact \
  "$current_backup_file" "$current_restore_database_url" "$current_restore_report" reset
jq --exit-status '.includes_history == true and .source_version != ""' \
  "$adjacent_restore_report" >/dev/null
jq --exit-status '.includes_history == false and .restore_test.target_cleanup == "schema_reset"' \
  "$history_free_restore_report" >/dev/null
jq --exit-status '.includes_history == true and .restore_test.target_cleanup == "schema_reset"' \
  "$current_restore_report" >/dev/null

phase="restored-api-smoke"
source_admin_token="$admin_token"
"${compose[@]}" up -d restored-api restored-worker
wait_for_ready restored-api
restored_readiness=true
password_output="$(
  "${compose[@]}" run --rm --no-deps -T --entrypoint /usr/local/bin/hubuum-admin \
    restored-api --reset-password admin
)"
restored_admin_password="${password_output##*reset to: }"
[[ -n "$restored_admin_password" && "$restored_admin_password" != "$password_output" ]]
admin_token=""
api_request restored-api POST /api/v0/auth/login \
  "{\"name\":\"admin\",\"password\":\"$restored_admin_password\"}"
admin_token="$(jq --exit-status --raw-output '.token' <<< "$api_body")"
restored_authentication=true

api_request restored-api GET "/api/v1/collections/$collection_id"
[[ "$(jq --raw-output '.description' <<< "$api_body")" == "adjacent release root" ]]
api_request restored-api GET "/api/v1/classes/$class_id/$object_id"
[[ "$(jq --raw-output '.data.owner' <<< "$api_body")" == "previous" ]]
api_request restored-api GET "/api/v1/iam/groups/$group_id/members/$user_id"
api_request restored-api GET "/api/v1/collections/$collection_id/permissions/group/$group_id"
[[ "$(jq '.permissions | length' <<< "$api_body")" -gt 0 ]]
api_request restored-api GET \
  "/api/v1/classes/$class_id/$object_id/relations/$related_class_id/$related_object_id"
[[ "$(jq --raw-output '.class_relation_id' <<< "$api_body")" == "$class_relation_id" ]]
api_request restored-api GET "/api/v1/export-templates/$template_id"
restored_state_reads=true
api_request restored-api GET "/api/v1/tasks/$probe_task_id"
[[ "$(jq --raw-output '.status' <<< "$api_body")" == "succeeded" ]]
restored_task_read=true
api_request restored-api GET "/api/v1/collections/$collection_id/events?limit=50"
[[ "$(jq 'length' <<< "$api_body")" -gt 0 ]]
restored_history_read=true

computed_ready=false
rebuild_started_ms="$(date +%s%3N)"
for _ in $(seq 1 100); do
  api_request restored-api GET "/api/v1/classes/$class_id/$object_id?include=computed"
  if jq --exit-status \
    '.computed.shared.materialization_stale == false and .computed.shared.values.owner_copy == "previous"' \
    <<< "$api_body" >/dev/null; then
    computed_ready=true
    break
  fi
  sleep 0.1
done
rebuild_finished_ms="$(date +%s%3N)"
rebuild_duration_ms="$((rebuild_finished_ms - rebuild_started_ms))"
[[ "$computed_ready" == "true" ]] || {
  echo "ERROR: restored computed-field materialization did not become current" >&2
  exit 1
}
restored_computed_rebuild=true

old_token_status="$(
  curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
    --header "Authorization: Bearer $source_admin_token" \
    "$(service_url restored-api)/api/v1/collections/$collection_id"
)"
[[ "$old_token_status" == "401" ]] || {
  echo "ERROR: a bearer token excluded from backup remained usable after restore" >&2
  exit 1
}
restored_token_exclusion=true

phase="complete"
echo "Adjacent release compatibility passed: $previous_tag -> ${GITHUB_SHA:-local candidate}."
