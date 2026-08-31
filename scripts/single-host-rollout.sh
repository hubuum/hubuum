#!/usr/bin/env bash

# Shared rolling-update primitives for the single-host installer and updater.
# The caller must define COMPOSE_CMD, ENGINE_PATH, and INSTALL_MODE before
# invoking hubuum_rollout. API_PORT defaults to 8080 and DATABASE_MANAGED
# defaults to true for legacy callers.

API_PORT="${API_PORT:-8080}"

hubuum_require_positive_seconds() {
  local setting_name="$1"
  local value="$2"

  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: $setting_name must be a positive integer; got '$value'" >&2
    return 1
  fi
}

hubuum_validate_rollout_timeouts() {
  hubuum_require_positive_seconds \
    "HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS" \
    "${HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS:-180}" || return 1
  hubuum_require_positive_seconds \
    "HUBUUM_ROLLOUT_CADDY_TIMEOUT_SECONDS" \
    "${HUBUUM_ROLLOUT_CADDY_TIMEOUT_SECONDS:-180}"
}

hubuum_service_container_id() {
  local service="$1"
  local container_id
  local container_service

  # Older podman-compose releases do not accept a service argument for `ps`,
  # unlike Docker Compose. List this project's containers and select the
  # requested service through the Compose label shared by both providers.
  while IFS= read -r container_id; do
    [[ -n "$container_id" ]] || continue
    container_service="$(
      "$ENGINE_PATH" inspect \
        --format '{{ index .Config.Labels "com.docker.compose.service" }}' \
        "$container_id" 2>/dev/null || true
    )"
    if [[ "$container_service" == "$service" ]]; then
      printf '%s\n' "$container_id"
      return 0
    fi
  done < <("${COMPOSE_CMD[@]}" ps -q)
}

hubuum_service_health() {
  local service="$1"
  local container_id

  container_id="$(hubuum_service_container_id "$service")"
  [[ -n "$container_id" ]] || {
    printf 'missing\n'
    return 0
  }

  "$ENGINE_PATH" inspect \
    --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
    "$container_id" 2>/dev/null || printf 'missing\n'
}

hubuum_wait_for_healthy() {
  local service="$1"
  local timeout_seconds="${2:-180}"
  local deadline
  local health

  hubuum_require_positive_seconds "health timeout" "$timeout_seconds" || return 1
  deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    health="$(hubuum_service_health "$service")"
    case "$health" in
      healthy|running)
        return 0
        ;;
      exited|dead)
        echo "ERROR: $service stopped while waiting for readiness" >&2
        "${COMPOSE_CMD[@]}" logs --tail 100 "$service" >&2 || true
        return 1
        ;;
    esac
    sleep 2
  done

  echo "ERROR: $service did not become healthy within ${timeout_seconds}s" >&2
  "${COMPOSE_CMD[@]}" logs --tail 100 "$service" >&2 || true
  return 1
}

hubuum_wait_for_rollout_health() {
  hubuum_wait_for_healthy "$1" "${HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS:-180}"
}

hubuum_service_is_healthy() {
  local health

  health="$(hubuum_service_health "$1")"
  [[ "$health" == "healthy" || "$health" == "running" ]]
}

hubuum_ensure_infrastructure_service() {
  local service="$1"

  if [[ -z "$(hubuum_service_container_id "$service")" ]]; then
    echo "Starting required infrastructure service $service..."
    "${COMPOSE_CMD[@]}" up -d --no-deps --no-recreate "$service"
  fi
  hubuum_wait_for_rollout_health "$service"
}

hubuum_ensure_infrastructure() {
  if [[ "${DATABASE_MANAGED:-true}" == "true" ]]; then
    hubuum_ensure_infrastructure_service postgres
  fi
  if [[ "$INSTALL_MODE" == "all" ]]; then
    hubuum_ensure_infrastructure_service valkey
  fi
}

hubuum_roll_service() {
  local service="$1"

  echo "Rolling $service..."
  "${COMPOSE_CMD[@]}" up -d --no-deps --force-recreate "$service"
  hubuum_wait_for_rollout_health "$service"
}

hubuum_caddy_is_running() {
  [[ -n "$(hubuum_service_container_id caddy)" ]]
}

hubuum_caddy_has_container_dependencies() {
  local container_id
  local dependencies

  container_id="$(hubuum_service_container_id caddy)"
  [[ -n "$container_id" ]] || return 1
  dependencies="$(
    "$ENGINE_PATH" inspect \
      --format '{{range .Dependencies}}{{println .}}{{end}}' \
      "$container_id" 2>/dev/null || true
  )"
  [[ -n "$dependencies" ]]
}

hubuum_remove_legacy_caddy_dependencies() {
  hubuum_caddy_has_container_dependencies || return 0

  echo "Recreating Caddy once to remove legacy Podman container dependencies..."
  "${COMPOSE_CMD[@]}" up -d --no-deps --force-recreate caddy
  hubuum_wait_for_rollout_health caddy
}

hubuum_reload_caddy() {
  local output

  echo "Reloading Caddy if its configuration changed..."
  if ! output="$(
    "${COMPOSE_CMD[@]}" exec -T caddy \
      caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile 2>&1
  )"; then
    echo "ERROR: Caddy reload failed" >&2
    printf '%s\n' "$output" >&2
    return 1
  fi
}

hubuum_caddy_upstreams() {
  "${COMPOSE_CMD[@]}" exec -T caddy \
    wget -qO- \
    http://127.0.0.1:2019/reverse_proxy/upstreams
}

hubuum_caddy_upstream_status_is_eligible() {
  local upstreams="$1"
  shift
  local address
  local found
  local normalized
  local upstream

  [[ "$#" -gt 0 ]] || return 1
  normalized="${upstreams//[[:space:]]/}"

  # Caddy returns a flat JSON array with one object per configured upstream.
  # An address can occur in more than one route, so require every matching
  # entry to be eligible. This prevents an eligible metrics entry from hiding
  # an ineligible public API entry for the same container.
  for address in "$@"; do
    found="false"
    while IFS= read -r -d '}' upstream || [[ -n "$upstream" ]]; do
      if [[ "$upstream" == *"\"address\":\"${address}\""* ]]; then
        found="true"
        [[ "$upstream" =~ \"fails\":0(,|$) ]] || return 1
      fi
    done <<<"$normalized"
    [[ "$found" == "true" ]] || return 1
  done
}

hubuum_caddy_upstreams_are_eligible() {
  local upstreams

  if ! upstreams="$(hubuum_caddy_upstreams 2>/dev/null)"; then
    return 1
  fi

  hubuum_caddy_upstream_status_is_eligible "$upstreams" "$@"
}

hubuum_wait_for_caddy_upstreams() {
  local timeout_seconds="${HUBUUM_ROLLOUT_CADDY_TIMEOUT_SECONDS:-180}"
  local deadline

  [[ "$#" -gt 0 ]] || {
    echo "ERROR: at least one required Caddy upstream address is required" >&2
    return 1
  }
  hubuum_require_positive_seconds "Caddy upstream timeout" "$timeout_seconds" || return 1
  deadline=$((SECONDS + timeout_seconds))

  echo "Waiting for required Caddy upstreams to clear failure marks..."
  while (( SECONDS < deadline )); do
    if hubuum_caddy_upstreams_are_eligible "$@"; then
      return 0
    fi
    sleep 2
  done

  echo "ERROR: Caddy did not report required upstreams eligible within ${timeout_seconds}s" >&2
  printf 'Required Caddy upstream: %s\n' "$@" >&2
  hubuum_caddy_upstreams >&2 || true
  return 1
}

hubuum_reload_caddy_and_wait_for_upstreams() {
  hubuum_reload_caddy || return 1
  hubuum_wait_for_caddy_upstreams "$@"
}

hubuum_run_migrations() {
  if [[ "${DATABASE_MANAGED:-true}" == "true" ]]; then
    echo "Reconciling managed database roles..."
    "${COMPOSE_CMD[@]}" run --rm --no-deps -T hubuum-migrate \
      --database-role-setup-sql | \
      "${COMPOSE_CMD[@]}" exec -T postgres \
        psql --set ON_ERROR_STOP=1 --username hubuum --dbname hubuum
    "${COMPOSE_CMD[@]}" run --rm --no-deps -T \
      --entrypoint /usr/local/bin/hubuum-set-database-role-passwords postgres
  fi
  echo "Running one-shot database migrations..."
  "${COMPOSE_CMD[@]}" run --rm --no-deps -T hubuum-migrate --migrate
}

hubuum_drain_primary_workers_for_migrations() {
  local standby_health

  if ! hubuum_service_is_healthy hubuum-api; then
    return 1
  fi

  standby_health="$(hubuum_service_health hubuum-api-standby)"
  if [[ "$standby_health" == "missing" ]]; then
    # First adoption of rolling services has no old API-only replica to carry
    # traffic. Its documented upgrade contract requires an idle task queue.
    return 1
  fi
  if [[ "$standby_health" != "healthy" && "$standby_health" != "running" ]]; then
    echo "ERROR: API standby is not healthy; refusing to migrate while old-version workers remain online" >&2
    return 2
  fi

  # Container health can recover before Caddy clears an earlier active or
  # passive failure mark. Do not drain the primary until the proxy confirms
  # that the standby can actually carry public traffic.
  if ! hubuum_wait_for_caddy_upstreams "hubuum-api-standby:${API_PORT}"; then
    echo "ERROR: Caddy does not have an eligible API standby; refusing to drain the primary" >&2
    return 2
  fi

  echo "Stopping the all-role primary to drain old-version workers..."
  "${COMPOSE_CMD[@]}" stop hubuum-api
}

hubuum_restart_primary_after_failed_migration() {
  echo "Migration failed; restarting the drained primary..."
  "${COMPOSE_CMD[@]}" start hubuum-api
  hubuum_wait_for_rollout_health hubuum-api
}

hubuum_start_stack() {
  echo "Starting the initial Hubuum stack..."

  hubuum_ensure_infrastructure
  hubuum_run_migrations

  "${COMPOSE_CMD[@]}" up -d --no-deps --force-recreate hubuum-restore-executor

  "${COMPOSE_CMD[@]}" up -d hubuum-api
  hubuum_wait_for_rollout_health hubuum-api

  "${COMPOSE_CMD[@]}" up -d --no-deps hubuum-api-standby
  hubuum_wait_for_rollout_health hubuum-api-standby

  if [[ "$INSTALL_MODE" == "all" ]]; then
    "${COMPOSE_CMD[@]}" up -d hubuum-web hubuum-web-standby
    hubuum_wait_for_rollout_health hubuum-web
    hubuum_wait_for_rollout_health hubuum-web-standby
  fi

  "${COMPOSE_CMD[@]}" up -d --no-deps caddy
}

hubuum_rollout() {
  local api_primary_recovered="false"
  local drain_status
  local primary_workers_drained="false"
  local web_primary_recovered="false"
  local web_primary_health
  local web_standby_health
  local -a primary_upstreams=()
  local -a recovered_upstreams=()
  local -a standby_upstreams=("hubuum-api-standby:${API_PORT}")

  hubuum_validate_rollout_timeouts || return 1

  if ! hubuum_caddy_is_running; then
    hubuum_start_stack
    return 0
  fi

  hubuum_remove_legacy_caddy_dependencies
  hubuum_ensure_infrastructure
  if hubuum_drain_primary_workers_for_migrations; then
    primary_workers_drained="true"
  else
    drain_status=$?
    if [[ "$drain_status" -eq 2 ]]; then
      return 1
    fi
  fi
  if ! hubuum_run_migrations; then
    if [[ "$primary_workers_drained" == "true" ]]; then
      hubuum_restart_primary_after_failed_migration || true
    fi
    return 1
  fi
  "${COMPOSE_CMD[@]}" up -d --no-deps --force-recreate hubuum-restore-executor
  if [[ "$primary_workers_drained" == "true" ]]; then
    hubuum_roll_service hubuum-api
    api_primary_recovered="true"
    recovered_upstreams+=("hubuum-api:${API_PORT}")
  fi

  # A previous rollout may have left the primary unhealthy. Recover it while
  # the healthy standby still owns traffic; recreating the standby first would
  # otherwise remove the only usable upstream.
  if ! hubuum_service_is_healthy hubuum-api; then
    if ! hubuum_service_is_healthy hubuum-api-standby; then
      echo "ERROR: neither backend replica is healthy; refusing to replace either one" >&2
      return 1
    fi
    hubuum_roll_service hubuum-api
    api_primary_recovered="true"
    recovered_upstreams+=("hubuum-api:${API_PORT}")
  fi

  if [[ "$INSTALL_MODE" == "all" ]] && ! hubuum_service_is_healthy hubuum-web; then
    web_primary_health="$(hubuum_service_health hubuum-web)"
    web_standby_health="$(hubuum_service_health hubuum-web-standby)"
    if hubuum_service_is_healthy hubuum-web-standby; then
      hubuum_roll_service hubuum-web
      web_primary_recovered="true"
      recovered_upstreams+=("hubuum-web:3000")
    elif [[ "$web_primary_health" != "missing" || "$web_standby_health" != "missing" ]]; then
      echo "ERROR: neither frontend replica is healthy; refusing to replace either one" >&2
      return 1
    fi
  fi

  if [[ "${#recovered_upstreams[@]}" -gt 0 ]]; then
    hubuum_reload_caddy_and_wait_for_upstreams "${recovered_upstreams[@]}"
  fi

  # Upgrade every standby while its primary remains available. Reload only
  # after all standbys are proven healthy, then wait for Caddy's passive failure
  # window to clear before replacing a primary. A changed Caddyfile, such as a
  # legacy upgrade, is still applied without forcing unchanged configs to be
  # reprovisioned.
  hubuum_roll_service hubuum-api-standby
  if [[ "$INSTALL_MODE" == "all" ]]; then
    hubuum_roll_service hubuum-web-standby
    standby_upstreams+=("hubuum-web-standby:3000")
  fi
  hubuum_reload_caddy_and_wait_for_upstreams "${standby_upstreams[@]}"

  if [[ "$api_primary_recovered" != "true" ]]; then
    hubuum_roll_service hubuum-api
    primary_upstreams+=("hubuum-api:${API_PORT}")
  fi
  if [[ "$INSTALL_MODE" == "all" && "$web_primary_recovered" != "true" ]]; then
    hubuum_roll_service hubuum-web
    primary_upstreams+=("hubuum-web:3000")
  fi
  if [[ "${#primary_upstreams[@]}" -gt 0 ]]; then
    hubuum_reload_caddy_and_wait_for_upstreams "${primary_upstreams[@]}"
  fi
}
