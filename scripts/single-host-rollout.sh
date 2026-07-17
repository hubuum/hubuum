#!/usr/bin/env bash

# Shared rolling-update primitives for the single-host installer and updater.
# The caller must define COMPOSE_CMD, ENGINE_PATH, and INSTALL_MODE before
# invoking hubuum_rollout. DATABASE_MANAGED defaults to true for legacy callers.

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
  local deadline=$((SECONDS + timeout_seconds))
  local health

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
  hubuum_wait_for_healthy "$service" "${HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS:-180}"
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
  hubuum_wait_for_healthy "$service" "${HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS:-180}"
}

hubuum_caddy_is_running() {
  [[ -n "$(hubuum_service_container_id caddy)" ]]
}

hubuum_reload_caddy() {
  echo "Reloading Caddy to refresh its upstream health state..."
  "${COMPOSE_CMD[@]}" exec -T caddy \
    caddy reload --force --config /etc/caddy/Caddyfile --adapter caddyfile
}

hubuum_run_migrations() {
  echo "Running one-shot database migrations while the primary remains online..."
  "${COMPOSE_CMD[@]}" run --rm --no-deps \
    --entrypoint /usr/local/bin/hubuum-admin hubuum-api --migrate
}

hubuum_start_stack() {
  echo "Starting the initial Hubuum stack..."

  # Start the migration-owning primary first. Starting both API containers at
  # once could make two fresh containers race to apply the same migrations.
  "${COMPOSE_CMD[@]}" up -d hubuum-api
  hubuum_wait_for_healthy hubuum-api "${HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS:-180}"

  "${COMPOSE_CMD[@]}" up -d --no-deps hubuum-api-standby
  hubuum_wait_for_healthy hubuum-api-standby "${HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS:-180}"

  if [[ "$INSTALL_MODE" == "all" ]]; then
    "${COMPOSE_CMD[@]}" up -d hubuum-web hubuum-web-standby
    hubuum_wait_for_healthy hubuum-web "${HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS:-180}"
    hubuum_wait_for_healthy hubuum-web-standby "${HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS:-180}"
  fi

  "${COMPOSE_CMD[@]}" up -d --no-deps caddy
}

hubuum_rollout() {
  local api_primary_recovered="false"
  local primary_rolled="false"
  local web_primary_recovered="false"
  local web_primary_health
  local web_standby_health

  if ! hubuum_caddy_is_running; then
    hubuum_start_stack
    return 0
  fi

  hubuum_ensure_infrastructure
  hubuum_run_migrations

  # A previous rollout may have failed after replacing a primary. Recover that
  # primary while the healthy standby still owns traffic; recreating the
  # standby first would otherwise remove the only usable upstream.
  if ! hubuum_service_is_healthy hubuum-api; then
    if ! hubuum_service_is_healthy hubuum-api-standby; then
      echo "ERROR: neither backend replica is healthy; refusing to replace either one" >&2
      return 1
    fi
    hubuum_roll_service hubuum-api
    api_primary_recovered="true"
  fi

  if [[ "$INSTALL_MODE" == "all" ]] && ! hubuum_service_is_healthy hubuum-web; then
    web_primary_health="$(hubuum_service_health hubuum-web)"
    web_standby_health="$(hubuum_service_health hubuum-web-standby)"
    if hubuum_service_is_healthy hubuum-web-standby; then
      hubuum_roll_service hubuum-web
      web_primary_recovered="true"
    elif [[ "$web_primary_health" != "missing" || "$web_standby_health" != "missing" ]]; then
      echo "ERROR: neither frontend replica is healthy; refusing to replace either one" >&2
      return 1
    fi
  fi

  if [[ "$api_primary_recovered" == "true" || "$web_primary_recovered" == "true" ]]; then
    hubuum_reload_caddy
  fi

  # Upgrade every standby while its primary remains available. Caddy can retain
  # a passive failure mark for a container that was unavailable during
  # replacement, so reload only after all standbys are proven healthy. The
  # configured stream_close_delay protects long-lived connections across this
  # zero-downtime configuration reload.
  hubuum_roll_service hubuum-api-standby
  if [[ "$INSTALL_MODE" == "all" ]]; then
    hubuum_roll_service hubuum-web-standby
  fi
  hubuum_reload_caddy

  if [[ "$api_primary_recovered" != "true" ]]; then
    hubuum_roll_service hubuum-api
    primary_rolled="true"
  fi
  if [[ "$INSTALL_MODE" == "all" && "$web_primary_recovered" != "true" ]]; then
    hubuum_roll_service hubuum-web
    primary_rolled="true"
  fi
  if [[ "$primary_rolled" == "true" ]]; then
    hubuum_reload_caddy
  fi
}
