#!/usr/bin/env bash

# Shared rolling-update primitives for the single-host installer and updater.
# The caller must define COMPOSE_CMD, ENGINE_PATH, and INSTALL_MODE before
# invoking hubuum_rollout.

hubuum_service_container_id() {
  local service="$1"

  "${COMPOSE_CMD[@]}" ps -q "$service"
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

hubuum_roll_service() {
  local service="$1"

  echo "Rolling $service..."
  "${COMPOSE_CMD[@]}" up -d --no-deps --force-recreate "$service"
  hubuum_wait_for_healthy "$service"
}

hubuum_caddy_is_running() {
  [[ -n "$(hubuum_service_container_id caddy)" ]]
}

hubuum_reload_caddy() {
  echo "Reloading Caddy with the redundant upstream configuration..."
  "${COMPOSE_CMD[@]}" exec -T caddy \
    caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
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
  hubuum_wait_for_healthy hubuum-api

  "${COMPOSE_CMD[@]}" up -d --no-deps hubuum-api-standby
  hubuum_wait_for_healthy hubuum-api-standby

  if [[ "$INSTALL_MODE" == "all" ]]; then
    "${COMPOSE_CMD[@]}" up -d hubuum-web hubuum-web-standby
    hubuum_wait_for_healthy hubuum-web
    hubuum_wait_for_healthy hubuum-web-standby
  fi

  "${COMPOSE_CMD[@]}" up -d --no-deps caddy
}

hubuum_rollout() {
  local reload_caddy="${1:-false}"

  if ! hubuum_caddy_is_running; then
    hubuum_start_stack
    return 0
  fi

  hubuum_run_migrations

  # The standby is upgraded and proven ready while the primary remains the
  # currently configured upstream. On first adoption, reload Caddy only after
  # that safety copy exists. Routine updates keep Caddy and its configuration
  # untouched, preserving long-lived connections.
  hubuum_roll_service hubuum-api-standby
  if [[ "$reload_caddy" == "true" ]]; then
    hubuum_reload_caddy
  fi

  if [[ "$INSTALL_MODE" == "all" ]]; then
    hubuum_roll_service hubuum-web-standby
  fi

  hubuum_roll_service hubuum-api
  if [[ "$INSTALL_MODE" == "all" ]]; then
    hubuum_roll_service hubuum-web
  fi
}
