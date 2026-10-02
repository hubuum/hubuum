#!/usr/bin/env bash

# Sourced by the installer; uses its engine, saved settings and error helpers.
hubuum_prepare_monitoring() {
  local ref="$MONITORING_ASSETS_REF" asset source_root="" staging
  MONITORING_HOST="$API_FQDN"
  [[ "$MODE" != "all" ]] || MONITORING_HOST="$WEB_FQDN"
  MONITORING_DEPLOYMENT="${MONITORING_DEPLOYMENT:-$API_FQDN}"
  [[ "$MONITORING_DEPLOYMENT" =~ ^[a-zA-Z0-9_.-]+$ ]] || die "MONITORING_DEPLOYMENT must contain only letters, digits, dots, underscores or hyphens"
  [[ "$MONITORING_HOST" =~ ^[a-zA-Z0-9.-]+$ ]] || die "monitoring requires a DNS hostname"
  [[ "$(read_env_value HUBUUM_METRICS_ENABLED || printf true)" != "false" ]] || die "enable HUBUUM_METRICS_ENABLED in .env before installing monitoring"

  if [[ "$BUILD_FROM_SOURCE" == "true" ]]; then
    source_root="$INSTALL_DIR/src/hubuum/observability"
  elif [[ "$ref" == "auto" ]]; then
    case "$BACKEND_IMAGE" in
      ghcr.io/hubuum/hubuum-server:*)
        # A tagged digest still identifies its release; an untagged digest needs
        # an explicit ref, as do private images with unrelated version schemes.
        ref="${BACKEND_IMAGE#ghcr.io/hubuum/hubuum-server:}"
        ref="${ref%%@*}"
        ;;
      *) die "use --monitoring-ref with a custom or untagged digest backend image" ;;
    esac
    if [[ "$ref" == "latest" ]]; then
      need_cmd curl
      ref="$(curl -fsSL -o /dev/null -w '%{url_effective}' https://github.com/hubuum/hubuum/releases/latest)"
      ref="${ref##*/}"
    fi
    [[ "$ref" == "main" || "$ref" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "use --monitoring-ref for this backend tag"
    if [[ "$ref" == "main" && -d "$SCRIPT_DIR/../observability" ]]; then
      source_root="$SCRIPT_DIR/../observability"
    fi
  fi
  [[ "$ref" =~ ^[a-zA-Z0-9_./-]+$ && "$ref" != *..* ]] || die "invalid --monitoring-ref"

  staging="$(mktemp -d "$INSTALL_DIR/.monitoring.XXXXXX")"
  if [[ -n "$source_root" ]]; then
    cp "$source_root/manifest.txt" "$staging/manifest.txt" || { rm -rf "$staging"; die "operator package lacks manifest.txt; upgrade the backend or choose --monitoring-ref"; }
  else
    need_cmd curl
    curl -fsSL "https://raw.githubusercontent.com/hubuum/hubuum/$ref/observability/manifest.txt" -o "$staging/manifest.txt" || { rm -rf "$staging"; die "operator package for $ref lacks manifest.txt; upgrade the backend or choose --monitoring-ref"; }
  fi
  # Fetch the maintained assets as a set before changing the installed copy.
  while IFS= read -r asset; do
    [[ "$asset" =~ ^(prometheus|dashboards)/[a-z-]+\.json$ ]] || { rm -rf "$staging"; die "invalid monitoring asset path"; }
    mkdir -p "$staging/$(dirname "$asset")"
    if [[ -n "$source_root" ]]; then
      if ! cp "$source_root/$asset" "$staging/$asset"; then
        rm -rf "$staging"
        die "operator package is missing $asset in $source_root"
      fi
    elif ! curl -fsSL "https://raw.githubusercontent.com/hubuum/hubuum/$ref/observability/$asset" -o "$staging/$asset"; then
      rm -rf "$staging"
      die "could not fetch the operator package for $ref; choose a release with monitoring assets"
    fi
  done < "$staging/manifest.txt"
  [[ -s "$staging/prometheus/alerts.json" && -s "$staging/prometheus/recording-rules.json" && -s "$staging/dashboards/overview.json" ]] || { rm -rf "$staging"; die "incomplete operator package"; }

  # The installer loads saved secrets even when disabled or using --recreate.
  # Generate them only when monitoring is first enabled.
  [[ -n "$GRAFANA_ADMIN_PASSWORD" ]] || GRAFANA_ADMIN_PASSWORD="$(random_hex 24)"
  [[ -n "$GRAFANA_SECRET_KEY" ]] || GRAFANA_SECRET_KEY="$(random_hex 32)"
  [[ -n "$PROMETHEUS_PASSWORD" ]] || PROMETHEUS_PASSWORD="$(random_hex 24)"
  # Caddy hashes stdin; plaintext credentials never appear in process arguments
  # or the generated proxy configuration.
  if ! PROMETHEUS_PASSWORD_HASH="$(printf '%s\n' "$PROMETHEUS_PASSWORD" | "$ENGINE_PATH" run --rm -i --entrypoint caddy "$CADDY_IMAGE" hash-password)"; then
    rm -rf "$staging"
    die "could not hash the Prometheus password with Caddy"
  fi
  [[ "$PROMETHEUS_PASSWORD_HASH" == \$2* ]] || { rm -rf "$staging"; die "Caddy did not return a bcrypt password hash"; }

  install -d -m 0755 "$INSTALL_DIR/monitoring" "$INSTALL_DIR/monitoring/prometheus" \
    "$INSTALL_DIR/monitoring/grafana" "$INSTALL_DIR/monitoring/grafana/provisioning" \
    "$INSTALL_DIR/monitoring/prometheus/rules" \
    "$INSTALL_DIR/monitoring/grafana/dashboards" \
    "$INSTALL_DIR/monitoring/grafana/provisioning/datasources" \
    "$INSTALL_DIR/monitoring/grafana/provisioning/dashboards"
  install -m 0644 "$staging/prometheus/alerts.json" "$INSTALL_DIR/monitoring/prometheus/rules/hubuum.json"
  install -m 0644 "$staging/prometheus/recording-rules.json" "$INSTALL_DIR/monitoring/prometheus/rules/hubuum-recording.json"
  install -m 0644 "$staging"/dashboards/*.json "$INSTALL_DIR/monitoring/grafana/dashboards/"
  printf '%s\n' "${source_root:-$ref}" > "$INSTALL_DIR/monitoring/asset-source.txt"
  rm -rf "$staging"

  # The main configuration and provisioning files are operator-owned after
  # installation. Only targets and the maintained assets refresh on update.
  if [[ ! -f "$INSTALL_DIR/monitoring/prometheus/prometheus.yml" ]]; then
    cat > "$INSTALL_DIR/monitoring/prometheus/prometheus.yml" <<'EOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s
rule_files:
  - /etc/prometheus/rules/*.json
  - /etc/prometheus/rules/*.yml
scrape_configs:
  - job_name: hubuum
    file_sd_configs:
      - files: [/etc/prometheus/targets.json]
        refresh_interval: 15s
EOF
    chmod 0644 "$INSTALL_DIR/monitoring/prometheus/prometheus.yml"
  fi
  cat > "$INSTALL_DIR/monitoring/prometheus/targets.json" <<EOF
[
  {"targets":["hubuum-api:${API_PORT}"],"labels":{"instance":"hubuum-api","deployment":"${MONITORING_DEPLOYMENT}"}},
  {"targets":["hubuum-api-standby:${API_PORT}"],"labels":{"instance":"hubuum-api-standby","deployment":"${MONITORING_DEPLOYMENT}"}}
]
EOF
  chmod 0644 "$INSTALL_DIR/monitoring/prometheus/targets.json"
  if [[ ! -f "$INSTALL_DIR/monitoring/grafana/provisioning/datasources/hubuum.yml" ]]; then
    cat > "$INSTALL_DIR/monitoring/grafana/provisioning/datasources/hubuum.yml" <<'EOF'
apiVersion: 1
datasources:
  - name: Hubuum Prometheus
    uid: hubuum-prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090/prometheus
    isDefault: true
    editable: false
EOF
    chmod 0644 "$INSTALL_DIR/monitoring/grafana/provisioning/datasources/hubuum.yml"
  fi
  if [[ ! -f "$INSTALL_DIR/monitoring/grafana/provisioning/dashboards/hubuum.yml" ]]; then
    cat > "$INSTALL_DIR/monitoring/grafana/provisioning/dashboards/hubuum.yml" <<'EOF'
apiVersion: 1
providers:
  - name: Hubuum
    folder: Hubuum
    type: file
    disableDeletion: true
    allowUiUpdates: false
    options:
      path: /etc/grafana/dashboards
EOF
    chmod 0644 "$INSTALL_DIR/monitoring/grafana/provisioning/dashboards/hubuum.yml"
  fi
}

hubuum_monitoring_env() {
  local setting
  printf 'PROMETHEUS_IMAGE=%s\n' "$PROMETHEUS_IMAGE"
  printf 'GRAFANA_IMAGE=%s\n' "$GRAFANA_IMAGE"
  printf 'PROMETHEUS_RETENTION_TIME=%s\n' "$PROMETHEUS_RETENTION_TIME"
  printf 'PROMETHEUS_RETENTION_SIZE=%s\n' "$PROMETHEUS_RETENTION_SIZE"
  printf 'PROMETHEUS_MEMORY_LIMIT=%s\n' "$PROMETHEUS_MEMORY_LIMIT"
  printf 'GRAFANA_MEMORY_LIMIT=%s\n' "$GRAFANA_MEMORY_LIMIT"
  for setting in MONITORING_ASSETS_REF MONITORING_DEPLOYMENT MONITORING_HOST \
    GRAFANA_ADMIN_PASSWORD GRAFANA_SECRET_KEY PROMETHEUS_PASSWORD; do
    if [[ -n "${!setting}" ]]; then
      printf '%s=%s\n' "$setting" "${!setting}"
    fi
  done
}

hubuum_monitoring_caddy() {
  cat <<EOF

(monitoring) {
	handle /grafana {
		redir /grafana/ 308
	}
	handle /grafana/* {
		reverse_proxy grafana:3000
	}
	@prometheus path /prometheus /prometheus/*
	handle @prometheus {
		basic_auth {
			admin ${PROMETHEUS_PASSWORD_HASH}
		}
		reverse_proxy prometheus:9090 {
			header_up -Authorization
		}
	}
}
EOF
}

hubuum_monitoring_compose() {
  cat <<'EOF'

  prometheus:
    image: ${PROMETHEUS_IMAGE}
    restart: unless-stopped
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.path=/prometheus
      - --storage.tsdb.retention.time=${PROMETHEUS_RETENTION_TIME}
      - --storage.tsdb.retention.size=${PROMETHEUS_RETENTION_SIZE}
      - --web.external-url=https://${MONITORING_HOST}/prometheus/
      - --web.route-prefix=/prometheus
    mem_limit: ${PROMETHEUS_MEMORY_LIMIT}
    cpus: 1.0
    volumes:
      - ./monitoring/prometheus:/etc/prometheus:ro,z
      - prometheus_data:/prometheus
    networks:
      - hubuum_net
    healthcheck:
      test: ["CMD", "wget", "-q", "-O", "/dev/null", "http://127.0.0.1:9090/prometheus/-/ready"]
      interval: 5s
      timeout: 3s
      retries: 36

  grafana:
    image: ${GRAFANA_IMAGE}
    restart: unless-stopped
    environment:
      GF_SERVER_ROOT_URL: https://${MONITORING_HOST}/grafana/
      GF_SERVER_SERVE_FROM_SUB_PATH: "true"
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD}
      GF_SECURITY_SECRET_KEY: ${GRAFANA_SECRET_KEY}
      GF_SECURITY_COOKIE_SECURE: "true"
      GF_AUTH_ANONYMOUS_ENABLED: "false"
      GF_USERS_ALLOW_SIGN_UP: "false"
      GF_ANALYTICS_REPORTING_ENABLED: "false"
      GF_ANALYTICS_CHECK_FOR_UPDATES: "false"
      GF_PLUGINS_PREINSTALL_DISABLED: "true"
    mem_limit: ${GRAFANA_MEMORY_LIMIT}
    cpus: 1.0
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro,z
      - ./monitoring/grafana/dashboards:/etc/grafana/dashboards:ro,z
      - grafana_data:/var/lib/grafana
    networks:
      - hubuum_net
    healthcheck:
      test: ["CMD", "wget", "-q", "-O", "/dev/null", "http://127.0.0.1:3000/grafana/api/health"]
      interval: 5s
      timeout: 3s
      retries: 36
EOF
}

hubuum_monitoring_summary() {
  cat <<EOF

Monitoring:
  Grafana:    https://${MONITORING_HOST}/grafana/
  Prometheus: https://${MONITORING_HOST}/prometheus/
  Username: admin (separate passwords; not Hubuum accounts)
  Initial passwords are saved in ${INSTALL_DIR}/.env, readable only by root.
  Retrieve GRAFANA_ADMIN_PASSWORD and PROMETHEUS_PASSWORD there using sudo.
  Grafana password changes are managed in Grafana; updates preserve its database.
EOF
}
