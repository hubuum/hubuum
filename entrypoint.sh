#!/bin/sh
set -e

runtime_role() {
    role="${HUBUUM_RUNTIME_ROLE:-all}"
    expects_value=false
    for argument in "$@"; do
        if [ "$expects_value" = true ]; then
            role="$argument"
            expects_value=false
            continue
        fi
        case "$argument" in
            --runtime-role)
                expects_value=true
                ;;
            --runtime-role=*)
                role="${argument#--runtime-role=}"
                ;;
        esac
    done
    printf '%s\n' "$role"
}

container_healthcheck() {
    role="$(runtime_role "$@")"
    if [ "$role" = worker ]; then
        # The server process supervises every registered background worker and
        # exits if any of them stops unexpectedly (or if none were started).
        # PID 1 liveness therefore represents worker liveness for this role.
        kill -0 1
        return $?
    fi

    scheme=http
    if [ -n "${HUBUUM_TLS_CERT_PATH:-}" ] && [ -n "${HUBUUM_TLS_KEY_PATH:-}" ]; then
        scheme=https
    fi
    wget --quiet --no-check-certificate --output-document=/dev/null \
        "${scheme}://127.0.0.1:${HUBUUM_BIND_PORT:-8080}/healthz"
}

if [ "${1:-}" = --container-healthcheck ]; then
    shift
    container_healthcheck "$@"
    exit $?
fi

echo "Waiting for database to be ready..."
until hubuum-admin --database-ready; do
    echo "Database is unavailable - sleeping"
    sleep 1
done

echo "Database is ready. Schema migrations are owned by a separate one-shot administrative workload."

# Start the application
echo "Starting the application..."
exec hubuum-server "$@"
