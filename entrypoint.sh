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

database_ready() {
    source_set=false
    root_set=false
    url_set=false
    backend_set=false
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --secret-source|--secret-file-root|--database-url|--storage-backend)
                option="$1"
                if [ "$#" -lt 2 ]; then
                    echo "Missing value for $option" >&2
                    exit 2
                fi
                value="$2"
                shift
                ;;
            --secret-source=*|--secret-file-root=*|--database-url=*|--storage-backend=*)
                option="${1%%=*}"
                value="${1#*=}"
                ;;
            *)
                shift
                continue
                ;;
        esac
        case "$option" in
            --secret-source) source_set=true; source_value="$value" ;;
            --secret-file-root) root_set=true; root_value="$value" ;;
            --database-url) url_set=true; url_value="$value" ;;
            --storage-backend) backend_set=true; backend_value="$value" ;;
        esac
        shift
    done

    set -- --database-ready
    if [ "$source_set" = true ]; then
        set -- "$@" --secret-source "$source_value"
    fi
    if [ "$root_set" = true ]; then
        set -- "$@" --secret-file-root "$root_value"
    fi
    if [ "$url_set" = true ]; then
        set -- "$@" --database-url "$url_value"
    fi
    if [ "$backend_set" = true ]; then
        set -- "$@" --storage-backend "$backend_value"
    fi
    hubuum-admin "$@"
}

echo "Waiting for database to be ready..."
until database_ready "$@"; do
    echo "Database is unavailable - sleeping"
    sleep 1
done

echo "Database is ready. Schema migrations are owned by a separate one-shot administrative workload."

# Start the application
echo "Starting the application..."
exec hubuum-server "$@"
