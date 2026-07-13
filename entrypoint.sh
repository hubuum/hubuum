#!/bin/sh
set -e

should_skip_migrations() {
    case "${HUBUUM_RUNTIME_ROLE:-all}" in
        api|worker)
            # Distributed long-running roles never own schema changes. Run
            # hubuum-admin --migrate as a one-shot job before rollout.
            return 0
            ;;
    esac

    case "${HUBUUM_SKIP_MIGRATIONS:-false}" in
        1|yes|y|true|on)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

echo "Waiting for database to be ready..."
until {
    if should_skip_migrations; then
        hubuum-admin --database-ready
    else
        hubuum-admin --migrate
    fi
}; do
    echo "Database is unavailable - sleeping"
    sleep 1
done

if should_skip_migrations; then
    echo "Database is ready; migrations were skipped."
else
    echo "Database is ready; all pending migrations were applied."
fi

# Start the application
echo "Starting the application..."
exec hubuum-server "$@"
