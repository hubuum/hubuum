#!/bin/sh
set -e

should_skip_migrations() {
    case "${HUBUUM_SKIP_MIGRATIONS:-false}" in
        1|yes|y|true|on)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# Function to wait for the database to be ready
wait_for_db() {
    echo "Waiting for database to be ready..."
    if should_skip_migrations; then
        while ! diesel migration list --migration-dir /migrations --database-url "$HUBUUM_DATABASE_URL" >/dev/null 2>&1; do
            echo "Database is unavailable - sleeping"
            sleep 1
        done
    else
        while ! diesel database setup --migration-dir /migrations --database-url "$HUBUUM_DATABASE_URL"; do
            echo "Database is unavailable - sleeping"
            sleep 1
        done
    fi
    echo "Database is up - executing command"
}

# Wait for the database to be ready
wait_for_db

if should_skip_migrations; then
    echo "HUBUUM_SKIP_MIGRATIONS is set; skipping database migrations."
fi

# Start the application
echo "Starting the application..."
exec hubuum-server "$@"
