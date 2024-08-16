#!/bin/bash
set -e

# Function to wait for the database to be ready
wait_for_db() {
    echo "Waiting for database to be ready..."    
    while ! diesel database setup  --migration-dir /migrations --database-url $HUBUUM_DATABASE_URL; do
        echo "Database @ $HUBUUM_DATABASE_URL is unavailable - sleeping"
        sleep 1
    done
    echo "Database is up - executing command"
}

# Wait for the database to be ready
wait_for_db

# Run migrations
echo "Running database migrations... (shouldn't be needed)"
diesel migration run --migration-dir /migrations --database-url $HUBUUM_DATABASE_URL

# Start the application
echo "Starting the application..."
exec hubuum-server "$@"