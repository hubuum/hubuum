#!/bin/bash

# Fetching database details from environment variables with HUBUUM_TEST_ prefix
DB_USER="${HUBUUM_TEST_DB_USER:-postgres}"   # Default to 'postgres' if not set
DB_PASSWORD="${HUBUUM_TEST_DB_PASSWORD}"     # No default for password
DB_HOST="${HUBUUM_TEST_DB_HOST:-localhost}"  # Default to 'localhost' if not set
DB_PORT="${HUBUUM_TEST_DB_PORT:-5432}"       # Default to '5432' if not set
TEST_DB_PREFIX="hubuum_test_db_"

# Check if HUBUUM_TEST_DB_PASSWORD is set
if [ -z "$DB_PASSWORD" ]; then
    echo "Error: HUBUUM_TEST_DB_PASSWORD is not set."
    exit 1
fi

# Function to drop a single database
drop_db() {
    DB_NAME=$1
    PGPASSWORD=$DB_PASSWORD dropdb -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME
    if [ $? -eq 0 ]; then
        echo "Dropped database: $DB_NAME"
    else
        echo "Failed to drop database: $DB_NAME"
    fi
}

# Fetch and drop all databases starting with the test prefix
DBS=$(PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -t -c "SELECT datname FROM pg_database WHERE datname LIKE '${TEST_DB_PREFIX}%';")

echo "Dropping test databases..."
for DB in $DBS; do
    drop_db $DB
done

echo "Cleanup complete."
