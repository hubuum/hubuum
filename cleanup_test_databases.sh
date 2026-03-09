#!/bin/bash

set -euo pipefail

# Fetching database details from environment variables with HUBUUM_TEST_ prefix
DB_USER="${HUBUUM_TEST_DB_USER:-postgres}"   # Default to 'postgres' if not set
DB_PASSWORD="${HUBUUM_TEST_DB_PASSWORD:-}"   # No default for password
DB_HOST="${HUBUUM_TEST_DB_HOST:-localhost}"  # Default to 'localhost' if not set
DB_PORT="${HUBUUM_TEST_DB_PORT:-5432}"       # Default to '5432' if not set
TEST_DB_PREFIX="hubuum_test_db_"
CA_CERT="aiven.pem"

# Check if HUBUUM_TEST_DB_PASSWORD is set
if [ -z "$DB_PASSWORD" ]; then
    echo "Error: HUBUUM_TEST_DB_PASSWORD is not set."
    exit 1
fi

# Determine if we are connecting to Aiven PostgreSQL and set SSL mode accordingly
ROOT_URL="postgres://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT"
if [[ "$DB_HOST" == *aivencloud.com ]]; then
    ROOT_URL="postgres://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/defaultdb?sslmode=require"
    export PGSSLMODE=require
    export PGSSLROOTCERT=$CA_CERT
fi

# Function to drop a single database
drop_db() {
    local db_name="$1"
    PGPASSWORD=$DB_PASSWORD psql "$ROOT_URL" \
        -v ON_ERROR_STOP=1 \
        -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$db_name' AND pid <> pg_backend_pid();" \
        > /dev/null
    PGPASSWORD=$DB_PASSWORD psql "$ROOT_URL" \
        -v ON_ERROR_STOP=1 \
        -c "DROP DATABASE IF EXISTS $db_name;" \
        > /dev/null
    echo "Dropped database: $db_name"
}

# Fetch and drop all databases starting with the test prefix
DBS=$(
    PGPASSWORD=$DB_PASSWORD psql "$ROOT_URL" \
        -v ON_ERROR_STOP=1 \
        -t \
        -c "SELECT datname FROM pg_database WHERE datname LIKE '${TEST_DB_PREFIX}%';"
)

echo "Dropping test databases..."
for DB in $DBS; do
    drop_db "$DB"
done

echo "Cleanup complete."
