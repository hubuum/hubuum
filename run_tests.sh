#!/bin/bash

# Fetching database details from environment variables with HUBUUM_TEST_ prefix
DB_USER="${HUBUUM_TEST_DB_USER:-postgres}"  # Default to 'postgres' if not set
DB_PASSWORD="${HUBUUM_TEST_DB_PASSWORD}"    # No default for password
DB_HOST="${HUBUUM_TEST_DB_HOST:-localhost}" # Default to 'localhost' if not set
DB_PORT="${HUBUUM_TEST_DB_PORT:-5432}"      # Default to '5432' if not set
TEST_DB_PREFIX="hubuum_test_db_"
MIGRATIONS_DIR="./migrations"               # Your migrations directory
CA_CERT="aiven.pem"

# Check if HUBUUM_TEST_DB_PASSWORD is set
if [ -z "$DB_PASSWORD" ]; then
    echo "Error: HUBUUM_TEST_DB_PASSWORD is not set."
    exit 1
fi

# Determine if we are connecting to Aiven PostgreSQL and set SSL mode accordingly
SSL_MODE=""
ROOT_URL="postgres://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT"
if [[ "$DB_HOST" == *aivencloud.com ]]; then
    SSL_MODE="?sslmode=require"
    ROOT_URL="postgres://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/defaultdb$SSL_MODE"
    export PGSSLMODE=require
    export PGSSLROOTCERT=$CA_CERT
fi

# Generate a unique database name
UNIQUE_SUFFIX=$(date +%s)                  # Using current timestamp
TEST_DB_NAME="${TEST_DB_PREFIX}${UNIQUE_SUFFIX}"

# Create a new database
PGPASSWORD=$DB_PASSWORD psql $ROOT_URL -c "CREATE DATABASE $TEST_DB_NAME;" > /dev/null

# Check if database creation was successful
if [ $? -ne 0 ]; then
    echo "Failed to create test database"
    exit 1
fi

echo "Created test database: $TEST_DB_NAME"


export HUBUUM_DATABASE_URL="postgres://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$TEST_DB_NAME$SSL_MODE"


# Run migrations
diesel migration run --migration-dir $MIGRATIONS_DIR --database-url $HUBUUM_DATABASE_URL 

# Run the tests
cargo test $@

# Optional: Drop the test database after tests are complete
PGPASSWORD=$DB_PASSWORD psql $ROOT_URL -c "DROP DATABASE $TEST_DB_NAME;" > /dev/null

echo "Test database dropped: $TEST_DB_NAME"
