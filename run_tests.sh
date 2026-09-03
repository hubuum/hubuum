#!/bin/bash

set -euo pipefail

# Fetching database details from environment variables with HUBUUM_TEST_ prefix
DB_USER="${HUBUUM_TEST_DB_USER:-postgres}"  # Default to 'postgres' if not set
DB_PASSWORD="${HUBUUM_TEST_DB_PASSWORD:-}"  # No default for password
DB_HOST="${HUBUUM_TEST_DB_HOST:-localhost}" # Default to 'localhost' if not set
DB_PORT="${HUBUUM_TEST_DB_PORT:-5432}"      # Default to '5432' if not set
TEST_THREADS="${HUBUUM_TEST_THREADS:-16}"
TEST_DB_PREFIX="hubuum_test_db_"
MIGRATIONS_DIR="./crates/hubuum-storage-postgres/migrations" # Adapter migrations
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

# Generate a collision-resistant database name. Keep it identifier-safe.
UNIQUE_SUFFIX="$(date +%s)_$$_${RANDOM}${RANDOM}"
TEST_DB_NAME="${TEST_DB_PREFIX}${UNIQUE_SUFFIX}"
SINGLE_TEST_DB_NAME="${TEST_DB_NAME}_single"
OWNER_ROLE="hubuum_test_owner_${UNIQUE_SUFFIX}"
MIGRATOR_ROLE="hubuum_test_migrator_${UNIQUE_SUFFIX}"
RUNTIME_ROLE="hubuum_test_runtime_${UNIQUE_SUFFIX}"
ADMIN_TEST_URL="postgres://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$TEST_DB_NAME$SSL_MODE"

cleanup() {
    if [ -n "${SINGLE_TEST_DB_NAME:-}" ]; then
        PGPASSWORD=$DB_PASSWORD psql "$ROOT_URL" \
            -v ON_ERROR_STOP=1 \
            -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$SINGLE_TEST_DB_NAME' AND pid <> pg_backend_pid();" \
            > /dev/null 2>&1 || true
        PGPASSWORD=$DB_PASSWORD psql "$ROOT_URL" \
            -v ON_ERROR_STOP=1 \
            -c "DROP DATABASE IF EXISTS $SINGLE_TEST_DB_NAME;" \
            > /dev/null 2>&1 || true
    fi
    if [ -n "${TEST_DB_NAME:-}" ]; then
        PGPASSWORD=$DB_PASSWORD psql "$ROOT_URL" \
            -v ON_ERROR_STOP=1 \
            -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$TEST_DB_NAME' AND pid <> pg_backend_pid();" \
            > /dev/null 2>&1 || true
        PGPASSWORD=$DB_PASSWORD psql "$ROOT_URL" \
            -v ON_ERROR_STOP=1 \
            -c "DROP DATABASE IF EXISTS $TEST_DB_NAME;" \
            > /dev/null 2>&1 || true
        PGPASSWORD=$DB_PASSWORD psql "$ROOT_URL" \
            -v ON_ERROR_STOP=1 \
            -c "REVOKE $OWNER_ROLE FROM $MIGRATOR_ROLE; DROP ROLE IF EXISTS $RUNTIME_ROLE; DROP ROLE IF EXISTS $MIGRATOR_ROLE; DROP ROLE IF EXISTS $OWNER_ROLE;" \
            > /dev/null 2>&1 || true
    fi
}

trap cleanup EXIT

# Prove the default topology can migrate a database owned by the connected
# login without creating or reconciling any cluster roles.
PGPASSWORD=$DB_PASSWORD psql "$ROOT_URL" -v ON_ERROR_STOP=1 \
    -c "CREATE DATABASE $SINGLE_TEST_DB_NAME;" > /dev/null
SINGLE_TEST_URL="postgres://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$SINGLE_TEST_DB_NAME$SSL_MODE"
env -u HUBUUM_MIGRATION_DATABASE_URL \
    -u HUBUUM_DATABASE_ROLE_MODE \
    -u HUBUUM_DATABASE_OWNER_ROLE \
    -u HUBUUM_DATABASE_MIGRATOR_ROLE \
    -u HUBUUM_DATABASE_RUNTIME_ROLE \
    cargo run --quiet --features embedded-migrations --bin hubuum-admin -- \
        --migrate \
        --database-url "$SINGLE_TEST_URL" > /dev/null
PGPASSWORD=$DB_PASSWORD psql "$SINGLE_TEST_URL" -v ON_ERROR_STOP=1 \
    -c "SELECT 1 FROM users LIMIT 1;" > /dev/null
PGPASSWORD=$DB_PASSWORD psql "$ROOT_URL" -v ON_ERROR_STOP=1 \
    -c "DROP DATABASE $SINGLE_TEST_DB_NAME;" > /dev/null
SINGLE_TEST_DB_NAME=""

# Create distinct cluster roles and a database owned by the non-login schema
# owner. Password interpolation is handled by psql, not by SQL string assembly.
PGPASSWORD=$DB_PASSWORD psql "$ROOT_URL" -v ON_ERROR_STOP=1 \
    -v owner_role="$OWNER_ROLE" \
    -v migrator_role="$MIGRATOR_ROLE" \
    -v runtime_role="$RUNTIME_ROLE" \
    -v role_password="$DB_PASSWORD" \
    -f scripts/create-test-database-roles.sql \
    > /dev/null
PGPASSWORD=$DB_PASSWORD psql "$ROOT_URL" -v ON_ERROR_STOP=1 \
    -c "CREATE DATABASE $TEST_DB_NAME OWNER $OWNER_ROLE;" > /dev/null

echo "Created test database: $TEST_DB_NAME"


export HUBUUM_MIGRATION_DATABASE_URL="postgres://$MIGRATOR_ROLE:$DB_PASSWORD@$DB_HOST:$DB_PORT/$TEST_DB_NAME$SSL_MODE"
export HUBUUM_DATABASE_URL="postgres://$RUNTIME_ROLE:$DB_PASSWORD@$DB_HOST:$DB_PORT/$TEST_DB_NAME$SSL_MODE"
export HUBUUM_DATABASE_ROLE_MODE="split"
export HUBUUM_DATABASE_OWNER_ROLE="$OWNER_ROLE"
export HUBUUM_DATABASE_MIGRATOR_ROLE="$MIGRATOR_ROLE"
export HUBUUM_DATABASE_RUNTIME_ROLE="$RUNTIME_ROLE"
export HUBUUM_DATABASE_PRIVILEGE_MODE="strict"
export HUBUUM_DATABASE_ROLE_TESTS="true"
# Every integration test owns a small connection pool. Bound parallelism so a
# high-core test host cannot exhaust PostgreSQL while retaining parallel tests.
export RUST_TEST_THREADS="$TEST_THREADS"


# Run migrations, lock the schema as we define views in the sql and those go bye-bye with print-schema.
# See https://github.com/diesel-rs/diesel/issues/1482.
PGOPTIONS="-c role=$OWNER_ROLE" diesel migration run \
    --migration-dir "$MIGRATIONS_DIR" \
    --database-url "$HUBUUM_MIGRATION_DATABASE_URL" \
    --locked-schema

# Apply the same generated manifest used by production migration tooling.
ROLE_SETUP_SQL="$(cargo run --quiet --bin hubuum-admin -- \
    --database-role-setup-sql \
    --database-owner-role "$OWNER_ROLE" \
    --database-migrator-role "$MIGRATOR_ROLE" \
    --database-runtime-role "$RUNTIME_ROLE")"
PGPASSWORD=$DB_PASSWORD psql "$ADMIN_TEST_URL" \
    -v ON_ERROR_STOP=1 -c "$ROLE_SETUP_SQL" > /dev/null

# Run adapter-native tests before the application suite while the isolated,
# migrated database is available.
if [ "$#" -eq 0 ]; then
    cargo test -p hubuum-storage-postgres \
        --features integration-test-support \
        --test database_privileges \
        split_role_reconciliation_adopts_existing_single_role_objects \
        -- --exact --ignored
fi
cargo test -p hubuum-storage-postgres \
    --features integration-test-support,scale-benchmark-support "$@"

# Run the application and request-level suites.
if cargo test --features integration-test-support "$@"; then
    echo "Test database dropped: $TEST_DB_NAME"
else
    exit 1
fi
