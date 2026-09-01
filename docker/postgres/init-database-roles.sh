#!/bin/sh
set -eu

require_identifier() {
    case "$2" in
        ''|[!A-Za-z_]*|*[!A-Za-z0-9_$]*)
            echo "invalid PostgreSQL identifier in $1" >&2
            exit 1
            ;;
    esac
}

: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"

role_mode="${HUBUUM_DATABASE_ROLE_MODE:-single}"
case "$role_mode" in
    single)
        exit 0
        ;;
    split)
        ;;
    *)
        echo "HUBUUM_DATABASE_ROLE_MODE must be single or split" >&2
        exit 1
        ;;
esac

: "${POSTGRES_MIGRATOR_PASSWORD:?POSTGRES_MIGRATOR_PASSWORD is required}"
: "${POSTGRES_RUNTIME_PASSWORD:?POSTGRES_RUNTIME_PASSWORD is required}"

owner_role="${HUBUUM_DATABASE_OWNER_ROLE:-hubuum_owner}"
migrator_role="${HUBUUM_DATABASE_MIGRATOR_ROLE:-hubuum_migrator}"
runtime_role="${HUBUUM_DATABASE_RUNTIME_ROLE:-hubuum_runtime}"
require_identifier HUBUUM_DATABASE_OWNER_ROLE "$owner_role"
require_identifier HUBUUM_DATABASE_MIGRATOR_ROLE "$migrator_role"
require_identifier HUBUUM_DATABASE_RUNTIME_ROLE "$runtime_role"

psql --set ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set database_name="$POSTGRES_DB" \
    --set owner_role="$owner_role" \
    --set migrator_role="$migrator_role" \
    --set runtime_role="$runtime_role" \
    --set migrator_password="$POSTGRES_MIGRATOR_PASSWORD" \
    --set runtime_password="$POSTGRES_RUNTIME_PASSWORD" <<'SQL'
CREATE ROLE :"owner_role" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE :"migrator_role" LOGIN PASSWORD :'migrator_password' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE :"runtime_role" LOGIN PASSWORD :'runtime_password' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
GRANT :"owner_role" TO :"migrator_role";
ALTER DATABASE :"database_name" OWNER TO :"owner_role";
ALTER SCHEMA public OWNER TO :"owner_role";
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SQL
