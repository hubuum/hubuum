use diesel::RunQueryDsl;
use diesel::connection::SimpleConnection;
use diesel::deserialize::QueryableByName;
use diesel::sql_types::{BigInt, Text};
use diesel::{Connection, PgConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use hubuum_storage_core::StorageError;

use crate::{DatabaseRoleNames, PostgresStorageError, database_role_reconciliation_sql};

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

#[derive(QueryableByName)]
struct DisposableDatabaseState {
    #[diesel(sql_type = Text)]
    database_name: String,
    #[diesel(sql_type = BigInt)]
    user_object_count: i64,
}

#[derive(QueryableByName)]
struct DisposableRestoreMarkers {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    has_migrations: bool,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    has_collections: bool,
}

/// Apply every embedded PostgreSQL migration not yet recorded by Diesel.
pub fn run_embedded_migrations(connection_url: &str) -> Result<usize, StorageError> {
    run_embedded_migrations_with_roles(connection_url, None)
}

/// Apply migrations as the schema-owner role and reconcile runtime grants.
pub fn run_embedded_migrations_as(
    connection_url: &str,
    roles: &DatabaseRoleNames,
) -> Result<usize, StorageError> {
    run_embedded_migrations_with_roles(connection_url, Some(roles))
}

/// Require a genuinely empty, non-maintenance database and migrate it for an
/// isolated restore verification. The emptiness check and migrations share
/// one connection so a caller cannot accidentally point this path at an
/// already initialized Hubuum database.
pub fn prepare_disposable_restore_database(connection_url: &str) -> Result<usize, StorageError> {
    let mut connection = PgConnection::establish(connection_url).map_err(|error| {
        StorageError::from(PostgresStorageError::database(format!(
            "failed to connect to the disposable restore-test database: {error}"
        )))
    })?;
    let state = diesel::sql_query(
        "SELECT current_database()::text AS database_name, COUNT(*)::bigint AS user_object_count \
         FROM ( \
           SELECT relation.oid \
           FROM pg_catalog.pg_class AS relation \
           JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace \
           WHERE namespace.nspname <> 'information_schema' \
             AND namespace.nspname NOT LIKE 'pg\\_%' ESCAPE '\\' \
             AND relation.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', 'c') \
           UNION ALL \
           SELECT routine.oid \
           FROM pg_catalog.pg_proc AS routine \
           JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = routine.pronamespace \
           WHERE namespace.nspname <> 'information_schema' \
             AND namespace.nspname NOT LIKE 'pg\\_%' ESCAPE '\\' \
           UNION ALL \
           SELECT type.oid \
           FROM pg_catalog.pg_type AS type \
           JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = type.typnamespace \
           WHERE namespace.nspname <> 'information_schema' \
             AND namespace.nspname NOT LIKE 'pg\\_%' ESCAPE '\\' \
             AND type.typtype IN ('c', 'd', 'e', 'm', 'r') \
             AND type.typrelid = 0 \
           UNION ALL \
           SELECT namespace.oid \
           FROM pg_catalog.pg_namespace AS namespace \
           WHERE namespace.nspname NOT IN ('information_schema', 'public') \
             AND namespace.nspname NOT LIKE 'pg\\_%' ESCAPE '\\' \
         ) AS user_objects",
    )
    .get_result::<DisposableDatabaseState>(&mut connection)
    .map_err(|error| {
        StorageError::from(PostgresStorageError::database(format!(
            "failed to inspect the disposable restore-test database: {error}"
        )))
    })?;
    if matches!(
        state.database_name.as_str(),
        "postgres" | "template0" | "template1"
    ) {
        return Err(StorageError::invalid_input(
            "Refused to use a PostgreSQL maintenance database for restore verification",
        ));
    }
    if state.user_object_count != 0 {
        return Err(StorageError::invalid_input(format!(
            "Refused restore verification because the target database contains {} user object(s); provide a newly created empty disposable database",
            state.user_object_count
        )));
    }
    connection
        .run_pending_migrations(MIGRATIONS)
        .map_err(|error| {
            StorageError::from(PostgresStorageError::database(format!(
                "failed to migrate the disposable restore-test database: {error}"
            )))
        })
        .map(|migrations| migrations.len())
}

/// Remove the Hubuum schema created by [`prepare_disposable_restore_database`]
/// after an isolated restore test. This deliberately refuses databases that
/// do not contain both expected Hubuum marker tables.
pub fn reset_disposable_restore_database(connection_url: &str) -> Result<(), StorageError> {
    let mut connection = PgConnection::establish(connection_url).map_err(|error| {
        StorageError::from(PostgresStorageError::database(format!(
            "failed to connect for disposable restore-test cleanup: {error}"
        )))
    })?;
    let markers = diesel::sql_query(
        "SELECT to_regclass('public.__diesel_schema_migrations') IS NOT NULL AS has_migrations, \
                to_regclass('public.collections') IS NOT NULL AS has_collections",
    )
    .get_result::<DisposableRestoreMarkers>(&mut connection)
    .map_err(|error| {
        StorageError::from(PostgresStorageError::database(format!(
            "failed to inspect disposable restore-test cleanup markers: {error}"
        )))
    })?;
    if !markers.has_migrations || !markers.has_collections {
        return Err(StorageError::invalid_input(
            "Refused disposable restore-test cleanup because Hubuum schema markers are missing",
        ));
    }
    connection
        .batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .map_err(|error| {
            StorageError::from(PostgresStorageError::database(format!(
                "failed to reset the disposable restore-test database: {error}"
            )))
        })
}

fn run_embedded_migrations_with_roles(
    connection_url: &str,
    roles: Option<&DatabaseRoleNames>,
) -> Result<usize, StorageError> {
    let mut connection = PgConnection::establish(connection_url).map_err(|error| {
        StorageError::from(PostgresStorageError::database(format!(
            "failed to connect for storage migrations: {error}"
        )))
    })?;
    if let Some(roles) = roles {
        connection
            .batch_execute(&format!("SET ROLE {};", roles.owner().quoted()))
            .map_err(|error| {
                StorageError::from(PostgresStorageError::database(format!(
                    "failed to assume database schema-owner role '{}': {error}",
                    roles.owner().as_str()
                )))
            })?;
    }
    let applied_count = connection
        .run_pending_migrations(MIGRATIONS)
        .map_err(|error| {
            StorageError::from(PostgresStorageError::database(format!(
                "failed to run storage migrations: {error}"
            )))
        })?
        .len();
    if let Some(roles) = roles {
        connection
            .batch_execute(&database_role_reconciliation_sql(roles))
            .map_err(|error| {
                StorageError::from(PostgresStorageError::database(format!(
                    "failed to reconcile database ownership and runtime grants: {error}"
                )))
            })?;
    }
    Ok(applied_count)
}
