use diesel::connection::SimpleConnection;
use diesel::{Connection, PgConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use hubuum_storage_core::StorageError;

use crate::{DatabaseRoleNames, PostgresStorageError, database_role_reconciliation_sql};

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

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
