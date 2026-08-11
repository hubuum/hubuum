use diesel::{Connection, PgConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::errors::ApiError;
use crate::storage::StorageError;

use super::error::map_postgres_error;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub(in crate::storage) fn run_embedded_migrations(
    connection_url: &str,
) -> Result<usize, StorageError> {
    let mut connection = PgConnection::establish(connection_url).map_err(|error| {
        map_postgres_error(ApiError::DbConnectionError(format!(
            "failed to connect for storage migrations: {error}"
        )))
    })?;
    let applied = connection
        .run_pending_migrations(MIGRATIONS)
        .map_err(|error| {
            map_postgres_error(ApiError::DatabaseError(format!(
                "failed to run storage migrations: {error}"
            )))
        })?;
    Ok(applied.len())
}
