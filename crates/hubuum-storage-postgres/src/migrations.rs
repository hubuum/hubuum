use diesel::{Connection, PgConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use hubuum_storage_core::{StorageError, StorageErrorKind};

use crate::PostgresStorageError;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// Apply every embedded PostgreSQL migration not yet recorded by Diesel.
pub fn run_embedded_migrations(connection_url: &str) -> Result<usize, StorageError> {
    let mut connection = PgConnection::establish(connection_url).map_err(|error| {
        StorageError::from(PostgresStorageError::new(
            StorageErrorKind::Database,
            format!("failed to connect for storage migrations: {error}"),
            None,
        ))
    })?;
    let applied = connection
        .run_pending_migrations(MIGRATIONS)
        .map_err(|error| {
            StorageError::from(PostgresStorageError::new(
                StorageErrorKind::Database,
                format!("failed to run storage migrations: {error}"),
                None,
            ))
        })?;
    Ok(applied.len())
}
