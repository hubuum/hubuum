//! Temporary application compatibility shims for PostgreSQL retention tests.

#[cfg(test)]
use crate::errors::ApiError;
#[cfg(test)]
use crate::events::EventRetentionSettings;
#[cfg(test)]
use crate::storage::StorageError;
#[cfg(test)]
use crate::storage::postgres::{PostgresConnection, PostgresPool, PostgresRuntime};

#[cfg(test)]
pub(crate) async fn try_acquire_event_retention_lock(
    connection: &mut PostgresConnection,
) -> Result<bool, ApiError> {
    hubuum_storage_postgres::operations::event_retention::try_acquire_event_retention_lock(
        connection,
    )
    .await
    .map_err(StorageError::from)
    .map_err(ApiError::from)
}

#[cfg(test)]
pub(crate) async fn purge_event_retention_without_archive(
    pool: &PostgresPool,
    settings: EventRetentionSettings,
) -> Result<crate::storage::EventRetentionSummary, ApiError> {
    hubuum_storage_postgres::operations::event_retention::purge_without_archive(
        &PostgresRuntime::new(pool.clone()),
        settings,
    )
    .await
    .map_err(StorageError::from)
    .map_err(ApiError::from)
}
