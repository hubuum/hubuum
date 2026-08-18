//! Temporary application compatibility shims for PostgreSQL fan-out test
//! support.
//!
//! Production capability dispatch calls the adapter crate directly. These
//! helpers remain while older in-crate integration tests still start from a
//! raw pool.

#[cfg(any(test, feature = "integration-test-support"))]
use crate::errors::ApiError;
#[cfg(test)]
use crate::events::EventFanoutSettings;
#[cfg(any(test, feature = "integration-test-support"))]
use crate::storage::StorageError;
#[cfg(any(test, feature = "integration-test-support"))]
use crate::storage::postgres::{PostgresPool, PostgresRuntime};

#[cfg(test)]
pub(crate) async fn claim_events_for_fanout(
    pool: &PostgresPool,
    settings: EventFanoutSettings,
) -> Result<Vec<i64>, ApiError> {
    hubuum_storage_postgres::operations::event_fanout::claim_event_ids(
        &PostgresRuntime::unobserved(pool.clone()),
        settings,
    )
    .await
    .map_err(StorageError::from)
    .map_err(ApiError::from)
}

#[cfg(any(test, feature = "integration-test-support"))]
pub async fn fanout_event(pool: &PostgresPool, event_id: i64) -> Result<usize, ApiError> {
    hubuum_storage_postgres::operations::event_fanout::fanout_event(
        &PostgresRuntime::unobserved(pool.clone()),
        event_id,
    )
    .await
    .map_err(StorageError::from)
    .map_err(ApiError::from)
}

#[cfg(test)]
pub(crate) async fn fanout_events(
    pool: &PostgresPool,
    event_ids: &[i64],
) -> Result<usize, ApiError> {
    hubuum_storage_postgres::operations::event_fanout::fanout_events(
        &PostgresRuntime::unobserved(pool.clone()),
        event_ids,
    )
    .await
    .map_err(StorageError::from)
    .map_err(ApiError::from)
}

#[cfg(test)]
pub(crate) async fn count_event_deliveries_for_event(
    pool: &PostgresPool,
    event_id: i64,
) -> Result<i64, ApiError> {
    hubuum_storage_postgres::operations::event_fanout::count_event_deliveries_for_event(
        &PostgresRuntime::unobserved(pool.clone()),
        event_id,
    )
    .await
    .map_err(StorageError::from)
    .map_err(ApiError::from)
}
