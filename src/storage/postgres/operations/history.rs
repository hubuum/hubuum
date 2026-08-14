use crate::errors::ApiError;
use crate::events::PrincipalNames;
use crate::storage::StorageError;
use crate::storage::postgres::{PostgresPool, PostgresRuntime};

/// Transitional adapter bridge for event/task code that still owns its query
/// flow in the application crate. History reads themselves are implemented by
/// `hubuum-storage-postgres` and exposed to ordinary consumers through
/// `HistoryStorage`.
pub(crate) async fn resolve_principal_names(
    pool: &PostgresPool,
    principal_ids: Vec<i32>,
) -> Result<PrincipalNames, ApiError> {
    Ok(
        hubuum_storage_postgres::operations::history::resolve_principal_names(
            &PostgresRuntime::new(pool.clone()),
            principal_ids,
        )
        .await
        .map_err(StorageError::from)?
        .into_iter()
        .map(|row| row.into_parts())
        .collect(),
    )
}
