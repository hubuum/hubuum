#[cfg(test)]
use crate::errors::ApiError;
#[cfg(test)]
use crate::models::MaintenanceState;
#[cfg(test)]
use crate::storage::StorageError;
#[cfg(test)]
use crate::storage::postgres::PostgresConnection;

#[cfg(test)]
pub(crate) async fn maintenance_state_conn(
    connection: &mut PostgresConnection,
) -> Result<MaintenanceState, ApiError> {
    hubuum_storage_postgres::operations::maintenance::maintenance_state_on_connection(connection)
        .await
        .map_err(StorageError::from)
        .map_err(ApiError::from)
}
