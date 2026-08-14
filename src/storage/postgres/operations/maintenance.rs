use crate::errors::ApiError;
use crate::models::MaintenanceState;
use crate::storage::StorageError;
use crate::storage::postgres::PostgresConnection;

pub(crate) async fn maintenance_state_conn(
    connection: &mut PostgresConnection,
) -> Result<MaintenanceState, ApiError> {
    hubuum_storage_postgres::operations::maintenance::maintenance_state_on_connection(connection)
        .await
        .map_err(StorageError::from)
        .map_err(ApiError::from)
}
