use hubuum_storage_core::StorageReadinessSnapshot;

use crate::operations::maintenance::maintenance_state_on_connection;
use crate::runtime::postgres_schema_is_ready;
use crate::{PostgresRuntime, PostgresStorageError};

pub async fn load_readiness_snapshot(
    runtime: &PostgresRuntime,
) -> Result<StorageReadinessSnapshot, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            let schema_ready = postgres_schema_is_ready(connection).await?;
            let maintenance_state = maintenance_state_on_connection(connection).await?;

            Ok::<_, PostgresStorageError>(StorageReadinessSnapshot::new(
                schema_ready,
                maintenance_state,
            ))
        })
        .await
}
