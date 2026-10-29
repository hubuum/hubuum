use async_trait::async_trait;
use diesel::sql_types::{Jsonb, Text};
use diesel::{QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::{
    DeletionControlCapture, DeletionControlChange, DeletionControlState, DeletionControlStorage,
    DeletionRecoveryPosition, StorageError,
};
use serde_json::Value;

use super::PostgresStorage;
use crate::PostgresStorageError;

#[derive(QueryableByName)]
struct CaptureRow {
    #[diesel(sql_type = Jsonb)]
    material: Value,
    #[diesel(sql_type = Text)]
    system_identifier: String,
    #[diesel(sql_type = Text)]
    wal_file: String,
    #[diesel(sql_type = Text)]
    lsn: String,
}

#[derive(QueryableByName)]
struct ControlRow {
    #[diesel(sql_type = Jsonb)]
    material: Value,
}

#[async_trait]
impl DeletionControlStorage for PostgresStorage {
    async fn read_deletion_control(&self) -> Result<DeletionControlState, StorageError> {
        self.runtime()
            .with_connection(async |connection| {
                let row = sql_query("SELECT public.hubuum_deletion_control_read() AS material")
                    .get_result::<ControlRow>(connection)
                    .await?;
                serde_json::from_value(row.material).map_err(|_| {
                    PostgresStorageError::database(
                        "Persisted deletion authority is invalid; keep recovery quarantined",
                    )
                })
            })
            .await
            .map_err(StorageError::from)
    }

    async fn capture_deletion_control(&self) -> Result<DeletionControlCapture, StorageError> {
        self.runtime().with_connection(async |connection| {
            // One statement binds a committed control snapshot to the physical
            // cluster/timeline position. This privileged capability is not used
            // by ordinary runtime requests and is unavailable on a standby.
            let row = sql_query("WITH position AS MATERIALIZED (SELECT pg_current_wal_insert_lsn() AS lsn) SELECT material, (pg_control_system()).system_identifier::text AS system_identifier, pg_walfile_name(position.lsn) AS wal_file, position.lsn::text AS lsn FROM hubuum_control.deletion_authority CROSS JOIN position WHERE id = 1")
                .get_result::<CaptureRow>(connection).await?;
            let state: DeletionControlState = serde_json::from_value(row.material)
                .map_err(|_| PostgresStorageError::database("Persisted deletion authority is invalid; keep recovery quarantined"))?;
            Ok::<_, PostgresStorageError>(DeletionControlCapture::new(state,
                DeletionRecoveryPosition::postgresql(row.system_identifier, row.wal_file, row.lsn)
                    .map_err(|error| PostgresStorageError::invalid_persisted_value("deletion recovery coordinates", error))?))
        }).await.map_err(StorageError::from)
    }

    async fn commit_deletion_control(
        &self,
        change: DeletionControlChange,
    ) -> Result<(), StorageError> {
        let before = serde_json::to_value(change.previous())
            .map_err(|_| StorageError::internal("Cannot encode deletion authority"))?;
        let after = serde_json::to_value(change.next())
            .map_err(|_| StorageError::internal("Cannot encode deletion authority"))?;
        self.runtime().with_transaction(async move |connection| {
            let changed = sql_query("UPDATE hubuum_control.deletion_authority SET material = $1 WHERE id = 1 AND material = $2")
                .bind::<Jsonb, _>(after).bind::<Jsonb, _>(before).execute(connection).await?;
            if changed != 1 { return Err(PostgresStorageError::conflict("Deletion authority changed; recapture before retrying")); }
            Ok::<_, PostgresStorageError>(())
        }).await.map_err(StorageError::from)
    }
}
