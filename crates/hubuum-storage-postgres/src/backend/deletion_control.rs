use async_trait::async_trait;
use diesel::sql_types::{BigInt, Bool, Jsonb, Text, Uuid as SqlUuid};
use diesel::{QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::{
    DeletionControlCapture, DeletionControlChange, DeletionControlState, DeletionControlStorage,
    DeletionRecoveryPosition, HotObjectHistoryCleanup, HotObjectHistoryDeletion,
    RuntimeRegistration, StorageError, StorageIdentityEvidence,
};
use serde_json::Value;
use uuid::Uuid;

use super::PostgresStorage;
use crate::{PostgresConnection, PostgresStorageError};

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

#[derive(QueryableByName)]
struct AllowedRow {
    #[diesel(sql_type = Bool)]
    allowed: bool,
}

#[derive(QueryableByName)]
struct HistoryCopyRow {
    #[diesel(sql_type = BigInt)]
    history_id: i64,
    #[diesel(sql_type = Jsonb)]
    logical_identity: Value,
    #[diesel(sql_type = Bool)]
    closed: bool,
}

async fn lock_offline_authority(
    connection: &mut PostgresConnection,
    expected: &DeletionControlState,
) -> Result<(), PostgresStorageError> {
    let row =
        sql_query("SELECT material FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE")
            .get_result::<ControlRow>(connection)
            .await?;
    let current: DeletionControlState = serde_json::from_value(row.material)
        .map_err(|_| PostgresStorageError::database("Persisted deletion authority is invalid"))?;
    if &current != expected {
        return Err(PostgresStorageError::conflict(
            "Deletion authority changed; recapture before retrying",
        ));
    }
    let offline = sql_query(
        "SELECT NOT EXISTS (SELECT 1 FROM hubuum_control.runtime_registrations) AS allowed",
    )
    .get_result::<AllowedRow>(connection)
    .await?;
    if !offline.allowed {
        return Err(PostgresStorageError::unavailable(
            "Registered or uncertain runtimes prevent deletion cleanup",
        ));
    }
    Ok(())
}

async fn exact_closed_history_copy(
    connection: &mut PostgresConnection,
    request: &HotObjectHistoryDeletion,
) -> Result<Option<i64>, PostgresStorageError> {
    let rows = sql_query("SELECT history_id, logical_identity, valid_to IS NOT NULL AS closed FROM public.hubuumobject_history WHERE (logical_identity #>> '{subject,version}')::uuid = $1 LIMIT 2 FOR UPDATE")
        .bind::<SqlUuid, _>(Uuid::from_bytes(*request.target().version().as_bytes())).load::<HistoryCopyRow>(connection).await?;
    if rows.len() > 1 {
        return Err(PostgresStorageError::conflict(
            "History version identity is ambiguous",
        ));
    }
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let identity: StorageIdentityEvidence =
        serde_json::from_value(row.logical_identity).map_err(|_| {
            PostgresStorageError::conflict("History copy identity is invalid or unresolved")
        })?;
    if !row.closed
        || identity
            .subject()
            .and_then(|subject| subject.record_identity())
            != Some(request.target())
    {
        return Err(PostgresStorageError::conflict(
            "History cleanup requires the exact closed resource/version identity",
        ));
    }
    Ok(Some(row.history_id))
}

async fn store_control(
    connection: &mut PostgresConnection,
    state: &DeletionControlState,
) -> Result<(), PostgresStorageError> {
    state
        .encode()
        .map_err(|error| PostgresStorageError::validation_failed(error.to_string()))?;
    let material = serde_json::to_value(state)
        .map_err(|_| PostgresStorageError::database("Cannot encode deletion authority"))?;
    sql_query("UPDATE hubuum_control.deletion_authority SET material=$1 WHERE id=1")
        .bind::<Jsonb, _>(material)
        .execute(connection)
        .await?;
    Ok(())
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

    async fn register_runtime(&self) -> Result<RuntimeRegistration, StorageError> {
        let registration = RuntimeRegistration::generate();
        self.runtime()
            .with_transaction(async move |connection| {
                let result = sql_query("SELECT public.hubuum_register_runtime($1, $2) AS allowed")
                    .bind::<SqlUuid, _>(registration.id())
                    .bind::<SqlUuid, _>(registration.ownership_token())
                    .get_result::<AllowedRow>(connection)
                    .await?;
                if !result.allowed {
                    return Err(PostgresStorageError::unavailable(
                        "Deletion quarantine or runtime registration capacity prevents startup",
                    ));
                }
                // The SQL function retains the authority lock. Validate the full
                // material before committing ownership, including fields that the
                // minimal database shape constraint cannot establish.
                let authority =
                    sql_query("SELECT public.hubuum_deletion_control_read() AS material")
                        .get_result::<ControlRow>(connection)
                        .await?;
                let authority: DeletionControlState = serde_json::from_value(authority.material)
                    .map_err(|_| {
                        PostgresStorageError::database(
                            "Persisted deletion authority is invalid; startup remains blocked",
                        )
                    })?;
                if !authority.permits_exposure() {
                    return Err(PostgresStorageError::unavailable(
                        "Deletion quarantine prevents startup",
                    ));
                }
                Ok::<_, PostgresStorageError>(registration)
            })
            .await
            .map_err(StorageError::from)
    }

    async fn unregister_runtime(
        &self,
        registration: RuntimeRegistration,
    ) -> Result<(), StorageError> {
        self.runtime()
            .with_transaction(async move |connection| {
                let result =
                    sql_query("SELECT public.hubuum_unregister_runtime($1, $2) AS allowed")
                        .bind::<SqlUuid, _>(registration.id())
                        .bind::<SqlUuid, _>(registration.ownership_token())
                        .get_result::<AllowedRow>(connection)
                        .await?;
                if !result.allowed {
                    return Err(PostgresStorageError::conflict(
                        "Runtime registration ownership differs; registration retained",
                    ));
                }
                Ok::<_, PostgresStorageError>(())
            })
            .await
            .map_err(StorageError::from)
    }

    async fn prepare_hot_object_history_deletion(
        &self,
        expected: DeletionControlState,
        request: HotObjectHistoryDeletion,
    ) -> Result<DeletionControlState, StorageError> {
        self.runtime()
            .with_transaction(async move |connection| {
                lock_offline_authority(connection, &expected).await?;
                let change = expected
                    .record_hot_object_history_intent(&request)
                    .map_err(|error| PostgresStorageError::conflict(error.to_string()))?;
                let row = exact_closed_history_copy(connection, &request).await?;
                if row.is_none() && !expected.intents().contains_key(&request.intent()) {
                    return Err(PostgresStorageError::not_found(
                        "Exact closed history version was not found",
                    ));
                }
                store_control(connection, change.next()).await?;
                Ok::<_, PostgresStorageError>(change.next().clone())
            })
            .await
            .map_err(StorageError::from)
    }

    async fn cleanup_hot_object_history(
        &self,
        request: HotObjectHistoryCleanup,
    ) -> Result<DeletionControlState, StorageError> {
        self.runtime().with_transaction(async move |connection| {
            lock_offline_authority(connection, request.expected()).await?;
            // Always inspect the physical copy, even for an existing Removed
            // receipt: primary recovery may have brought the old version back.
            let row = exact_closed_history_copy(connection, request.request()).await?;
            let change = request.expected().record_hot_object_history_removed(&request)
                .map_err(|error| PostgresStorageError::conflict(error.to_string()))?;
            if let Some(id) = row {
                let removed = sql_query("DELETE FROM public.hubuumobject_history WHERE history_id=$1 AND valid_to IS NOT NULL AND (logical_identity #>> '{subject,version}')::uuid = $2 AND (logical_identity #>> '{subject,identity,identity}')::uuid = $3")
                    .bind::<BigInt, _>(id)
                    .bind::<SqlUuid, _>(Uuid::from_bytes(*request.request().target().version().as_bytes()))
                    .bind::<SqlUuid, _>(Uuid::from_bytes(*request.request().target().resource().as_bytes()))
                    .execute(connection).await?;
                if removed != 1 { return Err(PostgresStorageError::conflict("History copy changed during cleanup")); }
            }
            store_control(connection, change.next()).await?;
            Ok::<_, PostgresStorageError>(change.next().clone())
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
