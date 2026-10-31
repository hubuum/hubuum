use async_trait::async_trait;
use diesel::sql_types::{BigInt, Bool, Jsonb, Nullable, Text, Uuid as SqlUuid};
use diesel::{QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use futures_util::TryStreamExt;
use hubuum_storage_core::*;
use serde::Deserialize;
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
    #[diesel(sql_type = Jsonb)]
    application: Value,
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

#[derive(QueryableByName)]
struct OptionalControlRow {
    #[diesel(sql_type = Nullable<Jsonb>)]
    material: Option<Value>,
}

#[derive(Deserialize)]
struct ApplicationRow {
    incarnation: Uuid,
    revision: i64,
    reapplication_revision: Option<i64>,
    released_revision: Option<i64>,
    checkpoint: Option<DeletionCheckpointIdentity>,
}

fn decode_application(material: Value) -> Result<ApplicationControlState, PostgresStorageError> {
    let row: ApplicationRow = serde_json::from_value(material)
        .map_err(|_| PostgresStorageError::database("Persisted application control is invalid"))?;
    let fence = ApplicationFence::new(row.incarnation, row.revision)?;
    let receipt = row
        .reapplication_revision
        .map(|revision| {
            ApplicationFence::new(row.incarnation, revision).map(ApplicationReapplication::new)
        })
        .transpose()?;
    Ok(ApplicationControlState::new(
        fence,
        receipt,
        row.released_revision,
        row.checkpoint,
    )?)
}

pub(crate) async fn read_application_on(
    connection: &mut PostgresConnection,
) -> Result<ApplicationControlState, PostgresStorageError> {
    let row = sql_query("SELECT public.hubuum_application_control_read() AS material")
        .get_result::<ControlRow>(connection)
        .await?;
    decode_application(row.material)
}

pub(crate) async fn capture_application_execution_on(
    connection: &mut PostgresConnection,
    owner: Option<RuntimeOwner>,
) -> Result<StorageExecutionPermit, PostgresStorageError> {
    let row = sql_query("SELECT public.hubuum_capture_application_execution($1,$2) AS material")
        .bind::<Nullable<SqlUuid>, _>(owner.as_ref().map(RuntimeOwner::id))
        .bind::<Nullable<SqlUuid>, _>(owner.as_ref().map(RuntimeOwner::ownership_token))
        .get_result::<OptionalControlRow>(connection)
        .await?;
    let material = row.material.ok_or_else(|| {
        PostgresStorageError::unavailable(
            "Application quarantine or retired runtime prevents admission",
        )
    })?;
    read_authority_on(connection).await?;
    Ok(StorageExecutionPermit::new(
        owner,
        decode_application(material)?.fence(),
    ))
}

async fn read_authority_on(
    connection: &mut PostgresConnection,
) -> Result<DeletionControlState, PostgresStorageError> {
    let authority = sql_query("SELECT public.hubuum_deletion_control_read() AS material")
        .get_result::<ControlRow>(connection)
        .await?;
    serde_json::from_value(authority.material).map_err(|_| {
        PostgresStorageError::database(
            "Persisted deletion authority is invalid; admission remains blocked",
        )
    })
}

async fn acknowledge_runtime_drain_on(
    connection: &mut PostgresConnection,
    owner: &RuntimeOwner,
    fence: ApplicationFence,
) -> Result<(), PostgresStorageError> {
    let row = sql_query("SELECT public.hubuum_acknowledge_runtime_drain($1,$2,$3,$4) AS allowed")
        .bind::<SqlUuid, _>(owner.id())
        .bind::<SqlUuid, _>(owner.ownership_token())
        .bind::<SqlUuid, _>(fence.incarnation())
        .bind::<BigInt, _>(fence.revision())
        .get_result::<AllowedRow>(connection)
        .await?;
    if !row.allowed {
        return Err(PostgresStorageError::conflict(
            "Runtime ownership or application drain generation changed",
        ));
    }
    Ok(())
}

/// Run before any restore-table write, sharing the coordinator's acquisition.
pub(crate) async fn acknowledge_coordinator_drain_on(
    connection: &mut PostgresConnection,
    owner: &RuntimeOwner,
    local_work_is_settled: &(dyn Fn() -> bool + Send + Sync),
) -> Result<(), PostgresStorageError> {
    // This existing helper pins authority even when exposure is closed. The
    // subsequent reads and callback therefore refer to one protected fence.
    sql_query("SELECT public.hubuum_deletion_execution_allowed() AS allowed")
        .get_result::<AllowedRow>(connection)
        .await?;
    let application = read_application_on(connection).await?;
    let authority = read_authority_on(connection).await?;
    if !application.is_released_for(&authority) && local_work_is_settled() {
        acknowledge_runtime_drain_on(connection, owner, application.fence()).await?;
    }
    Ok(())
}

async fn lock_authority(
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
    Ok(())
}

async fn require_drained(
    connection: &mut PostgresConnection,
    fence: ApplicationFence,
) -> Result<(), PostgresStorageError> {
    let row = sql_query("SELECT NOT EXISTS (SELECT 1 FROM hubuum_control.runtime_registrations WHERE drained_incarnation IS DISTINCT FROM $1 OR drained_revision IS DISTINCT FROM $2) AS allowed")
        .bind::<SqlUuid,_>(fence.incarnation()).bind::<BigInt,_>(fence.revision()).get_result::<AllowedRow>(connection).await?;
    if !row.allowed {
        return Err(PostgresStorageError::unavailable(
            "Registered or uncertain runtimes have not drained this application generation",
        ));
    }
    Ok(())
}

async fn exact_closed_history_copy(
    connection: &mut PostgresConnection,
    request: &HotObjectHistoryDeletion,
) -> Result<Option<i64>, PostgresStorageError> {
    let unresolved=sql_query("SELECT EXISTS (SELECT 1 FROM public.hubuumobject WHERE logical_identity #>> '{subject,kind}' IS DISTINCT FROM 'resource' OR logical_identity #>> '{subject,identity,state}' IS DISTINCT FROM 'known' OR logical_identity #>> '{subject,identity,identity}' IS NULL OR (logical_identity #>> '{subject,identity,identity}')::uuid = '00000000-0000-0000-0000-000000000000'::uuid OR logical_identity #>> '{subject,version}' IS NULL OR (logical_identity #>> '{subject,version}')::uuid = '00000000-0000-0000-0000-000000000000'::uuid UNION ALL SELECT 1 FROM public.hubuumobject_history WHERE logical_identity #>> '{subject,kind}' IS DISTINCT FROM 'resource' OR logical_identity #>> '{subject,identity,state}' IS DISTINCT FROM 'known' OR logical_identity #>> '{subject,identity,identity}' IS NULL OR (logical_identity #>> '{subject,identity,identity}')::uuid = '00000000-0000-0000-0000-000000000000'::uuid OR logical_identity #>> '{subject,version}' IS NULL OR (logical_identity #>> '{subject,version}')::uuid = '00000000-0000-0000-0000-000000000000'::uuid) AS allowed")
        .get_result::<AllowedRow>(connection).await?;
    if unresolved.allowed {
        return Err(PostgresStorageError::conflict(
            "Unresolved current or historical identity prevents safe deletion reapplication",
        ));
    }
    require_no_restored_descendant(connection, request).await?;
    let live = sql_query("SELECT EXISTS (SELECT 1 FROM public.hubuumobject WHERE (logical_identity #>> '{subject,version}')::uuid=$1) AS allowed")
        .bind::<SqlUuid,_>(Uuid::from_bytes(*request.target().version().as_bytes())).get_result::<AllowedRow>(connection).await?;
    if live.allowed {
        return Err(PostgresStorageError::conflict(
            "Retained deletion targets a live object version",
        ));
    }
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

async fn require_no_restored_descendant(
    connection: &mut PostgresConnection,
    request: &HotObjectHistoryDeletion,
) -> Result<(), PostgresStorageError> {
    // Stream candidates: a resource may retain arbitrarily many versions. The
    // exact source key also finds inconsistent descendants on another resource,
    // which must not be silently certified as absent.
    let rows = sql_query("SELECT logical_identity AS material FROM public.hubuumobject WHERE (logical_identity #>> '{subject,identity,identity}')::uuid=$1 OR logical_identity->'references' ? $2 UNION ALL SELECT logical_identity AS material FROM public.hubuumobject_history WHERE (logical_identity #>> '{subject,identity,identity}')::uuid=$1 OR logical_identity->'references' ? $2")
        .bind::<SqlUuid, _>(Uuid::from_bytes(*request.target().resource().as_bytes()))
        .bind::<Text, _>(format!("/restore_source_versions/{}", request.target().version()))
        .load_stream::<ControlRow>(connection)
        .await?;
    futures_util::pin_mut!(rows);
    while let Some(row) = rows.try_next().await? {
        let evidence: StorageIdentityEvidence = serde_json::from_value(row.material)
            .map_err(|_| PostgresStorageError::conflict("Restored copy provenance is invalid"))?;
        let identity = evidence
            .subject()
            .and_then(|subject| subject.record_identity())
            .ok_or_else(|| {
                PostgresStorageError::conflict("Restored copy identity is unresolved")
            })?;
        if evidence
            .restore_source_versions()
            .contains(request.target().version())
            || (identity.resource() == request.target().resource()
                && identity.version() != request.target().version()
                && evidence.has_unknown_restore_source())
        {
            return Err(PostgresStorageError::conflict(
                "A restored version may retain the deleted source; exact closed-copy cleanup cannot certify its removal",
            ));
        }
    }
    Ok(())
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
    let changed = sql_query("UPDATE hubuum_control.deletion_authority SET material=$1 WHERE id=1 AND material IS DISTINCT FROM $1")
        .bind::<Jsonb, _>(material)
        .execute(connection)
        .await?;
    if changed != 0 {
        sql_query("UPDATE hubuum_control.application_control SET reapplication_revision=NULL, released_revision=NULL, checkpoint=NULL WHERE id=1").execute(connection).await?;
    }
    Ok(())
}

#[async_trait]
impl DeletionControlStorage for PostgresStorage {
    async fn read_deletion_control(&self) -> Result<DeletionControlState, StorageError> {
        self.runtime()
            .with_control_connection(async |connection| {
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
        self.runtime().with_control_connection(async |connection| {
            // One statement binds a committed control snapshot to the physical
            // cluster/timeline position. This privileged capability is not used
            // by ordinary runtime requests and is unavailable on a standby.
            let row = sql_query("WITH position AS MATERIALIZED (SELECT pg_current_wal_insert_lsn() AS lsn) SELECT material, (pg_control_system()).system_identifier::text AS system_identifier, pg_walfile_name(position.lsn) AS wal_file, position.lsn::text AS lsn, public.hubuum_application_control_read() AS application FROM hubuum_control.deletion_authority CROSS JOIN position WHERE id = 1")
                .get_result::<CaptureRow>(connection).await?;
            let state: DeletionControlState = serde_json::from_value(row.material)
                .map_err(|_| PostgresStorageError::database("Persisted deletion authority is invalid; keep recovery quarantined"))?;
            let application = decode_application(row.application)?;
            let capture = DeletionControlCapture::new(state,
                DeletionRecoveryPosition::postgresql(row.system_identifier, row.wal_file, row.lsn)
                    .map_err(|error| PostgresStorageError::invalid_persisted_value("deletion recovery coordinates", error))?);
            Ok::<_, PostgresStorageError>(match application.reapplication().filter(|receipt| receipt.fence() == application.fence()) { Some(receipt) => capture.with_reapplication(receipt.clone())?, None => capture })
        }).await.map_err(StorageError::from)
    }

    async fn register_runtime_isolated(
        &self,
        isolation: Option<RuntimeIsolationDomain>,
    ) -> Result<RuntimeRegistration, StorageError> {
        let registration = RuntimeRegistration::generate();
        self.runtime()
            .with_control_transaction(async move |connection| {
                let result = sql_query(
                    "SELECT public.hubuum_register_runtime_isolated($1,$2,$3) AS allowed",
                )
                .bind::<SqlUuid, _>(registration.id())
                .bind::<SqlUuid, _>(registration.ownership_token())
                .bind::<Nullable<Jsonb>, _>(
                    isolation
                        .map(serde_json::to_value)
                        .transpose()
                        .map_err(|_| {
                            PostgresStorageError::internal("Cannot encode isolation domain")
                        })?,
                )
                .get_result::<AllowedRow>(connection)
                .await?;
                if !result.allowed {
                    return Err(PostgresStorageError::unavailable(
                        "Application quarantine or runtime capacity prevents startup",
                    ));
                }
                let authority =
                    sql_query("SELECT public.hubuum_deletion_control_read() AS material")
                        .get_result::<ControlRow>(connection)
                        .await?;
                serde_json::from_value::<DeletionControlState>(authority.material).map_err(
                    |_| {
                        PostgresStorageError::database(
                            "Persisted deletion authority is invalid; startup remains blocked",
                        )
                    },
                )?;
                Ok::<_, PostgresStorageError>(registration)
            })
            .await
            .map_err(StorageError::from)
    }

    async fn capture_application_execution(
        &self,
        owner: Option<RuntimeOwner>,
    ) -> Result<StorageExecutionPermit, StorageError> {
        self.runtime()
            .with_control_transaction(async move |connection| {
                capture_application_execution_on(connection, owner).await
            })
            .await
            .map_err(StorageError::from)
    }

    async fn capture_application_execution_with_maintenance(
        &self,
        owner: Option<RuntimeOwner>,
    ) -> Result<ApplicationWorkAdmission, StorageError> {
        if self
            .runtime()
            .runtime_owner()
            .is_some_and(|bound| Some(bound) != owner.as_ref())
        {
            return Err(StorageError::unavailable(
                "Worker admission requires its bound runtime owner",
            ));
        }
        let inherited = self
            .current_execution_scope()
            .application_access_override()
            .cloned();
        self.runtime().with_control_transaction(async move |connection| {
            let permit = if let Some(permit) = inherited {
                if permit.owner() != owner.as_ref() {
                    return Err(PostgresStorageError::unavailable("Inherited worker permit belongs to another runtime"));
                }
                let allowed = sql_query("SELECT public.hubuum_application_execution_allowed($1,$2,$3,$4) AS allowed")
                    .bind::<Nullable<SqlUuid>, _>(owner.as_ref().map(RuntimeOwner::id))
                    .bind::<Nullable<SqlUuid>, _>(owner.as_ref().map(RuntimeOwner::ownership_token))
                    .bind::<SqlUuid, _>(permit.fence().incarnation())
                    .bind::<BigInt, _>(permit.fence().revision())
                    .get_result::<AllowedRow>(connection).await?;
                if !allowed.allowed {
                    return Err(PostgresStorageError::unavailable("Inherited worker permit is stale, retired, or quarantined"));
                }
                permit
            } else {
                capture_application_execution_on(connection, owner).await?
            };
            let maintenance = crate::operations::maintenance::maintenance_state_on_connection(connection).await?;
            Ok::<_, PostgresStorageError>(ApplicationWorkAdmission::new(permit, maintenance))
        }).await.map_err(StorageError::from)
    }

    async fn read_application_control(&self) -> Result<ApplicationControlState, StorageError> {
        self.runtime()
            .with_control_connection(async |connection| read_application_on(connection).await)
            .await
            .map_err(StorageError::from)
    }

    async fn acknowledge_runtime_drain(
        &self,
        owner: RuntimeOwner,
        fence: ApplicationFence,
    ) -> Result<(), StorageError> {
        self.runtime()
            .with_control_transaction(async move |connection| {
                acknowledge_runtime_drain_on(connection, &owner, fence).await
            })
            .await
            .map_err(StorageError::from)
    }

    async fn reapply_deletion_decisions(
        &self,
        expected: DeletionControlState,
        fence: ApplicationFence,
        checkpoint: DeletionCheckpointIdentity,
    ) -> Result<ApplicationReapplication, StorageError> {
        self.runtime().with_control_transaction(async move |connection| {
            lock_authority(connection,&expected).await?;
            let application = read_application_on(connection).await?;
            if application.fence()!=fence { return Err(PostgresStorageError::conflict("Application image changed before reapplication")); }
            require_drained(connection, fence).await?;
            let mut targets = Vec::new();
            for (intent_id,intent) in expected.intents() {
                if intent.bindings().len()!=intent.copies().len() || intent.bindings().is_empty() {
                    return Err(PostgresStorageError::conflict("Unknown retained copies prevent reapplication"));
                }
                for (copy,binding) in intent.bindings() {
                    let request=HotObjectHistoryDeletion::new(*intent_id,*copy,binding.target())?;
                    // Validate all obligations before any physical removal.
                    let row=exact_closed_history_copy(connection,&request).await?;
                    targets.push((request,row));
                }
            }
            let mut state=expected.clone();
            for (request,row) in targets {
                let cleanup=HotObjectHistoryCleanup::new(state.clone(),request,checkpoint.clone())?;
                let change=state.record_hot_object_history_removed(&cleanup)?;
                if let Some(id)=row { sql_query("DELETE FROM public.hubuumobject_history WHERE history_id=$1").bind::<BigInt,_>(id).execute(connection).await?; }
                state=change.next().clone();
            }
            if state==expected && let Some(receipt)=application.reapplication().filter(|receipt|receipt.fence()==fence) { return Ok::<_,PostgresStorageError>(receipt.clone()); }
            let change=state.record_application_reapplication()?;
            store_control(connection,change.next()).await?;
            let receipt=ApplicationReapplication::new(ApplicationFence::new(fence.incarnation(),change.next().revision())?);
            sql_query("UPDATE hubuum_control.application_control SET reapplication_revision=$1 WHERE id=1")
                .bind::<BigInt,_>(receipt.revision()).execute(connection).await?;
            Ok(receipt)
        }).await.map_err(StorageError::from)
    }

    async fn release_application_exposure(
        &self,
        receipt: ApplicationReapplication,
        checkpoint: DeletionCheckpointIdentity,
    ) -> Result<(), StorageError> {
        self.runtime().with_control_transaction(async move |connection| {
            sql_query("SELECT material FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE").get_result::<ControlRow>(connection).await?;
            let application=read_application_on(connection).await?;
            application.release(&receipt, checkpoint.clone()).map_err(|error|PostgresStorageError::conflict(error.to_string()))?;
            sql_query("UPDATE public.system_maintenance SET generation=0,state='normal',restore_job_id=NULL,entered_at=NULL,updated_at=timezone('UTC',clock_timestamp()) WHERE id=1 AND EXISTS (SELECT 1 FROM hubuum_control.application_control WHERE id=1 AND pending_restore)").execute(connection).await?;
            sql_query("SELECT pg_notify('hubuum_maintenance','normal') WHERE EXISTS (SELECT 1 FROM hubuum_control.application_control WHERE id=1 AND pending_restore)").execute(connection).await?;
            sql_query("UPDATE hubuum_control.application_control SET released_revision=$1,checkpoint=$2,pending_restore=FALSE WHERE id=1")
                .bind::<BigInt,_>(receipt.revision()).bind::<Text,_>(checkpoint.as_str()).execute(connection).await?;
            Ok::<_,PostgresStorageError>(())
        }).await.map_err(StorageError::from)
    }

    async fn recover_application_control(
        &self,
        change: DeletionControlChange,
        isolation: RuntimeIsolationDomain,
    ) -> Result<ApplicationFence, StorageError> {
        self.runtime().with_control_transaction(async move |connection| {
            lock_authority(connection,change.previous()).await?;
            sql_query("UPDATE hubuum_control.deletion_authority SET material=material WHERE id=1").execute(connection).await?;
            let domain=serde_json::to_value(isolation).map_err(|_|PostgresStorageError::internal("Cannot encode isolation domain"))?;
            let row=sql_query("SELECT NOT EXISTS (SELECT 1 FROM hubuum_control.runtime_registrations WHERE isolation IS DISTINCT FROM $1) AS allowed")
                .bind::<Jsonb,_>(domain).get_result::<AllowedRow>(connection).await?;
            if !row.allowed { return Err(PostgresStorageError::unavailable("Unknown runtime isolation ownership prevents recovery")); }
            sql_query("DELETE FROM hubuum_control.runtime_registrations").execute(connection).await?;
            sql_query("DELETE FROM hubuum_control.task_execution_claims").execute(connection).await?;
            store_control(connection,change.next()).await?;
            let fence=ApplicationFence::new(Uuid::new_v4(),change.next().revision())?;
            sql_query("UPDATE hubuum_control.application_control SET incarnation=$1,reapplication_revision=NULL,released_revision=NULL,checkpoint=NULL WHERE id=1")
                .bind::<SqlUuid,_>(fence.incarnation()).execute(connection).await?;
            Ok::<_,PostgresStorageError>(fence)
        }).await.map_err(StorageError::from)
    }

    async fn unregister_runtime(
        &self,
        registration: RuntimeRegistration,
    ) -> Result<(), StorageError> {
        self.runtime()
            .with_control_transaction(async move |connection| {
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
            .with_control_transaction(async move |connection| {
                lock_authority(connection, &expected).await?;
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
        self.runtime().with_control_transaction(async move |connection| {
            lock_authority(connection, request.expected()).await?;
            let fence=read_application_on(connection).await?.fence();
            require_drained(connection, fence).await?;
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
        self.runtime()
            .with_control_transaction(async move |connection| {
                lock_authority(connection, change.previous()).await?;
                store_control(connection, change.next()).await
            })
            .await
            .map_err(StorageError::from)
    }
}
