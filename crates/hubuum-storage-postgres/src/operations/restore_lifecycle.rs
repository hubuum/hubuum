//! PostgreSQL-owned restore staging and coordinator lifecycle.

use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::dsl::sql;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::sql_types::{BigInt, Text, Timestamp};
use diesel::{Insertable, Queryable, QueryableByName, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{MaintenanceState, PrincipalId, RestoreJobId};
use hubuum_events_core::{Action, ActorKind, AuditDocument, EntityType, NewEvent};
use hubuum_storage_core::{
    StorageBackupHistorySection, StorageBackupReplay, StorageBackupStateSection, StorageCallSite,
    StorageRestoreArtifactJob, StorageRestoreArtifactSummary, StorageRestoreCompletion,
    StorageRestoreCoordinatorSnapshot, StorageRestoreDrainState, StorageRestoreFailure,
    StorageRestoreInitiator, StorageRestoreInstance, StorageRestoreJobStatus,
    StorageRestoreJobSummary, StorageRestoreReplayApply, StorageRestoreStageArtifact,
    StorageRestoreStatus, StorageRestoreTimestamps,
};
use serde_json::Value;
use uuid::Uuid;

use crate::operations::backup::{
    history_row_to_postgres, history_table, state_row_to_postgres, state_table,
};
use crate::operations::computed_fields::enqueue_restored_computed_rebuilds_on_connection;
use crate::operations::event_record::append_event;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError, with_storage_call_site};

// A single row may exceed the batch byte target, but remains bounded by the
// admitted backup document. Never duplicate an entire section for insertion.
const RESTORE_BATCH_ROWS: usize = 256;
const RESTORE_BATCH_BYTES: usize = 1024 * 1024;

const DATABASE_UTC_NOW_SQL: &str = "clock_timestamp() AT TIME ZONE 'UTC'";

const TRUNCATE_TABLES: &[&str] = &[
    "computed_field_history",
    "query_usage_resource_owners",
    "schema_repair_reports",
    "schema_impact_findings",
    "schema_validation_work",
    "object_schema_evidence",
    "class_schema_history",
    "class_schema_state",
    "class_schema_revisions",
    "object_computed_data",
    "class_computation_state",
    "computed_field_definitions",
    "query_usage_declarations",
    "event_deliveries",
    "events",
    "schema_profile_outputs",
    "backup_task_outputs",
    "export_task_outputs",
    "remote_call_results",
    "import_task_results",
    "tasks",
    "credential_sources",
    "credential_activity",
    "refresh_tokens",
    "auth_sessions",
    "token_scopes",
    "tokens",
    "event_sink_collection_grants",
    "event_subscriptions",
    "event_sinks",
    "remote_targets_history",
    "remote_targets",
    "export_templates_history",
    "export_templates",
    "permissions",
    "collection_authorization_state",
    "hubuumobject_relation_history",
    "hubuumobject_relation",
    "hubuumobject_history",
    "hubuumobject",
    "hubuumclass_relation_history",
    "hubuumclass_relation",
    "hubuumclass_reachability",
    "hubuumclass_history",
    "hubuumclass",
    "collection_closure",
    "collections_history",
    "collections",
    "group_membership_sources",
    "group_memberships",
    "service_accounts",
    "users",
    "principals",
    "groups",
    "identity_scopes",
];

const SERIAL_ID_TABLES: &[&str] = &[
    "class_schema_history",
    "identity_scopes",
    "groups",
    "principals",
    "collections",
    "permissions",
    "hubuumclass",
    "computed_field_definitions",
    "query_usage_declarations",
    "hubuumclass_relation",
    "hubuumobject",
    "hubuumobject_relation",
    "export_templates",
    "remote_targets",
    "event_sinks",
    "event_subscriptions",
    "tokens",
    "tasks",
    "import_task_results",
    "export_task_outputs",
    "remote_call_results",
];

const HISTORY_SEQUENCE_TABLES: &[&str] = &[
    "computed_field_history",
    "collections_history",
    "hubuumclass_history",
    "hubuumclass_relation_history",
    "hubuumobject_history",
    "hubuumobject_relation_history",
    "export_templates_history",
    "remote_targets_history",
];

#[derive(Insertable)]
#[diesel(table_name = crate::schema::restore_jobs)]
struct NewRestoreJobRow {
    status: String,
    requested_by: Option<i32>,
    requested_by_identity_scope: String,
    requested_by_name: String,
    document: Vec<u8>,
    byte_size: i64,
    sha256: String,
    capability_hash: String,
    validation_summary: serde_json::Value,
    expires_at: NaiveDateTime,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::restore_jobs)]
struct RestoreJobStatusRow {
    id: i64,
    status: String,
    requested_by: Option<i32>,
    requested_by_identity_scope: String,
    requested_by_name: String,
    byte_size: i64,
    sha256: String,
    capability_hash: String,
    validation_summary: serde_json::Value,
    error: Option<String>,
    expires_at: NaiveDateTime,
    confirmed_at: Option<NaiveDateTime>,
    finished_at: Option<NaiveDateTime>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::restore_success_receipts)]
struct RestoreSuccessReceiptRow {
    id: i64,
    requested_by: Option<i32>,
    requested_by_identity_scope: String,
    requested_by_name: String,
    byte_size: i64,
    sha256: String,
    capability_hash: String,
    validation_summary: serde_json::Value,
    expires_at: NaiveDateTime,
    confirmed_at: NaiveDateTime,
    finished_at: NaiveDateTime,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::restore_jobs)]
struct RestoreApplyRow {
    id: i64,
    status: String,
    requested_by: Option<i32>,
    requested_by_identity_scope: String,
    requested_by_name: String,
    sha256: String,
}

#[derive(Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = crate::schema::server_instances)]
struct ServerInstanceRow {
    instance_id: Uuid,
    maintenance_generation: i64,
    drained: bool,
    last_heartbeat_at: NaiveDateTime,
    started_at: NaiveDateTime,
}

struct RestoreSummaryParts {
    id: i64,
    status: String,
    requested_by: Option<i32>,
    requested_by_identity_scope: String,
    requested_by_name: String,
    byte_size: i64,
    sha256: String,
    error: Option<String>,
    expires_at: NaiveDateTime,
    confirmed_at: Option<NaiveDateTime>,
    finished_at: Option<NaiveDateTime>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

fn summary_from_parts(
    parts: RestoreSummaryParts,
) -> Result<StorageRestoreJobSummary, PostgresStorageError> {
    let status = StorageRestoreJobStatus::from_stored(&parts.status)
        .map_err(|error| PostgresStorageError::invalid_persisted_value("restore status", error))?;
    let initiator = StorageRestoreInitiator::try_new(
        parts.requested_by.map(PrincipalId::new).transpose()?,
        parts.requested_by_identity_scope,
        parts.requested_by_name,
    )
    .map_err(|error| PostgresStorageError::invalid_persisted_value("restore initiator", error))?;
    let artifact =
        StorageRestoreArtifactSummary::try_new(parts.byte_size, parts.sha256).map_err(|error| {
            PostgresStorageError::invalid_persisted_value("restore artifact", error)
        })?;
    let timestamps = StorageRestoreTimestamps::try_new(
        parts.expires_at.and_utc(),
        parts.confirmed_at.map(|value| value.and_utc()),
        parts.finished_at.map(|value| value.and_utc()),
        parts.created_at.and_utc(),
        parts.updated_at.and_utc(),
    )
    .map_err(|error| PostgresStorageError::invalid_persisted_value("restore timestamps", error))?;
    crate::validate_persisted(
        "restore job summary",
        StorageRestoreJobSummary::try_new(
            RestoreJobId::new(parts.id)?,
            status,
            initiator,
            artifact,
            parts.error,
            timestamps,
        ),
    )
}

fn status_to_storage(
    row: RestoreJobStatusRow,
) -> Result<StorageRestoreStatus, PostgresStorageError> {
    let summary = summary_from_parts(RestoreSummaryParts {
        id: row.id,
        status: row.status,
        requested_by: row.requested_by,
        requested_by_identity_scope: row.requested_by_identity_scope,
        requested_by_name: row.requested_by_name,
        byte_size: row.byte_size,
        sha256: row.sha256,
        error: row.error,
        expires_at: row.expires_at,
        confirmed_at: row.confirmed_at,
        finished_at: row.finished_at,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })?;
    crate::validate_persisted(
        "restore status",
        StorageRestoreStatus::try_new(summary, row.capability_hash, row.validation_summary),
    )
}

fn success_receipt_to_storage(
    row: RestoreSuccessReceiptRow,
) -> Result<StorageRestoreStatus, PostgresStorageError> {
    let summary = summary_from_parts(RestoreSummaryParts {
        id: row.id,
        status: StorageRestoreJobStatus::Succeeded.as_str().to_owned(),
        requested_by: row.requested_by,
        requested_by_identity_scope: row.requested_by_identity_scope,
        requested_by_name: row.requested_by_name,
        byte_size: row.byte_size,
        sha256: row.sha256,
        error: None,
        expires_at: row.expires_at,
        confirmed_at: Some(row.confirmed_at),
        finished_at: Some(row.finished_at),
        created_at: row.created_at,
        updated_at: row.updated_at,
    })?;
    crate::validate_persisted(
        "restore status",
        StorageRestoreStatus::try_new(summary, row.capability_hash, row.validation_summary),
    )
}

fn instance_to_storage(
    instance: ServerInstanceRow,
) -> Result<StorageRestoreInstance, PostgresStorageError> {
    crate::validate_persisted(
        "restore coordinator instance",
        StorageRestoreInstance::try_new(
            instance.instance_id,
            instance.maintenance_generation,
            instance.drained,
        ),
    )
}

pub async fn stage_restore_artifact(
    runtime: &PostgresRuntime,
    request: StorageRestoreStageArtifact,
) -> Result<StorageRestoreStatus, PostgresStorageError> {
    use std::io::Read;
    let (initiator, artifact, capability_hash, validation_summary, expires_at) =
        request.into_parts();
    let initiator = initiator.into_parts();
    let input = NewRestoreJobRow {
        status: StorageRestoreJobStatus::Validated.as_str().to_owned(),
        requested_by: initiator.principal_id().map(|id| id.id()),
        requested_by_identity_scope: initiator.identity_scope().to_owned(),
        requested_by_name: initiator.name().to_owned(),
        document: Vec::new(),
        byte_size: artifact.summary().byte_size(),
        sha256: artifact.summary().sha256().to_owned(),
        capability_hash,
        validation_summary,
        expires_at: expires_at.naive_utc(),
    };
    runtime.with_transaction(async move |connection| -> Result<_, PostgresStorageError> {
        let staged_at = sql::<Timestamp>("statement_timestamp() AT TIME ZONE 'UTC'");
        let row = diesel::insert_into(crate::schema::restore_jobs::table)
            .values((input, crate::schema::restore_jobs::created_at.eq(staged_at.clone()), crate::schema::restore_jobs::updated_at.eq(staged_at)))
            .returning(RestoreJobStatusRow::as_returning()).get_result::<RestoreJobStatusRow>(connection).await?;
        let mut reader = artifact.reader();
        let mut ordinal = 0i32;
        let mut buffer = vec![0; 1024 * 1024];
        loop {
            let mut size = 0;
            while size < buffer.len() {
                let count = reader.read(&mut buffer[size..]).map_err(|e| PostgresStorageError::internal(e.to_string()))?;
                if count == 0 { break; } size += count;
            }
            if size == 0 { break; }
            diesel::sql_query("INSERT INTO restore_artifact_chunks (restore_job_id, ordinal, content) VALUES ($1, $2, $3)")
                .bind::<BigInt,_>(row.id).bind::<diesel::sql_types::Integer,_>(ordinal).bind::<diesel::sql_types::Binary,_>(&buffer[..size])
                .execute(connection).await?;
            ordinal = ordinal.checked_add(1).ok_or_else(|| PostgresStorageError::invalid_input("Too many restore chunks"))?;
        }
        status_to_storage(row)
    }).await
}

#[derive(QueryableByName)]
struct ArtifactChunk {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    ordinal: i32,
    #[diesel(sql_type = diesel::sql_types::Binary)]
    content: Vec<u8>,
}

pub async fn get_restore_artifact(
    runtime: &PostgresRuntime,
    job_id: i64,
) -> Result<StorageRestoreArtifactJob, PostgresStorageError> {
    use hubuum_storage_core::StorageRestoreArtifactBuilder;
    runtime.with_read_only_snapshot(async |connection| -> Result<_, PostgresStorageError> {
        let row = crate::schema::restore_jobs::table.filter(crate::schema::restore_jobs::id.eq(job_id))
            .select(RestoreJobStatusRow::as_select()).first::<RestoreJobStatusRow>(connection).await?;
        let (summary, capability, _) = status_to_storage(row)?.into_parts();
        let terminal = matches!(summary.status(), StorageRestoreJobStatus::Failed | StorageRestoreJobStatus::Expired | StorageRestoreJobStatus::Succeeded);
        let artifact = if terminal { None } else {
            let limit = usize::try_from(summary.artifact().byte_size()).map_err(|_| PostgresStorageError::database("Restore artifact size exceeds address space"))?;
            let mut builder = StorageRestoreArtifactBuilder::new(limit)?;
            let mut ordinal = 0i32;
            loop {
                // One chunk per query bounds both database-driver and application buffers.
                let chunk = diesel::sql_query("SELECT ordinal, content FROM restore_artifact_chunks WHERE restore_job_id=$1 AND ordinal >= $2 ORDER BY ordinal LIMIT 1")
                    .bind::<BigInt,_>(job_id).bind::<diesel::sql_types::Integer,_>(ordinal).get_result::<ArtifactChunk>(connection).await.optional()?;
                let Some(chunk) = chunk else { break; };
                if chunk.ordinal != ordinal { return Err(PostgresStorageError::database("Restore artifact has a missing chunk")); }
                builder.push(&chunk.content)?;
                ordinal = ordinal.checked_add(1).ok_or_else(|| PostgresStorageError::database("Restore chunk ordinal overflow"))?;
            }
            if ordinal == 0 {
                // Read legacy BYTEA stages through bounded substrings, too.
                for offset in (0..limit).step_by(1024 * 1024) {
                    let chunk = diesel::sql_query("SELECT 0 AS ordinal, substring(document FROM $2 FOR 1048576) AS content FROM restore_jobs WHERE id=$1")
                        .bind::<BigInt,_>(job_id).bind::<diesel::sql_types::Integer,_>(i32::try_from(offset + 1).map_err(|_| PostgresStorageError::database("Legacy restore BYTEA exceeds supported length"))?)
                        .get_result::<ArtifactChunk>(connection).await?;
                    builder.push(&chunk.content)?;
                }
            }
            Some(builder.finish()?)
        };
        StorageRestoreArtifactJob::try_new(summary, artifact, capability).map_err(|e| PostgresStorageError::invalid_persisted_value("restore artifact", e))
    }).await
}

/// Load the document-free status of a staged restore.
pub async fn get_restore_status(
    runtime: &PostgresRuntime,
    job_id: i64,
) -> Result<StorageRestoreStatus, PostgresStorageError> {
    let row = runtime
        .with_connection(async |connection| {
            crate::schema::restore_jobs::table
                .filter(crate::schema::restore_jobs::id.eq(job_id))
                .select(RestoreJobStatusRow::as_select())
                .first::<RestoreJobStatusRow>(connection)
                .await
                .optional()
        })
        .await?;
    if let Some(row) = row {
        return status_to_storage(row);
    }

    let receipt = runtime
        .with_connection(async |connection| {
            crate::schema::restore_success_receipts::table
                .filter(crate::schema::restore_success_receipts::id.eq(job_id))
                .select(RestoreSuccessReceiptRow::as_select())
                .first::<RestoreSuccessReceiptRow>(connection)
                .await
                .optional()
        })
        .await?
        .ok_or_else(|| {
            PostgresStorageError::not_found(format!("Restore stage {job_id} was not found"))
        })?;
    success_receipt_to_storage(receipt)
}

/// Expire a still-valid staged restore and erase its document.
pub async fn expire_restore_stage(
    runtime: &PostgresRuntime,
    job_id: i64,
) -> Result<bool, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            diesel::update(
                crate::schema::restore_jobs::table
                    .filter(crate::schema::restore_jobs::id.eq(job_id))
                    .filter(
                        crate::schema::restore_jobs::status
                            .eq(StorageRestoreJobStatus::Validated.as_str()),
                    ),
            )
            .set((
                crate::schema::restore_jobs::status.eq(StorageRestoreJobStatus::Expired.as_str()),
                crate::schema::restore_jobs::document.eq(Vec::<u8>::new()),
            ))
            .execute(connection)
            .await
        })
        .await
        .map(|changed| changed == 1)
}

/// Confirm a staged restore and atomically enter draining maintenance.
pub async fn start_restore_draining(
    runtime: &PostgresRuntime,
    request: hubuum_storage_core::StorageRestoreConfirmation,
) -> Result<DateTime<Utc>, PostgresStorageError> {
    let job_id = request.job_id().id();
    runtime
        .with_transaction(async |connection| -> Result<_, PostgresStorageError> {
            diesel::sql_query("SELECT pg_advisory_xact_lock(4850188191125217)")
                .execute(connection)
                .await?;
            if let Some(approval) = request.approval() {
                if approval.claim().restore_job_id() != Some(request.job_id()) {
                    return Err(PostgresStorageError::reauthentication_required());
                }
                super::credential_approval::consume(
                    connection,
                    Some(approval.claim()),
                    hubuum_storage_core::StorageCredentialOperation::ConfirmRestore,
                    None,
                    approval.context(),
                )
                .await?;
            }
            let confirmation_time = diesel::select(sql::<Timestamp>(DATABASE_UTC_NOW_SQL))
                .get_result::<NaiveDateTime>(connection)
                .await?;
            preserve_explicit_timestamps(connection).await?;
            let confirmation_time = diesel::update(
                crate::schema::restore_jobs::table
                    .filter(crate::schema::restore_jobs::id.eq(job_id))
                    .filter(crate::schema::restore_jobs::expires_at.gt(confirmation_time))
                    .filter(
                        crate::schema::restore_jobs::status
                            .eq(StorageRestoreJobStatus::Validated.as_str()),
                    ),
            )
            .set((
                crate::schema::restore_jobs::status.eq(StorageRestoreJobStatus::Confirmed.as_str()),
                crate::schema::restore_jobs::confirmed_at.eq(Some(confirmation_time)),
                crate::schema::restore_jobs::error.eq::<Option<String>>(None),
                crate::schema::restore_jobs::updated_at.eq(confirmation_time),
            ))
            .returning(crate::schema::restore_jobs::confirmed_at)
            .get_result::<Option<NaiveDateTime>>(connection)
            .await
            .optional()?
            .flatten()
            .ok_or_else(|| {
                PostgresStorageError::conflict("Restore stage was confirmed concurrently")
            })?;
            let maintenance_changed = diesel::sql_query(
                "UPDATE system_maintenance \
                 SET generation=generation+1, state='draining', restore_job_id=$1, \
                     entered_at=now(), updated_at=now() \
                 WHERE id=1 AND state='normal'",
            )
            .bind::<diesel::sql_types::BigInt, _>(job_id)
            .execute(connection)
            .await?;
            if maintenance_changed != 1 {
                return Err(PostgresStorageError::conflict(
                    "Another maintenance operation is already active",
                ));
            }
            diesel::sql_query("SELECT pg_notify('hubuum_maintenance', 'draining')")
                .execute(connection)
                .await?;
            crate::reach_fault_point(
                crate::PostgresFaultPoint::RestoreAfterDrainTransition,
                Some(connection),
            )
            .await?;
            Ok(confirmation_time.and_utc())
        })
        .await
}

/// Replace the complete durable backend state with one validated backup.
pub async fn apply_restore_replay(
    runtime: &PostgresRuntime,
    request: StorageRestoreReplayApply,
) -> Result<StorageRestoreCompletion, PostgresStorageError> {
    let (job_id, metadata, snapshot, includes_history) = request.into_parts();
    let snapshot = snapshot.with_schema_limits(runtime.schema_limits())?;
    apply_restore_replay_parts(
        runtime,
        job_id.id(),
        metadata.into_parts(),
        snapshot,
        includes_history,
    )
    .await
}

async fn apply_restore_replay_parts(
    runtime: &PostgresRuntime,
    job_id: i64,
    metadata: (i32, DateTime<Utc>, String),
    snapshot: StorageBackupReplay,
    includes_history: bool,
) -> Result<StorageRestoreCompletion, PostgresStorageError> {
    let (backup_version, backup_created_at, backup_source_version) = metadata;

    runtime
        .with_transaction(
            async move |connection| -> Result<StorageRestoreCompletion, PostgresStorageError> {
                diesel::sql_query("SELECT pg_advisory_xact_lock(4850188191125217)")
                    .execute(connection)
                    .await?;

                let job = crate::schema::restore_jobs::table
                    .filter(crate::schema::restore_jobs::id.eq(job_id))
                    .for_update()
                    .select(RestoreApplyRow::as_select())
                    .first::<RestoreApplyRow>(connection)
                    .await?;
                let (maintenance_state_value, maintenance_restore_job_id) =
                    crate::schema::system_maintenance::table
                        .filter(crate::schema::system_maintenance::id.eq(1_i16))
                        .select((
                            crate::schema::system_maintenance::state,
                            crate::schema::system_maintenance::restore_job_id,
                        ))
                        .first::<(String, Option<i64>)>(connection)
                        .await?;
                let maintenance_state = MaintenanceState::try_from(
                    maintenance_state_value.as_str(),
                )
                .map_err(|error| {
                    PostgresStorageError::database(format!(
                        "Invalid persisted maintenance state: {error}"
                    ))
                })?;
                if job.status != StorageRestoreJobStatus::Confirmed.as_str()
                    || maintenance_state != MaintenanceState::Draining
                    || maintenance_restore_job_id != Some(job.id)
                {
                    return Err(PostgresStorageError::conflict(format!(
                        "Restore stage {} is no longer confirmed and draining",
                        job.id
                    )));
                }

                let started_at = Utc::now().naive_utc();
                diesel::sql_query("UPDATE credential_approvals SET invalidated_at=timezone('UTC', clock_timestamp()) WHERE consumed_at IS NULL AND invalidated_at IS NULL").execute(connection).await?;
                enable_restore_session_settings(connection).await?;
                replace_backend_replay(connection, snapshot)
                    .await?;
                enqueue_restored_computed_rebuilds_on_connection(connection).await?;
                super::schema_evolution::enqueue_restored_schema_work_on(connection).await?;

                // Restored event rows must not fan out while they are inserted.
                // This is the one deliberate post-restore provenance event.
                diesel::sql_query("SELECT set_config('hubuum.restore_events', 'off', true)")
                    .execute(connection)
                    .await?;
                let approval = super::credential_approval::restore_evidence(connection, job.id).await?;
                let document = AuditDocument::try_new(
                    "System restore completed",
                    None,
                    None,
                    serde_json::json!({
                        "restore_job_id": job.id,
                        "backup_sha256": job.sha256,
                        "credential_approval": approval,
                        "backup_version": backup_version,
                        "backup_source_version": backup_source_version,
                        "backup_created_at": backup_created_at,
                        "includes_history": includes_history,
                        "initiated_by": {
                            "principal_id": job.requested_by,
                            "identity_scope": job.requested_by_identity_scope,
                            "name": job.requested_by_name,
                        },
                    }),
                )?;
                let provenance = NewEvent::from_document(
                    EntityType::Restore,
                    Action::Succeeded,
                    ActorKind::System,
                    document,
                )
                .map_err(|error| PostgresStorageError::internal(error.to_string()))?
                .with_entity_name(job.sha256.clone());
                append_event(connection, &provenance).await?;

                let finished_at = Utc::now().naive_utc();
                finish_restore(connection, job.id, finished_at).await?;
                crate::validate_persisted(
                    "restore completion",
                    StorageRestoreCompletion::try_new(started_at.and_utc(), finished_at.and_utc()),
                )
            },
        )
        .await
}

async fn enable_restore_session_settings(
    connection: &mut PostgresConnection,
) -> Result<(), PostgresStorageError> {
    for setting in [
        "hubuum.restore_history",
        "hubuum.restore_events",
        "hubuum.restore_revisions",
    ] {
        diesel::sql_query("SELECT set_config($1, 'on', true)")
            .bind::<diesel::sql_types::Text, _>(setting)
            .execute(connection)
            .await?;
    }
    Ok(())
}

async fn replace_backend_replay(
    connection: &mut PostgresConnection,
    snapshot: StorageBackupReplay,
) -> Result<(), PostgresStorageError> {
    for table in TRUNCATE_TABLES {
        validate_restore_identifier(table, None)?;
    }
    let lock_tables = TRUNCATE_TABLES.join(", ");
    diesel::sql_query(format!("LOCK TABLE {lock_tables} IN ACCESS EXCLUSIVE MODE"))
        .execute(connection)
        .await?;
    diesel::sql_query(format!(
        "TRUNCATE TABLE {lock_tables} RESTART IDENTITY CASCADE"
    ))
    .execute(connection)
    .await?;

    for section in StorageBackupStateSection::ALL.iter().copied() {
        let table = state_table(section);
        let rows = snapshot
            .state_rows(section)
            .map(|row| state_row_to_postgres(section, row?.into_value()));
        insert_restore_rows(connection, table, rows).await?;
    }
    if snapshot.includes_history() {
        for section in StorageBackupHistorySection::ALL.iter().copied() {
            let table = history_table(section);
            let rows = snapshot
                .history_rows(section)
                .map(|row| history_row_to_postgres(section, row?.into_value()));
            insert_restore_rows(connection, table, rows).await?;
        }
    }

    for table in SERIAL_ID_TABLES {
        reset_restore_sequence(connection, table, "id").await?;
    }
    reset_restore_sequence(connection, "events", "id").await?;
    reset_restore_sequence(connection, "event_deliveries", "id").await?;
    for table in HISTORY_SEQUENCE_TABLES {
        reset_restore_sequence(connection, table, "history_id").await?;
    }
    Ok(())
}

#[derive(QueryableByName)]
struct RestoreRowProjection {
    #[diesel(sql_type = Text)]
    columns: String,
    #[diesel(sql_type = Text)]
    values: String,
}

async fn insert_restore_rows(
    connection: &mut PostgresConnection,
    table: &str,
    rows: impl Iterator<Item = Result<Value, PostgresStorageError>> + Send,
) -> Result<(), PostgresStorageError> {
    validate_restore_identifier(table, None)?;
    let mut rows = rows.peekable();
    if rows.peek().is_none() {
        return Ok(());
    }
    // PostgreSQL's record conversion maps JSON null to SQL NULL, including
    // required JSONB columns such as object data. Read those values directly
    // from the document; retain record conversion for nullable columns where
    // backup null represents SQL NULL. Missing required values still fail the
    // database constraint. Catalog-derived identifiers are quoted by format().
    let projection = diesel::sql_query(
        "SELECT string_agg(format('%I', attname), ', ' ORDER BY attnum) AS columns, \
         string_agg(CASE WHEN atttypid = 'jsonb'::regtype AND attnotnull \
             THEN format('source.value -> %L', attname) \
             ELSE format('restored.%I', attname) END, ', ' ORDER BY attnum) AS values \
         FROM pg_attribute \
         WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped",
    )
    .bind::<Text, _>(table)
    .get_result::<RestoreRowProjection>(connection)
    .await?;
    let query = format!(
        "INSERT INTO {table} ({}) SELECT {} \
         FROM jsonb_array_elements($1::jsonb) AS source(value) \
         CROSS JOIN LATERAL jsonb_populate_record(NULL::{table}, source.value) AS restored",
        projection.columns, projection.values,
    );
    // Collections can reference parents later in their section. Their immediate
    // self foreign key requires one INSERT. All other section dependencies have
    // already been loaded, and all batches remain in the fenced transaction.
    let batch_sections = table != "collections";
    let mut batch = String::from("[");
    let mut row_count = 0;
    for row in rows {
        let encoded = serde_json::to_string(&row?)
            .map_err(|_| PostgresStorageError::database("Failed to serialize a restore row"))?;
        if batch_sections
            && row_count > 0
            && (row_count == RESTORE_BATCH_ROWS
                || batch.len().saturating_add(encoded.len()).saturating_add(2)
                    > RESTORE_BATCH_BYTES)
        {
            batch.push(']');
            diesel::sql_query(&query)
                .bind::<Text, _>(&batch)
                .execute(connection)
                .await?;
            batch.clear();
            batch.push('[');
            row_count = 0;
        }
        if row_count > 0 {
            batch.push(',');
        }
        batch.push_str(&encoded);
        row_count += 1;
    }
    if row_count > 0 {
        batch.push(']');
        diesel::sql_query(&query)
            .bind::<Text, _>(&batch)
            .execute(connection)
            .await?;
    }
    Ok(())
}

async fn reset_restore_sequence(
    connection: &mut PostgresConnection,
    table: &str,
    column: &str,
) -> Result<(), PostgresStorageError> {
    validate_restore_identifier(table, Some(column))?;
    // Deleted resources still own their identities while history retains them.
    // Reusing a class ID also merges unrelated immutable schema revisions.
    let retained = match (table, column) {
        ("collections", "id") => "SELECT id FROM collections_history",
        ("hubuumclass", "id") => {
            "SELECT id FROM hubuumclass_history UNION ALL \
             SELECT class_id FROM class_schema_history UNION ALL \
             SELECT class_id FROM computed_field_history"
        }
        ("hubuumobject", "id") => "SELECT id FROM hubuumobject_history",
        ("hubuumclass_relation", "id") => "SELECT id FROM hubuumclass_relation_history",
        ("hubuumobject_relation", "id") => "SELECT id FROM hubuumobject_relation_history",
        ("export_templates", "id") => "SELECT id FROM export_templates_history",
        ("remote_targets", "id") => "SELECT id FROM remote_targets_history",
        ("computed_field_definitions", "id") => {
            "SELECT (definition->>'id')::bigint FROM computed_field_history \
             CROSS JOIN LATERAL jsonb_array_elements(definitions) definition"
        }
        _ => "SELECT NULL::bigint WHERE false",
    };
    let query = format!(
        "SELECT setval(pg_get_serial_sequence('{table}', '{column}'), \
         COALESCE(MAX(id), 1), MAX(id) IS NOT NULL) FROM (\
         SELECT {column}::bigint AS id FROM {table} UNION ALL {retained}) retained"
    );
    diesel::sql_query(query).execute(connection).await?;
    Ok(())
}

fn validate_restore_identifier(
    table: &str,
    column: Option<&str>,
) -> Result<(), PostgresStorageError> {
    let known_table = StorageBackupStateSection::ALL
        .iter()
        .copied()
        .map(state_table)
        .chain(
            StorageBackupHistorySection::ALL
                .iter()
                .copied()
                .map(history_table),
        )
        .chain(TRUNCATE_TABLES.iter().copied())
        .any(|known| known == table);
    let known_column = column.is_none_or(|value| matches!(value, "id" | "history_id"));
    if known_table && known_column {
        Ok(())
    } else {
        Err(PostgresStorageError::internal(
            "Refused an unsafe restore SQL identifier",
        ))
    }
}

async fn finish_restore(
    connection: &mut PostgresConnection,
    job_id: i64,
    finished_at: NaiveDateTime,
) -> Result<(), PostgresStorageError> {
    // The shared updated-at trigger uses transaction-start time. Preserve the
    // explicit completion clock so the terminal receipt cannot appear to have
    // finished after its own updated_at value.
    preserve_explicit_timestamps(connection).await?;
    diesel::sql_query(
        "UPDATE system_maintenance \
         SET generation=0, state='normal', restore_job_id=NULL, \
             entered_at=NULL, updated_at=$1 \
         WHERE id=1",
    )
    .bind::<Timestamp, _>(finished_at)
    .execute(connection)
    .await?;
    diesel::sql_query("DELETE FROM restore_success_receipts")
        .execute(connection)
        .await?;
    let completed = diesel::sql_query(
        "INSERT INTO restore_success_receipts (\
             id, requested_by, requested_by_identity_scope, requested_by_name, \
             byte_size, sha256, capability_hash, validation_summary, expires_at, \
             confirmed_at, finished_at, created_at, updated_at\
         ) \
         SELECT id, requested_by, requested_by_identity_scope, requested_by_name, \
                byte_size, sha256, capability_hash, validation_summary, expires_at, \
                confirmed_at, $2, created_at, $2 \
         FROM restore_jobs \
         WHERE id=$1 AND status='confirmed' AND confirmed_at IS NOT NULL",
    )
    .bind::<BigInt, _>(job_id)
    .bind::<Timestamp, _>(finished_at)
    .execute(connection)
    .await?;
    if completed != 1 {
        return Err(PostgresStorageError::conflict(
            "Confirmed restore receipt changed concurrently",
        ));
    }
    diesel::sql_query("DELETE FROM restore_jobs")
        .execute(connection)
        .await?;
    diesel::sql_query("DELETE FROM server_instances")
        .execute(connection)
        .await?;
    diesel::sql_query("SELECT pg_notify('hubuum_maintenance', 'normal')")
        .execute(connection)
        .await?;
    Ok(())
}

async fn preserve_explicit_timestamps(
    connection: &mut PostgresConnection,
) -> Result<(), PostgresStorageError> {
    diesel::sql_query("SELECT set_config('hubuum.preserve_imported_timestamps', 'on', true)")
        .execute(connection)
        .await?;
    Ok(())
}

/// Mark a restore failed and atomically return to normal operation.
pub async fn fail_restore_and_resume(
    runtime: &PostgresRuntime,
    request: StorageRestoreFailure,
) -> Result<(), PostgresStorageError> {
    let (job_id, public_error) = request.into_parts();
    let job_id = job_id.id();
    runtime
        .with_transaction(async |connection| -> Result<_, PostgresStorageError> {
            diesel::sql_query(
                "UPDATE restore_jobs \
                 SET status='failed', error=$2, finished_at=now(), document=''::bytea \
                 WHERE id=$1 AND status IN ('validated', 'confirmed')",
            )
            .bind::<diesel::sql_types::BigInt, _>(job_id)
            .bind::<diesel::sql_types::Text, _>(public_error)
            .execute(connection)
            .await?;
            diesel::sql_query(
                "UPDATE system_maintenance \
                 SET state='normal', restore_job_id=NULL, entered_at=NULL, updated_at=now() \
                 WHERE id=1 AND restore_job_id=$1 AND state='draining'",
            )
            .bind::<diesel::sql_types::BigInt, _>(job_id)
            .execute(connection)
            .await?;
            diesel::sql_query("SELECT pg_notify('hubuum_maintenance', 'normal')")
                .execute(connection)
                .await?;
            Ok(())
        })
        .await
}

/// Read maintenance ownership and backend time from one snapshot.
pub async fn get_restore_coordinator_snapshot(
    runtime: &PostgresRuntime,
) -> Result<StorageRestoreCoordinatorSnapshot, PostgresStorageError> {
    runtime
        .with_connection(async |connection| -> Result<_, PostgresStorageError> {
            let (state, restore_job_id, backend_now) = crate::schema::system_maintenance::table
                .filter(crate::schema::system_maintenance::id.eq(1_i16))
                .select((
                    crate::schema::system_maintenance::state,
                    crate::schema::system_maintenance::restore_job_id,
                    sql::<Timestamp>(DATABASE_UTC_NOW_SQL),
                ))
                .first::<(String, Option<i64>, NaiveDateTime)>(connection)
                .await?;
            let maintenance_state =
                MaintenanceState::try_from(state.as_str()).map_err(|error| {
                    PostgresStorageError::database(format!(
                        "Invalid persisted maintenance state: {error}"
                    ))
                })?;
            Ok(StorageRestoreCoordinatorSnapshot::new(
                maintenance_state,
                restore_job_id.map(RestoreJobId::new).transpose()?,
                backend_now.and_utc(),
            ))
        })
        .await
}

/// Recover draining maintenance without an owning restore.
pub async fn resume_maintenance_without_restore(
    runtime: &PostgresRuntime,
) -> Result<(), PostgresStorageError> {
    runtime
        .with_transaction(async |connection| -> Result<_, PostgresStorageError> {
            diesel::sql_query(
                "UPDATE system_maintenance \
                 SET state='normal', entered_at=NULL, updated_at=now() \
                 WHERE id=1 AND restore_job_id IS NULL AND state='draining'",
            )
            .execute(connection)
            .await?;
            diesel::sql_query("SELECT pg_notify('hubuum_maintenance', 'normal')")
                .execute(connection)
                .await?;
            Ok(())
        })
        .await
}

/// Recover draining maintenance owned by a terminal restore.
pub async fn resume_terminal_restore(
    runtime: &PostgresRuntime,
    job_id: i64,
) -> Result<(), PostgresStorageError> {
    runtime
        .with_transaction(async |connection| -> Result<_, PostgresStorageError> {
            diesel::sql_query(
                "UPDATE system_maintenance \
                 SET state='normal', restore_job_id=NULL, entered_at=NULL, updated_at=now() \
                 WHERE id=1 AND restore_job_id=$1 AND state='draining'",
            )
            .bind::<diesel::sql_types::BigInt, _>(job_id)
            .execute(connection)
            .await?;
            diesel::sql_query("SELECT pg_notify('hubuum_maintenance', 'normal')")
                .execute(connection)
                .await?;
            Ok(())
        })
        .await
}

/// Publish one coordinator heartbeat and return the observed state.
pub async fn tick_restore_coordinator(
    runtime: &PostgresRuntime,
    instance_id: Uuid,
    local_work_is_idle: &(dyn Fn() -> bool + Send + Sync),
    expire_validated_jobs: bool,
) -> Result<StorageRestoreCoordinatorSnapshot, PostgresStorageError> {
    with_storage_call_site(StorageCallSite::RestoreCoordinator, async {
        runtime
            .with_transaction(async |connection| -> Result<_, PostgresStorageError> {
                if expire_validated_jobs {
                    diesel::sql_query(
                        "UPDATE restore_jobs \
                         SET status='expired', document=''::bytea \
                         WHERE status='validated' AND expires_at <= now()",
                    )
                    .execute(connection)
                    .await?;
                }
                let (generation, state, restore_job_id, backend_now) =
                    crate::schema::system_maintenance::table
                        .filter(crate::schema::system_maintenance::id.eq(1_i16))
                        .select((
                            crate::schema::system_maintenance::generation,
                            crate::schema::system_maintenance::state,
                            crate::schema::system_maintenance::restore_job_id,
                            sql::<Timestamp>(DATABASE_UTC_NOW_SQL),
                        ))
                        .first::<(i64, String, Option<i64>, NaiveDateTime)>(connection)
                        .await?;
                let maintenance_state =
                    MaintenanceState::try_from(state.as_str()).map_err(|error| {
                        PostgresStorageError::database(format!(
                            "Invalid persisted maintenance state: {error}"
                        ))
                    })?;
                let drained = !maintenance_state.is_normal() && local_work_is_idle();
                let record = ServerInstanceRow {
                    instance_id,
                    maintenance_generation: generation,
                    drained,
                    last_heartbeat_at: backend_now,
                    started_at: backend_now,
                };
                diesel::insert_into(crate::schema::server_instances::table)
                    .values(&record)
                    .on_conflict(crate::schema::server_instances::instance_id)
                    .do_update()
                    .set((
                        crate::schema::server_instances::maintenance_generation
                            .eq(record.maintenance_generation),
                        crate::schema::server_instances::drained.eq(record.drained),
                        crate::schema::server_instances::last_heartbeat_at
                            .eq(record.last_heartbeat_at),
                    ))
                    .execute(connection)
                    .await?;
                crate::reach_fault_point(
                    crate::PostgresFaultPoint::RestoreCoordinatorAfterHeartbeat,
                    Some(connection),
                )
                .await?;
                Ok(StorageRestoreCoordinatorSnapshot::new(
                    maintenance_state,
                    restore_job_id.map(RestoreJobId::new).transpose()?,
                    backend_now.and_utc(),
                ))
            })
            .await
    })
    .await
}

/// Return the current generation and live coordinator instances.
pub async fn get_restore_drain_state(
    runtime: &PostgresRuntime,
    heartbeat_cutoff: DateTime<Utc>,
) -> Result<StorageRestoreDrainState, PostgresStorageError> {
    let heartbeat_cutoff = heartbeat_cutoff.naive_utc();
    let (generation, instances) = runtime
        .with_connection(async |connection| {
            let generation = crate::schema::system_maintenance::table
                .filter(crate::schema::system_maintenance::id.eq(1_i16))
                .select(crate::schema::system_maintenance::generation)
                .first::<i64>(connection)
                .await?;
            let instances = crate::schema::server_instances::table
                .filter(crate::schema::server_instances::last_heartbeat_at.gt(heartbeat_cutoff))
                .load::<ServerInstanceRow>(connection)
                .await?;
            Ok::<_, diesel::result::Error>((generation, instances))
        })
        .await?;
    let instances = instances
        .into_iter()
        .map(instance_to_storage)
        .collect::<Result<Vec<_>, _>>()?;
    crate::validate_persisted(
        "restore drain state",
        StorageRestoreDrainState::try_new(generation, instances),
    )
}

/// Remove one process from restore coordinator membership.
pub async fn remove_restore_instance(
    runtime: &PostgresRuntime,
    instance_id: Uuid,
) -> Result<(), PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            diesel::delete(
                crate::schema::server_instances::table
                    .filter(crate::schema::server_instances::instance_id.eq(instance_id)),
            )
            .execute(connection)
            .await
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use diesel::SelectableHelper;
    use diesel::prelude::{ExpressionMethods, QueryDsl};
    use rstest::rstest;

    use super::{RestoreJobStatusRow, RestoreSuccessReceiptRow, validate_restore_identifier};

    #[test]
    fn restore_status_projection_excludes_document() {
        let query = crate::schema::restore_jobs::table
            .filter(crate::schema::restore_jobs::id.eq(42_i64))
            .select(RestoreJobStatusRow::as_select());
        let sql = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();

        assert!(!sql.contains("\"restore_jobs\".\"document\""));
        assert!(sql.contains("\"restore_jobs\".\"capability_hash\""));
    }

    #[test]
    fn successful_restore_projection_is_document_free() {
        let query = crate::schema::restore_success_receipts::table
            .filter(crate::schema::restore_success_receipts::id.eq(42_i64))
            .select(RestoreSuccessReceiptRow::as_select());
        let sql = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();

        assert!(!sql.contains("document"));
        assert!(sql.contains("\"restore_success_receipts\".\"capability_hash\""));
    }

    #[rstest]
    #[case::known("collections", Some("id"), true)]
    #[case::unknown_table("collections; DROP TABLE users", None, false)]
    #[case::unknown_column("collections", Some("id DESC"), false)]
    fn restore_sql_identifiers_come_from_closed_lists(
        #[case] table: &str,
        #[case] column: Option<&str>,
        #[case] expected_valid: bool,
    ) {
        assert_eq!(
            validate_restore_identifier(table, column).is_ok(),
            expected_valid
        );
    }

    #[cfg(feature = "integration-test-support")]
    mod database {
        use diesel::QueryableByName;
        use diesel::sql_types::{BigInt, Jsonb, Nullable};
        use diesel_async::RunQueryDsl;
        use rstest::rstest;
        use serde_json::{Value, json};

        use super::super::{insert_restore_rows, reset_restore_sequence};
        use crate::test_support::integration_test_pool;
        use crate::{PostgresStorageError, with_transaction};

        #[derive(QueryableByName)]
        struct RestoredJsonRow {
            #[diesel(sql_type = Jsonb)]
            data: Value,
            #[diesel(sql_type = Nullable<Jsonb>)]
            optional_data: Option<Value>,
        }

        #[derive(QueryableByName)]
        struct AllocatedId {
            #[diesel(sql_type = BigInt)]
            id: i64,
        }

        #[derive(QueryableByName)]
        struct BatchSizes {
            #[diesel(sql_type = Jsonb)]
            sizes: Value,
        }

        #[rstest]
        #[case::empty(0, 0, json!([]))]
        #[case::row_boundary(257, 0, json!([256, 1]))]
        #[case::byte_boundary(3, 600_000, json!([1, 1, 1]))]
        #[case::large_row(2, 1_100_000, json!([1, 1]))]
        #[tokio::test]
        async fn restore_insert_bounds_each_batch(
            #[case] count: usize,
            #[case] payload_bytes: usize,
            #[case] expected: Value,
        ) {
            let pool = integration_test_pool(1);
            let sizes = with_transaction(&pool, async |connection| {
                for query in [
                    "CREATE TEMP TABLE hubuumobject (id BIGINT, data JSONB NOT NULL) ON COMMIT DROP",
                    "CREATE TEMP TABLE restore_batches (ordinal BIGINT GENERATED ALWAYS AS IDENTITY, n BIGINT) ON COMMIT DROP",
                    "CREATE FUNCTION pg_temp.record_restore_batch() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN INSERT INTO restore_batches(n) SELECT count(*) FROM inserted; RETURN NULL; END $$",
                    "CREATE TRIGGER record_batch AFTER INSERT ON hubuumobject REFERENCING NEW TABLE AS inserted FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.record_restore_batch()",
                ] {
                    diesel::sql_query(query).execute(connection).await?;
                }
                let rows = (0..count).map(|id| Ok(json!({"id": id, "data": "x".repeat(payload_bytes)})));
                insert_restore_rows(connection, "hubuumobject", rows).await?;
                diesel::sql_query("SELECT COALESCE(jsonb_agg(n ORDER BY ordinal), '[]'::jsonb) AS sizes FROM restore_batches")
                    .get_result::<BatchSizes>(connection).await.map_err(PostgresStorageError::from)
            }).await.unwrap();
            assert_eq!(sizes.sizes, expected);
        }

        #[tokio::test]
        async fn restore_collections_preserves_forward_parent_references() {
            let pool = integration_test_pool(1);
            let count = with_transaction(&pool, async |connection| {
                diesel::sql_query("CREATE TEMP TABLE collections (id BIGINT PRIMARY KEY, parent_collection_id BIGINT REFERENCES collections(id)) ON COMMIT DROP")
                    .execute(connection).await?;
                // The parent is beyond the ordinary row-batch boundary.
                let rows = (1..=257).map(|id| Ok(json!({
                    "id": id,
                    "parent_collection_id": (id < 257).then_some(257)
                })));
                insert_restore_rows(connection, "collections", rows).await?;
                diesel::sql_query("SELECT count(*) AS id FROM collections")
                    .get_result::<AllocatedId>(connection).await.map_err(PostgresStorageError::from)
            }).await.unwrap();
            assert_eq!(count.id, 257);
        }

        #[tokio::test]
        async fn later_batch_failure_rolls_back_earlier_batches() {
            let pool = integration_test_pool(1);
            with_transaction(&pool, async |connection| {
                diesel::sql_query("CREATE TEMP TABLE hubuumobject (id BIGINT PRIMARY KEY)")
                    .execute(connection)
                    .await?;
                diesel::sql_query("INSERT INTO hubuumobject VALUES (-1)")
                    .execute(connection)
                    .await?;
                Ok::<_, PostgresStorageError>(())
            })
            .await
            .unwrap();
            let result = with_transaction(&pool, async |connection| {
                // Row 257 violates a key inserted by the first batch.
                let rows = (0..257).map(|id| Ok(json!({"id": id % 256})));
                insert_restore_rows(connection, "hubuumobject", rows).await
            })
            .await;
            let rows = with_transaction(&pool, async |connection| {
                let rows = diesel::sql_query("SELECT id FROM hubuumobject")
                    .load::<AllocatedId>(connection)
                    .await?;
                diesel::sql_query("DROP TABLE pg_temp.hubuumobject")
                    .execute(connection)
                    .await?;
                Ok::<_, PostgresStorageError>(rows)
            })
            .await
            .unwrap();
            assert!(result.is_err());
            assert_eq!(rows.into_iter().map(|row| row.id).collect::<Vec<_>>(), [-1]);
        }

        #[rstest]
        #[case("collections", "collections_history", "id")]
        #[case("hubuumclass", "hubuumclass_history", "id")]
        #[case("hubuumclass", "class_schema_history", "class_id")]
        #[case("hubuumclass", "computed_field_history", "class_id")]
        #[case("hubuumobject", "hubuumobject_history", "id")]
        #[case("hubuumclass_relation", "hubuumclass_relation_history", "id")]
        #[case("hubuumobject_relation", "hubuumobject_relation_history", "id")]
        #[case("export_templates", "export_templates_history", "id")]
        #[case("remote_targets", "remote_targets_history", "id")]
        #[tokio::test]
        async fn restored_sequence_preserves_deleted_resource_identities(
            #[case] table: &str,
            #[case] history: &str,
            #[case] identity: &str,
        ) {
            let pool = integration_test_pool(1);
            let allocated = with_transaction(&pool, async |connection| {
                diesel::sql_query(format!(
                    "CREATE TEMP TABLE {table} (id BIGINT GENERATED BY DEFAULT AS IDENTITY) ON COMMIT DROP"
                )).execute(connection).await?;
                let history_tables = if table == "hubuumclass" {
                    vec![("hubuumclass_history", "id"), ("class_schema_history", "class_id"), ("computed_field_history", "class_id")]
                } else {
                    vec![(history, identity)]
                };
                for (history_table, column) in history_tables {
                    diesel::sql_query(format!(
                        "CREATE TEMP TABLE {history_table} ({column} BIGINT) ON COMMIT DROP"
                    )).execute(connection).await?;
                }
                diesel::sql_query(format!("INSERT INTO {table} (id) VALUES (7)"))
                    .execute(connection).await?;
                diesel::sql_query(format!("INSERT INTO {history} ({identity}) VALUES (42)"))
                    .execute(connection).await?;
                reset_restore_sequence(connection, table, "id").await?;
                diesel::sql_query(format!("INSERT INTO {table} DEFAULT VALUES RETURNING id"))
                    .get_result::<AllocatedId>(connection).await.map_err(PostgresStorageError::from)
            }).await.unwrap();
            assert_eq!(allocated.id, 43);
        }

        async fn restore_json_row(row: Value) -> Result<RestoredJsonRow, PostgresStorageError> {
            let pool = integration_test_pool(1);
            with_transaction(&pool, async |connection| {
                // A connection-local shadow table exercises the production
                // insert without replacing shared integration fixtures.
                diesel::sql_query(
                    "CREATE TEMP TABLE hubuumobject (data JSONB NOT NULL, optional_data JSONB) \
                     ON COMMIT DROP",
                )
                .execute(connection)
                .await?;
                insert_restore_rows(connection, "hubuumobject", std::iter::once(Ok(row))).await?;
                Ok::<_, PostgresStorageError>(
                    diesel::sql_query("SELECT data, optional_data FROM hubuumobject")
                        .get_result(connection)
                        .await?,
                )
            })
            .await
        }

        #[rstest]
        #[case::null(json!(null))]
        #[case::object(json!({"nested": null}))]
        #[case::array(json!([null, 1, "value"]))]
        #[case::number(json!(42))]
        #[case::boolean(json!(false))]
        #[case::string(json!("null"))]
        #[tokio::test]
        async fn restore_preserves_required_json_values(#[case] data: Value) {
            let restored = restore_json_row(json!({"data": data})).await.unwrap();

            assert_eq!(restored.data, data);
        }

        #[rstest]
        #[case::sql_null(json!(null), None)]
        #[case::object(json!({"nested": null}), Some(json!({"nested": null})))]
        #[tokio::test]
        async fn restore_preserves_nullable_json_values(
            #[case] value: Value,
            #[case] expected: Option<Value>,
        ) {
            let restored = restore_json_row(json!({"data": {}, "optional_data": value}))
                .await
                .unwrap();

            assert_eq!(restored.optional_data, expected);
        }

        #[tokio::test]
        async fn restore_rejects_missing_required_json_value() {
            assert!(restore_json_row(json!({})).await.is_err());
        }
    }
}
