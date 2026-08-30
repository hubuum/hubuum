//! PostgreSQL-owned restore staging and coordinator lifecycle.

use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::NullableExpressionMethods;
use diesel::dsl::sql;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::sql_types::{Jsonb, Timestamp};
use diesel::{Insertable, Queryable, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{MaintenanceState, PrincipalId, RestoreJobId};
use hubuum_events_core::{Action, ActorKind, AuditDocument, EntityType, NewEvent};
use hubuum_storage_core::{
    StorageBackupHistorySection, StorageBackupHistorySections, StorageBackupRow,
    StorageBackupStateSection, StorageBackupStateSections, StorageCallSite, StorageRestoreApply,
    StorageRestoreArtifactSummary, StorageRestoreCompletion, StorageRestoreCoordinatorSnapshot,
    StorageRestoreDrainState, StorageRestoreFailure, StorageRestoreInitiator,
    StorageRestoreInstance, StorageRestoreJob, StorageRestoreJobStatus, StorageRestoreJobSummary,
    StorageRestoreStageCreate, StorageRestoreStatus, StorageRestoreTimestamps,
};
use serde_json::Value;
use uuid::Uuid;

use crate::operations::backup::{
    history_row_to_postgres, history_table, state_row_to_postgres, state_table,
};
use crate::operations::computed_fields::enqueue_restored_computed_rebuilds_on_connection;
use crate::operations::event_record::append_event;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError, with_storage_call_site};

const DATABASE_UTC_NOW_SQL: &str = "clock_timestamp() AT TIME ZONE 'UTC'";

const TRUNCATE_TABLES: &[&str] = &[
    "object_computed_data",
    "class_computation_state",
    "computed_field_definitions",
    "event_deliveries",
    "events",
    "backup_task_outputs",
    "export_task_outputs",
    "remote_call_results",
    "import_task_results",
    "tasks",
    "token_scopes",
    "tokens",
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
    "identity_scopes",
    "groups",
    "principals",
    "collections",
    "permissions",
    "hubuumclass",
    "computed_field_definitions",
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
    "collections_history",
    "hubuumclass_history",
    "hubuumclass_relation_history",
    "hubuumobject_history",
    "hubuumobject_relation_history",
    "export_templates_history",
    "remote_targets_history",
];

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::restore_jobs)]
struct RestoreJobRow {
    id: i64,
    status: String,
    requested_by: Option<i32>,
    requested_by_identity_scope: String,
    requested_by_name: String,
    document: Vec<u8>,
    byte_size: i64,
    sha256: String,
    capability_hash: String,
    error: Option<String>,
    expires_at: NaiveDateTime,
    confirmed_at: Option<NaiveDateTime>,
    finished_at: Option<NaiveDateTime>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

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

fn job_to_storage(row: RestoreJobRow) -> Result<StorageRestoreJob, PostgresStorageError> {
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
        "restore job",
        StorageRestoreJob::try_new(summary, row.document, row.capability_hash),
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

/// Stage one validated restore artifact.
pub async fn stage_restore(
    runtime: &PostgresRuntime,
    request: StorageRestoreStageCreate,
) -> Result<StorageRestoreJob, PostgresStorageError> {
    let (initiator, document, artifact, capability_hash, validation_summary, expires_at) =
        request.into_parts();
    let initiator = initiator.into_parts();
    let requested_by = initiator.principal_id();
    let requested_by_identity_scope = initiator.identity_scope().to_owned();
    let requested_by_name = initiator.name().to_owned();
    let artifact = artifact.into_parts();
    let byte_size = artifact.byte_size();
    let sha256 = artifact.sha256().to_owned();
    let input = NewRestoreJobRow {
        status: StorageRestoreJobStatus::Validated.as_str().to_string(),
        requested_by: requested_by.map(|principal_id| principal_id.id()),
        requested_by_identity_scope,
        requested_by_name,
        document,
        byte_size,
        sha256,
        capability_hash,
        validation_summary,
        expires_at: expires_at.naive_utc(),
    };
    runtime
        .with_connection(async |connection| {
            diesel::insert_into(crate::schema::restore_jobs::table)
                .values(input)
                .returning(RestoreJobRow::as_returning())
                .get_result::<RestoreJobRow>(connection)
                .await
        })
        .await
        .and_then(job_to_storage)
}

/// Load a complete staged restore artifact.
pub async fn get_restore_job(
    runtime: &PostgresRuntime,
    job_id: i64,
) -> Result<StorageRestoreJob, PostgresStorageError> {
    let row = runtime
        .with_connection(async |connection| {
            crate::schema::restore_jobs::table
                .filter(crate::schema::restore_jobs::id.eq(job_id))
                .select(RestoreJobRow::as_select())
                .first::<RestoreJobRow>(connection)
                .await
                .optional()
        })
        .await?
        .ok_or_else(|| {
            PostgresStorageError::not_found(format!("Restore stage {job_id} was not found"))
        })?;
    job_to_storage(row)
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
        .await?
        .ok_or_else(|| {
            PostgresStorageError::not_found(format!("Restore stage {job_id} was not found"))
        })?;
    status_to_storage(row)
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
    job_id: i64,
) -> Result<DateTime<Utc>, PostgresStorageError> {
    runtime
        .with_transaction(async |connection| -> Result<_, PostgresStorageError> {
            diesel::sql_query("SELECT pg_advisory_xact_lock(4850188191125217)")
                .execute(connection)
                .await?;
            let confirmation_time = diesel::update(
                crate::schema::restore_jobs::table
                    .filter(crate::schema::restore_jobs::id.eq(job_id))
                    .filter(
                        crate::schema::restore_jobs::status
                            .eq(StorageRestoreJobStatus::Validated.as_str()),
                    ),
            )
            .set((
                crate::schema::restore_jobs::status.eq(StorageRestoreJobStatus::Confirmed.as_str()),
                crate::schema::restore_jobs::confirmed_at
                    .eq(sql::<Timestamp>(DATABASE_UTC_NOW_SQL).nullable()),
                crate::schema::restore_jobs::error.eq::<Option<String>>(None),
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
pub async fn apply_restore(
    runtime: &PostgresRuntime,
    request: StorageRestoreApply,
) -> Result<StorageRestoreCompletion, PostgresStorageError> {
    let (job_id, document) = request.into_parts();
    let job_id = job_id.id();
    let (metadata, snapshot) = document.into_parts();
    let (backup_version, backup_created_at, backup_source_version) = metadata.into_parts();
    let (state_sections, history_sections) = snapshot.into_parts();
    let includes_history = history_sections.is_some();

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
                enable_restore_session_settings(connection).await?;
                replace_backend_state(connection, &state_sections, history_sections.as_ref())
                    .await?;
                enqueue_restored_computed_rebuilds_on_connection(connection).await?;

                // Restored event rows must not fan out while they are inserted.
                // This is the one deliberate post-restore provenance event.
                diesel::sql_query("SELECT set_config('hubuum.restore_events', 'off', true)")
                    .execute(connection)
                    .await?;
                let document = AuditDocument::try_new(
                    "System restore completed",
                    None,
                    None,
                    serde_json::json!({
                        "restore_job_id": job.id,
                        "backup_sha256": job.sha256,
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
                finish_restore(connection, finished_at).await?;
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

async fn replace_backend_state(
    connection: &mut PostgresConnection,
    state_sections: &StorageBackupStateSections,
    history_sections: Option<&StorageBackupHistorySections>,
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
        let rows = state_sections
            .get(&section)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let rows = rows
            .iter()
            .cloned()
            .map(StorageBackupRow::into_value)
            .map(|row| state_row_to_postgres(section, row))
            .collect::<Result<Vec<_>, _>>()?;
        insert_restore_rows(connection, table, rows).await?;
    }
    if let Some(history_sections) = history_sections {
        for section in StorageBackupHistorySection::ALL.iter().copied() {
            let table = history_table(section);
            let rows = history_sections
                .get(&section)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let rows = rows
                .iter()
                .cloned()
                .map(StorageBackupRow::into_value)
                .map(|row| history_row_to_postgres(section, row))
                .collect::<Result<Vec<_>, _>>()?;
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

async fn insert_restore_rows(
    connection: &mut PostgresConnection,
    table: &str,
    rows: Vec<Value>,
) -> Result<(), PostgresStorageError> {
    validate_restore_identifier(table, None)?;
    if rows.is_empty() {
        return Ok(());
    }
    let query = format!(
        "INSERT INTO {table} SELECT * FROM jsonb_populate_recordset(NULL::{table}, $1::jsonb)"
    );
    diesel::sql_query(query)
        .bind::<Jsonb, _>(Value::Array(rows))
        .execute(connection)
        .await?;
    Ok(())
}

async fn reset_restore_sequence(
    connection: &mut PostgresConnection,
    table: &str,
    column: &str,
) -> Result<(), PostgresStorageError> {
    validate_restore_identifier(table, Some(column))?;
    let query = format!(
        "SELECT setval(pg_get_serial_sequence('{table}', '{column}'), \
         COALESCE((SELECT MAX({column}) FROM {table}), 1), \
         (SELECT MAX({column}) IS NOT NULL FROM {table}))"
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
    finished_at: NaiveDateTime,
) -> Result<(), PostgresStorageError> {
    diesel::sql_query(
        "UPDATE system_maintenance \
         SET generation=0, state='normal', restore_job_id=NULL, \
             entered_at=NULL, updated_at=$1 \
         WHERE id=1",
    )
    .bind::<Timestamp, _>(finished_at)
    .execute(connection)
    .await?;
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

    use super::{RestoreJobStatusRow, validate_restore_identifier};

    #[test]
    fn restore_status_projection_excludes_document() {
        let query = crate::schema::restore_jobs::table
            .filter(crate::schema::restore_jobs::id.eq(42_i64))
            .select(RestoreJobStatusRow::as_select());
        let sql = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();

        assert!(!sql.contains("\"restore_jobs\".\"document\""));
        assert!(sql.contains("\"restore_jobs\".\"capability_hash\""));
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
}
