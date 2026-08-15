//! PostgreSQL-owned restore staging and coordinator lifecycle.

use chrono::NaiveDateTime;
use diesel::NullableExpressionMethods;
use diesel::dsl::sql;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::sql_types::Timestamp;
use diesel::{Insertable, Queryable, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::MaintenanceState;
use hubuum_storage_core::{
    StorageCallSite, StorageRestoreArtifactSummary, StorageRestoreCoordinatorSnapshot,
    StorageRestoreDrainState, StorageRestoreFailure, StorageRestoreInitiator,
    StorageRestoreInstance, StorageRestoreJob, StorageRestoreJobStatus, StorageRestoreJobSummary,
    StorageRestoreStageCreate, StorageRestoreStatus, StorageRestoreTimestamps,
};
use uuid::Uuid;

use crate::{PostgresRuntime, PostgresStorageError, with_storage_call_site};

const DATABASE_UTC_NOW_SQL: &str = "clock_timestamp() AT TIME ZONE 'UTC'";

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
    Ok(StorageRestoreJobSummary::new(
        parts.id,
        StorageRestoreJobStatus::from_stored(&parts.status)?,
        StorageRestoreInitiator::new(
            parts.requested_by,
            parts.requested_by_identity_scope,
            parts.requested_by_name,
        ),
        StorageRestoreArtifactSummary::new(parts.byte_size, parts.sha256),
        parts.error,
        StorageRestoreTimestamps::new(
            parts.expires_at,
            parts.confirmed_at,
            parts.finished_at,
            parts.created_at,
            parts.updated_at,
        ),
    ))
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
    Ok(StorageRestoreJob::new(
        summary,
        row.document,
        row.capability_hash,
    ))
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
    Ok(StorageRestoreStatus::new(
        summary,
        row.capability_hash,
        row.validation_summary,
    ))
}

fn instance_to_storage(instance: ServerInstanceRow) -> StorageRestoreInstance {
    StorageRestoreInstance::new(
        instance.instance_id,
        instance.maintenance_generation,
        instance.drained,
    )
}

/// Stage one validated restore artifact.
pub async fn stage_restore(
    runtime: &PostgresRuntime,
    request: StorageRestoreStageCreate,
) -> Result<StorageRestoreJob, PostgresStorageError> {
    let (initiator, document, artifact, capability_hash, validation_summary, expires_at) =
        request.into_parts();
    let (requested_by, requested_by_identity_scope, requested_by_name) = initiator.into_parts();
    let (byte_size, sha256) = artifact.into_parts();
    let input = NewRestoreJobRow {
        status: StorageRestoreJobStatus::Validated.as_str().to_string(),
        requested_by,
        requested_by_identity_scope,
        requested_by_name,
        document,
        byte_size,
        sha256,
        capability_hash,
        validation_summary,
        expires_at,
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
) -> Result<NaiveDateTime, PostgresStorageError> {
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
            Ok(confirmation_time)
        })
        .await
}

/// Mark a restore failed and atomically return to normal operation.
pub async fn fail_restore_and_resume(
    runtime: &PostgresRuntime,
    request: StorageRestoreFailure,
) -> Result<(), PostgresStorageError> {
    let (job_id, public_error) = request.into_parts();
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
pub async fn restore_coordinator_snapshot(
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
                restore_job_id,
                backend_now,
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
                Ok(StorageRestoreCoordinatorSnapshot::new(
                    maintenance_state,
                    restore_job_id,
                    backend_now,
                ))
            })
            .await
    })
    .await
}

/// Return the current generation and live coordinator instances.
pub async fn restore_drain_state(
    runtime: &PostgresRuntime,
    heartbeat_cutoff: NaiveDateTime,
) -> Result<StorageRestoreDrainState, PostgresStorageError> {
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
    Ok(StorageRestoreDrainState::new(
        generation,
        instances.into_iter().map(instance_to_storage).collect(),
    ))
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

    use super::RestoreJobStatusRow;

    #[test]
    fn restore_status_projection_excludes_document() {
        let query = crate::schema::restore_jobs::table
            .filter(crate::schema::restore_jobs::id.eq(42_i64))
            .select(RestoreJobStatusRow::as_select());
        let sql = diesel::debug_query::<diesel::pg::Pg, _>(&query).to_string();

        assert!(!sql.contains("\"restore_jobs\".\"document\""));
        assert!(sql.contains("\"restore_jobs\".\"capability_hash\""));
    }
}
