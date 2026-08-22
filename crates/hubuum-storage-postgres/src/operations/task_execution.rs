//! PostgreSQL-owned task worker state machine.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::NaiveDateTime;
use diesel::dsl::sql;
use diesel::prelude::{BoolExpressionMethods, ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::sql_types::{Array, BigInt, Integer, Nullable, Text, Timestamp};
use diesel::{Insertable, QueryableByName, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{PrincipalId, TaskId};
use hubuum_events_core::{Action, EntityType, MutationProvenance, NewEvent};
use hubuum_storage_core::{
    StorageBackupTaskArtifact, StorageExportTaskArtifact, StorageRemoteCallTaskArtifact,
    StorageTask, StorageTaskClaim, StorageTaskClaimToken, StorageTaskCompletion,
    StorageTaskCompletionArtifact, StorageTaskEventAppend, StorageTaskEventInput,
    StorageTaskFailure, StorageTaskKind, StorageTaskLease, StorageTaskLeaseDuration,
    StorageTaskResultCounts, StorageTaskStateUpdate, StorageTaskStatus,
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::operations::event_record::append_event;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

use super::task_rows::TaskRow;

const DATABASE_UTC_NOW_SQL: &str = "clock_timestamp() AT TIME ZONE 'UTC'";
const DATABASE_UTC_NOW_QUERY: &str = "SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now";
const DATABASE_UTC_LEASE_EXPIRY_SQL_PREFIX: &str = "((clock_timestamp() AT TIME ZONE 'UTC') + (";
const DATABASE_LEASE_EXPIRY_SQL_SUFFIX: &str = " * INTERVAL '1 millisecond'))";
const LEASE_EXPIRED_MESSAGE: &str =
    "Task worker lease expired; task failed without automatic replay";

const CLAIM_NEXT_QUEUED_TASK_SQL: &str = "\
    SELECT candidate.id \
    FROM unnest($2::text[]) WITH ORDINALITY AS claim_order(kind, priority) \
    CROSS JOIN LATERAL ( \
        SELECT id \
        FROM tasks \
        WHERE status = $1 \
          AND tasks.kind = claim_order.kind \
        ORDER BY created_at ASC \
        FOR UPDATE SKIP LOCKED \
        LIMIT 1 \
    ) AS candidate \
    ORDER BY claim_order.priority \
    LIMIT 1";

static NEXT_TASK_KIND: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy)]
pub(super) struct ClaimedTask {
    pub(super) id: i32,
    pub(super) token: Uuid,
}

#[derive(Clone)]
struct TaskStateUpdate {
    status: StorageTaskStatus,
    summary: Option<String>,
    counts: StorageTaskResultCounts,
    started_at: Option<NaiveDateTime>,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::export_task_outputs)]
struct NewExportOutputRow {
    task_id: i32,
    template_name: Option<String>,
    content_type: String,
    json_output: Option<Value>,
    text_output: Option<String>,
    meta_json: Value,
    warnings_json: Value,
    warning_count: i32,
    truncated: bool,
    output_expires_at: NaiveDateTime,
    total_duration_ms: i32,
    query_duration_ms: i32,
    hydration_duration_ms: i32,
    render_duration_ms: i32,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::backup_task_outputs)]
struct NewBackupOutputRow {
    task_id: i32,
    document: Vec<u8>,
    byte_size: i64,
    sha256: String,
    output_expires_at: NaiveDateTime,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::remote_call_results)]
struct NewRemoteCallResultRow {
    task_id: i32,
    target_id: Option<i32>,
    subject_type: String,
    subject_id: i32,
    method: String,
    rendered_url: String,
    response_status: Option<i32>,
    response_headers: Option<Value>,
    response_body_preview: Option<String>,
    duration_ms: i32,
    success: bool,
    error: Option<String>,
}

#[derive(QueryableByName)]
struct ClaimableTaskId {
    #[diesel(sql_type = Integer)]
    id: i32,
}

#[derive(QueryableByName)]
struct DatabaseTimeRow {
    #[diesel(sql_type = Timestamp)]
    now: NaiveDateTime,
}

pub async fn claim_next_task(
    runtime: &PostgresRuntime,
    lease_duration: StorageTaskLeaseDuration,
) -> Result<Option<StorageTaskClaim>, PostgresStorageError> {
    let lease_milliseconds = validate_lease_duration(lease_duration)?;
    let row = runtime
        .with_transaction(async move |connection| {
            if !maintenance_is_normal(connection).await? {
                return Ok(None);
            }
            let task_kinds = StorageTaskKind::ALL.map(StorageTaskKind::as_str);
            let first_kind = NEXT_TASK_KIND.fetch_add(1, Ordering::Relaxed) % task_kinds.len();
            let claim_order =
                std::array::from_fn::<_, { StorageTaskKind::ALL.len() }, _>(|offset| {
                    task_kinds[(first_kind + offset) % task_kinds.len()]
                });
            let selected = diesel::sql_query(CLAIM_NEXT_QUEUED_TASK_SQL)
                .bind::<Text, _>(StorageTaskStatus::Queued.as_str())
                .bind::<Array<Text>, _>(claim_order.to_vec())
                .get_result::<ClaimableTaskId>(connection)
                .await
                .optional()?
                .map(|row| row.id);
            let Some(task_id) = selected else {
                return Ok(None);
            };
            use crate::schema::tasks::dsl as tasks;
            let claim_token = Uuid::new_v4();
            let row = diesel::update(tasks::tasks.filter(tasks::id.eq(task_id)))
                .set((
                    tasks::status.eq(StorageTaskStatus::Validating.as_str()),
                    tasks::started_at.eq(sql::<Nullable<Timestamp>>(DATABASE_UTC_NOW_SQL)),
                    tasks::lease_token.eq(Some(claim_token)),
                    tasks::lease_expires_at.eq(sql::<Nullable<Timestamp>>(
                        DATABASE_UTC_LEASE_EXPIRY_SQL_PREFIX,
                    )
                    .bind::<BigInt, _>(lease_milliseconds)
                    .sql(DATABASE_LEASE_EXPIRY_SQL_SUFFIX)),
                    tasks::attempt_count.eq(tasks::attempt_count + 1),
                    tasks::updated_at.eq(sql::<Timestamp>(DATABASE_UTC_NOW_SQL)),
                ))
                .returning(TaskRow::as_returning())
                .get_result::<TaskRow>(connection)
                .await?;
            append_task_lifecycle_event(
                connection,
                &row,
                StorageTaskEventInput::new(
                    StorageTaskStatus::Validating.as_str(),
                    "Task claimed for validation",
                ),
                &worker_provenance(&row)?,
            )
            .await?;
            Ok::<_, PostgresStorageError>(Some(row))
        })
        .await?;

    row.map(|row| {
        let token = row.lease_token.ok_or_else(|| {
            PostgresStorageError::database("Claimed task did not include a backend claim token")
        })?;
        tracing::info!(
            message = "Task claimed for validation",
            backend = "postgresql",
            task_id = row.id,
            task_kind = row.kind,
            previous_status = StorageTaskStatus::Queued.as_str(),
            status = row.status,
            submitted_by = ?row.submitted_by,
            total_items = row.total_items,
        );
        let lease = StorageTaskLease::new(
            TaskId::new(row.id)?,
            StorageTaskClaimToken::new(token.to_string()),
        );
        Ok(StorageTaskClaim::new(row.into_storage()?, lease))
    })
    .transpose()
}

pub async fn renew_task_lease(
    runtime: &PostgresRuntime,
    lease: StorageTaskLease,
    lease_duration: StorageTaskLeaseDuration,
) -> Result<bool, PostgresStorageError> {
    let claimed = claimed_task(&lease)?;
    let lease_milliseconds = validate_lease_duration(lease_duration)?;
    let updated = runtime
        .with_task_lease_connection(async move |connection| {
            crate::reach_fault_point(
                crate::PostgresFaultPoint::TaskLeaseBeforeRenewal,
                Some(connection),
            )
            .await?;
            use crate::schema::tasks::dsl as tasks;
            Ok::<_, PostgresStorageError>(
                diesel::update(
                    tasks::tasks
                        .filter(tasks::id.eq(claimed.id))
                        .filter(tasks::lease_token.eq(Some(claimed.token)))
                        .filter(
                            tasks::lease_expires_at
                                .gt(sql::<Nullable<Timestamp>>(DATABASE_UTC_NOW_SQL)),
                        )
                        .filter(tasks::status.eq_any(active_statuses())),
                )
                .set((
                    tasks::lease_expires_at.eq(sql::<Nullable<Timestamp>>(
                        DATABASE_UTC_LEASE_EXPIRY_SQL_PREFIX,
                    )
                    .bind::<BigInt, _>(lease_milliseconds)
                    .sql(DATABASE_LEASE_EXPIRY_SQL_SUFFIX)),
                    tasks::updated_at.eq(sql::<Timestamp>(DATABASE_UTC_NOW_SQL)),
                ))
                .execute(connection)
                .await?,
            )
        })
        .await?;
    Ok(updated == 1)
}

pub async fn recover_expired_task_leases(
    runtime: &PostgresRuntime,
    batch_size: usize,
) -> Result<Vec<StorageTask>, PostgresStorageError> {
    let batch_size = i64::try_from(batch_size).map_err(|_| {
        PostgresStorageError::invalid_input("Task recovery batch size is too large")
    })?;
    let recovered = runtime
        .with_transaction(async move |connection| {
            use crate::schema::tasks::dsl as tasks;
            let now = database_now(connection).await?;
            let stale = tasks::tasks
                .filter(tasks::status.eq_any(active_statuses()))
                .filter(tasks::deleted_at.is_null())
                .filter(
                    tasks::lease_expires_at
                        .is_null()
                        .or(tasks::lease_expires_at.le(now)),
                )
                .order(tasks::id.asc())
                .limit(batch_size)
                .for_update()
                .skip_locked()
                .select(TaskRow::as_select())
                .load::<TaskRow>(connection)
                .await?;
            let mut recovered = Vec::with_capacity(stale.len());
            for stale_task in stale {
                let kind = stored_task_kind(&stale_task)?;
                let counts = recovered_counts(connection, &stale_task, kind).await?;
                if kind == StorageTaskKind::Reindex {
                    mark_recovered_reindex_failed_on_connection(
                        connection,
                        stale_task.id,
                        LEASE_EXPIRED_MESSAGE,
                    )
                    .await?;
                }
                append_task_lifecycle_event(
                    connection,
                    &stale_task,
                    StorageTaskEventInput::new(StorageTaskStatus::Failed.as_str(), LEASE_EXPIRED_MESSAGE)
                        .with_data(Some(json!({
                            "previous_status": stale_task.status,
                            "lease_expires_at": stale_task.lease_expires_at,
                            "attempt_count": stale_task.attempt_count,
                            "operator_action": "inspect task history and submit a new task if replay is safe",
                        }))),
                    &system_provenance(&stale_task)?,
                )
                .await?;
                let row = diesel::update(tasks::tasks.filter(tasks::id.eq(stale_task.id)))
                    .set((
                        tasks::status.eq(StorageTaskStatus::Failed.as_str()),
                        tasks::summary.eq(Some(LEASE_EXPIRED_MESSAGE.to_string())),
                        tasks::processed_items.eq(counts.processed()),
                        tasks::success_items.eq(counts.succeeded()),
                        tasks::failed_items.eq(counts.failed()),
                        tasks::finished_at.eq(Some(now)),
                        tasks::request_payload.eq::<Option<Value>>(None),
                        tasks::request_redacted_at.eq(Some(now)),
                        tasks::lease_token.eq::<Option<Uuid>>(None),
                        tasks::lease_expires_at.eq::<Option<NaiveDateTime>>(None),
                        tasks::updated_at.eq(now),
                    ))
                    .returning(TaskRow::as_returning())
                    .get_result::<TaskRow>(connection)
                    .await?;
                recovered.push(row);
            }
            Ok::<_, PostgresStorageError>(recovered)
        })
        .await?;
    recovered
        .into_iter()
        .map(|row| {
            record_task_terminal(runtime, &row);
            row.into_storage()
        })
        .collect()
}

pub async fn append_task_event(
    runtime: &PostgresRuntime,
    request: StorageTaskEventAppend,
) -> Result<(), PostgresStorageError> {
    let (lease, event) = request.into_parts();
    let claimed = claimed_task(&lease)?;
    runtime
        .with_transaction(async move |connection| {
            let row = live_claimed_task(connection, claimed).await?;
            append_task_lifecycle_event(connection, &row, event, &worker_provenance(&row)?)
                .await
                .map(|_| ())
        })
        .await
}

pub async fn update_task_state(
    runtime: &PostgresRuntime,
    request: StorageTaskStateUpdate,
) -> Result<StorageTask, PostgresStorageError> {
    let (claimed, update) = state_update(request)?;
    if !update.status.is_active() {
        return Err(PostgresStorageError::invalid_input(format!(
            "Task state updates require an active status, received '{}'",
            update.status.as_str()
        )));
    }
    let row = update_task_state_row(runtime, claimed, update).await?;
    row.into_storage()
}

pub async fn complete_task(
    runtime: &PostgresRuntime,
    completion: StorageTaskCompletion,
) -> Result<StorageTask, PostgresStorageError> {
    let (update, event, artifact) = completion.into_parts();
    let (claimed, update) = state_update(update)?;
    if !update.status.is_terminal() {
        return Err(PostgresStorageError::invalid_input(format!(
            "Task completion requires a terminal status, received '{}'",
            update.status.as_str()
        )));
    }
    let stored = find_task(runtime, claimed.id).await?;
    let kind = stored_task_kind(&stored)?;
    let artifact_matches = matches!(
        (&artifact, kind),
        (
            StorageTaskCompletionArtifact::None,
            StorageTaskKind::Import | StorageTaskKind::Reindex
        ) | (
            StorageTaskCompletionArtifact::Export(_),
            StorageTaskKind::Export
        ) | (
            StorageTaskCompletionArtifact::Backup(_),
            StorageTaskKind::Backup
        ) | (
            StorageTaskCompletionArtifact::RemoteCall(_),
            StorageTaskKind::RemoteCall
        )
    );
    if !artifact_matches {
        return Err(PostgresStorageError::invalid_input(format!(
            "Task completion artifact does not match task kind '{}'",
            kind.as_str()
        )));
    }
    let row = match artifact {
        StorageTaskCompletionArtifact::None => {
            finalize_task(runtime, claimed, update, event, None).await?
        }
        StorageTaskCompletionArtifact::Export(artifact) => {
            let output = export_artifact(claimed.id, artifact);
            finalize_task(
                runtime,
                claimed,
                update,
                event,
                Some(TaskArtifact::Export(output)),
            )
            .await?
        }
        StorageTaskCompletionArtifact::Backup(artifact) => {
            let output = backup_artifact(claimed.id, artifact);
            finalize_task(
                runtime,
                claimed,
                update,
                event,
                Some(TaskArtifact::Backup(output)),
            )
            .await?
        }
        StorageTaskCompletionArtifact::RemoteCall(artifact) => {
            let output = remote_call_artifact(claimed.id, artifact);
            finalize_task(
                runtime,
                claimed,
                update,
                event,
                Some(TaskArtifact::RemoteCall(output)),
            )
            .await?
        }
    };
    record_task_terminal(runtime, &row);
    row.into_storage()
}

pub(super) async fn complete_task_on_connection(
    connection: &mut PostgresConnection,
    update: StorageTaskStateUpdate,
    event: StorageTaskEventInput,
) -> Result<TaskRow, PostgresStorageError> {
    let (claimed, update) = state_update(update)?;
    if !update.status.is_terminal() {
        return Err(PostgresStorageError::invalid_input(format!(
            "Task completion requires a terminal status, received '{}'",
            update.status.as_str()
        )));
    }
    finalize_task_connection(connection, claimed, update, event).await
}

pub async fn fail_task(
    runtime: &PostgresRuntime,
    failure: StorageTaskFailure,
) -> Result<StorageTask, PostgresStorageError> {
    let (lease, summary, event) = failure.into_parts();
    let claimed = claimed_task(&lease)?;
    let stored = find_task(runtime, claimed.id).await?;
    let kind = stored_task_kind(&stored)?;
    let counts = match kind {
        StorageTaskKind::Import => import_result_counts(runtime, claimed.id).await?,
        StorageTaskKind::Export | StorageTaskKind::Backup | StorageTaskKind::RemoteCall => {
            StorageTaskResultCounts::new(1, 0, 1)
        }
        StorageTaskKind::Reindex => {
            validated_counts(stored.processed_items, stored.success_items, 1)?
        }
    };
    let update = TaskStateUpdate {
        status: StorageTaskStatus::Failed,
        summary: Some(summary.clone()),
        counts,
        started_at: stored.started_at,
    };
    let row = if kind == StorageTaskKind::Reindex {
        let row = runtime
            .with_transaction(async move |connection| {
                live_claimed_task(connection, claimed).await?;
                mark_reindex_failed(connection, &stored, &summary).await?;
                finalize_task_connection(connection, claimed, update, event).await
            })
            .await?;
        runtime.record_computed_rebuild_finished("failed", Duration::ZERO);
        row
    } else {
        finalize_task(runtime, claimed, update, event, None).await?
    };
    record_task_terminal(runtime, &row);
    row.into_storage()
}

pub async fn purge_expired_export_outputs(
    runtime: &PostgresRuntime,
) -> Result<usize, PostgresStorageError> {
    purge_expired_outputs(runtime, OutputTable::Export).await
}

pub async fn purge_expired_backup_outputs(
    runtime: &PostgresRuntime,
) -> Result<usize, PostgresStorageError> {
    purge_expired_outputs(runtime, OutputTable::Backup).await
}

enum TaskArtifact {
    Export(NewExportOutputRow),
    Backup(NewBackupOutputRow),
    RemoteCall(NewRemoteCallResultRow),
}

async fn finalize_task(
    runtime: &PostgresRuntime,
    claimed: ClaimedTask,
    update: TaskStateUpdate,
    event: StorageTaskEventInput,
    artifact: Option<TaskArtifact>,
) -> Result<TaskRow, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| {
            if let Some(artifact) = artifact {
                persist_artifact(connection, artifact).await?;
            }
            finalize_task_connection(connection, claimed, update, event).await
        })
        .await
}

async fn persist_artifact(
    connection: &mut PostgresConnection,
    artifact: TaskArtifact,
) -> Result<(), PostgresStorageError> {
    match artifact {
        TaskArtifact::Export(output) => {
            use crate::schema::export_task_outputs::dsl as outputs;
            diesel::insert_into(outputs::export_task_outputs)
                .values(output)
                .on_conflict(outputs::task_id)
                .do_nothing()
                .execute(connection)
                .await?;
        }
        TaskArtifact::Backup(output) => {
            use crate::schema::backup_task_outputs::dsl as outputs;
            diesel::insert_into(outputs::backup_task_outputs)
                .values(output)
                .on_conflict(outputs::task_id)
                .do_nothing()
                .execute(connection)
                .await?;
        }
        TaskArtifact::RemoteCall(output) => upsert_remote_call_result(connection, output).await?,
    }
    Ok(())
}

async fn finalize_task_connection(
    connection: &mut PostgresConnection,
    claimed: ClaimedTask,
    update: TaskStateUpdate,
    event: StorageTaskEventInput,
) -> Result<TaskRow, PostgresStorageError> {
    use crate::schema::tasks::dsl as tasks;
    let row = tasks::tasks
        .filter(tasks::id.eq(claimed.id))
        .select(TaskRow::as_select())
        .first::<TaskRow>(connection)
        .await?;
    let recorded =
        append_task_lifecycle_event(connection, &row, event, &worker_provenance(&row)?).await?;
    let occurred_at = recorded.into_parts().0.occurred_at;
    crate::reach_fault_point(
        crate::PostgresFaultPoint::TaskFinalizeAfterEvent,
        Some(connection),
    )
    .await?;
    diesel::update(
        tasks::tasks
            .filter(tasks::id.eq(claimed.id))
            .filter(tasks::lease_token.eq(Some(claimed.token)))
            .filter(tasks::lease_expires_at.gt(sql::<Nullable<Timestamp>>(DATABASE_UTC_NOW_SQL))),
    )
    .set((
        tasks::status.eq(update.status.as_str()),
        tasks::summary.eq(update.summary),
        tasks::processed_items.eq(update.counts.processed()),
        tasks::success_items.eq(update.counts.succeeded()),
        tasks::failed_items.eq(update.counts.failed()),
        tasks::started_at.eq(update.started_at),
        tasks::finished_at.eq(Some(occurred_at)),
        tasks::request_payload.eq::<Option<Value>>(None),
        tasks::request_redacted_at.eq(Some(occurred_at)),
        tasks::lease_token.eq::<Option<Uuid>>(None),
        tasks::lease_expires_at.eq::<Option<NaiveDateTime>>(None),
        tasks::updated_at.eq(occurred_at),
    ))
    .returning(TaskRow::as_returning())
    .get_result::<TaskRow>(connection)
    .await
    .map_err(PostgresStorageError::from)
}

async fn update_task_state_row(
    runtime: &PostgresRuntime,
    claimed: ClaimedTask,
    update: TaskStateUpdate,
) -> Result<TaskRow, PostgresStorageError> {
    let row = runtime
        .with_connection(async move |connection| {
            use crate::schema::tasks::dsl as tasks;
            diesel::update(
                tasks::tasks
                    .filter(tasks::id.eq(claimed.id))
                    .filter(tasks::lease_token.eq(Some(claimed.token)))
                    .filter(
                        tasks::lease_expires_at
                            .gt(sql::<Nullable<Timestamp>>(DATABASE_UTC_NOW_SQL)),
                    ),
            )
            .set((
                tasks::status.eq(update.status.as_str()),
                tasks::summary.eq(update.summary),
                tasks::processed_items.eq(update.counts.processed()),
                tasks::success_items.eq(update.counts.succeeded()),
                tasks::failed_items.eq(update.counts.failed()),
                tasks::started_at.eq(update.started_at),
                tasks::updated_at.eq(sql::<Timestamp>(DATABASE_UTC_NOW_SQL)),
            ))
            .returning(TaskRow::as_returning())
            .get_result::<TaskRow>(connection)
            .await
        })
        .await?;
    tracing::info!(
        message = "Task state updated",
        backend = "postgresql",
        task_id = row.id,
        task_kind = row.kind,
        status = row.status,
        processed_items = row.processed_items,
        success_items = row.success_items,
        failed_items = row.failed_items,
    );
    Ok(row)
}

async fn append_task_lifecycle_event(
    connection: &mut PostgresConnection,
    task: &TaskRow,
    event: StorageTaskEventInput,
    provenance: &MutationProvenance,
) -> Result<hubuum_storage_core::StorageRecordedEvent, PostgresStorageError> {
    let (event_type, message, data) = event.into_parts();
    let action = Action::parse(&event_type).map_err(|_| {
        PostgresStorageError::internal(format!("Unknown task event type '{event_type}'"))
    })?;
    let mut metadata = json!({
        "task_id": task.id,
        "task_kind": task.kind,
    });
    if let Some(data) = data {
        metadata["data"] = data;
    }
    let event = NewEvent::new(EntityType::Task, action, provenance.actor_kind(), message)
        .map_err(|error| PostgresStorageError::internal(error.to_string()))?
        .with_entity_id(hubuum_events_core::EventEntityId::new(task.id)?)
        .with_metadata(metadata)
        .with_mutation_provenance(provenance);
    append_event(connection, &event).await
}

fn worker_provenance(task: &TaskRow) -> Result<MutationProvenance, PostgresStorageError> {
    Ok(MutationProvenance::worker(
        task.initiator_user_id.map(PrincipalId::new).transpose()?,
        TaskId::new(task.id)?,
    ))
}

fn system_provenance(task: &TaskRow) -> Result<MutationProvenance, PostgresStorageError> {
    Ok(MutationProvenance::system_for_task(
        task.initiator_user_id.map(PrincipalId::new).transpose()?,
        TaskId::new(task.id)?,
    ))
}

pub(super) async fn live_claimed_task(
    connection: &mut PostgresConnection,
    claimed: ClaimedTask,
) -> Result<TaskRow, PostgresStorageError> {
    use crate::schema::tasks::dsl as tasks;
    tasks::tasks
        .filter(tasks::id.eq(claimed.id))
        .filter(tasks::lease_token.eq(Some(claimed.token)))
        .filter(tasks::lease_expires_at.gt(sql::<Nullable<Timestamp>>(DATABASE_UTC_NOW_SQL)))
        .filter(tasks::status.eq_any(active_statuses()))
        .for_update()
        .select(TaskRow::as_select())
        .first::<TaskRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

pub(super) async fn find_task(
    runtime: &PostgresRuntime,
    task_id: i32,
) -> Result<TaskRow, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            use crate::schema::tasks::dsl as tasks;
            tasks::tasks
                .filter(tasks::id.eq(task_id))
                .select(TaskRow::as_select())
                .first::<TaskRow>(connection)
                .await
        })
        .await
}

fn state_update(
    request: StorageTaskStateUpdate,
) -> Result<(ClaimedTask, TaskStateUpdate), PostgresStorageError> {
    let (lease, status, summary, counts, started_at) = request.into_parts();
    validate_counts(counts)?;
    Ok((
        claimed_task(&lease)?,
        TaskStateUpdate {
            status,
            summary,
            counts,
            started_at: started_at.map(|timestamp| timestamp.naive_utc()),
        },
    ))
}

pub(super) fn claimed_task(lease: &StorageTaskLease) -> Result<ClaimedTask, PostgresStorageError> {
    let token = Uuid::parse_str(lease.token().adapter_value()).map_err(|_| {
        PostgresStorageError::invalid_input("Task claim token is not valid for this backend")
    })?;
    Ok(ClaimedTask {
        id: lease.task_id().id(),
        token,
    })
}

fn validate_lease_duration(
    duration: StorageTaskLeaseDuration,
) -> Result<i64, PostgresStorageError> {
    let milliseconds = duration.milliseconds();
    if milliseconds > 0 {
        Ok(milliseconds)
    } else {
        Err(PostgresStorageError::invalid_input(
            "Task lease duration must be greater than zero",
        ))
    }
}

fn validate_counts(counts: StorageTaskResultCounts) -> Result<(), PostgresStorageError> {
    if counts.processed() < 0 || counts.succeeded() < 0 || counts.failed() < 0 {
        Err(PostgresStorageError::internal(
            "Task result counts must not be negative",
        ))
    } else {
        Ok(())
    }
}

fn validated_counts(
    processed: i32,
    succeeded: i32,
    failed: i32,
) -> Result<StorageTaskResultCounts, PostgresStorageError> {
    let counts = StorageTaskResultCounts::new(processed, succeeded, failed);
    validate_counts(counts)?;
    Ok(counts)
}

fn stored_task_kind(task: &TaskRow) -> Result<StorageTaskKind, PostgresStorageError> {
    StorageTaskKind::from_persisted(&task.kind).ok_or_else(|| {
        PostgresStorageError::database(format!("Unknown stored task kind '{}'", task.kind))
    })
}

fn active_statuses() -> [&'static str; 2] {
    [
        StorageTaskStatus::Validating.as_str(),
        StorageTaskStatus::Running.as_str(),
    ]
}

async fn maintenance_is_normal(
    connection: &mut PostgresConnection,
) -> Result<bool, PostgresStorageError> {
    use crate::schema::system_maintenance::dsl as maintenance;
    let state = maintenance::system_maintenance
        .filter(maintenance::id.eq(1_i16))
        .select(maintenance::state)
        .first::<String>(connection)
        .await?;
    Ok(state == "normal")
}

async fn database_now(
    connection: &mut PostgresConnection,
) -> Result<NaiveDateTime, PostgresStorageError> {
    Ok(diesel::sql_query(DATABASE_UTC_NOW_QUERY)
        .get_result::<DatabaseTimeRow>(connection)
        .await?
        .now)
}

async fn recovered_counts(
    connection: &mut PostgresConnection,
    task: &TaskRow,
    kind: StorageTaskKind,
) -> Result<StorageTaskResultCounts, PostgresStorageError> {
    match kind {
        StorageTaskKind::Import => import_result_counts_connection(connection, task.id).await,
        StorageTaskKind::Export | StorageTaskKind::Backup | StorageTaskKind::RemoteCall => {
            Ok(StorageTaskResultCounts::new(1, 0, 1))
        }
        StorageTaskKind::Reindex => {
            validated_counts(task.processed_items, task.success_items, task.failed_items)
        }
    }
}

async fn import_result_counts(
    runtime: &PostgresRuntime,
    task_id: i32,
) -> Result<StorageTaskResultCounts, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            import_result_counts_connection(connection, task_id).await
        })
        .await
}

async fn import_result_counts_connection(
    connection: &mut PostgresConnection,
    task_id: i32,
) -> Result<StorageTaskResultCounts, PostgresStorageError> {
    use crate::schema::import_task_results::dsl as results;
    let processed = results::import_task_results
        .filter(results::task_id.eq(task_id))
        .count()
        .get_result::<i64>(connection)
        .await?;
    let failed = results::import_task_results
        .filter(results::task_id.eq(task_id))
        .filter(results::outcome.eq("failed"))
        .count()
        .get_result::<i64>(connection)
        .await?;
    let succeeded = processed.checked_sub(failed).ok_or_else(|| {
        PostgresStorageError::database("Import task failed count exceeds its processed count")
    })?;
    let processed = i32::try_from(processed)
        .map_err(|_| PostgresStorageError::database("Import task result count is out of range"))?;
    let succeeded = i32::try_from(succeeded)
        .map_err(|_| PostgresStorageError::database("Import task success count is out of range"))?;
    let failed = i32::try_from(failed)
        .map_err(|_| PostgresStorageError::database("Import task failure count is out of range"))?;
    Ok(StorageTaskResultCounts::new(processed, succeeded, failed))
}

async fn mark_reindex_failed(
    connection: &mut PostgresConnection,
    task: &TaskRow,
    stored_error: &str,
) -> Result<(), PostgresStorageError> {
    let Some(payload) = task.request_payload.as_ref() else {
        return Ok(());
    };
    let Some(class_id) = payload.get("class_id").and_then(Value::as_i64) else {
        return Ok(());
    };
    let Some(target_revision) = payload.get("target_revision").and_then(Value::as_i64) else {
        return Ok(());
    };
    let class_id = i32::try_from(class_id)
        .map_err(|_| PostgresStorageError::database("Stored reindex class id is out of range"))?;
    use crate::schema::class_computation_state::dsl as state;
    diesel::update(
        state::class_computation_state
            .filter(state::class_id.eq(class_id))
            .filter(state::evaluation_revision.eq(target_revision))
            .filter(state::active_task_id.eq(Some(task.id))),
    )
    .set((
        state::rebuild_status.eq("failed"),
        state::active_task_id.eq::<Option<i32>>(None),
        state::last_error.eq(Some(stored_error.chars().take(512).collect::<String>())),
        state::updated_at.eq(diesel::dsl::now),
    ))
    .execute(connection)
    .await?;
    Ok(())
}

#[doc(hidden)]
pub async fn mark_recovered_reindex_failed_on_connection(
    connection: &mut PostgresConnection,
    task_id: i32,
    stored_error: &str,
) -> Result<(), PostgresStorageError> {
    use crate::schema::class_computation_state::dsl as state;
    diesel::update(state::class_computation_state.filter(state::active_task_id.eq(Some(task_id))))
        .set((
            state::rebuild_status.eq("failed"),
            state::active_task_id.eq::<Option<i32>>(None),
            state::last_error.eq(Some(stored_error.chars().take(512).collect::<String>())),
            state::updated_at.eq(diesel::dsl::now),
        ))
        .execute(connection)
        .await?;
    Ok(())
}

fn export_artifact(task_id: i32, artifact: StorageExportTaskArtifact) -> NewExportOutputRow {
    let (identity, content, report, output_expires_at, durations) = artifact.into_parts();
    let (template_name, content_type) = identity.into_parts();
    let (json_output, text_output) = content.into_parts();
    let (meta_json, warnings_json, warning_count, truncated) = report.into_parts();
    NewExportOutputRow {
        task_id,
        template_name,
        content_type,
        json_output,
        text_output,
        meta_json,
        warnings_json,
        warning_count,
        truncated,
        output_expires_at: output_expires_at.naive_utc(),
        total_duration_ms: durations.total_ms(),
        query_duration_ms: durations.query_ms(),
        hydration_duration_ms: durations.hydration_ms(),
        render_duration_ms: durations.render_ms(),
    }
}

fn backup_artifact(task_id: i32, artifact: StorageBackupTaskArtifact) -> NewBackupOutputRow {
    let (document, byte_size, sha256, output_expires_at) = artifact.into_parts();
    NewBackupOutputRow {
        task_id,
        document,
        byte_size,
        sha256,
        output_expires_at: output_expires_at.naive_utc(),
    }
}

fn remote_call_artifact(
    task_id: i32,
    artifact: StorageRemoteCallTaskArtifact,
) -> NewRemoteCallResultRow {
    let (target, response, outcome) = artifact.into_parts();
    let target = target.into_parts();
    let (response_status, response_headers, response_body_preview) = response.into_parts();
    let (duration_ms, success, error) = outcome.into_parts();
    NewRemoteCallResultRow {
        task_id,
        target_id: target.target_id().map(|id| id.id()),
        subject_type: target.subject_type().as_str().to_string(),
        subject_id: target.subject_id().id(),
        method: target.method().map_or_else(
            || "unknown".to_string(),
            |method| method.as_str().to_string(),
        ),
        rendered_url: target.rendered_url().to_owned(),
        response_status,
        response_headers,
        response_body_preview,
        duration_ms,
        success,
        error,
    }
}

async fn upsert_remote_call_result(
    connection: &mut PostgresConnection,
    row: NewRemoteCallResultRow,
) -> Result<(), PostgresStorageError> {
    use crate::schema::remote_call_results::dsl as results;
    diesel::insert_into(results::remote_call_results)
        .values(&row)
        .on_conflict(results::task_id)
        .do_update()
        .set((
            results::target_id.eq(row.target_id),
            results::subject_type.eq(row.subject_type.clone()),
            results::subject_id.eq(row.subject_id),
            results::method.eq(row.method.clone()),
            results::rendered_url.eq(row.rendered_url.clone()),
            results::response_status.eq(row.response_status),
            results::response_headers.eq(row.response_headers.clone()),
            results::response_body_preview.eq(row.response_body_preview.clone()),
            results::duration_ms.eq(row.duration_ms),
            results::success.eq(row.success),
            results::error.eq(row.error.clone()),
        ))
        .execute(connection)
        .await?;
    Ok(())
}

#[derive(Clone, Copy)]
enum OutputTable {
    Export,
    Backup,
}

async fn purge_expired_outputs(
    runtime: &PostgresRuntime,
    table: OutputTable,
) -> Result<usize, PostgresStorageError> {
    let cleaned = runtime
        .with_transaction(async move |connection| {
            let now = chrono::Utc::now().naive_utc();
            let task_ids = match table {
                OutputTable::Export => {
                    use crate::schema::export_task_outputs::dsl as outputs;
                    diesel::delete(
                        outputs::export_task_outputs.filter(outputs::output_expires_at.le(now)),
                    )
                    .returning(outputs::task_id)
                    .get_results::<i32>(connection)
                    .await?
                }
                OutputTable::Backup => {
                    use crate::schema::backup_task_outputs::dsl as outputs;
                    diesel::delete(
                        outputs::backup_task_outputs.filter(outputs::output_expires_at.le(now)),
                    )
                    .returning(outputs::task_id)
                    .get_results::<i32>(connection)
                    .await?
                }
            };
            if task_ids.is_empty() {
                return Ok(0);
            }
            use crate::schema::tasks::dsl as tasks;
            let rows = tasks::tasks
                .filter(tasks::id.eq_any(task_ids))
                .select(TaskRow::as_select())
                .load::<TaskRow>(connection)
                .await?;
            let (event_type, message) = match table {
                OutputTable::Export => {
                    ("export", "Stored export output expired and was cleaned up")
                }
                OutputTable::Backup => {
                    ("backup", "Stored backup output expired and was cleaned up")
                }
            };
            for row in &rows {
                append_task_lifecycle_event(
                    connection,
                    row,
                    StorageTaskEventInput::new(Action::Cleanup.as_str(), message)
                        .with_data(Some(json!({ "cleaned_at": now }))),
                    &system_provenance(row)?,
                )
                .await?;
            }
            tracing::info!(
                message = "Expired task outputs cleaned up",
                backend = "postgresql",
                artifact = event_type,
                cleaned_count = rows.len(),
            );
            Ok::<_, PostgresStorageError>(rows.len())
        })
        .await?;
    Ok(cleaned)
}

pub(super) fn record_task_terminal(runtime: &PostgresRuntime, row: &TaskRow) {
    tracing::info!(
        message = "Task reached terminal state",
        backend = "postgresql",
        task_id = row.id,
        task_kind = row.kind,
        status = row.status,
        processed_items = row.processed_items,
        success_items = row.success_items,
        failed_items = row.failed_items,
        summary = row.summary.as_deref(),
    );
    let Ok(kind) = stored_task_kind(row) else {
        return;
    };
    let Some(status) = StorageTaskStatus::from_persisted(&row.status) else {
        return;
    };
    runtime.record_task_completed(
        kind.as_str(),
        status.as_str(),
        row.started_at
            .and_then(|started| duration_between(started, row.finished_at)),
    );
}

fn duration_between(start: NaiveDateTime, end: Option<NaiveDateTime>) -> Option<Duration> {
    let milliseconds = end?.signed_duration_since(start).num_milliseconds();
    (milliseconds >= 0).then(|| Duration::from_millis(milliseconds as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_order_rotates_all_required_task_kinds() {
        let kinds = StorageTaskKind::ALL.map(StorageTaskKind::as_str);
        let start = 2;
        let order = std::array::from_fn::<_, { StorageTaskKind::ALL.len() }, _>(|offset| {
            kinds[(start + offset) % kinds.len()]
        });

        assert_eq!(order.len(), StorageTaskKind::ALL.len());
        assert_eq!(order[0], kinds[start]);
    }

    #[test]
    fn invalid_claim_tokens_are_rejected_at_the_adapter_boundary() {
        let lease = StorageTaskLease::new(
            TaskId::new(7).unwrap(),
            StorageTaskClaimToken::new("not-a-uuid"),
        );
        let error = claimed_task(&lease)
            .err()
            .expect("invalid claim token must be rejected");

        assert_eq!(
            error.kind(),
            hubuum_storage_core::StorageErrorKind::InvalidInput
        );
    }
}
