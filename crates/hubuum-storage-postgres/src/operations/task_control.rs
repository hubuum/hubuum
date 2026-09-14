//! Durable cancellation, execution admission and stop acknowledgement.

use diesel::prelude::*;
use diesel::sql_types::{Integer, Text};
use diesel_async::RunQueryDsl;
use hubuum_domain::TaskId;
use hubuum_events_core::MutationProvenance;
use hubuum_storage_core::{
    StorageTask, StorageTaskCancellationChange, StorageTaskCancellationOutcome,
    StorageTaskCancellationRequest, StorageTaskEventInput, StorageTaskExecutionAdmission,
    StorageTaskExecutionObservation, StorageTaskKind, StorageTaskLease, StorageTaskRemoteDispatch,
    StorageTaskResultCounts, StorageTaskStatus,
};
use hubuum_task_core::TaskStopReason;
use serde_json::json;

use super::task_execution::{
    append_task_lifecycle_event, claimed_task, database_now, import_result_counts_connection,
    live_claimed_task, stored_task_kind, system_provenance,
};
use super::task_rows::TaskRow;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

/// Take computed-class locks before task/state locks, matching definition
/// changes. A task whose active class link has disappeared needs no cleanup lock.
pub(super) async fn lock_reindex_cleanup(
    connection: &mut PostgresConnection,
    task_ids: &[i32],
) -> Result<(), PostgresStorageError> {
    use crate::schema::class_computation_state::dsl as state;
    let classes = state::class_computation_state
        .filter(
            state::active_task_id.eq_any(task_ids.iter().copied().map(Some).collect::<Vec<_>>()),
        )
        .select(state::class_id)
        .order(state::class_id.asc())
        .load::<i32>(connection)
        .await?;
    for class in classes {
        super::computed_materialization::acquire_computed_class_shared_lock(connection, class)
            .await?;
    }
    Ok(())
}

pub async fn begin_remote_dispatch(
    runtime: &PostgresRuntime,
    request: StorageTaskRemoteDispatch,
) -> Result<(), PostgresStorageError> {
    let (lease, target) = request.into_parts();
    let claimed = claimed_task(&lease)?;
    runtime
        .with_transaction(async move |connection| {
            let row = live_claimed_task(connection, claimed).await?;
            if stored_task_kind(&row)? != StorageTaskKind::RemoteCall {
                return Err(PostgresStorageError::invalid_input(
                    "Only remote-call tasks may dispatch HTTP requests",
                ));
            }
            let now = database_now(connection).await?;
            if let Some(reason) = row
                .control(StorageTaskKind::RemoteCall)?
                .stop_reason(now.and_utc())
            {
                return Err(PostgresStorageError::task_stopped(reason));
            }
            if row.remote_dispatched_at.is_some() {
                return Err(PostgresStorageError::conflict(
                    "Remote dispatch was already admitted; automatic replay is forbidden",
                ));
            }
            super::task_execution::persist_remote_dispatch(connection, row.id, target).await?;
            use crate::schema::tasks::dsl as tasks;
            diesel::update(tasks::tasks.filter(tasks::id.eq(row.id)))
                .set(tasks::remote_dispatched_at.eq(Some(now)))
                .execute(connection)
                .await?;
            Ok(())
        })
        .await
}

pub async fn request_task_cancellation(
    runtime: &PostgresRuntime,
    request: StorageTaskCancellationRequest,
) -> Result<StorageTaskCancellationOutcome, PostgresStorageError> {
    runtime.with_transaction(async move |connection| {
        use crate::schema::tasks::dsl as tasks;
        if request.kind() == StorageTaskKind::Reindex {
            lock_reindex_cleanup(connection, &[request.task_id().id()]).await?;
        }
        // A non-key lock does not conflict with import receipt foreign keys.
        // The commit fence acquires the same lock to order commit vs request.
        let row = tasks::tasks.filter(tasks::id.eq(request.task_id().id()))
            .filter(tasks::deleted_at.is_null()).for_no_key_update()
            .select(TaskRow::as_select()).first::<TaskRow>(connection).await?;
        if row.kind != request.kind().as_str()
            || row.submitted_by != request.submitted_by().map(|id| id.id()) {
            return Err(PostgresStorageError::conflict("Task ownership changed; authorize cancellation again"));
        }
        let projected = row.clone().into_storage()?;
        if projected.status().is_terminal() || projected.control().cancellation().is_some() {
            return Ok(StorageTaskCancellationOutcome::new(projected, StorageTaskCancellationChange::Unchanged));
        }
        if request.required_status().is_some_and(|status| status != projected.status()) {
            return Err(PostgresStorageError::conflict("Task status no longer matches expected_status"));
        }
        let now = database_now(connection).await?;
        let row = diesel::update(tasks::tasks.filter(tasks::id.eq(row.id)))
            .set((tasks::cancel_requested_at.eq(Some(now)),
                tasks::cancel_requested_by.eq(request.event_context().actor_user_id().map(|id| id.id())),
                tasks::cancel_reason.eq(request.cancellation_reason().map(|reason| reason.as_str())),
                tasks::updated_at.eq(now)))
            .returning(TaskRow::as_returning()).get_result::<TaskRow>(connection).await?;
        let provenance = request.event_context().mutation_provenance();
        if projected.status() == StorageTaskStatus::Queued {
            let reason = row.control(request.kind())?.stop_reason(now.and_utc()).expect("persisted cancellation has a cause");
            let stopped = finish_stopped_on(connection, row, reason, provenance).await?;
            return Ok(StorageTaskCancellationOutcome::new(stopped.into_storage()?, StorageTaskCancellationChange::CancelledQueued));
        }
        append_task_lifecycle_event(connection, &row,
            StorageTaskEventInput::new("cancel_requested", "Task cancellation requested")
                .with_data(Some(json!({"reason": "cancel_requested", "cancel_requested_by": row.cancel_requested_by}))),
            provenance).await?;
        Ok(StorageTaskCancellationOutcome::new(row.into_storage()?, StorageTaskCancellationChange::Requested))
    }).await
}

pub async fn admit_task_execution(
    runtime: &PostgresRuntime,
    request: StorageTaskExecutionAdmission,
) -> Result<StorageTaskExecutionObservation, PostgresStorageError> {
    let (lease, limit) = request.into_parts();
    let claimed = claimed_task(&lease)?;
    runtime
        .with_transaction(async move |connection| {
            let row = live_claimed_task(connection, claimed).await?;
            let kind = stored_task_kind(&row)?;
            let now = database_now(connection).await?;
            if row.execution_deadline_at.is_some() {
                return Ok::<_, PostgresStorageError>(StorageTaskExecutionObservation::new(
                    row.control(kind)?,
                    now.and_utc(),
                ));
            }
            let duration = chrono::Duration::from_std(limit.duration()).map_err(|_| {
                PostgresStorageError::invalid_input("Execution duration is out of range")
            })?;
            // Anchor to the persisted first claim, so initialization cannot extend
            // execution by waiting behind database locks.
            let started = row.started_at.unwrap_or(now);
            let deadline = started.checked_add_signed(duration).ok_or_else(|| {
                PostgresStorageError::invalid_input("Execution deadline is out of range")
            })?;
            use crate::schema::tasks::dsl as tasks;
            let row = diesel::update(tasks::tasks.filter(tasks::id.eq(row.id)))
                .set(tasks::execution_deadline_at.eq(Some(deadline)))
                .returning(TaskRow::as_returning())
                .get_result::<TaskRow>(connection)
                .await?;
            Ok(StorageTaskExecutionObservation::new(
                row.control(kind)?,
                now.and_utc(),
            ))
        })
        .await
}

pub async fn poll_task_execution(
    runtime: &PostgresRuntime,
    lease: StorageTaskLease,
) -> Result<StorageTaskExecutionObservation, PostgresStorageError> {
    let claimed = claimed_task(&lease)?;
    runtime
        .with_task_lease_connection(async move |connection| {
            use crate::schema::tasks::dsl as tasks;
            let now = database_now(connection).await?;
            let row = tasks::tasks
                .filter(tasks::id.eq(claimed.id))
                .filter(tasks::lease_token.eq(Some(claimed.token)))
                .filter(tasks::lease_expires_at.gt(Some(now)))
                .filter(tasks::status.eq_any(["validating", "running"]))
                .select(TaskRow::as_select())
                .first::<TaskRow>(connection)
                .await?;
            Ok::<_, PostgresStorageError>(StorageTaskExecutionObservation::new(
                row.control(stored_task_kind(&row)?)?,
                now.and_utc(),
            ))
        })
        .await
}

pub async fn acknowledge_task_stop(
    runtime: &PostgresRuntime,
    lease: StorageTaskLease,
) -> Result<StorageTask, PostgresStorageError> {
    let claimed = claimed_task(&lease)?;
    let row = runtime
        .with_transaction(async move |connection| {
            lock_reindex_cleanup(connection, &[claimed.id]).await?;
            let row = live_claimed_task(connection, claimed).await?;
            let control = row.control(stored_task_kind(&row)?)?;
            let now = database_now(connection).await?;
            let reason = control.stop_reason(now.and_utc()).ok_or_else(|| {
                PostgresStorageError::conflict(
                    "Task has no cancellation request or expired deadline",
                )
            })?;
            let provenance = system_provenance(&row)?;
            finish_stopped_on(connection, row, reason, &provenance).await
        })
        .await?;
    row.into_storage()
}

/// Caller owns the task row and (for a worker) has verified the live lease.
/// The task, domain checkpoint cleanup and lifecycle event commit together.
pub(super) async fn finish_stopped_on(
    connection: &mut PostgresConnection,
    row: TaskRow,
    reason: TaskStopReason,
    provenance: &MutationProvenance,
) -> Result<TaskRow, PostgresStorageError> {
    use crate::schema::tasks::dsl as tasks;
    let kind = stored_task_kind(&row)?;
    let counts = if kind == StorageTaskKind::Import {
        import_result_counts_connection(connection, row.id).await?
    } else {
        crate::validate_persisted(
            "stopped task counts",
            StorageTaskResultCounts::try_new(
                row.processed_items,
                row.success_items,
                row.failed_items,
            ),
        )?
    };
    let completed_import = kind == StorageTaskKind::Import
        && (row.import_effects_committed_at.is_some()
            || (counts.processed() > 0 && counts.processed() == row.total_items));
    let status = if completed_import {
        if counts.failed() == 0 {
            StorageTaskStatus::Succeeded
        } else if counts.succeeded() > 0 {
            StorageTaskStatus::PartiallySucceeded
        } else {
            StorageTaskStatus::Failed
        }
    } else {
        StorageTaskStatus::Cancelled
    };
    let unattempted = row.total_items.saturating_sub(counts.processed()).max(0);
    // A claim from an older worker has no dispatch protocol evidence. Absence
    // of that evidence must never be presented as proof of remote rollback.
    let remote_possible = kind == StorageTaskKind::RemoteCall
        && (row.remote_dispatched_at.is_some()
            || (row.execution_deadline_at.is_none()
                && (row.started_at.is_some() || row.attempt_count > 0)));
    let summary = if completed_import {
        "Import effects committed before cancellation could stop execution".to_string()
    } else if kind == StorageTaskKind::Import {
        format!(
            "Task stopped: {}; {} committed/successful items, {} failed, {} unattempted",
            reason.as_str(),
            counts.succeeded(),
            counts.failed(),
            unattempted
        )
    } else if remote_possible {
        format!(
            "Task stopped: {}; remote side effects may have occurred; do not retry without reconciliation",
            reason.as_str()
        )
    } else {
        format!("Task stopped: {}", reason.as_str())
    };

    if status == StorageTaskStatus::Cancelled {
        match kind {
            StorageTaskKind::Reindex => {
                super::task_execution::mark_recovered_reindex_failed_on_connection(
                    connection, row.id, &summary,
                )
                .await?
            }
            StorageTaskKind::SchemaValidation => {
                super::schema_evolution::mark_schema_work_cancelled_on(
                    connection,
                    TaskId::new(row.id)?,
                )
                .await?
            }
            StorageTaskKind::Export => {
                diesel::delete(
                    crate::schema::export_task_outputs::table
                        .filter(crate::schema::export_task_outputs::task_id.eq(row.id)),
                )
                .execute(connection)
                .await?;
            }
            StorageTaskKind::Backup => {
                diesel::delete(
                    crate::schema::backup_task_outputs::table
                        .filter(crate::schema::backup_task_outputs::task_id.eq(row.id)),
                )
                .execute(connection)
                .await?;
            }
            StorageTaskKind::RemoteCall => {
                diesel::sql_query("UPDATE remote_call_results SET error=$2, side_effect_state=CASE WHEN response_status IS NULL THEN 'possibly_sent' ELSE 'response_received' END WHERE task_id=$1")
                    .bind::<Integer,_>(row.id).bind::<Text,_>(&summary).execute(connection).await?;
            }
            StorageTaskKind::Import => {}
        }
    }
    if kind == StorageTaskKind::Import && unattempted > 0 {
        // Planning order and execution order can differ. Report an exact
        // aggregate for work with no receipt, without inventing item identities.
        diesel::sql_query("INSERT INTO import_task_results (task_id, entity_kind, action, outcome, details) VALUES ($1, 'task', 'not_attempted', 'unattempted', $2)")
            .bind::<Integer,_>(row.id).bind::<diesel::sql_types::Jsonb,_>(json!({"count": unattempted, "reason": reason.as_str()})).execute(connection).await?;
    }
    let event = append_task_lifecycle_event(
        connection,
        &row,
        StorageTaskEventInput::new(status.as_str(), &summary).with_data(Some(json!({
            "reason": reason.as_str(), "cancel_requested_by": row.cancel_requested_by,
            "cancel_requested_at": row.cancel_requested_at,
            "processed_items": counts.processed(), "success_items": counts.succeeded(),
            "failed_items": counts.failed(), "unattempted_items": unattempted,
            "remote_side_effects_possible": remote_possible,
            "effects_committed": completed_import,
        }))),
        provenance,
    )
    .await?;
    let at = event.into_parts().0.occurred_at().naive_utc();
    diesel::update(tasks::tasks.filter(tasks::id.eq(row.id)))
        .set((
            tasks::status.eq(status.as_str()),
            tasks::summary.eq(Some(summary)),
            tasks::terminal_reason
                .eq((status == StorageTaskStatus::Cancelled).then_some(reason.as_str())),
            tasks::processed_items.eq(counts.processed()),
            tasks::success_items.eq(counts.succeeded()),
            tasks::failed_items.eq(counts.failed()),
            tasks::finished_at.eq(Some(at)),
            tasks::updated_at.eq(at),
            tasks::request_payload.eq::<Option<serde_json::Value>>(None),
            tasks::request_redacted_at.eq(Some(at)),
            tasks::lease_token.eq::<Option<uuid::Uuid>>(None),
            tasks::lease_expires_at.eq::<Option<chrono::NaiveDateTime>>(None),
        ))
        .returning(TaskRow::as_returning())
        .get_result::<TaskRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}
