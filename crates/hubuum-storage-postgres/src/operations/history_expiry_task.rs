//! Exact task ownership and native admission share one authority transaction.
use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_types::{Nullable, Timestamp};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::{
    HistoryExpiryTaskCheckpoint, HistoryExpiryTaskRequest, StorageExecutionPermit,
    StorageIdentityEvidence, StorageTaskEventInput,
};
use uuid::Uuid;

use super::task_execution::{
    append_task_lifecycle_event, claimed_task, database_now, require_claimed_execution,
    runnable_claimed_task, worker_provenance,
};
use crate::{PostgresRuntime, PostgresStorageError};

pub(crate) async fn process(
    runtime: &PostgresRuntime,
    scope: StorageExecutionPermit,
    request: HistoryExpiryTaskRequest,
) -> Result<HistoryExpiryTaskCheckpoint, PostgresStorageError> {
    let claimed = claimed_task(request.lease())?;
    runtime
        .with_history_control_transaction(scope, async move |connection| {
            // History authority precedes policy/accounting, task and source locks.
            // Do not wait for a writer holding the task before its accounting lock.
            diesel::sql_query("SELECT public.hubuum_task_capacity_lock()")
                .execute(connection)
                .await?;
            use crate::schema::tasks::dsl as tasks;
            let locked = tasks::tasks
                .filter(tasks::id.eq(claimed.id))
                .for_update()
                .skip_locked()
                .select(tasks::id)
                .first::<i32>(connection)
                .await
                .optional()?;
            if locked.is_none() {
                return Err(PostgresStorageError::conflict(
                    "Task is busy; retry expiry checkpointing",
                ));
            }
            require_claimed_execution(connection, claimed).await?;
            let row = runnable_claimed_task(connection, claimed).await?;
            let stored = row.clone().into_storage()?;
            if stored.control().deadline().is_none() {
                return Err(PostgresStorageError::conflict(
                    "History expiry requires a pinned task deadline",
                ));
            }
            let checkpoint = checkpoint_for(connection, &row).await?;
            let snapshot = crate::backend::capture_history_lifecycle_on(connection).await?;
            let (next, total, removed, scanned) = if checkpoint.admitted() {
                let total = checkpoint.retained_manifest(&snapshot)?.members().len() as i32;
                let Some(protection) = request.protection() else {
                    return Ok(checkpoint);
                };
                let (next, removed) = crate::backend::remove_history_expiry_on(connection, &checkpoint, protection).await?;
                (next, total, removed, 0)
            } else {
                // A retained original admission without its checkpoint is corruption or
                // incomplete recovery, never permission to create a replacement batch.
                if stored.checkpoint().is_none()
                    && snapshot
                        .control()
                        .authority()
                        .expiry()
                        .intents()
                        .iter()
                        .any(|intent| {
                            intent.manifest().run()
                                == Uuid::from_bytes(*checkpoint.task_identity().as_bytes())
                        })
                {
                    return Err(PostgresStorageError::unavailable(
                        "Recover the original history expiry task checkpoint",
                    ));
                }
                let Some(selection) = checkpoint.selection_request(snapshot.fence())? else {
                    return Ok(checkpoint);
                };
                request.require_selection_authorization(&checkpoint)?;
                let page = crate::backend::admit_history_expiry_on(connection, selection).await?;
                let total = page.manifest().map_or(0, |manifest| manifest.members().len() as i32);
                (checkpoint.record_page(&page)?, total, 0, page.scanned())
            };
            let encoded = next.encode()?;
            append_task_lifecycle_event(
                connection,
                &row,
                StorageTaskEventInput::new("running", "History expiry continuation committed")
                    .with_data(Some(serde_json::json!({
                        "batch": next.batch(), "admitted": next.admitted(),
                        "pass_finished": next.pass_finished(), "finished": next.finished(), "removed": removed, "scanned": scanned,
                    }))),
                &worker_provenance(&row)?,
            )
            .await?;
            runnable_claimed_task(connection, claimed).await?;
            let now = database_now(connection).await?;
            diesel::update(
                tasks::tasks
                    .filter(tasks::id.eq(claimed.id))
                    .filter(tasks::lease_token.eq(Some(claimed.token)))
                    .filter(tasks::lease_expires_at.gt(sql::<Nullable<Timestamp>>(
                        "clock_timestamp() AT TIME ZONE 'UTC'",
                    )))
                    .filter(tasks::execution_deadline_at.gt(sql::<Nullable<Timestamp>>(
                        "clock_timestamp() AT TIME ZONE 'UTC'",
                    )))
                    .filter(tasks::cancel_requested_at.is_null()),
            )
            .set((
                tasks::status.eq("running"),
                tasks::total_items.eq(total),
                tasks::processed_items.eq(removed as i32),
                tasks::success_items.eq(removed as i32),
                tasks::failed_items.eq(0),
                tasks::task_checkpoint.eq(Some(encoded.encoded())),
                tasks::updated_at.eq(now),
            ))
            .returning(tasks::id)
            .get_result::<i32>(connection)
            .await?;
            Ok::<_, PostgresStorageError>(next)
        })
        .await
}

pub(super) async fn checkpoint_for(
    connection: &mut crate::PostgresConnection,
    row: &super::task_rows::TaskRow,
) -> Result<HistoryExpiryTaskCheckpoint, PostgresStorageError> {
    use crate::schema::tasks::dsl as tasks;
    let evidence = tasks::tasks
        .filter(tasks::id.eq(row.id))
        .select(tasks::logical_identity)
        .first::<Option<serde_json::Value>>(connection)
        .await?
        .ok_or_else(|| {
            PostgresStorageError::unavailable("History expiry task identity is absent")
        })?;
    let evidence: StorageIdentityEvidence = serde_json::from_value(evidence).map_err(|error| {
        PostgresStorageError::invalid_persisted_value("expiry task identity", error)
    })?;
    HistoryExpiryTaskCheckpoint::from_task(&row.clone().into_storage()?, &evidence)
        .map_err(Into::into)
}
