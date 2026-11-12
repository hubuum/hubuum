//! One retained obligation, one live task owner. No selection or deletion here.
use super::super::task_control::resume_on;
use super::super::task_execution::{maintenance_is_normal, system_provenance};
use super::super::task_rows::TaskRow;
use super::*;
use diesel::sql_types::{Integer, Text};
use diesel::{QueryableByName, sql_query};
use hubuum_storage_core::{
    HistoryExpiryTaskPayload, StorageTask, StorageTaskKind, StorageTaskMetadata,
    StorageTaskResumeOutcome, StorageTaskStatus, TaskMetadataDetails,
};

pub(crate) async fn recover(
    runtime: &PostgresRuntime,
    scope: StorageExecutionPermit,
    batch: Uuid,
) -> Result<Option<StorageTask>, PostgresStorageError> {
    runtime.with_history_control_transaction(scope, async move |connection| {
        let snapshot = crate::backend::capture_history_lifecycle_on(connection).await?;
        let intent = snapshot.control().authority().expiry().intent(batch)
            .ok_or_else(|| PostgresStorageError::not_found("Retained history batch was not found"))?;
        let payload = HistoryExpiryTaskPayload::resume_retained(intent.manifest())?;
        if payload.application().incarnation() != snapshot.fence().incarnation()
            || payload.application().revision() as u64 != snapshot.fence().deletion_revision().value() {
            return Err(PostgresStorageError::unavailable("History recovery requires the original application image"));
        }
        sql_query("SELECT public.hubuum_task_capacity_lock()").execute(connection).await?;
        if !maintenance_is_normal(connection).await? {
            return Err(PostgresStorageError::unavailable("History task recovery is paused during maintenance"));
        }
        // Both predicates have dedicated indexes. A retained journal has at most
        // 256 batches; failed cleanup attempts do not widen this lookup.
        let original = sql_query("SELECT id FROM public.tasks WHERE logical_identity #>> '{subject,identity,identity}'=$1 LIMIT 1")
            .bind::<Text,_>(intent.manifest().run().to_string())
            .get_result::<TaskIdRow>(connection).await.optional()?;
        let cleanup = sql_query("SELECT id FROM public.tasks WHERE kind='history_expiry' AND discovery_metadata #>> '{data,recovery_batch}'=$1 ORDER BY id DESC LIMIT 1")
            .bind::<Text,_>(batch.to_string())
            .get_result::<TaskIdRow>(connection).await.optional()?;
        use crate::schema::tasks::dsl as tasks;
        for id in original.iter().chain(cleanup.iter()).map(|row| row.id) {
            // Never mistake a busy row for deletion and adopt its obligation.
            let Some(row) = tasks::tasks.filter(tasks::id.eq(id)).for_update().skip_locked()
                .select(TaskRow::as_select()).first::<TaskRow>(connection).await.optional()? else {
                return Err(PostgresStorageError::conflict("History task owner is busy; retry recovery"));
            };
            let stored = row.clone().into_storage()?;
            if stored.kind() != StorageTaskKind::HistoryExpiry {
                return Err(PostgresStorageError::conflict("Retained history task identity changed kind"));
            }
            if stored.status() == StorageTaskStatus::Succeeded {
                if !intent.is_complete() || stored.progress().succeeded() as usize != intent.removals().len() {
                    return Err(PostgresStorageError::conflict("Successful history task differs from retained receipts"));
                }
                // Terminalization cleared the checkpoint after protected finalization.
                return Ok(Some(stored));
            }
            if stored.status().is_terminal() { continue; }
            let checkpoint = checkpoint_for(connection, &row).await?;
            if checkpoint.batch() != batch { return Err(PostgresStorageError::conflict("History task batch changed")); }
            checkpoint.retained_manifest(&snapshot)?;
            if stored.status() != StorageTaskStatus::Waiting { return Ok(Some(stored)); }
            let now = database_now(connection).await?;
            if stored.control().stop_reason(now.and_utc()).is_none()
                && now.and_utc() < stored.updated_at() + chrono::Duration::seconds(30) {
                return Ok(Some(stored));
            }
            let resumed = resume_on(connection, row).await?;
            match resumed {
                StorageTaskResumeOutcome::Queued(task) => return Ok(Some(task)),
                StorageTaskResumeOutcome::Stopped(_) => {},
                StorageTaskResumeOutcome::Unchanged => unreachable!("locked waiting owner"),
            }
        }
        // This internal cleanup has no new initiating principal. Original origin
        // remains in the immutable manifest, even after its principal/task purge.
        let raw = serde_json::to_value(&payload).map_err(|error| PostgresStorageError::invalid_persisted_value("history recovery payload", error))?;
        let removed = intent.removals().len() as i32;
        let row = diesel::insert_into(tasks::tasks).values((
            tasks::kind.eq("history_expiry"), tasks::status.eq("queued"),
            tasks::request_payload.eq(Some(raw)),
            tasks::discovery_metadata.eq(Some(StorageTaskMetadata::new(
                TaskMetadataDetails::HistoryExpiry { recovery_batch: Some(batch) })?.to_value())),
            tasks::total_items.eq(intent.manifest().members().len() as i32),
            tasks::processed_items.eq(removed), tasks::success_items.eq(removed),
        )).returning(TaskRow::as_returning()).get_result::<TaskRow>(connection).await?;
        let checkpoint = checkpoint_for(connection, &row).await?;
        checkpoint.retained_manifest(&snapshot)?;
        append_task_lifecycle_event(connection, &row,
            StorageTaskEventInput::new("queued", "Retained history batch adopted for cleanup")
                .with_data(Some(serde_json::json!({"batch": batch,"original_run": intent.manifest().run()}))),
            &system_provenance(&row)?).await?;
        crate::worker_notifications::notify_task_queue(connection, row.id).await?;
        Ok(Some(row.into_storage()?))
    }).await
}

#[derive(QueryableByName)]
struct TaskIdRow {
    #[diesel(sql_type = Integer)]
    id: i32,
}
