//! Shared exact-scope admission under history authority and task capacity locks.
use super::super::task_execution::maintenance_is_normal;
use super::super::task_rows::TaskRow;
use super::*;
use diesel::sql_types::Jsonb;
use hubuum_storage_core::{
    HistoryExpiryTaskAdmission, HistoryExpiryTaskPayload, HistoryExpiryTaskSubmission,
    HistoryLifecycleSnapshot, HistoryPolicyKey, StorageTask,
};

pub(crate) async fn submit(
    runtime: &PostgresRuntime,
    scope: StorageExecutionPermit,
    request: HistoryExpiryTaskSubmission,
) -> Result<HistoryExpiryTaskAdmission, PostgresStorageError> {
    runtime
        .with_history_control_transaction(scope, async move |connection| {
            let snapshot = crate::backend::capture_history_lifecycle_on(connection).await?;
            request.validate(&snapshot)?;
            diesel::sql_query("SELECT public.hubuum_task_capacity_lock()")
                .execute(connection)
                .await?;
            if !maintenance_is_normal(connection).await? {
                return Err(PostgresStorageError::unavailable(
                    "History task admission is paused during maintenance",
                ));
            }
            if let Some(submitter) = request.submitter() {
                use crate::schema::principals;
                let raw = principals::table
                    .filter(principals::id.eq(submitter.id()))
                    .select(principals::logical_identity)
                    .first::<Option<serde_json::Value>>(connection)
                    .await?
                    .ok_or_else(|| {
                        PostgresStorageError::conflict("History initiating principal is unresolved")
                    })?;
                let evidence = serde_json::from_value(raw).map_err(|error| {
                    PostgresStorageError::invalid_persisted_value("history principal", error)
                })?;
                request.validate_principal(&evidence)?;
            }
            let key = request.payload().scan().key();
            if !snapshot.control().retains_scope(key.scope())
                && !crate::backend::history_scope_known_on(connection, key.scope()).await?
            {
                return Err(PostgresStorageError::not_found(
                    "History policy scope was not found",
                ));
            }
            if let Some(task) = active_on(connection, key, &snapshot).await? {
                return Ok(HistoryExpiryTaskAdmission::new(task, false));
            }
            for intent in snapshot.control().authority().expiry().intents() {
                if intent.manifest().key() == key
                    && let Some(task) =
                        super::recovery::recover_on(connection, intent.manifest().identity())
                            .await?
                    && !task.status().is_terminal()
                {
                    return Ok(HistoryExpiryTaskAdmission::new(task, true));
                }
            }
            let task = super::recovery::create_on(
                connection,
                request.payload(),
                request.submitter(),
                0,
                0,
            )
            .await?;
            Ok(HistoryExpiryTaskAdmission::new(task, true))
        })
        .await
}

pub(super) async fn active_on(
    connection: &mut crate::PostgresConnection,
    key: HistoryPolicyKey,
    snapshot: &HistoryLifecycleSnapshot,
) -> Result<Option<StorageTask>, PostgresStorageError> {
    let key = serde_json::to_value(key)
        .map_err(|error| PostgresStorageError::invalid_persisted_value("history scope", error))?;
    let id = diesel::sql_query("SELECT id FROM public.tasks WHERE kind='history_expiry' AND status IN ('queued','validating','running','waiting') AND coalesce(nullif(discovery_metadata #> '{data,admission_scope}','null'::jsonb),request_payload #> '{scan,key}')=$1 LIMIT 1")
        .bind::<Jsonb,_>(key).get_result::<super::recovery::TaskIdRow>(connection).await.optional()?;
    let Some(id) = id else {
        return Ok(None);
    };
    use crate::schema::tasks::dsl as tasks;
    let row = tasks::tasks
        .filter(tasks::id.eq(id.id))
        .for_update()
        .skip_locked()
        .select(TaskRow::as_select())
        .first::<TaskRow>(connection)
        .await
        .optional()?
        .ok_or_else(|| {
            PostgresStorageError::conflict("History task owner is busy; retry admission")
        })?;
    let stored = row.clone().into_storage()?;
    let payload = HistoryExpiryTaskPayload::decode(
        stored
            .request_payload()
            .ok_or_else(|| PostgresStorageError::conflict("History task payload is absent"))?,
    )?;
    if payload.application().incarnation() != snapshot.fence().incarnation()
        || payload.application().revision() as u64 != snapshot.fence().deletion_revision().value()
    {
        return Err(PostgresStorageError::conflict(
            "Active history task belongs to another application image",
        ));
    }
    if let Some(reason) = stored
        .control()
        .stop_reason(database_now(connection).await?.and_utc())
    {
        let provenance = super::super::task_execution::system_provenance(&row)?;
        super::super::task_control::finish_stopped_on(connection, row, reason, &provenance).await?;
        return Ok(None);
    }
    checkpoint_for(connection, &row).await?;
    Ok(Some(row.into_storage()?))
}
