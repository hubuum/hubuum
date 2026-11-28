//! Bounded exact-identity cleanup; SQL owns eligibility and physical receipts.

use super::*;

#[derive(Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum Progress {
    More {
        rows: u32,
        bytes: u64,
    },
    Blocked {
        reason: String,
    },
    Removed {
        revision: TaskControlRevision,
        rows: u32,
        bytes: u64,
    },
}

fn block(reason: &str) -> Result<TaskPurgeBlock, PostgresStorageError> {
    match reason {
        "held" => Ok(TaskPurgeBlock::Held),
        "output_available" => Ok(TaskPurgeBlock::OutputAvailable),
        "dependency" => Ok(TaskPurgeBlock::Dependency),
        "unknown_identity" => Ok(TaskPurgeBlock::UnknownIdentity),
        "nonterminal" => Ok(TaskPurgeBlock::Nonterminal),
        "oversized_row" => Ok(TaskPurgeBlock::OversizedRow),
        _ => Err(PostgresStorageError::database(
            "Invalid stored task purge blocker",
        )),
    }
}

pub async fn prepare_task_details_purge(
    runtime: &PostgresRuntime,
    selection: TaskPurgeSelection,
) -> Result<Option<TaskPurgeTicket>, PostgresStorageError> {
    prepare(runtime, selection, "task_details").await
}

pub async fn prepare_expired_task_outputs(
    runtime: &PostgresRuntime,
    selection: TaskPurgeSelection,
) -> Result<Option<TaskPurgeTicket>, PostgresStorageError> {
    prepare(runtime, selection, "expired_outputs").await
}

async fn prepare(
    runtime: &PostgresRuntime,
    selection: TaskPurgeSelection,
    scope: &'static str,
) -> Result<Option<TaskPurgeTicket>, PostgresStorageError> {
    runtime
        .with_control_transaction(async move |connection| {
            let row =
                diesel::sql_query("SELECT public.hubuum_task_purge_prepare($1,$2,$3) AS material")
                    .bind::<SqlUuid, _>(Uuid::new_v4())
                    .bind::<Text, _>(scope)
                    .bind::<Integer, _>(
                        selection.max_tasks().min(selection.budget().max_rows()) as i32
                    )
                    .get_result::<OptionalMaterialRow>(connection)
                    .await?;
            let ticket: Option<TaskPurgeTicket> = row.material.map(decode).transpose()?;
            if ticket
                .as_ref()
                .map(canonical)
                .transpose()?
                .is_some_and(|value| value.len() as u64 > selection.budget().max_bytes())
            {
                return Err(PostgresStorageError::unavailable(
                    "Task purge manifest exceeds the selection byte budget",
                ));
            }
            Ok(ticket)
        })
        .await
}

pub async fn list_pending_task_purges(
    runtime: &PostgresRuntime,
    limit: u32,
) -> Result<Vec<TaskPurgeTicket>, PostgresStorageError> {
    if limit == 0 || limit as usize > TASK_CONTROL_MAX_PURGE_TASKS {
        return Err(PostgresStorageError::invalid_input(
            "Task purge list requires 1..=64 batches",
        ));
    }
    runtime
        .with_control_connection(async move |connection| {
            let rows =
                diesel::sql_query("SELECT public.hubuum_task_purges_pending($1) AS material")
                    .bind::<Integer, _>(limit as i32)
                    .load::<MaterialRow>(connection)
                    .await?;
            rows.into_iter()
                .map(|row| decode(row.material))
                .collect::<Result<Vec<_>, _>>()
        })
        .await
}

pub(crate) async fn execute_on(
    connection: &mut PostgresConnection,
    request: &ProtectedTaskPurge,
    budget: TaskPurgeBudget,
) -> Result<TaskPurgeProgress, PostgresStorageError> {
    let ticket = serde_json::to_value(request.ticket())
        .map_err(|_| PostgresStorageError::internal("Cannot encode purge ticket"))?;
    let checkpoint = serde_json::to_value(request.checkpoint())
        .map_err(|_| PostgresStorageError::internal("Cannot encode purge checkpoint"))?;
    let row = diesel::sql_query("SELECT public.hubuum_task_purge_execute($1,$2,$3,$4) AS material")
        .bind::<Jsonb, _>(ticket)
        .bind::<Jsonb, _>(checkpoint)
        .bind::<Integer, _>(budget.max_rows() as i32)
        .bind::<BigInt, _>(budget.max_bytes() as i64)
        .get_result::<MaterialRow>(connection)
        .await?;
    match decode::<Progress>(row.material)? {
        Progress::More { rows, bytes } => Ok(TaskPurgeProgress::More { rows, bytes }),
        Progress::Blocked { reason } => Ok(TaskPurgeProgress::Blocked(block(&reason)?)),
        Progress::Removed {
            revision,
            rows,
            bytes,
        } => Ok(TaskPurgeProgress::Removed {
            revision,
            rows,
            bytes,
        }),
    }
}

pub async fn purge_task_details_batch(
    runtime: &PostgresRuntime,
    request: ProtectedTaskPurge,
    budget: TaskPurgeBudget,
) -> Result<TaskPurgeProgress, PostgresStorageError> {
    runtime
        .with_control_transaction(async move |connection| {
            execute_on(connection, &request, budget).await
        })
        .await
}
