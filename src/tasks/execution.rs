use tracing::{Instrument, info, info_span, warn};

use crate::errors::ApiError;
use crate::models::{
    ImportAtomicity, ImportCollisionPolicy, ImportMode, ImportPermissionPolicy, ImportRequest,
    NewTaskEventRecord, TaskResultCounts, TaskStatus, TokenScope,
};
use crate::observability::metrics;
use crate::permissions::AuthorizationContext;
use crate::services::tasks::{
    ClaimedTask, TaskStateChange, append_task_event, complete_task, update_task_state,
};
use crate::storage::{ImportStorage, StorageTaskCompletionPayload};

use super::helpers::{
    flush_import_result_batches, import_failure_outcome, sanitize_error_for_storage,
};
use super::planning::plan_import;
use super::types::{ExecutionAccumulator, PlannedItem, TerminalTaskUpdate};

pub(super) async fn execute_import_task<C>(
    backend: &C,
    task: &ClaimedTask,
    user: &impl crate::traits::AuthzSubject,
    scopes: Option<&TokenScope>,
) -> Result<(), ApiError>
where
    C: AuthorizationContext,
{
    let total_timer = metrics::import_phase_timer(metrics::ImportMetricPhase::Total);
    let pool = backend;
    let payload = task
        .request_payload
        .clone()
        .ok_or_else(|| ApiError::BadRequest("Import task payload is missing".to_string()))?;
    let request: ImportRequest = serde_json::from_value(payload)?;
    request.validate()?;
    let mode = request.mode();
    let atomicity = mode.atomicity.unwrap_or(ImportAtomicity::Strict);
    let collision_policy = mode
        .collision_policy
        .unwrap_or(ImportCollisionPolicy::Abort);
    let permission_policy = mode
        .permission_policy
        .unwrap_or(ImportPermissionPolicy::Abort);
    let import_span = info_span!(
        "import_task",
        task_id = task.id,
        task_kind = %task.kind,
        submitted_by = user.principal_id(),
        total_items = task.total_items,
        dry_run = request.dry_run(),
        atomicity = ?atomicity,
        collision_policy = ?collision_policy,
        permission_policy = ?permission_policy
    );

    async {
        let planning_timer = metrics::import_phase_timer(metrics::ImportMetricPhase::Planning);
        let planning = plan_import(backend, user, scopes, &request)
            .instrument(info_span!("import_planning"))
            .await;
        let planning_time = planning_timer.finish(metrics::ImportMetricOutcome::Success);

        info!(
            message = "Import planning finished",
            task_id = task.id,
            planned_items = planning.planned_items.len(),
            validation_failures = planning.failures.len(),
            aborted = planning.aborted,
            planning_time = ?planning_time
        );

        let mut accumulator = ExecutionAccumulator::default();

        if !planning.failures.is_empty()
            && (matches!(atomicity, ImportAtomicity::Strict) || planning.aborted)
        {
            let results = planning
                .failures
                .into_iter()
                .map(|failure| failure.into_result(task.id))
                .collect::<Vec<_>>();
            let failed_count = results.len() as i32;
            info!(
                message = "Import validation failed before execution",
                task_id = task.id,
                dry_run = request.dry_run(),
                planned_items = 0,
                validation_failures = failed_count,
                atomicity = ?atomicity,
                planning_time = ?planning_time,
                total_time = ?total_timer.elapsed()
            );
            crate::storage::storage_handle(pool)
                .record_import_results(results)
                .await?;
            let summary = format!("Import validation failed for {failed_count} item(s)");
            finalize_task(
                pool,
                task,
                TerminalTaskUpdate {
                    status: TaskStatus::Failed,
                    summary,
                    counts: TaskResultCounts::from_outcomes(0, failed_count)?,
                    event_data: None,
                },
            )
            .await?;
            metrics::import_items(failed_count, 0, failed_count);
            total_timer.finish(metrics::ImportMetricOutcome::Failed);
            return Ok(());
        }

        let super::types::PlanningOutcome {
            planned_items,
            failures,
            aborted: _,
        } = planning;

        info!(
            message = "Import execution starting",
            task_id = task.id,
            dry_run = request.dry_run(),
            planned_items = planned_items.len(),
            validation_failures = failures.len(),
            atomicity = ?atomicity,
            collision_policy = ?collision_policy,
            permission_policy = ?permission_policy,
            planning_time = ?planning_time
        );

        append_task_event(
            pool,
            task,
            NewTaskEventRecord {
                event_type: "running".to_string(),
                message: if request.dry_run() {
                    "Import dry run planned successfully".to_string()
                } else if failures.is_empty() {
                    "Import execution started".to_string()
                } else {
                    format!(
                        "Import execution started with {} planned failure(s)",
                        failures.len()
                    )
                },
                data: None,
            },
        )
        .await?;

        update_task_state(
            pool,
            task,
            TaskStateChange::new(TaskStatus::Running, TaskResultCounts::default())
                .started_at(task.started_at),
        )
        .await?;

        let execution_timer = metrics::import_phase_timer(metrics::ImportMetricPhase::Execution);
        if request.dry_run() {
            for failure in failures {
                let outcome = failure.outcome();
                accumulator.push_failure(
                    task.id,
                    &failure.item,
                    failure.message_for_storage(),
                    outcome,
                );
                flush_import_result_batches(pool, &mut accumulator, false).await?;
            }
            for item in &planned_items {
                accumulator.push_success(task.id, &item.result, "planned");
                flush_import_result_batches(pool, &mut accumulator, false).await?;
            }
        } else {
            for failure in failures {
                let outcome = failure.outcome();
                accumulator.push_failure(
                    task.id,
                    &failure.item,
                    failure.message_for_storage(),
                    outcome,
                );
                flush_import_result_batches(pool, &mut accumulator, false).await?;
            }
            match atomicity {
                ImportAtomicity::Strict => {
                    execute_import_strict(pool, task.id, &planned_items, &mut accumulator)
                        .instrument(info_span!("import_apply", mode = "strict"))
                        .await?;
                }
                ImportAtomicity::BestEffort => {
                    execute_import_best_effort(
                        pool,
                        task.id,
                        &planned_items,
                        &mode,
                        &mut accumulator,
                    )
                    .instrument(info_span!("import_apply", mode = "best_effort"))
                    .await?;
                }
            }
        }

        flush_import_result_batches(pool, &mut accumulator, true).await?;

        let status = if accumulator.failed == 0 {
            TaskStatus::Succeeded
        } else if accumulator.success == 0 {
            TaskStatus::Failed
        } else {
            TaskStatus::PartiallySucceeded
        };
        let metric_outcome = match status {
            TaskStatus::Succeeded => metrics::ImportMetricOutcome::Success,
            TaskStatus::Failed => metrics::ImportMetricOutcome::Failed,
            TaskStatus::PartiallySucceeded => metrics::ImportMetricOutcome::PartiallySucceeded,
            _ => unreachable!("import execution produced a non-terminal status"),
        };

        let summary = format!(
            "Import finished with {} succeeded and {} failed items",
            accumulator.success, accumulator.failed
        );

        let execution_time = execution_timer.finish(metric_outcome);
        info!(
            message = "Import execution finished",
            task_id = task.id,
            processed_items = accumulator.processed,
            success_items = accumulator.success,
            failed_items = accumulator.failed,
            execution_time = ?execution_time,
            total_time = ?total_timer.elapsed()
        );

        finalize_task(
            pool,
            task,
            TerminalTaskUpdate {
                status,
                summary,
                counts: TaskResultCounts::from_outcomes(accumulator.success, accumulator.failed)?,
                event_data: Some(serde_json::json!({
                    "processed_items": accumulator.processed,
                    "success_items": accumulator.success,
                    "failed_items": accumulator.failed
                })),
            },
        )
        .await?;
        metrics::import_items(
            accumulator.processed,
            accumulator.success,
            accumulator.failed,
        );
        total_timer.finish(metric_outcome);

        Ok(())
    }
    .instrument(import_span)
    .await
}

async fn finalize_task(
    pool: &impl crate::storage::StorageContext,
    task: &ClaimedTask,
    terminal: TerminalTaskUpdate,
) -> Result<(), ApiError> {
    complete_task(
        pool,
        task,
        TaskStateChange::new(terminal.status, terminal.counts)
            .summary(terminal.summary.clone())
            .started_at(task.started_at),
        NewTaskEventRecord {
            event_type: terminal.status.as_str().to_string(),
            message: terminal.summary.clone(),
            data: terminal.event_data,
        },
        StorageTaskCompletionPayload::Import,
    )
    .await?;
    Ok(())
}

fn import_storage_plan(
    planned_items: &[PlannedItem],
) -> Result<crate::storage::StorageImportPlan, ApiError> {
    let items = planned_items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| item.execution.clone().map(|execution| (index, execution)))
        .map(|(index, execution)| {
            crate::services::import_boundary::import_operation_to_storage(execution)
                .map(|execution| crate::storage::StorageImportPlanItem::new(index, execution))
        })
        .collect::<Result<Vec<_>, _>>()?;
    crate::storage::StorageImportPlan::try_new(items).map_err(ApiError::from)
}

pub(super) async fn execute_import_strict(
    pool: &impl crate::storage::StorageContext,
    task_id: i32,
    planned_items: &[PlannedItem],
    accumulator: &mut ExecutionAccumulator,
) -> Result<(), ApiError> {
    crate::storage::storage_handle(pool)
        .apply_import_strict(import_storage_plan(planned_items)?)
        .await?;

    for item in planned_items {
        accumulator.push_success(task_id, &item.result, "succeeded");
        flush_import_result_batches(pool, accumulator, false).await?;
    }
    Ok(())
}

pub(super) async fn execute_import_best_effort(
    pool: &impl crate::storage::StorageContext,
    task_id: i32,
    planned_items: &[PlannedItem],
    mode: &ImportMode,
    accumulator: &mut ExecutionAccumulator,
) -> Result<(), ApiError> {
    let (outcomes, aborted) = crate::storage::storage_handle(pool)
        .apply_import_best_effort(
            import_storage_plan(planned_items)?,
            crate::services::import_boundary::import_mode_to_storage(mode.clone()),
        )
        .await?
        .into_parts();
    let cutoff = aborted
        .then(|| outcomes.last().map(|item| item.index()))
        .flatten();
    let mut outcomes = outcomes
        .into_iter()
        .map(|item| {
            let (index, error) = item.into_parts();
            (index, error)
        })
        .collect::<std::collections::HashMap<_, _>>();

    for (index, item) in planned_items.iter().enumerate() {
        if cutoff.is_some_and(|cutoff| index > cutoff) {
            break;
        }
        match outcomes.remove(&index) {
            Some(Some(error)) => {
                let error = ApiError::from(error);
                let outcome = import_failure_outcome(&error);
                let sanitized_error = sanitize_error_for_storage(&error);
                accumulator.push_failure(task_id, &item.result, sanitized_error, outcome);
            }
            Some(None) | None if item.execution.is_none() => {
                accumulator.push_success(task_id, &item.result, "succeeded");
            }
            Some(None) => {
                accumulator.push_success(task_id, &item.result, "succeeded");
            }
            None => continue,
        }
        flush_import_result_batches(pool, accumulator, false).await?;
    }

    if aborted {
        warn!(
            message = "Import best-effort execution aborted early",
            task_id = task_id,
            processed_items = accumulator.processed,
            success_items = accumulator.success,
            failed_items = accumulator.failed
        );
    }

    Ok(())
}
