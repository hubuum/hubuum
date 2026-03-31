use std::time::Instant;

use tracing::{Instrument, error, info, info_span, warn};

use crate::db::traits::task::{
    TaskStateUpdate, append_task_event, finalize_task_terminal_state, update_task_state,
};
use crate::db::{DbPool, with_transaction};
use crate::errors::ApiError;
use crate::models::{
    ImportAtomicity, ImportCollisionPolicy, ImportMode, ImportPermissionPolicy, ImportRequest,
    NewTaskEventRecord, TaskRecord, TaskStatus, User,
};

use super::helpers::{
    flush_import_result_batches, sanitize_error_for_storage, should_abort_best_effort_execution,
};
use super::planning::plan_import;
use super::resolution::{resolve_class_runtime, resolve_namespace_runtime, resolve_object_runtime};
use super::types::{
    ExecutionAccumulator, PlannedExecution, PlannedItem, PlannedTaskResult, RuntimeState,
    TerminalTaskUpdate,
};
use crate::db::traits::task_import::{
    apply_permissions_db, create_class_db, create_class_relation_db, create_namespace_db,
    create_object_db, create_object_relation_db, lookup_group_by_name_db, update_class_db,
    update_namespace_db, update_object_db,
};

pub(super) async fn execute_import_task(
    pool: &DbPool,
    task: &TaskRecord,
    user: &User,
) -> Result<(), ApiError> {
    let payload = task
        .request_payload
        .clone()
        .ok_or_else(|| ApiError::BadRequest("Import task payload is missing".to_string()))?;
    let request: ImportRequest = serde_json::from_value(payload)?;
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
        submitted_by = user.id,
        total_items = task.total_items,
        dry_run = request.dry_run(),
        atomicity = ?atomicity,
        collision_policy = ?collision_policy,
        permission_policy = ?permission_policy
    );

    async {
        let total_start = Instant::now();
        let planning_start = Instant::now();
        let planning = plan_import(pool, user, &request)
            .instrument(info_span!("import_planning"))
            .await;
        let planning_time = planning_start.elapsed();

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
                total_time = ?total_start.elapsed()
            );
            crate::db::traits::task::insert_import_results(pool, &results).await?;
            let summary = format!("Import validation failed for {failed_count} item(s)");
            finalize_task(
                pool,
                task,
                TerminalTaskUpdate {
                    status: TaskStatus::Failed,
                    summary,
                    processed_items: failed_count,
                    success_items: 0,
                    failed_items: failed_count,
                    event_data: None,
                },
            )
            .await?;
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
            NewTaskEventRecord {
                task_id: task.id,
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
            task.id,
            TaskStateUpdate {
                status: TaskStatus::Running,
                summary: None,
                processed_items: 0,
                success_items: 0,
                failed_items: 0,
                started_at: task.started_at,
                finished_at: None,
            },
        )
        .await?;

        let execution_start = Instant::now();
        if request.dry_run() {
            for failure in failures {
                accumulator.push_failure(
                    task.id,
                    &failure.item,
                    failure.message_for_storage(),
                    "failed",
                );
                flush_import_result_batches(pool, &mut accumulator, false).await?;
            }
            for item in &planned_items {
                accumulator.push_success(task.id, &item.result, "planned");
                flush_import_result_batches(pool, &mut accumulator, false).await?;
            }
        } else {
            for failure in failures {
                accumulator.push_failure(
                    task.id,
                    &failure.item,
                    failure.message_for_storage(),
                    "failed",
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

        let summary = format!(
            "Import finished with {} succeeded and {} failed items",
            accumulator.success, accumulator.failed
        );

        info!(
            message = "Import execution finished",
            task_id = task.id,
            processed_items = accumulator.processed,
            success_items = accumulator.success,
            failed_items = accumulator.failed,
            execution_time = ?execution_start.elapsed(),
            total_time = ?total_start.elapsed()
        );

        finalize_task(
            pool,
            task,
            TerminalTaskUpdate {
                status,
                summary,
                processed_items: accumulator.processed,
                success_items: accumulator.success,
                failed_items: accumulator.failed,
                event_data: Some(serde_json::json!({
                    "processed_items": accumulator.processed,
                    "success_items": accumulator.success,
                    "failed_items": accumulator.failed
                })),
            },
        )
        .await?;

        Ok(())
    }
    .instrument(import_span)
    .await
}

async fn finalize_task(
    pool: &DbPool,
    task: &TaskRecord,
    terminal: TerminalTaskUpdate,
) -> Result<(), ApiError> {
    finalize_task_terminal_state(
        pool,
        task.id,
        TaskStateUpdate {
            status: terminal.status,
            summary: Some(terminal.summary.clone()),
            processed_items: terminal.processed_items,
            success_items: terminal.success_items,
            failed_items: terminal.failed_items,
            started_at: task.started_at,
            finished_at: None,
        },
        NewTaskEventRecord {
            task_id: task.id,
            event_type: terminal.status.as_str().to_string(),
            message: terminal.summary.clone(),
            data: terminal.event_data,
        },
    )
    .await?;
    Ok(())
}

pub(super) async fn execute_import_strict(
    pool: &DbPool,
    task_id: i32,
    planned_items: &[PlannedItem],
    accumulator: &mut ExecutionAccumulator,
) -> Result<(), ApiError> {
    let execution = with_transaction(pool, |conn| -> Result<Vec<PlannedTaskResult>, ApiError> {
        let mut runtime = RuntimeState::default();
        let mut completed = Vec::with_capacity(planned_items.len());

        for item in planned_items {
            if let Some(execution) = &item.execution {
                let identifier = item
                    .result
                    .identifier
                    .clone()
                    .unwrap_or_else(|| item.result.entity_kind.clone());
                if let Err(err) = execute_planned_item(conn, &mut runtime, execution) {
                    error!(
                        message = "Import execution failed during strict transaction",
                        identifier = %identifier,
                        error = %err
                    );
                    return Err(err);
                }
            }
            completed.push(item.result.clone());
        }

        Ok(completed)
    });

    match execution {
        Ok(completed) => {
            for result in &completed {
                accumulator.push_success(task_id, result, "succeeded");
                flush_import_result_batches(pool, accumulator, false).await?;
            }
            Ok(())
        }
        Err(err) => Err(err),
    }
}

pub(super) async fn execute_import_best_effort(
    pool: &DbPool,
    task_id: i32,
    planned_items: &[PlannedItem],
    mode: &ImportMode,
    accumulator: &mut ExecutionAccumulator,
) -> Result<(), ApiError> {
    let mut runtime = RuntimeState::default();

    for item in planned_items {
        let result = if let Some(execution) = &item.execution {
            with_transaction(pool, |conn| {
                execute_planned_item(conn, &mut runtime, execution)
            })
            .map(|_| ())
        } else {
            Ok(())
        };

        match result {
            Ok(()) => {
                accumulator.push_success(task_id, &item.result, "succeeded");
                flush_import_result_batches(pool, accumulator, false).await?;
            }
            Err(err) => {
                let sanitized_error = sanitize_error_for_storage(&err);
                accumulator.push_failure(task_id, &item.result, sanitized_error, "failed");
                flush_import_result_batches(pool, accumulator, false).await?;
                if should_abort_best_effort_execution(&err, mode) {
                    warn!(
                        message = "Import best-effort execution aborted early",
                        task_id = task_id,
                        processed_items = accumulator.processed,
                        success_items = accumulator.success,
                        failed_items = accumulator.failed,
                        error = %err
                    );
                    break;
                }
            }
        }
    }

    Ok(())
}

pub(super) fn execute_planned_item(
    conn: &mut diesel::PgConnection,
    runtime: &mut RuntimeState,
    execution: &PlannedExecution,
) -> Result<(), ApiError> {
    match execution {
        PlannedExecution::CreateNamespace(input) => {
            let created = create_namespace_db(conn, input)?;
            if let Some(reference) = &input.ref_ {
                runtime.namespaces_by_ref.insert(reference.clone(), created);
            }
        }
        PlannedExecution::UpdateNamespace {
            namespace_id,
            input,
        } => {
            let updated = update_namespace_db(conn, *namespace_id, input)?;
            if let Some(reference) = &input.ref_ {
                runtime.namespaces_by_ref.insert(reference.clone(), updated);
            }
        }
        PlannedExecution::CreateClass(input) => {
            let namespace = resolve_namespace_runtime(
                conn,
                runtime,
                input.namespace_ref.as_deref(),
                input.namespace_key.as_ref(),
            )?;
            let created = create_class_db(conn, input, namespace.id)?;
            if let Some(reference) = &input.ref_ {
                runtime.classes_by_ref.insert(reference.clone(), created);
            }
        }
        PlannedExecution::UpdateClass { class_id, input } => {
            let updated = update_class_db(conn, *class_id, input)?;
            if let Some(reference) = &input.ref_ {
                runtime.classes_by_ref.insert(reference.clone(), updated);
            }
        }
        PlannedExecution::CreateObject(input) => {
            let class = resolve_class_runtime(
                conn,
                runtime,
                input.class_ref.as_deref(),
                input.class_key.as_ref(),
            )?;
            let created = create_object_db(conn, input, &class)?;
            if let Some(reference) = &input.ref_ {
                runtime.objects_by_ref.insert(reference.clone(), created);
            }
        }
        PlannedExecution::UpdateObject { object_id, input } => {
            let updated = update_object_db(conn, *object_id, input)?;
            if let Some(reference) = &input.ref_ {
                runtime.objects_by_ref.insert(reference.clone(), updated);
            }
        }
        PlannedExecution::CreateClassRelation(input) => {
            let from_class = resolve_class_runtime(
                conn,
                runtime,
                input.from_class_ref.as_deref(),
                input.from_class_key.as_ref(),
            )?;
            let to_class = resolve_class_runtime(
                conn,
                runtime,
                input.to_class_ref.as_deref(),
                input.to_class_key.as_ref(),
            )?;
            create_class_relation_db(
                conn,
                from_class.id,
                to_class.id,
                input.forward_template_alias.clone(),
                input.reverse_template_alias.clone(),
            )?;
        }
        PlannedExecution::CreateObjectRelation(input) => {
            let from_object = resolve_object_runtime(
                conn,
                runtime,
                input.from_object_ref.as_deref(),
                input.from_object_key.as_ref(),
            )?;
            let to_object = resolve_object_runtime(
                conn,
                runtime,
                input.to_object_ref.as_deref(),
                input.to_object_key.as_ref(),
            )?;
            create_object_relation_db(conn, &from_object, &to_object)?;
        }
        PlannedExecution::ApplyNamespacePermissions(input) => {
            let namespace = resolve_namespace_runtime(
                conn,
                runtime,
                input.namespace_ref.as_deref(),
                input.namespace_key.as_ref(),
            )?;
            let group =
                lookup_group_by_name_db(conn, &input.group_key.groupname)?.ok_or_else(|| {
                    ApiError::NotFound(format!("Group '{}' not found", input.group_key.groupname))
                })?;
            apply_permissions_db(
                conn,
                namespace.id,
                group.id,
                &input.permissions,
                input.replace_existing.unwrap_or(false),
            )?;
        }
    }

    Ok(())
}
