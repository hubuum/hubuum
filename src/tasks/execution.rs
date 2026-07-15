use std::time::Instant;

use tracing::{Instrument, error, info, info_span, warn};

use crate::db::traits::task::{TaskBackend, TaskStateUpdate};
use crate::db::{DbPool, with_transaction};
use crate::errors::ApiError;
use crate::models::{
    Collection, EventSinkKey, GroupKey, IdentityScopeKey, ImportAtomicity, ImportCollisionPolicy,
    ImportExportTemplateInput, ImportMode, ImportPermissionPolicy, ImportPrincipalSubtype,
    ImportRequest, NewTaskEventRecord, PrincipalKey, TaskRecord, TaskStatus,
};
use crate::observability::metrics;
use crate::traits::BackendContext;

use super::helpers::{
    flush_import_result_batches, sanitize_error_for_storage, should_abort_best_effort_execution,
};
use super::planning::plan_runtime_admin_import;
use super::resolution::{
    resolve_class_runtime, resolve_collection_parent_runtime, resolve_collection_runtime,
    resolve_object_runtime,
};
use super::types::{
    ExecutionAccumulator, PlannedExecution, PlannedItem, PlannedTaskResult, RuntimeState,
    TerminalTaskUpdate,
};
use crate::db::traits::task_import::{
    apply_permissions_db, create_class_db, create_class_relation_db, create_collection_db,
    create_object_db, create_object_relation_db, load_export_template_sources_db,
    lookup_event_sink_id_by_name_db, lookup_group_by_name_db, lookup_identity_scope_id_by_name_db,
    lookup_principal_id_by_name_db, update_class_db, update_collection_db, update_object_db,
    upsert_event_sink_db, upsert_event_subscription_db, upsert_export_template_db, upsert_group_db,
    upsert_group_membership_db, upsert_identity_scope_db, upsert_principal_db,
    upsert_remote_target_db,
};

async fn resolve_identity_scope_runtime(
    conn: &mut crate::db::DbConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&IdentityScopeKey>,
) -> Result<i32, ApiError> {
    if let Some(reference) = reference
        && let Some(found_id) = runtime.identity_scopes_by_ref.get(reference)
    {
        return Ok(*found_id);
    }
    let name = key.map(|key| key.name.as_str()).ok_or_else(|| {
        ApiError::BadRequest(
            "Identity-scope reference was not resolved and no identity_scope_key was supplied"
                .to_string(),
        )
    })?;
    lookup_identity_scope_id_by_name_db(conn, name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("Identity scope '{name}' not found")))
}

async fn validate_import_template_composition(
    conn: &mut crate::db::DbConnection,
    runtime: &RuntimeState,
    input: &ImportExportTemplateInput,
    collection: &Collection,
) -> Result<(), ApiError> {
    let mut sources = load_export_template_sources_db(conn, collection.id).await?;
    for candidate in &runtime.import_export_templates {
        let candidate_collection = resolve_collection_runtime(
            conn,
            runtime,
            candidate.collection_ref.as_deref(),
            candidate.collection_key.as_ref(),
        )
        .await;
        match candidate_collection {
            Ok(candidate_collection) if candidate_collection.id == collection.id => {
                sources.push((candidate.name.clone(), candidate.template.clone()));
            }
            Ok(_) | Err(ApiError::BadRequest(_) | ApiError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
    }

    input.validate_composition(&sources)
}

async fn resolve_group_runtime(
    conn: &mut crate::db::DbConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&GroupKey>,
) -> Result<i32, ApiError> {
    if let Some(reference) = reference
        && let Some(id) = runtime.groups_by_ref.get(reference)
    {
        return Ok(*id);
    }
    let key = key.ok_or_else(|| {
        ApiError::BadRequest(
            "Group reference was not resolved and no group_key was supplied".to_string(),
        )
    })?;
    let scope = key.identity_scope_name();
    lookup_group_by_name_db(conn, scope, &key.groupname)
        .await?
        .map(|group| group.id)
        .ok_or_else(|| ApiError::NotFound(format!("Group '{scope}/{}' not found", key.groupname)))
}

async fn resolve_principal_runtime(
    conn: &mut crate::db::DbConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&PrincipalKey>,
) -> Result<i32, ApiError> {
    if let Some(reference) = reference
        && let Some(id) = runtime.principals_by_ref.get(reference)
    {
        return Ok(*id);
    }
    let key = key.ok_or_else(|| {
        ApiError::BadRequest(
            "Principal reference was not resolved and no principal_key was supplied".to_string(),
        )
    })?;
    let scope = key.identity_scope_name();
    lookup_principal_id_by_name_db(conn, scope, &key.name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("Principal '{scope}/{}' not found", key.name)))
}

async fn resolve_event_sink_runtime(
    conn: &mut crate::db::DbConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&EventSinkKey>,
) -> Result<i32, ApiError> {
    if let Some(reference) = reference
        && let Some(found_id) = runtime.event_sinks_by_ref.get(reference)
    {
        return Ok(*found_id);
    }
    let name = key.map(|key| key.name.as_str()).ok_or_else(|| {
        ApiError::BadRequest(
            "Event-sink reference was not resolved and no sink_key was supplied".to_string(),
        )
    })?;
    lookup_event_sink_id_by_name_db(conn, name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("Event sink '{name}' not found")))
}

pub(super) async fn execute_import_task<C>(
    backend: &C,
    task: &TaskRecord,
    user: &impl crate::db::traits::authz::AuthzSubject,
) -> Result<(), ApiError>
where
    C: BackendContext + ?Sized,
{
    let pool = backend.db_pool();
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
        let total_start = Instant::now();
        let planning_start = Instant::now();
        let planning = plan_runtime_admin_import(backend, user, &request)
            .instrument(info_span!("import_planning"))
            .await;
        let planning_time = planning_start.elapsed();
        metrics::import_phase_duration("planning", planning_time);

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
            metrics::import_items(failed_count, 0, failed_count);
            metrics::import_phase_duration("total", total_start.elapsed());
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
        }
        .append(pool)
        .await?;

        task.update_state(
            pool,
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

        let execution_time = execution_start.elapsed();
        metrics::import_phase_duration("execution", execution_time);
        info!(
            message = "Import execution finished",
            task_id = task.id,
            processed_items = accumulator.processed,
            success_items = accumulator.success,
            failed_items = accumulator.failed,
            execution_time = ?execution_time,
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
        metrics::import_items(
            accumulator.processed,
            accumulator.success,
            accumulator.failed,
        );
        metrics::import_phase_duration("total", total_start.elapsed());

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
    task.finalize_terminal(
        pool,
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
    let execution = with_transaction(
        pool,
        async |conn| -> Result<Vec<PlannedTaskResult>, ApiError> {
            let mut runtime = RuntimeState::for_planned_items(planned_items);
            let mut completed = Vec::with_capacity(planned_items.len());

            for item in planned_items {
                if let Some(execution) = &item.execution {
                    let identifier = item
                        .result
                        .identifier
                        .clone()
                        .unwrap_or_else(|| item.result.entity_kind.clone());
                    if let Err(err) = execute_planned_item(conn, &mut runtime, execution).await {
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
        },
    )
    .await;

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
    let mut runtime = RuntimeState::for_planned_items(planned_items);

    for item in planned_items {
        let result = if let Some(execution) = &item.execution {
            with_transaction(pool, async |conn| {
                execute_planned_item(conn, &mut runtime, execution).await
            })
            .await
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

pub(super) async fn execute_planned_item(
    conn: &mut crate::db::DbConnection,
    runtime: &mut RuntimeState,
    execution: &PlannedExecution,
) -> Result<(), ApiError> {
    match execution {
        PlannedExecution::UpsertIdentityScope { input, overwrite } => {
            let id = upsert_identity_scope_db(conn, input, *overwrite).await?;
            if let Some(reference) = &input.ref_ {
                runtime.identity_scopes_by_ref.insert(reference.clone(), id);
            }
        }
        PlannedExecution::UpsertGroup { input, overwrite } => {
            let scope_id = resolve_identity_scope_runtime(
                conn,
                runtime,
                input.identity_scope_ref.as_deref(),
                input.identity_scope_key.as_ref(),
            )
            .await?;
            let id = upsert_group_db(conn, input, scope_id, *overwrite).await?;
            if let Some(reference) = &input.ref_ {
                runtime.groups_by_ref.insert(reference.clone(), id);
            }
        }
        PlannedExecution::UpsertPrincipal { input, overwrite } => {
            let scope_id = resolve_identity_scope_runtime(
                conn,
                runtime,
                input.identity_scope_ref.as_deref(),
                input.identity_scope_key.as_ref(),
            )
            .await?;
            let (owner_group_id, created_by) = match &input.subtype {
                ImportPrincipalSubtype::Human { .. } => (None, None),
                ImportPrincipalSubtype::ServiceAccount {
                    owner_group_ref,
                    owner_group_key,
                    created_by_ref,
                    created_by_key,
                    ..
                } => (
                    Some(
                        resolve_group_runtime(
                            conn,
                            runtime,
                            owner_group_ref.as_deref(),
                            owner_group_key.as_ref(),
                        )
                        .await?,
                    ),
                    if created_by_ref.is_some() || created_by_key.is_some() {
                        Some(
                            resolve_principal_runtime(
                                conn,
                                runtime,
                                created_by_ref.as_deref(),
                                created_by_key.as_ref(),
                            )
                            .await?,
                        )
                    } else {
                        None
                    },
                ),
            };
            let id = upsert_principal_db(
                conn,
                input,
                scope_id,
                owner_group_id,
                created_by,
                *overwrite,
            )
            .await?;
            if let Some(reference) = &input.ref_ {
                runtime.principals_by_ref.insert(reference.clone(), id);
            }
        }
        PlannedExecution::UpsertGroupMembership { input, overwrite } => {
            let principal_id = resolve_principal_runtime(
                conn,
                runtime,
                input.principal_ref.as_deref(),
                input.principal_key.as_ref(),
            )
            .await?;
            let group_id = resolve_group_runtime(
                conn,
                runtime,
                input.group_ref.as_deref(),
                input.group_key.as_ref(),
            )
            .await?;
            let mut source_scope_ids = Vec::with_capacity(input.sources.len());
            for source in &input.sources {
                source_scope_ids.push(
                    resolve_identity_scope_runtime(
                        conn,
                        runtime,
                        source.source_scope_ref.as_deref(),
                        source.source_scope_key.as_ref(),
                    )
                    .await?,
                );
            }
            upsert_group_membership_db(
                conn,
                input,
                principal_id,
                group_id,
                &source_scope_ids,
                *overwrite,
            )
            .await?;
        }
        PlannedExecution::CreateCollection(input) => {
            let parent = resolve_collection_parent_runtime(conn, runtime, input).await?;
            let created = create_collection_db(conn, input, Some(parent.id)).await?;
            if let Some(reference) = &input.ref_ {
                runtime
                    .collections_by_ref
                    .insert(reference.clone(), created);
            }
        }
        PlannedExecution::UpdateCollection {
            collection_id,
            input,
        } => {
            let updated = update_collection_db(conn, *collection_id, input).await?;
            if let Some(reference) = &input.ref_ {
                runtime
                    .collections_by_ref
                    .insert(reference.clone(), updated);
            }
        }
        PlannedExecution::CreateClass(input) => {
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            let created = create_class_db(conn, input, collection.id).await?;
            if let Some(reference) = &input.ref_ {
                runtime.classes_by_ref.insert(reference.clone(), created);
            }
        }
        PlannedExecution::UpdateClass { class_id, input } => {
            let updated = update_class_db(conn, *class_id, input).await?;
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
            )
            .await?;
            let created = create_object_db(conn, input, &class).await?;
            if let Some(reference) = &input.ref_ {
                runtime.objects_by_ref.insert(reference.clone(), created);
            }
        }
        PlannedExecution::UpdateObject { object_id, input } => {
            let updated = update_object_db(conn, *object_id, input).await?;
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
            )
            .await?;
            let to_class = resolve_class_runtime(
                conn,
                runtime,
                input.to_class_ref.as_deref(),
                input.to_class_key.as_ref(),
            )
            .await?;
            create_class_relation_db(
                conn,
                from_class.id,
                to_class.id,
                input.forward_template_alias.clone(),
                input.reverse_template_alias.clone(),
            )
            .await?;
        }
        PlannedExecution::CreateObjectRelation(input) => {
            let from_object = resolve_object_runtime(
                conn,
                runtime,
                input.from_object_ref.as_deref(),
                input.from_object_key.as_ref(),
            )
            .await?;
            let to_object = resolve_object_runtime(
                conn,
                runtime,
                input.to_object_ref.as_deref(),
                input.to_object_key.as_ref(),
            )
            .await?;
            create_object_relation_db(conn, &from_object, &to_object).await?;
        }
        PlannedExecution::ApplyCollectionPermissions(input) => {
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            let identity_scope = input.group_key.identity_scope_name();
            let group = lookup_group_by_name_db(conn, identity_scope, &input.group_key.groupname)
                .await?
                .ok_or_else(|| {
                    ApiError::NotFound(format!(
                        "Group '{}/{}' not found",
                        identity_scope, input.group_key.groupname
                    ))
                })?;
            apply_permissions_db(
                conn,
                collection.id,
                group.id,
                &input.permissions,
                input.replace_existing.unwrap_or(false),
            )
            .await?;
        }
        PlannedExecution::UpsertExportTemplate { input, overwrite } => {
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            let class_id = if input.class_ref.is_some() || input.class_key.is_some() {
                let class = resolve_class_runtime(
                    conn,
                    runtime,
                    input.class_ref.as_deref(),
                    input.class_key.as_ref(),
                )
                .await?;
                class.ensure_in_collection(collection.id, "Export template")?;
                Some(class.id)
            } else {
                None
            };
            validate_import_template_composition(conn, runtime, input, &collection).await?;
            upsert_export_template_db(conn, input, collection.id, class_id, *overwrite).await?;
        }
        PlannedExecution::UpsertRemoteTarget { input, overwrite } => {
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            let class_id = if input.class_ref.is_some() || input.class_key.is_some() {
                let class = resolve_class_runtime(
                    conn,
                    runtime,
                    input.class_ref.as_deref(),
                    input.class_key.as_ref(),
                )
                .await?;
                class.ensure_in_collection(collection.id, "Remote target")?;
                Some(class.id)
            } else {
                None
            };
            upsert_remote_target_db(conn, input, collection.id, class_id, *overwrite).await?;
        }
        PlannedExecution::UpsertEventSink { input, overwrite } => {
            let id = upsert_event_sink_db(conn, input, *overwrite).await?;
            if let Some(reference) = &input.ref_ {
                runtime.event_sinks_by_ref.insert(reference.clone(), id);
            }
        }
        PlannedExecution::UpsertEventSubscription { input, overwrite } => {
            let collection = resolve_collection_runtime(
                conn,
                runtime,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            )
            .await?;
            let sink_id = resolve_event_sink_runtime(
                conn,
                runtime,
                input.sink_ref.as_deref(),
                input.sink_key.as_ref(),
            )
            .await?;
            upsert_event_subscription_db(conn, input, collection.id, sink_id, *overwrite).await?;
        }
    }

    Ok(())
}
