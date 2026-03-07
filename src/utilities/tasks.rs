use std::collections::{HashMap, HashSet};
use std::sync::Once;
use std::time::Duration;

use actix_rt::time::sleep;
use chrono::Utc;
use diesel::prelude::*;
use sha2::{Digest, Sha256};
use tracing::error;

use crate::config::{DEFAULT_TASK_POLL_INTERVAL_MS, get_config};
use crate::db::traits::UserPermissions;
use crate::db::traits::task::{
    append_task_event, claim_next_queued_task, insert_import_results, redact_task_payload,
    update_task_state, TaskStateUpdate,
};
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::models::{
    ClassKey, Group, HubuumClass, HubuumClassRelation, HubuumObject, HubuumObjectRelation,
    ImportAtomicity, ImportClassInput, ImportClassRelationInput, ImportCollisionPolicy, ImportMode,
    ImportNamespaceInput, ImportNamespacePermissionInput, ImportObjectInput,
    ImportObjectRelationInput, ImportPermissionPolicy, ImportRequest, Namespace, NamespaceID,
    NamespaceKey, NewHubuumClass, NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation,
    NewImportTaskResultRecord, NewPermission, NewTaskEventRecord, ObjectKey, Permission,
    Permissions, PermissionsList, TaskKind, TaskRecord, TaskStatus, UpdateHubuumClass,
    UpdateHubuumObject, UpdateNamespace, UpdatePermission, User, UserID,
};
use crate::traits::GroupMemberships;

static TASK_WORKER: Once = Once::new();
const DRY_RUN_SENTINEL: &str = "__hubuum_import_dry_run__";

#[derive(Clone)]
struct NamespaceResolution {
    id: i32,
    name: String,
    description: String,
    exists_in_db: bool,
}

#[derive(Clone)]
struct ClassResolution {
    id: i32,
    name: String,
    namespace_id: i32,
    json_schema: Option<serde_json::Value>,
    validate_schema: bool,
}

#[derive(Clone)]
struct ObjectResolution {
    id: i32,
    name: String,
    namespace_id: i32,
    class_id: i32,
}

#[derive(Default)]
struct PlanningState {
    next_temp_id: i32,
    namespaces_by_ref: HashMap<String, NamespaceResolution>,
    namespaces_by_name: HashMap<String, NamespaceResolution>,
    classes_by_ref: HashMap<String, ClassResolution>,
    classes_by_key: HashMap<(i32, String), ClassResolution>,
    objects_by_ref: HashMap<String, ObjectResolution>,
    objects_by_key: HashMap<(i32, String), ObjectResolution>,
    class_relations: HashSet<(i32, i32)>,
    object_relations: HashSet<(i32, i32)>,
}

impl PlanningState {
    fn new() -> Self {
        Self {
            next_temp_id: -1,
            ..Self::default()
        }
    }

    fn next_virtual_id(&mut self) -> i32 {
        let id = self.next_temp_id;
        self.next_temp_id -= 1;
        id
    }
}

#[derive(Default)]
struct RuntimeState {
    namespaces_by_ref: HashMap<String, Namespace>,
    classes_by_ref: HashMap<String, HubuumClass>,
    objects_by_ref: HashMap<String, HubuumObject>,
}

#[derive(Clone)]
enum PlannedExecution {
    CreateNamespace(ImportNamespaceInput),
    UpdateNamespace {
        namespace_id: i32,
        input: ImportNamespaceInput,
    },
    CreateClass(ImportClassInput),
    UpdateClass {
        class_id: i32,
        input: ImportClassInput,
    },
    CreateObject(ImportObjectInput),
    UpdateObject {
        object_id: i32,
        input: ImportObjectInput,
    },
    CreateClassRelation(ImportClassRelationInput),
    CreateObjectRelation(ImportObjectRelationInput),
    ApplyNamespacePermissions(ImportNamespacePermissionInput),
}

#[derive(Clone)]
struct PlannedTaskResult {
    item_ref: Option<String>,
    entity_kind: String,
    action: String,
    identifier: Option<String>,
    details: Option<serde_json::Value>,
}

#[derive(Clone)]
struct PlannedItem {
    result: PlannedTaskResult,
    execution: Option<PlannedExecution>,
}

#[derive(Default)]
struct ExecutionAccumulator {
    results: Vec<NewImportTaskResultRecord>,
    processed: i32,
    success: i32,
    failed: i32,
}

impl ExecutionAccumulator {
    fn push_success(&mut self, task_id: i32, planned: &PlannedTaskResult, outcome: &str) {
        self.processed += 1;
        self.success += 1;
        self.results.push(NewImportTaskResultRecord {
            task_id,
            item_ref: planned.item_ref.clone(),
            entity_kind: planned.entity_kind.clone(),
            action: planned.action.clone(),
            identifier: planned.identifier.clone(),
            outcome: outcome.to_string(),
            error: None,
            details: planned.details.clone(),
        });
    }

    fn push_failure(
        &mut self,
        task_id: i32,
        planned: &PlannedTaskResult,
        error: impl Into<String>,
        outcome: &str,
    ) {
        self.processed += 1;
        self.failed += 1;
        self.results.push(NewImportTaskResultRecord {
            task_id,
            item_ref: planned.item_ref.clone(),
            entity_kind: planned.entity_kind.clone(),
            action: planned.action.clone(),
            identifier: planned.identifier.clone(),
            outcome: outcome.to_string(),
            error: Some(error.into()),
            details: planned.details.clone(),
        });
    }
}

#[derive(Clone, Copy)]
enum FailureKind {
    Permission,
    Collision,
    Validation,
    Resolution,
    Runtime,
}

struct PlanningFailure {
    kind: FailureKind,
    item: PlannedTaskResult,
    message: String,
}

#[derive(Default)]
struct PlanningOutcome {
    planned_items: Vec<PlannedItem>,
    failures: Vec<PlanningFailure>,
    aborted: bool,
}

impl PlanningFailure {
    fn into_result(self, task_id: i32) -> NewImportTaskResultRecord {
        NewImportTaskResultRecord {
            task_id,
            item_ref: self.item.item_ref,
            entity_kind: self.item.entity_kind,
            action: self.item.action,
            identifier: self.item.identifier,
            outcome: "failed".to_string(),
            error: Some(self.message),
            details: self.item.details,
        }
    }
}

pub fn request_hash(payload: &serde_json::Value) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(payload)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn configured_task_worker_count() -> usize {
    get_config()
        .map(|config| config.task_workers)
        .unwrap_or(1)
}

fn configured_task_poll_interval() -> Duration {
    let interval_ms = get_config()
        .map(|config| config.task_poll_interval_ms)
        .unwrap_or(DEFAULT_TASK_POLL_INTERVAL_MS);
    Duration::from_millis(interval_ms)
}

fn spawn_task_worker_loop(pool: DbPool, poll_interval: Duration) {
    actix_rt::spawn(async move {
        loop {
            match process_one_task(&pool).await {
                Ok(true) => {}
                Ok(false) => {}
                Err(err) => error!(message = "Task worker iteration failed", error = %err),
            }
            sleep(poll_interval).await;
        }
    });
}

pub fn ensure_task_worker_running(pool: DbPool) {
    let worker_count = configured_task_worker_count();
    let poll_interval = configured_task_poll_interval();
    TASK_WORKER.call_once(move || {
        for _ in 0..worker_count {
            spawn_task_worker_loop(pool.clone(), poll_interval);
        }
    });
}

pub fn kick_task_worker(pool: DbPool) {
    actix_rt::spawn(async move {
        loop {
            match process_one_task(&pool).await {
                Ok(true) => continue,
                Ok(false) => break,
                Err(err) => {
                    error!(message = "Task worker iteration failed", error = %err);
                    break;
                }
            }
        }
    });
}

async fn process_one_task(pool: &DbPool) -> Result<bool, ApiError> {
    let Some(task) = claim_next_queued_task(pool).await? else {
        return Ok(false);
    };

    append_task_event(
        pool,
        NewTaskEventRecord {
            task_id: task.id,
            event_type: "validating".to_string(),
            message: "Task claimed for validation".to_string(),
            data: None,
        },
    )
    .await?;

    let submitted_by = UserID(task.submitted_by).user(pool).await?;

    let outcome = match TaskKind::from_db(&task.kind)? {
        TaskKind::Import => execute_import_task(pool, &task, &submitted_by).await,
        other => Err(ApiError::BadRequest(format!(
            "Task kind '{}' is not implemented",
            other.as_str()
        ))),
    };

    match outcome {
        Ok(()) => {}
        Err(err) => {
            let finished_at = Utc::now().naive_utc();
            update_task_state(
                pool,
                task.id,
                TaskStateUpdate {
                    status: TaskStatus::Failed,
                    summary: Some(err.to_string()),
                    processed_items: task.total_items,
                    success_items: 0,
                    failed_items: task.total_items.max(1),
                    started_at: task.started_at,
                    finished_at: Some(finished_at),
                },
            )
            .await?;
            append_task_event(
                pool,
                NewTaskEventRecord {
                    task_id: task.id,
                    event_type: "failed".to_string(),
                    message: "Task failed".to_string(),
                    data: Some(serde_json::json!({ "error": err.to_string() })),
                },
            )
            .await?;
            redact_task_payload(pool, task.id).await?;
        }
    }

    Ok(true)
}

async fn execute_import_task(
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
    let planning = plan_import(pool, user, &request).await;

    let mut accumulator = ExecutionAccumulator::default();
    let finished_at = Utc::now().naive_utc();

    if !planning.failures.is_empty()
        && (matches!(mode.atomicity.unwrap_or(ImportAtomicity::Strict), ImportAtomicity::Strict)
            || planning.aborted)
    {
        let results = planning
            .failures
            .into_iter()
            .map(|failure| failure.into_result(task.id))
            .collect::<Vec<_>>();
        let failed_count = results.len() as i32;
        insert_import_results(pool, &results).await?;
        let summary = format!("Import validation failed for {failed_count} item(s)");
        update_task_state(
            pool,
            task.id,
            TaskStateUpdate {
                status: TaskStatus::Failed,
                summary: Some(summary.clone()),
                processed_items: failed_count,
                success_items: 0,
                failed_items: failed_count,
                started_at: task.started_at,
                finished_at: Some(finished_at),
            },
        )
        .await?;
        append_task_event(
            pool,
            NewTaskEventRecord {
                task_id: task.id,
                event_type: "failed".to_string(),
                message: summary,
                data: None,
            },
        )
        .await?;
        redact_task_payload(pool, task.id).await?;
        return Ok(());
    }

    let PlanningOutcome {
        planned_items,
        failures,
        aborted: _,
    } = planning;

    {
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

            if request.dry_run() {
                for failure in failures {
                    accumulator.push_failure(task.id, &failure.item, failure.message, "failed");
                }
                for item in &planned_items {
                    accumulator.push_success(task.id, &item.result, "planned");
                }
            } else {
                for failure in failures {
                    accumulator.push_failure(task.id, &failure.item, failure.message, "failed");
                }
                match mode.atomicity.unwrap_or(ImportAtomicity::Strict) {
                    ImportAtomicity::Strict => {
                        execute_import_strict(pool, task.id, &planned_items, &mut accumulator)
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
                        .await?;
                    }
                }
            }

            insert_import_results(pool, &accumulator.results).await?;

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

            update_task_state(
                pool,
                task.id,
                TaskStateUpdate {
                    status,
                    summary: Some(summary.clone()),
                    processed_items: accumulator.processed,
                    success_items: accumulator.success,
                    failed_items: accumulator.failed,
                    started_at: task.started_at,
                    finished_at: Some(finished_at),
                },
            )
            .await?;
            append_task_event(
                pool,
                NewTaskEventRecord {
                    task_id: task.id,
                    event_type: status.as_str().to_string(),
                    message: summary,
                    data: Some(serde_json::json!({
                        "processed_items": accumulator.processed,
                        "success_items": accumulator.success,
                        "failed_items": accumulator.failed
                    })),
                },
            )
            .await?;
            redact_task_payload(pool, task.id).await?;
    }

    Ok(())
}

async fn execute_import_strict(
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
                execute_planned_item(conn, &mut runtime, execution).map_err(|err| {
                    ApiError::BadRequest(format!(
                        "Import execution failed for {}: {}",
                        item.result
                            .identifier
                            .clone()
                            .unwrap_or_else(|| item.result.entity_kind.clone()),
                        err
                    ))
                })?;
            }
            completed.push(item.result.clone());
        }

        Ok(completed)
    });

    match execution {
        Ok(completed) => {
            for result in &completed {
                accumulator.push_success(task_id, result, "succeeded");
            }
            Ok(())
        }
        Err(err) if err.to_string() == DRY_RUN_SENTINEL => Ok(()),
        Err(err) => Err(err),
    }
}

async fn execute_import_best_effort(
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
            Ok(()) => accumulator.push_success(task_id, &item.result, "succeeded"),
            Err(err) => {
                accumulator.push_failure(task_id, &item.result, err.to_string(), "failed");
                if matches!(mode.permission_policy, Some(ImportPermissionPolicy::Abort))
                    || matches!(mode.collision_policy, Some(ImportCollisionPolicy::Abort))
                {
                    break;
                }
            }
        }
    }

    Ok(())
}

fn execute_planned_item(
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
            update_namespace_db(conn, *namespace_id, input)?;
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
            update_class_db(conn, *class_id, input)?;
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
            update_object_db(conn, *object_id, input)?;
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
            create_class_relation_db(conn, from_class.id, to_class.id)?;
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

async fn plan_import(
    pool: &DbPool,
    user: &User,
    request: &ImportRequest,
) -> PlanningOutcome {
    let mode = request.mode();
    let mut state = PlanningState::new();
    let mut planned_items = Vec::with_capacity(request.total_items() as usize);
    let mut failures = Vec::new();
    let mut aborted = false;

    macro_rules! push_or_stop {
        ($expr:expr) => {{
            match $expr.await {
                Ok(item) => planned_items.push(item),
                Err(failure) => {
                    let stop = should_abort_import(
                        mode.atomicity.unwrap_or(ImportAtomicity::Strict),
                        mode.permission_policy
                            .unwrap_or(ImportPermissionPolicy::Abort),
                        mode.collision_policy
                            .unwrap_or(ImportCollisionPolicy::Abort),
                        failure.kind,
                    );
                    failures.push(failure);
                    if stop {
                        aborted = true;
                        break;
                    }
                }
            }
        }};
    }

    for namespace in &request.graph.namespaces {
        push_or_stop!(plan_namespace(pool, user, &mode, &mut state, namespace));
    }
    if aborted {
        return PlanningOutcome {
            planned_items,
            failures,
            aborted,
        };
    }
    for class in &request.graph.classes {
        push_or_stop!(plan_class(pool, user, &mode, &mut state, class));
    }
    if aborted {
        return PlanningOutcome {
            planned_items,
            failures,
            aborted,
        };
    }
    for object in &request.graph.objects {
        push_or_stop!(plan_object(pool, user, &mode, &mut state, object));
    }
    if aborted {
        return PlanningOutcome {
            planned_items,
            failures,
            aborted,
        };
    }
    for relation in &request.graph.class_relations {
        push_or_stop!(plan_class_relation(pool, user, &mode, &mut state, relation));
    }
    if aborted {
        return PlanningOutcome {
            planned_items,
            failures,
            aborted,
        };
    }
    for relation in &request.graph.object_relations {
        push_or_stop!(plan_object_relation(
            pool, user, &mode, &mut state, relation
        ));
    }
    if aborted {
        return PlanningOutcome {
            planned_items,
            failures,
            aborted,
        };
    }
    for acl in &request.graph.namespace_permissions {
        push_or_stop!(plan_namespace_permission(
            pool, user, &mode, &mut state, acl
        ));
    }

    PlanningOutcome {
        planned_items,
        failures,
        aborted,
    }
}

fn should_abort_import(
    atomicity: ImportAtomicity,
    permission_policy: ImportPermissionPolicy,
    collision_policy: ImportCollisionPolicy,
    kind: FailureKind,
) -> bool {
    if matches!(atomicity, ImportAtomicity::Strict) {
        return true;
    }

    match kind {
        FailureKind::Permission => matches!(permission_policy, ImportPermissionPolicy::Abort),
        FailureKind::Collision => matches!(collision_policy, ImportCollisionPolicy::Abort),
        FailureKind::Validation | FailureKind::Resolution | FailureKind::Runtime => false,
    }
}

async fn plan_namespace(
    pool: &DbPool,
    user: &User,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportNamespaceInput,
) -> Result<PlannedItem, PlanningFailure> {
    if let Some(reference) = &input.ref_
        && state.namespaces_by_ref.contains_key(reference)
    {
        return Err(PlanningFailure {
            kind: FailureKind::Validation,
            item: planned_result(
                "namespace",
                "create",
                input.ref_.clone(),
                Some(input.name.clone()),
            ),
            message: format!("Duplicate namespace ref '{reference}'"),
        });
    }

    let existing = state
        .namespaces_by_name
        .get(&input.name)
        .cloned()
        .map(|ns| Namespace {
            id: ns.id,
            name: ns.name,
            description: ns.description,
            created_at: Utc::now().naive_utc(),
            updated_at: Utc::now().naive_utc(),
        })
        .or(lookup_namespace_by_name(pool, &input.name)
            .await
            .map_err(|message| PlanningFailure {
                kind: FailureKind::Runtime,
                item: planned_result(
                    "namespace",
                    "lookup",
                    input.ref_.clone(),
                    Some(input.name.clone()),
                ),
                message: message.to_string(),
            })?);

    if let Some(namespace) = existing {
        ensure_namespace_permission(
            pool,
            user,
            &NamespaceResolution {
                id: namespace.id,
                name: namespace.name.clone(),
                description: namespace.description.clone(),
                exists_in_db: true,
            },
            Permissions::UpdateCollection,
        )
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Permission,
            item: planned_result(
                "namespace",
                "update",
                input.ref_.clone(),
                Some(namespace.name.clone()),
            ),
            message,
        })?;

        if matches!(mode.collision_policy, Some(ImportCollisionPolicy::Abort)) {
            return Err(PlanningFailure {
                kind: FailureKind::Collision,
                item: planned_result(
                    "namespace",
                    "update",
                    input.ref_.clone(),
                    Some(namespace.name),
                ),
                message: format!("Namespace '{}' already exists", input.name),
            });
        }

        let resolution = NamespaceResolution {
            id: namespace.id,
            name: namespace.name.clone(),
            description: input.description.clone(),
            exists_in_db: true,
        };
        remember_namespace(state, input.ref_.clone(), resolution.clone());

        Ok(PlannedItem {
            result: planned_result(
                "namespace",
                "update",
                input.ref_.clone(),
                Some(identifier_namespace(&resolution)),
            ),
            execution: Some(PlannedExecution::UpdateNamespace {
                namespace_id: namespace.id,
                input: input.clone(),
            }),
        })
    } else {
        if !user.is_admin(pool).await.map_err(|err| PlanningFailure {
            kind: FailureKind::Permission,
            item: planned_result(
                "namespace",
                "create",
                input.ref_.clone(),
                Some(input.name.clone()),
            ),
            message: err.to_string(),
        })? {
            return Err(PlanningFailure {
                kind: FailureKind::Permission,
                item: planned_result(
                    "namespace",
                    "create",
                    input.ref_.clone(),
                    Some(input.name.clone()),
                ),
                message: "Only admins may create namespaces".to_string(),
            });
        }

        let resolution = NamespaceResolution {
            id: state.next_virtual_id(),
            name: input.name.clone(),
            description: input.description.clone(),
            exists_in_db: false,
        };
        remember_namespace(state, input.ref_.clone(), resolution.clone());

        Ok(PlannedItem {
            result: planned_result(
                "namespace",
                "create",
                input.ref_.clone(),
                Some(identifier_namespace(&resolution)),
            ),
            execution: Some(PlannedExecution::CreateNamespace(input.clone())),
        })
    }
}

async fn plan_class(
    pool: &DbPool,
    user: &User,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportClassInput,
) -> Result<PlannedItem, PlanningFailure> {
    let namespace = resolve_namespace_planning(
        pool,
        state,
        input.namespace_ref.as_deref(),
        input.namespace_key.as_ref(),
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Resolution,
        item: planned_result(
            "class",
            "resolve",
            input.ref_.clone(),
            Some(input.name.clone()),
        ),
        message,
    })?;

    let existing = state
        .classes_by_key
        .get(&(namespace.id, input.name.clone()))
        .cloned()
        .or(
            lookup_class_by_namespace_and_name(pool, namespace.id, &input.name)
                .await
                .map_err(|err| PlanningFailure {
                    kind: FailureKind::Runtime,
                    item: planned_result(
                        "class",
                        "lookup",
                        input.ref_.clone(),
                        Some(input.name.clone()),
                    ),
                    message: err.to_string(),
                })?
                .map(class_to_resolution),
        );

    let identifier = format!("{}::{}", namespace.name, input.name);

    if let Some(class) = existing {
        ensure_namespace_permission(pool, user, &namespace, Permissions::UpdateClass)
            .await
            .map_err(|message| PlanningFailure {
                kind: FailureKind::Permission,
                item: planned_result(
                    "class",
                    "update",
                    input.ref_.clone(),
                    Some(identifier.clone()),
                ),
                message,
            })?;

        if matches!(mode.collision_policy, Some(ImportCollisionPolicy::Abort)) {
            return Err(PlanningFailure {
                kind: FailureKind::Collision,
                item: planned_result("class", "update", input.ref_.clone(), Some(identifier)),
                message: format!(
                    "Class '{}' already exists in namespace '{}'",
                    input.name, namespace.name
                ),
            });
        }

        let updated = ClassResolution {
            id: class.id,
            name: input.name.clone(),
            namespace_id: namespace.id,
            json_schema: input.json_schema.clone(),
            validate_schema: input.validate_schema.unwrap_or(false),
        };
        remember_class(state, input.ref_.clone(), updated.clone());

        Ok(PlannedItem {
            result: planned_result(
                "class",
                "update",
                input.ref_.clone(),
                Some(format!("{}::{}", namespace.name, input.name)),
            ),
            execution: Some(PlannedExecution::UpdateClass {
                class_id: class.id,
                input: input.clone(),
            }),
        })
    } else {
        ensure_namespace_permission(pool, user, &namespace, Permissions::CreateClass)
            .await
            .map_err(|message| PlanningFailure {
                kind: FailureKind::Permission,
                item: planned_result(
                    "class",
                    "create",
                    input.ref_.clone(),
                    Some(identifier.clone()),
                ),
                message,
            })?;

        let created = ClassResolution {
            id: state.next_virtual_id(),
            name: input.name.clone(),
            namespace_id: namespace.id,
            json_schema: input.json_schema.clone(),
            validate_schema: input.validate_schema.unwrap_or(false),
        };
        remember_class(state, input.ref_.clone(), created.clone());

        Ok(PlannedItem {
            result: planned_result("class", "create", input.ref_.clone(), Some(identifier)),
            execution: Some(PlannedExecution::CreateClass(input.clone())),
        })
    }
}

async fn plan_object(
    pool: &DbPool,
    user: &User,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportObjectInput,
) -> Result<PlannedItem, PlanningFailure> {
    let class = resolve_class_planning(
        pool,
        state,
        input.class_ref.as_deref(),
        input.class_key.as_ref(),
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Resolution,
        item: planned_result(
            "object",
            "resolve",
            input.ref_.clone(),
            Some(input.name.clone()),
        ),
        message,
    })?;

    if class.validate_schema
        && let Some(schema) = &class.json_schema
    {
        jsonschema::validate(schema, &input.data).map_err(|err| PlanningFailure {
            kind: FailureKind::Validation,
            item: planned_result(
                "object",
                "validate",
                input.ref_.clone(),
                Some(format!("{}::{}", class.name, input.name)),
            ),
            message: err.to_string(),
        })?;
    }

    let existing = state
        .objects_by_key
        .get(&(class.id, input.name.clone()))
        .cloned()
        .or(lookup_object_by_class_and_name(pool, class.id, &input.name)
            .await
            .map_err(|err| PlanningFailure {
                kind: FailureKind::Runtime,
                item: planned_result(
                    "object",
                    "lookup",
                    input.ref_.clone(),
                    Some(input.name.clone()),
                ),
                message: err.to_string(),
            })?
            .map(object_to_resolution));

    let identifier = format!("{}::{}", class.name, input.name);
    let namespace = resolve_namespace_by_id_planning(pool, state, class.namespace_id)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result(
                "object",
                "resolve",
                input.ref_.clone(),
                Some(identifier.clone()),
            ),
            message,
        })?;

    if let Some(object) = existing {
        ensure_namespace_permission(pool, user, &namespace, Permissions::UpdateObject)
            .await
            .map_err(|message| PlanningFailure {
                kind: FailureKind::Permission,
                item: planned_result(
                    "object",
                    "update",
                    input.ref_.clone(),
                    Some(identifier.clone()),
                ),
                message,
            })?;

        if matches!(mode.collision_policy, Some(ImportCollisionPolicy::Abort)) {
            return Err(PlanningFailure {
                kind: FailureKind::Collision,
                item: planned_result("object", "update", input.ref_.clone(), Some(identifier)),
                message: format!(
                    "Object '{}' already exists in class '{}'",
                    input.name, class.name
                ),
            });
        }

        let updated = ObjectResolution {
            id: object.id,
            name: input.name.clone(),
            namespace_id: namespace.id,
            class_id: class.id,
        };
        remember_object(state, input.ref_.clone(), updated.clone());

        Ok(PlannedItem {
            result: planned_result(
                "object",
                "update",
                input.ref_.clone(),
                Some(format!("{}::{}", class.name, input.name)),
            ),
            execution: Some(PlannedExecution::UpdateObject {
                object_id: object.id,
                input: input.clone(),
            }),
        })
    } else {
        ensure_namespace_permission(pool, user, &namespace, Permissions::CreateObject)
            .await
            .map_err(|message| PlanningFailure {
                kind: FailureKind::Permission,
                item: planned_result(
                    "object",
                    "create",
                    input.ref_.clone(),
                    Some(identifier.clone()),
                ),
                message,
            })?;

        let created = ObjectResolution {
            id: state.next_virtual_id(),
            name: input.name.clone(),
            namespace_id: namespace.id,
            class_id: class.id,
        };
        remember_object(state, input.ref_.clone(), created.clone());

        Ok(PlannedItem {
            result: planned_result("object", "create", input.ref_.clone(), Some(identifier)),
            execution: Some(PlannedExecution::CreateObject(input.clone())),
        })
    }
}

async fn plan_class_relation(
    pool: &DbPool,
    user: &User,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportClassRelationInput,
) -> Result<PlannedItem, PlanningFailure> {
    let from_class = resolve_class_planning(
        pool,
        state,
        input.from_class_ref.as_deref(),
        input.from_class_key.as_ref(),
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Resolution,
        item: planned_result("class_relation", "create", input.ref_.clone(), None),
        message,
    })?;
    let to_class = resolve_class_planning(
        pool,
        state,
        input.to_class_ref.as_deref(),
        input.to_class_key.as_ref(),
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Resolution,
        item: planned_result("class_relation", "create", input.ref_.clone(), None),
        message,
    })?;
    let pair = normalize_pair(from_class.id, to_class.id);
    let identifier = Some(format!("{}<->{}", from_class.name, to_class.name));

    let from_namespace = resolve_namespace_by_id_planning(pool, state, from_class.namespace_id)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result(
                "class_relation",
                "create",
                input.ref_.clone(),
                identifier.clone(),
            ),
            message,
        })?;
    let to_namespace = resolve_namespace_by_id_planning(pool, state, to_class.namespace_id)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result(
                "class_relation",
                "create",
                input.ref_.clone(),
                identifier.clone(),
            ),
            message,
        })?;

    ensure_namespace_permission(
        pool,
        user,
        &from_namespace,
        Permissions::CreateClassRelation,
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Permission,
        item: planned_result(
            "class_relation",
            "create",
            input.ref_.clone(),
            identifier.clone(),
        ),
        message,
    })?;
    ensure_namespace_permission(pool, user, &to_namespace, Permissions::CreateClassRelation)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Permission,
            item: planned_result(
                "class_relation",
                "create",
                input.ref_.clone(),
                identifier.clone(),
            ),
            message,
        })?;

    if state.class_relations.contains(&pair)
        || lookup_direct_class_relation(pool, pair.0, pair.1)
            .await
            .map_err(|err| PlanningFailure {
                kind: FailureKind::Runtime,
                item: planned_result(
                    "class_relation",
                    "lookup",
                    input.ref_.clone(),
                    identifier.clone(),
                ),
                message: err.to_string(),
            })?
            .is_some()
    {
        if matches!(mode.collision_policy, Some(ImportCollisionPolicy::Abort)) {
            return Err(PlanningFailure {
                kind: FailureKind::Collision,
                item: planned_result("class_relation", "create", input.ref_.clone(), identifier),
                message: "Class relation already exists".to_string(),
            });
        }

        return Ok(PlannedItem {
            result: planned_result("class_relation", "noop", input.ref_.clone(), identifier),
            execution: None,
        });
    }

    state.class_relations.insert(pair);

    Ok(PlannedItem {
        result: planned_result(
            "class_relation",
            "create",
            input.ref_.clone(),
            Some(format!("{}<->{}", from_class.name, to_class.name)),
        ),
        execution: Some(PlannedExecution::CreateClassRelation(input.clone())),
    })
}

async fn plan_object_relation(
    pool: &DbPool,
    user: &User,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportObjectRelationInput,
) -> Result<PlannedItem, PlanningFailure> {
    let from_object = resolve_object_planning(
        pool,
        state,
        input.from_object_ref.as_deref(),
        input.from_object_key.as_ref(),
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Resolution,
        item: planned_result("object_relation", "create", input.ref_.clone(), None),
        message,
    })?;
    let to_object = resolve_object_planning(
        pool,
        state,
        input.to_object_ref.as_deref(),
        input.to_object_key.as_ref(),
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Resolution,
        item: planned_result("object_relation", "create", input.ref_.clone(), None),
        message,
    })?;
    let pair = normalize_pair(from_object.id, to_object.id);

    let from_namespace = resolve_namespace_by_id_planning(pool, state, from_object.namespace_id)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result("object_relation", "create", input.ref_.clone(), None),
            message,
        })?;
    let to_namespace = resolve_namespace_by_id_planning(pool, state, to_object.namespace_id)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result("object_relation", "create", input.ref_.clone(), None),
            message,
        })?;

    ensure_namespace_permission(
        pool,
        user,
        &from_namespace,
        Permissions::CreateObjectRelation,
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Permission,
        item: planned_result("object_relation", "create", input.ref_.clone(), None),
        message,
    })?;
    ensure_namespace_permission(pool, user, &to_namespace, Permissions::CreateObjectRelation)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Permission,
            item: planned_result("object_relation", "create", input.ref_.clone(), None),
            message,
        })?;

    let class_pair = normalize_pair(from_object.class_id, to_object.class_id);
    let class_relation_exists = state.class_relations.contains(&class_pair)
        || lookup_direct_class_relation(pool, class_pair.0, class_pair.1)
            .await
            .map_err(|err| PlanningFailure {
                kind: FailureKind::Runtime,
                item: planned_result("object_relation", "lookup", input.ref_.clone(), None),
                message: err.to_string(),
            })?
            .is_some();

    if !class_relation_exists {
        return Err(PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result("object_relation", "create", input.ref_.clone(), None),
            message: "Object relation requires a direct class relation between the object classes"
                .to_string(),
        });
    }

    if state.object_relations.contains(&pair)
        || lookup_object_relation(pool, pair.0, pair.1)
            .await
            .map_err(|err| PlanningFailure {
                kind: FailureKind::Runtime,
                item: planned_result("object_relation", "lookup", input.ref_.clone(), None),
                message: err.to_string(),
            })?
            .is_some()
    {
        if matches!(mode.collision_policy, Some(ImportCollisionPolicy::Abort)) {
            return Err(PlanningFailure {
                kind: FailureKind::Collision,
                item: planned_result("object_relation", "create", input.ref_.clone(), None),
                message: "Object relation already exists".to_string(),
            });
        }

        return Ok(PlannedItem {
            result: planned_result("object_relation", "noop", input.ref_.clone(), None),
            execution: None,
        });
    }

    state.object_relations.insert(pair);

    Ok(PlannedItem {
        result: planned_result(
            "object_relation",
            "create",
            input.ref_.clone(),
            Some(format!("{}<->{}", from_object.name, to_object.name)),
        ),
        execution: Some(PlannedExecution::CreateObjectRelation(input.clone())),
    })
}

async fn plan_namespace_permission(
    pool: &DbPool,
    user: &User,
    _mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportNamespacePermissionInput,
) -> Result<PlannedItem, PlanningFailure> {
    let namespace = resolve_namespace_planning(
        pool,
        state,
        input.namespace_ref.as_deref(),
        input.namespace_key.as_ref(),
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Resolution,
        item: planned_result(
            "namespace_permission",
            "apply",
            input.ref_.clone(),
            Some(input.group_key.groupname.clone()),
        ),
        message,
    })?;

    ensure_namespace_permission(pool, user, &namespace, Permissions::DelegateCollection)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Permission,
            item: planned_result(
                "namespace_permission",
                "apply",
                input.ref_.clone(),
                Some(format!("{}::{}", namespace.name, input.group_key.groupname)),
            ),
            message,
        })?;

    let group = lookup_group_by_name(pool, &input.group_key.groupname)
        .await
        .map_err(|err| PlanningFailure {
            kind: FailureKind::Runtime,
            item: planned_result(
                "namespace_permission",
                "lookup",
                input.ref_.clone(),
                Some(input.group_key.groupname.clone()),
            ),
            message: err.to_string(),
        })?
        .ok_or_else(|| PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result(
                "namespace_permission",
                "apply",
                input.ref_.clone(),
                Some(input.group_key.groupname.clone()),
            ),
            message: format!("Group '{}' not found", input.group_key.groupname),
        })?;

    Ok(PlannedItem {
        result: planned_result(
            "namespace_permission",
            if input.replace_existing.unwrap_or(false) {
                "replace"
            } else {
                "grant"
            },
            input.ref_.clone(),
            Some(format!("{}::{}", namespace.name, group.groupname)),
        ),
        execution: Some(PlannedExecution::ApplyNamespacePermissions(input.clone())),
    })
}

async fn resolve_namespace_planning(
    pool: &DbPool,
    state: &PlanningState,
    reference: Option<&str>,
    key: Option<&NamespaceKey>,
) -> Result<NamespaceResolution, String> {
    match (reference, key) {
        (Some(reference), None) => state
            .namespaces_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| format!("Unknown namespace ref '{reference}'")),
        (None, Some(key)) => {
            if let Some(namespace) = state.namespaces_by_name.get(&key.name) {
                return Ok(namespace.clone());
            }

            lookup_namespace_by_name(pool, &key.name)
                .await
                .map_err(|err| err.to_string())?
                .map(namespace_to_resolution)
                .ok_or_else(|| format!("Namespace '{}' not found", key.name))
        }
        _ => Err("Exactly one of namespace_ref or namespace_key must be provided".to_string()),
    }
}

async fn resolve_namespace_by_id_planning(
    pool: &DbPool,
    state: &PlanningState,
    namespace_id: i32,
) -> Result<NamespaceResolution, String> {
    if let Some(namespace) = state
        .namespaces_by_name
        .values()
        .find(|namespace| namespace.id == namespace_id)
    {
        return Ok(namespace.clone());
    }

    lookup_namespace_by_id(pool, namespace_id)
        .await
        .map_err(|err| err.to_string())?
        .map(namespace_to_resolution)
        .ok_or_else(|| format!("Namespace id '{}' not found", namespace_id))
}

async fn resolve_class_planning(
    pool: &DbPool,
    state: &PlanningState,
    reference: Option<&str>,
    key: Option<&ClassKey>,
) -> Result<ClassResolution, String> {
    match (reference, key) {
        (Some(reference), None) => state
            .classes_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| format!("Unknown class ref '{reference}'")),
        (None, Some(key)) => {
            let namespace = resolve_namespace_planning(
                pool,
                state,
                key.namespace_ref.as_deref(),
                key.namespace_key.as_ref(),
            )
            .await?;
            if let Some(class) = state.classes_by_key.get(&(namespace.id, key.name.clone())) {
                return Ok(class.clone());
            }

            lookup_class_by_namespace_and_name(pool, namespace.id, &key.name)
                .await
                .map_err(|err| err.to_string())?
                .map(class_to_resolution)
                .ok_or_else(|| {
                    format!(
                        "Class '{}' not found in namespace '{}'",
                        key.name, namespace.name
                    )
                })
        }
        _ => Err("Exactly one of class_ref or class_key must be provided".to_string()),
    }
}

async fn resolve_object_planning(
    pool: &DbPool,
    state: &PlanningState,
    reference: Option<&str>,
    key: Option<&ObjectKey>,
) -> Result<ObjectResolution, String> {
    match (reference, key) {
        (Some(reference), None) => state
            .objects_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| format!("Unknown object ref '{reference}'")),
        (None, Some(key)) => {
            let class = resolve_class_planning(
                pool,
                state,
                key.class_ref.as_deref(),
                key.class_key.as_ref(),
            )
            .await?;
            if let Some(object) = state.objects_by_key.get(&(class.id, key.name.clone())) {
                return Ok(object.clone());
            }

            lookup_object_by_class_and_name(pool, class.id, &key.name)
                .await
                .map_err(|err| err.to_string())?
                .map(object_to_resolution)
                .ok_or_else(|| format!("Object '{}' not found in class '{}'", key.name, class.name))
        }
        _ => Err("Exactly one of object_ref or object_key must be provided".to_string()),
    }
}

fn resolve_namespace_runtime(
    conn: &mut diesel::PgConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&NamespaceKey>,
) -> Result<Namespace, ApiError> {
    match (reference, key) {
        (Some(reference), None) => runtime
            .namespaces_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| ApiError::BadRequest(format!("Unknown namespace ref '{reference}'"))),
        (None, Some(key)) => lookup_namespace_by_name_db(conn, &key.name)?.ok_or_else(|| {
            ApiError::NotFound(format!(
                "Namespace '{}' not found during execution",
                key.name
            ))
        }),
        _ => Err(ApiError::BadRequest(
            "Exactly one of namespace_ref or namespace_key must be provided".to_string(),
        )),
    }
}

fn resolve_class_runtime(
    conn: &mut diesel::PgConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&ClassKey>,
) -> Result<HubuumClass, ApiError> {
    match (reference, key) {
        (Some(reference), None) => runtime
            .classes_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| ApiError::BadRequest(format!("Unknown class ref '{reference}'"))),
        (None, Some(key)) => {
            let namespace = resolve_namespace_runtime(
                conn,
                runtime,
                key.namespace_ref.as_deref(),
                key.namespace_key.as_ref(),
            )?;
            lookup_class_by_namespace_and_name_db(conn, namespace.id, &key.name)?.ok_or_else(|| {
                ApiError::NotFound(format!(
                    "Class '{}' not found in namespace '{}' during execution",
                    key.name, namespace.name
                ))
            })
        }
        _ => Err(ApiError::BadRequest(
            "Exactly one of class_ref or class_key must be provided".to_string(),
        )),
    }
}

fn resolve_object_runtime(
    conn: &mut diesel::PgConnection,
    runtime: &RuntimeState,
    reference: Option<&str>,
    key: Option<&ObjectKey>,
) -> Result<HubuumObject, ApiError> {
    match (reference, key) {
        (Some(reference), None) => runtime
            .objects_by_ref
            .get(reference)
            .cloned()
            .ok_or_else(|| ApiError::BadRequest(format!("Unknown object ref '{reference}'"))),
        (None, Some(key)) => {
            let class = resolve_class_runtime(
                conn,
                runtime,
                key.class_ref.as_deref(),
                key.class_key.as_ref(),
            )?;
            lookup_object_by_class_and_name_db(conn, class.id, &key.name)?.ok_or_else(|| {
                ApiError::NotFound(format!(
                    "Object '{}' not found in class '{}' during execution",
                    key.name, class.name
                ))
            })
        }
        _ => Err(ApiError::BadRequest(
            "Exactly one of object_ref or object_key must be provided".to_string(),
        )),
    }
}

fn remember_namespace(
    state: &mut PlanningState,
    reference: Option<String>,
    namespace: NamespaceResolution,
) {
    state
        .namespaces_by_name
        .insert(namespace.name.clone(), namespace.clone());
    if let Some(reference) = reference {
        state.namespaces_by_ref.insert(reference, namespace);
    }
}

fn remember_class(state: &mut PlanningState, reference: Option<String>, class: ClassResolution) {
    state
        .classes_by_key
        .insert((class.namespace_id, class.name.clone()), class.clone());
    if let Some(reference) = reference {
        state.classes_by_ref.insert(reference, class);
    }
}

fn remember_object(state: &mut PlanningState, reference: Option<String>, object: ObjectResolution) {
    state
        .objects_by_key
        .insert((object.class_id, object.name.clone()), object.clone());
    if let Some(reference) = reference {
        state.objects_by_ref.insert(reference, object);
    }
}

fn planned_result(
    entity_kind: &str,
    action: &str,
    item_ref: Option<String>,
    identifier: Option<String>,
) -> PlannedTaskResult {
    PlannedTaskResult {
        item_ref,
        entity_kind: entity_kind.to_string(),
        action: action.to_string(),
        identifier,
        details: None,
    }
}

fn identifier_namespace(namespace: &NamespaceResolution) -> String {
    namespace.name.clone()
}

fn namespace_to_resolution(namespace: Namespace) -> NamespaceResolution {
    NamespaceResolution {
        id: namespace.id,
        name: namespace.name,
        description: namespace.description,
        exists_in_db: true,
    }
}

fn class_to_resolution(class: HubuumClass) -> ClassResolution {
    ClassResolution {
        id: class.id,
        name: class.name,
        namespace_id: class.namespace_id,
        json_schema: class.json_schema,
        validate_schema: class.validate_schema,
    }
}

fn object_to_resolution(object: HubuumObject) -> ObjectResolution {
    ObjectResolution {
        id: object.id,
        name: object.name,
        namespace_id: object.namespace_id,
        class_id: object.hubuum_class_id,
    }
}

fn normalize_pair(left: i32, right: i32) -> (i32, i32) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

async fn ensure_namespace_permission(
    pool: &DbPool,
    user: &User,
    namespace: &NamespaceResolution,
    permission: Permissions,
) -> Result<(), String> {
    if !namespace.exists_in_db {
        let is_admin = user.is_admin(pool).await.map_err(|err| err.to_string())?;
        if is_admin {
            return Ok(());
        }
        return Err(
            "Only admins may operate on newly created namespaces within an import".to_string(),
        );
    }

    user.can(pool, vec![permission], vec![NamespaceID(namespace.id)])
        .await
        .map_err(|err| err.to_string())
}

async fn lookup_namespace_by_name(
    pool: &DbPool,
    value: &str,
) -> Result<Option<Namespace>, ApiError> {
    use crate::schema::namespaces::dsl::{name, namespaces};

    with_connection(pool, |conn| {
        namespaces
            .filter(name.eq(value))
            .first::<Namespace>(conn)
            .optional()
    })
}

async fn lookup_namespace_by_id(
    pool: &DbPool,
    namespace_id: i32,
) -> Result<Option<Namespace>, ApiError> {
    use crate::schema::namespaces::dsl::{id, namespaces};

    with_connection(pool, |conn| {
        namespaces
            .filter(id.eq(namespace_id))
            .first::<Namespace>(conn)
            .optional()
    })
}

async fn lookup_class_by_namespace_and_name(
    pool: &DbPool,
    namespace_id_value: i32,
    class_name: &str,
) -> Result<Option<HubuumClass>, ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, name, namespace_id};

    with_connection(pool, |conn| {
        hubuumclass
            .filter(namespace_id.eq(namespace_id_value))
            .filter(name.eq(class_name))
            .first::<HubuumClass>(conn)
            .optional()
    })
}

async fn lookup_object_by_class_and_name(
    pool: &DbPool,
    class_id_value: i32,
    object_name: &str,
) -> Result<Option<HubuumObject>, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuum_class_id, hubuumobject, name};

    with_connection(pool, |conn| {
        hubuumobject
            .filter(hubuum_class_id.eq(class_id_value))
            .filter(name.eq(object_name))
            .first::<HubuumObject>(conn)
            .optional()
    })
}

async fn lookup_direct_class_relation(
    pool: &DbPool,
    left: i32,
    right: i32,
) -> Result<Option<HubuumClassRelation>, ApiError> {
    use crate::schema::hubuumclass_relation::dsl::{
        from_hubuum_class_id, hubuumclass_relation, to_hubuum_class_id,
    };
    let pair = normalize_pair(left, right);

    with_connection(pool, |conn| {
        hubuumclass_relation
            .filter(from_hubuum_class_id.eq(pair.0))
            .filter(to_hubuum_class_id.eq(pair.1))
            .first::<HubuumClassRelation>(conn)
            .optional()
    })
}

async fn lookup_object_relation(
    pool: &DbPool,
    left: i32,
    right: i32,
) -> Result<Option<HubuumObjectRelation>, ApiError> {
    use crate::schema::hubuumobject_relation::dsl::{
        from_hubuum_object_id, hubuumobject_relation, to_hubuum_object_id,
    };
    let pair = normalize_pair(left, right);

    with_connection(pool, |conn| {
        hubuumobject_relation
            .filter(from_hubuum_object_id.eq(pair.0))
            .filter(to_hubuum_object_id.eq(pair.1))
            .first::<HubuumObjectRelation>(conn)
            .optional()
    })
}

async fn lookup_group_by_name(pool: &DbPool, value: &str) -> Result<Option<Group>, ApiError> {
    use crate::schema::groups::dsl::{groupname, groups};

    with_connection(pool, |conn| {
        groups
            .filter(groupname.eq(value))
            .first::<Group>(conn)
            .optional()
    })
}

fn lookup_namespace_by_name_db(
    conn: &mut diesel::PgConnection,
    value: &str,
) -> Result<Option<Namespace>, ApiError> {
    use crate::schema::namespaces::dsl::{name, namespaces};

    namespaces
        .filter(name.eq(value))
        .first::<Namespace>(conn)
        .optional()
        .map_err(ApiError::from)
}

fn lookup_class_by_namespace_and_name_db(
    conn: &mut diesel::PgConnection,
    namespace_id_value: i32,
    class_name: &str,
) -> Result<Option<HubuumClass>, ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, name, namespace_id};

    hubuumclass
        .filter(namespace_id.eq(namespace_id_value))
        .filter(name.eq(class_name))
        .first::<HubuumClass>(conn)
        .optional()
        .map_err(ApiError::from)
}

fn lookup_object_by_class_and_name_db(
    conn: &mut diesel::PgConnection,
    class_id_value: i32,
    object_name: &str,
) -> Result<Option<HubuumObject>, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuum_class_id, hubuumobject, name};

    hubuumobject
        .filter(hubuum_class_id.eq(class_id_value))
        .filter(name.eq(object_name))
        .first::<HubuumObject>(conn)
        .optional()
        .map_err(ApiError::from)
}

fn lookup_group_by_name_db(
    conn: &mut diesel::PgConnection,
    value: &str,
) -> Result<Option<Group>, ApiError> {
    use crate::schema::groups::dsl::{groupname, groups};

    groups
        .filter(groupname.eq(value))
        .first::<Group>(conn)
        .optional()
        .map_err(ApiError::from)
}

fn create_namespace_db(
    conn: &mut diesel::PgConnection,
    input: &ImportNamespaceInput,
) -> Result<Namespace, ApiError> {
    use crate::schema::namespaces::dsl::namespaces;

    diesel::insert_into(namespaces)
        .values((
            crate::schema::namespaces::name.eq(&input.name),
            crate::schema::namespaces::description.eq(&input.description),
        ))
        .get_result::<Namespace>(conn)
        .map_err(ApiError::from)
}

fn update_namespace_db(
    conn: &mut diesel::PgConnection,
    namespace_id_value: i32,
    input: &ImportNamespaceInput,
) -> Result<Namespace, ApiError> {
    use crate::schema::namespaces::dsl::{id, namespaces};

    let update = UpdateNamespace {
        name: Some(input.name.clone()),
        description: Some(input.description.clone()),
    };

    diesel::update(namespaces.filter(id.eq(namespace_id_value)))
        .set(&update)
        .get_result::<Namespace>(conn)
        .map_err(ApiError::from)
}

fn create_class_db(
    conn: &mut diesel::PgConnection,
    input: &ImportClassInput,
    namespace_id_value: i32,
) -> Result<HubuumClass, ApiError> {
    use crate::schema::hubuumclass::dsl::hubuumclass;

    let new_class = NewHubuumClass {
        name: input.name.clone(),
        namespace_id: namespace_id_value,
        json_schema: input.json_schema.clone(),
        validate_schema: input.validate_schema,
        description: input.description.clone(),
    };

    diesel::insert_into(hubuumclass)
        .values(&new_class)
        .get_result::<HubuumClass>(conn)
        .map_err(ApiError::from)
}

fn update_class_db(
    conn: &mut diesel::PgConnection,
    class_id_value: i32,
    input: &ImportClassInput,
) -> Result<HubuumClass, ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, id};

    let update = UpdateHubuumClass {
        name: Some(input.name.clone()),
        namespace_id: None,
        json_schema: input.json_schema.clone(),
        validate_schema: input.validate_schema,
        description: Some(input.description.clone()),
    };

    diesel::update(hubuumclass.filter(id.eq(class_id_value)))
        .set(&update)
        .get_result::<HubuumClass>(conn)
        .map_err(ApiError::from)
}

fn create_object_db(
    conn: &mut diesel::PgConnection,
    input: &ImportObjectInput,
    class: &HubuumClass,
) -> Result<HubuumObject, ApiError> {
    use crate::schema::hubuumobject::dsl::hubuumobject;

    let new_object = NewHubuumObject {
        name: input.name.clone(),
        namespace_id: class.namespace_id,
        hubuum_class_id: class.id,
        data: input.data.clone(),
        description: input.description.clone(),
    };

    diesel::insert_into(hubuumobject)
        .values(&new_object)
        .get_result::<HubuumObject>(conn)
        .map_err(ApiError::from)
}

fn update_object_db(
    conn: &mut diesel::PgConnection,
    object_id_value: i32,
    input: &ImportObjectInput,
) -> Result<HubuumObject, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuumobject, id};

    let update = UpdateHubuumObject {
        name: Some(input.name.clone()),
        namespace_id: None,
        hubuum_class_id: None,
        data: Some(input.data.clone()),
        description: Some(input.description.clone()),
    };

    diesel::update(hubuumobject.filter(id.eq(object_id_value)))
        .set(&update)
        .get_result::<HubuumObject>(conn)
        .map_err(ApiError::from)
}

fn create_class_relation_db(
    conn: &mut diesel::PgConnection,
    left: i32,
    right: i32,
) -> Result<HubuumClassRelation, ApiError> {
    use crate::schema::hubuumclass_relation::dsl::hubuumclass_relation;
    let pair = normalize_pair(left, right);
    let new_relation = NewHubuumClassRelation {
        from_hubuum_class_id: pair.0,
        to_hubuum_class_id: pair.1,
    };

    diesel::insert_into(hubuumclass_relation)
        .values(&new_relation)
        .get_result::<HubuumClassRelation>(conn)
        .map_err(ApiError::from)
}

fn create_object_relation_db(
    conn: &mut diesel::PgConnection,
    from_object: &HubuumObject,
    to_object: &HubuumObject,
) -> Result<HubuumObjectRelation, ApiError> {
    use crate::schema::hubuumclass_relation::dsl::{
        from_hubuum_class_id, hubuumclass_relation, to_hubuum_class_id,
    };
    use crate::schema::hubuumobject_relation::dsl::hubuumobject_relation;
    let class_pair = normalize_pair(from_object.hubuum_class_id, to_object.hubuum_class_id);
    let relation = hubuumclass_relation
        .filter(from_hubuum_class_id.eq(class_pair.0))
        .filter(to_hubuum_class_id.eq(class_pair.1))
        .first::<HubuumClassRelation>(conn)?;

    let object_pair = normalize_pair(from_object.id, to_object.id);
    let new_relation = NewHubuumObjectRelation {
        from_hubuum_object_id: object_pair.0,
        to_hubuum_object_id: object_pair.1,
        class_relation_id: relation.id,
    };

    diesel::insert_into(hubuumobject_relation)
        .values(&new_relation)
        .get_result::<HubuumObjectRelation>(conn)
        .map_err(ApiError::from)
}

fn apply_permissions_db(
    conn: &mut diesel::PgConnection,
    namespace_id_value: i32,
    group_id_value: i32,
    permissions: &[Permissions],
    replace_existing: bool,
) -> Result<Permission, ApiError> {
    use crate::schema::permissions::dsl::{
        group_id, namespace_id, permissions as permissions_table,
    };

    let existing = permissions_table
        .filter(namespace_id.eq(namespace_id_value))
        .filter(group_id.eq(group_id_value))
        .first::<Permission>(conn)
        .optional()?;

    let permission_list = PermissionsList::new(permissions.to_vec());
    match existing {
        Some(_) => {
            let mut update = if replace_existing {
                UpdatePermission {
                    has_read_namespace: Some(false),
                    has_update_namespace: Some(false),
                    has_delete_namespace: Some(false),
                    has_delegate_namespace: Some(false),
                    has_create_class: Some(false),
                    has_read_class: Some(false),
                    has_update_class: Some(false),
                    has_delete_class: Some(false),
                    has_create_object: Some(false),
                    has_read_object: Some(false),
                    has_update_object: Some(false),
                    has_delete_object: Some(false),
                    has_create_class_relation: Some(false),
                    has_read_class_relation: Some(false),
                    has_update_class_relation: Some(false),
                    has_delete_class_relation: Some(false),
                    has_create_object_relation: Some(false),
                    has_read_object_relation: Some(false),
                    has_update_object_relation: Some(false),
                    has_delete_object_relation: Some(false),
                    has_read_template: Some(false),
                    has_create_template: Some(false),
                    has_update_template: Some(false),
                    has_delete_template: Some(false),
                }
            } else {
                UpdatePermission::default()
            };
            apply_permission_list_to_update(&mut update, permissions);

            diesel::update(
                permissions_table
                    .filter(namespace_id.eq(namespace_id_value))
                    .filter(group_id.eq(group_id_value)),
            )
            .set(&update)
            .get_result::<Permission>(conn)
            .map_err(ApiError::from)
        }
        None => {
            let new_entry = NewPermission {
                namespace_id: namespace_id_value,
                group_id: group_id_value,
                has_read_namespace: permission_list.contains(&Permissions::ReadCollection),
                has_update_namespace: permission_list.contains(&Permissions::UpdateCollection),
                has_delete_namespace: permission_list.contains(&Permissions::DeleteCollection),
                has_delegate_namespace: permission_list.contains(&Permissions::DelegateCollection),
                has_create_class: permission_list.contains(&Permissions::CreateClass),
                has_read_class: permission_list.contains(&Permissions::ReadClass),
                has_update_class: permission_list.contains(&Permissions::UpdateClass),
                has_delete_class: permission_list.contains(&Permissions::DeleteClass),
                has_create_object: permission_list.contains(&Permissions::CreateObject),
                has_read_object: permission_list.contains(&Permissions::ReadObject),
                has_update_object: permission_list.contains(&Permissions::UpdateObject),
                has_delete_object: permission_list.contains(&Permissions::DeleteObject),
                has_create_class_relation: permission_list
                    .contains(&Permissions::CreateClassRelation),
                has_read_class_relation: permission_list.contains(&Permissions::ReadClassRelation),
                has_update_class_relation: permission_list
                    .contains(&Permissions::UpdateClassRelation),
                has_delete_class_relation: permission_list
                    .contains(&Permissions::DeleteClassRelation),
                has_create_object_relation: permission_list
                    .contains(&Permissions::CreateObjectRelation),
                has_read_object_relation: permission_list
                    .contains(&Permissions::ReadObjectRelation),
                has_update_object_relation: permission_list
                    .contains(&Permissions::UpdateObjectRelation),
                has_delete_object_relation: permission_list
                    .contains(&Permissions::DeleteObjectRelation),
                has_read_template: permission_list.contains(&Permissions::ReadTemplate),
                has_create_template: permission_list.contains(&Permissions::CreateTemplate),
                has_update_template: permission_list.contains(&Permissions::UpdateTemplate),
                has_delete_template: permission_list.contains(&Permissions::DeleteTemplate),
            };

            diesel::insert_into(permissions_table)
                .values(&new_entry)
                .get_result::<Permission>(conn)
                .map_err(ApiError::from)
        }
    }
}

fn apply_permission_list_to_update(update: &mut UpdatePermission, permissions: &[Permissions]) {
    for permission in permissions {
        match permission {
            Permissions::ReadCollection => update.has_read_namespace = Some(true),
            Permissions::UpdateCollection => update.has_update_namespace = Some(true),
            Permissions::DeleteCollection => update.has_delete_namespace = Some(true),
            Permissions::DelegateCollection => update.has_delegate_namespace = Some(true),
            Permissions::CreateClass => update.has_create_class = Some(true),
            Permissions::ReadClass => update.has_read_class = Some(true),
            Permissions::UpdateClass => update.has_update_class = Some(true),
            Permissions::DeleteClass => update.has_delete_class = Some(true),
            Permissions::CreateObject => update.has_create_object = Some(true),
            Permissions::ReadObject => update.has_read_object = Some(true),
            Permissions::UpdateObject => update.has_update_object = Some(true),
            Permissions::DeleteObject => update.has_delete_object = Some(true),
            Permissions::CreateClassRelation => update.has_create_class_relation = Some(true),
            Permissions::ReadClassRelation => update.has_read_class_relation = Some(true),
            Permissions::UpdateClassRelation => update.has_update_class_relation = Some(true),
            Permissions::DeleteClassRelation => update.has_delete_class_relation = Some(true),
            Permissions::CreateObjectRelation => update.has_create_object_relation = Some(true),
            Permissions::ReadObjectRelation => update.has_read_object_relation = Some(true),
            Permissions::UpdateObjectRelation => update.has_update_object_relation = Some(true),
            Permissions::DeleteObjectRelation => update.has_delete_object_relation = Some(true),
            Permissions::ReadTemplate => update.has_read_template = Some(true),
            Permissions::CreateTemplate => update.has_create_template = Some(true),
            Permissions::UpdateTemplate => update.has_update_template = Some(true),
            Permissions::DeleteTemplate => update.has_delete_template = Some(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
    use futures::executor::block_on;

    use super::{
        ExecutionAccumulator, PlannedExecution, PlannedItem, execute_import_best_effort,
        execute_import_strict, planned_result,
    };
    use crate::models::{
        ImportClassInput, ImportCollisionPolicy, ImportMode, ImportNamespaceInput,
        ImportPermissionPolicy,
    };
    use crate::schema::hubuumclass::dsl::{hubuumclass, name as class_name};
    use crate::schema::namespaces::dsl::{name as namespace_name, namespaces};
    use crate::tests::TestContext;
    use crate::db::with_connection;

    #[test]
    fn test_execute_import_strict_rolls_back_on_runtime_failure() {
        let context = block_on(TestContext::new());
        let namespace = context.scoped_name("strict_rollback_ns");
        let class = context.scoped_name("strict_rollback_class");
        let planned_items = vec![
            PlannedItem {
                result: planned_result(
                    "namespace",
                    "create",
                    Some("ns:ok".to_string()),
                    Some(namespace.clone()),
                ),
                execution: Some(PlannedExecution::CreateNamespace(ImportNamespaceInput {
                    ref_: Some("ns:ok".to_string()),
                    name: namespace.clone(),
                    description: "Rollback namespace".to_string(),
                })),
            },
            PlannedItem {
                result: planned_result(
                    "class",
                    "create",
                    Some("class:bad".to_string()),
                    Some(class.clone()),
                ),
                execution: Some(PlannedExecution::CreateClass(ImportClassInput {
                    ref_: Some("class:bad".to_string()),
                    name: class.clone(),
                    description: "Fails at runtime".to_string(),
                    json_schema: None,
                    validate_schema: Some(false),
                    namespace_ref: Some("ns:missing".to_string()),
                    namespace_key: None,
                })),
            },
        ];

        let mut accumulator = ExecutionAccumulator::default();
        let result = block_on(execute_import_strict(
            &context.pool,
            1,
            &planned_items,
            &mut accumulator,
        ));
        assert!(result.is_err());

        let namespace_exists = with_connection(&context.pool, |conn| {
            namespaces
                .filter(namespace_name.eq(&namespace))
                .count()
                .get_result::<i64>(conn)
        })
        .unwrap();
        let class_exists = with_connection(&context.pool, |conn| {
            hubuumclass
                .filter(class_name.eq(&class))
                .count()
                .get_result::<i64>(conn)
        })
        .unwrap();

        assert_eq!(namespace_exists, 0);
        assert_eq!(class_exists, 0);
        assert_eq!(accumulator.processed, 0);
    }

    #[test]
    fn test_execute_import_best_effort_keeps_successful_items() {
        let context = block_on(TestContext::new());
        let namespace_one = context.scoped_name("best_effort_ns_one");
        let namespace_two = context.scoped_name("best_effort_ns_two");
        let class_bad = context.scoped_name("best_effort_class_bad");
        let planned_items = vec![
            PlannedItem {
                result: planned_result(
                    "namespace",
                    "create",
                    Some("ns:one".to_string()),
                    Some(namespace_one.clone()),
                ),
                execution: Some(PlannedExecution::CreateNamespace(ImportNamespaceInput {
                    ref_: Some("ns:one".to_string()),
                    name: namespace_one.clone(),
                    description: "Best effort namespace one".to_string(),
                })),
            },
            PlannedItem {
                result: planned_result(
                    "class",
                    "create",
                    Some("class:bad".to_string()),
                    Some(class_bad),
                ),
                execution: Some(PlannedExecution::CreateClass(ImportClassInput {
                    ref_: Some("class:bad".to_string()),
                    name: "bad".to_string(),
                    description: "Fails at runtime".to_string(),
                    json_schema: None,
                    validate_schema: Some(false),
                    namespace_ref: Some("ns:missing".to_string()),
                    namespace_key: None,
                })),
            },
            PlannedItem {
                result: planned_result(
                    "namespace",
                    "create",
                    Some("ns:two".to_string()),
                    Some(namespace_two.clone()),
                ),
                execution: Some(PlannedExecution::CreateNamespace(ImportNamespaceInput {
                    ref_: Some("ns:two".to_string()),
                    name: namespace_two.clone(),
                    description: "Best effort namespace two".to_string(),
                })),
            },
        ];

        let mut accumulator = ExecutionAccumulator::default();
        block_on(execute_import_best_effort(
            &context.pool,
            1,
            &planned_items,
            &ImportMode {
                atomicity: Some(crate::models::ImportAtomicity::BestEffort),
                collision_policy: Some(ImportCollisionPolicy::Overwrite),
                permission_policy: Some(ImportPermissionPolicy::Continue),
            },
            &mut accumulator,
        ))
        .unwrap();

        let namespace_count = with_connection(&context.pool, |conn| {
            namespaces
                .filter(namespace_name.eq_any([namespace_one.clone(), namespace_two.clone()]))
                .count()
                .get_result::<i64>(conn)
        })
        .unwrap();

        assert_eq!(namespace_count, 2);
        assert_eq!(accumulator.processed, 3);
        assert_eq!(accumulator.success, 2);
        assert_eq!(accumulator.failed, 1);
    }
}
