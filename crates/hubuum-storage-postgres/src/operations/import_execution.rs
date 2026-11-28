//! Atomic PostgreSQL execution for validated backend-neutral import plans.
mod identity;
use identity::{
    execute_group, execute_group_membership, execute_identity_scope, execute_principal,
    resolve_group, resolve_identity_scope, resolve_principal,
};
mod relations;
use relations::{
    check_class_relation_condition, check_object_relation_condition,
    class_relation_observed_revision, create_class_relation, create_object_relation,
    object_relation_observed_revision, update_class_relation_timestamps,
    update_object_relation_timestamps,
};
mod integrations;
use integrations::{
    execute_event_sink, upsert_event_subscription, upsert_export_template, upsert_remote_target,
};

use super::task_execution::{claimed_task, live_claimed_task};
use hubuum_domain::ClassId;
use hubuum_domain::JsonSchemaLimits;
use hubuum_storage_core::{
    FencedImportItem, FencedImportPlan, FencedImportResults, StorageImportResult, StorageTaskLease,
};

use std::collections::HashMap;

use chrono::{NaiveDateTime, Utc};
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::{PgExpressionMethods, SelectableHelper};
use diesel_async::{AsyncConnection, RunQueryDsl};
use hubuum_computed_fields::ResultType;
use hubuum_storage_core::{
    StorageClass, StorageCollection, StorageError, StorageErrorKind, StorageImportApply,
    StorageImportApplyItem, StorageImportAtomicity, StorageImportClass, StorageImportClassKey,
    StorageImportCollection, StorageImportCollectionKey, StorageImportCollectionPermission,
    StorageImportCollisionPolicy, StorageImportComputedField, StorageImportComputedFieldVisibility,
    StorageImportExportTemplate, StorageImportMode, StorageImportObject, StorageImportObjectKey,
    StorageImportOperation, StorageImportPermissionPolicy, StorageImportPlan,
    StorageImportPlanItem, StorageImportPreflight, StorageImportPreflightItem,
    StorageImportTimestamps, StorageImportWriteCondition, StorageObject,
};

use super::authorization::{NewPermission, UpdatePermission};
use super::class::ClassRow;
use super::collection::{CollectionRow, insert_collection_closure_rows};
use super::computed_definition::ComputedDefinitionRow;
use super::computed_fields::advance_revision_and_enqueue_on_connection;
use super::computed_materialization::{
    acquire_computed_class_shared_lock, materialize_object_on_connection,
};
use super::import_workflow::{
    class_by_name_on_connection, collection_by_key_on_connection, object_by_name_on_connection,
    root_collection_on_connection,
};
use super::object::ObjectRow;
use super::schema_evolution::{
    record_class_schema_on, record_object_schema_on, validate_import_object_on,
};
use crate::runtime::ambient_event_context;
use crate::{
    PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError, SendAsyncFn,
};

const DRY_RUN_ROLLBACK: &str = "hubuum import dry-run rollback";

#[derive(Default)]
struct ImportRuntime {
    identity_scopes_by_ref: HashMap<String, i32>,
    groups_by_ref: HashMap<String, i32>,
    principals_by_ref: HashMap<String, i32>,
    collections_by_ref: HashMap<String, StorageCollection>,
    classes_by_ref: HashMap<String, StorageClass>,
    objects_by_ref: HashMap<String, StorageObject>,
    event_sinks_by_ref: HashMap<String, i32>,
    export_templates: Vec<StorageImportExportTemplate>,
}

// Each operation stages at most one reference. Ownership moves into the runtime
// only after its transaction (including any receipt and deferred checks) succeeds.
enum ImportReferenceChange {
    IdentityScope(String, i32),
    Group(String, i32),
    Principal(String, i32),
    Collection(String, StorageCollection),
    Class(String, StorageClass),
    Object(String, StorageObject),
    EventSink(String, i32),
}

impl ImportRuntime {
    fn publish(&mut self, change: Option<ImportReferenceChange>) {
        match change {
            Some(ImportReferenceChange::IdentityScope(reference, value)) => {
                self.identity_scopes_by_ref.insert(reference, value);
            }
            Some(ImportReferenceChange::Group(reference, value)) => {
                self.groups_by_ref.insert(reference, value);
            }
            Some(ImportReferenceChange::Principal(reference, value)) => {
                self.principals_by_ref.insert(reference, value);
            }
            Some(ImportReferenceChange::Collection(reference, value)) => {
                self.collections_by_ref.insert(reference, value);
            }
            Some(ImportReferenceChange::Class(reference, value)) => {
                self.classes_by_ref.insert(reference, value);
            }
            Some(ImportReferenceChange::Object(reference, value)) => {
                self.objects_by_ref.insert(reference, value);
            }
            Some(ImportReferenceChange::EventSink(reference, value)) => {
                self.event_sinks_by_ref.insert(reference, value);
            }
            None => {}
        }
    }

    fn for_plan(items: &[StorageImportPlanItem]) -> Self {
        let export_templates = items
            .iter()
            .filter_map(|item| match item.operation() {
                StorageImportOperation::UpsertExportTemplate { input, .. } => Some(input.clone()),
                _ => None,
            })
            .collect();
        Self {
            export_templates,
            ..Self::default()
        }
    }
}

pub async fn preflight_import(
    runtime: &PostgresRuntime,
    plan: StorageImportPlan,
    mode: StorageImportMode,
) -> Result<StorageImportPreflight, PostgresStorageError> {
    let schema_limits = runtime.schema_limits();
    let items = plan.into_items();
    let telemetry_runtime = runtime.clone();
    runtime
        .with_connection(async move |connection| {
            let mut outcomes = Vec::with_capacity(items.len());
            let mut aborted = false;
            let mut state = ImportRuntime::for_plan(&items);
            let transaction = connection
                .transaction::<(), PostgresStorageError, _>(async |connection| {
                    for item in items {
                        crate::runtime::task_execution_checkpoint()?;
                        let (index, operation) = item.into_parts();
                        let observed_revision =
                            observed_revision(connection, &state, &operation).await;
                        let (revision, result) = match observed_revision {
                            Ok(revision) => {
                                let result = connection
                                    .transaction::<Option<ImportReferenceChange>, PostgresStorageError, _>(
                                        async |connection| {
                                            execute_operation(schema_limits, connection, &state, operation)
                                                .await
                                        },
                                    )
                                    .await;
                                (revision, result)
                            }
                            Err(error) => (None, Err(error)),
                        };
                        match result {
                            Ok(change) => {
                                state.publish(change);
                                outcomes.push(StorageImportPreflightItem::success(
                                    index,
                                    revision.map(PostgresRevision::into_domain),
                                ));
                            }
                            Err(error) => {
                                record_revision_condition(&telemetry_runtime, &error);
                                aborted = should_abort_preflight(&error, &mode);
                                outcomes.push(StorageImportPreflightItem::failure(
                                    index,
                                    revision.map(PostgresRevision::into_domain),
                                    StorageError::from(error),
                                ));
                                if aborted {
                                    break;
                                }
                            }
                        }
                    }
                    Err(PostgresStorageError::internal(DRY_RUN_ROLLBACK))
                })
                .await;

            match transaction {
                Err(error)
                    if error.kind() == StorageErrorKind::Internal
                        && error.to_string() == DRY_RUN_ROLLBACK =>
                {
                    Ok(StorageImportPreflight::new(outcomes, aborted))
                }
                Err(error) => Err(error),
                Ok(()) => Err(PostgresStorageError::internal(
                    "Import dry run unexpectedly committed",
                )),
            }
        })
        .await
}

fn fenced_operations(items: &[FencedImportItem]) -> Vec<StorageImportPlanItem> {
    items
        .iter()
        .cloned()
        .filter_map(|item| {
            let (index, operation, _) = item.into_parts();
            operation.map(|operation| StorageImportPlanItem::new(index, operation))
        })
        .collect()
}

pub async fn apply_claimed_import_strict(
    runtime: &PostgresRuntime,
    plan: FencedImportPlan,
) -> Result<(), PostgresStorageError> {
    let schema_limits = runtime.schema_limits();
    let (lease, items) = plan.into_parts();
    runtime
        .with_transaction(async move |connection| {
            super::task_execution::require_claimed_execution(connection, claimed_task(&lease)?)
                .await?;
            let mut state = ImportRuntime::for_plan(&fenced_operations(&items));
            for item in items {
                crate::runtime::task_execution_checkpoint()?;
                let (index, operation, result) = item.into_parts();
                if let Some(operation) = operation {
                    state.publish(
                        execute_operation(schema_limits, connection, &state, operation).await?,
                    );
                }
                record_execution_receipt(connection, &lease, Some(index), result).await?;
            }
            crate::reach_fault_point(
                crate::PostgresFaultPoint::ImportBeforeCommitFence,
                Some(connection),
            )
            .await?;
            // Lock only at the commit boundary so long imports do not block lease
            // renewal. The deferred receipt trigger checks expiry again at commit.
            let task = live_claimed_task(connection, claimed_task(&lease)?).await?;
            if let Some(reason) = task
                .control(hubuum_storage_core::StorageTaskKind::Import)?
                .stop_reason(
                    super::task_execution::database_now(connection)
                        .await?
                        .and_utc(),
                )
            {
                return Err(PostgresStorageError::task_stopped(reason));
            }
            use crate::schema::tasks::dsl as tasks;
            diesel::update(tasks::tasks.filter(tasks::id.eq(task.id)))
                .set(
                    tasks::import_effects_committed_at
                        .eq(Some(super::task_execution::database_now(connection).await?)),
                )
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await?;
    crate::reach_fault_point(crate::PostgresFaultPoint::ImportAfterCommit, None).await
}

pub async fn apply_claimed_import_best_effort(
    runtime: &PostgresRuntime,
    plan: FencedImportPlan,
    mode: StorageImportMode,
) -> Result<StorageImportApply, PostgresStorageError> {
    let schema_limits = runtime.schema_limits();
    let (lease, items) = plan.into_parts();
    let mut state = ImportRuntime::for_plan(&fenced_operations(&items));
    let mut outcomes = Vec::new();
    let mut aborted = false;
    for item in items {
        crate::runtime::task_execution_checkpoint()?;
        let (index, operation, result) = item.into_parts();
        let receipt = result.clone();
        let outcome = runtime
            .with_transaction(async |connection| {
                super::task_execution::require_claimed_execution(connection, claimed_task(&lease)?)
                    .await?;
                let change = if let Some(operation) = operation {
                    execute_operation(schema_limits, connection, &state, operation).await?
                } else {
                    None
                };
                record_execution_receipt(connection, &lease, Some(index), receipt).await?;
                live_claimed_task(connection, claimed_task(&lease)?).await?;
                Ok::<_, PostgresStorageError>(change)
            })
            .await;
        match outcome {
            Ok(change) => {
                state.publish(change);
                outcomes.push(StorageImportApplyItem::success(index));
            }
            Err(error) => {
                if matches!(
                    error.kind(),
                    StorageErrorKind::TaskCancelled | StorageErrorKind::TaskDeadlineExceeded
                ) {
                    return Err(error);
                }
                record_revision_condition(runtime, &error);
                aborted = should_abort_best_effort(&error, &mode);
                let error = StorageError::from(error);
                runtime
                    .with_transaction(async |connection| {
                        super::task_execution::require_claimed_execution(
                            connection,
                            claimed_task(&lease)?,
                        )
                        .await?;
                        live_claimed_task(connection, claimed_task(&lease)?).await?;
                        record_execution_receipt(
                            connection,
                            &lease,
                            Some(index),
                            result.failed(&error),
                        )
                        .await
                    })
                    .await?;
                outcomes.push(StorageImportApplyItem::failure(index, error));
                if aborted {
                    break;
                }
            }
        }
    }
    crate::reach_fault_point(crate::PostgresFaultPoint::ImportAfterCommit, None).await?;
    Ok(StorageImportApply::new(outcomes, aborted))
}

pub async fn record_claimed_import_results(
    runtime: &PostgresRuntime,
    results: FencedImportResults,
) -> Result<(), PostgresStorageError> {
    let (lease, results) = results.into_parts();
    runtime
        .with_transaction(async move |connection| {
            super::task_execution::require_claimed_execution(connection, claimed_task(&lease)?)
                .await?;
            for result in results {
                record_execution_receipt(connection, &lease, None, result).await?;
            }
            live_claimed_task(connection, claimed_task(&lease)?).await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
}

async fn record_execution_receipt(
    connection: &mut PostgresConnection,
    lease: &StorageTaskLease,
    index: Option<usize>,
    result: StorageImportResult,
) -> Result<(), PostgresStorageError> {
    use diesel::sql_types::{BigInt, Integer, Jsonb, Nullable, Text, Uuid as SqlUuid};
    let (task_id, item_ref, entity_kind, action, identifier, outcome, error, details) =
        result.into_parts();
    let token = claimed_task(lease)?.token;
    let index = index.map(|index| {
        i64::try_from(index).expect("fenced plan indexes fit the storage representation")
    });
    diesel::sql_query("INSERT INTO import_task_results (task_id, item_ref, entity_kind, action, identifier, outcome, error, details, execution_index, execution_claim_token) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
        .bind::<Integer, _>(task_id.id())
        .bind::<Nullable<Text>, _>(item_ref)
        .bind::<Text, _>(entity_kind)
        .bind::<Text, _>(action)
        .bind::<Nullable<Text>, _>(identifier)
        .bind::<Text, _>(outcome)
        .bind::<Nullable<Text>, _>(error)
        .bind::<Nullable<Jsonb>, _>(details)
        .bind::<Nullable<BigInt>, _>(index)
        .bind::<SqlUuid, _>(token)
        .execute(connection).await?;
    Ok(())
}

pub async fn apply_import_strict(
    runtime: &PostgresRuntime,
    plan: StorageImportPlan,
) -> Result<(), PostgresStorageError> {
    let schema_limits = runtime.schema_limits();
    let items = plan.into_items();
    let result = runtime
        .with_transaction(async move |connection| {
            let mut state = ImportRuntime::for_plan(&items);
            for item in items {
                crate::runtime::task_execution_checkpoint()?;
                let (_, operation) = item.into_parts();
                state.publish(
                    execute_operation(schema_limits, connection, &state, operation).await?,
                );
            }
            Ok::<_, PostgresStorageError>(())
        })
        .await;
    if let Err(error) = &result {
        record_revision_condition(runtime, error);
    }
    result
}

pub async fn apply_import_best_effort(
    runtime: &PostgresRuntime,
    plan: StorageImportPlan,
    mode: StorageImportMode,
) -> Result<StorageImportApply, PostgresStorageError> {
    let schema_limits = runtime.schema_limits();
    let items = plan.into_items();
    let mut state = ImportRuntime::for_plan(&items);
    let mut outcomes = Vec::with_capacity(items.len());
    let mut aborted = false;
    for item in items {
        let (index, operation) = item.into_parts();
        let result = runtime
            .with_transaction(async |connection| {
                execute_operation(schema_limits, connection, &state, operation).await
            })
            .await;
        if let Err(error) = &result {
            record_revision_condition(runtime, error);
        }
        match result {
            Ok(change) => {
                state.publish(change);
                outcomes.push(StorageImportApplyItem::success(index));
            }
            Err(error) => {
                aborted = should_abort_best_effort(&error, &mode);
                outcomes.push(StorageImportApplyItem::failure(
                    index,
                    StorageError::from(error),
                ));
                if aborted {
                    break;
                }
            }
        }
    }
    Ok(StorageImportApply::new(outcomes, aborted))
}

fn record_revision_condition(runtime: &PostgresRuntime, error: &PostgresStorageError) {
    if error.kind() == StorageErrorKind::RevisionConflict {
        runtime.record_revision_condition("async_stale");
    }
}

async fn execute_operation(
    schema_limits: JsonSchemaLimits,
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    operation: StorageImportOperation,
) -> Result<Option<ImportReferenceChange>, PostgresStorageError> {
    match operation {
        StorageImportOperation::CreateCollection(input) => {
            let parent = resolve_collection_parent(connection, state, &input).await?;
            let reference = input.clone().into_parts().reference;
            let created = create_collection(connection, input, Some(parent.id().id())).await?;
            return Ok(
                reference.map(|reference| ImportReferenceChange::Collection(reference, created))
            );
        }
        StorageImportOperation::UpdateCollection {
            collection_id,
            input,
        } => {
            let reference = input.clone().into_parts().reference;
            let updated = update_collection(connection, collection_id.id(), input).await?;
            return Ok(
                reference.map(|reference| ImportReferenceChange::Collection(reference, updated))
            );
        }
        StorageImportOperation::CreateClass(input) => {
            let parts = input.clone().into_parts();
            let collection = resolve_collection(
                connection,
                state,
                parts.collection_ref.as_deref(),
                parts.collection_key.as_ref(),
            )
            .await?;
            let created =
                create_class(schema_limits, connection, input, collection.id().id()).await?;
            return Ok(parts
                .reference
                .map(|reference| ImportReferenceChange::Class(reference, created)));
        }
        StorageImportOperation::UpdateClass { class_id, input } => {
            let parts = input.clone().into_parts();
            let collection = resolve_collection(
                connection,
                state,
                parts.collection_ref.as_deref(),
                parts.collection_key.as_ref(),
            )
            .await?;
            let reference = parts.reference;
            let updated = update_class(
                schema_limits,
                connection,
                class_id.id(),
                collection.id().id(),
                input,
            )
            .await?;
            return Ok(reference.map(|reference| ImportReferenceChange::Class(reference, updated)));
        }
        StorageImportOperation::CreateObject(input) => {
            let parts = input.clone().into_parts();
            let class = resolve_class(
                connection,
                state,
                parts.class_ref.as_deref(),
                parts.class_key.as_ref(),
            )
            .await?;
            let created = create_object(schema_limits, connection, input, &class).await?;
            return Ok(parts
                .reference
                .map(|reference| ImportReferenceChange::Object(reference, created)));
        }
        StorageImportOperation::UpdateObject { object_id, input } => {
            let parts = input.clone().into_parts();
            let class = resolve_class(
                connection,
                state,
                parts.class_ref.as_deref(),
                parts.class_key.as_ref(),
            )
            .await?;
            let reference = parts.reference;
            let updated = update_object(
                schema_limits,
                connection,
                object_id.id(),
                class.id().id(),
                input,
            )
            .await?;
            return Ok(reference.map(|reference| ImportReferenceChange::Object(reference, updated)));
        }
        StorageImportOperation::UpsertIdentityScope { input, overwrite } => {
            return execute_identity_scope(connection, input, overwrite).await;
        }
        StorageImportOperation::UpsertGroup { input, overwrite } => {
            return execute_group(connection, state, input, overwrite).await;
        }
        StorageImportOperation::UpsertPrincipal { input, overwrite } => {
            return execute_principal(connection, state, input, overwrite).await;
        }
        StorageImportOperation::UpsertGroupMembership { input, overwrite } => {
            execute_group_membership(connection, state, input, overwrite).await?;
        }
        StorageImportOperation::UpsertComputedField { input, overwrite } => {
            execute_computed_field(connection, state, input, overwrite).await?;
        }
        StorageImportOperation::CreateClassRelation(input) => {
            create_class_relation(connection, state, input).await?;
        }
        StorageImportOperation::UpdateClassRelationTimestamps { input, timestamps } => {
            update_class_relation_timestamps(connection, state, &input, timestamps).await?;
        }
        StorageImportOperation::CheckClassRelationCondition(input) => {
            check_class_relation_condition(connection, state, &input).await?;
        }
        StorageImportOperation::CreateObjectRelation(input) => {
            create_object_relation(connection, state, input).await?;
        }
        StorageImportOperation::UpdateObjectRelationTimestamps { input, timestamps } => {
            update_object_relation_timestamps(connection, state, &input, timestamps).await?;
        }
        StorageImportOperation::CheckObjectRelationCondition(input) => {
            check_object_relation_condition(connection, state, &input).await?;
        }
        StorageImportOperation::ApplyCollectionPermissions { input, overwrite } => {
            apply_collection_permissions(connection, state, input, overwrite).await?;
        }
        StorageImportOperation::UpsertExportTemplate { input, overwrite } => {
            upsert_export_template(connection, state, input, overwrite).await?;
        }
        StorageImportOperation::UpsertRemoteTarget { input, overwrite } => {
            upsert_remote_target(connection, state, input, overwrite).await?;
        }
        StorageImportOperation::UpsertEventSink { input, overwrite } => {
            return execute_event_sink(connection, input, overwrite).await;
        }
        StorageImportOperation::UpsertEventSubscription { input, overwrite } => {
            upsert_event_subscription(connection, state, input, overwrite).await?;
        }
    }
    Ok(None)
}

async fn resolve_collection(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    reference: Option<&str>,
    key: Option<&StorageImportCollectionKey>,
) -> Result<StorageCollection, PostgresStorageError> {
    match (reference, key) {
        (Some(reference), None) => {
            state
                .collections_by_ref
                .get(reference)
                .cloned()
                .ok_or_else(|| {
                    PostgresStorageError::invalid_input(format!(
                        "Unknown collection ref '{reference}'"
                    ))
                })
        }
        (None, Some(key)) => collection_by_key_on_connection(connection, key.clone())
            .await?
            .ok_or_else(|| {
                PostgresStorageError::not_found(format!(
                    "Collection '{}' not found during execution",
                    collection_key_label(key)
                ))
            }),
        _ => Err(PostgresStorageError::invalid_input(
            "Exactly one of collection_ref or collection_key must be provided",
        )),
    }
}

async fn resolve_collection_parent(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportCollection,
) -> Result<StorageCollection, PostgresStorageError> {
    let parts = input.clone().into_parts();
    match (
        parts.parent_collection_ref.as_deref(),
        parts.parent_collection_key.as_ref(),
    ) {
        (None, None) => root_collection_on_connection(connection).await,
        (reference, key) => resolve_collection(connection, state, reference, key).await,
    }
}

async fn resolve_class(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    reference: Option<&str>,
    key: Option<&StorageImportClassKey>,
) -> Result<StorageClass, PostgresStorageError> {
    match (reference, key) {
        (Some(reference), None) => state.classes_by_ref.get(reference).cloned().ok_or_else(|| {
            PostgresStorageError::invalid_input(format!("Unknown class ref '{reference}'"))
        }),
        (None, Some(key)) => {
            let parts = key.clone().into_parts();
            let collection = resolve_collection(
                connection,
                state,
                parts.collection_ref.as_deref(),
                parts.collection_key.as_ref(),
            )
            .await?;
            class_by_name_on_connection(connection, collection.id().id(), &parts.name)
                .await?
                .ok_or_else(|| {
                    PostgresStorageError::not_found(format!(
                        "Class '{}' not found in collection '{}' during execution",
                        parts.name,
                        collection.name()
                    ))
                })
        }
        _ => Err(PostgresStorageError::invalid_input(
            "Exactly one of class_ref or class_key must be provided",
        )),
    }
}

async fn resolve_object(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    reference: Option<&str>,
    key: Option<&StorageImportObjectKey>,
) -> Result<StorageObject, PostgresStorageError> {
    match (reference, key) {
        (Some(reference), None) => state.objects_by_ref.get(reference).cloned().ok_or_else(|| {
            PostgresStorageError::invalid_input(format!("Unknown object ref '{reference}'"))
        }),
        (None, Some(key)) => {
            let parts = key.clone().into_parts();
            let class = resolve_class(
                connection,
                state,
                parts.class_ref.as_deref(),
                parts.class_key.as_ref(),
            )
            .await?;
            object_by_name_on_connection(connection, class.id().id(), &parts.name)
                .await?
                .ok_or_else(|| {
                    PostgresStorageError::not_found(format!(
                        "Object '{}' not found in class '{}' during execution",
                        parts.name,
                        class.name()
                    ))
                })
        }
        _ => Err(PostgresStorageError::invalid_input(
            "Exactly one of object_ref or object_key must be provided",
        )),
    }
}

fn collection_key_label(key: &StorageImportCollectionKey) -> String {
    let parts = key.clone().into_parts();
    parts
        .path
        .map_or(parts.name, |path| format!("/{}", path.join("/")))
}

fn assert_import_revision(
    condition: Option<StorageImportWriteCondition>,
    current_revision: PostgresRevision,
) -> Result<(), PostgresStorageError> {
    let Some(expected) = condition.and_then(StorageImportWriteCondition::expected_revision) else {
        return Ok(());
    };
    if expected == current_revision.get() {
        return Ok(());
    }
    Err(PostgresStorageError::precondition_failed(
        format!(
            "stale_revision: expected revision {expected}, observed {}",
            current_revision.get()
        ),
        Some(current_revision.into_domain()),
    ))
}

fn assert_import_create_condition(
    condition: Option<StorageImportWriteCondition>,
) -> Result<(), PostgresStorageError> {
    if condition.is_some_and(StorageImportWriteCondition::requires_existing) {
        return Err(PostgresStorageError::precondition_failed(
            "conditional_import_target_missing",
            None,
        ));
    }
    Ok(())
}

fn require_existing<T>(
    target: Option<T>,
    condition: Option<StorageImportWriteCondition>,
) -> Result<T, PostgresStorageError> {
    match target {
        Some(target) => Ok(target),
        None => {
            assert_import_create_condition(condition)?;
            Err(PostgresStorageError::not_found(
                "Import target was not found",
            ))
        }
    }
}

async fn create_collection(
    connection: &mut PostgresConnection,
    input: StorageImportCollection,
    parent_collection_id: Option<i32>,
) -> Result<StorageCollection, PostgresStorageError> {
    let parts = input.into_parts();
    assert_import_create_condition(parts.condition)?;
    let row = match parts.timestamps {
        Some(timestamps) => {
            let (created_at, updated_at) = import_timestamp_pair(timestamps);
            diesel::insert_into(crate::schema::collections::table)
                .values((
                    crate::schema::collections::name.eq(parts.name),
                    crate::schema::collections::description.eq(parts.description),
                    crate::schema::collections::parent_collection_id.eq(parent_collection_id),
                    crate::schema::collections::created_at.eq(created_at),
                    crate::schema::collections::updated_at.eq(updated_at),
                ))
                .returning(CollectionRow::as_select())
                .get_result::<CollectionRow>(connection)
                .await?
        }
        None => {
            diesel::insert_into(crate::schema::collections::table)
                .values((
                    crate::schema::collections::name.eq(parts.name),
                    crate::schema::collections::description.eq(parts.description),
                    crate::schema::collections::parent_collection_id.eq(parent_collection_id),
                ))
                .returning(CollectionRow::as_select())
                .get_result::<CollectionRow>(connection)
                .await?
        }
    };
    let collection = row.into_storage()?;
    if let Some(parent_collection_id) = parent_collection_id {
        insert_collection_closure_rows(connection, collection.id().id(), parent_collection_id)
            .await?;
    }
    Ok(collection)
}

async fn update_collection(
    connection: &mut PostgresConnection,
    collection_id: i32,
    input: StorageImportCollection,
) -> Result<StorageCollection, PostgresStorageError> {
    let parts = input.into_parts();
    let current = crate::schema::collections::table
        .filter(crate::schema::collections::id.eq(collection_id))
        .select(crate::schema::collections::revision)
        .for_update()
        .first::<PostgresRevision>(connection)
        .await
        .optional()?;
    assert_import_revision(parts.condition, require_existing(current, parts.condition)?)?;
    let updated = if let Some(timestamps) = parts.timestamps {
        let (created_at, updated_at) = import_timestamp_pair(timestamps);
        with_imported_timestamp_override(connection, async |connection| {
            updated_or_current(
                diesel::update(
                    crate::schema::collections::table
                        .filter(crate::schema::collections::id.eq(collection_id)),
                )
                .set((
                    crate::schema::collections::name.eq(parts.name),
                    crate::schema::collections::description.eq(parts.description),
                    crate::schema::collections::created_at.eq(created_at),
                    crate::schema::collections::updated_at.eq(updated_at),
                ))
                .returning(CollectionRow::as_select())
                .get_result::<CollectionRow>(connection)
                .await
                .optional(),
                async || {
                    crate::schema::collections::table
                        .filter(crate::schema::collections::id.eq(collection_id))
                        .select(CollectionRow::as_select())
                        .first::<CollectionRow>(connection)
                        .await
                },
            )
            .await
            .map_err(PostgresStorageError::from)
        })
        .await?
    } else {
        updated_or_current(
            diesel::update(
                crate::schema::collections::table
                    .filter(crate::schema::collections::id.eq(collection_id)),
            )
            .set((
                crate::schema::collections::name.eq(parts.name),
                crate::schema::collections::description.eq(parts.description),
            ))
            .returning(CollectionRow::as_select())
            .get_result::<CollectionRow>(connection)
            .await
            .optional(),
            async || {
                crate::schema::collections::table
                    .filter(crate::schema::collections::id.eq(collection_id))
                    .select(CollectionRow::as_select())
                    .first::<CollectionRow>(connection)
                    .await
            },
        )
        .await?
    };
    updated.into_storage()
}

async fn create_class(
    schema_limits: JsonSchemaLimits,
    connection: &mut PostgresConnection,
    input: StorageImportClass,
    collection_id: i32,
) -> Result<StorageClass, PostgresStorageError> {
    let parts = input.into_parts();
    if parts.schema_activation.is_some() {
        return Err(PostgresStorageError::invalid_input(
            "Schema activation requires an existing class",
        ));
    }
    assert_import_create_condition(parts.condition)?;
    hubuum_storage_core::StorageValidatedSchemaPolicy::try_new_with_limits(
        parts.schema_policy.clone(),
        schema_limits,
    )
    .map_err(|_| PostgresStorageError::invalid_input("Invalid imported JSON Schema policy"))?;
    let (json_schema, validate_schema) = parts.schema_policy.into_parts();
    let row = match parts.timestamps {
        Some(timestamps) => {
            let (created_at, updated_at) = import_timestamp_pair(timestamps);
            diesel::insert_into(crate::schema::hubuumclass::table)
                .values((
                    crate::schema::hubuumclass::name.eq(parts.name),
                    crate::schema::hubuumclass::collection_id.eq(collection_id),
                    crate::schema::hubuumclass::json_schema.eq(json_schema),
                    crate::schema::hubuumclass::validate_schema.eq(validate_schema),
                    crate::schema::hubuumclass::description.eq(parts.description),
                    crate::schema::hubuumclass::created_at.eq(created_at),
                    crate::schema::hubuumclass::updated_at.eq(updated_at),
                ))
                .returning(ClassRow::as_select())
                .get_result::<ClassRow>(connection)
                .await?
        }
        None => {
            diesel::insert_into(crate::schema::hubuumclass::table)
                .values((
                    crate::schema::hubuumclass::name.eq(parts.name),
                    crate::schema::hubuumclass::collection_id.eq(collection_id),
                    crate::schema::hubuumclass::json_schema.eq(json_schema),
                    crate::schema::hubuumclass::validate_schema.eq(validate_schema),
                    crate::schema::hubuumclass::description.eq(parts.description),
                ))
                .returning(ClassRow::as_select())
                .get_result::<ClassRow>(connection)
                .await?
        }
    };
    record_class_schema_on(schema_limits, connection, &row, &ambient_event_context()).await?;
    row.into_storage()
}

async fn update_class(
    schema_limits: JsonSchemaLimits,
    connection: &mut PostgresConnection,
    class_id: i32,
    authorized_collection: i32,
    input: StorageImportClass,
) -> Result<StorageClass, PostgresStorageError> {
    let parts = input.into_parts();
    hubuum_storage_core::StorageValidatedSchemaPolicy::try_new_with_limits(
        parts.schema_policy.clone(),
        schema_limits,
    )
    .map_err(|_| PostgresStorageError::invalid_input("Invalid imported JSON Schema policy"))?;
    super::computed_materialization::acquire_computed_class_exclusive_lock(connection, class_id)
        .await?;
    let current = crate::schema::hubuumclass::table
        .filter(crate::schema::hubuumclass::id.eq(class_id))
        .select(ClassRow::as_select())
        .for_update()
        .first::<ClassRow>(connection)
        .await
        .optional()?;
    let before = require_existing(current, parts.condition)?;
    assert_import_revision(parts.condition, before.revision)?;
    if before.collection_id != authorized_collection {
        return Err(PostgresStorageError::conflict(
            "Class moved outside the import collection",
        ));
    }
    if let Some(intent) = &parts.schema_activation {
        super::schema_evolution::activate_import_schema_on(
            schema_limits,
            connection,
            &before,
            intent,
            &parts.schema_policy,
        )
        .await?;
    }
    let (json_schema, validate_schema) = parts.schema_policy.into_parts();
    let values = (
        crate::schema::hubuumclass::name.eq(parts.name),
        crate::schema::hubuumclass::json_schema.eq(json_schema),
        crate::schema::hubuumclass::validate_schema.eq(validate_schema),
        crate::schema::hubuumclass::description.eq(parts.description),
    );
    let row = if let Some(timestamps) = parts.timestamps {
        let (created_at, updated_at) = import_timestamp_pair(timestamps);
        with_imported_timestamp_override(connection, async |connection| {
            updated_or_current(
                diesel::update(
                    crate::schema::hubuumclass::table
                        .filter(crate::schema::hubuumclass::id.eq(class_id)),
                )
                .set((
                    values,
                    crate::schema::hubuumclass::created_at.eq(created_at),
                    crate::schema::hubuumclass::updated_at.eq(updated_at),
                ))
                .returning(ClassRow::as_select())
                .get_result::<ClassRow>(connection)
                .await
                .optional(),
                async || {
                    crate::schema::hubuumclass::table
                        .filter(crate::schema::hubuumclass::id.eq(class_id))
                        .select(ClassRow::as_select())
                        .first::<ClassRow>(connection)
                        .await
                },
            )
            .await
            .map_err(PostgresStorageError::from)
        })
        .await?
    } else {
        updated_or_current(
            diesel::update(
                crate::schema::hubuumclass::table
                    .filter(crate::schema::hubuumclass::id.eq(class_id)),
            )
            .set(values)
            .returning(ClassRow::as_select())
            .get_result::<ClassRow>(connection)
            .await
            .optional(),
            async || {
                crate::schema::hubuumclass::table
                    .filter(crate::schema::hubuumclass::id.eq(class_id))
                    .select(ClassRow::as_select())
                    .first::<ClassRow>(connection)
                    .await
            },
        )
        .await?
    };
    if parts.schema_activation.is_none()
        && (row.json_schema != before.json_schema || row.validate_schema != before.validate_schema)
    {
        record_class_schema_on(schema_limits, connection, &row, &ambient_event_context()).await?;
    }
    row.into_storage()
}

async fn create_object(
    schema_limits: JsonSchemaLimits,
    connection: &mut PostgresConnection,
    input: StorageImportObject,
    class: &StorageClass,
) -> Result<StorageObject, PostgresStorageError> {
    let parts = input.into_parts();
    assert_import_create_condition(parts.condition)?;
    acquire_computed_class_shared_lock(connection, class.id().id()).await?;
    validate_import_object_on(
        schema_limits,
        connection,
        class.id(),
        Some(class.collection_id()),
        &parts.data,
    )
    .await?;
    let row = match parts.timestamps {
        Some(timestamps) => {
            let (created_at, updated_at) = import_timestamp_pair(timestamps);
            diesel::insert_into(crate::schema::hubuumobject::table)
                .values((
                    crate::schema::hubuumobject::name.eq(parts.name),
                    crate::schema::hubuumobject::collection_id.eq(class.collection_id().id()),
                    crate::schema::hubuumobject::hubuum_class_id.eq(class.id().id()),
                    crate::schema::hubuumobject::data.eq(parts.data),
                    crate::schema::hubuumobject::description.eq(parts.description),
                    crate::schema::hubuumobject::created_at.eq(created_at),
                    crate::schema::hubuumobject::updated_at.eq(updated_at),
                ))
                .returning(ObjectRow::as_select())
                .get_result::<ObjectRow>(connection)
                .await?
        }
        None => {
            diesel::insert_into(crate::schema::hubuumobject::table)
                .values((
                    crate::schema::hubuumobject::name.eq(parts.name),
                    crate::schema::hubuumobject::collection_id.eq(class.collection_id().id()),
                    crate::schema::hubuumobject::hubuum_class_id.eq(class.id().id()),
                    crate::schema::hubuumobject::data.eq(parts.data),
                    crate::schema::hubuumobject::description.eq(parts.description),
                ))
                .returning(ObjectRow::as_select())
                .get_result::<ObjectRow>(connection)
                .await?
        }
    };
    materialize_object_on_connection(connection, row.id, row.hubuum_class_id, &row.data).await?;
    record_object_schema_on(schema_limits, connection, &row, &ambient_event_context()).await?;
    row.into_storage()
}

async fn update_object(
    schema_limits: JsonSchemaLimits,
    connection: &mut PostgresConnection,
    object_id: i32,
    selected_class: i32,
    input: StorageImportObject,
) -> Result<StorageObject, PostgresStorageError> {
    let parts = input.into_parts();
    let class_id = crate::schema::hubuumobject::table
        .filter(crate::schema::hubuumobject::id.eq(object_id))
        .select(crate::schema::hubuumobject::hubuum_class_id)
        .first::<i32>(connection)
        .await?;
    if class_id != selected_class {
        return Err(PostgresStorageError::not_found(
            "Imported object no longer belongs to the selected class",
        ));
    }
    acquire_computed_class_shared_lock(connection, class_id).await?;
    validate_import_object_on(
        schema_limits,
        connection,
        ClassId::new(class_id)?,
        None,
        &parts.data,
    )
    .await?;
    let current = crate::schema::hubuumobject::table
        .filter(crate::schema::hubuumobject::id.eq(object_id))
        .select((
            crate::schema::hubuumobject::revision,
            crate::schema::hubuumobject::hubuum_class_id,
        ))
        .for_update()
        .first::<(PostgresRevision, i32)>(connection)
        .await
        .optional()?;
    let (current_revision, current_class) = require_existing(current, parts.condition)?;
    if current_class != class_id {
        return Err(PostgresStorageError::conflict(
            "Object class changed during import",
        ));
    }
    assert_import_revision(parts.condition, current_revision)?;
    let values = (
        crate::schema::hubuumobject::name.eq(parts.name),
        crate::schema::hubuumobject::data.eq(parts.data),
        crate::schema::hubuumobject::description.eq(parts.description),
    );
    let row = if let Some(timestamps) = parts.timestamps {
        let (created_at, updated_at) = import_timestamp_pair(timestamps);
        with_imported_timestamp_override(connection, async |connection| {
            updated_or_current(
                diesel::update(
                    crate::schema::hubuumobject::table
                        .filter(crate::schema::hubuumobject::id.eq(object_id)),
                )
                .set((
                    values,
                    crate::schema::hubuumobject::created_at.eq(created_at),
                    crate::schema::hubuumobject::updated_at.eq(updated_at),
                ))
                .returning(ObjectRow::as_select())
                .get_result::<ObjectRow>(connection)
                .await
                .optional(),
                async || {
                    crate::schema::hubuumobject::table
                        .filter(crate::schema::hubuumobject::id.eq(object_id))
                        .select(ObjectRow::as_select())
                        .first::<ObjectRow>(connection)
                        .await
                },
            )
            .await
            .map_err(PostgresStorageError::from)
        })
        .await?
    } else {
        updated_or_current(
            diesel::update(
                crate::schema::hubuumobject::table
                    .filter(crate::schema::hubuumobject::id.eq(object_id)),
            )
            .set(values)
            .returning(ObjectRow::as_select())
            .get_result::<ObjectRow>(connection)
            .await
            .optional(),
            async || {
                crate::schema::hubuumobject::table
                    .filter(crate::schema::hubuumobject::id.eq(object_id))
                    .select(ObjectRow::as_select())
                    .first::<ObjectRow>(connection)
                    .await
            },
        )
        .await?
    };
    materialize_object_on_connection(connection, row.id, row.hubuum_class_id, &row.data).await?;
    record_object_schema_on(schema_limits, connection, &row, &ambient_event_context()).await?;
    row.into_storage()
}

async fn with_imported_timestamp_override<F, R>(
    connection: &mut PostgresConnection,
    operation: F,
) -> Result<R, PostgresStorageError>
where
    F: for<'connection> AsyncFnOnce(
            &'connection mut PostgresConnection,
        ) -> Result<R, PostgresStorageError>
        + for<'connection> SendAsyncFn<
            &'connection mut PostgresConnection,
            Result<R, PostgresStorageError>,
            Fut: Send,
        > + Send,
    R: Send,
{
    connection
        .transaction::<R, PostgresStorageError, _>(async move |connection| {
            let previous = diesel::select(diesel::dsl::sql::<
                diesel::sql_types::Nullable<diesel::sql_types::Text>,
            >(
                "current_setting('hubuum.preserve_imported_timestamps', true)",
            ))
            .get_result::<Option<String>>(connection)
            .await?;
            set_imported_timestamp_override(connection, "on").await?;
            let result = operation(connection).await?;
            set_imported_timestamp_override(connection, previous.as_deref().unwrap_or("off"))
                .await?;
            Ok(result)
        })
        .await
}

async fn set_imported_timestamp_override(
    connection: &mut PostgresConnection,
    value: &str,
) -> Result<(), PostgresStorageError> {
    diesel::sql_query("SELECT set_config('hubuum.preserve_imported_timestamps', $1, true)")
        .bind::<diesel::sql_types::Text, _>(value)
        .execute(connection)
        .await?;
    Ok(())
}

async fn updated_or_current<T, E>(
    updated: Result<Option<T>, E>,
    select_current: impl AsyncFnOnce() -> Result<T, E>,
) -> Result<T, E> {
    match updated? {
        Some(row) => Ok(row),
        None => select_current().await,
    }
}

fn should_abort_preflight(error: &PostgresStorageError, mode: &StorageImportMode) -> bool {
    if mode.atomicity() == StorageImportAtomicity::Strict {
        return true;
    }
    should_abort_for_policy(error, mode, true)
}

fn should_abort_best_effort(error: &PostgresStorageError, mode: &StorageImportMode) -> bool {
    should_abort_for_policy(error, mode, false)
}

fn should_abort_for_policy(
    error: &PostgresStorageError,
    mode: &StorageImportMode,
    include_precondition: bool,
) -> bool {
    match error.kind() {
        StorageErrorKind::PermissionDenied | StorageErrorKind::AuthenticationRequired => {
            mode.permission_policy() == StorageImportPermissionPolicy::Abort
        }
        StorageErrorKind::Conflict => {
            mode.collision_policy() == StorageImportCollisionPolicy::Abort
        }
        StorageErrorKind::RevisionConflict if include_precondition => {
            mode.collision_policy() == StorageImportCollisionPolicy::Abort
        }
        _ => false,
    }
}

fn imported_timestamps(
    timestamps: Option<StorageImportTimestamps>,
) -> (NaiveDateTime, NaiveDateTime) {
    timestamps.map_or_else(
        || {
            let now = Utc::now().naive_utc();
            (now, now)
        },
        import_timestamp_pair,
    )
}

fn import_timestamp_pair(timestamps: StorageImportTimestamps) -> (NaiveDateTime, NaiveDateTime) {
    let (created_at, updated_at) = timestamps.as_pair();
    (created_at.naive_utc(), updated_at.naive_utc())
}

async fn execute_computed_field(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportComputedField,
    overwrite: bool,
) -> Result<(), PostgresStorageError> {
    let parts = input.into_parts();
    let definition = parts.definition;
    let key = definition.key().as_str().to_string();
    let label = definition.label().to_string();
    let description = definition.description().to_string();
    let operation = serde_json::to_value(definition.operation())
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    let result_type = computed_result_type_name(definition.result_type()).to_string();
    let enabled = definition.enabled();
    let semantics_version = definition.semantics_version();
    let class = resolve_class(
        connection,
        state,
        parts.class_ref.as_deref(),
        parts.class_key.as_ref(),
    )
    .await?;
    let (visibility, owner_id) = match parts.visibility {
        StorageImportComputedFieldVisibility::Shared => ("shared", None),
        StorageImportComputedFieldVisibility::Personal => (
            "personal",
            Some(
                resolve_principal(
                    connection,
                    state,
                    parts.owner_ref.as_deref(),
                    parts.owner_key.as_ref(),
                )
                .await?,
            ),
        ),
    };
    let existing = crate::schema::computed_field_definitions::table
        .filter(crate::schema::computed_field_definitions::class_id.eq(class.id().id()))
        .filter(crate::schema::computed_field_definitions::visibility.eq(visibility))
        .filter(crate::schema::computed_field_definitions::key.eq(&key))
        .filter(
            crate::schema::computed_field_definitions::owner_user_id.is_not_distinct_from(owner_id),
        )
        .for_update()
        .select(ComputedDefinitionRow::as_select())
        .first::<ComputedDefinitionRow>(connection)
        .await
        .optional()?;

    let changed = match existing {
        Some(existing) => {
            assert_import_revision(parts.condition, existing.revision())?;
            if !overwrite {
                return Err(PostgresStorageError::conflict(format!(
                    "Computed field '{}' already exists in its scope",
                    key
                )));
            }
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((existing.created_at(), existing.updated_at()));
            with_imported_timestamp_override(connection, async |connection| {
                let updated = diesel::update(
                    crate::schema::computed_field_definitions::table
                        .filter(crate::schema::computed_field_definitions::id.eq(existing.id())),
                )
                .set((
                    crate::schema::computed_field_definitions::label.eq(label),
                    crate::schema::computed_field_definitions::description.eq(description),
                    crate::schema::computed_field_definitions::operation.eq(operation),
                    crate::schema::computed_field_definitions::result_type.eq(result_type),
                    crate::schema::computed_field_definitions::enabled.eq(enabled),
                    crate::schema::computed_field_definitions::created_at.eq(created_at),
                    crate::schema::computed_field_definitions::updated_at.eq(updated_at),
                ))
                .returning(crate::schema::computed_field_definitions::id)
                .get_result::<i32>(connection)
                .await
                .optional()?;
                Ok(updated.is_some())
            })
            .await?
        }
        None => {
            assert_import_create_condition(parts.condition)?;
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::computed_field_definitions::table)
                .values((
                    crate::schema::computed_field_definitions::class_id.eq(class.id().id()),
                    crate::schema::computed_field_definitions::visibility.eq(visibility),
                    crate::schema::computed_field_definitions::owner_user_id.eq(owner_id),
                    crate::schema::computed_field_definitions::key.eq(key),
                    crate::schema::computed_field_definitions::label.eq(label),
                    crate::schema::computed_field_definitions::description.eq(description),
                    crate::schema::computed_field_definitions::operation.eq(operation),
                    crate::schema::computed_field_definitions::result_type.eq(result_type),
                    crate::schema::computed_field_definitions::enabled.eq(enabled),
                    crate::schema::computed_field_definitions::semantics_version
                        .eq(semantics_version),
                    crate::schema::computed_field_definitions::created_by.eq(owner_id),
                    crate::schema::computed_field_definitions::updated_by.eq(owner_id),
                    crate::schema::computed_field_definitions::created_at.eq(created_at),
                    crate::schema::computed_field_definitions::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
            true
        }
    };
    if changed && parts.visibility == StorageImportComputedFieldVisibility::Shared {
        advance_revision_and_enqueue_on_connection(connection, class.id().id(), None).await?;
    }
    Ok(())
}

const fn computed_result_type_name(value: ResultType) -> &'static str {
    match value {
        ResultType::String => "string",
        ResultType::Number => "number",
        ResultType::Integer => "integer",
        ResultType::Boolean => "boolean",
        ResultType::Object => "object",
        ResultType::Array => "array",
    }
}

async fn apply_collection_permissions(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportCollectionPermission,
    overwrite: bool,
) -> Result<(), PostgresStorageError> {
    let parts = input.into_parts();
    let collection = resolve_collection(
        connection,
        state,
        parts.collection_ref.as_deref(),
        parts.collection_key.as_ref(),
    )
    .await?;
    let group_id = resolve_group(connection, state, None, Some(&parts.group_key)).await?;
    let authorization_revision = crate::schema::collection_authorization_state::table
        .filter(
            crate::schema::collection_authorization_state::collection_id.eq(collection.id().id()),
        )
        .select(crate::schema::collection_authorization_state::revision)
        .for_update()
        .first::<PostgresRevision>(connection)
        .await
        .optional()?;
    assert_import_revision(
        parts.condition,
        require_existing(authorization_revision, parts.condition)?,
    )?;

    let existing = crate::schema::permissions::table
        .filter(crate::schema::permissions::collection_id.eq(collection.id().id()))
        .filter(crate::schema::permissions::group_id.eq(group_id))
        .select(crate::schema::permissions::id)
        .first::<i32>(connection)
        .await
        .optional()?;
    if existing.is_some() && !overwrite {
        return Err(PostgresStorageError::conflict(format!(
            "Permissions for group {group_id} already exist on collection {}",
            collection.id().id()
        )));
    }
    if existing.is_some() {
        diesel::update(
            crate::schema::permissions::table
                .filter(crate::schema::permissions::collection_id.eq(collection.id().id()))
                .filter(crate::schema::permissions::group_id.eq(group_id)),
        )
        .set(UpdatePermission::grant(
            &parts.permissions,
            parts.replace_existing,
        ))
        .execute(connection)
        .await?;
    } else {
        diesel::insert_into(crate::schema::permissions::table)
            .values(NewPermission::new(
                collection.id().id(),
                group_id,
                &parts.permissions,
            ))
            .execute(connection)
            .await?;
    }
    Ok(())
}

fn assert_upsert_condition(
    revision: Option<PostgresRevision>,
    condition: Option<StorageImportWriteCondition>,
) -> Result<(), PostgresStorageError> {
    match revision {
        Some(revision) => assert_import_revision(condition, revision),
        None => assert_import_create_condition(condition),
    }
}

fn ensure_class_collection(
    class: &StorageClass,
    collection: &StorageCollection,
    resource: &str,
) -> Result<(), PostgresStorageError> {
    if class.collection_id().id() == collection.id().id() {
        Ok(())
    } else {
        Err(PostgresStorageError::invalid_input(format!(
            "{resource} class {} belongs to collection {}, not target collection {}",
            class.id(),
            class.collection_id(),
            collection.id().id()
        )))
    }
}

async fn observed_revision(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    operation: &StorageImportOperation,
) -> Result<Option<PostgresRevision>, PostgresStorageError> {
    let revision = match operation {
        StorageImportOperation::UpsertIdentityScope { input, .. } => {
            let parts = input.clone().into_parts();
            crate::schema::identity_scopes::table
                .filter(crate::schema::identity_scopes::name.eq(parts.name))
                .select(crate::schema::identity_scopes::revision)
                .first::<PostgresRevision>(connection)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertGroup { input, .. } => {
            let parts = input.clone().into_parts();
            let scope_id = resolve_identity_scope(
                connection,
                state,
                parts.identity_scope_ref.as_deref(),
                parts.identity_scope_key.as_ref(),
            )
            .await?;
            crate::schema::groups::table
                .filter(crate::schema::groups::identity_scope_id.eq(scope_id))
                .filter(crate::schema::groups::groupname.eq(parts.name))
                .select(crate::schema::groups::revision)
                .first::<PostgresRevision>(connection)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertPrincipal { input, .. } => {
            let parts = input.clone().into_parts();
            let scope_id = resolve_identity_scope(
                connection,
                state,
                parts.identity_scope_ref.as_deref(),
                parts.identity_scope_key.as_ref(),
            )
            .await?;
            crate::schema::principals::table
                .filter(crate::schema::principals::identity_scope_id.eq(scope_id))
                .filter(crate::schema::principals::name.eq(parts.name))
                .select(crate::schema::principals::revision)
                .first::<PostgresRevision>(connection)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertGroupMembership { input, .. } => {
            let parts = input.clone().into_parts();
            let principal_id = resolve_principal(
                connection,
                state,
                parts.principal_ref.as_deref(),
                parts.principal_key.as_ref(),
            )
            .await?;
            let group_id = resolve_group(
                connection,
                state,
                parts.group_ref.as_deref(),
                parts.group_key.as_ref(),
            )
            .await?;
            crate::schema::group_memberships::table
                .filter(crate::schema::group_memberships::principal_id.eq(principal_id))
                .filter(crate::schema::group_memberships::group_id.eq(group_id))
                .select(crate::schema::group_memberships::revision)
                .first::<PostgresRevision>(connection)
                .await
                .optional()?
        }
        StorageImportOperation::UpdateCollection { collection_id, .. } => {
            crate::schema::collections::table
                .filter(crate::schema::collections::id.eq(collection_id.id()))
                .select(crate::schema::collections::revision)
                .first::<PostgresRevision>(connection)
                .await
                .optional()?
        }
        StorageImportOperation::UpdateClass { class_id, .. } => crate::schema::hubuumclass::table
            .filter(crate::schema::hubuumclass::id.eq(class_id.id()))
            .select(crate::schema::hubuumclass::revision)
            .first::<PostgresRevision>(connection)
            .await
            .optional()?,
        StorageImportOperation::UpdateObject { object_id, .. } => {
            crate::schema::hubuumobject::table
                .filter(crate::schema::hubuumobject::id.eq(object_id.id()))
                .select(crate::schema::hubuumobject::revision)
                .first::<PostgresRevision>(connection)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertComputedField { input, .. } => {
            let parts = input.clone().into_parts();
            let class = resolve_class(
                connection,
                state,
                parts.class_ref.as_deref(),
                parts.class_key.as_ref(),
            )
            .await?;
            let (visibility, owner_id) = match parts.visibility {
                StorageImportComputedFieldVisibility::Shared => ("shared", None),
                StorageImportComputedFieldVisibility::Personal => (
                    "personal",
                    Some(
                        resolve_principal(
                            connection,
                            state,
                            parts.owner_ref.as_deref(),
                            parts.owner_key.as_ref(),
                        )
                        .await?,
                    ),
                ),
            };
            crate::schema::computed_field_definitions::table
                .filter(crate::schema::computed_field_definitions::class_id.eq(class.id().id()))
                .filter(crate::schema::computed_field_definitions::visibility.eq(visibility))
                .filter(
                    crate::schema::computed_field_definitions::key
                        .eq(parts.definition.key().as_str()),
                )
                .filter(
                    crate::schema::computed_field_definitions::owner_user_id
                        .is_not_distinct_from(owner_id),
                )
                .select(crate::schema::computed_field_definitions::revision)
                .first::<PostgresRevision>(connection)
                .await
                .optional()?
        }
        StorageImportOperation::UpdateClassRelationTimestamps { input, .. }
        | StorageImportOperation::CheckClassRelationCondition(input) => {
            class_relation_observed_revision(connection, state, input).await?
        }
        StorageImportOperation::UpdateObjectRelationTimestamps { input, .. }
        | StorageImportOperation::CheckObjectRelationCondition(input) => {
            object_relation_observed_revision(connection, state, input).await?
        }
        StorageImportOperation::ApplyCollectionPermissions { input, .. } => {
            let parts = input.clone().into_parts();
            let collection = resolve_collection(
                connection,
                state,
                parts.collection_ref.as_deref(),
                parts.collection_key.as_ref(),
            )
            .await?;
            crate::schema::collection_authorization_state::table
                .filter(
                    crate::schema::collection_authorization_state::collection_id
                        .eq(collection.id().id()),
                )
                .select(crate::schema::collection_authorization_state::revision)
                .first::<PostgresRevision>(connection)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertExportTemplate { input, .. } => {
            let parts = input.clone().into_parts();
            let collection = resolve_collection(
                connection,
                state,
                parts.collection_ref.as_deref(),
                parts.collection_key.as_ref(),
            )
            .await?;
            crate::schema::export_templates::table
                .filter(crate::schema::export_templates::collection_id.eq(collection.id().id()))
                .filter(crate::schema::export_templates::name.eq(parts.name))
                .select(crate::schema::export_templates::revision)
                .first::<PostgresRevision>(connection)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertRemoteTarget { input, .. } => {
            let parts = input.clone().into_parts();
            let collection = resolve_collection(
                connection,
                state,
                parts.collection_ref.as_deref(),
                parts.collection_key.as_ref(),
            )
            .await?;
            crate::schema::remote_targets::table
                .filter(crate::schema::remote_targets::collection_id.eq(collection.id().id()))
                .filter(crate::schema::remote_targets::name.eq(parts.name))
                .select(crate::schema::remote_targets::revision)
                .first::<PostgresRevision>(connection)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertEventSink { input, .. } => {
            let parts = input.clone().into_parts();
            crate::schema::event_sinks::table
                .filter(crate::schema::event_sinks::name.eq(parts.name))
                .select(crate::schema::event_sinks::revision)
                .first::<PostgresRevision>(connection)
                .await
                .optional()?
        }
        StorageImportOperation::UpsertEventSubscription { input, .. } => {
            let parts = input.clone().into_parts();
            let collection = resolve_collection(
                connection,
                state,
                parts.collection_ref.as_deref(),
                parts.collection_key.as_ref(),
            )
            .await?;
            crate::schema::event_subscriptions::table
                .filter(crate::schema::event_subscriptions::collection_id.eq(collection.id().id()))
                .filter(crate::schema::event_subscriptions::name.eq(parts.name))
                .select(crate::schema::event_subscriptions::revision)
                .first::<PostgresRevision>(connection)
                .await
                .optional()?
        }
        StorageImportOperation::CreateCollection(_)
        | StorageImportOperation::CreateClass(_)
        | StorageImportOperation::CreateObject(_)
        | StorageImportOperation::CreateClassRelation(_)
        | StorageImportOperation::CreateObjectRelation(_) => None,
    };
    Ok(revision)
}
