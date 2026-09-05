//! Atomic PostgreSQL execution for validated backend-neutral import plans.
use super::task_execution::{claimed_task, live_claimed_task};
use hubuum_storage_core::{
    FencedImportItem, FencedImportPlan, FencedImportResults, StorageImportResult, StorageTaskLease,
};

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use argon2::{
    Argon2,
    password_hash::{PasswordHasher, phc::PasswordHash},
};
use chrono::{NaiveDateTime, Utc};
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::{PgExpressionMethods, SelectableHelper};
use diesel_async::{AsyncConnection, RunQueryDsl};
use hubuum_computed_fields::ResultType;
use hubuum_storage_core::{
    StorageClass, StorageClassRelationCreate, StorageCollection, StorageError, StorageErrorKind,
    StorageImportApply, StorageImportApplyItem, StorageImportAtomicity, StorageImportClass,
    StorageImportClassKey, StorageImportClassRelation, StorageImportCollection,
    StorageImportCollectionKey, StorageImportCollectionPermission, StorageImportCollisionPolicy,
    StorageImportComputedField, StorageImportComputedFieldVisibility, StorageImportEventSink,
    StorageImportEventSinkKey, StorageImportEventSubscription, StorageImportExportTemplate,
    StorageImportGroup, StorageImportGroupKey, StorageImportGroupMembership,
    StorageImportIdentityScope, StorageImportIdentityScopeKey, StorageImportMembershipSourceParts,
    StorageImportMode, StorageImportObject, StorageImportObjectKey, StorageImportObjectRelation,
    StorageImportOperation, StorageImportPermissionPolicy, StorageImportPlan,
    StorageImportPlanItem, StorageImportPreflight, StorageImportPreflightItem,
    StorageImportPrincipal, StorageImportPrincipalKey, StorageImportPrincipalParts,
    StorageImportPrincipalSubtype, StorageImportRemoteTarget, StorageImportTimestamps,
    StorageImportWriteCondition, StorageObject, StorageRemoteTargetSubjectType,
};
use hubuum_templates::{TemplateAutoEscape, TemplateLimits, validate_template_composition};
use tokio::sync::Semaphore;

use super::authorization::{NewPermission, UpdatePermission};
use super::class::ClassRow;
use super::collection::{CollectionRow, insert_collection_closure_rows};
use super::computed_definition::ComputedDefinitionRow;
use super::computed_fields::advance_revision_and_enqueue_on_connection;
use super::computed_materialization::materialize_object_on_connection;
use super::group::GroupRow;
use super::import_workflow::{
    class_by_name_on_connection, collection_by_key_on_connection, object_by_name_on_connection,
    root_collection_on_connection,
};
use super::object::ObjectRow;
use super::principal::PrincipalRow;
use super::relation::normalize_class_relation_create;
use super::service_account::ServiceAccountRow;
use super::user::UserRow;
use crate::{
    PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError, SendAsyncFn,
};

const DRY_RUN_ROLLBACK: &str = "hubuum import dry-run rollback";
const PASSWORD_WORK_MAX_CONCURRENCY: usize = 4;
const IMPORT_TEMPLATE_RECURSION_LIMIT: usize = 64;
const IMPORT_TEMPLATE_FUEL: u64 = 50_000;

static PASSWORD_WORK_SEMAPHORE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(PASSWORD_WORK_MAX_CONCURRENCY)));

#[derive(Clone, Default)]
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

impl ImportRuntime {
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
                        let (index, operation) = item.into_parts();
                        let observed_revision =
                            observed_revision(connection, &state, &operation).await;
                        let (revision, result) = match observed_revision {
                            Ok(revision) => {
                                let result = connection
                                    .transaction::<(), PostgresStorageError, _>(
                                        async |connection| {
                                            execute_operation(connection, &mut state, operation)
                                                .await
                                        },
                                    )
                                    .await;
                                (revision, result)
                            }
                            Err(error) => (None, Err(error)),
                        };
                        match result {
                            Ok(()) => outcomes.push(StorageImportPreflightItem::success(
                                index,
                                revision.map(PostgresRevision::into_domain),
                            )),
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
    let (lease, items) = plan.into_parts();
    runtime
        .with_transaction(async move |connection| {
            let mut state = ImportRuntime::for_plan(&fenced_operations(&items));
            for item in items {
                let (index, operation, result) = item.into_parts();
                if let Some(operation) = operation {
                    execute_operation(connection, &mut state, operation).await?;
                }
                record_execution_receipt(connection, &lease, Some(index), result).await?;
            }
            // Lock only at the commit boundary so long imports do not block lease
            // renewal. The deferred receipt trigger checks expiry again at commit.
            live_claimed_task(connection, claimed_task(&lease)?).await?;
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
    let (lease, items) = plan.into_parts();
    let mut state = ImportRuntime::for_plan(&fenced_operations(&items));
    let mut outcomes = Vec::new();
    let mut aborted = false;
    for item in items {
        let (index, operation, result) = item.into_parts();
        let receipt = result.clone();
        let before = state.clone();
        let outcome = runtime
            .with_transaction(async |connection| {
                if let Some(operation) = operation {
                    execute_operation(connection, &mut state, operation).await?;
                }
                record_execution_receipt(connection, &lease, Some(index), receipt).await?;
                live_claimed_task(connection, claimed_task(&lease)?).await?;
                Ok::<_, PostgresStorageError>(())
            })
            .await;
        match outcome {
            Ok(()) => outcomes.push(StorageImportApplyItem::success(index)),
            Err(error) => {
                state = before;
                record_revision_condition(runtime, &error);
                aborted = should_abort_best_effort(&error, &mode);
                let error = StorageError::from(error);
                runtime
                    .with_transaction(async |connection| {
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
    let items = plan.into_items();
    let result = runtime
        .with_transaction(async move |connection| {
            let mut state = ImportRuntime::for_plan(&items);
            for item in items {
                let (_, operation) = item.into_parts();
                execute_operation(connection, &mut state, operation).await?;
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
    let items = plan.into_items();
    let mut state = ImportRuntime::for_plan(&items);
    let mut outcomes = Vec::with_capacity(items.len());
    let mut aborted = false;
    for item in items {
        let (index, operation) = item.into_parts();
        let result = runtime
            .with_transaction(async |connection| {
                execute_operation(connection, &mut state, operation).await
            })
            .await;
        if let Err(error) = &result {
            record_revision_condition(runtime, error);
        }
        match result {
            Ok(()) => outcomes.push(StorageImportApplyItem::success(index)),
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
    connection: &mut PostgresConnection,
    state: &mut ImportRuntime,
    operation: StorageImportOperation,
) -> Result<(), PostgresStorageError> {
    match operation {
        StorageImportOperation::CreateCollection(input) => {
            let parent = resolve_collection_parent(connection, state, &input).await?;
            let reference = input.clone().into_parts().reference;
            let created = create_collection(connection, input, Some(parent.id().id())).await?;
            if let Some(reference) = reference {
                state.collections_by_ref.insert(reference, created);
            }
        }
        StorageImportOperation::UpdateCollection {
            collection_id,
            input,
        } => {
            let reference = input.clone().into_parts().reference;
            let updated = update_collection(connection, collection_id.id(), input).await?;
            if let Some(reference) = reference {
                state.collections_by_ref.insert(reference, updated);
            }
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
            let created = create_class(connection, input, collection.id().id()).await?;
            if let Some(reference) = parts.reference {
                state.classes_by_ref.insert(reference, created);
            }
        }
        StorageImportOperation::UpdateClass { class_id, input } => {
            let reference = input.clone().into_parts().reference;
            let updated = update_class(connection, class_id.id(), input).await?;
            if let Some(reference) = reference {
                state.classes_by_ref.insert(reference, updated);
            }
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
            let created = create_object(connection, input, &class).await?;
            if let Some(reference) = parts.reference {
                state.objects_by_ref.insert(reference, created);
            }
        }
        StorageImportOperation::UpdateObject { object_id, input } => {
            let reference = input.clone().into_parts().reference;
            let updated = update_object(connection, object_id.id(), input).await?;
            if let Some(reference) = reference {
                state.objects_by_ref.insert(reference, updated);
            }
        }
        StorageImportOperation::UpsertIdentityScope { input, overwrite } => {
            execute_identity_scope(connection, state, input, overwrite).await?;
        }
        StorageImportOperation::UpsertGroup { input, overwrite } => {
            execute_group(connection, state, input, overwrite).await?;
        }
        StorageImportOperation::UpsertPrincipal { input, overwrite } => {
            execute_principal(connection, state, input, overwrite).await?;
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
            execute_event_sink(connection, state, input, overwrite).await?;
        }
        StorageImportOperation::UpsertEventSubscription { input, overwrite } => {
            upsert_event_subscription(connection, state, input, overwrite).await?;
        }
    }
    Ok(())
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
                .get_result::<CollectionRow>(connection)
                .await
                .optional(),
                async || {
                    crate::schema::collections::table
                        .filter(crate::schema::collections::id.eq(collection_id))
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
            .get_result::<CollectionRow>(connection)
            .await
            .optional(),
            async || {
                crate::schema::collections::table
                    .filter(crate::schema::collections::id.eq(collection_id))
                    .first::<CollectionRow>(connection)
                    .await
            },
        )
        .await?
    };
    updated.into_storage()
}

async fn create_class(
    connection: &mut PostgresConnection,
    input: StorageImportClass,
    collection_id: i32,
) -> Result<StorageClass, PostgresStorageError> {
    let parts = input.into_parts();
    assert_import_create_condition(parts.condition)?;
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
                .get_result::<ClassRow>(connection)
                .await?
        }
    };
    row.into_storage()
}

async fn update_class(
    connection: &mut PostgresConnection,
    class_id: i32,
    input: StorageImportClass,
) -> Result<StorageClass, PostgresStorageError> {
    let parts = input.into_parts();
    let (json_schema, validate_schema) = parts.schema_policy.into_parts();
    let current = crate::schema::hubuumclass::table
        .filter(crate::schema::hubuumclass::id.eq(class_id))
        .select(crate::schema::hubuumclass::revision)
        .for_update()
        .first::<PostgresRevision>(connection)
        .await
        .optional()?;
    assert_import_revision(parts.condition, require_existing(current, parts.condition)?)?;
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
                .get_result::<ClassRow>(connection)
                .await
                .optional(),
                async || {
                    crate::schema::hubuumclass::table
                        .filter(crate::schema::hubuumclass::id.eq(class_id))
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
            .get_result::<ClassRow>(connection)
            .await
            .optional(),
            async || {
                crate::schema::hubuumclass::table
                    .filter(crate::schema::hubuumclass::id.eq(class_id))
                    .first::<ClassRow>(connection)
                    .await
            },
        )
        .await?
    };
    row.into_storage()
}

async fn create_object(
    connection: &mut PostgresConnection,
    input: StorageImportObject,
    class: &StorageClass,
) -> Result<StorageObject, PostgresStorageError> {
    let parts = input.into_parts();
    assert_import_create_condition(parts.condition)?;
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
                .get_result::<ObjectRow>(connection)
                .await?
        }
    };
    materialize_object_on_connection(connection, row.id, row.hubuum_class_id, &row.data).await?;
    row.into_storage()
}

async fn update_object(
    connection: &mut PostgresConnection,
    object_id: i32,
    input: StorageImportObject,
) -> Result<StorageObject, PostgresStorageError> {
    let parts = input.into_parts();
    let current = crate::schema::hubuumobject::table
        .filter(crate::schema::hubuumobject::id.eq(object_id))
        .select(crate::schema::hubuumobject::revision)
        .for_update()
        .first::<PostgresRevision>(connection)
        .await
        .optional()?;
    assert_import_revision(parts.condition, require_existing(current, parts.condition)?)?;
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
                .get_result::<ObjectRow>(connection)
                .await
                .optional(),
                async || {
                    crate::schema::hubuumobject::table
                        .filter(crate::schema::hubuumobject::id.eq(object_id))
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
            .get_result::<ObjectRow>(connection)
            .await
            .optional(),
            async || {
                crate::schema::hubuumobject::table
                    .filter(crate::schema::hubuumobject::id.eq(object_id))
                    .first::<ObjectRow>(connection)
                    .await
            },
        )
        .await?
    };
    materialize_object_on_connection(connection, row.id, row.hubuum_class_id, &row.data).await?;
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

async fn execute_identity_scope(
    connection: &mut PostgresConnection,
    state: &mut ImportRuntime,
    input: StorageImportIdentityScope,
    overwrite: bool,
) -> Result<(), PostgresStorageError> {
    let parts = input.into_parts();
    let reference = parts.reference.clone();
    let existing = crate::schema::identity_scopes::table
        .filter(crate::schema::identity_scopes::name.eq(&parts.name))
        .select((
            crate::schema::identity_scopes::id,
            crate::schema::identity_scopes::created_at,
            crate::schema::identity_scopes::updated_at,
            crate::schema::identity_scopes::revision,
        ))
        .for_update()
        .first::<(i32, NaiveDateTime, NaiveDateTime, PostgresRevision)>(connection)
        .await
        .optional()?;
    let identity_scope_id = match existing {
        Some((_, _, _, revision)) if !overwrite => {
            assert_import_revision(parts.condition, revision)?;
            return Err(PostgresStorageError::conflict(format!(
                "Identity scope '{}' already exists",
                parts.name
            )));
        }
        Some((id, existing_created_at, existing_updated_at, revision)) => {
            assert_import_revision(parts.condition, revision)?;
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((existing_created_at, existing_updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                updated_or_current(
                    diesel::update(
                        crate::schema::identity_scopes::table
                            .filter(crate::schema::identity_scopes::id.eq(id)),
                    )
                    .set((
                        crate::schema::identity_scopes::provider_kind.eq(parts.provider_kind),
                        crate::schema::identity_scopes::created_at.eq(created_at),
                        crate::schema::identity_scopes::updated_at.eq(updated_at),
                    ))
                    .returning(crate::schema::identity_scopes::id)
                    .get_result::<i32>(connection)
                    .await
                    .optional(),
                    async || {
                        crate::schema::identity_scopes::table
                            .filter(crate::schema::identity_scopes::id.eq(id))
                            .select(crate::schema::identity_scopes::id)
                            .first::<i32>(connection)
                            .await
                    },
                )
                .await
                .map_err(PostgresStorageError::from)
            })
            .await?
        }
        None => {
            assert_import_create_condition(parts.condition)?;
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::identity_scopes::table)
                .values((
                    crate::schema::identity_scopes::name.eq(parts.name),
                    crate::schema::identity_scopes::provider_kind.eq(parts.provider_kind),
                    crate::schema::identity_scopes::created_at.eq(created_at),
                    crate::schema::identity_scopes::updated_at.eq(updated_at),
                ))
                .returning(crate::schema::identity_scopes::id)
                .get_result::<i32>(connection)
                .await?
        }
    };
    if let Some(reference) = reference {
        state
            .identity_scopes_by_ref
            .insert(reference, identity_scope_id);
    }
    Ok(())
}

async fn execute_group(
    connection: &mut PostgresConnection,
    state: &mut ImportRuntime,
    input: StorageImportGroup,
    overwrite: bool,
) -> Result<(), PostgresStorageError> {
    let parts = input.into_parts();
    let identity_scope_id = resolve_identity_scope(
        connection,
        state,
        parts.identity_scope_ref.as_deref(),
        parts.identity_scope_key.as_ref(),
    )
    .await?;
    let existing = crate::schema::groups::table
        .filter(crate::schema::groups::identity_scope_id.eq(identity_scope_id))
        .filter(crate::schema::groups::groupname.eq(&parts.name))
        .for_update()
        .first::<GroupRow>(connection)
        .await
        .optional()?;
    let group_id = match existing {
        Some(existing) if !overwrite => {
            assert_import_revision(parts.condition, existing.revision)?;
            return Err(PostgresStorageError::conflict(format!(
                "Group '{}' already exists in its identity scope",
                parts.name
            )));
        }
        Some(existing) => {
            assert_import_revision(parts.condition, existing.revision)?;
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((existing.created_at, existing.updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                updated_or_current(
                    diesel::update(
                        crate::schema::groups::table
                            .filter(crate::schema::groups::id.eq(existing.id)),
                    )
                    .set((
                        crate::schema::groups::description.eq(parts.description),
                        crate::schema::groups::managed_by.eq(parts.managed_by),
                        crate::schema::groups::external_key.eq(parts.external_key),
                        crate::schema::groups::last_sync_attempted_at.eq(parts
                            .last_sync_attempted_at
                            .map(|timestamp| timestamp.naive_utc())),
                        crate::schema::groups::last_sync_success_at.eq(parts
                            .last_sync_success_at
                            .map(|timestamp| timestamp.naive_utc())),
                        crate::schema::groups::created_at.eq(created_at),
                        crate::schema::groups::updated_at.eq(updated_at),
                    ))
                    .returning(crate::schema::groups::id)
                    .get_result::<i32>(connection)
                    .await
                    .optional(),
                    async || {
                        crate::schema::groups::table
                            .filter(crate::schema::groups::id.eq(existing.id))
                            .select(crate::schema::groups::id)
                            .first::<i32>(connection)
                            .await
                    },
                )
                .await
                .map_err(PostgresStorageError::from)
            })
            .await?
        }
        None => {
            assert_import_create_condition(parts.condition)?;
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::groups::table)
                .values((
                    crate::schema::groups::groupname.eq(parts.name),
                    crate::schema::groups::description.eq(parts.description),
                    crate::schema::groups::identity_scope_id.eq(identity_scope_id),
                    crate::schema::groups::managed_by.eq(parts.managed_by),
                    crate::schema::groups::external_key.eq(parts.external_key),
                    crate::schema::groups::last_sync_attempted_at.eq(parts
                        .last_sync_attempted_at
                        .map(|timestamp| timestamp.naive_utc())),
                    crate::schema::groups::last_sync_success_at.eq(parts
                        .last_sync_success_at
                        .map(|timestamp| timestamp.naive_utc())),
                    crate::schema::groups::created_at.eq(created_at),
                    crate::schema::groups::updated_at.eq(updated_at),
                ))
                .returning(crate::schema::groups::id)
                .get_result::<i32>(connection)
                .await?
        }
    };
    if let Some(reference) = parts.reference {
        state.groups_by_ref.insert(reference, group_id);
    }
    Ok(())
}

async fn execute_principal(
    connection: &mut PostgresConnection,
    state: &mut ImportRuntime,
    input: StorageImportPrincipal,
    overwrite: bool,
) -> Result<(), PostgresStorageError> {
    let parts = input.into_parts();
    let identity_scope_id = resolve_identity_scope(
        connection,
        state,
        parts.identity_scope_ref.as_deref(),
        parts.identity_scope_key.as_ref(),
    )
    .await?;
    let (owner_group_id, created_by) = match &parts.subtype {
        StorageImportPrincipalSubtype::Human { .. } => (None, None),
        StorageImportPrincipalSubtype::ServiceAccount {
            owner_group_ref,
            owner_group_key,
            created_by_ref,
            created_by_key,
            ..
        } => (
            Some(
                resolve_group(
                    connection,
                    state,
                    owner_group_ref.as_deref(),
                    owner_group_key.as_ref(),
                )
                .await?,
            ),
            if created_by_ref.is_some() || created_by_key.is_some() {
                Some(
                    resolve_principal(
                        connection,
                        state,
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
    let principal_id = upsert_principal(
        connection,
        &parts,
        identity_scope_id,
        owner_group_id,
        created_by,
        overwrite,
    )
    .await?;
    if let Some(reference) = parts.reference {
        state.principals_by_ref.insert(reference, principal_id);
    }
    Ok(())
}

async fn execute_group_membership(
    connection: &mut PostgresConnection,
    state: &mut ImportRuntime,
    input: StorageImportGroupMembership,
    overwrite: bool,
) -> Result<(), PostgresStorageError> {
    let parts = input.into_parts();
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
    let mut sources = Vec::with_capacity(parts.sources.len());
    for source in parts.sources {
        let source = source.into_parts();
        let scope_id = resolve_identity_scope(
            connection,
            state,
            source.source_scope_ref.as_deref(),
            source.source_scope_key.as_ref(),
        )
        .await?;
        sources.push((source, scope_id));
    }
    upsert_group_membership(
        connection,
        principal_id,
        group_id,
        &sources,
        parts.condition,
        parts.timestamps,
        overwrite,
    )
    .await
}

async fn resolve_identity_scope(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    reference: Option<&str>,
    key: Option<&StorageImportIdentityScopeKey>,
) -> Result<i32, PostgresStorageError> {
    if let Some(reference) = reference
        && let Some(id) = state.identity_scopes_by_ref.get(reference)
    {
        return Ok(*id);
    }
    let name = key
        .cloned()
        .map(StorageImportIdentityScopeKey::into_parts)
        .map(|parts| parts.name)
        .ok_or_else(|| {
            PostgresStorageError::invalid_input(
                "Identity-scope reference was not resolved and no identity_scope_key was supplied",
            )
        })?;
    crate::schema::identity_scopes::table
        .filter(crate::schema::identity_scopes::name.eq(name))
        .select(crate::schema::identity_scopes::id)
        .first::<i32>(connection)
        .await
        .optional()?
        .ok_or_else(|| PostgresStorageError::not_found("Identity scope was not found"))
}

async fn resolve_group(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    reference: Option<&str>,
    key: Option<&StorageImportGroupKey>,
) -> Result<i32, PostgresStorageError> {
    if let Some(reference) = reference
        && let Some(id) = state.groups_by_ref.get(reference)
    {
        return Ok(*id);
    }
    let parts = key
        .cloned()
        .map(StorageImportGroupKey::into_parts)
        .ok_or_else(|| {
            PostgresStorageError::invalid_input(
                "Group reference was not resolved and no group_key was supplied",
            )
        })?;
    let scope_name = parts.identity_scope;
    crate::schema::groups::table
        .inner_join(crate::schema::identity_scopes::table)
        .filter(crate::schema::identity_scopes::name.eq(scope_name))
        .filter(crate::schema::groups::groupname.eq(parts.name))
        .select(crate::schema::groups::id)
        .first::<i32>(connection)
        .await
        .optional()?
        .ok_or_else(|| PostgresStorageError::not_found("Group was not found"))
}

async fn resolve_principal(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    reference: Option<&str>,
    key: Option<&StorageImportPrincipalKey>,
) -> Result<i32, PostgresStorageError> {
    if let Some(reference) = reference
        && let Some(id) = state.principals_by_ref.get(reference)
    {
        return Ok(*id);
    }
    let parts = key
        .cloned()
        .map(StorageImportPrincipalKey::into_parts)
        .ok_or_else(|| {
            PostgresStorageError::invalid_input(
                "Principal reference was not resolved and no principal_key was supplied",
            )
        })?;
    let scope_name = parts.identity_scope;
    crate::schema::principals::table
        .inner_join(crate::schema::identity_scopes::table)
        .filter(crate::schema::identity_scopes::name.eq(scope_name))
        .filter(crate::schema::principals::name.eq(parts.name))
        .select(crate::schema::principals::id)
        .first::<i32>(connection)
        .await
        .optional()?
        .ok_or_else(|| PostgresStorageError::not_found("Principal was not found"))
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

async fn upsert_principal(
    connection: &mut PostgresConnection,
    parts: &StorageImportPrincipalParts,
    identity_scope_id: i32,
    owner_group_id: Option<i32>,
    created_by: Option<i32>,
    overwrite: bool,
) -> Result<i32, PostgresStorageError> {
    validate_principal_credentials(&parts.subtype)?;
    let supplied_password = match &parts.subtype {
        StorageImportPrincipalSubtype::Human {
            password: Some(password),
            password_hash: None,
            ..
        } => Some(hash_password_async(password.clone()).await?),
        StorageImportPrincipalSubtype::Human {
            password: None,
            password_hash,
            ..
        } => password_hash.clone(),
        StorageImportPrincipalSubtype::Human {
            password: Some(_),
            password_hash: Some(_),
            ..
        } => unreachable!("credentials were validated above"),
        StorageImportPrincipalSubtype::ServiceAccount { .. } => None,
    };
    let expected_kind = match &parts.subtype {
        StorageImportPrincipalSubtype::Human { .. } => "human",
        StorageImportPrincipalSubtype::ServiceAccount { .. } => "service_account",
    };
    let existing = crate::schema::principals::table
        .filter(crate::schema::principals::identity_scope_id.eq(identity_scope_id))
        .filter(crate::schema::principals::name.eq(&parts.name))
        .for_update()
        .first::<PrincipalRow>(connection)
        .await
        .optional()?;
    if let Some(existing) = &existing {
        assert_import_revision(parts.condition, existing.revision)?;
        if !overwrite {
            return Err(PostgresStorageError::conflict(format!(
                "Principal '{}' already exists in its identity scope",
                parts.name
            )));
        }
        if existing.kind != expected_kind {
            return Err(PostgresStorageError::conflict(format!(
                "Principal '{}' exists with kind '{}' instead of '{expected_kind}'",
                parts.name, existing.kind
            )));
        }
    }
    let principal_id = match existing {
        Some(existing) => {
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((existing.created_at, existing.updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                updated_or_current(
                    diesel::update(
                        crate::schema::principals::table
                            .filter(crate::schema::principals::id.eq(existing.id)),
                    )
                    .set((
                        crate::schema::principals::provider_managed.eq(parts.provider_managed),
                        crate::schema::principals::settings.eq(&parts.settings),
                        crate::schema::principals::external_subject.eq(&parts.external_subject),
                        crate::schema::principals::last_sync_attempted_at.eq(parts
                            .last_sync_attempted_at
                            .map(|timestamp| timestamp.naive_utc())),
                        crate::schema::principals::last_sync_success_at.eq(parts
                            .last_sync_success_at
                            .map(|timestamp| timestamp.naive_utc())),
                        crate::schema::principals::created_at.eq(created_at),
                        crate::schema::principals::updated_at.eq(updated_at),
                    ))
                    .returning(crate::schema::principals::id)
                    .get_result::<i32>(connection)
                    .await
                    .optional(),
                    async || {
                        crate::schema::principals::table
                            .filter(crate::schema::principals::id.eq(existing.id))
                            .select(crate::schema::principals::id)
                            .first::<i32>(connection)
                            .await
                    },
                )
                .await
                .map_err(PostgresStorageError::from)
            })
            .await?
        }
        None => {
            assert_import_create_condition(parts.condition)?;
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::principals::table)
                .values((
                    crate::schema::principals::kind.eq(expected_kind),
                    crate::schema::principals::name.eq(&parts.name),
                    crate::schema::principals::identity_scope_id.eq(identity_scope_id),
                    crate::schema::principals::provider_managed.eq(parts.provider_managed),
                    crate::schema::principals::settings.eq(&parts.settings),
                    crate::schema::principals::external_subject.eq(&parts.external_subject),
                    crate::schema::principals::last_sync_attempted_at.eq(parts
                        .last_sync_attempted_at
                        .map(|timestamp| timestamp.naive_utc())),
                    crate::schema::principals::last_sync_success_at.eq(parts
                        .last_sync_success_at
                        .map(|timestamp| timestamp.naive_utc())),
                    crate::schema::principals::created_at.eq(created_at),
                    crate::schema::principals::updated_at.eq(updated_at),
                ))
                .returning(crate::schema::principals::id)
                .get_result::<i32>(connection)
                .await?
        }
    };

    match &parts.subtype {
        StorageImportPrincipalSubtype::Human {
            proper_name,
            email,
            anonymized_at,
            ..
        } => {
            let existing = crate::schema::users::table
                .filter(crate::schema::users::id.eq(principal_id))
                .first::<UserRow>(connection)
                .await
                .optional()?;
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .or_else(|| {
                    existing
                        .as_ref()
                        .map(|row| (row.created_at, row.updated_at))
                })
                .unwrap_or_else(|| imported_timestamps(None));
            if let Some(existing) = existing {
                with_imported_timestamp_override(connection, async |connection| {
                    diesel::update(
                        crate::schema::users::table
                            .filter(crate::schema::users::id.eq(principal_id)),
                    )
                    .set((
                        crate::schema::users::password.eq(supplied_password.or(existing.password)),
                        crate::schema::users::proper_name.eq(proper_name),
                        crate::schema::users::email.eq(email),
                        crate::schema::users::anonymized_at
                            .eq(anonymized_at.map(|timestamp| timestamp.naive_utc())),
                        crate::schema::users::created_at.eq(created_at),
                        crate::schema::users::updated_at.eq(updated_at),
                    ))
                    .execute(connection)
                    .await?;
                    Ok(())
                })
                .await?;
            } else {
                diesel::insert_into(crate::schema::users::table)
                    .values((
                        crate::schema::users::id.eq(principal_id),
                        crate::schema::users::kind.eq("human"),
                        crate::schema::users::password.eq(supplied_password),
                        crate::schema::users::proper_name.eq(proper_name),
                        crate::schema::users::email.eq(email),
                        crate::schema::users::anonymized_at
                            .eq(anonymized_at.map(|timestamp| timestamp.naive_utc())),
                        crate::schema::users::created_at.eq(created_at),
                        crate::schema::users::updated_at.eq(updated_at),
                    ))
                    .execute(connection)
                    .await?;
            }
        }
        StorageImportPrincipalSubtype::ServiceAccount {
            description,
            disabled_at,
            ..
        } => {
            let owner_group_id = owner_group_id.ok_or_else(|| {
                PostgresStorageError::invalid_input(
                    "Service-account import requires an owner group",
                )
            })?;
            let existing = crate::schema::service_accounts::table
                .filter(crate::schema::service_accounts::id.eq(principal_id))
                .first::<ServiceAccountRow>(connection)
                .await
                .optional()?;
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .or_else(|| {
                    existing
                        .as_ref()
                        .map(|row| (row.created_at, row.updated_at))
                })
                .unwrap_or_else(|| imported_timestamps(None));
            if existing.is_some() {
                with_imported_timestamp_override(connection, async |connection| {
                    diesel::update(
                        crate::schema::service_accounts::table
                            .filter(crate::schema::service_accounts::id.eq(principal_id)),
                    )
                    .set((
                        crate::schema::service_accounts::description.eq(description),
                        crate::schema::service_accounts::owner_group_id.eq(owner_group_id),
                        crate::schema::service_accounts::created_by.eq(created_by),
                        crate::schema::service_accounts::disabled_at
                            .eq(disabled_at.map(|timestamp| timestamp.naive_utc())),
                        crate::schema::service_accounts::created_at.eq(created_at),
                        crate::schema::service_accounts::updated_at.eq(updated_at),
                    ))
                    .execute(connection)
                    .await?;
                    Ok(())
                })
                .await?;
            } else {
                diesel::insert_into(crate::schema::service_accounts::table)
                    .values((
                        crate::schema::service_accounts::id.eq(principal_id),
                        crate::schema::service_accounts::kind.eq("service_account"),
                        crate::schema::service_accounts::description.eq(description),
                        crate::schema::service_accounts::owner_group_id.eq(owner_group_id),
                        crate::schema::service_accounts::created_by.eq(created_by),
                        crate::schema::service_accounts::disabled_at
                            .eq(disabled_at.map(|timestamp| timestamp.naive_utc())),
                        crate::schema::service_accounts::created_at.eq(created_at),
                        crate::schema::service_accounts::updated_at.eq(updated_at),
                    ))
                    .execute(connection)
                    .await?;
            }
        }
    }
    Ok(principal_id)
}

async fn upsert_group_membership(
    connection: &mut PostgresConnection,
    principal_id: i32,
    group_id: i32,
    sources: &[(StorageImportMembershipSourceParts, i32)],
    condition: Option<StorageImportWriteCondition>,
    timestamps: Option<StorageImportTimestamps>,
    overwrite: bool,
) -> Result<(), PostgresStorageError> {
    let existing = crate::schema::group_memberships::table
        .filter(crate::schema::group_memberships::principal_id.eq(principal_id))
        .filter(crate::schema::group_memberships::group_id.eq(group_id))
        .select((
            crate::schema::group_memberships::created_at,
            crate::schema::group_memberships::updated_at,
            crate::schema::group_memberships::revision,
        ))
        .for_update()
        .first::<(NaiveDateTime, NaiveDateTime, PostgresRevision)>(connection)
        .await
        .optional()?;
    if let Some((_, _, revision)) = existing {
        assert_import_revision(condition, revision)?;
        if !overwrite {
            return Err(PostgresStorageError::conflict(format!(
                "Principal {principal_id} is already a member of group {group_id}"
            )));
        }
    } else {
        assert_import_create_condition(condition)?;
    }
    with_imported_timestamp_override(connection, async |connection| {
        let membership_timestamps = timestamps
            .map(import_timestamp_pair)
            .or(existing.map(|(created, updated, _)| (created, updated)))
            .unwrap_or_else(|| imported_timestamps(None));
        if existing.is_some() {
            diesel::update(
                crate::schema::group_memberships::table
                    .filter(crate::schema::group_memberships::principal_id.eq(principal_id))
                    .filter(crate::schema::group_memberships::group_id.eq(group_id)),
            )
            .set((
                crate::schema::group_memberships::created_at.eq(membership_timestamps.0),
                crate::schema::group_memberships::updated_at.eq(membership_timestamps.1),
            ))
            .execute(connection)
            .await?;
        } else {
            diesel::insert_into(crate::schema::group_memberships::table)
                .values((
                    crate::schema::group_memberships::principal_id.eq(principal_id),
                    crate::schema::group_memberships::group_id.eq(group_id),
                    crate::schema::group_memberships::created_at.eq(membership_timestamps.0),
                    crate::schema::group_memberships::updated_at.eq(membership_timestamps.1),
                ))
                .execute(connection)
                .await?;
        }
        for (source, source_scope_id) in sources {
            let existing_source = crate::schema::group_membership_sources::table
                .filter(crate::schema::group_membership_sources::principal_id.eq(principal_id))
                .filter(crate::schema::group_membership_sources::group_id.eq(group_id))
                .filter(crate::schema::group_membership_sources::source.eq(&source.source))
                .filter(
                    crate::schema::group_membership_sources::source_scope_id.eq(*source_scope_id),
                )
                .filter(crate::schema::group_membership_sources::source_key.eq(&source.source_key))
                .select((
                    crate::schema::group_membership_sources::created_at,
                    crate::schema::group_membership_sources::updated_at,
                ))
                .first::<(NaiveDateTime, NaiveDateTime)>(connection)
                .await
                .optional()?;
            let source_timestamps = source
                .timestamps
                .map(import_timestamp_pair)
                .or(existing_source)
                .unwrap_or_else(|| imported_timestamps(None));
            if existing_source.is_some() {
                diesel::update(
                    crate::schema::group_membership_sources::table
                        .filter(
                            crate::schema::group_membership_sources::principal_id.eq(principal_id),
                        )
                        .filter(crate::schema::group_membership_sources::group_id.eq(group_id))
                        .filter(crate::schema::group_membership_sources::source.eq(&source.source))
                        .filter(
                            crate::schema::group_membership_sources::source_scope_id
                                .eq(*source_scope_id),
                        )
                        .filter(
                            crate::schema::group_membership_sources::source_key
                                .eq(&source.source_key),
                        ),
                )
                .set((
                    crate::schema::group_membership_sources::created_at.eq(source_timestamps.0),
                    crate::schema::group_membership_sources::updated_at.eq(source_timestamps.1),
                ))
                .execute(connection)
                .await?;
            } else {
                diesel::insert_into(crate::schema::group_membership_sources::table)
                    .values((
                        crate::schema::group_membership_sources::principal_id.eq(principal_id),
                        crate::schema::group_membership_sources::group_id.eq(group_id),
                        crate::schema::group_membership_sources::source.eq(&source.source),
                        crate::schema::group_membership_sources::source_scope_id
                            .eq(*source_scope_id),
                        crate::schema::group_membership_sources::source_key.eq(&source.source_key),
                        crate::schema::group_membership_sources::created_at.eq(source_timestamps.0),
                        crate::schema::group_membership_sources::updated_at.eq(source_timestamps.1),
                    ))
                    .execute(connection)
                    .await?;
            }
        }
        Ok(())
    })
    .await
}

fn validate_principal_credentials(
    subtype: &StorageImportPrincipalSubtype,
) -> Result<(), PostgresStorageError> {
    let StorageImportPrincipalSubtype::Human {
        password,
        password_hash,
        ..
    } = subtype
    else {
        return Ok(());
    };
    if password.is_some() && password_hash.is_some() {
        return Err(PostgresStorageError::invalid_input(
            "A human principal import accepts password or password_hash, not both",
        ));
    }
    if let Some(hash) = password_hash {
        let parsed = PasswordHash::new(hash).map_err(|_| {
            PostgresStorageError::invalid_input(
                "Imported password_hash must be a valid Argon2 password hash",
            )
        })?;
        if !matches!(
            parsed.algorithm.as_str(),
            "argon2d" | "argon2i" | "argon2id"
        ) {
            return Err(PostgresStorageError::invalid_input(
                "Imported password_hash must use an Argon2 algorithm",
            ));
        }
    }
    Ok(())
}

async fn hash_password_async(password: String) -> Result<String, PostgresStorageError> {
    let permit = PASSWORD_WORK_SEMAPHORE
        .clone()
        .acquire_owned()
        .await
        .map_err(|error| {
            PostgresStorageError::internal(format!("Password worker is unavailable: {error}"))
        })?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        Argon2::default()
            .hash_password(password.as_bytes())
            .map(|hash| hash.to_string())
            .map_err(|error| PostgresStorageError::internal(error.to_string()))
    })
    .await
    .map_err(|error| PostgresStorageError::internal(format!("Password worker failed: {error}")))?
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

async fn resolve_class_relation_endpoints(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportClassRelation,
) -> Result<(StorageClass, StorageClass), PostgresStorageError> {
    let parts = input.clone().into_parts();
    let from = resolve_class(
        connection,
        state,
        parts.from_class_ref.as_deref(),
        parts.from_class_key.as_ref(),
    )
    .await?;
    let to = resolve_class(
        connection,
        state,
        parts.to_class_ref.as_deref(),
        parts.to_class_key.as_ref(),
    )
    .await?;
    Ok((from, to))
}

async fn create_class_relation(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportClassRelation,
) -> Result<(), PostgresStorageError> {
    let (from, to) = resolve_class_relation_endpoints(connection, state, &input).await?;
    let parts = input.into_parts();
    assert_import_create_condition(parts.condition)?;
    let command = normalize_class_relation_create(
        StorageClassRelationCreate::builder(from.id(), to.id())
            .template_aliases(parts.forward_template_alias, parts.reverse_template_alias)
            .relation_limits(parts.from_max_relations, parts.to_max_relations)
            .build(),
    )?;
    match parts.timestamps {
        Some(timestamps) => {
            let (created_at, updated_at) = import_timestamp_pair(timestamps);
            diesel::insert_into(crate::schema::hubuumclass_relation::table)
                .values((
                    crate::schema::hubuumclass_relation::from_hubuum_class_id
                        .eq(command.from_class_id().id()),
                    crate::schema::hubuumclass_relation::to_hubuum_class_id
                        .eq(command.to_class_id().id()),
                    crate::schema::hubuumclass_relation::forward_template_alias
                        .eq(command.forward_template_alias()),
                    crate::schema::hubuumclass_relation::reverse_template_alias
                        .eq(command.reverse_template_alias()),
                    crate::schema::hubuumclass_relation::from_max_relations
                        .eq(command.from_max_relations()),
                    crate::schema::hubuumclass_relation::to_max_relations
                        .eq(command.to_max_relations()),
                    crate::schema::hubuumclass_relation::created_at.eq(created_at),
                    crate::schema::hubuumclass_relation::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
        }
        None => {
            diesel::insert_into(crate::schema::hubuumclass_relation::table)
                .values((
                    crate::schema::hubuumclass_relation::from_hubuum_class_id
                        .eq(command.from_class_id().id()),
                    crate::schema::hubuumclass_relation::to_hubuum_class_id
                        .eq(command.to_class_id().id()),
                    crate::schema::hubuumclass_relation::forward_template_alias
                        .eq(command.forward_template_alias()),
                    crate::schema::hubuumclass_relation::reverse_template_alias
                        .eq(command.reverse_template_alias()),
                    crate::schema::hubuumclass_relation::from_max_relations
                        .eq(command.from_max_relations()),
                    crate::schema::hubuumclass_relation::to_max_relations
                        .eq(command.to_max_relations()),
                ))
                .execute(connection)
                .await?;
        }
    }
    Ok(())
}

async fn update_class_relation_timestamps(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportClassRelation,
    timestamps: StorageImportTimestamps,
) -> Result<(), PostgresStorageError> {
    let (from, to) = resolve_class_relation_endpoints(connection, state, input).await?;
    let parts = input.clone().into_parts();
    let pair = normalize_pair(from.id().id(), to.id().id());
    assert_relation_condition(
        class_relation_revision(connection, pair).await?,
        parts.condition,
    )?;
    let (created_at, updated_at) = import_timestamp_pair(timestamps);
    with_imported_timestamp_override(connection, async |connection| {
        diesel::update(
            crate::schema::hubuumclass_relation::table
                .filter(crate::schema::hubuumclass_relation::from_hubuum_class_id.eq(pair.0))
                .filter(crate::schema::hubuumclass_relation::to_hubuum_class_id.eq(pair.1)),
        )
        .set((
            crate::schema::hubuumclass_relation::created_at.eq(created_at),
            crate::schema::hubuumclass_relation::updated_at.eq(updated_at),
        ))
        .execute(connection)
        .await?;
        Ok(())
    })
    .await
}

async fn check_class_relation_condition(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportClassRelation,
) -> Result<(), PostgresStorageError> {
    let (from, to) = resolve_class_relation_endpoints(connection, state, input).await?;
    assert_relation_condition(
        class_relation_revision(connection, normalize_pair(from.id().id(), to.id().id())).await?,
        input.clone().into_parts().condition,
    )
}

async fn class_relation_revision(
    connection: &mut PostgresConnection,
    pair: (i32, i32),
) -> Result<Option<PostgresRevision>, PostgresStorageError> {
    Ok(crate::schema::hubuumclass_relation::table
        .filter(crate::schema::hubuumclass_relation::from_hubuum_class_id.eq(pair.0))
        .filter(crate::schema::hubuumclass_relation::to_hubuum_class_id.eq(pair.1))
        .select(crate::schema::hubuumclass_relation::revision)
        .for_update()
        .first::<PostgresRevision>(connection)
        .await
        .optional()?)
}

async fn resolve_object_relation_endpoints(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportObjectRelation,
) -> Result<(StorageObject, StorageObject), PostgresStorageError> {
    let parts = input.clone().into_parts();
    let from = resolve_object(
        connection,
        state,
        parts.from_object_ref.as_deref(),
        parts.from_object_key.as_ref(),
    )
    .await?;
    let to = resolve_object(
        connection,
        state,
        parts.to_object_ref.as_deref(),
        parts.to_object_key.as_ref(),
    )
    .await?;
    Ok((from, to))
}

async fn create_object_relation(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportObjectRelation,
) -> Result<(), PostgresStorageError> {
    let (from, to) = resolve_object_relation_endpoints(connection, state, &input).await?;
    let parts = input.into_parts();
    assert_import_create_condition(parts.condition)?;
    if from.id().id() == to.id().id() {
        return Err(PostgresStorageError::invalid_input(
            "from and to object ids cannot be the same",
        ));
    }
    let class_pair = normalize_pair(from.class_id().id(), to.class_id().id());
    let class_relation_id = crate::schema::hubuumclass_relation::table
        .filter(crate::schema::hubuumclass_relation::from_hubuum_class_id.eq(class_pair.0))
        .filter(crate::schema::hubuumclass_relation::to_hubuum_class_id.eq(class_pair.1))
        .select(crate::schema::hubuumclass_relation::id)
        .first::<i32>(connection)
        .await?;
    let object_pair = normalize_pair(from.id().id(), to.id().id());
    match parts.timestamps {
        Some(timestamps) => {
            let (created_at, updated_at) = import_timestamp_pair(timestamps);
            diesel::insert_into(crate::schema::hubuumobject_relation::table)
                .values((
                    crate::schema::hubuumobject_relation::from_hubuum_object_id.eq(object_pair.0),
                    crate::schema::hubuumobject_relation::to_hubuum_object_id.eq(object_pair.1),
                    crate::schema::hubuumobject_relation::class_relation_id.eq(class_relation_id),
                    crate::schema::hubuumobject_relation::created_at.eq(created_at),
                    crate::schema::hubuumobject_relation::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
        }
        None => {
            diesel::insert_into(crate::schema::hubuumobject_relation::table)
                .values((
                    crate::schema::hubuumobject_relation::from_hubuum_object_id.eq(object_pair.0),
                    crate::schema::hubuumobject_relation::to_hubuum_object_id.eq(object_pair.1),
                    crate::schema::hubuumobject_relation::class_relation_id.eq(class_relation_id),
                ))
                .execute(connection)
                .await?;
        }
    }
    Ok(())
}

async fn update_object_relation_timestamps(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportObjectRelation,
    timestamps: StorageImportTimestamps,
) -> Result<(), PostgresStorageError> {
    let (from, to) = resolve_object_relation_endpoints(connection, state, input).await?;
    let parts = input.clone().into_parts();
    let pair = normalize_pair(from.id().id(), to.id().id());
    assert_relation_condition(
        object_relation_revision(connection, pair).await?,
        parts.condition,
    )?;
    let (created_at, updated_at) = import_timestamp_pair(timestamps);
    with_imported_timestamp_override(connection, async |connection| {
        diesel::update(
            crate::schema::hubuumobject_relation::table
                .filter(crate::schema::hubuumobject_relation::from_hubuum_object_id.eq(pair.0))
                .filter(crate::schema::hubuumobject_relation::to_hubuum_object_id.eq(pair.1)),
        )
        .set((
            crate::schema::hubuumobject_relation::created_at.eq(created_at),
            crate::schema::hubuumobject_relation::updated_at.eq(updated_at),
        ))
        .execute(connection)
        .await?;
        Ok(())
    })
    .await
}

async fn check_object_relation_condition(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportObjectRelation,
) -> Result<(), PostgresStorageError> {
    let (from, to) = resolve_object_relation_endpoints(connection, state, input).await?;
    assert_relation_condition(
        object_relation_revision(connection, normalize_pair(from.id().id(), to.id().id())).await?,
        input.clone().into_parts().condition,
    )
}

async fn object_relation_revision(
    connection: &mut PostgresConnection,
    pair: (i32, i32),
) -> Result<Option<PostgresRevision>, PostgresStorageError> {
    Ok(crate::schema::hubuumobject_relation::table
        .filter(crate::schema::hubuumobject_relation::from_hubuum_object_id.eq(pair.0))
        .filter(crate::schema::hubuumobject_relation::to_hubuum_object_id.eq(pair.1))
        .select(crate::schema::hubuumobject_relation::revision)
        .for_update()
        .first::<PostgresRevision>(connection)
        .await
        .optional()?)
}

fn assert_relation_condition(
    revision: Option<PostgresRevision>,
    condition: Option<StorageImportWriteCondition>,
) -> Result<(), PostgresStorageError> {
    assert_import_revision(condition, require_existing(revision, condition)?)
}

const fn normalize_pair(left: i32, right: i32) -> (i32, i32) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
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

async fn upsert_export_template(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportExportTemplate,
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
    let class_id = if parts.class_ref.is_some() || parts.class_key.is_some() {
        let class = resolve_class(
            connection,
            state,
            parts.class_ref.as_deref(),
            parts.class_key.as_ref(),
        )
        .await?;
        ensure_class_collection(&class, &collection, "Export template")?;
        Some(class.id().id())
    } else {
        None
    };
    validate_import_template_composition(
        connection,
        state,
        collection.id().id(),
        &parts.name,
        &parts.template,
        &parts.content_type,
    )
    .await?;
    let existing = crate::schema::export_templates::table
        .filter(crate::schema::export_templates::collection_id.eq(collection.id().id()))
        .filter(crate::schema::export_templates::name.eq(&parts.name))
        .select((
            crate::schema::export_templates::id,
            crate::schema::export_templates::created_at,
            crate::schema::export_templates::updated_at,
            crate::schema::export_templates::revision,
        ))
        .for_update()
        .first::<(i32, NaiveDateTime, NaiveDateTime, PostgresRevision)>(connection)
        .await
        .optional()?;
    assert_upsert_condition(existing.map(|row| row.3), parts.condition)?;
    if existing.is_some() && !overwrite {
        return Err(PostgresStorageError::conflict(format!(
            "Export template '{}' already exists in the collection",
            parts.name
        )));
    }
    match existing {
        Some((id, old_created_at, old_updated_at, _)) => {
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((old_created_at, old_updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                diesel::update(
                    crate::schema::export_templates::table
                        .filter(crate::schema::export_templates::id.eq(id)),
                )
                .set((
                    crate::schema::export_templates::description.eq(parts.description),
                    crate::schema::export_templates::content_type.eq(parts.content_type),
                    crate::schema::export_templates::template.eq(parts.template),
                    crate::schema::export_templates::kind.eq(parts.kind),
                    crate::schema::export_templates::scope_kind.eq(parts.scope_kind),
                    crate::schema::export_templates::class_id.eq(class_id),
                    crate::schema::export_templates::default_query.eq(parts.default_query),
                    crate::schema::export_templates::include.eq(parts.include),
                    crate::schema::export_templates::relation_context.eq(parts.relation_context),
                    crate::schema::export_templates::default_missing_data_policy
                        .eq(parts.default_missing_data_policy),
                    crate::schema::export_templates::default_limits.eq(parts.default_limits),
                    crate::schema::export_templates::created_at.eq(created_at),
                    crate::schema::export_templates::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
                Ok(())
            })
            .await?;
        }
        None => {
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::export_templates::table)
                .values((
                    crate::schema::export_templates::collection_id.eq(collection.id().id()),
                    crate::schema::export_templates::name.eq(parts.name),
                    crate::schema::export_templates::description.eq(parts.description),
                    crate::schema::export_templates::content_type.eq(parts.content_type),
                    crate::schema::export_templates::template.eq(parts.template),
                    crate::schema::export_templates::kind.eq(parts.kind),
                    crate::schema::export_templates::scope_kind.eq(parts.scope_kind),
                    crate::schema::export_templates::class_id.eq(class_id),
                    crate::schema::export_templates::default_query.eq(parts.default_query),
                    crate::schema::export_templates::include.eq(parts.include),
                    crate::schema::export_templates::relation_context.eq(parts.relation_context),
                    crate::schema::export_templates::default_missing_data_policy
                        .eq(parts.default_missing_data_policy),
                    crate::schema::export_templates::default_limits.eq(parts.default_limits),
                    crate::schema::export_templates::created_at.eq(created_at),
                    crate::schema::export_templates::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
        }
    }
    Ok(())
}

async fn validate_import_template_composition(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    collection_id: i32,
    name: &str,
    template: &str,
    content_type: &str,
) -> Result<(), PostgresStorageError> {
    let mut sources = crate::schema::export_templates::table
        .filter(crate::schema::export_templates::collection_id.eq(collection_id))
        .order(crate::schema::export_templates::id.asc())
        .select((
            crate::schema::export_templates::name,
            crate::schema::export_templates::template,
        ))
        .load::<(String, String)>(connection)
        .await?;
    for candidate in &state.export_templates {
        let candidate = candidate.clone().into_parts();
        let resolved = resolve_collection(
            connection,
            state,
            candidate.collection_ref.as_deref(),
            candidate.collection_key.as_ref(),
        )
        .await;
        match resolved {
            Ok(collection) if collection.id().id() == collection_id => {
                sources.push((candidate.name, candidate.template));
            }
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    StorageErrorKind::InvalidInput | StorageErrorKind::NotFound
                ) => {}
            Err(error) => return Err(error),
        }
    }
    let context = serde_json::json!({
        "items": [],
        "meta": {
            "count": 0,
            "truncated": false,
            "scope": { "kind": "objects_in_class", "class_id": 0, "object_id": 0 },
            "content_type": content_type,
        },
        "warnings": [],
        "request": {
            "scope": { "kind": "objects_in_class", "class_id": 0, "object_id": 0 },
            "query": "",
        },
        "source": {
            "id": 0,
            "name": "",
            "description": "",
            "collection_id": 0,
            "hubuum_class_id": 0,
            "data": {},
            "path": [],
            "path_objects": [],
            "related": {},
            "reachable": {},
            "paths": {},
        },
    });
    let auto_escape = if content_type == "text/html" {
        TemplateAutoEscape::Html
    } else {
        TemplateAutoEscape::None
    };
    validate_template_composition(
        name,
        template,
        &sources,
        &context,
        auto_escape,
        TemplateLimits::new(IMPORT_TEMPLATE_RECURSION_LIMIT, IMPORT_TEMPLATE_FUEL),
    )
    .await
    .map_err(|error| {
        PostgresStorageError::invalid_input(format!(
            "Invalid export template composition '{name}': {error}"
        ))
    })
}

async fn upsert_remote_target(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportRemoteTarget,
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
    let class_id = if parts.class_ref.is_some() || parts.class_key.is_some() {
        let class = resolve_class(
            connection,
            state,
            parts.class_ref.as_deref(),
            parts.class_key.as_ref(),
        )
        .await?;
        ensure_class_collection(&class, &collection, "Remote target")?;
        Some(class.id().id())
    } else {
        None
    };
    let existing = crate::schema::remote_targets::table
        .filter(crate::schema::remote_targets::collection_id.eq(collection.id().id()))
        .filter(crate::schema::remote_targets::name.eq(&parts.name))
        .select((
            crate::schema::remote_targets::id,
            crate::schema::remote_targets::created_at,
            crate::schema::remote_targets::updated_at,
            crate::schema::remote_targets::revision,
        ))
        .for_update()
        .first::<(i32, NaiveDateTime, NaiveDateTime, PostgresRevision)>(connection)
        .await
        .optional()?;
    assert_upsert_condition(existing.map(|row| row.3), parts.condition)?;
    if existing.is_some() && !overwrite {
        return Err(PostgresStorageError::conflict(format!(
            "Remote target '{}' already exists in the collection",
            parts.name
        )));
    }
    let allowed_subject_types = serde_json::to_value(
        parts
            .allowed_subject_types
            .into_iter()
            .map(StorageRemoteTargetSubjectType::as_str)
            .collect::<Vec<_>>(),
    )
    .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    match existing {
        Some((id, old_created_at, old_updated_at, _)) => {
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((old_created_at, old_updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                diesel::update(
                    crate::schema::remote_targets::table
                        .filter(crate::schema::remote_targets::id.eq(id)),
                )
                .set((
                    crate::schema::remote_targets::class_id.eq(class_id),
                    crate::schema::remote_targets::description.eq(parts.description),
                    crate::schema::remote_targets::method.eq(parts.method.as_str()),
                    crate::schema::remote_targets::url_template.eq(parts.url_template),
                    crate::schema::remote_targets::headers_template.eq(parts.headers_template),
                    crate::schema::remote_targets::body_template.eq(parts.body_template),
                    crate::schema::remote_targets::auth_config.eq(parts.auth_config),
                    crate::schema::remote_targets::allowed_subject_types.eq(allowed_subject_types),
                    crate::schema::remote_targets::timeout_ms.eq(parts.timeout_ms),
                    crate::schema::remote_targets::enabled.eq(parts.enabled),
                    crate::schema::remote_targets::created_at.eq(created_at),
                    crate::schema::remote_targets::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
                Ok(())
            })
            .await?;
        }
        None => {
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::remote_targets::table)
                .values((
                    crate::schema::remote_targets::collection_id.eq(collection.id().id()),
                    crate::schema::remote_targets::class_id.eq(class_id),
                    crate::schema::remote_targets::name.eq(parts.name),
                    crate::schema::remote_targets::description.eq(parts.description),
                    crate::schema::remote_targets::method.eq(parts.method.as_str()),
                    crate::schema::remote_targets::url_template.eq(parts.url_template),
                    crate::schema::remote_targets::headers_template.eq(parts.headers_template),
                    crate::schema::remote_targets::body_template.eq(parts.body_template),
                    crate::schema::remote_targets::auth_config.eq(parts.auth_config),
                    crate::schema::remote_targets::allowed_subject_types.eq(allowed_subject_types),
                    crate::schema::remote_targets::timeout_ms.eq(parts.timeout_ms),
                    crate::schema::remote_targets::enabled.eq(parts.enabled),
                    crate::schema::remote_targets::created_at.eq(created_at),
                    crate::schema::remote_targets::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
        }
    }
    Ok(())
}

async fn execute_event_sink(
    connection: &mut PostgresConnection,
    state: &mut ImportRuntime,
    input: StorageImportEventSink,
    overwrite: bool,
) -> Result<(), PostgresStorageError> {
    let parts = input.into_parts();
    let existing = crate::schema::event_sinks::table
        .filter(crate::schema::event_sinks::name.eq(&parts.name))
        .select((
            crate::schema::event_sinks::id,
            crate::schema::event_sinks::created_at,
            crate::schema::event_sinks::updated_at,
            crate::schema::event_sinks::revision,
        ))
        .for_update()
        .first::<(i32, NaiveDateTime, NaiveDateTime, PostgresRevision)>(connection)
        .await
        .optional()?;
    assert_upsert_condition(existing.map(|row| row.3), parts.condition)?;
    if existing.is_some() && !overwrite {
        return Err(PostgresStorageError::conflict(format!(
            "Event sink '{}' already exists",
            parts.name
        )));
    }
    let sink_id = match existing {
        Some((id, old_created_at, old_updated_at, _)) => {
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((old_created_at, old_updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                diesel::update(
                    crate::schema::event_sinks::table.filter(crate::schema::event_sinks::id.eq(id)),
                )
                .set((
                    crate::schema::event_sinks::kind.eq(parts.kind),
                    crate::schema::event_sinks::config.eq(parts.config),
                    crate::schema::event_sinks::secret_ref.eq(parts.secret_ref),
                    crate::schema::event_sinks::enabled.eq(parts.enabled),
                    crate::schema::event_sinks::created_at.eq(created_at),
                    crate::schema::event_sinks::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
                Ok(id)
            })
            .await?
        }
        None => {
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::event_sinks::table)
                .values((
                    crate::schema::event_sinks::name.eq(parts.name),
                    crate::schema::event_sinks::kind.eq(parts.kind),
                    crate::schema::event_sinks::config.eq(parts.config),
                    crate::schema::event_sinks::secret_ref.eq(parts.secret_ref),
                    crate::schema::event_sinks::enabled.eq(parts.enabled),
                    crate::schema::event_sinks::created_at.eq(created_at),
                    crate::schema::event_sinks::updated_at.eq(updated_at),
                ))
                .returning(crate::schema::event_sinks::id)
                .get_result::<i32>(connection)
                .await?
        }
    };
    if let Some(reference) = parts.reference {
        state.event_sinks_by_ref.insert(reference, sink_id);
    }
    Ok(())
}

async fn upsert_event_subscription(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportEventSubscription,
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
    let sink_id = resolve_event_sink(
        connection,
        state,
        parts.sink_ref.as_deref(),
        parts.sink_key.as_ref(),
    )
    .await?;
    validate_event_subscription(
        &parts.entity_types,
        &parts.actions,
        &parts.filter,
        &parts.routing,
    )?;
    let existing = crate::schema::event_subscriptions::table
        .filter(crate::schema::event_subscriptions::collection_id.eq(collection.id().id()))
        .filter(crate::schema::event_subscriptions::name.eq(&parts.name))
        .select((
            crate::schema::event_subscriptions::id,
            crate::schema::event_subscriptions::created_at,
            crate::schema::event_subscriptions::updated_at,
            crate::schema::event_subscriptions::revision,
        ))
        .for_update()
        .first::<(i32, NaiveDateTime, NaiveDateTime, PostgresRevision)>(connection)
        .await
        .optional()?;
    assert_upsert_condition(existing.map(|row| row.3), parts.condition)?;
    if existing.is_some() && !overwrite {
        return Err(PostgresStorageError::conflict(format!(
            "Event subscription '{}' already exists in the collection",
            parts.name
        )));
    }
    let entity_types = serde_json::to_value(parts.entity_types)
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    let actions = serde_json::to_value(parts.actions)
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    match existing {
        Some((id, old_created_at, old_updated_at, _)) => {
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((old_created_at, old_updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                diesel::update(
                    crate::schema::event_subscriptions::table
                        .filter(crate::schema::event_subscriptions::id.eq(id)),
                )
                .set((
                    crate::schema::event_subscriptions::sink_id.eq(sink_id),
                    crate::schema::event_subscriptions::description.eq(parts.description),
                    crate::schema::event_subscriptions::entity_types.eq(entity_types),
                    crate::schema::event_subscriptions::actions.eq(actions),
                    crate::schema::event_subscriptions::filter.eq(parts.filter),
                    crate::schema::event_subscriptions::routing.eq(parts.routing),
                    crate::schema::event_subscriptions::enabled.eq(parts.enabled),
                    crate::schema::event_subscriptions::created_at.eq(created_at),
                    crate::schema::event_subscriptions::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
                Ok(())
            })
            .await?;
        }
        None => {
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::event_subscriptions::table)
                .values((
                    crate::schema::event_subscriptions::collection_id.eq(collection.id().id()),
                    crate::schema::event_subscriptions::sink_id.eq(sink_id),
                    crate::schema::event_subscriptions::name.eq(parts.name),
                    crate::schema::event_subscriptions::description.eq(parts.description),
                    crate::schema::event_subscriptions::entity_types.eq(entity_types),
                    crate::schema::event_subscriptions::actions.eq(actions),
                    crate::schema::event_subscriptions::filter.eq(parts.filter),
                    crate::schema::event_subscriptions::routing.eq(parts.routing),
                    crate::schema::event_subscriptions::enabled.eq(parts.enabled),
                    crate::schema::event_subscriptions::created_at.eq(created_at),
                    crate::schema::event_subscriptions::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
        }
    }
    Ok(())
}

async fn resolve_event_sink(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    reference: Option<&str>,
    key: Option<&StorageImportEventSinkKey>,
) -> Result<i32, PostgresStorageError> {
    if let Some(reference) = reference
        && let Some(id) = state.event_sinks_by_ref.get(reference)
    {
        return Ok(*id);
    }
    let name = key
        .cloned()
        .map(StorageImportEventSinkKey::into_parts)
        .map(|parts| parts.name)
        .ok_or_else(|| {
            PostgresStorageError::invalid_input(
                "Event-sink reference was not resolved and no sink_key was supplied",
            )
        })?;
    crate::schema::event_sinks::table
        .filter(crate::schema::event_sinks::name.eq(name))
        .select(crate::schema::event_sinks::id)
        .first::<i32>(connection)
        .await
        .optional()?
        .ok_or_else(|| PostgresStorageError::not_found("Event sink was not found"))
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

fn validate_event_subscription(
    entity_types: &[String],
    actions: &[String],
    filter: &serde_json::Value,
    routing: &serde_json::Value,
) -> Result<(), PostgresStorageError> {
    if entity_types.is_empty() {
        return Err(PostgresStorageError::invalid_input(
            "Event subscription entity_types must not be empty",
        ));
    }
    if actions.is_empty() {
        return Err(PostgresStorageError::invalid_input(
            "Event subscription actions must not be empty",
        ));
    }
    serde_json::from_value::<hubuum_events_core::EventSubscriptionFilter>(filter.clone()).map_err(
        |error| {
            PostgresStorageError::invalid_input(format!(
                "Invalid event subscription filter: {error}"
            ))
        },
    )?;
    if !routing.is_object() {
        return Err(PostgresStorageError::invalid_input(
            "Event subscription routing must be an object",
        ));
    }
    Ok(())
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
            let (from, to) = resolve_class_relation_endpoints(connection, state, input).await?;
            class_relation_revision(connection, normalize_pair(from.id().id(), to.id().id()))
                .await?
        }
        StorageImportOperation::UpdateObjectRelationTimestamps { input, .. }
        | StorageImportOperation::CheckObjectRelationCondition(input) => {
            let (from, to) = resolve_object_relation_endpoints(connection, state, input).await?;
            object_relation_revision(connection, normalize_pair(from.id().id(), to.id().id()))
                .await?
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
