mod import;
use import::plan_import_with_admin_status;

use crate::models::token_scope::TokenScope;
use hubuum_domain::JsonSchemaLimits;
use hubuum_domain::{ClassId, CollectionId, ObjectId};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use tracing::{Instrument, info, info_span};

use super::helpers::{
    identifier_collection, normalize_pair, planned_result, sanitize_error_for_storage,
    should_abort_import, storage_class_to_resolution, storage_collection_to_resolution,
    storage_object_to_resolution,
};
use super::preload::{preload_existing_classes, preload_existing_objects};
use super::resolution::{
    remember_class, remember_collection, remember_object, resolve_class_planning,
    resolve_collection_by_id_planning, resolve_collection_parent_planning,
    resolve_collection_planning, resolve_object_planning,
};
use super::types::{
    ClassResolution, CollectionResolution, FailureKind, ImportAdminStatus, ObjectResolution,
    PlannedExecution, PlannedItem, PlannedTaskResult, PlanningFailure, PlanningOutcome,
    PlanningState,
};
use crate::errors::ApiError;
use crate::models::{
    CONDITIONAL_IMPORT_TARGET_MISSING, CollectionID, ImportAtomicity, ImportClassInput,
    ImportClassRelationInput, ImportCollectionInput, ImportCollectionPermissionInput,
    ImportCollisionPolicy, ImportMode, ImportObjectInput, ImportObjectRelationInput,
    ImportPermissionPolicy, ImportPrincipalSubtype, ImportRequest, ImportWriteCondition,
    Permissions, RestoreTimestamps,
};
use crate::permissions::{
    AuthorizationContext, AuthorizationMode, ClassResourceEndpoint, ResourceRef,
    authorize_resources,
};
use crate::services::schema_evolution;

use crate::storage::{
    ImportStorage, StorageClassSchemaPolicy, StorageImportPlanItem, storage_handle,
};
use crate::traits::{UserPermissions, scope_allows, scope_allows_resource};

fn import_item_allows_overwrite(
    condition: Option<ImportWriteCondition>,
    mode: &ImportMode,
) -> bool {
    condition
        .map(ImportWriteCondition::allows_overwrite)
        .unwrap_or_else(|| {
            matches!(
                mode.collision_policy,
                Some(ImportCollisionPolicy::Overwrite)
            )
        })
}

fn import_item_requires_existing(condition: Option<ImportWriteCondition>) -> bool {
    condition.is_some_and(ImportWriteCondition::requires_existing)
}
use crate::permissions::PrincipalRef;

fn extended_graph_items(request: &ImportRequest) -> usize {
    request.graph.identity_scopes.len()
        + request.graph.groups.len()
        + request.graph.principals.len()
        + request.graph.group_memberships.len()
        + request.graph.computed_fields.len()
        + request.graph.export_templates.len()
        + request.graph.remote_targets.len()
        + request.graph.event_sinks.len()
        + request.graph.event_subscriptions.len()
}

fn duplicate_ref<'a>(references: impl Iterator<Item = Option<&'a str>>) -> Option<String> {
    let mut seen = HashSet::new();
    references
        .flatten()
        .find(|reference| !seen.insert((*reference).to_string()))
        .map(str::to_string)
}

fn duplicate_extended_ref(request: &ImportRequest) -> Option<(&'static str, String)> {
    [
        (
            "identity_scope",
            duplicate_ref(
                request
                    .graph
                    .identity_scopes
                    .iter()
                    .map(|input| input.ref_.as_deref()),
            ),
        ),
        (
            "group",
            duplicate_ref(
                request
                    .graph
                    .groups
                    .iter()
                    .map(|input| input.ref_.as_deref()),
            ),
        ),
        (
            "principal",
            duplicate_ref(
                request
                    .graph
                    .principals
                    .iter()
                    .map(|input| input.ref_.as_deref()),
            ),
        ),
        (
            "event_sink",
            duplicate_ref(
                request
                    .graph
                    .event_sinks
                    .iter()
                    .map(|input| input.ref_.as_deref()),
            ),
        ),
        (
            "computed_field",
            duplicate_ref(
                request
                    .graph
                    .computed_fields
                    .iter()
                    .map(|input| input.ref_.as_deref()),
            ),
        ),
    ]
    .into_iter()
    .find_map(|(kind, reference)| reference.map(|reference| (kind, reference)))
}

fn plan_system_item(
    entity_kind: &str,
    reference: Option<String>,
    identifier: String,
    execution: PlannedExecution,
) -> PlannedItem {
    PlannedItem {
        result: planned_result(entity_kind, "upsert", reference, Some(identifier)),
        execution: Some(execution),
    }
}

fn preflight_failure_kind(error: &ApiError) -> FailureKind {
    match error {
        ApiError::Forbidden(_) | ApiError::Unauthorized(_) | ApiError::ReauthenticationRequired => {
            FailureKind::Permission
        }
        ApiError::Conflict(_)
        | ApiError::PreconditionFailed(_, _)
        | ApiError::RevisionConflict(_, _) => FailureKind::Collision,
        ApiError::NotFound(_) | ApiError::Gone(_) => FailureKind::Resolution,
        ApiError::BadRequest(_)
        | ApiError::ValidationError(_)
        | ApiError::NotAcceptable(_)
        | ApiError::UnsupportedMediaType(_)
        | ApiError::PayloadTooLarge(_)
        | ApiError::OperatorMismatch(_)
        | ApiError::InvalidIntegerRange(_) => FailureKind::Validation,
        ApiError::TaskCapacity(_)
        | ApiError::TaskStopped(_)
        | ApiError::InternalServerError(_)
        | ApiError::DatabaseError(_)
        | ApiError::NotImplemented(_)
        | ApiError::PermissionBackendUnavailable(_)
        | ApiError::TooManyRequests(_)
        | ApiError::ServiceUnavailable(_)
        | ApiError::DbConnectionError(_)
        | ApiError::HashError(_) => FailureKind::Runtime,
    }
}

async fn preflight_dry_run(
    pool: &impl crate::storage::StorageContext,
    mode: &ImportMode,
    planned_items: Vec<PlannedItem>,
) -> Result<PlanningOutcome, ApiError> {
    let plan = planned_items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| item.execution.clone().map(|execution| (index, execution)))
        .map(|(index, execution)| {
            crate::services::import_boundary::import_operation_to_storage(execution)
                .map(|execution| StorageImportPlanItem::new(index, execution))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let plan = crate::storage::StorageImportPlan::try_new(plan).map_err(ApiError::from)?;
    let (preflight_items, aborted) = storage_handle(pool)
        .preflight_import(
            plan,
            crate::services::import_boundary::import_mode_to_storage(mode.clone()),
        )
        .await?
        .into_parts();
    let cutoff = aborted
        .then(|| preflight_items.last().map(|item| item.index()))
        .flatten();
    let mut outcomes = preflight_items
        .into_iter()
        .map(|item| {
            let (index, revision, error) = item.into_parts();
            (index, (revision, error))
        })
        .collect::<HashMap<_, _>>();
    let mut valid_items = Vec::with_capacity(planned_items.len());
    let mut failures = Vec::new();

    for (index, mut item) in planned_items.into_iter().enumerate() {
        if cutoff.is_some_and(|cutoff| index > cutoff) {
            break;
        }
        let Some((revision, error)) = outcomes.remove(&index) else {
            if item.execution.is_none() {
                valid_items.push(item);
            }
            continue;
        };
        item.result.set_observed_revision(revision);
        if let Some(error) = error {
            let error = ApiError::from(error);
            failures.push(PlanningFailure {
                kind: preflight_failure_kind(&error),
                item: item.result,
                message: sanitize_error_for_storage(&error),
            });
        } else {
            valid_items.push(item);
        }
    }

    Ok(PlanningOutcome {
        planned_items: valid_items,
        failures,
        aborted,
    })
}

async fn is_import_admin<C>(
    backend: &C,
    user: &impl crate::traits::AuthzSubject,
    state: &mut PlanningState,
) -> Result<bool, String>
where
    C: AuthorizationContext,
{
    if let Some(is_admin) = state.admin_status.known() {
        return Ok(is_admin);
    }
    let pool = backend;
    let is_admin = match backend.authorization_mode() {
        crate::permissions::AuthorizationMode::Delegated(permission_backend) => {
            let principal = PrincipalRef::load(pool, user)
                .await
                .map_err(|err| err.to_string())?;
            permission_backend
                .is_admin(&principal)
                .await
                .map_err(|err| err.to_string())?
        }
        crate::permissions::AuthorizationMode::LocalStorage => {
            user.is_admin(pool).await.map_err(|err| err.to_string())?
        }
    };
    state.admin_status = ImportAdminStatus::Known(is_admin);
    Ok(is_admin)
}

async fn ensure_collection_permission_cached<C>(
    backend: &C,
    user: &impl crate::traits::AuthzSubject,
    state: &mut PlanningState,
    collection_id: i32,
    collection_exists_in_db: bool,
    permission: Permissions,
) -> Result<(), String>
where
    C: AuthorizationContext,
{
    if state.scopes.is_none() && is_import_admin(backend, user, state).await? {
        return Ok(());
    }

    if !collection_exists_in_db {
        return Err("Only admins may operate on newly created collections within an import".into());
    }

    let key = (collection_id, permission);
    if let Some(result) = state.collection_permission_cache.get(&key) {
        return result.clone();
    }

    let collection = CollectionID::new(collection_id).map_err(|err| err.to_string())?;
    let scopes = state.scopes.clone();
    let result: Result<(), crate::errors::ApiError> = async {
        crate::can!(backend, user, scopes.as_ref(), [permission], collection);
        Ok(())
    }
    .await;
    let result = result.map_err(|err| err.to_string());
    state
        .collection_permission_cache
        .insert(key, result.clone());
    result
}

/// Existing resources must retain their identity for external policy and token
/// resource checks. Only local collection grants may share a cached decision.
async fn ensure_existing_resource_permission<C>(
    backend: &C,
    user: &impl crate::traits::AuthzSubject,
    state: &mut PlanningState,
    resource: ResourceRef,
    permission: Permissions,
) -> Result<(), String>
where
    C: AuthorizationContext,
{
    if state.scopes.is_none() && is_import_admin(backend, user, state).await? {
        return Ok(());
    }
    if !scope_allows(state.scopes.as_ref(), &[permission])
        || !scope_allows_resource(state.scopes.as_ref(), &resource)
    {
        return Err("Permission denied".into());
    }
    match backend.authorization_mode() {
        AuthorizationMode::Delegated(permission_backend) => authorize_resources(
            permission_backend,
            backend,
            user,
            state.scopes.as_ref(),
            vec![permission],
            vec![resource],
        )
        .await
        .map_err(|error| error.to_string()),
        AuthorizationMode::LocalStorage => {
            let collection_id = resource
                .collection_id()
                .ok_or_else(|| "Import resource has no collection".to_string())?;
            let key = (collection_id, permission);
            if let Some(result) = state.collection_permission_cache.get(&key) {
                return result.clone();
            }
            let collection = CollectionID::new(collection_id).map_err(|error| error.to_string())?;
            let result = user
                .can(backend, [permission], [collection], state.scopes.as_ref())
                .await
                .map_err(|error| error.to_string());
            state
                .collection_permission_cache
                .insert(key, result.clone());
            result
        }
    }
}

async fn class_relation_exists_cached(
    pool: &impl crate::storage::StorageContext,
    state: &mut PlanningState,
    left: i32,
    right: i32,
) -> Result<bool, String> {
    let pair = normalize_pair(left, right);
    if state.class_relations.contains(&pair) {
        return Ok(true);
    }
    // Negative IDs exist only inside this planning pass. A relation involving
    // a not-yet-persisted class cannot already exist in storage, and virtual
    // IDs must never cross the storage boundary.
    if pair.0 <= 0 || pair.1 <= 0 {
        return Ok(false);
    }
    if let Some(exists) = state.class_relation_exists_cache.get(&pair) {
        return Ok(*exists);
    }

    let exists = storage_handle(pool)
        .has_import_class_relation(
            ClassId::new(pair.0).map_err(|error| error.to_string())?,
            ClassId::new(pair.1).map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|err| sanitize_error_for_storage(&err.into()))?;
    state.class_relation_exists_cache.insert(pair, exists);
    Ok(exists)
}

async fn object_relation_exists_cached(
    pool: &impl crate::storage::StorageContext,
    state: &mut PlanningState,
    left: i32,
    right: i32,
) -> Result<bool, String> {
    let pair = normalize_pair(left, right);
    if state.object_relations.contains(&pair) {
        return Ok(true);
    }
    // Keep planner-local object IDs on the application side of the boundary.
    // The backend lookup contract accepts persisted IDs only.
    if pair.0 <= 0 || pair.1 <= 0 {
        return Ok(false);
    }
    if let Some(exists) = state.object_relation_exists_cache.get(&pair) {
        return Ok(*exists);
    }

    let exists = storage_handle(pool)
        .has_import_object_relation(
            ObjectId::new(pair.0).map_err(|error| error.to_string())?,
            ObjectId::new(pair.1).map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|err| sanitize_error_for_storage(&err.into()))?;
    state.object_relation_exists_cache.insert(pair, exists);
    Ok(exists)
}

#[derive(Debug)]
enum RelationPlanDecision {
    Create,
    Update(RestoreTimestamps),
    Noop,
    Collision,
}

impl RelationPlanDecision {
    fn new(
        relation_exists: bool,
        timestamps: Option<&RestoreTimestamps>,
        collision_policy: Option<ImportCollisionPolicy>,
    ) -> Self {
        if !relation_exists {
            return Self::Create;
        }
        if matches!(collision_policy, Some(ImportCollisionPolicy::Abort)) {
            return Self::Collision;
        }
        match timestamps {
            Some(timestamps) => Self::Update(timestamps.clone()),
            None => Self::Noop,
        }
    }

    fn permission_requirement(
        &self,
        create_permission: Permissions,
        update_permission: Permissions,
    ) -> (Permissions, &'static str) {
        match self {
            Self::Update(_) => (update_permission, "update"),
            Self::Create | Self::Noop | Self::Collision => (create_permission, "create"),
        }
    }
}

#[allow(
    clippy::result_large_err,
    reason = "planning failures retain the complete item for batched diagnostics and persistence"
)]
async fn ensure_relation_permissions<C>(
    backend: &C,
    user: &impl crate::traits::AuthzSubject,
    state: &mut PlanningState,
    collections: [&CollectionResolution; 2],
    permission: Permissions,
    failure_item: PlannedTaskResult,
) -> Result<(), PlanningFailure>
where
    C: AuthorizationContext,
{
    for collection in collections {
        ensure_collection_permission_cached(
            backend,
            user,
            state,
            collection.id,
            collection.exists_in_db,
            permission,
        )
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Permission,
            item: failure_item.clone(),
            message,
        })?;
    }
    Ok(())
}

pub(super) async fn plan_import<C>(
    backend: &C,
    user: &impl crate::traits::AuthzSubject,
    scopes: Option<&TokenScope>,
    request: &ImportRequest,
) -> PlanningOutcome
where
    C: AuthorizationContext,
{
    plan_import_with_admin_status(backend, user, scopes, request, ImportAdminStatus::Unknown).await
}

#[cfg(test)]
pub(super) async fn plan_runtime_admin_import<C>(
    backend: &C,
    user: &impl crate::traits::AuthzSubject,
    request: &ImportRequest,
) -> PlanningOutcome
where
    C: AuthorizationContext,
{
    plan_import_with_admin_status(backend, user, None, request, ImportAdminStatus::Known(true))
        .await
}

#[allow(
    clippy::result_large_err,
    reason = "planning failures retain the complete item for batched diagnostics and persistence"
)]
pub(super) async fn plan_collection<C>(
    backend: &C,
    user: &impl crate::traits::AuthzSubject,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportCollectionInput,
) -> Result<PlannedItem, PlanningFailure>
where
    C: AuthorizationContext,
{
    let pool = backend;
    if let Some(reference) = &input.ref_
        && state.collections_by_ref.contains_key(reference)
    {
        return Err(PlanningFailure {
            kind: FailureKind::Validation,
            item: planned_result(
                "collection",
                "create",
                input.ref_.clone(),
                Some(input.name.clone()),
            ),
            message: format!("Duplicate collection ref '{reference}'"),
        });
    }

    let parent = resolve_collection_parent_planning(pool, state, input)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Validation,
            item: planned_result(
                "collection",
                "lookup",
                input.ref_.clone(),
                Some(input.name.clone()),
            ),
            message,
        })?;
    let planned_key = (Some(parent.id), input.name.clone());

    if !state.planned_collection_keys.insert(planned_key.clone()) {
        return Err(PlanningFailure {
            kind: FailureKind::Validation,
            item: planned_result(
                "collection",
                "create",
                input.ref_.clone(),
                Some(input.name.clone()),
            ),
            message: format!(
                "Duplicate collection name '{}' under parent '{}' within import request",
                input.name, parent.name
            ),
        });
    }

    let existing = if parent.exists_in_db {
        if let Some(collection) = state
            .collections_by_parent_name
            .get(&(Some(parent.id), input.name.clone()))
            .cloned()
            .filter(|collection| collection.exists_in_db)
        {
            Some(CollectionResolution {
                id: collection.id,
                name: collection.name,
                description: collection.description,
                parent_collection_id: collection.parent_collection_id,
                exists_in_db: true,
            })
        } else {
            storage_handle(pool)
                .get_import_collection_child_by_name(
                    CollectionId::new(parent.id).map_err(|error| PlanningFailure {
                        kind: FailureKind::Runtime,
                        item: planned_result(
                            "collection",
                            "lookup",
                            input.ref_.clone(),
                            Some(input.name.clone()),
                        ),
                        message: error.to_string(),
                    })?,
                    &input.name,
                )
                .await
                .map_err(|message| PlanningFailure {
                    kind: FailureKind::Runtime,
                    item: planned_result(
                        "collection",
                        "lookup",
                        input.ref_.clone(),
                        Some(input.name.clone()),
                    ),
                    message: sanitize_error_for_storage(&message.into()),
                })?
                .map(storage_collection_to_resolution)
        }
    } else {
        None
    };

    if let Some(collection) = existing {
        ensure_collection_permission_cached(
            backend,
            user,
            state,
            collection.id,
            true,
            Permissions::UpdateCollection,
        )
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Permission,
            item: planned_result(
                "collection",
                "update",
                input.ref_.clone(),
                Some(collection.name.clone()),
            ),
            message,
        })?;

        if !import_item_allows_overwrite(input.condition, mode) {
            return Err(PlanningFailure {
                kind: FailureKind::Collision,
                item: planned_result(
                    "collection",
                    "update",
                    input.ref_.clone(),
                    Some(collection.name),
                ),
                message: format!("Collection '{}' already exists", input.name),
            });
        }

        let resolution = CollectionResolution {
            id: collection.id,
            name: collection.name.clone(),
            description: input.description.clone(),
            parent_collection_id: collection.parent_collection_id,
            exists_in_db: true,
        };
        remember_collection(state, input.ref_.clone(), resolution.clone());

        Ok(PlannedItem {
            result: planned_result(
                "collection",
                "update",
                input.ref_.clone(),
                Some(identifier_collection(&resolution)),
            ),
            execution: Some(PlannedExecution::UpdateCollection {
                collection_id: collection.id,
                input: input.clone(),
            }),
        })
    } else {
        if import_item_requires_existing(input.condition) {
            return Err(PlanningFailure {
                kind: FailureKind::Collision,
                item: planned_result(
                    "collection",
                    "update",
                    input.ref_.clone(),
                    Some(input.name.clone()),
                ),
                message: CONDITIONAL_IMPORT_TARGET_MISSING.to_string(),
            });
        }
        if state.scopes.is_some()
            || !is_import_admin(backend, user, state)
                .await
                .map_err(|err| PlanningFailure {
                    kind: FailureKind::Permission,
                    item: planned_result(
                        "collection",
                        "create",
                        input.ref_.clone(),
                        Some(input.name.clone()),
                    ),
                    message: err,
                })?
        {
            return Err(PlanningFailure {
                kind: FailureKind::Permission,
                item: planned_result(
                    "collection",
                    "create",
                    input.ref_.clone(),
                    Some(input.name.clone()),
                ),
                message: "Only unscoped administrators may create collections".to_string(),
            });
        }

        let resolution = CollectionResolution {
            id: state.next_virtual_id(),
            name: input.name.clone(),
            description: input.description.clone(),
            parent_collection_id: Some(parent.id),
            exists_in_db: false,
        };
        remember_collection(state, input.ref_.clone(), resolution.clone());

        Ok(PlannedItem {
            result: planned_result(
                "collection",
                "create",
                input.ref_.clone(),
                Some(identifier_collection(&resolution)),
            ),
            execution: Some(PlannedExecution::CreateCollection(input.clone())),
        })
    }
}

fn planned_class_schema_policy(
    schema_limits: JsonSchemaLimits,
    class: &ClassResolution,
) -> Result<StorageClassSchemaPolicy, ApiError> {
    let schema_policy =
        StorageClassSchemaPolicy::try_from_parts(class.json_schema.clone(), class.validate_schema)
            .map_err(|error| ApiError::from(error.into_request_error()))?;
    if let Some(schema) = schema_policy
        .json_schema()
        .filter(|_| schema_policy.validates_schema())
    {
        schema_limits
            .validate_schema_for_instances(schema)
            .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    }
    Ok(schema_policy)
}

#[allow(
    clippy::result_large_err,
    reason = "planning failures retain the complete item for batched diagnostics and persistence"
)]
pub(super) async fn plan_class<C>(
    backend: &C,
    user: &impl crate::traits::AuthzSubject,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportClassInput,
) -> Result<PlannedItem, PlanningFailure>
where
    C: AuthorizationContext,
{
    let pool = backend;
    if input.schema_activation.is_some()
        && (state.scopes.is_some()
            || !is_import_admin(backend, user, state)
                .await
                .map_err(|message| PlanningFailure {
                    kind: FailureKind::Permission,
                    item: planned_result(
                        "class",
                        "activate_schema",
                        input.ref_.clone(),
                        Some(input.name.clone()),
                    ),
                    message,
                })?)
    {
        return Err(PlanningFailure {
            kind: FailureKind::Permission,
            item: planned_result(
                "class",
                "activate_schema",
                input.ref_.clone(),
                Some(input.name.clone()),
            ),
            message: "Schema activation inside imports requires an unscoped administrator token"
                .to_string(),
        });
    }

    if let Some(schema) = input.json_schema.as_ref() {
        storage_handle(backend)
            .schema_limits()
            .validate_schema(schema)
            .map_err(|error| PlanningFailure {
                kind: FailureKind::Validation,
                item: planned_result(
                    "class",
                    "validate",
                    input.ref_.clone(),
                    Some(input.name.clone()),
                ),
                message: error.to_string(),
            })?;
    }
    if let Some(reference) = &input.ref_
        && state.classes_by_ref.contains_key(reference)
    {
        return Err(PlanningFailure {
            kind: FailureKind::Validation,
            item: planned_result(
                "class",
                "create",
                input.ref_.clone(),
                Some(input.name.clone()),
            ),
            message: format!("Duplicate class ref '{reference}'"),
        });
    }

    let collection = resolve_collection_planning(
        pool,
        state,
        input.collection_ref.as_deref(),
        input.collection_key.as_ref(),
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

    let class_key = (collection.id, input.name.clone());
    if !state.planned_class_keys.insert(class_key.clone()) {
        return Err(PlanningFailure {
            kind: FailureKind::Validation,
            item: planned_result(
                "class",
                "create",
                input.ref_.clone(),
                Some(format!("{}::{}", collection.name, input.name)),
            ),
            message: format!(
                "Duplicate class name '{}' within collection '{}'",
                input.name, collection.name
            ),
        });
    }

    let existing = if let Some(class) = state
        .classes_by_key
        .get(&class_key)
        .cloned()
        .filter(|class| class.exists_in_db)
    {
        Some(class)
    } else if state.missing_class_keys.contains(&class_key) {
        None
    } else {
        storage_handle(pool)
            .get_import_class_by_name(
                CollectionId::new(collection.id).map_err(|error| PlanningFailure {
                    kind: FailureKind::Runtime,
                    item: planned_result(
                        "class",
                        "lookup",
                        input.ref_.clone(),
                        Some(input.name.clone()),
                    ),
                    message: error.to_string(),
                })?,
                &input.name,
            )
            .await
            .map_err(|err| PlanningFailure {
                kind: FailureKind::Runtime,
                item: planned_result(
                    "class",
                    "lookup",
                    input.ref_.clone(),
                    Some(input.name.clone()),
                ),
                message: sanitize_error_for_storage(&err.into()),
            })?
            .map(storage_class_to_resolution)
    };

    let identifier = format!("{}::{}", collection.name, input.name);

    if let Some(class) = existing {
        ensure_existing_resource_permission(
            backend,
            user,
            state,
            ResourceRef::class(class.id, class.collection_id, Some(class.name.clone())),
            Permissions::UpdateClass,
        )
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

        if !import_item_allows_overwrite(input.condition, mode) {
            return Err(PlanningFailure {
                kind: FailureKind::Collision,
                item: planned_result("class", "update", input.ref_.clone(), Some(identifier)),
                message: format!(
                    "Class '{}' already exists in collection '{}'",
                    input.name, collection.name
                ),
            });
        }

        let updated = ClassResolution {
            id: class.id,
            name: input.name.clone(),
            collection_id: collection.id,
            json_schema: if input.schema_activation.is_some() {
                input.json_schema.clone()
            } else {
                input
                    .json_schema
                    .clone()
                    .or_else(|| class.json_schema.clone())
            },
            validate_schema: if input.schema_activation.is_some() {
                input.validate_schema.unwrap_or(false)
            } else {
                input.validate_schema.unwrap_or(class.validate_schema)
            },
            exists_in_db: true,
        };
        let schema_policy =
            planned_class_schema_policy(storage_handle(backend).schema_limits(), &updated)
                .map_err(|error| PlanningFailure {
                    kind: FailureKind::Validation,
                    item: planned_result(
                        "class",
                        "validate",
                        input.ref_.clone(),
                        Some(input.name.clone()),
                    ),
                    message: error.to_string(),
                })?;
        let (json_schema, validate_schema) = schema_policy.into_parts();
        let execution_input = ImportClassInput {
            json_schema,
            validate_schema: Some(validate_schema),
            ..input.clone()
        };
        remember_class(state, input.ref_.clone(), updated.clone());

        let mut result = planned_result(
            "class",
            "update",
            input.ref_.clone(),
            Some(format!("{}::{}", collection.name, input.name)),
        );
        if let Some(activation) = &input.schema_activation {
            let impact = if let Some(task) = activation.impact_task_id {
                Some(
                    schema_evolution::get_work(pool, task)
                        .await
                        .map_err(|error| PlanningFailure {
                            kind: FailureKind::Validation,
                            item: result.clone(),
                            message: error.to_string(),
                        })?,
                )
            } else {
                None
            };
            result.details =
                Some(serde_json::json!({"schema_activation":activation,"impact":impact}));
        }
        Ok(PlannedItem {
            result,
            execution: Some(PlannedExecution::UpdateClass {
                class_id: class.id,
                input: execution_input,
            }),
        })
    } else {
        if import_item_requires_existing(input.condition) {
            return Err(PlanningFailure {
                kind: FailureKind::Collision,
                item: planned_result("class", "update", input.ref_.clone(), Some(identifier)),
                message: CONDITIONAL_IMPORT_TARGET_MISSING.to_string(),
            });
        }
        ensure_collection_permission_cached(
            backend,
            user,
            state,
            collection.id,
            collection.exists_in_db,
            Permissions::CreateClass,
        )
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
            collection_id: collection.id,
            json_schema: input.json_schema.clone(),
            validate_schema: input.validate_schema.unwrap_or(false),
            exists_in_db: false,
        };
        let schema_policy =
            planned_class_schema_policy(storage_handle(backend).schema_limits(), &created)
                .map_err(|error| PlanningFailure {
                    kind: FailureKind::Validation,
                    item: planned_result(
                        "class",
                        "validate",
                        input.ref_.clone(),
                        Some(input.name.clone()),
                    ),
                    message: error.to_string(),
                })?;
        let (json_schema, validate_schema) = schema_policy.into_parts();
        let execution_input = ImportClassInput {
            json_schema,
            validate_schema: Some(validate_schema),
            ..input.clone()
        };
        remember_class(state, input.ref_.clone(), created.clone());

        Ok(PlannedItem {
            result: planned_result("class", "create", input.ref_.clone(), Some(identifier)),
            execution: Some(PlannedExecution::CreateClass(execution_input)),
        })
    }
}

#[allow(
    clippy::result_large_err,
    reason = "planning failures retain the complete item for batched diagnostics and persistence"
)]
pub(super) async fn plan_object<C>(
    backend: &C,
    user: &impl crate::traits::AuthzSubject,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportObjectInput,
) -> Result<PlannedItem, PlanningFailure>
where
    C: AuthorizationContext,
{
    let pool = backend;
    if let Some(reference) = &input.ref_
        && state.objects_by_ref.contains_key(reference)
    {
        return Err(PlanningFailure {
            kind: FailureKind::Validation,
            item: planned_result(
                "object",
                "create",
                input.ref_.clone(),
                Some(input.name.clone()),
            ),
            message: format!("Duplicate object ref '{reference}'"),
        });
    }

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
        storage_handle(backend)
            .schema_limits()
            .validate_value(schema, &input.data)
            .map_err(|err| PlanningFailure {
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

    let object_key = (class.id, input.name.clone());
    if !state.planned_object_keys.insert(object_key.clone()) {
        return Err(PlanningFailure {
            kind: FailureKind::Validation,
            item: planned_result(
                "object",
                "create",
                input.ref_.clone(),
                Some(format!("{}::{}", class.name, input.name)),
            ),
            message: format!(
                "Duplicate object name '{}' within class '{}'",
                input.name, class.name
            ),
        });
    }

    let existing = if let Some(object) = state
        .objects_by_key
        .get(&object_key)
        .cloned()
        .filter(|object| object.exists_in_db)
    {
        Some(object)
    } else if state.missing_object_keys.contains(&object_key) {
        None
    } else {
        storage_handle(pool)
            .get_import_object_by_name(
                ClassId::new(class.id).map_err(|error| PlanningFailure {
                    kind: FailureKind::Runtime,
                    item: planned_result(
                        "object",
                        "lookup",
                        input.ref_.clone(),
                        Some(input.name.clone()),
                    ),
                    message: error.to_string(),
                })?,
                &input.name,
            )
            .await
            .map_err(|err| PlanningFailure {
                kind: FailureKind::Runtime,
                item: planned_result(
                    "object",
                    "lookup",
                    input.ref_.clone(),
                    Some(input.name.clone()),
                ),
                message: sanitize_error_for_storage(&err.into()),
            })?
            .map(storage_object_to_resolution)
    };

    let identifier = format!("{}::{}", class.name, input.name);
    let collection = resolve_collection_by_id_planning(pool, state, class.collection_id)
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
        ensure_existing_resource_permission(
            backend,
            user,
            state,
            ResourceRef::object(
                object.id,
                ClassResourceEndpoint::new(object.collection_id, object.class_id),
                Some(object.name.clone()),
            ),
            Permissions::UpdateObject,
        )
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

        if !import_item_allows_overwrite(input.condition, mode) {
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
            collection_id: collection.id,
            class_id: class.id,
            exists_in_db: true,
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
        if import_item_requires_existing(input.condition) {
            return Err(PlanningFailure {
                kind: FailureKind::Collision,
                item: planned_result("object", "update", input.ref_.clone(), Some(identifier)),
                message: CONDITIONAL_IMPORT_TARGET_MISSING.to_string(),
            });
        }
        ensure_collection_permission_cached(
            backend,
            user,
            state,
            collection.id,
            collection.exists_in_db,
            Permissions::CreateObject,
        )
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
            collection_id: collection.id,
            class_id: class.id,
            exists_in_db: false,
        };
        remember_object(state, input.ref_.clone(), created.clone());

        Ok(PlannedItem {
            result: planned_result("object", "create", input.ref_.clone(), Some(identifier)),
            execution: Some(PlannedExecution::CreateObject(input.clone())),
        })
    }
}

#[allow(
    clippy::result_large_err,
    reason = "planning failures retain the complete item for batched diagnostics and persistence"
)]
pub(super) async fn plan_class_relation<C>(
    backend: &C,
    user: &impl crate::traits::AuthzSubject,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportClassRelationInput,
) -> Result<PlannedItem, PlanningFailure>
where
    C: AuthorizationContext,
{
    let pool = backend;
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

    let from_collection = resolve_collection_by_id_planning(pool, state, from_class.collection_id)
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
    let to_collection = resolve_collection_by_id_planning(pool, state, to_class.collection_id)
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

    let relation_exists = class_relation_exists_cached(pool, state, pair.0, pair.1)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Runtime,
            item: planned_result(
                "class_relation",
                "lookup",
                input.ref_.clone(),
                identifier.clone(),
            ),
            message,
        })?;
    if !relation_exists && import_item_requires_existing(input.condition) {
        return Err(PlanningFailure {
            kind: FailureKind::Collision,
            item: planned_result("class_relation", "update", input.ref_.clone(), identifier),
            message: CONDITIONAL_IMPORT_TARGET_MISSING.to_string(),
        });
    }
    let collision_policy = Some(if import_item_allows_overwrite(input.condition, mode) {
        ImportCollisionPolicy::Overwrite
    } else {
        ImportCollisionPolicy::Abort
    });
    let decision =
        RelationPlanDecision::new(relation_exists, input.timestamps.as_ref(), collision_policy);
    let (permission, permission_action) = decision.permission_requirement(
        Permissions::CreateClassRelation,
        Permissions::UpdateClassRelation,
    );
    let permission_item = planned_result(
        "class_relation",
        permission_action,
        input.ref_.clone(),
        identifier.clone(),
    );
    ensure_relation_permissions(
        backend,
        user,
        state,
        [&from_collection, &to_collection],
        permission,
        permission_item,
    )
    .await?;

    match decision {
        RelationPlanDecision::Collision => {
            return Err(PlanningFailure {
                kind: FailureKind::Collision,
                item: planned_result("class_relation", "create", input.ref_.clone(), identifier),
                message: "Class relation already exists".to_string(),
            });
        }
        RelationPlanDecision::Update(timestamps) => {
            return Ok(PlannedItem {
                result: planned_result("class_relation", "update", input.ref_.clone(), identifier),
                execution: Some(PlannedExecution::UpdateClassRelationTimestamps {
                    input: input.clone(),
                    timestamps,
                }),
            });
        }
        RelationPlanDecision::Noop => {
            return Ok(PlannedItem {
                result: planned_result("class_relation", "noop", input.ref_.clone(), identifier),
                execution: input
                    .condition
                    .and_then(ImportWriteCondition::expected_revision)
                    .map(|_| PlannedExecution::CheckClassRelationCondition(input.clone())),
            });
        }
        RelationPlanDecision::Create => {}
    }

    state.class_relations.insert(pair);

    Ok(PlannedItem {
        result: planned_result("class_relation", "create", input.ref_.clone(), identifier),
        execution: Some(PlannedExecution::CreateClassRelation(input.clone())),
    })
}

#[allow(
    clippy::result_large_err,
    reason = "planning failures retain the complete item for batched diagnostics and persistence"
)]
pub(super) async fn plan_object_relation<C>(
    backend: &C,
    user: &impl crate::traits::AuthzSubject,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportObjectRelationInput,
) -> Result<PlannedItem, PlanningFailure>
where
    C: AuthorizationContext,
{
    let pool = backend;
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
    let identifier = Some(format!("{}<->{}", from_object.name, to_object.name));

    let from_collection = resolve_collection_by_id_planning(pool, state, from_object.collection_id)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result(
                "object_relation",
                "create",
                input.ref_.clone(),
                identifier.clone(),
            ),
            message,
        })?;
    let to_collection = resolve_collection_by_id_planning(pool, state, to_object.collection_id)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result(
                "object_relation",
                "create",
                input.ref_.clone(),
                identifier.clone(),
            ),
            message,
        })?;

    let class_pair = normalize_pair(from_object.class_id, to_object.class_id);
    let class_relation_exists =
        class_relation_exists_cached(pool, state, class_pair.0, class_pair.1)
            .await
            .map_err(|message| PlanningFailure {
                kind: FailureKind::Runtime,
                item: planned_result(
                    "object_relation",
                    "lookup",
                    input.ref_.clone(),
                    identifier.clone(),
                ),
                message,
            })?;

    if !class_relation_exists {
        return Err(PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result("object_relation", "create", input.ref_.clone(), identifier),
            message: "Object relation requires a direct class relation between the object classes"
                .to_string(),
        });
    }

    let relation_exists = object_relation_exists_cached(pool, state, pair.0, pair.1)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Runtime,
            item: planned_result(
                "object_relation",
                "lookup",
                input.ref_.clone(),
                identifier.clone(),
            ),
            message,
        })?;
    if !relation_exists && import_item_requires_existing(input.condition) {
        return Err(PlanningFailure {
            kind: FailureKind::Collision,
            item: planned_result("object_relation", "update", input.ref_.clone(), identifier),
            message: CONDITIONAL_IMPORT_TARGET_MISSING.to_string(),
        });
    }
    let collision_policy = Some(if import_item_allows_overwrite(input.condition, mode) {
        ImportCollisionPolicy::Overwrite
    } else {
        ImportCollisionPolicy::Abort
    });
    let decision =
        RelationPlanDecision::new(relation_exists, input.timestamps.as_ref(), collision_policy);
    let (permission, permission_action) = decision.permission_requirement(
        Permissions::CreateObjectRelation,
        Permissions::UpdateObjectRelation,
    );
    let permission_item = planned_result(
        "object_relation",
        permission_action,
        input.ref_.clone(),
        identifier.clone(),
    );
    ensure_relation_permissions(
        backend,
        user,
        state,
        [&from_collection, &to_collection],
        permission,
        permission_item,
    )
    .await?;

    match decision {
        RelationPlanDecision::Collision => {
            return Err(PlanningFailure {
                kind: FailureKind::Collision,
                item: planned_result("object_relation", "create", input.ref_.clone(), identifier),
                message: "Object relation already exists".to_string(),
            });
        }
        RelationPlanDecision::Update(timestamps) => {
            return Ok(PlannedItem {
                result: planned_result("object_relation", "update", input.ref_.clone(), identifier),
                execution: Some(PlannedExecution::UpdateObjectRelationTimestamps {
                    input: input.clone(),
                    timestamps,
                }),
            });
        }
        RelationPlanDecision::Noop => {
            return Ok(PlannedItem {
                result: planned_result("object_relation", "noop", input.ref_.clone(), identifier),
                execution: input
                    .condition
                    .and_then(ImportWriteCondition::expected_revision)
                    .map(|_| PlannedExecution::CheckObjectRelationCondition(input.clone())),
            });
        }
        RelationPlanDecision::Create => {}
    }

    state.object_relations.insert(pair);

    Ok(PlannedItem {
        result: planned_result("object_relation", "create", input.ref_.clone(), identifier),
        execution: Some(PlannedExecution::CreateObjectRelation(input.clone())),
    })
}

#[allow(
    clippy::result_large_err,
    reason = "planning failures retain the complete item for batched diagnostics and persistence"
)]
pub(super) async fn plan_collection_permission<C>(
    backend: &C,
    user: &impl crate::traits::AuthzSubject,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportCollectionPermissionInput,
) -> Result<PlannedItem, PlanningFailure>
where
    C: AuthorizationContext,
{
    let pool = backend;
    let collection = resolve_collection_planning(
        pool,
        state,
        input.collection_ref.as_deref(),
        input.collection_key.as_ref(),
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Resolution,
        item: planned_result(
            "collection_permission",
            "apply",
            input.ref_.clone(),
            Some(input.group_key.groupname.clone()),
        ),
        message,
    })?;

    ensure_collection_permission_cached(
        backend,
        user,
        state,
        collection.id,
        collection.exists_in_db,
        Permissions::DelegateCollection,
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Permission,
        item: planned_result(
            "collection_permission",
            "apply",
            input.ref_.clone(),
            Some(format!(
                "{}::{}",
                collection.name, input.group_key.groupname
            )),
        ),
        message,
    })?;

    let identity_scope = input.group_key.resolve_identity_scope_name();
    let group_exists = storage_handle(pool)
        .has_import_group(identity_scope, &input.group_key.groupname)
        .await
        .map_err(|err| PlanningFailure {
            kind: FailureKind::Runtime,
            item: planned_result(
                "collection_permission",
                "lookup",
                input.ref_.clone(),
                Some(input.group_key.groupname.clone()),
            ),
            message: sanitize_error_for_storage(&err.into()),
        })?
        || state.planned_group_keys.contains(&(
            identity_scope.to_string(),
            input.group_key.groupname.clone(),
        ));
    if !group_exists {
        return Err(PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result(
                "collection_permission",
                "apply",
                input.ref_.clone(),
                Some(input.group_key.groupname.clone()),
            ),
            message: format!(
                "Group '{}/{}' not found",
                identity_scope, input.group_key.groupname
            ),
        });
    }

    Ok(PlannedItem {
        result: planned_result(
            "collection_permission",
            if input.replace_existing.unwrap_or(false) {
                "replace"
            } else {
                "grant"
            },
            input.ref_.clone(),
            Some(format!(
                "{}::{}",
                collection.name, input.group_key.groupname
            )),
        ),
        execution: Some(PlannedExecution::ApplyCollectionPermissions {
            input: input.clone(),
            overwrite: import_item_allows_overwrite(input.condition, mode),
        }),
    })
}
