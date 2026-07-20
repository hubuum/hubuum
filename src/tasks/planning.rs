use std::collections::HashSet;
use std::time::Instant;

use chrono::Utc;
use tracing::{Instrument, info, info_span};

use super::helpers::{
    class_to_resolution, identifier_collection, normalize_pair, object_to_resolution,
    planned_result, sanitize_error_for_storage, should_abort_import,
};
use super::preload::{preload_existing_classes, preload_existing_objects};
use super::resolution::{
    lookup_existing_collection_for_import_db, remember_class, remember_collection, remember_object,
    resolve_class_planning, resolve_collection_by_id_planning, resolve_collection_parent_planning,
    resolve_collection_planning, resolve_object_planning,
};
use super::types::{
    ClassResolution, CollectionResolution, FailureKind, ObjectResolution, PlannedExecution,
    PlannedItem, PlanningFailure, PlanningOutcome, PlanningState,
};
use crate::db::prelude::AsyncConnection;
use crate::db::traits::UserPermissions;
use crate::db::traits::task_import::{
    lookup_class_by_collection_and_name, lookup_direct_class_relation, lookup_group_by_name,
    lookup_object_by_class_and_name, lookup_object_relation,
};
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::{
    Collection, CollectionID, ImportAtomicity, ImportClassInput, ImportClassRelationInput,
    ImportCollectionInput, ImportCollectionPermissionInput, ImportCollisionPolicy, ImportMode,
    ImportObjectInput, ImportObjectRelationInput, ImportPermissionPolicy, ImportPrincipalSubtype,
    ImportRequest, Permissions,
};
use crate::permissions::PrincipalRef;
use crate::traits::BackendContext;

fn extended_graph_items(request: &ImportRequest) -> usize {
    request.graph.identity_scopes.len()
        + request.graph.groups.len()
        + request.graph.principals.len()
        + request.graph.group_memberships.len()
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

const DRY_RUN_ROLLBACK: &str = "hubuum import dry-run rollback";

fn preflight_failure_kind(error: &ApiError) -> FailureKind {
    match error {
        ApiError::Forbidden(_) | ApiError::Unauthorized(_) => FailureKind::Permission,
        ApiError::Conflict(_) => FailureKind::Collision,
        ApiError::NotFound(_) | ApiError::Gone(_) => FailureKind::Resolution,
        ApiError::BadRequest(_)
        | ApiError::ValidationError(_)
        | ApiError::NotAcceptable(_)
        | ApiError::UnsupportedMediaType(_)
        | ApiError::PayloadTooLarge(_)
        | ApiError::OperatorMismatch(_)
        | ApiError::InvalidIntegerRange(_) => FailureKind::Validation,
        ApiError::InternalServerError(_)
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
    pool: &DbPool,
    mode: &ImportMode,
    planned_items: Vec<PlannedItem>,
) -> Result<PlanningOutcome, ApiError> {
    let atomicity = mode.atomicity.unwrap_or(ImportAtomicity::Strict);
    let permission_policy = mode
        .permission_policy
        .unwrap_or(ImportPermissionPolicy::Abort);
    let collision_policy = mode
        .collision_policy
        .unwrap_or(ImportCollisionPolicy::Abort);

    with_connection(
        pool,
        async move |conn| -> Result<PlanningOutcome, ApiError> {
            let mut valid_items = Vec::with_capacity(planned_items.len());
            let mut failures = Vec::new();
            let mut aborted = false;
            let mut runtime = super::types::RuntimeState::for_planned_items(&planned_items);

            let transaction = conn
                .transaction::<(), ApiError, _>(async |conn| {
                    for item in planned_items {
                        let result = match &item.execution {
                            Some(execution) => {
                                conn.transaction::<(), ApiError, _>(async |conn| {
                                    super::execution::execute_planned_item(
                                        conn,
                                        &mut runtime,
                                        execution,
                                    )
                                    .await
                                })
                                .await
                            }
                            None => Ok(()),
                        };
                        match result {
                            Ok(()) => valid_items.push(item),
                            Err(error) => {
                                let kind = preflight_failure_kind(&error);
                                failures.push(PlanningFailure {
                                    kind,
                                    item: item.result,
                                    message: sanitize_error_for_storage(&error),
                                });
                                if should_abort_import(
                                    atomicity,
                                    permission_policy,
                                    collision_policy,
                                    kind,
                                ) {
                                    aborted = true;
                                    break;
                                }
                            }
                        }
                    }

                    Err(ApiError::InternalServerError(DRY_RUN_ROLLBACK.to_string()))
                })
                .await;

            match transaction {
                Err(ApiError::InternalServerError(message)) if message == DRY_RUN_ROLLBACK => {
                    Ok(PlanningOutcome {
                        planned_items: valid_items,
                        failures,
                        aborted,
                    })
                }
                Err(error) => Err(error),
                Ok(()) => Err(ApiError::InternalServerError(
                    "Import dry run unexpectedly committed".to_string(),
                )),
            }
        },
    )
    .await
}

async fn is_import_admin<C>(
    backend: &C,
    user: &impl crate::db::traits::authz::AuthzSubject,
    state: &mut PlanningState,
) -> Result<bool, String>
where
    C: BackendContext + ?Sized,
{
    if let Some(is_admin) = state.is_admin {
        return Ok(is_admin);
    }

    let pool = backend.db_pool();
    let is_admin = match backend.permission_backend() {
        Some(permission_backend) if !permission_backend.uses_sql_permission_store() => {
            let principal = PrincipalRef::load(pool, user)
                .await
                .map_err(|err| err.to_string())?;
            permission_backend
                .is_admin(&principal)
                .await
                .map_err(|err| err.to_string())?
        }
        _ => user.is_admin(pool).await.map_err(|err| err.to_string())?,
    };
    state.is_admin = Some(is_admin);
    Ok(is_admin)
}

async fn ensure_collection_permission_cached<C>(
    backend: &C,
    user: &impl crate::db::traits::authz::AuthzSubject,
    state: &mut PlanningState,
    collection_id: i32,
    collection_exists_in_db: bool,
    permission: Permissions,
) -> Result<(), String>
where
    C: BackendContext + ?Sized,
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
        crate::can!(backend, user, scopes.as_deref(), [permission], collection);
        Ok(())
    }
    .await;
    let result = result.map_err(|err| err.to_string());
    state
        .collection_permission_cache
        .insert(key, result.clone());
    result
}

async fn class_relation_exists_cached(
    pool: &DbPool,
    state: &mut PlanningState,
    left: i32,
    right: i32,
) -> Result<bool, String> {
    let pair = normalize_pair(left, right);
    if state.class_relations.contains(&pair) {
        return Ok(true);
    }
    if let Some(exists) = state.class_relation_exists_cache.get(&pair) {
        return Ok(*exists);
    }

    let exists = lookup_direct_class_relation(pool, pair.0, pair.1)
        .await
        .map_err(|err| sanitize_error_for_storage(&err))?
        .is_some();
    state.class_relation_exists_cache.insert(pair, exists);
    Ok(exists)
}

async fn object_relation_exists_cached(
    pool: &DbPool,
    state: &mut PlanningState,
    left: i32,
    right: i32,
) -> Result<bool, String> {
    let pair = normalize_pair(left, right);
    if state.object_relations.contains(&pair) {
        return Ok(true);
    }
    if let Some(exists) = state.object_relation_exists_cache.get(&pair) {
        return Ok(*exists);
    }

    let exists = lookup_object_relation(pool, pair.0, pair.1)
        .await
        .map_err(|err| sanitize_error_for_storage(&err))?
        .is_some();
    state.object_relation_exists_cache.insert(pair, exists);
    Ok(exists)
}

#[cfg(test)]
pub(super) async fn plan_import<C>(
    backend: &C,
    user: &impl crate::db::traits::authz::AuthzSubject,
    scopes: Option<&[Permissions]>,
    request: &ImportRequest,
) -> PlanningOutcome
where
    C: BackendContext + ?Sized,
{
    plan_import_with_admin_status(backend, user, scopes, request, None).await
}

pub(super) async fn plan_runtime_admin_import<C>(
    backend: &C,
    user: &impl crate::db::traits::authz::AuthzSubject,
    request: &ImportRequest,
) -> PlanningOutcome
where
    C: BackendContext + ?Sized,
{
    plan_import_with_admin_status(backend, user, None, request, Some(true)).await
}

async fn plan_import_with_admin_status<C>(
    backend: &C,
    user: &impl crate::db::traits::authz::AuthzSubject,
    scopes: Option<&[Permissions]>,
    request: &ImportRequest,
    is_admin: Option<bool>,
) -> PlanningOutcome
where
    C: BackendContext + ?Sized,
{
    let pool = backend.db_pool();
    let mode = request.mode();
    let mut state = PlanningState::new();
    state.scopes = scopes.map(|s| s.to_vec());
    state.is_admin = is_admin;
    let mut planned_items = Vec::with_capacity(request.total_items() as usize);
    let mut failures = Vec::new();
    let mut aborted = false;
    let overwrite = matches!(
        mode.collision_policy,
        Some(ImportCollisionPolicy::Overwrite)
    );

    if extended_graph_items(request) > 0 {
        let authorized = scopes.is_none()
            && is_import_admin(backend, user, &mut state)
                .await
                .unwrap_or(false);
        if !authorized {
            failures.push(PlanningFailure {
                kind: FailureKind::Permission,
                item: planned_result("system", "import", None, None),
                message: "Identity and integration imports require an unscoped administrator token"
                    .to_string(),
            });
            return PlanningOutcome {
                planned_items,
                failures,
                aborted: true,
            };
        }

        if let Some((kind, reference)) = duplicate_extended_ref(request) {
            failures.push(PlanningFailure {
                kind: FailureKind::Validation,
                item: planned_result(kind, "create", Some(reference.clone()), None),
                message: format!("Duplicate {kind} ref '{reference}'"),
            });
            return PlanningOutcome {
                planned_items,
                failures,
                aborted: true,
            };
        }

        for input in &request.graph.identity_scopes {
            if let Some(reference) = &input.ref_ {
                state
                    .planned_identity_scope_names_by_ref
                    .insert(reference.clone(), input.name.clone());
            }
            planned_items.push(plan_system_item(
                "identity_scope",
                input.ref_.clone(),
                input.name.clone(),
                PlannedExecution::UpsertIdentityScope {
                    input: input.clone(),
                    overwrite,
                },
            ));
        }
        for input in &request.graph.groups {
            let scope = input
                .identity_scope_key
                .as_ref()
                .map(|key| key.name.clone())
                .or_else(|| {
                    input.identity_scope_ref.as_ref().and_then(|reference| {
                        state
                            .planned_identity_scope_names_by_ref
                            .get(reference)
                            .cloned()
                    })
                });
            if let Some(scope) = &scope {
                state
                    .planned_group_keys
                    .insert((scope.clone(), input.groupname.clone()));
            }
            let identifier = scope
                .as_deref()
                .map(|scope| format!("{scope}/{}", input.groupname))
                .unwrap_or_else(|| {
                    format!(
                        "{}/{}",
                        input.identity_scope_ref.as_deref().unwrap_or("unresolved"),
                        input.groupname
                    )
                });
            planned_items.push(plan_system_item(
                "group",
                input.ref_.clone(),
                identifier,
                PlannedExecution::UpsertGroup {
                    input: input.clone(),
                    overwrite,
                },
            ));
        }
        for input in request
            .graph
            .principals
            .iter()
            .filter(|input| matches!(input.subtype, ImportPrincipalSubtype::Human { .. }))
        {
            planned_items.push(plan_system_item(
                "principal",
                input.ref_.clone(),
                input.name.clone(),
                PlannedExecution::UpsertPrincipal {
                    input: input.clone(),
                    overwrite,
                },
            ));
        }
        for input in
            request.graph.principals.iter().filter(|input| {
                matches!(input.subtype, ImportPrincipalSubtype::ServiceAccount { .. })
            })
        {
            planned_items.push(plan_system_item(
                "principal",
                input.ref_.clone(),
                input.name.clone(),
                PlannedExecution::UpsertPrincipal {
                    input: input.clone(),
                    overwrite,
                },
            ));
        }
        for input in &request.graph.group_memberships {
            planned_items.push(plan_system_item(
                "group_membership",
                input.ref_.clone(),
                input
                    .ref_
                    .clone()
                    .unwrap_or_else(|| "membership".to_string()),
                PlannedExecution::UpsertGroupMembership {
                    input: input.clone(),
                    overwrite,
                },
            ));
        }
    }

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

    let collection_count = request.graph.collections.len();
    let collection_start = Instant::now();
    async {
        for collection in &request.graph.collections {
            push_or_stop!(plan_collection(
                backend, user, &mode, &mut state, collection
            ));
        }
    }
    .instrument(info_span!(
        "import_planning_phase",
        phase = "collections",
        item_count = collection_count
    ))
    .await;
    info!(
        message = "Import planning phase finished",
        phase = "collections",
        item_count = collection_count,
        planned_items = planned_items.len(),
        failures = failures.len(),
        aborted = aborted,
        elapsed = ?collection_start.elapsed()
    );
    if aborted {
        return PlanningOutcome {
            planned_items,
            failures,
            aborted,
        };
    }

    if let Err(message) = preload_existing_classes(pool, &mut state, request).await {
        failures.push(PlanningFailure {
            kind: FailureKind::Runtime,
            item: planned_result("class", "lookup", None, None),
            message,
        });
        return PlanningOutcome {
            planned_items,
            failures,
            aborted: true,
        };
    }

    let class_count = request.graph.classes.len();
    let class_start = Instant::now();
    async {
        for class in &request.graph.classes {
            push_or_stop!(plan_class(backend, user, &mode, &mut state, class));
        }
    }
    .instrument(info_span!(
        "import_planning_phase",
        phase = "classes",
        item_count = class_count
    ))
    .await;
    info!(
        message = "Import planning phase finished",
        phase = "classes",
        item_count = class_count,
        planned_items = planned_items.len(),
        failures = failures.len(),
        aborted = aborted,
        elapsed = ?class_start.elapsed()
    );
    if aborted {
        return PlanningOutcome {
            planned_items,
            failures,
            aborted,
        };
    }

    if let Err(message) = preload_existing_objects(pool, &mut state, request).await {
        failures.push(PlanningFailure {
            kind: FailureKind::Runtime,
            item: planned_result("object", "lookup", None, None),
            message,
        });
        return PlanningOutcome {
            planned_items,
            failures,
            aborted: true,
        };
    }

    let object_count = request.graph.objects.len();
    let object_start = Instant::now();
    async {
        for object in &request.graph.objects {
            push_or_stop!(plan_object(backend, user, &mode, &mut state, object));
        }
    }
    .instrument(info_span!(
        "import_planning_phase",
        phase = "objects",
        item_count = object_count
    ))
    .await;
    info!(
        message = "Import planning phase finished",
        phase = "objects",
        item_count = object_count,
        planned_items = planned_items.len(),
        failures = failures.len(),
        aborted = aborted,
        elapsed = ?object_start.elapsed()
    );
    if aborted {
        return PlanningOutcome {
            planned_items,
            failures,
            aborted,
        };
    }
    let class_relation_count = request.graph.class_relations.len();
    let class_relation_start = Instant::now();
    async {
        for relation in &request.graph.class_relations {
            push_or_stop!(plan_class_relation(
                backend, user, &mode, &mut state, relation
            ));
        }
    }
    .instrument(info_span!(
        "import_planning_phase",
        phase = "class_relations",
        item_count = class_relation_count
    ))
    .await;
    info!(
        message = "Import planning phase finished",
        phase = "class_relations",
        item_count = class_relation_count,
        planned_items = planned_items.len(),
        failures = failures.len(),
        aborted = aborted,
        elapsed = ?class_relation_start.elapsed()
    );
    if aborted {
        return PlanningOutcome {
            planned_items,
            failures,
            aborted,
        };
    }
    let object_relation_count = request.graph.object_relations.len();
    let object_relation_start = Instant::now();
    async {
        for relation in &request.graph.object_relations {
            push_or_stop!(plan_object_relation(
                backend, user, &mode, &mut state, relation
            ));
        }
    }
    .instrument(info_span!(
        "import_planning_phase",
        phase = "object_relations",
        item_count = object_relation_count
    ))
    .await;
    info!(
        message = "Import planning phase finished",
        phase = "object_relations",
        item_count = object_relation_count,
        planned_items = planned_items.len(),
        failures = failures.len(),
        aborted = aborted,
        elapsed = ?object_relation_start.elapsed()
    );
    if aborted {
        return PlanningOutcome {
            planned_items,
            failures,
            aborted,
        };
    }
    let collection_permission_count = request.graph.collection_permissions.len();
    let collection_permission_start = Instant::now();
    async {
        for acl in &request.graph.collection_permissions {
            push_or_stop!(plan_collection_permission(
                backend, user, &mode, &mut state, acl
            ));
        }
    }
    .instrument(info_span!(
        "import_planning_phase",
        phase = "collection_permissions",
        item_count = collection_permission_count
    ))
    .await;
    info!(
        message = "Import planning phase finished",
        phase = "collection_permissions",
        item_count = collection_permission_count,
        planned_items = planned_items.len(),
        failures = failures.len(),
        aborted = aborted,
        elapsed = ?collection_permission_start.elapsed()
    );

    for input in &request.graph.export_templates {
        planned_items.push(plan_system_item(
            "export_template",
            input.ref_.clone(),
            input.name.clone(),
            PlannedExecution::UpsertExportTemplate {
                input: input.clone(),
                overwrite,
            },
        ));
    }
    for input in &request.graph.remote_targets {
        planned_items.push(plan_system_item(
            "remote_target",
            input.ref_.clone(),
            input.name.clone(),
            PlannedExecution::UpsertRemoteTarget {
                input: input.clone(),
                overwrite,
            },
        ));
    }
    for input in &request.graph.event_sinks {
        planned_items.push(plan_system_item(
            "event_sink",
            input.ref_.clone(),
            input.name.clone(),
            PlannedExecution::UpsertEventSink {
                input: input.clone(),
                overwrite,
            },
        ));
    }
    for input in &request.graph.event_subscriptions {
        planned_items.push(plan_system_item(
            "event_subscription",
            input.ref_.clone(),
            input.name.clone(),
            PlannedExecution::UpsertEventSubscription {
                input: input.clone(),
                overwrite,
            },
        ));
    }

    if request.dry_run() {
        match preflight_dry_run(pool, &mode, planned_items).await {
            Ok(mut preflight) => {
                preflight.aborted |= aborted;
                preflight.failures.splice(0..0, failures);
                return preflight;
            }
            Err(error) => {
                failures.push(PlanningFailure {
                    kind: FailureKind::Runtime,
                    item: planned_result("system", "dry_run", None, None),
                    message: sanitize_error_for_storage(&error),
                });
                return PlanningOutcome {
                    planned_items: Vec::new(),
                    failures,
                    aborted: true,
                };
            }
        }
    }

    PlanningOutcome {
        planned_items,
        failures,
        aborted,
    }
}

pub(super) async fn plan_collection<C>(
    backend: &C,
    user: &impl crate::db::traits::authz::AuthzSubject,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportCollectionInput,
) -> Result<PlannedItem, PlanningFailure>
where
    C: BackendContext + ?Sized,
{
    let pool = backend.db_pool();
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
            Some(Collection {
                id: collection.id,
                name: collection.name,
                description: collection.description,
                created_at: Utc::now().naive_utc(),
                updated_at: Utc::now().naive_utc(),
                parent_collection_id: collection.parent_collection_id,
            })
        } else {
            with_connection(pool, async |conn| {
                lookup_existing_collection_for_import_db(conn, parent.id, &input.name).await
            })
            .await
            .map_err(|message| PlanningFailure {
                kind: FailureKind::Runtime,
                item: planned_result(
                    "collection",
                    "lookup",
                    input.ref_.clone(),
                    Some(input.name.clone()),
                ),
                message: sanitize_error_for_storage(&message),
            })?
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

        if matches!(mode.collision_policy, Some(ImportCollisionPolicy::Abort)) {
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
        if !is_import_admin(backend, user, state)
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
                message: "Only admins may create collections".to_string(),
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

fn validate_planned_class_schema(class: &ClassResolution) -> Result<(), ApiError> {
    if !class.validate_schema {
        return Ok(());
    }
    let Some(schema) = class.json_schema.as_ref() else {
        return Ok(());
    };
    crate::utilities::json_schema::compile_json_schema(schema).map(|_| ())
}

pub(super) async fn plan_class<C>(
    backend: &C,
    user: &impl crate::db::traits::authz::AuthzSubject,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportClassInput,
) -> Result<PlannedItem, PlanningFailure>
where
    C: BackendContext + ?Sized,
{
    let pool = backend.db_pool();
    if let Some(schema) = input.json_schema.as_ref() {
        crate::utilities::json_schema::validate_json_schema(schema).map_err(|error| {
            PlanningFailure {
                kind: FailureKind::Validation,
                item: planned_result(
                    "class",
                    "validate",
                    input.ref_.clone(),
                    Some(input.name.clone()),
                ),
                message: error.to_string(),
            }
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
        lookup_class_by_collection_and_name(pool, collection.id, &input.name)
            .await
            .map_err(|err| PlanningFailure {
                kind: FailureKind::Runtime,
                item: planned_result(
                    "class",
                    "lookup",
                    input.ref_.clone(),
                    Some(input.name.clone()),
                ),
                message: sanitize_error_for_storage(&err),
            })?
            .map(class_to_resolution)
    };

    let identifier = format!("{}::{}", collection.name, input.name);

    if let Some(class) = existing {
        ensure_collection_permission_cached(
            backend,
            user,
            state,
            collection.id,
            collection.exists_in_db,
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

        if matches!(mode.collision_policy, Some(ImportCollisionPolicy::Abort)) {
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
            json_schema: input
                .json_schema
                .clone()
                .or_else(|| class.json_schema.clone()),
            validate_schema: input.validate_schema.unwrap_or(class.validate_schema),
            exists_in_db: true,
        };
        validate_planned_class_schema(&updated).map_err(|error| PlanningFailure {
            kind: FailureKind::Validation,
            item: planned_result(
                "class",
                "validate",
                input.ref_.clone(),
                Some(input.name.clone()),
            ),
            message: error.to_string(),
        })?;
        remember_class(state, input.ref_.clone(), updated.clone());

        Ok(PlannedItem {
            result: planned_result(
                "class",
                "update",
                input.ref_.clone(),
                Some(format!("{}::{}", collection.name, input.name)),
            ),
            execution: Some(PlannedExecution::UpdateClass {
                class_id: class.id,
                input: input.clone(),
            }),
        })
    } else {
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
        validate_planned_class_schema(&created).map_err(|error| PlanningFailure {
            kind: FailureKind::Validation,
            item: planned_result(
                "class",
                "validate",
                input.ref_.clone(),
                Some(input.name.clone()),
            ),
            message: error.to_string(),
        })?;
        remember_class(state, input.ref_.clone(), created.clone());

        Ok(PlannedItem {
            result: planned_result("class", "create", input.ref_.clone(), Some(identifier)),
            execution: Some(PlannedExecution::CreateClass(input.clone())),
        })
    }
}

pub(super) async fn plan_object<C>(
    backend: &C,
    user: &impl crate::db::traits::authz::AuthzSubject,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportObjectInput,
) -> Result<PlannedItem, PlanningFailure>
where
    C: BackendContext + ?Sized,
{
    let pool = backend.db_pool();
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
        crate::utilities::json_schema::validate_json_value(schema, &input.data).map_err(|err| {
            PlanningFailure {
                kind: FailureKind::Validation,
                item: planned_result(
                    "object",
                    "validate",
                    input.ref_.clone(),
                    Some(format!("{}::{}", class.name, input.name)),
                ),
                message: err.to_string(),
            }
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
        lookup_object_by_class_and_name(pool, class.id, &input.name)
            .await
            .map_err(|err| PlanningFailure {
                kind: FailureKind::Runtime,
                item: planned_result(
                    "object",
                    "lookup",
                    input.ref_.clone(),
                    Some(input.name.clone()),
                ),
                message: sanitize_error_for_storage(&err),
            })?
            .map(object_to_resolution)
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
        ensure_collection_permission_cached(
            backend,
            user,
            state,
            collection.id,
            collection.exists_in_db,
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

pub(super) async fn plan_class_relation<C>(
    backend: &C,
    user: &impl crate::db::traits::authz::AuthzSubject,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportClassRelationInput,
) -> Result<PlannedItem, PlanningFailure>
where
    C: BackendContext + ?Sized,
{
    let pool = backend.db_pool();
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

    ensure_collection_permission_cached(
        backend,
        user,
        state,
        from_collection.id,
        from_collection.exists_in_db,
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
    ensure_collection_permission_cached(
        backend,
        user,
        state,
        to_collection.id,
        to_collection.exists_in_db,
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

    if class_relation_exists_cached(pool, state, pair.0, pair.1)
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
        })?
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

pub(super) async fn plan_object_relation<C>(
    backend: &C,
    user: &impl crate::db::traits::authz::AuthzSubject,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportObjectRelationInput,
) -> Result<PlannedItem, PlanningFailure>
where
    C: BackendContext + ?Sized,
{
    let pool = backend.db_pool();
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

    let from_collection = resolve_collection_by_id_planning(pool, state, from_object.collection_id)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result("object_relation", "create", input.ref_.clone(), None),
            message,
        })?;
    let to_collection = resolve_collection_by_id_planning(pool, state, to_object.collection_id)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result("object_relation", "create", input.ref_.clone(), None),
            message,
        })?;

    ensure_collection_permission_cached(
        backend,
        user,
        state,
        from_collection.id,
        from_collection.exists_in_db,
        Permissions::CreateObjectRelation,
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Permission,
        item: planned_result("object_relation", "create", input.ref_.clone(), None),
        message,
    })?;
    ensure_collection_permission_cached(
        backend,
        user,
        state,
        to_collection.id,
        to_collection.exists_in_db,
        Permissions::CreateObjectRelation,
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Permission,
        item: planned_result("object_relation", "create", input.ref_.clone(), None),
        message,
    })?;

    let class_pair = normalize_pair(from_object.class_id, to_object.class_id);
    let class_relation_exists =
        class_relation_exists_cached(pool, state, class_pair.0, class_pair.1)
            .await
            .map_err(|message| PlanningFailure {
                kind: FailureKind::Runtime,
                item: planned_result("object_relation", "lookup", input.ref_.clone(), None),
                message,
            })?;

    if !class_relation_exists {
        return Err(PlanningFailure {
            kind: FailureKind::Resolution,
            item: planned_result("object_relation", "create", input.ref_.clone(), None),
            message: "Object relation requires a direct class relation between the object classes"
                .to_string(),
        });
    }

    if object_relation_exists_cached(pool, state, pair.0, pair.1)
        .await
        .map_err(|message| PlanningFailure {
            kind: FailureKind::Runtime,
            item: planned_result("object_relation", "lookup", input.ref_.clone(), None),
            message,
        })?
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

pub(super) async fn plan_collection_permission<C>(
    backend: &C,
    user: &impl crate::db::traits::authz::AuthzSubject,
    _mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportCollectionPermissionInput,
) -> Result<PlannedItem, PlanningFailure>
where
    C: BackendContext + ?Sized,
{
    let pool = backend.db_pool();
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

    let identity_scope = input.group_key.identity_scope_name();
    let group = lookup_group_by_name(pool, identity_scope, &input.group_key.groupname)
        .await
        .map_err(|err| PlanningFailure {
            kind: FailureKind::Runtime,
            item: planned_result(
                "collection_permission",
                "lookup",
                input.ref_.clone(),
                Some(input.group_key.groupname.clone()),
            ),
            message: sanitize_error_for_storage(&err),
        })?
        .or_else(|| {
            state
                .planned_group_keys
                .contains(&(
                    identity_scope.to_string(),
                    input.group_key.groupname.clone(),
                ))
                .then(|| crate::models::Group {
                    id: state.next_temp_id,
                    groupname: input.group_key.groupname.clone(),
                    description: String::new(),
                    created_at: Utc::now().naive_utc(),
                    updated_at: Utc::now().naive_utc(),
                    identity_scope_id: state.next_temp_id,
                    managed_by: "local".to_string(),
                    external_key: None,
                    last_sync_attempted_at: None,
                    last_sync_success_at: None,
                })
        })
        .ok_or_else(|| PlanningFailure {
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
        })?;

    Ok(PlannedItem {
        result: planned_result(
            "collection_permission",
            if input.replace_existing.unwrap_or(false) {
                "replace"
            } else {
                "grant"
            },
            input.ref_.clone(),
            Some(format!("{}::{}", collection.name, group.groupname)),
        ),
        execution: Some(PlannedExecution::ApplyCollectionPermissions(input.clone())),
    })
}
