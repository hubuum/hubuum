use std::collections::{HashMap, HashSet};
use std::time::Instant;

use chrono::Utc;
use tracing::{Instrument, info, info_span};

use super::helpers::{
    class_to_resolution, identifier_namespace, namespace_to_resolution, normalize_pair,
    object_to_resolution, planned_result, sanitize_error_for_storage, should_abort_import,
};
use super::resolution::{
    remember_class, remember_namespace, remember_object, resolve_class_planning,
    resolve_namespace_by_id_planning, resolve_namespace_planning, resolve_object_planning,
};
use super::types::{
    ClassResolution, FailureKind, NamespaceResolution, ObjectResolution, PlannedExecution,
    PlannedItem, PlanningFailure, PlanningOutcome, PlanningState,
};
use crate::db::DbPool;
use crate::db::traits::UserPermissions;
use crate::db::traits::task_import::{
    lookup_class_by_namespace_and_name, lookup_classes_by_namespace_and_names,
    lookup_direct_class_relation, lookup_group_by_name, lookup_namespace_by_name,
    lookup_namespaces_by_names, lookup_object_by_class_and_name, lookup_object_relation,
    lookup_objects_by_class_and_names,
};
use crate::models::{
    ClassKey, ImportAtomicity, ImportClassInput, ImportClassRelationInput, ImportCollisionPolicy,
    ImportMode, ImportNamespaceInput, ImportNamespacePermissionInput, ImportObjectInput,
    ImportObjectRelationInput, ImportPermissionPolicy, ImportRequest, Namespace, NamespaceID,
    NamespaceKey, ObjectKey, Permissions, User,
};
use crate::traits::GroupMemberships;

async fn is_import_admin(
    pool: &DbPool,
    user: &User,
    state: &mut PlanningState,
) -> Result<bool, String> {
    if let Some(is_admin) = state.is_admin {
        return Ok(is_admin);
    }

    let is_admin = user.is_admin(pool).await.map_err(|err| err.to_string())?;
    state.is_admin = Some(is_admin);
    Ok(is_admin)
}

async fn ensure_namespace_permission_cached(
    pool: &DbPool,
    user: &User,
    state: &mut PlanningState,
    namespace_id: i32,
    namespace_exists_in_db: bool,
    permission: Permissions,
) -> Result<(), String> {
    if !namespace_exists_in_db {
        if is_import_admin(pool, user, state).await? {
            return Ok(());
        }
        return Err("Only admins may operate on newly created namespaces within an import".into());
    }

    let key = (namespace_id, permission);
    if let Some(result) = state.namespace_permission_cache.get(&key) {
        return result.clone();
    }

    let result = user
        .can(pool, vec![permission], vec![NamespaceID(namespace_id)])
        .await
        .map_err(|err| err.to_string());
    state.namespace_permission_cache.insert(key, result.clone());
    result
}

fn collect_request_class_keys(request: &ImportRequest) -> Vec<ClassKey> {
    let mut keys = Vec::new();
    for class in &request.graph.classes {
        if class.namespace_ref.is_some() || class.namespace_key.is_some() {
            keys.push(ClassKey {
                name: class.name.clone(),
                namespace_ref: class.namespace_ref.clone(),
                namespace_key: class.namespace_key.clone(),
            });
        }
    }
    for object in &request.graph.objects {
        if let Some(key) = &object.class_key {
            keys.push(key.clone());
        }
    }
    for relation in &request.graph.class_relations {
        if let Some(key) = &relation.from_class_key {
            keys.push(key.clone());
        }
        if let Some(key) = &relation.to_class_key {
            keys.push(key.clone());
        }
    }
    for relation in &request.graph.object_relations {
        for object_key in [&relation.from_object_key, &relation.to_object_key]
            .into_iter()
            .flatten()
        {
            if let Some(class_key) = &object_key.class_key {
                keys.push(class_key.clone());
            }
        }
    }
    keys
}

fn collect_request_object_keys(request: &ImportRequest) -> Vec<ObjectKey> {
    let mut keys = Vec::new();
    for object in &request.graph.objects {
        if object.class_ref.is_some() || object.class_key.is_some() {
            keys.push(ObjectKey {
                name: object.name.clone(),
                class_ref: object.class_ref.clone(),
                class_key: object.class_key.clone(),
            });
        }
    }
    for relation in &request.graph.object_relations {
        if let Some(key) = &relation.from_object_key {
            keys.push(key.clone());
        }
        if let Some(key) = &relation.to_object_key {
            keys.push(key.clone());
        }
    }
    keys
}

async fn preload_namespaces_for_class_keys(
    pool: &DbPool,
    state: &mut PlanningState,
    class_keys: &[ClassKey],
) -> Result<(), String> {
    let names = class_keys
        .iter()
        .filter_map(|key| {
            key.namespace_key
                .as_ref()
                .map(|namespace| namespace.name.clone())
        })
        .filter(|name| {
            !state.namespaces_by_name.contains_key(name)
                && !state.missing_namespace_names.contains(name)
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let namespaces = lookup_namespaces_by_names(pool, &names)
        .await
        .map_err(|err| err.to_string())?;
    let found_names = namespaces
        .iter()
        .map(|namespace| namespace.name.clone())
        .collect::<HashSet<_>>();
    for namespace in namespaces {
        remember_namespace(state, None, namespace_to_resolution(namespace));
    }
    for name in names {
        if !found_names.contains(&name) {
            state.missing_namespace_names.insert(name);
        }
    }

    Ok(())
}

async fn preload_existing_classes(
    pool: &DbPool,
    state: &mut PlanningState,
    request: &ImportRequest,
) -> Result<(), String> {
    let class_keys = collect_request_class_keys(request);
    preload_namespaces_for_class_keys(pool, state, &class_keys).await?;

    let mut requested = HashMap::<i32, HashSet<String>>::new();
    for key in class_keys {
        let Some(namespace) = resolve_class_namespace_for_preload(pool, state, &key).await? else {
            continue;
        };
        if !namespace.exists_in_db {
            state
                .missing_class_keys
                .insert((namespace.id, key.name.clone()));
            continue;
        }
        if state
            .classes_by_key
            .contains_key(&(namespace.id, key.name.clone()))
            || state
                .missing_class_keys
                .contains(&(namespace.id, key.name.clone()))
        {
            continue;
        }
        requested
            .entry(namespace.id)
            .or_default()
            .insert(key.name.clone());
    }

    for (namespace_id, names) in requested {
        let names = names.into_iter().collect::<Vec<_>>();
        let classes = lookup_classes_by_namespace_and_names(pool, namespace_id, &names)
            .await
            .map_err(|err| err.to_string())?;
        let found_names = classes
            .iter()
            .map(|class| class.name.clone())
            .collect::<HashSet<_>>();
        for class in classes {
            remember_class(state, None, class_to_resolution(class));
        }
        for name in names {
            if !found_names.contains(&name) {
                state.missing_class_keys.insert((namespace_id, name));
            }
        }
    }

    Ok(())
}

async fn resolve_class_namespace_for_preload(
    pool: &DbPool,
    state: &mut PlanningState,
    key: &ClassKey,
) -> Result<Option<NamespaceResolution>, String> {
    match (key.namespace_ref.as_deref(), key.namespace_key.as_ref()) {
        (Some(reference), None) => Ok(state.namespaces_by_ref.get(reference).cloned()),
        (None, Some(NamespaceKey { name })) => {
            if let Some(namespace) = state.namespaces_by_name.get(name) {
                return Ok(Some(namespace.clone()));
            }
            if state.missing_namespace_names.contains(name) {
                return Ok(None);
            }
            let namespace = lookup_namespace_by_name(pool, name)
                .await
                .map_err(|err| err.to_string())?
                .map(namespace_to_resolution);
            if let Some(namespace) = &namespace {
                remember_namespace(state, None, namespace.clone());
            } else {
                state.missing_namespace_names.insert(name.clone());
            }
            Ok(namespace)
        }
        _ => Ok(None),
    }
}

async fn preload_existing_objects(
    pool: &DbPool,
    state: &mut PlanningState,
    request: &ImportRequest,
) -> Result<(), String> {
    let object_keys = collect_request_object_keys(request);
    let mut requested = HashMap::<i32, HashSet<String>>::new();

    for key in object_keys {
        let class = match resolve_class_planning(
            pool,
            state,
            key.class_ref.as_deref(),
            key.class_key.as_ref(),
        )
        .await
        {
            Ok(class) => class,
            Err(_) => continue,
        };

        if !class.exists_in_db {
            state
                .missing_object_keys
                .insert((class.id, key.name.clone()));
            continue;
        }
        if state
            .objects_by_key
            .contains_key(&(class.id, key.name.clone()))
            || state
                .missing_object_keys
                .contains(&(class.id, key.name.clone()))
        {
            continue;
        }
        requested
            .entry(class.id)
            .or_default()
            .insert(key.name.clone());
    }

    for (class_id, names) in requested {
        let names = names.into_iter().collect::<Vec<_>>();
        let objects = lookup_objects_by_class_and_names(pool, class_id, &names)
            .await
            .map_err(|err| err.to_string())?;
        let found_names = objects
            .iter()
            .map(|object| object.name.clone())
            .collect::<HashSet<_>>();
        for object in objects {
            remember_object(state, None, object_to_resolution(object));
        }
        for name in names {
            if !found_names.contains(&name) {
                state.missing_object_keys.insert((class_id, name));
            }
        }
    }

    Ok(())
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

pub(super) async fn plan_import(
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

    let namespace_count = request.graph.namespaces.len();
    let namespace_start = Instant::now();
    async {
        for namespace in &request.graph.namespaces {
            push_or_stop!(plan_namespace(pool, user, &mode, &mut state, namespace));
        }
    }
    .instrument(info_span!(
        "import_planning_phase",
        phase = "namespaces",
        item_count = namespace_count
    ))
    .await;
    info!(
        message = "Import planning phase finished",
        phase = "namespaces",
        item_count = namespace_count,
        planned_items = planned_items.len(),
        failures = failures.len(),
        aborted = aborted,
        elapsed = ?namespace_start.elapsed()
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
            push_or_stop!(plan_class(pool, user, &mode, &mut state, class));
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
            push_or_stop!(plan_object(pool, user, &mode, &mut state, object));
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
            push_or_stop!(plan_class_relation(pool, user, &mode, &mut state, relation));
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
                pool, user, &mode, &mut state, relation
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
    let namespace_permission_count = request.graph.namespace_permissions.len();
    let namespace_permission_start = Instant::now();
    async {
        for acl in &request.graph.namespace_permissions {
            push_or_stop!(plan_namespace_permission(
                pool, user, &mode, &mut state, acl
            ));
        }
    }
    .instrument(info_span!(
        "import_planning_phase",
        phase = "namespace_permissions",
        item_count = namespace_permission_count
    ))
    .await;
    info!(
        message = "Import planning phase finished",
        phase = "namespace_permissions",
        item_count = namespace_permission_count,
        planned_items = planned_items.len(),
        failures = failures.len(),
        aborted = aborted,
        elapsed = ?namespace_permission_start.elapsed()
    );

    PlanningOutcome {
        planned_items,
        failures,
        aborted,
    }
}

pub(super) async fn plan_namespace(
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

    if !state.planned_namespace_names.insert(input.name.clone()) {
        return Err(PlanningFailure {
            kind: FailureKind::Validation,
            item: planned_result(
                "namespace",
                "create",
                input.ref_.clone(),
                Some(input.name.clone()),
            ),
            message: format!(
                "Duplicate namespace name '{}' within import request",
                input.name
            ),
        });
    }

    let existing = state
        .namespaces_by_name
        .get(&input.name)
        .cloned()
        .filter(|namespace| namespace.exists_in_db)
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
                message: sanitize_error_for_storage(&message),
            })?);

    if let Some(namespace) = existing {
        ensure_namespace_permission_cached(
            pool,
            user,
            state,
            namespace.id,
            true,
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
        if !is_import_admin(pool, user, state)
            .await
            .map_err(|err| PlanningFailure {
                kind: FailureKind::Permission,
                item: planned_result(
                    "namespace",
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

pub(super) async fn plan_class(
    pool: &DbPool,
    user: &User,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportClassInput,
) -> Result<PlannedItem, PlanningFailure> {
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

    let class_key = (namespace.id, input.name.clone());
    if !state.planned_class_keys.insert(class_key.clone()) {
        return Err(PlanningFailure {
            kind: FailureKind::Validation,
            item: planned_result(
                "class",
                "create",
                input.ref_.clone(),
                Some(format!("{}::{}", namespace.name, input.name)),
            ),
            message: format!(
                "Duplicate class name '{}' within namespace '{}'",
                input.name, namespace.name
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
                message: sanitize_error_for_storage(&err),
            })?
            .map(class_to_resolution)
    };

    let identifier = format!("{}::{}", namespace.name, input.name);

    if let Some(class) = existing {
        ensure_namespace_permission_cached(
            pool,
            user,
            state,
            namespace.id,
            namespace.exists_in_db,
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
                    "Class '{}' already exists in namespace '{}'",
                    input.name, namespace.name
                ),
            });
        }

        let updated = ClassResolution {
            id: class.id,
            name: input.name.clone(),
            namespace_id: namespace.id,
            json_schema: input
                .json_schema
                .clone()
                .or_else(|| class.json_schema.clone()),
            validate_schema: input.validate_schema.unwrap_or(class.validate_schema),
            exists_in_db: true,
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
        ensure_namespace_permission_cached(
            pool,
            user,
            state,
            namespace.id,
            namespace.exists_in_db,
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
            namespace_id: namespace.id,
            json_schema: input.json_schema.clone(),
            validate_schema: input.validate_schema.unwrap_or(false),
            exists_in_db: false,
        };
        remember_class(state, input.ref_.clone(), created.clone());

        Ok(PlannedItem {
            result: planned_result("class", "create", input.ref_.clone(), Some(identifier)),
            execution: Some(PlannedExecution::CreateClass(input.clone())),
        })
    }
}

pub(super) async fn plan_object(
    pool: &DbPool,
    user: &User,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportObjectInput,
) -> Result<PlannedItem, PlanningFailure> {
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
        ensure_namespace_permission_cached(
            pool,
            user,
            state,
            namespace.id,
            namespace.exists_in_db,
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
            namespace_id: namespace.id,
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
        ensure_namespace_permission_cached(
            pool,
            user,
            state,
            namespace.id,
            namespace.exists_in_db,
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
            namespace_id: namespace.id,
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

pub(super) async fn plan_class_relation(
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

    ensure_namespace_permission_cached(
        pool,
        user,
        state,
        from_namespace.id,
        from_namespace.exists_in_db,
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
    ensure_namespace_permission_cached(
        pool,
        user,
        state,
        to_namespace.id,
        to_namespace.exists_in_db,
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

pub(super) async fn plan_object_relation(
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

    ensure_namespace_permission_cached(
        pool,
        user,
        state,
        from_namespace.id,
        from_namespace.exists_in_db,
        Permissions::CreateObjectRelation,
    )
    .await
    .map_err(|message| PlanningFailure {
        kind: FailureKind::Permission,
        item: planned_result("object_relation", "create", input.ref_.clone(), None),
        message,
    })?;
    ensure_namespace_permission_cached(
        pool,
        user,
        state,
        to_namespace.id,
        to_namespace.exists_in_db,
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

pub(super) async fn plan_namespace_permission(
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

    ensure_namespace_permission_cached(
        pool,
        user,
        state,
        namespace.id,
        namespace.exists_in_db,
        Permissions::DelegateCollection,
    )
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
            message: sanitize_error_for_storage(&err),
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
