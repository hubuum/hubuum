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
use crate::db::traits::UserPermissions;
use crate::db::traits::task_import::{
    lookup_class_by_collection_and_name, lookup_direct_class_relation, lookup_group_by_name,
    lookup_object_by_class_and_name, lookup_object_relation,
};
use crate::db::{DbPool, with_connection};
use crate::models::{
    Collection, CollectionID, ImportAtomicity, ImportClassInput, ImportClassRelationInput,
    ImportCollectionInput, ImportCollectionPermissionInput, ImportCollisionPolicy, ImportMode,
    ImportObjectInput, ImportObjectRelationInput, ImportPermissionPolicy, ImportRequest,
    Permissions,
};

async fn is_import_admin(
    pool: &DbPool,
    user: &impl crate::db::traits::authz::AuthzSubject,
    state: &mut PlanningState,
) -> Result<bool, String> {
    if let Some(is_admin) = state.is_admin {
        return Ok(is_admin);
    }

    let is_admin = user.is_admin(pool).await.map_err(|err| err.to_string())?;
    state.is_admin = Some(is_admin);
    Ok(is_admin)
}

async fn ensure_collection_permission_cached(
    pool: &DbPool,
    user: &impl crate::db::traits::authz::AuthzSubject,
    state: &mut PlanningState,
    collection_id: i32,
    collection_exists_in_db: bool,
    permission: Permissions,
) -> Result<(), String> {
    if !collection_exists_in_db {
        // Admin-bypass for newly-created collections applies only to unscoped
        // tokens; a scoped import can never operate on new collections.
        if state.scopes.is_none() && is_import_admin(pool, user, state).await? {
            return Ok(());
        }
        return Err("Only admins may operate on newly created collections within an import".into());
    }

    let key = (collection_id, permission);
    if let Some(result) = state.collection_permission_cache.get(&key) {
        return result.clone();
    }

    let collection = CollectionID::new(collection_id).map_err(|err| err.to_string())?;
    let scopes = state.scopes.clone();
    let result = user
        .can(pool, vec![permission], vec![collection], scopes.as_deref())
        .await
        .map_err(|err| err.to_string());
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

pub(super) async fn plan_import(
    pool: &DbPool,
    user: &impl crate::db::traits::authz::AuthzSubject,
    scopes: Option<&[Permissions]>,
    request: &ImportRequest,
) -> PlanningOutcome {
    let mode = request.mode();
    let mut state = PlanningState::new();
    state.scopes = scopes.map(|s| s.to_vec());
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

    let collection_count = request.graph.collections.len();
    let collection_start = Instant::now();
    async {
        for collection in &request.graph.collections {
            push_or_stop!(plan_collection(pool, user, &mode, &mut state, collection));
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
    let collection_permission_count = request.graph.collection_permissions.len();
    let collection_permission_start = Instant::now();
    async {
        for acl in &request.graph.collection_permissions {
            push_or_stop!(plan_collection_permission(
                pool, user, &mode, &mut state, acl
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

    PlanningOutcome {
        planned_items,
        failures,
        aborted,
    }
}

pub(super) async fn plan_collection(
    pool: &DbPool,
    user: &impl crate::db::traits::authz::AuthzSubject,
    mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportCollectionInput,
) -> Result<PlannedItem, PlanningFailure> {
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
            pool,
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
        if !is_import_admin(pool, user, state)
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

pub(super) async fn plan_class(
    pool: &DbPool,
    user: &impl crate::db::traits::authz::AuthzSubject,
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
            pool,
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
            pool,
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
        remember_class(state, input.ref_.clone(), created.clone());

        Ok(PlannedItem {
            result: planned_result("class", "create", input.ref_.clone(), Some(identifier)),
            execution: Some(PlannedExecution::CreateClass(input.clone())),
        })
    }
}

pub(super) async fn plan_object(
    pool: &DbPool,
    user: &impl crate::db::traits::authz::AuthzSubject,
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
            pool,
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
            pool,
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

pub(super) async fn plan_class_relation(
    pool: &DbPool,
    user: &impl crate::db::traits::authz::AuthzSubject,
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
        pool,
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
        pool,
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

pub(super) async fn plan_object_relation(
    pool: &DbPool,
    user: &impl crate::db::traits::authz::AuthzSubject,
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
        pool,
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
        pool,
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

pub(super) async fn plan_collection_permission(
    pool: &DbPool,
    user: &impl crate::db::traits::authz::AuthzSubject,
    _mode: &ImportMode,
    state: &mut PlanningState,
    input: &ImportCollectionPermissionInput,
) -> Result<PlannedItem, PlanningFailure> {
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
        pool,
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
