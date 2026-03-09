use chrono::Utc;

use super::helpers::{
    class_to_resolution, identifier_namespace, normalize_pair, object_to_resolution,
    planned_result, sanitize_error_for_storage, should_abort_import,
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
use crate::db::traits::task_import::{
    ensure_namespace_permission, lookup_class_by_namespace_and_name, lookup_direct_class_relation,
    lookup_group_by_name, lookup_namespace_by_name, lookup_object_by_class_and_name,
    lookup_object_relation,
};
use crate::models::{
    ImportAtomicity, ImportClassInput, ImportClassRelationInput, ImportCollisionPolicy, ImportMode,
    ImportNamespaceInput, ImportNamespacePermissionInput, ImportObjectInput,
    ImportObjectRelationInput, ImportPermissionPolicy, ImportRequest, Namespace, Permissions, User,
};
use crate::traits::GroupMemberships;

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
        ensure_namespace_permission(
            pool,
            user,
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

pub(super) async fn plan_class(
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

    let existing = state
        .classes_by_key
        .get(&class_key)
        .cloned()
        .filter(|class| class.exists_in_db)
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
                    message: sanitize_error_for_storage(&err),
                })?
                .map(class_to_resolution),
        );

    let identifier = format!("{}::{}", namespace.name, input.name);

    if let Some(class) = existing {
        ensure_namespace_permission(
            pool,
            user,
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
        ensure_namespace_permission(
            pool,
            user,
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

    let existing = state
        .objects_by_key
        .get(&object_key)
        .cloned()
        .filter(|object| object.exists_in_db)
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
                message: sanitize_error_for_storage(&err),
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
        ensure_namespace_permission(
            pool,
            user,
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
        ensure_namespace_permission(
            pool,
            user,
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

    ensure_namespace_permission(
        pool,
        user,
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
    ensure_namespace_permission(
        pool,
        user,
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
                message: sanitize_error_for_storage(&err),
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

    ensure_namespace_permission(
        pool,
        user,
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
    ensure_namespace_permission(
        pool,
        user,
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
    let class_relation_exists = state.class_relations.contains(&class_pair)
        || lookup_direct_class_relation(pool, class_pair.0, class_pair.1)
            .await
            .map_err(|err| PlanningFailure {
                kind: FailureKind::Runtime,
                item: planned_result("object_relation", "lookup", input.ref_.clone(), None),
                message: sanitize_error_for_storage(&err),
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
                message: sanitize_error_for_storage(&err),
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

    ensure_namespace_permission(
        pool,
        user,
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
