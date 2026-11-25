use super::*;
use crate::tasks::execution::execute_import_task;

#[test]
fn test_remember_collection_populates_collection_id_index() {
    let mut state = PlanningState::default();
    let collection = CollectionResolution {
        id: -42,
        name: "planned".to_string(),
        description: "planned collection".to_string(),
        parent_collection_id: None,
        exists_in_db: false,
    };

    state.remember_collection(Some("collection:planned".to_string()), collection.clone());

    assert_eq!(
        state.collection_by_id(collection.id).unwrap().name,
        collection.name
    );
}

#[rstest]
#[case::unqueried(false)]
#[case::preloaded_miss(true)]
fn remembering_a_planned_object_preserves_its_database_lookup(#[case] database_miss: bool) {
    let mut state = PlanningState::default();
    let key = (7, String::from("planned"));
    if database_miss {
        state.objects.record_missing(key.clone());
    }
    state.remember_object(
        None,
        ObjectResolution {
            id: -1,
            class_id: key.0,
            collection_id: 3,
            name: key.1.clone(),
            exists_in_db: false,
        },
    );

    assert!(matches!(state.objects.lookup(&key), CacheLookup::Found(object) if object.id == -1));
    let persisted = state
        .objects
        .lookup_where(&key, |object| object.exists_in_db);
    assert!(matches!(
        (database_miss, persisted),
        (true, CacheLookup::Missing) | (false, CacheLookup::Unknown)
    ));
}

#[tokio::test]
async fn test_plan_collection_rejects_duplicate_name_within_request() {
    let context = (TestContext::new()).await;
    let mut state = PlanningState::default();
    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportCollectionInput {
        ref_: Some("collection:one".to_string()),
        name: context.scoped_name("duplicate_collection"),
        description: "first".to_string(),
        condition: None,
        timestamps: None,
        parent_collection_ref: None,
        parent_collection_key: None,
    };

    (plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportCollectionInput {
        ref_: Some("collection:two".to_string()),
        ..input
    };
    let err = (plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate collection name"));
}

#[tokio::test]
async fn test_plan_collection_allows_duplicate_names_under_different_parents() {
    let context = (TestContext::new()).await;
    let parent_one = (context.collection_fixture("duplicate_import_parent_one")).await;
    let parent_two = (context.collection_fixture("duplicate_import_parent_two")).await;
    let mut state = PlanningState::default();
    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let child_name = context.scoped_name("duplicate_import_child");
    let input_one = ImportCollectionInput {
        ref_: Some("collection:one".to_string()),
        name: child_name.clone(),
        description: "first".to_string(),
        condition: None,
        timestamps: None,
        parent_collection_ref: None,
        parent_collection_key: Some(CollectionKey {
            name: parent_one.collection.name.clone(),
            path: None,
        }),
    };
    let input_two = ImportCollectionInput {
        ref_: Some("collection:two".to_string()),
        name: child_name,
        description: "second".to_string(),
        condition: None,
        timestamps: None,
        parent_collection_ref: None,
        parent_collection_key: Some(CollectionKey {
            name: parent_two.collection.name.clone(),
            path: None,
        }),
    };

    (plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input_one,
    ))
    .await
    .unwrap();
    (plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input_two,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn test_plan_class_rejects_duplicate_name_against_virtual_planned_class() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("duplicate_virtual_class")).await;
    let mut state = PlanningState::default();
    state.remember_collection(
        Some("collection:existing".to_string()),
        CollectionResolution {
            id: fixture.collection.id,
            name: fixture.collection.name.clone(),
            description: fixture.collection.description.clone(),
            parent_collection_id: fixture.collection.parent_collection_id,
            exists_in_db: true,
        },
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportClassInput {
        schema_activation: None,
        ref_: Some("class:one".to_string()),
        name: context.scoped_name("duplicate_class"),
        description: "first".to_string(),
        condition: None,
        timestamps: None,
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:existing".to_string()),
        collection_key: None,
    };

    (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportClassInput {
        schema_activation: None,
        ref_: Some("class:two".to_string()),
        ..input
    };
    let err = (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate class name"));
}

#[tokio::test]
async fn best_effort_planning_rejects_invalid_class_schema_policy_per_item() {
    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("invalid_class_schema_policy")
        .await;
    let collection_key = CollectionKey {
        name: fixture.collection.name.clone(),
        path: None,
    };
    let class_input = |reference: &str, name: &str, validate_schema| ImportClassInput {
        schema_activation: None,
        ref_: Some(reference.to_string()),
        name: context.scoped_name(name),
        description: name.to_string(),
        condition: None,
        timestamps: None,
        json_schema: None,
        validate_schema: Some(validate_schema),
        collection_ref: None,
        collection_key: Some(collection_key.clone()),
    };
    let request = ImportRequest {
        version: CURRENT_IMPORT_VERSION,
        dry_run: Some(false),
        mode: Some(ImportMode {
            atomicity: Some(ImportAtomicity::BestEffort),
            collision_policy: Some(ImportCollisionPolicy::Overwrite),
            permission_policy: Some(ImportPermissionPolicy::Continue),
        }),
        graph: ImportGraph {
            classes: vec![
                class_input("class:invalid", "invalid_policy", true),
                class_input("class:valid", "valid_policy", false),
            ],
            ..ImportGraph::default()
        },
    };

    let planning = plan_import(&context.pool, &context.admin_user, None, &request).await;

    assert!(!planning.aborted);
    assert!(matches!(
        planning.failures.as_slice(),
        [PlanningFailure {
            kind: FailureKind::Validation,
            message,
            ..
        }] if message.contains("without a schema")
    ));
    assert!(matches!(
        planning.planned_items.as_slice(),
        [PlannedItem {
            execution: Some(PlannedExecution::CreateClass(_)),
            ..
        }]
    ));
}

#[tokio::test]
async fn test_plan_object_rejects_duplicate_name_against_virtual_planned_object() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("duplicate_virtual_object")).await;
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("duplicate_virtual_object_class"),
        description: "existing class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let mut state = PlanningState::default();
    state.remember_class(
        Some("class:existing".to_string()),
        class_to_resolution(class.clone()),
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportObjectInput {
        ref_: Some("object:one".to_string()),
        name: context.scoped_name("duplicate_object"),
        description: "first".to_string(),
        condition: None,
        timestamps: None,
        data: serde_json::json!({"hostname":"first"}),
        class_ref: Some("class:existing".to_string()),
        class_key: None,
    };

    (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportObjectInput {
        ref_: Some("object:two".to_string()),
        ..input
    };
    let err = (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate object name"));
}

#[tokio::test]
async fn test_plan_class_rejects_duplicate_ref_against_virtual_planned_class() {
    let context = (TestContext::new()).await;
    let fixture_one = (context.collection_fixture("duplicate_class_ref_one")).await;
    let fixture_two = (context.collection_fixture("duplicate_class_ref_two")).await;
    let mut state = PlanningState::default();
    state.remember_collection(
        Some("collection:one".to_string()),
        CollectionResolution {
            id: fixture_one.collection.id,
            name: fixture_one.collection.name.clone(),
            description: fixture_one.collection.description.clone(),
            parent_collection_id: fixture_one.collection.parent_collection_id,
            exists_in_db: true,
        },
    );
    state.remember_collection(
        Some("collection:two".to_string()),
        CollectionResolution {
            id: fixture_two.collection.id,
            name: fixture_two.collection.name.clone(),
            description: fixture_two.collection.description.clone(),
            parent_collection_id: fixture_two.collection.parent_collection_id,
            exists_in_db: true,
        },
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportClassInput {
        schema_activation: None,
        ref_: Some("class:shared".to_string()),
        name: context.scoped_name("duplicate_class_ref_one"),
        description: "first".to_string(),
        condition: None,
        timestamps: None,
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:one".to_string()),
        collection_key: None,
    };

    (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportClassInput {
        schema_activation: None,
        name: context.scoped_name("duplicate_class_ref_two"),
        collection_ref: Some("collection:two".to_string()),
        ..input
    };
    let err = (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate class ref"));
}

#[tokio::test]
async fn test_plan_object_rejects_duplicate_ref_against_virtual_planned_object() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("duplicate_object_ref")).await;
    let class_one = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("duplicate_object_ref_class_one"),
        description: "first class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let class_two = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("duplicate_object_ref_class_two"),
        description: "second class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let mut state = PlanningState::default();
    state.remember_class(
        Some("class:one".to_string()),
        class_to_resolution(class_one.clone()),
    );
    state.remember_class(
        Some("class:two".to_string()),
        class_to_resolution(class_two.clone()),
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportObjectInput {
        ref_: Some("object:shared".to_string()),
        name: context.scoped_name("duplicate_object_ref_one"),
        description: "first".to_string(),
        condition: None,
        timestamps: None,
        data: serde_json::json!({"hostname":"first"}),
        class_ref: Some("class:one".to_string()),
        class_key: None,
    };

    (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportObjectInput {
        name: context.scoped_name("duplicate_object_ref_two"),
        class_ref: Some("class:two".to_string()),
        ..input
    };
    let err = (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate object ref"));
}

#[tokio::test]
async fn test_resolve_collection_planning_backfills_caches_after_db_lookup() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("planning_collection_cache")).await;
    let mut state = PlanningState::default();

    let resolved = (resolve_collection_planning(
        &context.pool,
        &mut state,
        None,
        Some(&CollectionKey {
            name: fixture.collection.name.clone(),
            path: None,
        }),
    ))
    .await
    .unwrap();

    assert_eq!(resolved.id, fixture.collection.id);
    assert!(
        matches!(state.collections_named(&fixture.collection.name), CacheLookup::Found([cached]) if cached.id == fixture.collection.id)
    );
    assert_eq!(
        state.collection_by_id(fixture.collection.id).unwrap().name,
        fixture.collection.name
    );
}

#[tokio::test]
async fn test_resolve_collection_planning_rejects_ambiguous_bare_name() {
    let context = (TestContext::new()).await;
    let parent_one = (context.collection_fixture("ambiguous_parent_one")).await;
    let parent_two = (context.collection_fixture("ambiguous_parent_two")).await;
    let child_name = context.scoped_name("ambiguous_child");

    ((NewCollectionWithAssignee {
        name: child_name.clone(),
        description: "first ambiguous child".to_string(),
        group_id: GroupID::new(parent_one.owner_group.id).unwrap(),
        parent_collection_id: Some(CollectionID::new(parent_one.collection.id).unwrap()),
    })
    .save_without_events(&context.pool))
    .await
    .unwrap();
    ((NewCollectionWithAssignee {
        name: child_name.clone(),
        description: "second ambiguous child".to_string(),
        group_id: GroupID::new(parent_two.owner_group.id).unwrap(),
        parent_collection_id: Some(CollectionID::new(parent_two.collection.id).unwrap()),
    })
    .save_without_events(&context.pool))
    .await
    .unwrap();

    let mut state = PlanningState::default();
    let err = (resolve_collection_planning(
        &context.pool,
        &mut state,
        None,
        Some(&CollectionKey {
            name: child_name.clone(),
            path: None,
        }),
    ))
    .await
    .unwrap_err();

    assert!(err.contains("ambiguous"));
    assert!(err.contains("collection_key.path"));
}

#[tokio::test]
async fn test_resolve_collection_planning_uses_path_to_disambiguate_name() {
    let context = (TestContext::new()).await;
    let parent_one = (context.collection_fixture("path_parent_one")).await;
    let parent_two = (context.collection_fixture("path_parent_two")).await;
    let child_name = context.scoped_name("path_child");

    ((NewCollectionWithAssignee {
        name: child_name.clone(),
        description: "first path child".to_string(),
        group_id: GroupID::new(parent_one.owner_group.id).unwrap(),
        parent_collection_id: Some(CollectionID::new(parent_one.collection.id).unwrap()),
    })
    .save_without_events(&context.pool))
    .await
    .unwrap();
    let target_child = ((NewCollectionWithAssignee {
        name: child_name.clone(),
        description: "second path child".to_string(),
        group_id: GroupID::new(parent_two.owner_group.id).unwrap(),
        parent_collection_id: Some(CollectionID::new(parent_two.collection.id).unwrap()),
    })
    .save_without_events(&context.pool))
    .await
    .unwrap();

    let mut state = PlanningState::default();
    let resolved = (resolve_collection_planning(
        &context.pool,
        &mut state,
        None,
        Some(&CollectionKey {
            name: child_name.clone(),
            path: Some(vec![parent_two.collection.name.clone(), child_name]),
        }),
    ))
    .await
    .unwrap();

    assert_eq!(resolved.id, target_child.id);
}

#[tokio::test]
async fn test_resolve_collection_by_id_planning_backfills_caches_after_db_lookup() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("planning_collection_id_cache")).await;
    let mut state = PlanningState::default();

    let resolved =
        (resolve_collection_by_id_planning(&context.pool, &mut state, fixture.collection.id))
            .await
            .unwrap();

    assert_eq!(resolved.name, fixture.collection.name);
    assert!(
        matches!(state.collections_named(&fixture.collection.name), CacheLookup::Found([cached]) if cached.id == fixture.collection.id)
    );
    assert_eq!(
        state.collection_by_id(fixture.collection.id).unwrap().name,
        fixture.collection.name
    );
}

#[tokio::test]
async fn test_resolve_class_planning_backfills_cache_after_db_lookup() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("planning_class_cache")).await;
    let class_name_value = context.scoped_name("planning_class_cache_value");
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: class_name_value.clone(),
        description: "cached class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let mut state = PlanningState::default();

    let resolved = (resolve_class_planning(
        &context.pool,
        &mut state,
        None,
        Some(&ClassKey {
            name: class.name.clone(),
            collection_ref: None,
            collection_key: Some(CollectionKey {
                name: fixture.collection.name.clone(),
                path: None,
            }),
        }),
    ))
    .await
    .unwrap();

    assert_eq!(resolved.id, class.id);
    assert!(
        matches!(state.classes.lookup(&(fixture.collection.id, class.name.clone())), CacheLookup::Found(cached) if cached.id == class.id)
    );
}

#[tokio::test]
async fn test_resolve_object_planning_backfills_cache_after_db_lookup() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("planning_object_cache")).await;
    let class_name_value = context.scoped_name("planning_object_cache_class");
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: class_name_value.clone(),
        description: "cached class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let object_name_value = context.scoped_name("planning_object_cache_value");
    let object = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        name: object_name_value.clone(),
        description: "cached object".to_string(),
        data: serde_json::json!({"hostname":"cached"}),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let mut state = PlanningState::default();

    let resolved = (resolve_object_planning(
        &context.pool,
        &mut state,
        None,
        Some(&ObjectKey {
            name: object.name.clone(),
            class_ref: None,
            class_key: Some(ClassKey {
                name: class.name.clone(),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            }),
        }),
    ))
    .await
    .unwrap();

    assert_eq!(resolved.id, object.id);
    assert!(
        matches!(state.objects.lookup(&(class.id, object.name.clone())), CacheLookup::Found(cached) if cached.id == object.id)
    );
}

#[tokio::test]
async fn test_update_collection_refreshes_runtime_ref_for_following_items() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("update_collection_ref")).await;
    let updated_description = context.scoped_name("updated_collection_description");
    let execution = PlannedExecution::UpdateCollection {
        collection_id: fixture.collection.id,
        input: ImportCollectionInput {
            ref_: Some("collection:existing".to_string()),
            name: fixture.collection.name.clone(),
            description: updated_description.clone(),
            condition: None,
            timestamps: None,
            parent_collection_ref: None,
            parent_collection_key: None,
        },
    };

    let class_input = ImportClassInput {
        schema_activation: None,
        ref_: Some("class:child".to_string()),
        name: context.scoped_name("class_after_collection_update"),
        description: "child".to_string(),
        condition: None,
        timestamps: None,
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:existing".to_string()),
        collection_key: None,
    };

    let operations = [execution, PlannedExecution::CreateClass(class_input)]
        .into_iter()
        .enumerate()
        .map(|(index, operation)| {
            crate::services::import_boundary::import_operation_to_storage(operation)
                .map(|operation| StorageImportPlanItem::new(index, operation))
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let operations = StorageImportPlan::try_new(operations).unwrap();
    let backend = PostgresStorage::unobserved(context.pool.get_ref().clone());
    backend.apply_import_strict(operations).await.unwrap();
    let collection = backend
        .get_import_collection_by_id(
            hubuum_domain::CollectionId::new(fixture.collection.id).unwrap(),
        )
        .await
        .unwrap();

    let collection = collection.expect("collection should remain available after update");
    assert_eq!(collection.id().id(), fixture.collection.id);
    assert_eq!(collection.description(), updated_description);
}

#[tokio::test]
async fn test_update_class_refreshes_runtime_ref_for_following_items() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("update_class_ref")).await;
    let class_name_value = context.scoped_name("existing_class_for_update");
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: class_name_value.clone(),
        description: "existing class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let execution = PlannedExecution::UpdateClass {
        class_id: class.id,
        input: ImportClassInput {
            schema_activation: None,
            ref_: Some("class:existing".to_string()),
            name: class.name.clone(),
            description: "updated class".to_string(),
            condition: None,
            timestamps: None,
            json_schema: None,
            validate_schema: Some(false),
            collection_ref: None,
            collection_key: Some(CollectionKey {
                name: fixture.collection.name.clone(),
                path: None,
            }),
        },
    };

    let object_input = ImportObjectInput {
        ref_: Some("object:child".to_string()),
        name: context.scoped_name("object_after_class_update"),
        description: "child".to_string(),
        condition: None,
        timestamps: None,
        data: serde_json::json!({"hostname":"child"}),
        class_ref: Some("class:existing".to_string()),
        class_key: None,
    };

    let operations = [execution, PlannedExecution::CreateObject(object_input)]
        .into_iter()
        .enumerate()
        .map(|(index, operation)| {
            crate::services::import_boundary::import_operation_to_storage(operation)
                .map(|operation| StorageImportPlanItem::new(index, operation))
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let operations = StorageImportPlan::try_new(operations).unwrap();
    let backend = PostgresStorage::unobserved(context.pool.get_ref().clone());
    backend.apply_import_strict(operations).await.unwrap();
    let updated = backend
        .get_import_class_by_name(
            hubuum_domain::CollectionId::new(fixture.collection.id).unwrap(),
            &class.name,
        )
        .await
        .unwrap();

    let updated = updated.expect("class should remain available after update");
    assert_eq!(updated.id().id(), class.id);
    assert_eq!(updated.name(), class.name);
}

#[tokio::test]
async fn test_plan_class_update_preserves_existing_schema_for_following_objects() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("update_class_schema_ref")).await;
    let schema = serde_json::json!({
        "type": "object",
        "required": ["hostname"],
        "properties": {
            "hostname": {"type": "string"}
        }
    });
    let class_name_value = context.scoped_name("existing_class_with_schema");
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: class_name_value.clone(),
        description: "existing class".to_string(),
        json_schema: Some(schema.clone()),
        validate_schema: Some(true),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let mut state = PlanningState::default();
    state.remember_collection(
        Some("collection:existing".to_string()),
        CollectionResolution {
            id: fixture.collection.id,
            name: fixture.collection.name.clone(),
            description: fixture.collection.description.clone(),
            parent_collection_id: fixture.collection.parent_collection_id,
            exists_in_db: true,
        },
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };

    (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &ImportClassInput {
            schema_activation: None,
            ref_: Some("class:existing".to_string()),
            name: class.name.clone(),
            description: "updated description".to_string(),
            condition: None,
            timestamps: None,
            json_schema: None,
            validate_schema: None,
            collection_ref: Some("collection:existing".to_string()),
            collection_key: None,
        },
    ))
    .await
    .unwrap();

    let err = (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &ImportObjectInput {
            ref_: Some("object:invalid".to_string()),
            name: context.scoped_name("invalid_object_after_class_update"),
            description: "invalid".to_string(),
            condition: None,
            timestamps: None,
            data: serde_json::json!({"hostname": 42}),
            class_ref: Some("class:existing".to_string()),
            class_key: None,
        },
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
}

#[rstest]
#[case::legacy_overwrite(false, None)]
#[case::schema_removal(true, Some(false))]
#[case::schema_removal_default_validation(true, None)]
#[tokio::test]
async fn test_class_overwrite_omitting_schema_respects_activation_policy(
    #[case] activate_removal: bool,
    #[case] validate_schema: Option<bool>,
) {
    use hubuum_domain::{ClassId, CollectionId, SchemaRevision};
    use hubuum_storage_core::{
        SchemaEvolutionStorage, StorageClassSchemaPolicy, StorageSchemaStage,
        StorageValidatedSchemaPolicy,
    };

    use crate::events::EventContext;
    use crate::models::ImportSchemaActivation;
    use crate::models::schema_evolution::SchemaActivationPolicy;

    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("persist_class_schema_on_overwrite")).await;
    let schema = serde_json::json!({
        "type": "object",
        "required": ["hostname"],
        "properties": {
            "hostname": {"type": "string"}
        }
    });
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("persisted_class_schema"),
        description: "existing class".to_string(),
        json_schema: Some(schema.clone()),
        validate_schema: Some(true),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let backend = PostgresStorage::unobserved(context.pool.get_ref().clone());
    let activation = if activate_removal {
        let revision = backend
            .stage_schema_revision(StorageSchemaStage::new(
                CollectionId::new(fixture.collection.id).unwrap(),
                ClassId::new(class.id).unwrap(),
                StorageValidatedSchemaPolicy::try_new(
                    StorageClassSchemaPolicy::try_from_parts(None, false).unwrap(),
                )
                .unwrap(),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        Some(ImportSchemaActivation {
            revision: revision.reference().revision(),
            expected_active_revision: SchemaRevision::INITIAL,
            policy: SchemaActivationPolicy::AllowPending,
            impact_task_id: None,
        })
    } else {
        None
    };
    let mut state = PlanningState::default();
    state.remember_collection(
        Some("collection:existing".to_string()),
        CollectionResolution {
            id: fixture.collection.id,
            name: fixture.collection.name.clone(),
            description: fixture.collection.description.clone(),
            parent_collection_id: fixture.collection.parent_collection_id,
            exists_in_db: true,
        },
    );
    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::Strict),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Abort),
    };
    let planned = plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &ImportClassInput {
            schema_activation: activation,
            ref_: Some("class:existing".to_string()),
            name: class.name.clone(),
            description: "updated class".to_string(),
            json_schema: None,
            validate_schema,
            collection_ref: None,
            collection_key: Some(CollectionKey {
                name: fixture.collection.name.clone(),
                path: None,
            }),
            condition: None,
            timestamps: None,
        },
    )
    .await
    .unwrap();
    let operation = crate::services::import_boundary::import_operation_to_storage(
        planned
            .execution
            .expect("class overwrite must be executable"),
    )
    .map(|operation| StorageImportPlanItem::new(0, operation))
    .unwrap();
    backend
        .apply_import_strict(StorageImportPlan::try_new(vec![operation]).unwrap())
        .await
        .unwrap();

    let updated = backend
        .get_import_class_by_name(
            hubuum_domain::CollectionId::new(fixture.collection.id).unwrap(),
            &class.name,
        )
        .await
        .unwrap()
        .expect("updated class must remain available");
    assert_eq!(
        updated.json_schema(),
        (!activate_removal).then_some(&schema)
    );
    assert_eq!(updated.validates_schema(), !activate_removal);
}

#[tokio::test]
async fn test_update_object_refreshes_runtime_ref_for_following_items() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("update_object_ref")).await;
    let class_name_value = context.scoped_name("existing_class_for_object_update");
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: class_name_value.clone(),
        description: "existing class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let object_name_value = context.scoped_name("existing_object_for_update");
    let object = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        name: object_name_value.clone(),
        description: "existing object".to_string(),
        data: serde_json::json!({"hostname":"existing"}),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let execution = PlannedExecution::UpdateObject {
        object_id: object.id,
        input: ImportObjectInput {
            ref_: Some("object:existing".to_string()),
            name: object.name.clone(),
            description: "updated object".to_string(),
            condition: None,
            timestamps: None,
            data: serde_json::json!({"hostname":"updated"}),
            class_ref: None,
            class_key: Some(ClassKey {
                name: class.name.clone(),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            }),
        },
    };

    let operation = crate::services::import_boundary::import_operation_to_storage(execution)
        .map(|operation| StorageImportPlanItem::new(0, operation))
        .unwrap();
    let plan = StorageImportPlan::try_new(vec![operation]).unwrap();
    let backend = PostgresStorage::unobserved(context.pool.get_ref().clone());
    backend.apply_import_strict(plan).await.unwrap();
    let resolved = backend
        .get_import_object_by_name(ClassId::new(class.id).unwrap(), &object.name)
        .await
        .unwrap()
        .expect("updated object should remain addressable by name");

    assert_eq!(resolved.id().id(), object.id);
    assert_eq!(resolved.description(), "updated object");
}

#[tokio::test]
async fn import_overwrite_does_not_reuse_collection_grants_outside_object_scope() {
    use crate::models::{HubuumObjectID, TokenResourceScope, TokenScope};
    use crate::traits::CanDelete;

    let context = TestContext::new().await;
    let fixture = context.collection_fixture("import_scoped_overwrite").await;
    fixture
        .owner_group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("import_scoped_class"),
        description: "scoped overwrite".into(),
        json_schema: None,
        validate_schema: None,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let mut objects = Vec::new();
    for label in ["allowed", "denied"] {
        objects.push(
            NewHubuumObject {
                collection_id: fixture.collection.id,
                hubuum_class_id: class.id,
                name: context.scoped_name(label),
                description: "original".into(),
                data: serde_json::json!({}),
            }
            .save_without_events(&context.pool)
            .await
            .unwrap(),
        );
    }
    let scope = TokenScope::from_stored_parts(
        Some(vec![Permissions::UpdateObject]),
        Some(vec![TokenResourceScope::Object(
            HubuumObjectID::new(objects[0].id).unwrap(),
        )]),
    )
    .unwrap();
    let request: ImportRequest = serde_json::from_value(serde_json::json!({
        "version": CURRENT_IMPORT_VERSION,
        "mode": {"collision_policy": "overwrite", "permission_policy": "continue", "atomicity": "best_effort"},
        "graph": {"objects": objects.iter().map(|object| serde_json::json!({
            "name": object.name,
            "description": "updated",
            "data": {},
            "class_key": {"name": class.name, "collection_key": {"name": fixture.collection.name}}
        })).collect::<Vec<_>>()}
    })).unwrap();

    let planning = plan_import(&context.pool, &context.normal_user, Some(&scope), &request).await;
    assert!(matches!(planning.planned_items.as_slice(), [PlannedItem {
        execution: Some(PlannedExecution::UpdateObject {object_id, ..}), ..
    }] if *object_id == objects[0].id));
    assert!(matches!(
        planning.failures.as_slice(),
        [PlanningFailure {
            kind: FailureKind::Permission,
            ..
        }]
    ));

    for object in objects {
        object.delete_without_events(&context.pool).await.unwrap();
    }
    class.delete_without_events(&context.pool).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[case::object_denied(ResourceKind::Object, false)]
#[case::object_allowed(ResourceKind::Object, true)]
#[case::class_denied(ResourceKind::Class, false)]
#[case::class_allowed(ResourceKind::Class, true)]
#[tokio::test]
async fn import_overwrite_authorizes_existing_resource(
    #[case] kind: ResourceKind,
    #[case] allowed: bool,
    #[values(false, true)] scoped: bool,
) {
    use crate::models::{HubuumClassID, HubuumObjectID, TokenResourceScope, TokenScope};
    use crate::traits::{CanDelete, SelfAccessors};
    let context = TestContext::new().await;
    let fixture = context.collection_fixture("import_overwrite_policy").await;
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("import_overwrite_class"),
        description: "original".into(),
        json_schema: None,
        validate_schema: None,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let object = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        name: context.scoped_name("import_overwrite_object"),
        description: "original".into(),
        data: serde_json::json!({"protected": true}),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let (permission, resource_id, graph) = match kind {
        ResourceKind::Object => (
            Permissions::UpdateObject,
            object.id,
            serde_json::json!({"objects": [{
                "name": object.name,
                "description": "updated",
                "data": {"protected": false},
                "class_key": {"name": class.name, "collection_key": {"name": fixture.collection.name}}
            }]}),
        ),
        ResourceKind::Class => (
            Permissions::UpdateClass,
            class.id,
            serde_json::json!({"classes": [{
                "name": class.name,
                "description": "updated",
                "collection_key": {"name": fixture.collection.name}
            }]}),
        ),
        _ => unreachable!("only class and object overwrites are exercised"),
    };
    let policy = MockTreetopBackend::new();
    // A collection probe may succeed even when the actual resource is denied.
    if allowed {
        policy.add_rule(MockAllowRule {
            group_id: fixture.owner_group.id,
            action: permission,
            resource_kind: kind.clone(),
            resource_id: Some(resource_id),
            attrs: ResourceFields::default(),
        });
    } else {
        policy.add_prospective_rule(MockAllowRule {
            group_id: fixture.owner_group.id,
            action: permission,
            resource_kind: kind.clone(),
            resource_id: None,
            attrs: ResourceFields {
                collection_id: Some(fixture.collection.id),
                ..ResourceFields::default()
            },
        });
    }
    let backend = crate::tests::app_context_with_permission_backend(
        context.pool.get_ref().clone(),
        Arc::new(policy),
    );
    let scopes = scoped.then(|| {
        let resource = match kind {
            ResourceKind::Object => {
                TokenResourceScope::Object(HubuumObjectID::new(object.id).unwrap())
            }
            ResourceKind::Class => TokenResourceScope::Class(HubuumClassID::new(class.id).unwrap()),
            _ => unreachable!(),
        };
        TokenScope::from_stored_parts(Some(vec![permission]), Some(vec![resource])).unwrap()
    });
    let request: ImportRequest = serde_json::from_value(serde_json::json!({
        "version": CURRENT_IMPORT_VERSION,
        "mode": {"collision_policy": "overwrite"},
        "graph": graph
    }))
    .unwrap();
    let planning = plan_import(&backend, &context.admin_user, scopes.as_ref(), &request).await;
    if allowed {
        assert!(planning.failures.is_empty());
        assert_eq!(planning.planned_items.len(), 1);
    } else {
        assert!(matches!(
            planning.failures.as_slice(),
            [PlanningFailure {
                kind: FailureKind::Permission,
                ..
            }]
        ));
        assert!(planning.planned_items.is_empty());
    }
    let task = create_worker_test_task(
        &context,
        StorageTaskKind::Import,
        serde_json::to_value(request).unwrap(),
        1,
        "import_overwrite_policy",
    )
    .await;
    let claimed = claim_worker_test_task(&context, task.id).await;
    let principal = crate::services::authentication::ExecutionPrincipal::resolve(
        &backend,
        context.admin_user.id,
    )
    .await
    .unwrap();
    let status = execute_import_task(&backend, &claimed, &principal, scopes.as_ref())
        .await
        .unwrap();
    assert_eq!(
        status,
        if allowed {
            TaskStatus::Succeeded
        } else {
            TaskStatus::Failed
        }
    );
    let description = match kind {
        ResourceKind::Object => {
            HubuumObjectID::new(object.id)
                .unwrap()
                .instance(&context.pool)
                .await
                .unwrap()
                .description
        }
        ResourceKind::Class => {
            HubuumClassID::new(class.id)
                .unwrap()
                .instance(&context.pool)
                .await
                .unwrap()
                .description
        }
        _ => unreachable!(),
    };
    assert_eq!(description, if allowed { "updated" } else { "original" });
    object.delete_without_events(&context.pool).await.unwrap();
    class.delete_without_events(&context.pool).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[case::persisted((10,20))]
#[case::virtual_endpoint((-1,20))]
#[tokio::test]
async fn a_planned_relation_supersedes_an_absent_storage_row(#[case] pair: (i32, i32)) {
    let mut state = PlanningState::default();
    let absent = state
        .class_relations
        .exists(pair, async {
            assert!(pair.0 > 0, "virtual endpoints must stay local");
            Ok(false)
        })
        .await
        .unwrap();
    assert!(!absent);
    state.class_relations.record_creation(pair);
    let exists = state
        .class_relations
        .exists(pair, async {
            panic!("a planned relation must not query storage")
        })
        .await
        .unwrap();
    assert!(exists);
}

#[tokio::test]
async fn best_effort_import_resolves_a_parent_planned_after_a_name_lookup_miss() {
    let context = TestContext::new().await;
    let parent_name = context.scoped_name("late_parent");
    let parent_key = CollectionKey {
        name: parent_name.clone(),
        path: None,
    };
    let collection = |name, parent_collection_key| ImportCollectionInput {
        name,
        description: String::new(),
        parent_collection_key,
        parent_collection_ref: None,
        ref_: None,
        condition: None,
        timestamps: None,
    };
    let later_child = context.scoped_name("later_child");
    let request = ImportRequest {
        version: CURRENT_IMPORT_VERSION,
        dry_run: Some(false),
        mode: Some(ImportMode {
            atomicity: Some(ImportAtomicity::BestEffort),
            ..ImportMode::default()
        }),
        graph: ImportGraph {
            collections: vec![
                collection(context.scoped_name("early_child"), Some(parent_key.clone())),
                collection(parent_name, None),
                collection(later_child.clone(), Some(parent_key)),
            ],
            ..ImportGraph::default()
        },
    };

    let plan = plan_import(&context.pool, &context.admin_user, None, &request).await;
    assert!(plan.planned_items.iter().any(|item| matches!(
        &item.execution,
        Some(PlannedExecution::CreateCollection(input)) if input.name == later_child
    )));
}
