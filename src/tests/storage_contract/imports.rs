use super::*;

#[actix_web::test]
async fn every_available_storage_backend_supplies_the_complete_import_contract() {
    let _permit = postgres_permit().await;
    let preflight_name = prefix("import_preflight_collection");
    let best_effort_name = prefix("import_best_effort_collection");
    let rollback_name = prefix("import_rollback_collection");
    let class_name = prefix("import_preflight_class");
    let object_name = prefix("import_preflight_object");
    let collection_input = |name: &str, reference: &str| ImportCollectionInput {
        ref_: Some(reference.to_string()),
        name: name.to_string(),
        description: "storage compatibility import".to_string(),
        parent_collection_ref: None,
        parent_collection_key: None,
        condition: None,
        timestamps: None,
    };

    for backend in available_backends() {
        let root = backend
            .get_import_root_collection()
            .await
            .expect("certified backend should resolve the import root");
        let root_id = root.id();
        let root_name = root.name().to_string();
        assert_eq!(
            backend
                .get_import_collection_by_id(root_id)
                .await
                .expect("certified backend should look up import collections by id")
                .map(|collection| collection.id()),
            Some(root_id)
        );
        let root_key = crate::services::import_boundary::collection_key_to_storage(CollectionKey {
            name: root_name.clone(),
            path: Some(Vec::new()),
        });
        assert!(
            backend
                .get_import_collection_by_key(&root_key)
                .await
                .expect("certified backend should look up import collections by path")
                .is_some()
        );
        assert!(
            backend
                .list_import_collections_by_name(&root_name)
                .await
                .expect("certified backend should look up import collections by name")
                .iter()
                .any(|collection| collection.id() == root_id)
        );
        assert!(
            backend
                .get_import_collection_child_by_name(root_id, &preflight_name)
                .await
                .expect("certified backend should look up import children")
                .is_none()
        );
        assert!(
            backend
                .get_import_class_by_name(root_id, &prefix("missing_import_class"))
                .await
                .expect("certified backend should look up import classes")
                .is_none()
        );
        assert!(
            backend
                .list_import_classes_by_names(root_id, &[])
                .await
                .expect("certified backend should batch import class lookups")
                .is_empty()
        );
        assert!(
            backend
                .get_import_object_by_name(
                    ClassId::new(i32::MAX).unwrap(),
                    &prefix("missing_import_object"),
                )
                .await
                .expect("certified backend should look up import objects")
                .is_none()
        );
        assert!(
            backend
                .list_import_objects_by_names(ClassId::new(i32::MAX).unwrap(), &[])
                .await
                .expect("certified backend should batch import object lookups")
                .is_empty()
        );
        assert!(
            !backend
                .has_import_class_relation(
                    ClassId::new(i32::MAX - 1).unwrap(),
                    ClassId::new(i32::MAX).unwrap(),
                )
                .await
                .expect("certified backend should look up import class relations")
        );
        assert!(
            !backend
                .has_import_object_relation(
                    ObjectId::new(i32::MAX - 1).unwrap(),
                    ObjectId::new(i32::MAX).unwrap(),
                )
                .await
                .expect("certified backend should look up import object relations")
        );
        assert!(
            !backend
                .has_import_group(LOCAL_IDENTITY_SCOPE, &prefix("missing_import_group"),)
                .await
                .expect("certified backend should look up import groups")
        );

        let preflight_plan = StorageImportPlan::try_new(vec![
            StorageImportPlanItem::new(
                0,
                crate::services::import_boundary::import_operation_to_storage(
                    ApplicationImportOperation::CreateCollection(collection_input(
                        &preflight_name,
                        "collection:preflight",
                    )),
                )
                .expect("valid collection input should cross the storage boundary"),
            ),
            StorageImportPlanItem::new(
                1,
                crate::services::import_boundary::import_operation_to_storage(
                    ApplicationImportOperation::CreateClass(ImportClassInput {
                        schema_activation: None,
                        ref_: Some("class:preflight".to_string()),
                        name: class_name.clone(),
                        description: "storage compatibility import class".to_string(),
                        json_schema: None,
                        validate_schema: Some(false),
                        collection_ref: Some("collection:preflight".to_string()),
                        collection_key: None,
                        condition: None,
                        timestamps: None,
                    }),
                )
                .expect("valid class input should cross the storage boundary"),
            ),
            StorageImportPlanItem::new(
                2,
                crate::services::import_boundary::import_operation_to_storage(
                    ApplicationImportOperation::CreateObject(ImportObjectInput {
                        ref_: Some("object:preflight".to_string()),
                        name: object_name.clone(),
                        description: "storage compatibility import object".to_string(),
                        data: serde_json::json!({"state": "created"}),
                        class_ref: Some("class:preflight".to_string()),
                        class_key: None,
                        condition: None,
                        timestamps: None,
                    }),
                )
                .expect("valid object input should cross the storage boundary"),
            ),
        ])
        .expect("valid operations should form an import plan");
        let (preflight, aborted) = backend
            .preflight_import(
                preflight_plan.clone(),
                crate::services::import_boundary::import_mode_to_storage(ImportMode::default()),
            )
            .await
            .expect("certified backend should preflight an import")
            .into_parts();
        assert!(!aborted);
        assert_eq!(preflight.len(), 3);
        assert!(
            preflight
                .into_iter()
                .all(|result| result.into_parts().2.is_none())
        );
        assert!(
            backend
                .get_import_collection_child_by_name(root_id, &preflight_name)
                .await
                .expect("preflight rollback should remain queryable")
                .is_none(),
            "import preflight must roll back every mutation"
        );

        backend
            .apply_import_strict(preflight_plan)
            .await
            .expect("certified backend should atomically apply a strict import");

        let imported_collection = backend
            .get_import_collection_child_by_name(root_id, &preflight_name)
            .await
            .expect("committed import collection should remain queryable")
            .expect("committed import collection should exist");
        let imported_class = backend
            .get_import_class_by_name(imported_collection.id(), &class_name)
            .await
            .expect("committed import class should remain queryable")
            .expect("committed import class should exist");
        let imported_object = backend
            .get_import_object_by_name(imported_class.id(), &object_name)
            .await
            .expect("committed import object should remain queryable")
            .expect("committed import object should exist");
        assert_eq!(
            imported_object.data(),
            &serde_json::json!({"state": "created"})
        );

        let stale_revision = ResourceRevision::new(
            imported_object
                .revision()
                .get()
                .checked_add(1)
                .expect("test revision should not overflow"),
        )
        .expect("test revision should remain positive");
        let stale_update = StorageImportPlan::try_new(vec![StorageImportPlanItem::new(
            0,
            crate::services::import_boundary::import_operation_to_storage(
                ApplicationImportOperation::UpdateObject {
                    object_id: imported_object.id().id(),
                    input: ImportObjectInput {
                        ref_: None,
                        name: object_name.clone(),
                        description: "stale update must not persist".to_string(),
                        data: serde_json::json!({"state": "stale"}),
                        class_ref: None,
                        class_key: Some(ClassKey {
                            name: class_name.clone(),
                            collection_ref: None,
                            collection_key: Some(CollectionKey {
                                name: preflight_name.clone(),
                                path: Some(vec![preflight_name.clone()]),
                            }),
                        }),
                        condition: Some(ImportWriteCondition::IfRevision {
                            expected_revision: stale_revision,
                        }),
                        timestamps: None,
                    },
                },
            )
            .expect("valid object update should cross the storage boundary"),
        )])
        .expect("valid object update should form an import plan");
        let error = backend
            .apply_import_strict(stale_update)
            .await
            .expect_err("stale import updates must be rejected");
        assert_eq!(
            error.kind(),
            StorageErrorKind::PreconditionFailed,
            "{error:?}"
        );
        assert_eq!(error.current_revision(), Some(imported_object.revision()));
        assert_eq!(
            backend
                .get_import_object_by_name(imported_class.id(), &object_name)
                .await
                .expect("rejected import object should remain queryable")
                .expect("rejected import object should still exist")
                .data(),
            &serde_json::json!({"state": "created"})
        );

        let rollback_plan = StorageImportPlan::try_new(vec![
            StorageImportPlanItem::new(
                0,
                crate::services::import_boundary::import_operation_to_storage(
                    ApplicationImportOperation::CreateCollection(collection_input(
                        &rollback_name,
                        "collection:rollback",
                    )),
                )
                .expect("valid collection input should cross the storage boundary"),
            ),
            StorageImportPlanItem::new(
                1,
                crate::services::import_boundary::import_operation_to_storage(
                    ApplicationImportOperation::CreateClass(ImportClassInput {
                        schema_activation: None,
                        ref_: Some("class:rollback_failure".to_string()),
                        name: prefix("import_rollback_class"),
                        description: "must fail".to_string(),
                        json_schema: None,
                        validate_schema: Some(false),
                        collection_ref: Some("collection:missing".to_string()),
                        collection_key: None,
                        condition: None,
                        timestamps: None,
                    }),
                )
                .expect("valid class input should cross the storage boundary"),
            ),
        ])
        .expect("valid operations should form a rollback import plan");
        assert!(backend.apply_import_strict(rollback_plan).await.is_err());
        assert!(
            backend
                .get_import_collection_child_by_name(root_id, &rollback_name)
                .await
                .expect("strict rollback should remain queryable")
                .is_none(),
            "strict import must roll back earlier successful items"
        );

        let best_effort = backend
            .apply_import_best_effort(
                StorageImportPlan::try_new(vec![
                    StorageImportPlanItem::new(
                        0,
                        crate::services::import_boundary::import_operation_to_storage(
                            ApplicationImportOperation::CreateCollection(collection_input(
                                &best_effort_name,
                                "collection:best_effort",
                            )),
                        )
                        .expect("valid collection input should cross the storage boundary"),
                    ),
                    StorageImportPlanItem::new(
                        1,
                        crate::services::import_boundary::import_operation_to_storage(
                            ApplicationImportOperation::CreateClass(ImportClassInput {
                                schema_activation: None,
                                ref_: Some("class:best_effort_failure".to_string()),
                                name: prefix("import_best_effort_class"),
                                description: "must fail".to_string(),
                                json_schema: None,
                                validate_schema: Some(false),
                                collection_ref: Some("collection:missing".to_string()),
                                collection_key: None,
                                condition: None,
                                timestamps: None,
                            }),
                        )
                        .expect("valid class input should cross the storage boundary"),
                    ),
                ])
                .expect("valid operations should form a best-effort import plan"),
                crate::services::import_boundary::import_mode_to_storage(ImportMode {
                    atomicity: Some(ImportAtomicity::BestEffort),
                    ..ImportMode::default()
                }),
            )
            .await
            .expect("certified backend should apply a best-effort import");
        let (best_effort, aborted) = best_effort.into_parts();
        assert!(!aborted);
        assert_eq!(best_effort.len(), 2);
        assert!(best_effort[0].error().is_none());
        assert!(best_effort[1].error().is_some());

        let object_target = backend
            .object_store()
            .resolve_object(StorageObjectSelector::Ids {
                class_id: imported_class.id(),
                object_id: imported_object.id(),
            })
            .await
            .expect("imported object should resolve for cleanup");
        backend
            .object_store()
            .delete_object(&object_target, &EventContext::system())
            .await
            .expect("imported object should be removed")
            .into_value();
        let class_target = backend
            .class_store()
            .resolve_class(StorageClassSelector::Id(imported_class.id()))
            .await
            .expect("imported class should resolve for cleanup");
        backend
            .class_store()
            .delete_class(&class_target, &EventContext::system())
            .await
            .expect("imported class should be removed")
            .into_value();
        let best_effort_collection = backend
            .get_import_collection_child_by_name(root_id, &best_effort_name)
            .await
            .expect("committed import collection should remain queryable")
            .expect("committed import collection should exist");
        for collection in [imported_collection, best_effort_collection] {
            backend
                .collection_store()
                .delete_collection(collection.id(), &EventContext::system())
                .await
                .expect("import compatibility fixture should be removed")
                .into_value();
        }
    }
}

#[rstest::rstest]
#[case::strict(true)]
#[case::best_effort(false)]
#[actix_web::test]
async fn every_available_storage_backend_commits_claimed_import_effects_and_receipts(
    #[case] strict: bool,
) {
    use hubuum_storage_core::{FencedImportItem, FencedImportPlan};
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let backend = environment.storage();
        let user = create_backend_user(&backend, &prefix("fenced_import_user")).await;
        let task = backend
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Import,
                    user.principal_id,
                    serde_json::json!({}),
                    1,
                )
                .try_build()
                .unwrap(),
            )
            .await
            .map(expect_legacy_task)
            .unwrap();
        let duration = StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap();
        let claimed = match &environment {
            BackendTestEnvironment::Postgres { pool } => {
                hubuum_storage_postgres::test_support::claim_task_by_id_with_lease(
                    pool,
                    task.id(),
                    duration,
                )
                .await
                .unwrap()
            }
            BackendTestEnvironment::Memory { .. } => backend
                .claim_next_task(StorageTaskClaimRequest::new(
                    TaskWorkerId::generate(),
                    duration,
                ))
                .await
                .unwrap()
                .unwrap(),
        };
        let name = prefix("fenced_import_collection");
        let operation = crate::services::import_boundary::import_operation_to_storage(
            ApplicationImportOperation::CreateCollection(ImportCollectionInput {
                ref_: None,
                name: name.clone(),
                description: "atomic receipt".into(),
                parent_collection_ref: None,
                parent_collection_key: None,
                condition: None,
                timestamps: None,
            }),
        )
        .unwrap();
        let plan = FencedImportPlan::try_new(
            claimed.lease().clone(),
            vec![FencedImportItem::new(
                0,
                Some(operation),
                StorageImportResult::builder(task.id(), "collection", "create", "succeeded")
                    .identifier(Some(name.clone()))
                    .build(),
            )],
        )
        .unwrap();
        if strict {
            backend.apply_claimed_import_strict(plan).await.unwrap();
        } else {
            backend
                .apply_claimed_import_best_effort(
                    plan,
                    crate::services::import_boundary::import_mode_to_storage(ImportMode::default()),
                )
                .await
                .unwrap();
        }
        let root = backend.get_import_root_collection().await.unwrap();
        let collection = backend
            .get_import_collection_child_by_name(root.id(), &name)
            .await
            .unwrap();
        let (receipts, _) = backend
            .list_import_task_results(StorageTaskChildListQuery::new(
                task.id(),
                QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true).unwrap(),
            ))
            .await
            .unwrap()
            .into_parts();
        assert_eq!(
            (collection.is_some(), receipts.len()),
            (true, 1),
            "a committed effect must have exactly one durable receipt"
        );
        backend
            .collection_store()
            .delete_collection(collection.unwrap().id(), &EventContext::system())
            .await
            .unwrap()
            .into_value();
        if let BackendTestEnvironment::Postgres { pool } = &environment {
            hubuum_storage_postgres::test_support::delete_task(pool, task.id())
                .await
                .unwrap();
        }
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_fences_planning_result_writes() {
    use hubuum_storage_core::FencedImportResults;
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let backend = environment.storage();
        let user = create_backend_user(&backend, &prefix("planning_receipt_user")).await;
        let task = backend
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Import,
                    user.principal_id,
                    serde_json::json!({}),
                    1,
                )
                .try_build()
                .unwrap(),
            )
            .await
            .map(expect_legacy_task)
            .unwrap();
        let lease = StorageTaskLease::new(
            task.id(),
            StorageTaskClaimToken::new("00000000-0000-0000-0000-000000000001"),
        );
        let results = FencedImportResults::try_new(
            lease,
            vec![
                StorageImportResult::builder(task.id(), "collection", "create", "failed")
                    .error(Some("planning failure".into()))
                    .build(),
            ],
        )
        .unwrap();
        assert!(
            backend
                .record_claimed_import_results(results)
                .await
                .is_err(),
            "a queued task does not authorize worker writes"
        );
        let (receipts, _) = backend
            .list_import_task_results(StorageTaskChildListQuery::new(
                task.id(),
                QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true).unwrap(),
            ))
            .await
            .unwrap()
            .into_parts();
        assert!(
            receipts.is_empty(),
            "rejected claims cannot leave planning results"
        );
        if let BackendTestEnvironment::Postgres { pool } = &environment {
            hubuum_storage_postgres::test_support::delete_task(pool, task.id())
                .await
                .unwrap();
        }
        delete_backend_user(&backend, user).await;
    }
}
