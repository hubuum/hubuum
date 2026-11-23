use super::*;

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_temporal_history() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("history_resource"), Vec::new()).await;
        let actor_name = prefix("history_actor");
        let actor = create_backend_user(&backend, &actor_name).await;
        let at = chrono::Utc::now();
        let collection_options = prepare_db_pagination::<CollectionHistory>(
            &crate::models::search::parse_query_parameter("limit=10")
                .expect("history compatibility query should parse"),
        )
        .expect("collection history pagination should prepare");
        let collection_page = backend
            .list_collection_history(StorageHistoryListQuery::new(
                ResourceId::new(fixture.collection.id().id()).unwrap(),
                collection_options,
                StorageHistoryCollectionScope::All,
            ))
            .await
            .expect("certified backend should list collection history");
        let (collection_rows, total_count) = collection_page.into_parts();
        assert!(!collection_rows.is_empty());
        assert!(total_count.is_some_and(|total| total >= 1));
        assert!(
            backend
                .get_collection_history_as_of(StorageHistoryAsOfQuery::new(
                    ResourceId::new(fixture.collection.id().id()).unwrap(),
                    at,
                ))
                .await
                .expect("certified backend should load collection history as of a point")
                .is_some()
        );

        let class_options = prepare_db_pagination::<HubuumClassHistory>(
            &crate::models::search::parse_query_parameter("limit=10")
                .expect("history compatibility query should parse"),
        )
        .expect("class history pagination should prepare");
        backend
            .list_class_history(StorageHistoryListQuery::new(
                ResourceId::new(i32::MAX).unwrap(),
                class_options,
                StorageHistoryCollectionScope::All,
            ))
            .await
            .expect("certified backend should list class history");
        assert!(
            backend
                .get_class_history_as_of(StorageHistoryAsOfQuery::new(
                    ResourceId::new(i32::MAX).unwrap(),
                    at,
                ))
                .await
                .expect("certified backend should query class history as of a point")
                .is_none()
        );

        let object_options = prepare_db_pagination::<HubuumObjectHistory>(
            &crate::models::search::parse_query_parameter("limit=10")
                .expect("history compatibility query should parse"),
        )
        .expect("object history pagination should prepare");
        backend
            .list_object_history(StorageObjectHistoryListQuery::new(
                ObjectId::new(i32::MAX).unwrap(),
                ClassId::new(i32::MAX).unwrap(),
                object_options,
                StorageHistoryCollectionScope::All,
            ))
            .await
            .expect("certified backend should list object history");
        assert!(
            backend
                .get_object_history_as_of(StorageObjectHistoryAsOfQuery::new(
                    ObjectId::new(i32::MAX).unwrap(),
                    ClassId::new(i32::MAX).unwrap(),
                    at,
                ))
                .await
                .expect("certified backend should query object history as of a point")
                .is_none()
        );

        let template_options = prepare_db_pagination::<ExportTemplateHistory>(
            &crate::models::search::parse_query_parameter("limit=10")
                .expect("history compatibility query should parse"),
        )
        .expect("template history pagination should prepare");
        backend
            .list_export_template_history(StorageHistoryListQuery::new(
                ResourceId::new(i32::MAX).unwrap(),
                template_options,
                StorageHistoryCollectionScope::All,
            ))
            .await
            .expect("certified backend should list template history");
        assert!(
            backend
                .get_export_template_history_as_of(StorageHistoryAsOfQuery::new(
                    ResourceId::new(i32::MAX).unwrap(),
                    at,
                ))
                .await
                .expect("certified backend should query template history as of a point")
                .is_none()
        );

        let remote_target_options = prepare_db_pagination::<RemoteTargetHistory>(
            &crate::models::search::parse_query_parameter("limit=10")
                .expect("history compatibility query should parse"),
        )
        .expect("remote-target history pagination should prepare");
        backend
            .list_remote_target_history(StorageHistoryListQuery::new(
                ResourceId::new(i32::MAX).unwrap(),
                remote_target_options,
                StorageHistoryCollectionScope::All,
            ))
            .await
            .expect("certified backend should list remote-target history");
        assert!(
            backend
                .get_remote_target_history_as_of(StorageHistoryAsOfQuery::new(
                    ResourceId::new(i32::MAX).unwrap(),
                    at,
                ))
                .await
                .expect("certified backend should query remote-target history as of a point")
                .is_none()
        );

        let names = backend
            .resolve_history_principal_names(vec![actor.principal_id])
            .await
            .expect("certified backend should resolve history principal names");
        assert!(names.into_iter().any(|row| {
            let (principal_id, name) = row.into_parts();
            principal_id == actor.principal_id && name == actor_name
        }));
        delete_backend_object_fixture(&backend, fixture).await;
        delete_backend_user(&backend, actor).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_catalog_queries() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let needle = prefix("catalog_query");
        let fixture = create_backend_object_fixture(
            &backend,
            &needle,
            vec![serde_json::json!({"needle": needle})],
        )
        .await;
        let request = || {
            StorageCatalogListQuery::new(
                QueryOptions::new(
                    vec![
                        ParsedQueryParam::from_parts(
                            FilterField::Name,
                            SearchOperator::Contains { is_negated: false },
                            needle.clone(),
                        )
                        .unwrap(),
                    ],
                    Vec::new(),
                    Some(10),
                    None,
                    true,
                )
                .expect("contract query must be valid"),
                StorageVisibility::new(
                    principal_id(i32::MAX),
                    true,
                    None::<Vec<StorageAuthorizationPermission>>,
                    None,
                ),
            )
        };

        let (collections, collection_total) = backend
            .list_collections(request())
            .await
            .expect("certified backend should list collections")
            .into_parts();
        assert_eq!(collection_total, Some(1));
        assert!(collections.into_iter().any(|row| {
            let (id, ..) = row.into_parts();
            id == fixture.collection.id()
        }));

        let (classes, class_total) = backend
            .list_classes(request())
            .await
            .expect("certified backend should list classes")
            .into_parts();
        assert_eq!(class_total, Some(1));
        assert!(classes.into_iter().any(|row| {
            let (id, ..) = row.into_parts();
            id == fixture.class.id()
        }));

        let (objects, object_total) = backend
            .list_objects(request())
            .await
            .expect("certified backend should list objects")
            .into_parts();
        assert_eq!(object_total, Some(1));
        assert!(objects.into_iter().any(|row| {
            let (id, ..) = row.into_parts();
            id == fixture.objects[0].id()
        }));
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_computed_object_queries() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let needle = prefix("computed_object_query");
        let owner = create_backend_user(&backend, &prefix("computed_object_owner")).await;
        let fixture = create_backend_object_fixture(
            &backend,
            &needle,
            vec![serde_json::json!({"compatibility": needle})],
        )
        .await;
        let class_id = fixture.class.id();
        let fixture_collection_id = fixture.collection.id();
        let created_definition = backend
            .create_shared_computed_field(StorageSharedComputedFieldCreate::new(
                class_id,
                fixture_collection_id,
                owner.principal_id,
                StorageComputedFieldDefinitionInput::new(
                    Definition::new(
                        FieldKey::new("compatibility").unwrap(),
                        "Compatibility",
                        "",
                        Operation::FirstNonNull {
                            paths: vec![JsonPointer::new("/compatibility").unwrap()],
                        },
                        ResultType::String,
                        true,
                    )
                    .unwrap(),
                ),
                EventContext::user(owner.principal_id, None, None),
            ))
            .await
            .expect("computed-object compatibility definition should be inserted")
            .into_value()
            .into_parts()
            .0;
        let (options, passthrough) = parse_query_parameter_with_computed_filters_and_passthrough(
            &format!("computed.shared.compatibility__equals={needle}&sort=id"),
            &[],
        )
        .expect("computed compatibility query should parse");
        assert!(passthrough.is_empty());
        let visibility = StorageVisibility::new(
            principal_id(i32::MAX),
            true,
            None::<Vec<StorageAuthorizationPermission>>,
            None,
        );
        let page_limit = crate::pagination::effective_page_limit(&options)
            .expect("computed-object page limit should be valid");
        let execution_options = prepare_db_pagination::<crate::models::HubuumObject>(&options)
            .expect("computed-object execution query should be valid");
        let (rows, total, computed, _) = backend
            .list_computed_objects(StorageComputedObjectListQuery::new(
                class_id,
                None,
                StorageComputedObjectQueryOptions::try_new(options, execution_options, page_limit)
                    .expect("computed-object queries should be coherent"),
                StorageComputedObjectVisibility::storage(visibility),
                StorageComputedObjectProjection::All,
            ))
            .await
            .expect("certified backend should query computed objects")
            .into_parts();
        assert_eq!(total, Some(1));
        assert_eq!(rows.len(), 1);
        assert_eq!(computed.len(), 1);

        let enriched = backend
            .enrich_objects_with_computed(StorageComputedObjectEnrichmentQuery::new(
                vec![fixture.objects[0].clone()],
                None,
            ))
            .await
            .expect("certified backend should enrich objects with computed values");
        assert_eq!(enriched.len(), 1);
        backend
            .delete_shared_computed_field(StorageSharedComputedFieldDelete::new(
                class_id,
                fixture_collection_id,
                ComputedFieldDefinitionId::new(created_definition.metadata().id().id()).unwrap(),
                owner.principal_id,
                EventContext::system(),
            ))
            .await
            .expect("computed-object compatibility definition should be removed")
            .into_value();
        delete_backend_object_fixture(&backend, fixture).await;
        delete_backend_user(&backend, owner).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_computed_fields() {
    let _permit = postgres_permit().await;
    let definition = |key: &str| {
        StorageComputedFieldDefinitionInput::new(
            Definition::new(
                FieldKey::new(key).unwrap(),
                "Compatibility",
                "Backend compatibility definition",
                Operation::FirstNonNull {
                    paths: vec![JsonPointer::new("/compatibility").unwrap()],
                },
                ResultType::String,
                true,
            )
            .unwrap(),
        )
    };

    for environment in available_backend_environments() {
        let backend = environment.storage();
        let owner = create_backend_user(&backend, &prefix("computed_fields_owner")).await;
        let fixture =
            create_backend_object_fixture(&backend, &prefix("computed_fields"), Vec::new()).await;
        let class_id = fixture.class.id();
        let collection_id = fixture.collection.id();
        let owner_id = owner.principal_id;
        let event_context = EventContext::user(owner_id, None, None);
        let initial_state = backend
            .get_computed_field_state(class_id)
            .await
            .expect("certified backend should supply computed-field state");
        assert_eq!(initial_state.class_id(), class_id);

        let (shared, created_state) = backend
            .create_shared_computed_field(StorageSharedComputedFieldCreate::new(
                class_id,
                collection_id,
                owner_id,
                definition("compatibility_shared"),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create a shared computed field")
            .into_value()
            .into_parts();
        assert_eq!(shared.visibility(), StorageComputedFieldVisibility::Shared);
        assert!(created_state.evaluation_revision() > initial_state.evaluation_revision());

        let shared_rows = backend
            .list_shared_computed_fields(class_id)
            .await
            .expect("certified backend should list shared computed fields");
        assert!(
            shared_rows
                .iter()
                .any(|row| row.metadata().id() == shared.metadata().id())
        );

        let shared_id = ComputedFieldDefinitionId::new(shared.metadata().id().id())
            .expect("persisted computed-field definition id must be positive");
        let loaded = backend
            .get_computed_field(shared_id)
            .await
            .expect("certified backend should load a computed field");
        assert_eq!(loaded.key(), "compatibility_shared");

        let (updated_shared, _) = backend
            .update_shared_computed_field(StorageSharedComputedFieldUpdate::new(
                class_id,
                collection_id,
                shared_id,
                owner_id,
                StorageComputedFieldDefinitionPatch::new()
                    .with_label(Some("Updated compatibility".to_string())),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should update a shared computed field")
            .into_value()
            .into_parts();
        assert_eq!(updated_shared.label(), "Updated compatibility");

        let rebuild_state = backend
            .request_computed_field_rebuild(StorageComputedFieldRebuildRequest::new(
                class_id,
                collection_id,
                Some(owner_id),
            ))
            .await
            .expect("certified backend should request a computed-field rebuild");
        assert_eq!(rebuild_state.class_id(), class_id);
        let rebuild_task_id = rebuild_state
            .active_task_id()
            .expect("rebuild request should identify its task");
        let lease = match &environment {
            BackendTestEnvironment::Postgres { pool } => {
                let claim_token = uuid::Uuid::new_v4();
                hubuum_storage_postgres::test_support::assign_task_lease(
                    pool,
                    rebuild_task_id,
                    StorageTaskStatus::Validating,
                    claim_token,
                    chrono::Utc::now().naive_utc()
                        + chrono::Duration::try_minutes(1).expect("valid compatibility lease"),
                )
                .await
                .expect("compatibility rebuild should receive a live backend claim");
                StorageTaskLease::new(
                    rebuild_task_id,
                    StorageTaskClaimToken::new(claim_token.to_string()),
                )
            }
            BackendTestEnvironment::Memory { .. } => backend
                .claim_next_task(
                    StorageTaskLeaseDuration::from_milliseconds(60_000)
                        .expect("computed rebuild lease duration should be valid"),
                )
                .await
                .expect("memory rebuild task should be claimable")
                .expect("memory rebuild task should be queued")
                .lease()
                .clone(),
        };
        let rebuilt = backend
            .execute_computed_field_rebuild(lease)
            .await
            .expect("certified backend should execute a claimed computed-field rebuild");
        assert_eq!(rebuilt.status(), StorageTaskStatus::Succeeded);
        let ready_state = backend
            .get_computed_field_state(class_id)
            .await
            .expect("certified backend should expose the completed rebuild state");
        assert_eq!(ready_state.rebuild_status().as_str(), "ready");
        assert_eq!(ready_state.active_task_id(), None);

        let personal_outcome = backend
            .create_personal_computed_field(StoragePersonalComputedFieldCreate::new(
                class_id,
                owner_id,
                definition("compatibility_personal"),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create a personal computed field");
        assert!(personal_outcome.is_committed());
        let personal = personal_outcome.into_value();
        assert_eq!(
            personal.visibility(),
            StorageComputedFieldVisibility::Personal { owner_id }
        );

        let (personal_rows, total) = backend
            .list_personal_computed_fields(StoragePersonalComputedFieldListQuery::new(
                owner_id,
                Some(class_id),
                QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
                    .expect("contract query must be valid"),
            ))
            .await
            .expect("certified backend should list personal computed fields")
            .into_parts();
        assert_eq!(total, Some(1));
        assert_eq!(personal_rows.len(), 1);

        let personal_id = ComputedFieldDefinitionId::new(personal.metadata().id().id())
            .expect("persisted computed-field definition id must be positive");
        let updated_personal_outcome = backend
            .update_personal_computed_field(StoragePersonalComputedFieldUpdate::new(
                owner_id,
                personal_id,
                StorageComputedFieldDefinitionPatch::new()
                    .with_label(Some("Updated personal compatibility".to_string())),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should update a personal computed field");
        assert!(updated_personal_outcome.is_committed());
        let updated_personal = updated_personal_outcome.into_value();
        assert_eq!(updated_personal.label(), "Updated personal compatibility");

        let deleted_personal = backend
            .delete_personal_computed_field(StoragePersonalComputedFieldDelete::new(
                owner_id,
                personal_id,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should delete a personal computed field");
        assert!(deleted_personal.is_committed());

        let deleted_state = backend
            .delete_shared_computed_field(StorageSharedComputedFieldDelete::new(
                class_id,
                collection_id,
                shared_id,
                owner_id,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should delete a shared computed field")
            .into_value();
        assert_eq!(deleted_state.class_id(), class_id);
        if let BackendTestEnvironment::Postgres { pool } = &environment {
            hubuum_storage_postgres::test_support::delete_task(pool, rebuild_task_id)
                .await
                .expect("computed rebuild compatibility task should be removed");
        }
        delete_backend_object_fixture(&backend, fixture).await;
        delete_backend_user(&backend, owner).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_object_aggregates() {
    use hubuum_storage_core::{StorageObjectAggregateDimension, StorageObjectAggregateScalarField};

    let _permit = postgres_permit().await;
    let visibility = || {
        StorageVisibility::new(
            principal_id(i32::MAX),
            true,
            None::<Vec<StorageAuthorizationPermission>>,
            None,
        )
    };
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("object_aggregate"),
            vec![serde_json::json!({"compatibility": true})],
        )
        .await;
        let query = || {
            StorageObjectAggregateQuery::builder(
                StorageObjectAggregateTarget::new(
                    fixture.class.id(),
                    fixture.class.name().to_string(),
                    fixture.collection.id(),
                ),
                QueryOptions::new(
                    vec![
                        ParsedQueryParam::from_parts(
                            FilterField::ClassId,
                            SearchOperator::Equals { is_negated: false },
                            fixture.class.id().to_string(),
                        )
                        .unwrap(),
                        ParsedQueryParam::from_parts(
                            FilterField::CollectionId,
                            SearchOperator::Equals { is_negated: false },
                            fixture.collection.id().to_string(),
                        )
                        .unwrap(),
                    ],
                    Vec::new(),
                    Some(50),
                    None,
                    true,
                )
                .expect("contract query must be valid"),
                StorageObjectAggregateSpec::try_new(
                    [StorageObjectAggregateDimension::Scalar(
                        StorageObjectAggregateScalarField::Name,
                    )],
                    [],
                    StorageObjectAggregateSort::DimensionsAscending,
                )
                .expect("compatibility aggregate spec should be valid"),
                visibility(),
            )
            .required_permissions([
                StorageAuthorizationPermission::ReadObject,
                StorageAuthorizationPermission::ReadCollection,
            ])
            .page_limit(50)
            .cursor_max_encoded_bytes(4_096)
            .try_build()
            .expect("compatibility aggregate query should be valid")
        };
        let storage_page = backend
            .aggregate_objects(query(), StorageObjectAggregateAuthorization::Storage)
            .await
            .expect("certified backend should aggregate with storage authorization");
        let (rows, total, next_cursor) = storage_page.into_parts();
        assert_eq!(rows.len(), 1);
        assert_eq!(total, Some(1));
        assert!(next_cursor.is_none());

        let delegated_page = backend
            .aggregate_objects(
                query(),
                StorageObjectAggregateAuthorization::Delegated(&AllowAllObjectAggregateAuthorizer),
            )
            .await
            .expect("certified backend should aggregate with delegated authorization");
        let (rows, total, next_cursor) = delegated_page.into_parts();
        assert_eq!(rows.len(), 1);
        assert_eq!(total, Some(1));
        assert!(next_cursor.is_none());
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[actix_web::test]
async fn delegated_object_aggregation_keeps_one_snapshot_across_authorization_batches() {
    use hubuum_storage_core::{StorageObjectAggregateDimension, StorageObjectAggregateScalarField};

    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("delegated_object_aggregate_snapshot");
    let collection = crate::tests::create_collection_fixture(pool.get_ref(), &needle).await;
    let initial_objects = (0..3)
        .map(|index| NewHubuumObject {
            name: format!("{needle}_object_{index}"),
            collection_id: 0,
            hubuum_class_id: 0,
            data: serde_json::json!({"snapshot": true}),
            description: "delegated object aggregate snapshot object".to_string(),
        })
        .collect();
    let fixture = crate::tests::create_object_fixture(
        pool.get_ref(),
        collection,
        NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: None,
            validate_schema: Some(false),
            description: "delegated object aggregate snapshot class".to_string(),
        },
        initial_objects,
    )
    .await
    .expect("delegated aggregate snapshot fixture should be created");
    let query = StorageObjectAggregateQuery::builder(
        StorageObjectAggregateTarget::new(
            ClassId::new(fixture.class.id).expect("persisted class id must be positive"),
            fixture.class.name.clone(),
            collection_id(fixture.class.collection_id),
        ),
        QueryOptions::new(Vec::new(), Vec::new(), Some(50), None, true)
            .expect("snapshot aggregate query must be valid"),
        StorageObjectAggregateSpec::try_new(
            [StorageObjectAggregateDimension::Scalar(
                StorageObjectAggregateScalarField::Name,
            )],
            [],
            StorageObjectAggregateSort::DimensionsAscending,
        )
        .expect("snapshot aggregate spec must be valid"),
        StorageVisibility::new(
            principal_id(i32::MAX),
            true,
            None::<Vec<StorageAuthorizationPermission>>,
            None,
        ),
    )
    .required_permissions([
        StorageAuthorizationPermission::ReadObject,
        StorageAuthorizationPermission::ReadCollection,
    ])
    .page_limit(50)
    .cursor_max_encoded_bytes(4_096)
    .try_build()
    .expect("snapshot aggregate query must be valid");
    let authorizer = Arc::new(PausingObjectAggregateAuthorizer::new());
    let storage = StorageHandle::postgres(pool.get_ref().clone());
    let aggregate_task = tokio::spawn({
        let authorizer = Arc::clone(&authorizer);
        async move {
            storage
                .aggregate_objects(
                    query,
                    StorageObjectAggregateAuthorization::Delegated(authorizer.as_ref()),
                )
                .await
        }
    });

    tokio::time::timeout(
        Duration::from_secs(10),
        authorizer.first_batch_seen.notified(),
    )
    .await
    .expect("delegated authorization should receive its first candidate batch");
    NewHubuumObject {
        name: format!("{needle}_concurrent_object"),
        collection_id: fixture.class.collection_id,
        hubuum_class_id: fixture.class.id,
        data: serde_json::json!({"snapshot": false}),
        description: "concurrently inserted object".to_string(),
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("concurrent object should be committed while authorization is paused");
    authorizer.resume.notify_one();

    let page = aggregate_task
        .await
        .expect("delegated aggregate task should join")
        .expect("delegated aggregate should succeed");
    let (rows, total, next_cursor) = page.into_parts();
    assert_eq!(rows.len(), 3);
    assert_eq!(total, Some(3));
    assert!(next_cursor.is_none());

    fixture
        .cleanup()
        .await
        .expect("delegated aggregate snapshot fixture should be removed");
}
