use super::*;

async fn get_with_permission_backend(
    context: &TestContext,
    token: &str,
    backend: Arc<dyn PermissionBackend>,
    endpoint: &str,
) -> actix_web::dev::ServiceResponse {
    let app = test::init_service(
        App::new()
            .app_data(Data::new(context.pool.get_ref().clone()))
            .app_data(Data::new(AppContext::new(
                context.pool.get_ref().clone(),
                backend,
            )))
            .configure(crate::api::config),
    )
    .await;
    test::TestRequest::get()
        .insert_header((
            actix_web::http::header::AUTHORIZATION,
            format!("Bearer {token}"),
        ))
        .uri(endpoint)
        .send_request(&app)
        .await
        .map_into_boxed_body()
}

#[rstest::rstest]
#[tokio::test]
async fn non_pushdown_authorization_filters_objects_before_aggregation(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "external permission groups").await;
    let group = create_test_group(&test_context.pool).await;
    group
        .add_member_without_events(&test_context.pool, &test_context.normal_user)
        .await
        .unwrap();
    let backend = Arc::new(MockTreetopBackend::new());
    for object in fixture.objects.iter().take(2) {
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(object.id),
            attrs: ResourceAttrs::default(),
        });
    }

    let response = get_with_permission_backend(
        &test_context,
        &test_context.normal_token,
        backend.clone(),
        &format!(
            "/api/v1/classes/{}/object-groups?group_by=description",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
        Some("1")
    );
    let rows: Vec<serde_json::Value> = test::read_body_json(response).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["object_count"], 2);
    assert_eq!(rows[0]["dimensions"][0]["value"], "alpha");
    assert_eq!(backend.authorization_batch_sizes(), vec![2, 2, 1]);

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[cfg(feature = "integration-test-support")]
#[rstest::rstest]
#[tokio::test]
async fn non_pushdown_candidate_batches_are_bounded_by_serialized_size(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = test_context
        .object_fixture(
            "external byte bounded candidates",
            NewHubuumClass {
                collection_id: 0,
                name: test_context.scoped_name("external byte bounded candidate class"),
                description: "Byte-bounded candidate class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
            },
            (0..3)
                .map(|index| NewHubuumObject {
                    collection_id: 0,
                    hubuum_class_id: 0,
                    name: test_context.scoped_name(&format!("large candidate {index}")),
                    description: "same group".to_string(),
                    data: serde_json::json!({"payload": "x".repeat(2048)}),
                })
                .collect(),
        )
        .await
        .unwrap();
    let group = create_test_group(&test_context.pool).await;
    group
        .add_member_without_events(&test_context.pool, &test_context.normal_user)
        .await
        .unwrap();
    let backend = Arc::new(MockTreetopBackend::new());
    for object in &fixture.objects {
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(object.id),
            attrs: ResourceAttrs::default(),
        });
    }

    let response = get_with_permission_backend(
        &test_context,
        &test_context.normal_token,
        backend.clone(),
        &format!(
            "/api/v1/classes/{}/object-groups?group_by=description",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let rows: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(rows[0]["object_count"], 3);
    assert_eq!(backend.authorization_batch_sizes(), vec![1, 1, 1]);

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn non_pushdown_high_cardinality_groups_paginate_from_bounded_accumulator(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "external accumulator pagination").await;
    let group = create_test_group(&test_context.pool).await;
    group
        .add_member_without_events(&test_context.pool, &test_context.normal_user)
        .await
        .unwrap();
    let backend = Arc::new(MockTreetopBackend::new());
    for object in &fixture.objects {
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(object.id),
            attrs: ResourceAttrs::default(),
        });
    }

    let endpoint = format!(
        "/api/v1/classes/{}/object-groups?collection_id={}&group_by=name&limit=2",
        fixture.class.id, fixture.collection.collection.id
    );
    let mut cursor = None;
    let mut values = Vec::new();
    for _ in 0..3 {
        let response = get_with_permission_backend(
            &test_context,
            &test_context.normal_token,
            backend.clone(),
            &cursor
                .as_ref()
                .map(|cursor| format!("{endpoint}&cursor={cursor}"))
                .unwrap_or_else(|| endpoint.clone()),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
            Some("5")
        );
        cursor = header_value(&response, NEXT_CURSOR_HEADER);
        let rows: Vec<serde_json::Value> = test::read_body_json(response).await;
        values.extend(
            rows.into_iter()
                .map(|row| row["dimensions"][0]["value"].as_str().unwrap().to_string()),
        );
    }

    let mut expected = fixture
        .objects
        .iter()
        .map(|object| object.name.clone())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(values, expected);
    assert!(cursor.is_none());

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn non_pushdown_grouping_uses_the_authorized_object_snapshot(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "external authorization snapshot").await;
    let group = create_test_group(&test_context.pool).await;
    group
        .add_member_without_events(&test_context.pool, &test_context.normal_user)
        .await
        .unwrap();
    let authorized_object = fixture.objects[0].clone();
    let renamed = test_context.scoped_name("renamed after authorization input");
    let backend = Arc::new(MockTreetopBackend::new());
    backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadObject,
        resource_kind: ResourceKind::Object,
        resource_id: Some(authorized_object.id),
        attrs: ResourceAttrs {
            name: Some(authorized_object.name.clone()),
            ..Default::default()
        },
    });
    let pool = test_context.pool.clone();
    let renamed_for_hook = renamed.clone();
    backend.set_authorization_hook(move || async move {
        UpdateHubuumObject {
            name: Some(renamed_for_hook),
            collection_id: None,
            hubuum_class_id: None,
            data: None,
            description: None,
        }
        .update_without_events(&pool, authorized_object.id)
        .await
        .unwrap();
    });

    let response = get_with_permission_backend(
        &test_context,
        &test_context.normal_token,
        backend,
        &format!(
            "/api/v1/classes/{}/object-groups?group_by=name",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let rows: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["dimensions"][0]["value"], fixture.objects[0].name);
    assert_ne!(rows[0]["dimensions"][0]["value"], renamed);

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn non_pushdown_authorization_honors_permission_filters(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "external filtered permissions").await;
    let group = create_test_group(&test_context.pool).await;
    group
        .add_member_without_events(&test_context.pool, &test_context.normal_user)
        .await
        .unwrap();
    let backend = Arc::new(MockTreetopBackend::new());
    for object in fixture.objects.iter().take(2) {
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(object.id),
            attrs: ResourceAttrs::default(),
        });
    }
    backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::UpdateObject,
        resource_kind: ResourceKind::Object,
        resource_id: Some(fixture.objects[0].id),
        attrs: ResourceAttrs::default(),
    });

    let response = get_with_permission_backend(
        &test_context,
        &test_context.normal_token,
        backend,
        &format!(
            "/api/v1/classes/{}/object-groups?permissions=UpdateObject&group_by=description",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
        Some("1")
    );
    let rows: Vec<serde_json::Value> = test::read_body_json(response).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["object_count"], 1);

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[case(Permissions::ReadClass, ResourceKind::Class, true)]
#[case(Permissions::ReadCollection, ResourceKind::Collection, false)]
#[tokio::test]
async fn non_pushdown_parent_permission_filters_use_complete_resources(
    #[future(awt)] test_context: TestContext,
    #[case] filtered_permission: Permissions,
    #[case] filtered_resource_kind: ResourceKind,
    #[case] uses_class_resource: bool,
) {
    let fixture = fixture(&test_context, "external compatible permission resources").await;
    let group = create_test_group(&test_context.pool).await;
    group
        .add_member_without_events(&test_context.pool, &test_context.normal_user)
        .await
        .unwrap();
    let backend = Arc::new(MockTreetopBackend::new());
    for object in fixture.objects.iter().take(2) {
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(object.id),
            attrs: ResourceAttrs::default(),
        });
    }
    let (filtered_resource_id, resource_name) = if uses_class_resource {
        (fixture.class.id, fixture.class.name.clone())
    } else {
        (
            fixture.collection.collection.id,
            fixture.collection.collection.name.clone(),
        )
    };
    backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: filtered_permission,
        resource_kind: filtered_resource_kind,
        resource_id: Some(filtered_resource_id),
        attrs: ResourceAttrs {
            name: Some(resource_name),
            ..Default::default()
        },
    });

    let response = get_with_permission_backend(
            &test_context,
            &test_context.normal_token,
            backend.clone(),
            &format!(
                "/api/v1/classes/{}/object-groups?permissions={filtered_permission}&group_by=description",
                fixture.class.id
            ),
        )
        .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let rows: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["object_count"], 2);
    assert_eq!(backend.authorization_batch_sizes(), vec![4, 4, 2]);

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[case(Permissions::CreateClass, ResourceKind::Class)]
#[case(Permissions::CreateObject, ResourceKind::Object)]
#[tokio::test]
async fn non_pushdown_create_permission_filters_use_prospective_resources(
    #[future(awt)] test_context: TestContext,
    #[case] filtered_permission: Permissions,
    #[case] filtered_resource_kind: ResourceKind,
) {
    let fixture = fixture(&test_context, "external prospective permission resources").await;
    let group = create_test_group(&test_context.pool).await;
    group
        .add_member_without_events(&test_context.pool, &test_context.normal_user)
        .await
        .unwrap();
    let backend = Arc::new(MockTreetopBackend::new());
    for object in fixture.objects.iter().take(2) {
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(object.id),
            attrs: ResourceAttrs::default(),
        });
    }
    backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: filtered_permission,
        resource_kind: filtered_resource_kind,
        resource_id: Some(0),
        attrs: ResourceAttrs {
            collection_id: Some(fixture.collection.collection.id),
            class_id: (filtered_permission == Permissions::CreateObject)
                .then_some(fixture.class.id),
            ..Default::default()
        },
    });

    let response = get_with_permission_backend(
            &test_context,
            &test_context.normal_token,
            backend,
            &format!(
                "/api/v1/classes/{}/object-groups?permissions={filtered_permission}&group_by=description",
                fixture.class.id
            ),
        )
        .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let rows: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["object_count"], 2);

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[case("known")]
#[case("disabled")]
#[case("unknown")]
#[tokio::test]
async fn non_pushdown_computed_selectors_are_not_disclosed_without_visible_objects(
    #[future(awt)] test_context: TestContext,
    #[case] selector: &str,
) {
    let fixture = fixture(&test_context, "external computed selector visibility").await;
    for (key, enabled) in [("known", true), ("disabled", false)] {
        create_shared_definition(
            &test_context.pool,
            fixture.class.id,
            fixture.class.collection_id,
            test_context.admin_user.id,
            computed_definition(key, "/bucket", enabled),
            &EventContext::system(),
        )
        .await
        .unwrap();
    }
    let backend = Arc::new(MockTreetopBackend::new());

    let response = get_with_permission_backend(
        &test_context,
        &test_context.normal_token,
        backend,
        &format!(
            "/api/v1/classes/{}/object-groups?group_by=computed.shared.{selector}",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let rows: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert!(rows.is_empty());

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn non_pushdown_authorization_does_not_hold_the_computed_definition_lock(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "external computed definition lock").await;
    let group = create_test_group(&test_context.pool).await;
    group
        .add_member_without_events(&test_context.pool, &test_context.normal_user)
        .await
        .unwrap();
    let created = create_shared_definition(
        &test_context.pool,
        fixture.class.id,
        fixture.class.collection_id,
        test_context.admin_user.id,
        computed_definition("lock_probe", "/bucket", true),
        &EventContext::system(),
    )
    .await
    .unwrap();
    let backend = Arc::new(MockTreetopBackend::new());
    for object in &fixture.objects {
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(object.id),
            attrs: ResourceAttrs::default(),
        });
    }
    let pool = test_context.pool.clone();
    let class_id = fixture.class.id;
    let collection_id = fixture.class.collection_id;
    let actor_id = test_context.admin_user.id;
    let definition_id = created.definition.id;
    let definition_revision = created.definition.revision;
    backend.set_authorization_hook(move || async move {
        let context = EventContext::system();
        update_shared_definition(
            &pool,
            class_id,
            collection_id,
            definition_id,
            actor_id,
            ComputedFieldDefinitionPatch {
                expected_revision: definition_revision,
                key: None,
                label: Some("Updated while authorizing".to_string()),
                description: None,
                operation: None,
                result_type: None,
                enabled: None,
            },
            &context,
        )
        .await
        .unwrap();
    });

    let response = tokio::time::timeout(
        Duration::from_secs(5),
        get_with_permission_backend(
            &test_context,
            &test_context.normal_token,
            backend,
            &format!(
                "/api/v1/classes/{}/object-groups?group_by=computed.shared.lock_probe",
                fixture.class.id
            ),
        ),
    )
    .await
    .expect("external authorization must not wait on a held definition lock");
    let response = assert_response_status(response, StatusCode::OK).await;
    let rows: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(summed_count(&rows), fixture.objects.len() as i64);

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn non_pushdown_permission_filters_respect_token_scopes(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "external permission scopes").await;
    let group = create_test_group(&test_context.pool).await;
    group
        .add_member_without_events(&test_context.pool, &test_context.normal_user)
        .await
        .unwrap();
    let backend = Arc::new(MockTreetopBackend::new());
    for action in [Permissions::ReadObject, Permissions::UpdateObject] {
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action,
            resource_kind: ResourceKind::Object,
            resource_id: Some(fixture.objects[0].id),
            attrs: ResourceAttrs::default(),
        });
    }
    let token = scoped_token(
        &test_context.pool,
        test_context.normal_user.id,
        &[Permissions::ReadObject],
    )
    .await;

    let response = get_with_permission_backend(
        &test_context,
        &token,
        backend,
        &format!(
            "/api/v1/classes/{}/object-groups?permissions=UpdateObject&group_by=description",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
        Some("0")
    );
    let rows: Vec<serde_json::Value> = test::read_body_json(response).await;
    assert!(rows.is_empty());

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}
