#[rstest::rstest]
#[case::existing("display_name")]
#[case::missing("hidden_definition")]
#[tokio::test]
async fn computed_query_keys_are_hidden_without_full_object_list_visibility(
    #[future(awt)] test_context: TestContext,
    #[case] key: &str,
) {
    let fixture = fixture(&test_context, "computed sort key visibility").await;
    let group = grant_normal_user(&test_context, &fixture, &[Permissions::ReadObject]).await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    let response = get_request(
        &test_context.pool,
        &test_context.normal_token,
        &format!(
            "/api/v1/classes/{}/?sort=computed.shared.{key}",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
        Some("0")
    );
    let objects: Vec<serde_json::Value> = test::read_body_json(response).await;
    assert!(objects.is_empty());

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}
#[rstest::rstest]
#[tokio::test]
async fn computed_query_honors_object_specific_treetop_visibility(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "computed object policy visibility").await;
    let group = create_test_group(&test_context.pool).await;
    group
        .add_member_without_events(&test_context.pool, &test_context.normal_user)
        .await
        .unwrap();
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;

    let backend = Arc::new(MockTreetopBackend::new());
    backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadObject,
        resource_kind: ResourceKind::Object,
        resource_id: Some(fixture.objects[0].id),
        attrs: ResourceAttrs::default(),
    });
    let response = get_request_with_permission_backend(
        &test_context.pool,
        &test_context.normal_token,
        &format!(
            "/api/v1/classes/{}/?sort=computed.shared.display_name",
            fixture.class.id
        ),
        backend,
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let objects: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0]["id"], fixture.objects[0].id);

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[case::existing("display_name")]
#[case::missing("hidden_definition")]
#[tokio::test]
async fn computed_query_keys_are_hidden_when_only_the_synthetic_object_is_visible(
    #[future(awt)] test_context: TestContext,
    #[case] key: &str,
) {
    let fixture = fixture(&test_context, "computed synthetic policy visibility").await;
    let group = create_test_group(&test_context.pool).await;
    group
        .add_member_without_events(&test_context.pool, &test_context.normal_user)
        .await
        .unwrap();
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;

    let backend = Arc::new(MockTreetopBackend::new());
    backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadObject,
        resource_kind: ResourceKind::Object,
        resource_id: Some(0),
        attrs: ResourceAttrs::default(),
    });
    let response = get_request_with_permission_backend(
        &test_context.pool,
        &test_context.normal_token,
        &format!(
            "/api/v1/classes/{}/?sort=computed.shared.{key}",
            fixture.class.id
        ),
        backend,
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let objects: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert!(objects.is_empty());

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[case::scope(ComputedQueryDenial::Scope)]
#[case::list_visibility(ComputedQueryDenial::ListVisibility)]
#[tokio::test]
async fn denied_computed_query_pages_omit_totals_when_disabled(
    #[future(awt)] test_context: TestContext,
    #[case] denial: ComputedQueryDenial,
) {
    let fixture = fixture(&test_context, "computed sort denied total").await;
    let token = match denial {
        ComputedQueryDenial::Scope => {
            scoped_token(
                &test_context.pool,
                test_context.admin_user.id,
                &[Permissions::ReadCollection],
            )
            .await
        }
        ComputedQueryDenial::ListVisibility => test_context.normal_token.clone(),
    };

    let response = get_request(
        &test_context.pool,
        &token,
        &format!(
            "/api/v1/classes/{}/?include_total=false&sort=computed.shared.hidden",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert!(header_value(&response, TOTAL_COUNT_HEADER).is_none());
    let objects: Vec<serde_json::Value> = test::read_body_json(response).await;
    assert!(objects.is_empty());

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn computed_query_rejects_more_than_two_explicit_sort_fields(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "computed sort field limit").await;

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?sort=computed.shared.first,computed.shared.second,name",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::BAD_REQUEST).await;
    let body: serde_json::Value = test::read_body_json(response).await;

    assert_eq!(
        body["message"],
        "Computed sorting supports at most 2 explicit sort fields per request"
    );

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn computed_filter_rejects_more_than_two_predicates_before_definition_resolution(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "computed filter count limit").await;
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?computed.shared.first=1&computed.shared.first__gte=0&computed.personal.second=2",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::BAD_REQUEST).await;
    let body: serde_json::Value = test::read_body_json(response).await;

    assert_eq!(
        body["message"],
        "Computed filtering supports at most 2 computed filter parameters per request"
    );

    fixture.cleanup().await.unwrap();
}
