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
        attrs: ResourceFields::default(),
    });
    let (response, queries) = capture_queries(get_request_with_permission_backend(
        &test_context.pool,
        &test_context.normal_token,
        &format!(
            "/api/v1/classes/{}/?sort=computed.shared.display_name",
            fixture.class.id
        ),
        backend.clone(),
    ))
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let objects: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0]["id"], fixture.objects[0].id);
    let object_queries = queries
        .query_counts()
        .keys()
        .filter(|sql| sql.contains("FROM \"hubuumobject\""))
        .collect::<Vec<_>>();
    assert_eq!(object_queries.len(), 1, "{queries:?}");
    assert!(object_queries[0].contains(" LIMIT "), "{queries:?}");
    // Personal-field visibility has one boundary check; the only real object
    // is then authorized once, with no SQL count or full authorized-ID query.
    assert_eq!(backend.authorization_batch_sizes(), vec![1, 1]);

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[case::existing_hidden("display_name", false, StatusCode::OK)]
#[case::missing_hidden("hidden_definition", false, StatusCode::OK)]
#[case::missing_visible("hidden_definition", true, StatusCode::BAD_REQUEST)]
#[tokio::test]
async fn computed_query_key_errors_require_real_object_visibility(
    #[future(awt)] test_context: TestContext,
    #[case] key: &str,
    #[case] visible: bool,
    #[case] expected_status: StatusCode,
    #[values(false, true)] include_total: bool,
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
    backend.add_prospective_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadObject,
        resource_kind: ResourceKind::Object,
        resource_id: None,
        attrs: ResourceFields::default(),
    });
    if visible {
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(fixture.objects[0].id),
            attrs: ResourceFields::default(),
        });
    }
    let response = get_request_with_permission_backend(
        &test_context.pool,
        &test_context.normal_token,
        &format!(
            "/api/v1/classes/{}/?sort=computed.shared.{key}&include_total={include_total}",
            fixture.class.id
        ),
        backend,
    )
    .await;
    let response = assert_response_status(response, expected_status).await;
    if expected_status == StatusCode::OK {
        assert_eq!(
            header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
            include_total.then_some("0")
        );
        let objects: Vec<serde_json::Value> = test::read_body_json(response).await;
        assert!(objects.is_empty());
    }

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
use super::*;
use crate::models::{ComputedFieldDefinition, TokenResourceScope};
use crate::tests::resource_scoped_token;

const PERSONAL_FIELDS_ENDPOINT: &str = "/api/v1/iam/me/computed-fields";

#[rstest::rstest]
#[case::permission_scope(false)]
#[case::resource_scope(true)]
#[tokio::test]
async fn personal_definition_list_respects_token_scope(
    #[future(awt)] test_context: TestContext,
    #[case] resource_scope: bool,
    #[values(false, true)] class_filter: bool,
) {
    let fixture = fixture(&test_context, "personal list token scope").await;
    create_personal_definition(
        &test_context.pool,
        typed_class_id(fixture.class.id),
        typed_principal_id(test_context.admin_user.id),
        definition_request("hidden"),
        &EventContext::system(),
    )
    .await
    .unwrap();
    let token = if resource_scope {
        resource_scoped_token(
            &test_context.pool,
            test_context.admin_user.id,
            vec![TokenResourceScope::Object(
                crate::models::HubuumObjectID::new(fixture.objects[0].id).unwrap(),
            )],
        )
        .await
    } else {
        scoped_token(
            &test_context.pool,
            test_context.admin_user.id,
            &[Permissions::ReadCollection],
        )
        .await
    };
    let endpoint = if class_filter {
        format!("{PERSONAL_FIELDS_ENDPOINT}?class_id={}", fixture.class.id)
    } else {
        PERSONAL_FIELDS_ENDPOINT.to_string()
    };

    let response = get_request(&test_context.pool, &token, &endpoint).await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
        Some("0")
    );
    let definitions: Vec<ComputedFieldDefinition> = test::read_body_json(response).await;
    assert!(definitions.is_empty());

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn personal_definition_list_requires_current_class_access(
    #[future(awt)] test_context: TestContext,
    #[values(false, true)] class_filter: bool,
) {
    let fixture = fixture(&test_context, "personal list revoked class").await;
    let group = grant_normal_user(&test_context, &fixture, &[Permissions::ReadClass]).await;
    create_personal_definition(
        &test_context.pool,
        typed_class_id(fixture.class.id),
        typed_principal_id(test_context.normal_user.id),
        definition_request("formerly_readable"),
        &EventContext::system(),
    )
    .await
    .unwrap();
    fixture
        .collection
        .collection
        .revoke_all_without_events(&test_context.pool, GroupID::new(group.id).unwrap())
        .await
        .unwrap();
    let endpoint = if class_filter {
        format!("{PERSONAL_FIELDS_ENDPOINT}?class_id={}", fixture.class.id)
    } else {
        PERSONAL_FIELDS_ENDPOINT.to_string()
    };

    let response = get_request(&test_context.pool, &test_context.normal_token, &endpoint).await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
        Some("0")
    );
    let definitions: Vec<ComputedFieldDefinition> = test::read_body_json(response).await;
    assert!(definitions.is_empty());

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[case::local(false)]
#[case::external(true)]
#[tokio::test]
async fn personal_definition_pages_exclude_hidden_rows_and_counts(
    #[future(awt)] test_context: TestContext,
    #[case] external: bool,
    #[values(false, true)] include_total: bool,
) {
    let visible = fixture(&test_context, "personal list visible class").await;
    let hidden = fixture(&test_context, "personal list hidden class").await;
    let group = grant_normal_user(&test_context, &visible, &[Permissions::ReadClass]).await;
    let mut expected_ids = Vec::new();
    for (index, class_id) in [
        hidden.class.id,
        visible.class.id,
        hidden.class.id,
        visible.class.id,
        hidden.class.id,
    ]
    .into_iter()
    .enumerate()
    {
        let definition = create_personal_definition(
            &test_context.pool,
            typed_class_id(class_id),
            typed_principal_id(test_context.normal_user.id),
            definition_request(&format!("field_{index}")),
            &EventContext::system(),
        )
        .await
        .unwrap();
        if class_id == visible.class.id {
            expected_ids.push(definition.id);
        }
    }
    let backend = Arc::new(MockTreetopBackend::new());
    backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadClass,
        resource_kind: ResourceKind::Class,
        resource_id: Some(visible.class.id),
        attrs: ResourceFields {
            collection_id: Some(visible.class.collection_id),
            name: Some(visible.class.name.clone()),
            ..ResourceFields::default()
        },
    });
    let base_endpoint =
        format!("{PERSONAL_FIELDS_ENDPOINT}?limit=1&sort=id&include_total={include_total}");
    let mut endpoint = base_endpoint.clone();
    let mut returned_ids = Vec::new();
    for page_index in 0..2 {
        let response = if external {
            get_request_with_permission_backend(
                &test_context.pool,
                &test_context.normal_token,
                &endpoint,
                backend.clone(),
            )
            .await
        } else {
            get_request(&test_context.pool, &test_context.normal_token, &endpoint).await
        };
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
            include_total.then_some("2")
        );
        let cursor = header_value(&response, NEXT_CURSOR_HEADER);
        assert_eq!(cursor.is_some(), page_index == 0);
        let definitions: Vec<ComputedFieldDefinition> = test::read_body_json(response).await;
        assert_eq!(definitions.len(), 1);
        returned_ids.extend(definitions.into_iter().map(|definition| definition.id));
        if let Some(cursor) = cursor {
            endpoint = format!("{base_endpoint}&cursor={cursor}");
        }
    }
    assert_eq!(returned_ids, expected_ids);

    hidden.cleanup().await.unwrap();
    visible.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn personal_definition_list_fails_closed_on_policy_error(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "personal list policy failure").await;
    create_personal_definition(
        &test_context.pool,
        typed_class_id(fixture.class.id),
        typed_principal_id(test_context.admin_user.id),
        definition_request("hidden"),
        &EventContext::system(),
    )
    .await
    .unwrap();
    let backend = Arc::new(MockTreetopBackend::new());
    backend.fail_authorization_after_calls(0);

    let response = get_request_with_permission_backend(
        &test_context.pool,
        &test_context.admin_token,
        PERSONAL_FIELDS_ENDPOINT,
        backend,
    )
    .await;
    assert_response_status(response, StatusCode::INTERNAL_SERVER_ERROR).await;

    fixture.cleanup().await.unwrap();
}
