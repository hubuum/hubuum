use super::*;
use crate::models::{HubuumObjectID, TokenResourceScope};
use crate::tests::resource_scoped_token;

async fn field_fixture(context: &TestContext, count: usize) -> crate::tests::ObjectFixture {
    context.object_fixture("field inventory", NewHubuumClass {
        collection_id:0, name:context.scoped_name("field inventory"), description:String::new(), json_schema:None, validate_schema:Some(false),
    }, (0..count).map(|index| NewHubuumObject {
        collection_id:0, hubuum_class_id:0, name:context.scoped_name(&format!("sample {index}")), description:String::new(),
        data:serde_json::json!({format!("visible_{index}"):index,"items":[{"value":null},{"value":index}]}),
    }).collect()).await.unwrap()
}

fn endpoint(fixture: &crate::tests::ObjectFixture) -> String {
    let name = percent_encoding::utf8_percent_encode(
        &fixture.class.name,
        percent_encoding::NON_ALPHANUMERIC,
    );
    format!("/api/v1/classes/by-name/{name}/fields")
}

#[rstest::rstest]
#[tokio::test]
async fn fields_sample_is_deterministic_and_does_not_count_whole_class(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = field_fixture(&test_context, 3).await;
    let (response, queries) = capture_queries(get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("{}?limit=1", endpoint(&fixture)),
    ))
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, "Cache-Control").as_deref(),
        Some("private, no-store")
    );
    let result: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(result["sampled_objects"], 1);
    assert_eq!(result["truncation"], serde_json::json!(["sample_limit"]));
    let names = result["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|field| field["field"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["data.items[*].value", "data.visible_0"]);
    assert!(
        !queries
            .query_counts()
            .keys()
            .any(|sql| sql.to_lowercase().contains("count(") && sql.contains("hubuumobject")),
        "{queries:?}"
    );
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn empty_class_keeps_all_enabled_shared_and_owner_personal_selectors(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = field_fixture(&test_context, 0).await;
    create_shared_definition(
        &test_context.pool,
        typed_class_id(fixture.class.id),
        typed_collection_id(fixture.class.collection_id),
        typed_principal_id(test_context.admin_user.id),
        definition_request("shared"),
        &EventContext::system(),
    )
    .await
    .unwrap();
    for index in 0..16 {
        create_personal_definition(
            &test_context.pool,
            typed_class_id(fixture.class.id),
            typed_principal_id(test_context.admin_user.id),
            definition_request(&format!("mine_{index}")),
            &EventContext::system(),
        )
        .await
        .unwrap();
    }
    create_personal_definition(
        &test_context.pool,
        typed_class_id(fixture.class.id),
        typed_principal_id(test_context.normal_user.id),
        definition_request("other_owner"),
        &EventContext::system(),
    )
    .await
    .unwrap();
    let mut disabled = definition_request("disabled");
    disabled.enabled = false;
    create_personal_definition(
        &test_context.pool,
        typed_class_id(fixture.class.id),
        typed_principal_id(test_context.normal_user.id),
        disabled,
        &EventContext::system(),
    )
    .await
    .unwrap();
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint(&fixture),
    )
    .await;
    let result: serde_json::Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(result["sampled_objects"], 0);
    let fields = result["fields"].as_array().unwrap();
    assert_eq!(fields.len(), 17);
    assert!(
        fields
            .iter()
            .all(|field| field["observation_count"] == 0 && field["example"].is_null())
    );
    assert!(
        fields
            .iter()
            .all(|field| !field["field"].as_str().unwrap().contains("other_owner"))
    );
    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[case::no_object_permission(vec![Permissions::ReadClass, Permissions::ReadCollection])]
#[case::no_collection_permission(vec![Permissions::ReadClass, Permissions::ReadObject])]
#[tokio::test]
async fn permission_scope_hides_object_observations(
    #[future(awt)] test_context: TestContext,
    #[case] permissions: Vec<Permissions>,
) {
    let fixture = field_fixture(&test_context, 1).await;
    let token = scoped_token(&test_context.pool, test_context.admin_user.id, &permissions).await;
    let response = get_request(&test_context.pool, &token, &endpoint(&fixture)).await;
    let result: serde_json::Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(result["sampled_objects"], 0);
    assert_eq!(result["fields"], serde_json::json!([]));
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn class_resource_scope_rejects_another_class(#[future(awt)] test_context: TestContext) {
    let fixture = field_fixture(&test_context, 1).await;
    let token = resource_scoped_token(
        &test_context.pool,
        test_context.admin_user.id,
        vec![TokenResourceScope::Object(
            HubuumObjectID::new(fixture.objects[0].id).unwrap(),
        )],
    )
    .await;
    let response = get_request(&test_context.pool, &token, &endpoint(&fixture)).await;
    assert_response_status(response, StatusCode::FORBIDDEN).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[case::no_grant(false, 0)]
#[case::one_object(true, 1)]
#[tokio::test]
async fn external_policy_filters_before_field_observation(
    #[future(awt)] test_context: TestContext,
    #[case] object_grant: bool,
    #[case] expected: usize,
) {
    let fixture = field_fixture(&test_context, 3).await;
    let group = create_test_group(&test_context.pool).await;
    group
        .add_member_without_events(&test_context.pool, &test_context.normal_user)
        .await
        .unwrap();
    let backend = Arc::new(MockTreetopBackend::new());
    for (action, kind, id) in [
        (
            Permissions::ReadClass,
            ResourceKind::Class,
            fixture.class.id,
        ),
        (
            Permissions::ReadCollection,
            ResourceKind::Collection,
            fixture.class.collection_id,
        ),
    ] {
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action,
            resource_kind: kind,
            resource_id: Some(id),
            attrs: ResourceFields::default(),
        });
    }
    if object_grant {
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(fixture.objects[1].id),
            attrs: ResourceFields::default(),
        });
    }
    let response = get_request_with_permission_backend(
        &test_context.pool,
        &test_context.normal_token,
        &endpoint(&fixture),
        backend,
    )
    .await;
    let result: serde_json::Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(result["sampled_objects"], expected);
    let names = result["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|field| field["field"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        if object_grant {
            vec!["data.items[*].value", "data.visible_1"]
        } else {
            vec![]
        }
    );
    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[case("limit=0")]
#[case("limit=251")]
#[case("depth=33")]
#[case("containers=invalid")]
#[case("include_total=true")]
#[tokio::test]
async fn field_discovery_rejects_invalid_options(
    #[future(awt)] test_context: TestContext,
    #[case] query: &str,
) {
    let fixture = field_fixture(&test_context, 0).await;
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("{}?{query}", endpoint(&fixture)),
    )
    .await;
    assert_response_status(response, StatusCode::BAD_REQUEST).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[case::enabled(true)]
#[case::disabled(false)]
#[tokio::test]
async fn computed_error_values_and_disabled_selectors_match_cli(
    #[future(awt)] test_context: TestContext,
    #[case] enabled: bool,
) {
    let fixture = field_fixture(&test_context, 1).await;
    let mut definition = definition_request("wrong_type");
    definition.enabled = enabled;
    definition.operation = serde_json::json!({"type":"first_non_null", "paths":["/visible_0"]});
    create_personal_definition(
        &test_context.pool,
        typed_class_id(fixture.class.id),
        typed_principal_id(test_context.admin_user.id),
        definition,
        &EventContext::system(),
    )
    .await
    .unwrap();
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint(&fixture),
    )
    .await;
    let result: serde_json::Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    let computed = result["fields"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|field| field["source"] == "personal_computed")
        .collect::<Vec<_>>();
    if enabled {
        assert_eq!(computed.len(), 1);
        assert_eq!(computed[0]["types"], serde_json::json!({"null":1}));
        assert_eq!(computed[0]["error_count"], 1);
        assert_eq!(computed[0]["example"], "null");
    } else {
        assert!(computed.is_empty());
    }
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn external_policy_scan_stops_at_candidate_budget(#[future(awt)] test_context: TestContext) {
    let fixture = field_fixture(&test_context, 0).await;
    with_connection(&test_context.pool, async |connection| {
        diesel::sql_query("INSERT INTO hubuumobject (name, collection_id, hubuum_class_id, description, data) SELECT 'hidden-' || n, $1, $2, '', '{\"secret\":true}'::jsonb FROM generate_series(1, 1025) n")
            .bind::<diesel::sql_types::Integer,_>(fixture.class.collection_id)
            .bind::<diesel::sql_types::Integer,_>(fixture.class.id)
            .execute(connection).await
    }).await.unwrap();
    let group = create_test_group(&test_context.pool).await;
    group
        .add_member_without_events(&test_context.pool, &test_context.normal_user)
        .await
        .unwrap();
    let backend = Arc::new(MockTreetopBackend::new());
    backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadClass,
        resource_kind: ResourceKind::Class,
        resource_id: Some(fixture.class.id),
        attrs: ResourceFields::default(),
    });
    let response = get_request_with_permission_backend(
        &test_context.pool,
        &test_context.normal_token,
        &endpoint(&fixture),
        backend.clone(),
    )
    .await;
    let result: serde_json::Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(result["sampled_objects"], 0);
    assert_eq!(result["fields"], serde_json::json!([]));
    assert_eq!(result["truncation"], serde_json::json!(["candidate_limit"]));
    // One class check, then two permissions for each of 1,024 candidates.
    assert_eq!(
        backend.authorization_batch_sizes().iter().sum::<usize>(),
        1 + 2 * 1024
    );
    with_connection(&test_context.pool, async |connection| {
        diesel::delete(
            crate::schema::hubuumobject::table
                .filter(crate::schema::hubuumobject::hubuum_class_id.eq(fixture.class.id)),
        )
        .execute(connection)
        .await
    })
    .await
    .unwrap();
    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}
