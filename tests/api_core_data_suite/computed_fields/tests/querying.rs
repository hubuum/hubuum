#[rstest::rstest]
#[tokio::test]
async fn enriched_pagination_preserves_totals_and_cursors(
    #[future(awt)] test_context: TestContext,
) {
    let mut fixture = fixture(&test_context, "computed pagination").await;
    let second = NewHubuumObject {
        collection_id: fixture.class.collection_id,
        hubuum_class_id: fixture.class.id,
        name: test_context.scoped_name("computed page object"),
        description: "Second computed pagination object".to_string(),
        data: serde_json::json!({"manual": {"hostname": "second.example"}}),
    }
    .save_without_events(&test_context.pool)
    .await
    .unwrap();
    fixture.objects.push(second);
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
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?include=computed&sort=id&limit=1",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
        Some("2")
    );
    let cursor = header_value(&response, NEXT_CURSOR_HEADER).expect("next cursor");
    let first_page: Vec<serde_json::Value> = test::read_body_json(response).await;
    assert_eq!(first_page.len(), 1);
    assert!(first_page[0]["computed"]["shared"]["values"]["display_name"].is_string());

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?include=computed&sort=id&limit=1&cursor={cursor}",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
        Some("2")
    );
    let second_page: Vec<serde_json::Value> = test::read_body_json(response).await;
    assert_eq!(second_page.len(), 1);
    assert_ne!(first_page[0]["id"], second_page[0]["id"]);

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}
#[rstest::rstest]
#[case::string("string", serde_json::json!("Edge.EXAMPLE"), "__icontains", "edge.example")]
#[case::string_in_whitespace("string", serde_json::json!(" edge "), "__in", " edge ")]
#[case::number("number", serde_json::json!(12.5), "__between", "12,13")]
#[case::boolean("boolean", serde_json::json!(true), "", "true")]
#[case::object(
    "object",
    serde_json::json!({"role": "edge", "priority": 2}),
    "__contains",
    r#"{"role":"edge"}"#
)]
#[case::array(
    "array",
    serde_json::json!(["edge", "core"]),
    "__array_length",
    "2"
)]
#[tokio::test]
async fn class_object_list_filters_by_computed_result_type(
    #[future(awt)] test_context: TestContext,
    #[case] result_type: &str,
    #[case] filter_value: serde_json::Value,
    #[case] operator: &str,
    #[case] expected: &str,
) {
    let mut fixture = fixture(&test_context, "computed filtering").await;
    let matching = NewHubuumObject {
        collection_id: fixture.class.collection_id,
        hubuum_class_id: fixture.class.id,
        name: test_context.scoped_name("computed filter match"),
        description: "Computed filtering match".to_string(),
        data: serde_json::json!({"filter_value": filter_value}),
    }
    .save_without_events(&test_context.pool)
    .await
    .unwrap();
    fixture.objects.push(matching.clone());
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        serde_json::json!({
            "key": "filter_value",
            "label": "Filter value",
            "operation": {"type": "first_non_null", "paths": ["/filter_value"]},
            "result_type": result_type
        }),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    let encoded =
        percent_encoding::utf8_percent_encode(expected, percent_encoding::NON_ALPHANUMERIC);
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?computed.shared.filter_value{operator}={encoded}",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
        Some("1")
    );
    let objects: Vec<serde_json::Value> = test::read_body_json(response).await;
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0]["id"], matching.id);
    assert!(objects[0].get("computed").is_none());

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn computed_filter_without_computed_response_skips_page_enrichment(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "computed filter raw response").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;

    let endpoint = format!(
        "/api/v1/classes/{}/?computed.shared.display_name__icontains=inventory&sort=id&include_total=false",
        fixture.class.id
    );
    let (raw_response, raw_queries) = capture_queries(get_request(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint,
    ))
    .await;
    let raw_response = assert_response_status(raw_response, StatusCode::OK).await;
    let raw_objects: Vec<serde_json::Value> = test::read_body_json(raw_response).await;
    assert_eq!(raw_objects.len(), 1);
    assert!(raw_objects[0].get("computed").is_none());

    let (enriched_response, enriched_queries) = capture_queries(get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("{endpoint}&include=computed"),
    ))
    .await;
    let enriched_response = assert_response_status(enriched_response, StatusCode::OK).await;
    let enriched_objects: Vec<serde_json::Value> = test::read_body_json(enriched_response).await;
    assert_eq!(enriched_objects.len(), 1);
    assert!(enriched_objects[0].get("computed").is_some());
    assert_eq!(
        enriched_queries.domain_queries(),
        raw_queries.domain_queries() + 1
    );

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn computed_filter_preserves_double_underscores_in_definition_keys(
    #[future(awt)] test_context: TestContext,
) {
    let mut fixture = fixture(&test_context, "computed double underscore filter").await;
    let matching = NewHubuumObject {
        collection_id: fixture.class.collection_id,
        hubuum_class_id: fixture.class.id,
        name: test_context.scoped_name("computed double underscore match"),
        description: "Computed double underscore filtering match".to_string(),
        data: serde_json::json!({"filter_value": "edge"}),
    }
    .save_without_events(&test_context.pool)
    .await
    .unwrap();
    fixture.objects.push(matching.clone());
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        serde_json::json!({
            "key": "filter__value",
            "label": "Filter value",
            "operation": {"type": "first_non_null", "paths": ["/filter_value"]},
            "result_type": "string"
        }),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?computed.shared.filter__value=edge",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let objects: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0]["id"], matching.id);

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn computed_query_replaces_conflicting_class_filters_with_path_class(
    #[future(awt)] test_context: TestContext,
) {
    let path_fixture = fixture(&test_context, "computed path class").await;
    let other_class = NewHubuumClass {
        collection_id: path_fixture.class.collection_id,
        name: test_context.scoped_name("computed conflicting class"),
        description: "Conflicting computed query class".to_string(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&test_context.pool)
    .await
    .unwrap();
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", path_fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?class_id={}&sort=computed.shared.display_name",
            path_fixture.class.id, other_class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let objects: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0]["id"], path_fixture.objects[0].id);

    finish_active_rebuild(&test_context, path_fixture.class.id).await;
    other_class
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
    path_fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn shared_computed_query_is_stable_across_cursor_pages(
    #[future(awt)] test_context: TestContext,
) {
    let mut fixture = fixture(&test_context, "shared computed sorting").await;
    for hostname in ["aardvark.example", "zulu.example"] {
        fixture.objects.push(
            NewHubuumObject {
                collection_id: fixture.class.collection_id,
                hubuum_class_id: fixture.class.id,
                name: test_context.scoped_name(hostname),
                description: "Computed sorting object".to_string(),
                data: serde_json::json!({"manual": {"hostname": hostname}}),
            }
            .save_without_events(&test_context.pool)
            .await
            .unwrap(),
        );
    }
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    let expected = [
        fixture.objects[1].id,
        fixture.objects[0].id,
        fixture.objects[2].id,
    ];
    let mut cursor = None;
    for (index, expected_id) in expected.into_iter().enumerate() {
        let cursor_query = cursor
            .as_ref()
            .map_or_else(String::new, |cursor| format!("&cursor={cursor}"));
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/?include=computed&sort=computed.shared.display_name&limit=1{cursor_query}",
                fixture.class.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
            Some("3")
        );
        cursor = header_value(&response, NEXT_CURSOR_HEADER);
        let page: Vec<serde_json::Value> = test::read_body_json(response).await;
        assert_eq!(page.len(), 1);
        assert_eq!(page[0]["id"], expected_id);
        assert!(page[0]["computed"]["shared"]["values"]["display_name"].is_string());
        assert_eq!(cursor.is_some(), index < 2);
    }

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?sort=computed.shared.display_name.desc&limit=3",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let objects: Vec<serde_json::Value> = test::read_body_json(response).await;
    assert_eq!(
        objects
            .iter()
            .map(|object| object["id"].as_i64().unwrap() as i32)
            .collect::<Vec<_>>(),
        vec![
            fixture.objects[2].id,
            fixture.objects[0].id,
            fixture.objects[1].id
        ]
    );
    assert!(
        objects
            .iter()
            .all(|object| object.get("computed").is_none())
    );

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[case::ascending_key("asc", "computed.shared.asc")]
#[case::descending_key("desc", "-computed.shared.desc")]
#[tokio::test]
async fn computed_query_accepts_keys_named_after_directions(
    #[future(awt)] test_context: TestContext,
    #[case] key: &str,
    #[case] sort: &str,
) {
    let fixture = fixture(&test_context, "computed direction key sorting").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition(key),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/?include=computed&sort={sort}",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let objects: Vec<serde_json::Value> = test::read_body_json(response).await;

    assert_eq!(objects.len(), 1);
    assert!(objects[0]["computed"]["shared"]["values"][key].is_string());

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}
use super::*;
