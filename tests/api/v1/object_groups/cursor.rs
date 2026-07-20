use super::*;

#[rstest::rstest]
#[tokio::test]
async fn count_sort_cursor_is_deterministic_across_duplicate_counts(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "count cursor").await;
    let endpoint = format!(
        "/api/v1/classes/{}/object-groups?group_by=description&sort=object_count.desc&limit=1",
        fixture.class.id
    );
    let mut cursor = None;
    let mut values = Vec::new();
    for _ in 0..3 {
        let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &cursor
                .as_ref()
                .map(|cursor| format!("{endpoint}&cursor={cursor}"))
                .unwrap_or_else(|| endpoint.clone()),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            header_value(&response, TOTAL_COUNT_HEADER).as_deref(),
            Some("3")
        );
        cursor = header_value(&response, NEXT_CURSOR_HEADER);
        let rows: Vec<serde_json::Value> = test::read_body_json(response).await;
        assert_eq!(rows.len(), 1);
        values.push(rows[0]["dimensions"][0]["value"].clone());
    }

    assert_eq!(
        values,
        vec![
            serde_json::json!("alpha"),
            serde_json::json!("beta"),
            serde_json::json!("gamma")
        ]
    );
    assert!(cursor.is_none());

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn oversized_group_key_is_rejected_before_a_cursor_is_emitted(
    #[future(awt)] test_context: TestContext,
) {
    let large_value = format!("a{}", "x".repeat(MAX_OBJECT_GROUP_CURSOR_LENGTH));
    let fixture = test_context
        .object_fixture(
            "oversized group cursor",
            NewHubuumClass {
                collection_id: 0,
                name: test_context.scoped_name("oversized group cursor class"),
                description: "Oversized group cursor".to_string(),
                json_schema: None,
                validate_schema: Some(false),
            },
            vec![
                NewHubuumObject {
                    collection_id: 0,
                    hubuum_class_id: 0,
                    name: test_context.scoped_name("oversized group cursor object"),
                    description: "Oversized".to_string(),
                    data: serde_json::json!({"group_key": large_value}),
                },
                NewHubuumObject {
                    collection_id: 0,
                    hubuum_class_id: 0,
                    name: test_context.scoped_name("small group cursor object"),
                    description: "Small".to_string(),
                    data: serde_json::json!({"group_key": "z"}),
                },
            ],
        )
        .await
        .unwrap();
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/object-groups?group_by=json_data.group_key&limit=1",
            fixture.class.id
        ),
    )
    .await;

    assert_response_status(response, StatusCode::PAYLOAD_TOO_LARGE).await;

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn include_total_false_omits_group_cardinality_header(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "skip total").await;
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/object-groups?group_by=description&include_total=false",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;

    assert!(header_value(&response, TOTAL_COUNT_HEADER).is_none());

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn empty_class_returns_empty_group_page(#[future(awt)] test_context: TestContext) {
    let fixture = test_context
        .object_fixture(
            "empty groups",
            NewHubuumClass {
                collection_id: 0,
                name: test_context.scoped_name("empty group class"),
                description: "Empty".to_string(),
                json_schema: None,
                validate_schema: Some(false),
            },
            Vec::new(),
        )
        .await
        .unwrap();
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/object-groups?group_by=name",
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
}

#[rstest::rstest]
#[tokio::test]
async fn empty_group_page_rejects_a_malformed_cursor(#[future(awt)] test_context: TestContext) {
    let fixture = test_context
        .object_fixture(
            "empty malformed group cursor",
            NewHubuumClass {
                collection_id: 0,
                name: test_context.scoped_name("empty malformed group cursor class"),
                description: "Empty".to_string(),
                json_schema: None,
                validate_schema: Some(false),
            },
            Vec::new(),
        )
        .await
        .unwrap();
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/object-groups?group_by=name&cursor=not-a-cursor",
            fixture.class.id
        ),
    )
    .await;

    assert_response_status(response, StatusCode::BAD_REQUEST).await;

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn empty_group_page_rejects_structurally_invalid_cursor(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "invalid cursor ordering values").await;
    let cursor = encoded_group_cursor(serde_json::json!([null]), 1);
    let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/object-groups?name__equals=no-such-object&group_by=name&cursor={cursor}",
                fixture.class.id
            ),
        )
        .await;

    assert_response_status(response, StatusCode::BAD_REQUEST).await;

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn empty_group_page_rejects_a_cursor_for_different_dimensions(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "mismatched empty group cursor").await;
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/object-groups?group_by=description&limit=1",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let cursor = header_value(&response, NEXT_CURSOR_HEADER).unwrap();
    let response = get_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!(
                "/api/v1/classes/{}/object-groups?name__equals=no-such-object&group_by=name&cursor={cursor}",
                fixture.class.id
            ),
        )
        .await;

    assert_response_status(response, StatusCode::BAD_REQUEST).await;

    fixture.cleanup().await.unwrap();
}
