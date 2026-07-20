use super::*;

#[rstest::rstest]
#[tokio::test]
async fn route_class_constraint_overrides_a_conflicting_filter(
    #[future(awt)] test_context: TestContext,
) {
    let route_fixture = fixture(&test_context, "route class constraint").await;
    let other_fixture = fixture(&test_context, "conflicting class filter").await;
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/object-aggregates?class_id={}&group_by=name",
            route_fixture.class.id, other_fixture.class.id
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

    route_fixture.cleanup().await.unwrap();
    other_fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn standard_object_list_contract_is_unchanged(#[future(awt)] test_context: TestContext) {
    let fixture = fixture(&test_context, "unchanged object list").await;
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/", fixture.class.id),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let objects: Vec<HubuumObject> = test::read_body_json(response).await;

    assert_eq!(objects.len(), fixture.objects.len());

    fixture.cleanup().await.unwrap();
}
