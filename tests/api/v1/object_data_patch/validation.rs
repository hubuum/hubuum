use super::*;

#[rstest]
#[actix_web::test]
async fn class_mismatch_returns_not_found(#[future(awt)] test_context: TestContext) {
    let classes = create_test_classes(&test_context, "JSON Patch class mismatch").await;
    let object = NewHubuumObject {
        collection_id: classes[0].collection_id,
        hubuum_class_id: classes[0].id,
        name: test_context.scoped_name("JSON Patch mismatch object"),
        description: "JSON Patch mismatch object".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&test_context.pool)
    .await
    .unwrap();

    let response = patch_request_with_content_type(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_endpoint(classes[1].id, object.id),
        serde_json::json!([]),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;

    assert_response_status(response, StatusCode::NOT_FOUND).await;
    crate::tests::cleanup_test_classes(&classes).await;
}

#[rstest]
#[actix_web::test]
async fn missing_object_returns_not_found(#[future(awt)] test_context: TestContext) {
    let fixture =
        object_fixture(&test_context, "missing patch object", serde_json::json!({})).await;

    let response = patch_request_with_content_type(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_endpoint(fixture.class.id, i32::MAX),
        serde_json::json!([]),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;

    assert_response_status(response, StatusCode::NOT_FOUND).await;
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[actix_web::test]
async fn missing_authentication_returns_unauthorized(#[future(awt)] test_context: TestContext) {
    let fixture = object_fixture(
        &test_context,
        "unauthenticated patch",
        serde_json::json!({}),
    )
    .await;

    let response = patch_request_with_content_type(
        &test_context.pool,
        "not-a-valid-token",
        &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
        serde_json::json!([]),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;

    assert_response_status(response, StatusCode::UNAUTHORIZED).await;
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[actix_web::test]
async fn insufficient_permission_returns_forbidden(#[future(awt)] test_context: TestContext) {
    let fixture = object_fixture(&test_context, "forbidden patch", serde_json::json!({})).await;

    let response = patch_request_with_content_type(
        &test_context.pool,
        &test_context.normal_token,
        &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
        serde_json::json!([]),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;

    assert_response_status(response, StatusCode::FORBIDDEN).await;
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[actix_web::test]
async fn malformed_json_returns_bad_request(#[future(awt)] test_context: TestContext) {
    let fixture = object_fixture(&test_context, "malformed patch", serde_json::json!({})).await;

    let response = patch_request_with_raw_body(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
        br#"[{"op":]"#.as_slice(),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;

    assert_response_status(response, StatusCode::BAD_REQUEST).await;
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[actix_web::test]
async fn invalid_patch_structure_returns_bad_request(#[future(awt)] test_context: TestContext) {
    let fixture = object_fixture(&test_context, "invalid patch", serde_json::json!({})).await;

    let response = patch_request_with_content_type(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
        serde_json::json!({"op": "add", "path": "/value", "value": 1}),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;

    assert_response_status(response, StatusCode::BAD_REQUEST).await;
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[actix_web::test]
async fn incorrect_content_type_returns_unsupported_media_type(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = object_fixture(
        &test_context,
        "incorrect content type",
        serde_json::json!({}),
    )
    .await;

    let response = patch_request_with_content_type(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
        serde_json::json!([]),
        "application/json",
    )
    .await;

    assert_response_status(response, StatusCode::UNSUPPORTED_MEDIA_TYPE).await;
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[actix_web::test]
async fn oversized_patch_returns_payload_too_large(#[future(awt)] test_context: TestContext) {
    let fixture = object_fixture(&test_context, "oversized patch", serde_json::json!({})).await;
    let body = vec![b' '; 2_097_153];

    let response = patch_request_with_raw_body(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
        body,
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;

    assert_response_status(response, StatusCode::PAYLOAD_TOO_LARGE).await;
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[actix_web::test]
async fn patch_result_larger_than_object_data_limit_returns_payload_too_large(
    #[future(awt)] test_context: TestContext,
) {
    let blob = "x".repeat(MAX_OBJECT_DATA_PATCH_BYTES / 2 + 1);
    let fixture = object_fixture(
        &test_context,
        "oversized patch result",
        serde_json::json!({"blob": blob}),
    )
    .await;

    let response = patch_request_with_content_type(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
        serde_json::json!([
            {"op": "copy", "from": "/blob", "path": "/copy"}
        ]),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;

    assert_response_status(response, StatusCode::PAYLOAD_TOO_LARGE).await;
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[case::nul_string(br#"[{"op":"add","path":"/invalid","value":"\u0000"}]"#)]
#[case::nul_key(br#"[{"op":"add","path":"/invalid","value":{"\u0000":true}}]"#)]
#[case::numeric_out_of_range(br#"[{"op":"add","path":"/invalid","value":1e131072}]"#)]
#[actix_web::test]
async fn postgres_jsonb_incompatible_patch_result_returns_bad_request(
    #[case] patch: &'static [u8],
    #[future(awt)] test_context: TestContext,
) {
    let fixture = object_fixture(
        &test_context,
        "PostgreSQL JSONB incompatible patch result",
        serde_json::json!({}),
    )
    .await;
    let object = fixture.objects[0].clone();

    let response = patch_request_with_raw_body(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_endpoint(fixture.class.id, object.id),
        patch,
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;

    assert_response_status(response, StatusCode::BAD_REQUEST).await;
    assert_eq!(current_object(&test_context, object.id).await, object);
    fixture.cleanup().await.unwrap();
}
