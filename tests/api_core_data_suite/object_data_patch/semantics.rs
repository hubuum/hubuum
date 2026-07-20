use super::*;

#[rstest]
#[case::missing_member(serde_json::json!({"keep": true}))]
#[case::existing_member(serde_json::json!({
    "keep": true,
    "facts": {"source": "old", "hostname": "srv-01"}
}))]
#[actix_web::test]
async fn add_facts_creates_or_completely_replaces_the_member(
    #[case] initial_data: serde_json::Value,
    #[future(awt)] test_context: TestContext,
) {
    let fixture = object_fixture(&test_context, "patch add facts", initial_data).await;
    let object = &fixture.objects[0];

    let response = patch_request_with_content_type(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_endpoint(fixture.class.id, object.id),
        serde_json::json!([
            {"op": "add", "path": "/facts", "value": {"source": "inventory"}}
        ]),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let updated: HubuumObject = test::read_body_json(response).await;

    assert_eq!(
        updated.data["facts"],
        serde_json::json!({"source": "inventory"})
    );
    assert_eq!(updated.data["keep"], true);
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[actix_web::test]
async fn empty_path_replaces_the_complete_data_document(#[future(awt)] test_context: TestContext) {
    let fixture = object_fixture(
        &test_context,
        "patch root replacement",
        serde_json::json!({"old": true}),
    )
    .await;
    let object = &fixture.objects[0];

    let response = patch_request_with_content_type(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_endpoint(fixture.class.id, object.id),
        serde_json::json!([
            {"op": "replace", "path": "", "value": ["complete", "replacement"]}
        ]),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let updated: HubuumObject = test::read_body_json(response).await;

    assert_eq!(updated.data, serde_json::json!(["complete", "replacement"]));
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[actix_web::test]
async fn by_name_path_percent_decodes_and_updates_the_named_object(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = object_fixture(
        &test_context,
        "patch by encoded name",
        serde_json::json!({"state": "before"}),
    )
    .await;
    let mut object = fixture.objects[0].clone();
    object.name = test_context.scoped_name("rack/a b");
    object = object
        .save_without_events(&test_context.pool)
        .await
        .unwrap();

    let response = patch_request_with_content_type(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_by_name_endpoint(&fixture.class.name, &object.name),
        serde_json::json!([
            {"op": "replace", "path": "/state", "value": "after"}
        ]),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let updated: HubuumObject = test::read_body_json(response).await;

    assert_eq!(updated.id, object.id);
    assert_eq!(updated.data["state"], "after");
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[actix_web::test]
async fn by_name_path_is_scoped_to_the_class(#[future(awt)] test_context: TestContext) {
    let classes = create_test_classes(&test_context, "JSON Patch name class scope").await;
    let shared_name = test_context.scoped_name("shared object name");
    let first = NewHubuumObject {
        collection_id: classes[0].collection_id,
        hubuum_class_id: classes[0].id,
        name: shared_name.clone(),
        description: "first class".to_string(),
        data: serde_json::json!({"class": "first"}),
    }
    .save_without_events(&test_context.pool)
    .await
    .unwrap();
    let second = NewHubuumObject {
        collection_id: classes[1].collection_id,
        hubuum_class_id: classes[1].id,
        name: shared_name.clone(),
        description: "second class".to_string(),
        data: serde_json::json!({"class": "second"}),
    }
    .save_without_events(&test_context.pool)
    .await
    .unwrap();

    let response = patch_request_with_content_type(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_by_name_endpoint(&classes[0].name, &shared_name),
        serde_json::json!([
            {"op": "replace", "path": "/class", "value": "patched"}
        ]),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;
    assert_response_status(response, StatusCode::OK).await;

    assert_eq!(
        current_object(&test_context, first.id).await.data["class"],
        "patched"
    );
    assert_eq!(
        current_object(&test_context, second.id).await.data["class"],
        "second"
    );
    crate::tests::cleanup_test_classes(&classes).await;
}

#[rstest]
#[actix_web::test]
async fn by_name_path_never_interprets_numeric_names_as_ids(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = object_fixture(
        &test_context,
        "patch numeric names",
        serde_json::json!({"state": "before"}),
    )
    .await;
    let mut class = fixture.class.clone();
    class.name = (i32::MAX - class.id).to_string();
    class = class.save_without_events(&test_context.pool).await.unwrap();
    let mut object = fixture.objects[0].clone();
    object.name = (i32::MAX - object.id).to_string();
    object = object
        .save_without_events(&test_context.pool)
        .await
        .unwrap();

    let response = patch_request_with_content_type(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_by_name_endpoint(&class.name, &object.name),
        serde_json::json!([
            {"op": "replace", "path": "/state", "value": "after"}
        ]),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let updated: HubuumObject = test::read_body_json(response).await;

    assert_eq!(updated.id, object.id);
    assert_eq!(updated.data["state"], "after");
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[actix_web::test]
async fn existing_object_patch_keeps_whole_data_replacement_semantics(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = object_fixture(
        &test_context,
        "existing patch replacement",
        serde_json::json!({"nested": {"keep": true, "replace": "before"}}),
    )
    .await;
    let object = &fixture.objects[0];

    let response = patch_request(
        &test_context.pool,
        &test_context.admin_token,
        &object_endpoint(fixture.class.id, object.id),
        serde_json::json!({"data": {"nested": {"replace": "after"}}}),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let updated: HubuumObject = test::read_body_json(response).await;

    assert_eq!(
        updated.data,
        serde_json::json!({"nested": {"replace": "after"}})
    );
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[actix_web::test]
async fn later_operation_failure_persists_none_of_the_patch(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = object_fixture(
        &test_context,
        "later patch failure",
        serde_json::json!({"state": "before"}),
    )
    .await;
    let object = fixture.objects[0].clone();
    let history_before = object_history_count(&test_context, object.id).await;
    let events_before = object_event_count(&test_context, object.id).await;

    let response = patch_request_with_content_type(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_endpoint(fixture.class.id, object.id),
        serde_json::json!([
            {"op": "replace", "path": "/state", "value": "intermediate"},
            {"op": "remove", "path": "/missing"}
        ]),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;
    assert_response_status(response, StatusCode::CONFLICT).await;

    assert_eq!(current_object(&test_context, object.id).await, object);
    assert_eq!(
        object_history_count(&test_context, object.id).await,
        history_before
    );
    assert_eq!(
        object_event_count(&test_context, object.id).await,
        events_before
    );
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[actix_web::test]
async fn failed_test_operation_leaves_the_object_unchanged(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = object_fixture(
        &test_context,
        "failed test operation",
        serde_json::json!({"version": 1}),
    )
    .await;
    let object = fixture.objects[0].clone();

    let response = patch_request_with_content_type(
        &test_context.pool,
        &test_context.admin_token,
        &data_patch_endpoint(fixture.class.id, object.id),
        serde_json::json!([
            {"op": "test", "path": "/version", "value": 2},
            {"op": "replace", "path": "/version", "value": 3}
        ]),
        JSON_PATCH_MEDIA_TYPE,
    )
    .await;
    assert_response_status(response, StatusCode::CONFLICT).await;

    assert_eq!(current_object(&test_context, object.id).await, object);
    fixture.cleanup().await.unwrap();
}
