#[rstest::rstest]
#[tokio::test]
async fn shared_definition_live_fallback_repairs_stale_materialization(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "shared fallback").await;
    let endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint,
        definition("display_name"),
    )
    .await;
    let response = assert_response_status(response, StatusCode::CREATED).await;
    let mutation: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(mutation["state"]["rebuild_status"], "rebuilding");
    assert!(mutation["state"]["active_task_id"].is_number());

    finish_active_rebuild(&test_context, fixture.class.id).await;
    with_connection(&test_context.pool, async |conn| {
        use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};

        diesel::delete(object_computed_data.filter(object_id.eq(fixture.objects[0].id)))
            .execute(conn)
            .await
    })
    .await
    .unwrap();

    let object_endpoint = format!(
        "/api/v1/classes/{}/{}?include=computed",
        fixture.class.id, fixture.objects[0].id
    );
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &object_endpoint,
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let first: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(
        first["computed"]["shared"]["values"]["display_name"],
        "inventory.example"
    );
    assert_eq!(first["computed"]["shared"]["materialization_stale"], true);

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &object_endpoint,
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let repaired: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(
        repaired["computed"]["shared"]["materialization_stale"],
        false
    );

    fixture.cleanup().await.unwrap();
}
#[rstest::rstest]
#[tokio::test]
async fn object_writes_materialize_the_current_shared_revision(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "write materialization").await;
    let endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint,
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    let object_endpoint = format!(
        "/api/v1/classes/{}/{}",
        fixture.class.id, fixture.objects[0].id
    );
    let response = patch_request(
        &test_context.pool,
        &test_context.admin_token,
        &object_endpoint,
        serde_json::json!({
            "data": {"manual": {"hostname": "updated.example"}}
        }),
    )
    .await;
    assert_response_status(response, StatusCode::OK).await;

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("{object_endpoint}?include=computed"),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let object: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(
        object["computed"]["shared"]["values"]["display_name"],
        "updated.example"
    );
    assert_eq!(object["computed"]["shared"]["materialization_stale"], false);

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn personal_definitions_are_separate_from_shared_values(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "personal values").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        "/api/v1/iam/me/computed-fields",
        serde_json::json!({
            "class_id": fixture.class.id,
            "key": "my_name",
            "label": "My name",
            "operation": {
                "type": "first_non_null",
                "paths": ["/manual/hostname"]
            },
            "result_type": "string",
            "enabled": true
        }),
    )
    .await;
    let response = assert_response_status(response, StatusCode::CREATED).await;
    let created: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(created["visibility"], "personal");

    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/{}?include=computed",
            fixture.class.id, fixture.objects[0].id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let object: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(
        object["computed"]["personal"]["values"]["my_name"],
        "manual.example"
    );
    assert_eq!(
        object["computed"]["shared"]["values"],
        serde_json::json!({})
    );

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn computed_input_is_rejected_even_when_null(#[future(awt)] test_context: TestContext) {
    let fixture = fixture(&test_context, "computed input rejection").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/", fixture.class.id),
        serde_json::json!({
            "name": "rejected",
            "collection_id": fixture.class.collection_id,
            "hubuum_class_id": fixture.class.id,
            "data": {},
            "description": "rejected",
            "computed": null
        }),
    )
    .await;
    assert_response_status(response, StatusCode::BAD_REQUEST).await;

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn shared_patch_requires_the_current_definition_revision(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "revision conflict").await;
    let endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint,
        definition("display_name"),
    )
    .await;
    let response = assert_response_status(response, StatusCode::CREATED).await;
    let created: serde_json::Value = test::read_body_json(response).await;
    let field_id = created["definition"]["id"].as_i64().unwrap();

    let response = patch_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("{endpoint}/{field_id}"),
        serde_json::json!({
            "expected_revision": 999,
            "label": "Wrong revision"
        }),
    )
    .await;
    assert_response_status(response, StatusCode::CONFLICT).await;

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn update_class_permission_cannot_manage_shared_definitions(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "shared mutation permission").await;
    let group = grant_normal_user(&test_context, &fixture, &[Permissions::UpdateClass]).await;
    let endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);

    let response = post_request(
        &test_context.pool,
        &test_context.normal_token,
        &endpoint,
        definition("forbidden_field"),
    )
    .await;
    assert_response_status(response, StatusCode::FORBIDDEN).await;

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn update_collection_permission_can_manage_shared_definitions(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "shared update collection permission").await;
    let group = grant_normal_user(&test_context, &fixture, &[Permissions::UpdateCollection]).await;
    let response = post_request(
        &test_context.pool,
        &test_context.normal_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("allowed_field"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn shared_mutation_rejects_a_stale_authorized_collection(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "shared stale collection authorization").await;

    let error = create_shared_definition(
        &test_context.pool,
        fixture.class.id,
        fixture.class.collection_id + 1,
        test_context.admin_user.id,
        definition_request("stale_authorization"),
        &EventContext::system(),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, crate::errors::ApiError::Conflict(_)));
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn personal_values_are_not_visible_to_another_human(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "personal isolation").await;
    let group = grant_normal_user(
        &test_context,
        &fixture,
        &[Permissions::ReadClass, Permissions::ReadObject],
    )
    .await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        "/api/v1/iam/me/computed-fields",
        serde_json::json!({
            "class_id": fixture.class.id,
            "key": "admin_only",
            "label": "Admin only",
            "operation": {"type": "first_non_null", "paths": ["/manual/hostname"]},
            "result_type": "string"
        }),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    let response = get_request(
        &test_context.pool,
        &test_context.normal_token,
        &format!(
            "/api/v1/classes/{}/{}?include=computed",
            fixture.class.id, fixture.objects[0].id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let object: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(
        object["computed"]["personal"]["values"],
        serde_json::json!({})
    );

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn service_accounts_cannot_manage_or_receive_personal_fields(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "service account personal exclusion").await;
    let group = create_test_group(&test_context.pool).await;
    for permission in [
        Permissions::ReadClass,
        Permissions::ReadCollection,
        Permissions::ReadObject,
    ] {
        fixture
            .collection
            .collection
            .grant_one(&test_context.pool, group.id, permission)
            .await
            .unwrap();
    }
    let account = create_test_service_account(&test_context.pool, &group, None).await;
    group
        .add_member_without_events(&test_context.pool, &account)
        .await
        .unwrap();
    let token = service_account_token(&test_context.pool, &account, None, None).await;

    let response = post_request(
        &test_context.pool,
        &token,
        "/api/v1/iam/me/computed-fields",
        serde_json::json!({
            "class_id": fixture.class.id,
            "key": "not_allowed",
            "label": "Not allowed",
            "operation": {"type": "first_non_null", "paths": ["/manual/hostname"]},
            "result_type": "string"
        }),
    )
    .await;
    assert_response_status(response, StatusCode::FORBIDDEN).await;

    let response = get_request(
        &test_context.pool,
        &token,
        &format!(
            "/api/v1/classes/{}/{}?include=computed",
            fixture.class.id, fixture.objects[0].id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let object: serde_json::Value = test::read_body_json(response).await;
    assert!(object["computed"].get("personal").is_none());

    let response = get_request(
        &test_context.pool,
        &token,
        &format!(
            "/api/v1/classes/{}/?sort=computed.personal.not_allowed",
            fixture.class.id
        ),
    )
    .await;
    assert_response_status(response, StatusCode::BAD_REQUEST).await;

    fixture.cleanup().await.unwrap();
    crate::models::ServiceAccountID::new(account.id)
        .unwrap()
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}
use super::*;
