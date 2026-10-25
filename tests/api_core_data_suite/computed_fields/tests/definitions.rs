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
    assert!(
        !response
            .headers()
            .contains_key(actix_web::http::header::ETAG),
        "the composite mutation body must not use the definition ETag"
    );
    let created: serde_json::Value = test::read_body_json(response).await;
    let field_id = created["definition"]["id"].as_i64().unwrap();

    let endpoint = format!("{endpoint}/{field_id}");
    let response = get_request(&test_context.pool, &test_context.admin_token, &endpoint).await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let etag = response
        .headers()
        .get(actix_web::http::header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let response = patch_request_with_headers(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint,
        serde_json::json!({
            "label": "First revisioned update"
        }),
        vec![(actix_web::http::header::IF_MATCH, etag.clone())],
    )
    .await;
    assert_response_status(response, StatusCode::OK).await;

    let response = patch_request_with_headers(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint,
        serde_json::json!({ "label": "First revisioned update" }),
        vec![(actix_web::http::header::IF_MATCH, etag)],
    )
    .await;
    assert_response_status(response, StatusCode::PRECONDITION_FAILED).await;

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn personal_patch_rejects_a_stale_semantic_no_op(#[future(awt)] test_context: TestContext) {
    let fixture = fixture(&test_context, "personal stale no-op").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        "/api/v1/iam/me/computed-fields",
        serde_json::json!({
            "class_id": fixture.class.id,
            "key": "personal_stale_no_op",
            "label": "Original label",
            "operation": {"type": "first_non_null", "paths": ["/manual/hostname"]},
            "result_type": "string",
            "enabled": true
        }),
    )
    .await;
    let response = assert_response_status(response, StatusCode::CREATED).await;
    let stale_etag = response
        .headers()
        .get(actix_web::http::header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let created: serde_json::Value = test::read_body_json(response).await;
    let endpoint = format!(
        "/api/v1/iam/me/computed-fields/{}",
        created["id"].as_i64().unwrap()
    );

    let latest = serde_json::json!({"label": "Latest label"});
    let response = patch_request_with_headers(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint,
        &latest,
        vec![(actix_web::http::header::IF_MATCH, stale_etag.clone())],
    )
    .await;
    assert_response_status(response, StatusCode::OK).await;

    let response = patch_request_with_headers(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint,
        &latest,
        vec![(actix_web::http::header::IF_MATCH, stale_etag)],
    )
    .await;
    assert_response_status(response, StatusCode::PRECONDITION_FAILED).await;

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
        typed_class_id(fixture.class.id),
        typed_collection_id(fixture.class.collection_id + 1),
        typed_principal_id(test_context.admin_user.id),
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
            .grant_one(
                &test_context.pool,
                GroupID::new(group.id).unwrap(),
                permission,
            )
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
    crate::services::identity::delete_service_account(
        &test_context.pool,
        account.id,
        &EventContext::system(),
    )
    .await
    .unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}
use super::*;

#[rstest::rstest]
#[case::none("none")]
#[case::disabled("disabled")]
#[case::personal("personal")]
#[tokio::test]
async fn enrichment_without_enabled_shared_fields_is_fresh(
    #[future(awt)] test_context: TestContext,
    #[case] mode: &str,
) {
    let fixture = fixture(&test_context, "empty shared scope").await;
    if mode == "disabled" {
        let mut definition = definition("disabled");
        definition["enabled"] = serde_json::json!(false);
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
            definition,
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;
    } else if mode == "personal" {
        create_personal_definition(
            &test_context.pool,
            typed_class_id(fixture.class.id),
            typed_principal_id(test_context.admin_user.id),
            definition_request("personal"),
            &EventContext::system(),
        )
        .await
        .unwrap();
    }
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
    assert_eq!(object["computed"]["shared"]["materialization_stale"], false);
    assert_eq!(
        object["computed"]["shared"]["values"],
        serde_json::json!({})
    );
    if mode == "personal" {
        assert_eq!(
            object["computed"]["personal"]["values"]["personal"],
            "inventory.example"
        );
    }
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[case::small(1, false)]
#[case::page(20, false)]
#[case::repeated_identity(1, true)]
#[tokio::test]
async fn fresh_enrichment_reads_definitions_and_materializations_once_per_page(
    #[future(awt)] test_context: TestContext,
    #[case] object_count: usize,
    #[case] duplicate: bool,
) {
    use hubuum_domain::ObjectId;
    use hubuum_storage_core::{
        ComputedObjectStorage, ObjectStorage, StorageComputedObjectEnrichmentQuery,
    };
    use hubuum_storage_postgres::PostgresStorage;

    let mut fixture = fixture(&test_context, "fresh enrichment budget").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;
    for index in 1..object_count {
        fixture.objects.push(
            NewHubuumObject {
                collection_id: fixture.class.collection_id,
                hubuum_class_id: fixture.class.id,
                name: test_context.scoped_name(&format!("fresh budget {index}")),
                description: String::new(),
                data: fixture.objects[0].data.clone(),
            }
            .save_without_events(&test_context.pool)
            .await
            .unwrap(),
        );
    }
    let storage = PostgresStorage::unobserved(test_context.pool.get_ref().clone());
    let mut objects = Vec::new();
    for object in &fixture.objects {
        objects.push(
            storage
                .get_object(ObjectId::new(object.id).unwrap())
                .await
                .unwrap()
                .object()
                .clone(),
        );
    }
    if duplicate {
        objects.push(objects[0].clone());
    }
    let (result, queries) = capture_queries(
        storage
            .enrich_objects_with_computed(StorageComputedObjectEnrichmentQuery::new(objects, None)),
    )
    .await;
    let enriched = result.unwrap();
    assert_eq!(enriched.len(), object_count + usize::from(duplicate));
    assert!(
        enriched
            .into_iter()
            .all(|object| !object.into_parts().1.into_parts().1)
    );
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("FROM \"computed_field_definitions\""),
        1
    );
    assert_eq!(queries.queries_matching("FROM \"object_computed_data\""), 1);
    assert_eq!(queries.queries_matching("FOR UPDATE"), 0);
    fixture.cleanup().await.unwrap();
}
