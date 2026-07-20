#[rstest::rstest]
#[tokio::test]
async fn object_and_materialization_rollback_together_on_unexpected_failure(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "computed atomicity").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;

    with_connection(&test_context.pool, async |conn| {
        use crate::schema::computed_field_definitions::dsl::{
            class_id, computed_field_definitions, operation,
        };
        diesel::update(computed_field_definitions.filter(class_id.eq(fixture.class.id)))
            .set(operation.eq(serde_json::json!({"type": "unknown", "paths": ["/a"]})))
            .execute(conn)
            .await
    })
    .await
    .unwrap();

    let response = patch_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/{}",
            fixture.class.id, fixture.objects[0].id
        ),
        serde_json::json!({"data": {"manual": {"hostname": "must-rollback"}}}),
    )
    .await;
    assert_response_status(response, StatusCode::INTERNAL_SERVER_ERROR).await;
    let current = crate::models::HubuumObjectID::new(fixture.objects[0].id)
        .unwrap()
        .instance(&test_context.pool)
        .await
        .unwrap();
    assert_eq!(current.data, fixture.objects[0].data);

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn concurrent_definition_and_object_updates_return_current_values(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "computed concurrent definition").await;
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
    finish_active_rebuild(&test_context, fixture.class.id).await;

    let definition_endpoint = format!("{endpoint}/{field_id}");
    let object_endpoint = format!(
        "/api/v1/classes/{}/{}",
        fixture.class.id, fixture.objects[0].id
    );
    let definition_update = patch_request(
        &test_context.pool,
        &test_context.admin_token,
        &definition_endpoint,
        serde_json::json!({
            "expected_revision": 1,
            "operation": {"type": "first_non_null", "paths": ["/manual/hostname"]}
        }),
    );
    let object_update = patch_request(
        &test_context.pool,
        &test_context.admin_token,
        &object_endpoint,
        serde_json::json!({
            "data": {"manual": {"hostname": "concurrent.example"}}
        }),
    );
    let (definition_response, object_response) = tokio::join!(definition_update, object_update);
    assert_response_status(definition_response, StatusCode::OK).await;
    assert_response_status(object_response, StatusCode::OK).await;

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
        "concurrent.example"
    );

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn concurrent_backfill_and_object_update_cannot_restore_old_source_data(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "computed concurrent backfill").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    let task = active_rebuild_task(&test_context, fixture.class.id).await;
    let object_endpoint = format!(
        "/api/v1/classes/{}/{}",
        fixture.class.id, fixture.objects[0].id
    );
    let rebuild = execute_computed_reindex_task(&test_context.pool, &task);
    let update = patch_request(
        &test_context.pool,
        &test_context.admin_token,
        &object_endpoint,
        serde_json::json!({
            "data": {"manual": {"hostname": "after-backfill.example"}}
        }),
    );
    let (rebuild_result, update_response) = tokio::join!(rebuild, update);
    let _ = rebuild_result;
    assert_response_status(update_response, StatusCode::OK).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;

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
        "after-backfill.example"
    );
    assert_eq!(object["computed"]["shared"]["materialization_stale"], false);

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn lease_recovery_marks_the_class_rebuild_failed(#[future(awt)] test_context: TestContext) {
    let fixture = fixture(&test_context, "computed lease recovery").await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    let state = class_computation_state_for(&test_context.pool, fixture.class.id)
        .await
        .unwrap();
    let task_id = state.active_task_id.unwrap();
    with_connection(&test_context.pool, async |conn| {
        use crate::schema::tasks::dsl::{id, status, tasks};
        diesel::update(tasks.filter(id.eq(task_id)))
            .set(status.eq(TaskStatus::Running.as_str()))
            .execute(conn)
            .await
    })
    .await
    .unwrap();

    let recovered = recover_expired_task_leases(&test_context.pool, 100)
        .await
        .unwrap();
    assert!(recovered.iter().any(|task| task.id == task_id));
    let state = class_computation_state_for(&test_context.pool, fixture.class.id)
        .await
        .unwrap();
    assert_eq!(state.rebuild_status, "failed");
    assert_eq!(state.active_task_id, None);

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn user_anonymization_removes_personal_definitions(#[future(awt)] test_context: TestContext) {
    let fixture = fixture(&test_context, "computed anonymization").await;
    let group = grant_normal_user(&test_context, &fixture, &[Permissions::ReadClass]).await;
    let response = post_request(
        &test_context.pool,
        &test_context.normal_token,
        "/api/v1/iam/me/computed-fields",
        serde_json::json!({
            "class_id": fixture.class.id,
            "key": "remove_me",
            "label": "Remove me",
            "operation": {"type": "first_non_null", "paths": ["/manual/hostname"]},
            "result_type": "string"
        }),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/iam/users/{}/anonymize",
            test_context.normal_user.id
        ),
        serde_json::json!({}),
    )
    .await;
    assert_response_status(response, StatusCode::NO_CONTENT).await;
    let remaining = with_connection(&test_context.pool, async |conn| {
        use crate::schema::computed_field_definitions::dsl::{
            computed_field_definitions, owner_user_id,
        };
        computed_field_definitions
            .filter(owner_user_id.eq(Some(test_context.normal_user.id)))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();
    assert_eq!(remaining, 0);

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn concurrent_personal_creates_preserve_scope_capacity(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "computed personal capacity").await;
    for index in 0..15 {
        create_personal_definition(
            &test_context.pool,
            fixture.class.id,
            test_context.normal_user.id,
            definition_request(&format!("existing_{index}")),
        )
        .await
        .unwrap();
    }

    let first = create_personal_definition(
        &test_context.pool,
        fixture.class.id,
        test_context.normal_user.id,
        definition_request("concurrent_first"),
    );
    let second = create_personal_definition(
        &test_context.pool,
        fixture.class.id,
        test_context.normal_user.id,
        definition_request("concurrent_second"),
    );
    let (first, second) = tokio::join!(first, second);
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);

    let count = with_connection(&test_context.pool, async |conn| {
        use crate::schema::computed_field_definitions::dsl::{
            class_id, computed_field_definitions, owner_user_id,
        };
        computed_field_definitions
            .filter(class_id.eq(fixture.class.id))
            .filter(owner_user_id.eq(Some(test_context.normal_user.id)))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();
    assert_eq!(count, 16);

    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn deleting_a_class_cascades_definitions_and_materialization(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "computed class cascade").await;
    let class_id_value = fixture.class.id;
    let object_id_value = fixture.objects[0].id;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!("/api/v1/classes/{class_id_value}/computed-fields"),
        definition("shared_value"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, class_id_value).await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        "/api/v1/iam/me/computed-fields",
        serde_json::json!({
            "class_id": class_id_value,
            "key": "personal_value",
            "label": "Personal value",
            "operation": {"type": "first_non_null", "paths": ["/manual/hostname"]},
            "result_type": "string"
        }),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;

    fixture.cleanup().await.unwrap();
    let (definitions, materializations) =
        with_connection(&test_context.pool, async |conn| -> QueryResult<_> {
            use crate::schema::computed_field_definitions::dsl as definition;
            use crate::schema::object_computed_data::dsl as computed;
            let definitions = definition::computed_field_definitions
                .filter(definition::class_id.eq(class_id_value))
                .count()
                .get_result::<i64>(conn)
                .await?;
            let materializations = computed::object_computed_data
                .filter(computed::object_id.eq(object_id_value))
                .count()
                .get_result::<i64>(conn)
                .await?;
            Ok((definitions, materializations))
        })
        .await
        .unwrap();
    assert_eq!((definitions, materializations), (0, 0));
}

#[rstest::rstest]
#[tokio::test]
async fn manual_rebuild_queues_the_current_revision(#[future(awt)] test_context: TestContext) {
    let fixture = fixture(&test_context, "computed manual rebuild").await;
    let definition_endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &definition_endpoint,
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    finish_active_rebuild(&test_context, fixture.class.id).await;
    let before = class_computation_state_for(&test_context.pool, fixture.class.id)
        .await
        .unwrap();
    let rebuild_endpoint = format!("{definition_endpoint}/rebuild");

    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &rebuild_endpoint,
        serde_json::json!({}),
    )
    .await;
    let response = assert_response_status(response, StatusCode::ACCEPTED).await;
    let response: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(response["evaluation_revision"], before.evaluation_revision);
    assert_eq!(response["rebuild_status"], "rebuilding");
    assert!(response["active_task_id"].is_number());

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}
use super::*;
