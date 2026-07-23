use super::*;

#[rstest::rstest]
#[tokio::test]
async fn shared_computed_groups_evaluate_snapshots_and_use_unavailable_bucket(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "shared computed groups").await;
    create_shared_definition(
        &test_context.pool,
        fixture.class.id,
        fixture.class.collection_id,
        test_context.admin_user.id,
        computed_definition("shared_bucket", "/bucket", true),
        &EventContext::system(),
    )
    .await
    .unwrap();

    let page = aggregate_rows(
        &test_context,
        &fixture,
        &test_context.admin_token,
        "group_by=computed.shared.shared_bucket",
    )
    .await;
    let count_for = |state: &str| {
        page.rows
            .iter()
            .find(|row| row["dimensions"][0]["state"] == state)
            .map(|row| row["object_count"].as_i64().unwrap())
    };
    assert_eq!(count_for("value"), Some(2));
    assert_eq!(count_for("null"), Some(2));
    assert_eq!(count_for("unavailable"), Some(1));
    assert_eq!(page.cache_control.as_deref(), Some("private, no-store"));

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn shared_computed_numeric_measure_uses_the_definition_snapshot(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "shared computed measure").await;
    create_shared_definition(
        &test_context.pool,
        fixture.class.id,
        fixture.class.collection_id,
        test_context.admin_user.id,
        numeric_computed_definition("shared_amount", "/amount", true),
        &EventContext::system(),
    )
    .await
    .unwrap();

    let page = aggregate_rows(
        &test_context,
        &fixture,
        &test_context.admin_token,
        "aggregate=sum:computed.shared.shared_amount",
    )
    .await;

    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0]["measures"][0]["value"], 30.5);
    assert_eq!(page.rows[0]["measures"][0]["value_count"], 2);
    assert_eq!(page.rows[0]["measures"][0]["skipped_count"], 3);
    assert_eq!(page.cache_control.as_deref(), Some("private, no-store"));

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn shared_computed_filters_apply_before_scalar_aggregation(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "shared computed aggregate filter").await;
    create_shared_definition(
        &test_context.pool,
        fixture.class.id,
        fixture.class.collection_id,
        test_context.admin_user.id,
        computed_definition("filter_status", "/status", true),
        &EventContext::system(),
    )
    .await
    .unwrap();

    let page = aggregate_rows(
        &test_context,
        &fixture,
        &test_context.admin_token,
        "computed.shared.filter_status__equals=active&group_by=description",
    )
    .await;
    let count_for = |description: &str| {
        page.rows
            .iter()
            .find(|row| row["dimensions"][0]["value"] == description)
            .map(|row| row["object_count"].as_i64().unwrap())
    };

    assert_eq!(summed_count(&page.rows), 4);
    assert_eq!(count_for("alpha"), Some(2));
    assert_eq!(count_for("beta"), Some(1));
    assert_eq!(count_for("gamma"), Some(1));
    assert_eq!(page.cache_control.as_deref(), Some("private, no-store"));

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[cfg(feature = "integration-test-support")]
#[rstest::rstest]
#[tokio::test]
async fn computed_grouping_paginates_byte_bounded_candidate_batches(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = test_context
        .object_fixture(
            "computed byte bounded candidates",
            NewHubuumClass {
                collection_id: 0,
                name: test_context.scoped_name("computed byte bounded candidate class"),
                description: "Byte-bounded computed candidate class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
            },
            (0..3)
                .map(|index| NewHubuumObject {
                    collection_id: 0,
                    hubuum_class_id: 0,
                    name: test_context.scoped_name(&format!("computed candidate {index}")),
                    description: String::new(),
                    data: serde_json::json!({
                        "bucket": "same",
                        "payload": "x".repeat(2048),
                    }),
                })
                .collect(),
        )
        .await
        .unwrap();
    create_shared_definition(
        &test_context.pool,
        fixture.class.id,
        fixture.class.collection_id,
        test_context.admin_user.id,
        computed_definition("byte_batch", "/bucket", true),
        &EventContext::system(),
    )
    .await
    .unwrap();

    let page = aggregate_rows(
        &test_context,
        &fixture,
        &test_context.admin_token,
        "group_by=computed.shared.byte_batch",
    )
    .await;

    assert_eq!(summed_count(&page.rows), 3);

    finish_active_rebuild(&test_context, fixture.class.id).await;
    fixture.cleanup().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn personal_computed_grouping_uses_the_requesting_owners_definition(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "owned personal computed groups").await;
    let group = grant_normal_user_read_access(&test_context, &fixture).await;
    create_personal_definition(
        &test_context.pool,
        fixture.class.id,
        test_context.normal_user.id,
        computed_definition("priority", "/bucket", true),
    )
    .await
    .unwrap();

    let page = aggregate_rows(
        &test_context,
        &fixture,
        &test_context.normal_token,
        "group_by=computed.personal.priority",
    )
    .await;

    assert_eq!(summed_count(&page.rows), 5);

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn personal_computed_filters_use_the_requesting_owners_definition(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "owned personal computed aggregate filter").await;
    let group = grant_normal_user_read_access(&test_context, &fixture).await;
    create_personal_definition(
        &test_context.pool,
        fixture.class.id,
        test_context.normal_user.id,
        computed_definition("filter_priority", "/bucket", true),
    )
    .await
    .unwrap();

    let page = aggregate_rows(
        &test_context,
        &fixture,
        &test_context.normal_token,
        "computed.personal.filter_priority__equals=a&group_by=description",
    )
    .await;

    assert_eq!(summed_count(&page.rows), 2);
    assert_eq!(page.rows.len(), 2);
    assert_eq!(page.cache_control.as_deref(), Some("private, no-store"));

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn personal_computed_grouping_rejects_another_owners_definition(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "other personal computed owner").await;
    let group = grant_normal_user_read_access(&test_context, &fixture).await;
    create_personal_definition(
        &test_context.pool,
        fixture.class.id,
        test_context.admin_user.id,
        computed_definition("admin_only", "/status", true),
    )
    .await
    .unwrap();

    let response = get_request(
        &test_context.pool,
        &test_context.normal_token,
        &format!(
            "/api/v1/classes/{}/object-aggregates?group_by=computed.personal.admin_only",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::BAD_REQUEST).await;
    let error: serde_json::Value = test::read_body_json(response).await;
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("accessible field")
    );

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn personal_computed_grouping_rejects_service_accounts(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "service personal computed groups").await;
    let group = grant_normal_user_read_access(&test_context, &fixture).await;
    create_personal_definition(
        &test_context.pool,
        fixture.class.id,
        test_context.normal_user.id,
        computed_definition("priority", "/bucket", true),
    )
    .await
    .unwrap();

    let account = create_test_service_account(&test_context.pool, &group, None).await;
    group
        .add_member_without_events(&test_context.pool, &account)
        .await
        .unwrap();
    let token = service_account_token(&test_context.pool, &account, None, None).await;
    let response = get_request(
        &test_context.pool,
        &token,
        &format!(
            "/api/v1/classes/{}/object-aggregates?group_by=computed.personal.priority",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::BAD_REQUEST).await;
    let error: serde_json::Value = test::read_body_json(response).await;
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("Service accounts")
    );

    ServiceAccountID::new(account.id)
        .unwrap()
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[case("unknown", false)]
#[case("disabled", true)]
#[tokio::test]
async fn invalid_computed_selectors_are_bad_requests(
    #[future(awt)] test_context: TestContext,
    #[case] key: &str,
    #[case] create_disabled: bool,
) {
    let fixture = fixture(&test_context, &format!("invalid computed {key}")).await;
    let group = grant_normal_user_read_access(&test_context, &fixture).await;
    if create_disabled {
        create_personal_definition(
            &test_context.pool,
            fixture.class.id,
            test_context.normal_user.id,
            computed_definition(key, "/bucket", false),
        )
        .await
        .unwrap();
    }
    let response = get_request(
        &test_context.pool,
        &test_context.normal_token,
        &format!(
            "/api/v1/classes/{}/object-aggregates?group_by=computed.personal.{key}",
            fixture.class.id
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::BAD_REQUEST).await;
    let error: serde_json::Value = test::read_body_json(response).await;
    let message = error["message"].as_str().unwrap();
    if create_disabled {
        assert!(message.contains("disabled"));
    } else {
        assert!(message.contains("accessible field"));
    }

    fixture.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}

#[rstest::rstest]
#[case("known")]
#[case("disabled")]
#[case("unknown")]
#[tokio::test]
async fn local_computed_filters_are_not_disclosed_outside_resource_scope(
    #[future(awt)] test_context: TestContext,
    #[case] selector: &str,
) {
    let target = fixture(&test_context, "local computed filter scope target").await;
    let outside = fixture(&test_context, "local computed filter outside scope").await;
    let group = grant_normal_user_read_access(&test_context, &target).await;
    for (key, enabled) in [("known", true), ("disabled", false)] {
        create_shared_definition(
            &test_context.pool,
            target.class.id,
            target.class.collection_id,
            test_context.admin_user.id,
            computed_definition(key, "/bucket", enabled),
            &EventContext::system(),
        )
        .await
        .unwrap();
    }
    let token = resource_scoped_token(
        &test_context.pool,
        test_context.normal_user.id,
        vec![TokenResourceScope::Object(
            HubuumObjectID::new(outside.objects[0].id).unwrap(),
        )],
    )
    .await;

    let page = aggregate_rows(
        &test_context,
        &target,
        &token,
        &format!("computed.shared.{selector}__equals=a&group_by=description"),
    )
    .await;

    assert!(page.rows.is_empty());
    assert_eq!(page.total_count.as_deref(), Some("0"));

    finish_active_rebuild(&test_context, target.class.id).await;
    target.cleanup().await.unwrap();
    outside.cleanup().await.unwrap();
    group
        .delete_without_events(&test_context.pool)
        .await
        .unwrap();
}
