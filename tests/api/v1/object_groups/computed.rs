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

    let page = group_rows(
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

    let page = group_rows(
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
            "/api/v1/classes/{}/object-groups?group_by=computed.personal.admin_only",
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
            "/api/v1/classes/{}/object-groups?group_by=computed.personal.priority",
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
            "/api/v1/classes/{}/object-groups?group_by=computed.personal.{key}",
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
