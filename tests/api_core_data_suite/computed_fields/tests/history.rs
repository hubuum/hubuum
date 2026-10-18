use super::*;

#[rstest::rstest]
#[case::missing_history(false)]
#[case::unsupported_evaluator(true)]
#[tokio::test]
async fn historical_computed_values_never_fall_back_to_live_definitions(
    #[case] unsupported: bool,
    #[future(awt)] test_context: TestContext,
) {
    let fixture = fixture(&test_context, "historical unavailable").await;
    let endpoint = format!("/api/v1/classes/{}/computed-fields", fixture.class.id);
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint,
        definition("display_name"),
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    let at = chrono::Utc::now();
    with_connection(&test_context.pool, async |connection| {
        let query = if unsupported {
            "UPDATE computed_field_history SET evaluator_version = 99 WHERE class_id = $1"
        } else {
            "DELETE FROM computed_field_history WHERE class_id = $1"
        };
        diesel::sql_query(query)
            .bind::<diesel::sql_types::Integer, _>(fixture.class.id)
            .execute(connection)
            .await
    })
    .await
    .unwrap();
    let response = get_request(
        &test_context.pool,
        &test_context.admin_token,
        &format!(
            "/api/v1/classes/{}/{}/history/computed/as-of?at={}",
            fixture.class.id,
            fixture.objects[0].id(),
            at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let result: serde_json::Value = test::read_body_json(response).await;
    if unsupported {
        assert_eq!(result["shared"]["fields"][0]["status"], "unavailable");
        assert_eq!(
            result["shared"]["fields"][0]["reason"],
            "unsupported_evaluator_version"
        );
        assert!(result["shared"]["fields"][0].get("value").is_none());
    } else {
        assert_eq!(result["shared"]["status"], "unavailable");
        assert_eq!(result["shared"]["reason"], "definition_history_unavailable");
        assert!(result["shared"].get("fields").is_none());
    }
    fixture.cleanup().await.unwrap();
}
