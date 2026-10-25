use actix_web::{http::StatusCode, test};
use rstest::rstest;
use serde_json::{Value, json};
use std::time::Duration;

use crate::models::{
    GroupID, NewHubuumClass, NewHubuumObject, Permissions, TaskKind, TaskResponse, TaskStatus,
};
use crate::tests::api_operations::{get_request, post_request};
use crate::tests::asserts::{assert_response_status, header_value};
use crate::tests::{ObjectFixture, TestContext, scoped_token, test_context};
use crate::traits::PermissionController;

async fn fixture(context: &TestContext) -> ObjectFixture {
    fixture_with_documents(
        context,
        (0..3)
            .map(|index| json!({format!("visible_{index}"):index,"value":index}))
            .collect(),
    )
    .await
}
async fn fixture_with_documents(context: &TestContext, documents: Vec<Value>) -> ObjectFixture {
    context
        .object_fixture(
            "schema profiling",
            NewHubuumClass {
                collection_id: 0,
                name: context.scoped_name("schema profiling"),
                description: String::new(),
                json_schema: None,
                validate_schema: Some(false),
            },
            documents
                .into_iter()
                .enumerate()
                .map(|(index, data)| NewHubuumObject {
                    collection_id: 0,
                    hubuum_class_id: 0,
                    name: context.scoped_name(&format!("profile {index}")),
                    description: String::new(),
                    data,
                })
                .collect(),
        )
        .await
        .unwrap()
}
fn endpoint(fixture: &ObjectFixture) -> String {
    format!("/api/v1/classes/{}/schema/profiles", fixture.class.id)
}
async fn submit(
    context: &TestContext,
    fixture: &ObjectFixture,
    token: &str,
    options: Value,
) -> TaskResponse {
    let response = post_request(&context.pool, token, &endpoint(fixture), options).await;
    let response = assert_response_status(response, StatusCode::ACCEPTED).await;
    test::read_body_json(response).await
}
async fn wait(context: &TestContext, id: i32) {
    wait_for_status(context, id, TaskStatus::Succeeded).await;
}
async fn wait_for_status(context: &TestContext, id: i32, expected: TaskStatus) {
    for _ in 0..200 {
        let response = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/tasks/{id}"),
        )
        .await;
        let task: TaskResponse =
            test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
        if matches!(
            task.status,
            TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Cancelled
        ) {
            assert_eq!(task.status, expected, "{:?}", task.summary);
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("profiling task {id} did not finish");
}

#[rstest]
#[tokio::test]
async fn schema_profile_returns_a_sampled_replayed_candidate(
    #[future(awt)] test_context: TestContext,
) {
    let context = test_context;
    let fixture = fixture(&context).await;
    let task = submit(
        &context,
        &fixture,
        &context.admin_token,
        json!({"sample_limit":1,"preset":"strict"}),
    )
    .await;
    assert_eq!(task.kind, TaskKind::SchemaProfiling);
    wait(&context, task.id).await;
    let response = get_request(
        &context.pool,
        &context.admin_token,
        &format!("{}/{}", endpoint(&fixture), task.id),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, "Cache-Control").as_deref(),
        Some("private, no-store")
    );
    let report: Value = test::read_body_json(response).await;
    assert_eq!(report["sampled_objects"], 1);
    assert_eq!(report["truncation"], "sample_limit");
    assert_eq!(report["sample_valid"], true);
    assert_eq!(
        report["schema"]["properties"]["visible_0"]["type"],
        "integer"
    );
    assert!(report["schema"]["properties"]["visible_1"].is_null());
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn incomplete_schema_profile_replay_fails_without_output(
    #[future(awt)] test_context: TestContext,
) {
    let context = test_context;
    let document = serde_json::from_str(&format!("{{\"value\":{}}}", "1".repeat(1100))).unwrap();
    let fixture = fixture_with_documents(&context, vec![document]).await;
    let task = submit(&context, &fixture, &context.admin_token, json!({})).await;
    wait_for_status(&context, task.id, TaskStatus::Failed).await;
    let response = get_request(
        &context.pool,
        &context.admin_token,
        &format!("{}/{}", endpoint(&fixture), task.id),
    )
    .await;
    assert_response_status(response, StatusCode::NOT_FOUND).await;
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[case(json!({"sample_limit":251}))]
#[case(json!({"preset":"invented"}))]
#[case(json!({"enums":true}))]
#[tokio::test]
async fn schema_profile_rejects_invalid_options_before_submission(
    #[future(awt)] test_context: TestContext,
    #[case] options: Value,
) {
    let fixture = fixture(&test_context).await;
    let response = post_request(
        &test_context.pool,
        &test_context.admin_token,
        &endpoint(&fixture),
        options,
    )
    .await;
    assert_response_status(response, StatusCode::BAD_REQUEST).await;
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn schema_profile_worker_preserves_the_submitting_permission_scope(
    #[future(awt)] test_context: TestContext,
) {
    let context = test_context;
    let fixture = fixture(&context).await;
    let token = scoped_token(
        &context.pool,
        context.admin_user.id,
        &[Permissions::ReadClass],
    )
    .await;
    let task = submit(&context, &fixture, &token, json!({})).await;
    wait(&context, task.id).await;
    let response = get_request(
        &context.pool,
        &token,
        &format!("{}/{}", endpoint(&fixture), task.id),
    )
    .await;
    let report: Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(report["status"], "insufficient");
    assert_eq!(report["sampled_objects"], 0);
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn schema_profile_download_rejects_a_narrower_token(
    #[future(awt)] test_context: TestContext,
) {
    let context = test_context;
    let fixture = fixture(&context).await;
    let task = submit(&context, &fixture, &context.admin_token, json!({})).await;
    wait(&context, task.id).await;
    let token = scoped_token(
        &context.pool,
        context.admin_user.id,
        &[Permissions::ReadClass],
    )
    .await;
    let response = get_request(
        &context.pool,
        &token,
        &format!("{}/{}", endpoint(&fixture), task.id),
    )
    .await;
    assert_response_status(response, StatusCode::FORBIDDEN).await;
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn schema_profile_download_rechecks_revoked_object_access(
    #[future(awt)] test_context: TestContext,
) {
    let context = test_context;
    let fixture = fixture(&context).await;
    let group = crate::tests::create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();
    for permission in [
        Permissions::ReadClass,
        Permissions::ReadCollection,
        Permissions::ReadObject,
    ] {
        fixture
            .collection
            .collection
            .grant_one(&context.pool, GroupID::new(group.id).unwrap(), permission)
            .await
            .unwrap();
    }
    let task = submit(&context, &fixture, &context.normal_token, json!({})).await;
    wait(&context, task.id).await;
    fixture
        .collection
        .collection
        .revoke_one(
            &context.pool,
            GroupID::new(group.id).unwrap(),
            Permissions::ReadObject,
        )
        .await
        .unwrap();
    let response = get_request(
        &context.pool,
        &context.normal_token,
        &format!("{}/{}", endpoint(&fixture), task.id),
    )
    .await;
    assert_response_status(response, StatusCode::FORBIDDEN).await;
    fixture.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}
