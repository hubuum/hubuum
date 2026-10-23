use actix_web::{http::StatusCode, test};
use rstest::rstest;
use serde_json::{Value, json};

use crate::tests::TestContext;
use crate::tests::api_operations::{get_request, post_request};
use crate::tests::asserts::assert_response_status;

async fn login(context: &TestContext) -> Value {
    let response = post_request(
        &context.pool,
        "",
        "/api/v1/auth/login",
        &json!({
            "client_id": "session-api-test",
            "name": context.normal_user.name(&context.pool).await.unwrap(),
            "password": "testpassword"
        }),
    )
    .await;
    assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
    test::read_body_json(assert_response_status(response, StatusCode::OK).await).await
}

#[actix_web::test]
async fn refresh_replay_invalidates_the_returned_pair() {
    let context = TestContext::new().await;
    let first = login(&context).await;
    let request = json!({"client_id":"session-api-test", "refresh_token":first["refresh_token"]});
    let response = post_request(&context.pool, "", "/api/v1/auth/refresh", &request).await;
    assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
    let second: Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(first["session_id"], second["session_id"]);
    assert_eq!(first["absolute_expires_at"], second["absolute_expires_at"]);
    assert_ne!(first["refresh_token"], second["refresh_token"]);
    let replay = post_request(&context.pool, "", "/api/v1/auth/refresh", &request).await;
    assert_eq!(replay.headers().get("cache-control").unwrap(), "no-store");
    assert_response_status(replay, StatusCode::UNAUTHORIZED).await;
    let response = get_request(
        &context.pool,
        second["access_token"].as_str().unwrap(),
        "/api/v1/auth/validate",
    )
    .await;
    assert_response_status(response, StatusCode::UNAUTHORIZED).await;
}

#[rstest]
#[case::refresh_as_bearer(true)]
#[case::bearer_as_refresh(false)]
#[actix_web::test]
async fn credentials_cannot_cross_endpoint_boundaries(#[case] refresh_as_bearer: bool) {
    let context = TestContext::new().await;
    let session = login(&context).await;
    let response = if refresh_as_bearer {
        get_request(
            &context.pool,
            session["refresh_token"].as_str().unwrap(),
            "/api/v1/iam/me",
        )
        .await
    } else {
        post_request(
            &context.pool,
            "",
            "/api/v1/auth/refresh",
            &json!({"client_id":"session-api-test", "refresh_token":session["access_token"]}),
        )
        .await
    };
    assert_response_status(response, StatusCode::UNAUTHORIZED).await;
}

#[rstest]
#[case::current("/api/v1/auth/logout")]
#[case::all("/api/v1/auth/logout_all")]
#[actix_web::test]
async fn logout_revokes_refresh_but_preserves_pat(#[case] endpoint: &str) {
    let context = TestContext::new().await;
    let session = login(&context).await;
    let response = post_request(
        &context.pool,
        session["access_token"].as_str().unwrap(),
        endpoint,
        &json!({}),
    )
    .await;
    assert_response_status(response, StatusCode::OK).await;
    let response = post_request(
        &context.pool,
        "",
        "/api/v1/auth/refresh",
        &json!({"client_id":"session-api-test", "refresh_token":session["refresh_token"]}),
    )
    .await;
    assert_response_status(response, StatusCode::UNAUTHORIZED).await;
    let response = get_request(
        &context.pool,
        &context.normal_token,
        "/api/v1/auth/validate",
    )
    .await;
    assert_response_status(response, StatusCode::OK).await;
}

#[actix_web::test]
async fn session_access_alone_cannot_mint_durable_credentials() {
    let context = TestContext::new().await;
    let session = login(&context).await;
    let endpoint = format!("/api/v1/iam/principals/{}/tokens", context.normal_user.id);
    let response = post_request(
        &context.pool,
        session["access_token"].as_str().unwrap(),
        &endpoint,
        &json!({"name":"unapproved"}),
    )
    .await;
    assert_response_status(response, StatusCode::FORBIDDEN).await;
}

#[actix_web::test]
async fn activity_reads_require_token_management_authorization() {
    let context = TestContext::new().await;
    let session = login(&context).await;
    let access =
        crate::models::Token::try_from(session["access_token"].as_str().unwrap().to_owned())
            .unwrap();
    let metadata = crate::services::authentication::authenticate_bearer_token(
        &crate::tests::app_context(&context.pool),
        &access,
    )
    .await
    .unwrap();
    let endpoint = format!(
        "/api/v1/iam/principals/{}/tokens/{}/activity?limit=1",
        context.normal_user.id,
        metadata.id().id()
    );
    let response = get_request(&context.pool, &context.normal_token, &endpoint).await;
    assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
    let activity: Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(activity["session_id"], session["session_id"]);
    assert_eq!(activity["creation"]["issuance_method"], "login");
    assert_eq!(activity["best_effort"], true);
    let unrelated = TestContext::new().await;
    let response = get_request(&context.pool, &unrelated.normal_token, &endpoint).await;
    assert_response_status(response, StatusCode::NOT_FOUND).await;
}

#[rstest]
#[case::creation(true)]
#[case::successful_validation(false)]
#[actix_web::test]
async fn activity_write_failure_preserves_authentication(#[case] during_creation: bool) {
    use hubuum_storage_postgres::{PostgresFaultController, PostgresFaultPoint};

    let context = TestContext::new().await;
    let fault = PostgresFaultController::failing(PostgresFaultPoint::CredentialActivityBeforeWrite);
    let session = if during_creation {
        fault.run(login(&context)).await
    } else {
        login(&context).await
    };
    let request = get_request(
        &context.pool,
        session["access_token"].as_str().unwrap(),
        "/api/v1/auth/validate",
    );
    let response = if during_creation {
        request.await
    } else {
        fault.run(request).await
    };
    assert_response_status(response, StatusCode::OK).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        fault.wait_until_reached(),
    )
    .await
    .expect("the activity write must actually have failed");
}
