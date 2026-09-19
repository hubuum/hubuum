use crate::tests::api_operations::{get_request, post_request, post_request_with_headers};
use crate::tests::asserts::assert_response_status;
use crate::tests::{TestContext, test_context};
use actix_web::{
    http::{StatusCode, header::HeaderName},
    test,
};
use rstest::rstest;
use serde_json::{Value, json};

fn operation(c: &TestContext) -> Value {
    json!({"kind":"create_token", "principal_id":c.normal_user.id, "token":{"name":"approved-client"}})
}
async fn issue(c: &TestContext, operation: Value) -> Value {
    let response = post_request(
        &c.pool,
        &c.normal_token,
        "/api/v1/iam/credential-approvals",
        json!({"password":"testpassword", "operation":operation}),
    )
    .await;
    let response = assert_response_status(response, StatusCode::CREATED).await;
    assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
    test::read_body_json(response).await
}

#[rstest]
#[case("token")]
#[case("user")]
#[case("password")]
#[case("restore")]
#[actix_web::test]
async fn bearer_alone_cannot_manage_credentials(
    #[case] target: &str,
    #[future(awt)] test_context: TestContext,
) {
    let c = test_context;
    let (path, body) = match target {
        "token" => (
            format!("/api/v1/iam/principals/{}/tokens", c.admin_user.id),
            json!({}),
        ),
        "user" => (
            "/api/v1/iam/users".to_string(),
            json!({"name":c.scoped_name("not_created"), "password":"secret"}),
        ),
        "password" => {
            let response = crate::tests::api_operations::patch_request(
                &c.pool,
                &c.admin_token,
                &format!("/api/v1/iam/users/{}", c.normal_user.id),
                json!({"password":"replacement"}),
            )
            .await;
            let response = assert_response_status(response, StatusCode::FORBIDDEN).await;
            let body: Value = test::read_body_json(response).await;
            assert_eq!(body["reason"], "reauthentication_required");
            return;
        }
        _ => (
            "/api/v1/restores/1/confirm".to_string(),
            json!({"restore_capability":"secret", "sha256":"digest", "confirmation":"REPLACE ALL HUBUUM DATA"}),
        ),
    };
    let response = post_request(&c.pool, &c.admin_token, &path, body).await;
    let response = assert_response_status(response, StatusCode::FORBIDDEN).await;
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["reason"], "reauthentication_required");
}

#[rstest]
#[actix_web::test]
async fn wrong_password_cannot_create_approval(#[future(awt)] test_context: TestContext) {
    let c = test_context;
    let response = post_request(
        &c.pool,
        &c.normal_token,
        "/api/v1/iam/credential-approvals",
        json!({"password":"wrong", "operation":operation(&c)}),
    )
    .await;
    assert_response_status(response, StatusCode::UNAUTHORIZED).await;
}

#[rstest]
#[case(false)]
#[case(true)]
#[actix_web::test]
async fn approved_mint_binds_exact_payload(
    #[case] tamper: bool,
    #[future(awt)] test_context: TestContext,
) {
    let c = test_context;
    let approval = issue(&c, operation(&c)).await;
    let response = post_request_with_headers(&c.pool, &c.normal_token,
        &format!("/api/v1/iam/principals/{}/tokens", c.normal_user.id),
        json!({"name":if tamper { "different-client" } else { "approved-client" }, "expires_at":approval["token_expires_at"]}),
        vec![(HeaderName::from_static("x-hubuum-credential-approval"), approval["approval"].as_str().unwrap().into())]).await;
    assert_response_status(
        response,
        if tamper {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::CREATED
        },
    )
    .await;
    let response = get_request(
        &c.pool,
        &c.normal_token,
        &format!(
            "/api/v1/iam/credential-approvals/{}",
            approval["record"]["id"]
        ),
    )
    .await;
    let record: Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(record["consumed_at"].is_null(), tamper);
    assert!(record.get("secret_digest").is_none());
    assert!(record.get("request_digest").is_none());
}

#[rstest]
#[actix_web::test]
async fn approval_cannot_move_to_another_token_for_same_human(
    #[future(awt)] test_context: TestContext,
) {
    let c = test_context;
    let approval = issue(&c, operation(&c)).await;
    let other = c
        .normal_user
        .create_token(&c.pool)
        .await
        .unwrap()
        .get_token();
    let response = post_request_with_headers(
        &c.pool,
        &other,
        &format!("/api/v1/iam/principals/{}/tokens", c.normal_user.id),
        json!({"name":"approved-client", "expires_at":approval["token_expires_at"]}),
        vec![(
            HeaderName::from_static("x-hubuum-credential-approval"),
            approval["approval"].as_str().unwrap().into(),
        )],
    )
    .await;
    assert_response_status(response, StatusCode::FORBIDDEN).await;
}

#[rstest]
#[actix_web::test]
async fn approval_audit_records_creation_and_consumption_without_secrets(
    #[future(awt)] test_context: TestContext,
) {
    use hubuum_storage_postgres::diesel_async_prelude::*;
    let c = test_context;
    let approval = issue(&c, operation(&c)).await;
    let response = post_request_with_headers(
        &c.pool,
        &c.normal_token,
        &format!("/api/v1/iam/principals/{}/tokens", c.normal_user.id),
        json!({"name":"approved-client", "expires_at":approval["token_expires_at"]}),
        vec![(
            HeaderName::from_static("x-hubuum-credential-approval"),
            approval["approval"].as_str().unwrap().into(),
        )],
    )
    .await;
    assert_response_status(response, StatusCode::CREATED).await;
    let id = approval["record"]["id"].as_i64().unwrap() as i32;
    let events = hubuum_storage_postgres::with_connection(&c.pool, async |connection| {
        crate::schema::events::table
            .filter(crate::schema::events::entity_type.eq("credential_approval"))
            .filter(crate::schema::events::entity_id.eq(id))
            .order(crate::schema::events::id.asc())
            .select((
                crate::schema::events::action,
                crate::schema::events::after,
                crate::schema::events::metadata,
            ))
            .load::<(String, Option<Value>, Value)>(connection)
            .await
    })
    .await
    .unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.0.as_str())
            .collect::<Vec<_>>(),
        ["created", "succeeded"]
    );
    let encoded = serde_json::to_string(&events).unwrap();
    for secret in [
        "testpassword",
        "secret_digest",
        "request_digest",
        approval["approval"].as_str().unwrap(),
    ] {
        assert!(!encoded.contains(secret));
    }
}

#[rstest]
#[case(false, false)]
#[case(true, false)]
#[case(false, true)]
#[case(true, true)]
#[actix_web::test]
async fn credential_imports_require_approval_including_hashes_and_dry_runs(
    #[case] hashed: bool,
    #[case] dry_run: bool,
    #[future(awt)] test_context: TestContext,
) {
    let c = test_context;
    let mut principal = json!({"name":c.scoped_name("import_approval"),"kind":"human", "identity_scope_key":{"name":"local"}, "provider_managed":false});
    if hashed {
        principal["password_hash"] =
            json!(crate::utilities::auth::hash_password("imported-password").unwrap());
    } else {
        principal["password"] = json!("imported-password");
    }
    let response = post_request(
        &c.pool,
        &c.admin_token,
        "/api/v1/imports",
        json!({"version":2,"dry_run":dry_run,"graph":{"principals":[principal]}}),
    )
    .await;
    let response = assert_response_status(response, StatusCode::FORBIDDEN).await;
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["reason"], "reauthentication_required");
}
