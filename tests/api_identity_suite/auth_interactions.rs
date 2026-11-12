use std::net::{IpAddr, Ipv6Addr, SocketAddr};

use actix_web::{
    App,
    dev::ServiceResponse,
    http::{StatusCode, header},
    test,
};
use hubuum_storage_postgres::{PostgresStorageError, diesel_async_prelude::*, with_connection};
use rstest::rstest;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::tests::api_operations::{get_request, get_request_with_headers, post_request};
use crate::tests::asserts::assert_response_status;
use crate::tests::{TestContext, app_context};

struct Fixture {
    context: TestContext,
    client: String,
    peer: SocketAddr,
    begun: Value,
}
impl Fixture {
    async fn new() -> Self {
        let context = TestContext::new().await;
        let client = context.scoped_name("interaction-http");
        let peer = SocketAddr::new(
            IpAddr::V6(Ipv6Addr::from(*Uuid::new_v4().as_bytes())),
            31337,
        );
        let mut fixture = Self {
            context,
            client,
            peer,
            begun: Value::Null,
        };
        let response = fixture.call("/api/v1/auth/interactions", json!({"client_id":fixture.client,"provider_id":"local","purpose":"login","delivery":"poll"}), None, &[]).await;
        fixture.begun =
            test::read_body_json(assert_response_status(response, StatusCode::CREATED).await).await;
        fixture
    }
    fn endpoint(&self, action: &str) -> String {
        format!(
            "/api/v1/auth/interactions/{}/{action}",
            self.begun["id"].as_str().unwrap()
        )
    }
    fn authorization(&self) -> String {
        format!(
            "Hubuum-Interaction {}",
            self.begun["redemption_secret"].as_str().unwrap()
        )
    }
    async fn call(
        &self,
        path: &str,
        body: Value,
        authorization: Option<String>,
        extra: &[(&str, &str)],
    ) -> ServiceResponse {
        let app = test::init_service(
            App::new()
                .wrap(crate::middlewares::TracingMiddleware::new())
                .app_data(app_context(&self.context.pool))
                .configure(crate::api::config),
        )
        .await;
        let mut request = test::TestRequest::post()
            .uri(path)
            .peer_addr(self.peer)
            .set_json(body);
        if let Some(value) = authorization {
            request = request.insert_header((header::AUTHORIZATION, value));
        }
        for (name, value) in extra {
            request = request.append_header((*name, *value));
        }
        let response = request.send_request(&app).await.map_into_boxed_body();
        assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
        assert_eq!(
            response.headers().get("referrer-policy").unwrap(),
            "no-referrer"
        );
        response
    }
    async fn action(&self, action: &str) -> ServiceResponse {
        self.call(
            &self.endpoint(action),
            json!({"client_id":self.client}),
            Some(self.authorization()),
            &[],
        )
        .await
    }
    async fn credentials(&self, password: &str) -> ServiceResponse {
        self.call(&self.endpoint("credentials"), json!({"client_id":self.client,"name":self.context.normal_user.name(&self.context.pool).await.unwrap(),"password":password}), Some(self.authorization()), &[]).await
    }
    async fn ready(&self) {
        let result: Value = test::read_body_json(
            assert_response_status(self.credentials("testpassword").await, StatusCode::OK).await,
        )
        .await;
        assert_eq!(result["state"], "ready");
        assert!(result.get("access_token").is_none());
    }
    async fn session(&self) -> Value {
        self.ready().await;
        test::read_body_json(
            assert_response_status(self.action("complete").await, StatusCode::OK).await,
        )
        .await
    }
    async fn cleanup(self) {
        with_connection(&self.context.pool, async |connection| {
            diesel::sql_query("DELETE FROM auth_interactions WHERE client_id=$1")
                .bind::<diesel::sql_types::Text, _>(self.client)
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
    }
}

async fn reason(response: ServiceResponse, status: StatusCode, expected: &str) {
    let body: Value = test::read_body_json(assert_response_status(response, status).await).await;
    assert_eq!(body["reason"], expected);
}

#[actix_web::test]
async fn discovery_exposes_only_public_password_method_metadata() {
    let context = TestContext::new().await;
    let response = get_request_with_headers(
        &context.pool,
        "",
        "/api/v1/auth/methods",
        vec![
            (header::ORIGIN, "https://frontend.example".into()),
            (
                header::HeaderName::from_static("sec-fetch-site"),
                "same-site".into(),
            ),
        ],
    )
    .await;
    let methods: Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(methods["interaction_version"], 1);
    assert_eq!(
        methods["methods"][0],
        json!({"provider_id":"local","name":"local","kind":"local","interaction":"credentials","fresh_authentication":true})
    );
    for method in methods["methods"].as_array().unwrap() {
        assert_eq!(method.as_object().unwrap().len(), 5);
        assert!(matches!(method["kind"].as_str().unwrap(), "local" | "ldap"));
    }
}

#[actix_web::test]
async fn completion_issues_existing_session_with_the_original_client_binding() {
    let fixture = Fixture::new().await;
    let session = fixture.session().await;
    assert_response_status(
        get_request(
            &fixture.context.pool,
            session["access_token"].as_str().unwrap(),
            "/api/v1/auth/validate",
        )
        .await,
        StatusCode::OK,
    )
    .await;
    assert_response_status(
        post_request(
            &fixture.context.pool,
            "",
            "/api/v1/auth/refresh",
            json!({"client_id":"wrong-client","refresh_token":session["refresh_token"]}),
        )
        .await,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    let rotated: Value = test::read_body_json(
        assert_response_status(
            post_request(
                &fixture.context.pool,
                "",
                "/api/v1/auth/refresh",
                json!({"client_id":fixture.client,"refresh_token":session["refresh_token"]}),
            )
            .await,
            StatusCode::OK,
        )
        .await,
    )
    .await;
    assert_eq!(rotated["session_id"], session["session_id"]);
    fixture.cleanup().await;
}

#[actix_web::test]
async fn concurrent_http_completion_has_one_winner() {
    let fixture = Fixture::new().await;
    fixture.ready().await;
    let (one, two) = tokio::join!(fixture.action("complete"), fixture.action("complete"));
    let (winner, loser) = if one.status() == StatusCode::OK {
        (one, two)
    } else {
        (two, one)
    };
    assert_response_status(winner, StatusCode::OK).await;
    reason(loser, StatusCode::CONFLICT, "interaction_consumed").await;
    fixture.cleanup().await;
}

#[actix_web::test]
async fn lost_response_cleanup_revokes_only_the_issued_family_and_rotated_descendants() {
    let fixture = Fixture::new().await;
    let session = fixture.session().await;
    let rotated: Value = test::read_body_json(
        assert_response_status(
            post_request(
                &fixture.context.pool,
                "",
                "/api/v1/auth/refresh",
                json!({"client_id":fixture.client,"refresh_token":session["refresh_token"]}),
            )
            .await,
            StatusCode::OK,
        )
        .await,
    )
    .await;
    let status: Value = test::read_body_json(
        assert_response_status(fixture.action("status").await, StatusCode::OK).await,
    )
    .await;
    assert_eq!(
        status,
        json!({"id":fixture.begun["id"],"expires_at":fixture.begun["expires_at"],"state":"consumed","session_id":session["session_id"]})
    );
    assert_response_status(
        fixture.action("issued-session/revoke").await,
        StatusCode::NO_CONTENT,
    )
    .await;
    assert_response_status(
        fixture.action("issued-session/revoke").await,
        StatusCode::NO_CONTENT,
    )
    .await;
    assert_response_status(
        get_request(
            &fixture.context.pool,
            rotated["access_token"].as_str().unwrap(),
            "/api/v1/auth/validate",
        )
        .await,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    assert_response_status(
        get_request(
            &fixture.context.pool,
            &fixture.context.normal_token,
            "/api/v1/auth/validate",
        )
        .await,
        StatusCode::OK,
    )
    .await;
    fixture.cleanup().await;
}

#[rstest]
#[case::secret("secret")]
#[case::client("client")]
#[case::id("id")]
#[case::missing("missing")]
#[case::bearer("bearer")]
#[case::duplicate("duplicate")]
#[actix_web::test]
async fn invalid_authority_has_one_non_disclosing_response(#[case] invalid: &str) {
    let fixture = Fixture::new().await;
    let secret = format!("Hubuum-Interaction hai1.{}", "0".repeat(64));
    let path = if invalid == "id" {
        format!("/api/v1/auth/interactions/{}/status", Uuid::new_v4())
    } else {
        fixture.endpoint("status")
    };
    let auth = match invalid {
        "missing" => None,
        "secret" => Some(secret),
        "bearer" => Some(format!(
            "Bearer {}",
            fixture.begun["redemption_secret"].as_str().unwrap()
        )),
        _ => Some(fixture.authorization()),
    };
    let extras = if invalid == "duplicate" {
        vec![("authorization", "Hubuum-Interaction duplicate")]
    } else {
        vec![]
    };
    let response = fixture
        .call(
            &path,
            json!({"client_id":if invalid=="client" { "other-client" } else { &fixture.client }}),
            auth,
            &extras,
        )
        .await;
    reason(response, StatusCode::UNAUTHORIZED, "interaction_invalid").await;
    fixture.cleanup().await;
}

#[actix_web::test]
async fn cancellation_is_repeatable_and_prevents_issuance() {
    let fixture = Fixture::new().await;
    assert_response_status(fixture.action("cancel").await, StatusCode::NO_CONTENT).await;
    assert_response_status(fixture.action("cancel").await, StatusCode::NO_CONTENT).await;
    reason(
        fixture.action("complete").await,
        StatusCode::FORBIDDEN,
        "interaction_cancelled",
    )
    .await;
    fixture.cleanup().await;
}

#[actix_web::test]
async fn premature_completion_does_not_consume_password_authentication() {
    let fixture = Fixture::new().await;
    reason(
        fixture.action("complete").await,
        StatusCode::CONFLICT,
        "interaction_pending",
    )
    .await;
    fixture.session().await;
    fixture.cleanup().await;
}

#[actix_web::test]
async fn early_polls_commit_increasing_retry_after() {
    let fixture = Fixture::new().await;
    let first = fixture.action("status").await;
    assert_eq!(first.headers().get("retry-after").unwrap(), "10");
    reason(first, StatusCode::TOO_MANY_REQUESTS, "slow_down").await;
    let second = fixture.action("status").await;
    assert_eq!(second.headers().get("retry-after").unwrap(), "15");
    reason(second, StatusCode::TOO_MANY_REQUESTS, "slow_down").await;
    fixture.cleanup().await;
}

#[actix_web::test]
async fn failed_password_releases_the_claim_for_an_explicit_retry() {
    let fixture = Fixture::new().await;
    reason(
        fixture.credentials("wrong-password").await,
        StatusCode::UNAUTHORIZED,
        "interaction_invalid",
    )
    .await;
    fixture.session().await;
    fixture.cleanup().await;
}

#[actix_web::test]
async fn three_failed_passwords_make_the_interaction_terminal() {
    let fixture = Fixture::new().await;
    for _ in 0..3 {
        reason(
            fixture.credentials("wrong-password").await,
            StatusCode::UNAUTHORIZED,
            "interaction_invalid",
        )
        .await;
    }
    reason(
        fixture.credentials("testpassword").await,
        StatusCode::CONFLICT,
        "interaction_failed",
    )
    .await;
    fixture.cleanup().await;
}

#[rstest]
#[case::provider(json!({"provider_id":"11111111-1111-4111-8111-111111111111"}), 422)]
#[case::bff(json!({"delivery":"bff","return_target_id":"frontend","return_nonce":"01234567890123456789012345678901"}), 422)]
#[case::verification(json!({"purpose":"provider_verification","candidate_id":"11111111-1111-4111-8111-111111111111"}), 422)]
#[case::approval(json!({"purpose":"credential_approval","operation":{"kind":"create_token","principal_id":1,"token":{}}}), 422)]
#[case::login_with_candidate(json!({"candidate_id":"11111111-1111-4111-8111-111111111111"}), 400)]
#[case::poll_with_return(json!({"return_target_id":"frontend"}), 400)]
#[actix_web::test]
async fn unsupported_or_mismatched_begin_cannot_downgrade(
    #[case] patch: Value,
    #[case] status: u16,
) {
    let fixture = Fixture::new().await;
    let mut body = json!({"client_id":fixture.client,"provider_id":"local","purpose":"login","delivery":"poll"});
    body.as_object_mut()
        .unwrap()
        .extend(patch.as_object().unwrap().clone());
    let response = fixture
        .call("/api/v1/auth/interactions", body, None, &[])
        .await;
    if status == 422 {
        reason(
            response,
            StatusCode::UNPROCESSABLE_ENTITY,
            "capability_unsupported",
        )
        .await;
    } else {
        assert_response_status(response, StatusCode::from_u16(status).unwrap()).await;
    }
    fixture.cleanup().await;
}

#[rstest]
#[case::origin("origin", "https://attacker.example")]
#[case::browser_fetch("sec-fetch-site", "same-origin")]
#[actix_web::test]
async fn browser_javascript_cannot_receive_interaction_credentials(
    #[case] name: &str,
    #[case] value: &str,
) {
    let fixture = Fixture::new().await;
    fixture.ready().await;
    let response = fixture
        .call(
            &fixture.endpoint("complete"),
            json!({"client_id":fixture.client}),
            Some(fixture.authorization()),
            &[(name, value)],
        )
        .await;
    assert_response_status(response, StatusCode::BAD_REQUEST).await;
    assert_response_status(fixture.action("complete").await, StatusCode::OK).await;
    fixture.cleanup().await;
}

#[actix_web::test]
async fn redemption_secret_is_not_a_normal_api_bearer_token() {
    let fixture = Fixture::new().await;
    assert_response_status(
        get_request(
            &fixture.context.pool,
            fixture.begun["redemption_secret"].as_str().unwrap(),
            "/api/v1/auth/validate",
        )
        .await,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    fixture.cleanup().await;
}

#[actix_web::test]
async fn completion_audit_resolves_actor_from_proof_and_retains_request_correlation() {
    let fixture = Fixture::new().await;
    fixture.ready().await;
    assert_response_status(
        fixture
            .call(
                &fixture.endpoint("complete"),
                json!({"client_id":fixture.client}),
                Some(fixture.authorization()),
                &[("x-correlation-id", "interaction-audit")],
            )
            .await,
        StatusCode::OK,
    )
    .await;
    let events = crate::test_support::audit_events(
        &fixture.context.pool,
        crate::events::EntityType::Authentication,
        fixture.context.normal_user.id,
        None,
    )
    .await
    .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].actor_user_id,
        Some(fixture.context.normal_user.id)
    );
    assert_eq!(events[0].actor_kind, "user");
    assert_eq!(
        events[0].correlation_id.as_deref(),
        Some("interaction-audit")
    );
    fixture.cleanup().await;
}

#[rstest]
#[case::provider_override(json!({"identity_scope":"other"}))]
#[case::empty_password(json!({"password":""}))]
#[case::too_long_password(json!({"password":"x".repeat(4097)}))]
#[case::empty_name(json!({"name":""}))]
#[actix_web::test]
async fn malformed_credentials_do_not_claim_an_attempt(#[case] patch: Value) {
    let fixture = Fixture::new().await;
    let mut body = json!({"client_id":fixture.client,"name":fixture.context.normal_user.name(&fixture.context.pool).await.unwrap(),"password":"testpassword"});
    body.as_object_mut()
        .unwrap()
        .extend(patch.as_object().unwrap().clone());
    for _ in 0..3 {
        assert_response_status(
            fixture
                .call(
                    &fixture.endpoint("credentials"),
                    body.clone(),
                    Some(fixture.authorization()),
                    &[],
                )
                .await,
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    fixture.session().await;
    fixture.cleanup().await;
}
