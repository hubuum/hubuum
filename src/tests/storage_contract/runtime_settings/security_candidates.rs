use super::*;
use hubuum_storage_core::StorageUserCreate;
use std::sync::LazyLock;

const CANDIDATES: &str = "/api/v1/admin/runtime-settings/security-candidates";
const PASSWORD: &str = "candidate-test-password";

async fn password_admin(backend: &StorageHandle) -> BackendUserFixture {
    static HASH: LazyLock<String> =
        LazyLock::new(|| crate::utilities::auth::hash_password(PASSWORD).unwrap());
    let name = prefix("candidate_admin");
    let user = backend
        .create_user(StorageUserCreate::new(
            None,
            name,
            HASH.clone(),
            None,
            None,
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value();
    let id = user.into_parts().id();
    let principal_id = principal_id(id.id());
    let _ = backend
        .add_group_member(principal_id, group_id(1), &EventContext::system())
        .await
        .unwrap();
    let raw_token = prefix("candidate_token");
    let token_hash = crate::models::Token::storage_hash_from_raw(&raw_token);
    let _ = backend
        .create_token(StorageTokenCreate::new(
            principal_id,
            StorageTokenDigest::legacy_unidentified(&token_hash),
            StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
            EventContext::system(),
        ))
        .await
        .unwrap();
    BackendUserFixture {
        id,
        principal_id,
        raw_token,
        token_hash,
    }
}

async fn local_candidate(backend: &StorageHandle, user: &BackendUserFixture) -> Value {
    let (status, candidate)=send_json(backend,&user.raw_token,test::TestRequest::post().uri(CANDIDATES).set_json(json!({"group":"authorization","expected_revision":1,"backend":"local","source":"runtime"}))).await;
    assert_eq!(status, http::StatusCode::CREATED, "{candidate}");
    candidate
}

#[actix_web::test]
async fn verified_candidate_does_not_change_live_settings() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let candidate = local_candidate(&backend, &user).await;
    assert_eq!(candidate["state"], "verified");
    assert_eq!(
        current_revision(&backend, StorageRuntimeSettingsGroup::Authorization).await,
        ResourceRevision::INITIAL
    );
}

#[actix_web::test]
async fn verification_receipt_is_never_redisclosed() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let candidate = local_candidate(&backend, &user).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::get().uri(&format!(
            "{CANDIDATES}/{}",
            candidate["id"].as_str().unwrap()
        )),
    )
    .await;
    assert_eq!(
        response.headers().get(http::header::CACHE_CONTROL).unwrap(),
        "no-store"
    );
    let status: Value = test::read_body_json(response).await;
    assert!(status.get("verification_receipt").is_none());
}

#[actix_web::test]
async fn verified_candidate_still_requires_fresh_approval() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let candidate = local_candidate(&backend, &user).await;
    let response=send(&backend,Some(&user.raw_token),test::TestRequest::post().uri(&format!("{CANDIDATES}/{}/activate",candidate["id"].as_str().unwrap())).set_json(json!({"expected_revision":1,"verification_receipt":candidate["verification_receipt"]}))).await;
    assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
}

#[actix_web::test]
async fn activation_with_real_password_approval_is_single_use() {
    let backend = StorageHandle::memory();
    let user = password_admin(&backend).await;
    let candidate = local_candidate(&backend, &user).await;
    let (status,approval)=send_json(&backend,&user.raw_token,test::TestRequest::post().uri("/api/v1/iam/credential-approvals").set_json(json!({"password":PASSWORD,"operation":{"kind":"activate_security_candidate","candidate_id":candidate["id"],"group":"authorization","expected_revision":1}}))).await;
    assert_eq!(status, http::StatusCode::CREATED, "{approval}");
    let path = format!(
        "{CANDIDATES}/{}/activate",
        candidate["id"].as_str().unwrap()
    );
    let request = || {
        test::TestRequest::post().uri(&path).insert_header(("X-Hubuum-Credential-Approval",approval["approval"].as_str().unwrap())).set_json(json!({"expected_revision":1,"verification_receipt":candidate["verification_receipt"]}))
    };
    let (status, body) = send_json(&backend, &user.raw_token, request()).await;
    assert_eq!(status, http::StatusCode::OK, "{body}");
    let response = send(&backend, Some(&user.raw_token), request()).await;
    assert_eq!(response.status(), http::StatusCode::CONFLICT);
}

#[actix_web::test]
async fn candidate_status_is_bound_to_the_initiating_credential() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let candidate = local_candidate(&backend, &user).await;
    let other = admin(&backend).await;
    let response = send(
        &backend,
        Some(&other.raw_token),
        test::TestRequest::get().uri(&format!(
            "{CANDIDATES}/{}",
            candidate["id"].as_str().unwrap()
        )),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn stale_candidate_revision_is_rejected_before_provider_work() {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let response=send(&backend,Some(&user.raw_token),test::TestRequest::post().uri(CANDIDATES).set_json(json!({"group":"authorization","expected_revision":2,"backend":"treetop","source":"deployment"}))).await;
    assert_eq!(response.status(), http::StatusCode::PRECONDITION_FAILED);
}

#[rstest::rstest]
#[case::put("PUT","/api/v1/admin/runtime-settings/authorization",json!({"expected_revision":1,"backend":"local"}))]
#[case::patch("PATCH","/api/v1/admin/runtime-settings/authorization",json!({"expected_revision":1,"changes":{"backend":"local"}}))]
#[case::delete("DELETE","/api/v1/admin/runtime-settings/authorization?expected_revision=1",json!(null))]
#[case::legacy_preflight("POST","/api/v1/admin/runtime-settings/authentication/preflight",json!({"expected_revision":1,"providers":[]}))]
#[actix_web::test]
async fn direct_security_writes_require_the_candidate_workflow(
    #[case] method: &str,
    #[case] path: &str,
    #[case] body: Value,
) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::default()
            .method(method.parse().unwrap())
            .uri(path)
            .set_json(body),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::CONFLICT);
}

fn disabled_oidc() -> Value {
    json!({
        "name": "staged_oidc", "kind": "oidc", "source": "runtime", "enabled": false,
        "configuration": {
            "issuer": "https://identity.example/tenant", "client_id": "hubuum",
            "client_authentication": "client_secret_basic", "scopes": ["openid"],
            "signing_algorithms": ["RS256"],
            "membership": {"mode": "local_only", "refresh_interval_seconds": 300, "max_stale_seconds": 900},
            "admission": {"mode": "existing_only"}, "credential": {"action": "clear"}
        }
    })
}

fn disabled_oauth() -> Value {
    json!({
        "name": "staged_oauth", "kind": "oauth", "source": "runtime", "enabled": false,
        "configuration": {
            "profile": "json_user",
            "authorization_endpoint": "https://identity.example/authorize",
            "token_endpoint": "https://identity.example/token",
            "profile_endpoint": "https://identity.example/user",
            "client_id": "hubuum", "client_authentication": "client_secret_basic",
            "scopes": ["profile"], "subject_pointer": "/id", "subject_type": "string",
            "name_pointer": "/name",
            "membership": {"mode": "local_only", "refresh_interval_seconds": 300, "max_stale_seconds": 900},
            "admission": {"mode": "existing_only"}, "credential": {"action": "clear"}
        }
    })
}

#[rstest::rstest]
#[case::oidc(disabled_oidc())]
#[case::oauth(disabled_oauth())]
#[actix_web::test]
async fn disabled_browser_definition_can_be_staged_without_contacting_the_provider(
    #[case] provider: Value,
) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    let (status, candidate) = send_json(
        &backend,
        &user.raw_token,
        test::TestRequest::post().uri(CANDIDATES).set_json(json!({
            "group": "authentication", "expected_revision": 1, "providers": [provider]
        })),
    )
    .await;
    assert_eq!(status, http::StatusCode::CREATED, "{candidate}");
    assert_eq!(candidate["state"], "verified");
    assert!(candidate["providers"][0]["provider_id"].as_str().is_some());
    assert_eq!(
        current_revision(&backend, StorageRuntimeSettingsGroup::Authentication).await,
        ResourceRevision::INITIAL
    );
}

#[rstest::rstest]
#[case::plaintext_oidc_secret(disabled_oidc(), "client_secret", json!("must-not-be-accepted"))]
#[case::reserved_oidc_envelope(disabled_oidc(), "protected_credential", json!({"alias": "injected"}))]
#[case::plaintext_oauth_secret(disabled_oauth(), "client_secret", json!("must-not-be-accepted"))]
#[case::reserved_oauth_envelope(disabled_oauth(), "protected_credential", json!({"alias": "injected"}))]
#[case::invalid_algorithm(disabled_oidc(), "signing_algorithms", json!(["none"]))]
#[actix_web::test]
async fn invalid_browser_fields_fail_at_the_request_boundary(
    #[case] mut provider: Value,
    #[case] field: &str,
    #[case] value: Value,
) {
    let backend = StorageHandle::memory();
    let user = admin(&backend).await;
    provider["configuration"][field] = value;
    let response = send(
        &backend,
        Some(&user.raw_token),
        test::TestRequest::post().uri(CANDIDATES).set_json(
            json!({"group":"authentication","expected_revision":1,"providers":[provider]}),
        ),
    )
    .await;
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
}
