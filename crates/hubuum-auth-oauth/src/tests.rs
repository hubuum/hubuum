use super::*;
use hubuum_domain::{
    IdentityScopeName, InteractionAuthority, InteractionBinding, InteractionClientId,
    InteractionProviderId, ResourceRevision,
};
use rstest::rstest;
use serde_json::json;
use uuid::Uuid;

fn definition(github: bool) -> OAuthProviderDefinition {
    serde_json::from_value(if github {
        json!({"profile":"github", "authorization_endpoint":"https://github.com/login/oauth/authorize", "token_endpoint":"https://github.com/login/oauth/access_token", "profile_endpoint":"https://api.github.com/user", "client_id":"fixture", "client_authentication":"client_secret_post", "scopes":[], "subject_pointer":"/id", "subject_type":"integer", "name_pointer":"/login", "membership":{"mode":"local_only", "refresh_interval_seconds":300, "max_stale_seconds":900}, "admission":{"mode":"provision_without_grants"}})
    } else {
        json!({"profile":"json_user", "authorization_endpoint":"https://other.example/authorize", "token_endpoint":"https://other.example/token", "profile_endpoint":"https://other.example/identity", "client_id":"fixture", "client_authentication":"client_secret_basic", "scopes":["identity"], "profile_headers":{"x-profile-version":"2"}, "subject_pointer":"/account/key", "subject_type":"string", "name_pointer":"/account/display", "membership":{"mode":"provider_groups", "refresh_interval_seconds":300, "max_stale_seconds":900, "claim_source":"profile", "claim_pointer":"/roles", "key_mode":"immutable_id"}, "admission":{"mode":"provision_without_grants"}})
    }).unwrap()
}
fn fixture(github: bool) -> (OAuthAdapter, ExternalAuthenticationContext) {
    let definition = definition(github);
    let binding = InteractionBinding::try_new(
        InteractionId::new(Uuid::new_v4()).unwrap(),
        InteractionProviderId::external(Uuid::new_v4()).unwrap(),
        InteractionClientId::new("cli").unwrap(),
        ResourceRevision::new(1).unwrap(),
        InteractionAuthority::login(),
    )
    .unwrap();
    let context = ExternalAuthenticationContext::oauth(
        binding,
        IdentityScopeName::new("oauth").unwrap(),
        &definition,
    )
    .unwrap();
    (
        OAuthAdapter::new(
            definition,
            SecretValue::new(b"secret".to_vec()).unwrap(),
            "https://hubuum.example/callback/provider",
            false,
        )
        .unwrap(),
        context,
    )
}
fn user(github: bool) -> Value {
    if github {
        json!({"id":9007199254740993u64, "type":"User", "login":"octocat", "email":null, "site_admin":true})
    } else {
        json!({"account":{"key":"Case:001", "display":"Person"}, "roles":["staff"]})
    }
}
fn token() -> BasicTokenResponse {
    serde_json::from_value(json!({"access_token":"fixture-access", "token_type":"bearer"})).unwrap()
}

#[rstest]
#[case::github(true)]
#[case::different_mapping(false)]
#[tokio::test]
async fn code_exchange_and_authenticated_profile_are_bound(#[case] github: bool) {
    let (adapter, context) = fixture(github);
    let (url, pending) = adapter.begin(context.binding().id()).unwrap();
    let parameters: std::collections::HashMap<_, _> = url::Url::parse(&url)
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect();
    assert_eq!(parameters["code_challenge_method"], "S256");
    let challenge = parameters["code_challenge"].clone();
    let state = pending.state().to_owned();
    let pending = PendingLogin::restore(&pending.protectable().unwrap()).unwrap();
    let accepted = adapter
        .accept(
            pending,
            &state,
            github.then_some(GITHUB_ISSUER),
            Some("fixture-code"),
            None,
        )
        .unwrap();
    let calls = std::cell::Cell::new(0);
    let client = |request: oauth2::HttpRequest| {
        let call = calls.get();
        calls.set(call + 1);
        let document = if call == 0 {
            assert_eq!(
                request.uri().to_string(),
                adapter.definition.endpoints()[1].as_str()
            );
            assert_eq!(request.method(), http::Method::POST);
            let form: std::collections::HashMap<_, _> = url::form_urlencoded::parse(request.body())
                .into_owned()
                .collect();
            assert_eq!(form["code"], "fixture-code");
            assert_eq!(
                PkceCodeVerifier::new(form["code_verifier"].clone())
                    .secret()
                    .len(),
                43
            );
            assert_eq!(
                oauth2::PkceCodeChallenge::from_code_verifier_sha256(&PkceCodeVerifier::new(
                    form["code_verifier"].clone()
                ))
                .as_str(),
                challenge
            );
            assert_eq!(
                form["redirect_uri"],
                "https://hubuum.example/callback/provider"
            );
            if github {
                assert_eq!(form["client_secret"], "secret");
            } else {
                assert!(request.headers().contains_key("authorization"));
            }
            json!({"access_token":"fixture-access", "token_type":"bearer"})
        } else {
            assert_eq!(call, 1);
            assert_eq!(
                request.uri().to_string(),
                adapter.definition.endpoints()[2].as_str()
            );
            assert_eq!(request.headers()["authorization"], "Bearer fixture-access");
            if !github {
                assert_eq!(request.headers()["x-profile-version"], "2");
            }
            user(github)
        };
        std::future::ready(Ok(http::Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&document).unwrap())
            .unwrap()))
    };
    let result = adapter
        .exchange_using(accepted, &context, &client)
        .await
        .unwrap();
    assert_eq!(
        result.proof().identity().subject().as_str(),
        if github {
            "9007199254740993"
        } else {
            "Case:001"
        }
    );
    assert_eq!(calls.get(), 2);
}

#[rstest]
#[case::missing(Value::Null)]
#[case::string(json!("9007199254740993"))]
#[case::float(json!(1.0))]
#[case::zero(json!(0))]
#[case::negative(json!(-1))]
fn github_rejects_invalid_subject(#[case] subject: Value) {
    let (adapter, context) = fixture(true);
    let mut document = user(true);
    document["id"] = subject;
    assert!(adapter.verify_document(&document, &context).is_err());
}

#[rstest]
#[case::absent(None)]
#[case::private(Some(Value::Null))]
#[case::public(Some(json!("person@example.invalid")))]
fn email_and_site_admin_do_not_affect_identity_or_grants(#[case] email: Option<Value>) {
    let (adapter, context) = fixture(true);
    let mut document = user(true);
    if let Some(email) = email {
        document["email"] = email;
    } else {
        document.as_object_mut().unwrap().remove("email");
    }
    let proof = adapter.verify_document(&document, &context).unwrap();
    assert!(proof.memberships().provider_groups().is_none());
}

#[test]
fn username_changes_preserve_identity() {
    let (adapter, context) = fixture(true);
    let first = adapter.verify_document(&user(true), &context).unwrap();
    let mut renamed = user(true);
    renamed["login"] = json!("renamed");
    assert_eq!(
        first.identity(),
        adapter
            .verify_document(&renamed, &context)
            .unwrap()
            .identity()
    );
}

#[rstest]
#[case::github_pinned(true, Some("https://github.com/login/oauth"), true)]
#[case::github_missing(true, None, false)]
#[case::github_wrong_host(true, Some("https://attacker.example/login/oauth"), false)]
#[case::github_wrong_path(true, Some("https://github.com"), false)]
#[case::github_trailing_slash(true, Some("https://github.com/login/oauth/"), false)]
#[case::github_query(true, Some("https://github.com/login/oauth?issuer=github"), false)]
#[case::github_fragment(true, Some("https://github.com/login/oauth#issuer"), false)]
#[case::github_http(true, Some("http://github.com/login/oauth"), false)]
#[case::generic_without_issuer(false, None, true)]
#[case::generic_github_issuer(false, Some("https://github.com/login/oauth"), false)]
#[case::generic_own_issuer(false, Some("https://other.example"), false)]
fn callback_issuer_matches_profile(
    #[case] github: bool,
    #[case] issuer: Option<&str>,
    #[case] accepted: bool,
) {
    let (adapter, context) = fixture(github);
    let (_, pending) = adapter.begin(context.binding().id()).unwrap();
    let state = pending.state().to_owned();
    let pending = PendingLogin::restore(&pending.protectable().unwrap()).unwrap();
    assert_eq!(
        adapter
            .accept(pending, &state, issuer, Some("code"), None)
            .is_ok(),
        accepted
    );
}

#[rstest]
#[case::different_profile(false)]
#[case::different_callback(true)]
fn callback_must_match_the_configured_adapter(#[case] same_profile: bool) {
    let (original, context) = fixture(true);
    let (_, pending) = original.begin(context.binding().id()).unwrap();
    let state = pending.state().to_owned();
    let receiver = OAuthAdapter::new(
        definition(same_profile),
        SecretValue::new(b"secret".to_vec()).unwrap(),
        if same_profile {
            "https://hubuum.example/callback/another-provider"
        } else {
            "https://hubuum.example/callback/provider"
        },
        false,
    )
    .unwrap();
    assert!(
        receiver
            .accept(
                pending,
                &state,
                same_profile.then_some(GITHUB_ISSUER),
                Some("code"),
                None,
            )
            .is_err()
    );
}

#[rstest]
#[case::wrong_state("wrong", Some(GITHUB_ISSUER), Some("code"), None)]
#[case::issuer("actual", Some("https://attacker.example"), Some("code"), None)]
#[case::missing_code("actual", Some(GITHUB_ISSUER), None, None)]
#[case::error("actual", Some(GITHUB_ISSUER), Some("code"), Some("access_denied"))]
fn callback_requires_correlated_success(
    #[case] state: &str,
    #[case] issuer: Option<&str>,
    #[case] code: Option<&str>,
    #[case] error: Option<&str>,
) {
    let (adapter, context) = fixture(true);
    let (_, pending) = adapter.begin(context.binding().id()).unwrap();
    let actual = pending.state().to_owned();
    assert!(
        adapter
            .accept(
                pending,
                if state == "actual" { &actual } else { state },
                issuer,
                code,
                error
            )
            .is_err()
    );
}

#[rstest]
#[case::expired(true)]
#[case::different_destination(false)]
#[tokio::test]
async fn renewal_rejects_credentials_before_network(#[case] expired: bool) {
    let (adapter, context) = fixture(true);
    let mut credential = adapter.credentials(token()).unwrap();
    if expired {
        credential.expires = Some(Utc::now() - chrono::Duration::seconds(1));
    } else {
        credential.definition_digest = definition_digest(&definition(false));
    }
    let calls = std::cell::Cell::new(0);
    let client = |_: oauth2::HttpRequest| {
        calls.set(calls.get() + 1);
        std::future::ready(Err(OAuthError::Denied))
    };
    assert!(
        adapter
            .authenticate_using(credential, &context, &client)
            .await
            .is_err()
    );
    assert_eq!(calls.get(), 0);
}

#[rstest]
#[case::not_bearer(json!({"access_token":"access", "token_type":"mac"}))]
#[case::empty(json!({"access_token":"", "token_type":"bearer"}))]
#[case::scope(json!({"access_token":"access", "token_type":"bearer", "scope":"unrelated"}))]
fn rejects_unusable_token_response(#[case] document: Value) {
    let (adapter, _) = fixture(false);
    assert!(
        adapter
            .credentials(serde_json::from_value(document).unwrap())
            .is_err()
    );
}

#[rstest]
#[case::github(true)]
#[case::generic(false)]
#[tokio::test]
async fn revocation_uses_only_supported_profile(#[case] github: bool) {
    let (adapter, _) = fixture(github);
    let credentials = adapter.credentials(token()).unwrap();
    let calls = std::cell::Cell::new(0);
    let client = |request: oauth2::HttpRequest| {
        calls.set(calls.get() + 1);
        assert_eq!(request.method(), http::Method::DELETE);
        assert_eq!(
            request.uri(),
            "https://api.github.com/applications/fixture/token"
        );
        assert_eq!(
            request.headers()["authorization"],
            basic_auth("fixture", "secret")
        );
        let body: Value = serde_json::from_slice(request.body()).unwrap();
        assert_eq!(body["access_token"], "fixture-access");
        std::future::ready(Ok(http::Response::builder()
            .status(204)
            .body(Vec::new())
            .unwrap()))
    };
    assert_eq!(
        adapter.revoke_using(credentials, &client).await.unwrap(),
        github
    );
    assert_eq!(calls.get(), usize::from(github));
}

#[rstest]
#[case::changed_subject(true)]
#[case::missing_membership(false)]
#[tokio::test]
async fn renewal_requires_same_subject_and_complete_memberships(#[case] changed_subject: bool) {
    let (adapter, context) = fixture(false);
    let mut credentials = adapter.credentials(token()).unwrap();
    credentials.subject = Some("Case:001".into());
    let mut document = user(false);
    if changed_subject {
        document["account"]["key"] = json!("different");
    } else {
        document.as_object_mut().unwrap().remove("roles");
    }
    let client = |_: oauth2::HttpRequest| {
        std::future::ready(Ok(http::Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&document).unwrap())
            .unwrap()))
    };
    assert!(
        adapter
            .authenticate_using(credentials, &context, &client)
            .await
            .is_err()
    );
}

#[rstest]
#[case::primary(true, 403, Some(("x-ratelimit-remaining", "0")), "API rate limit exceeded", OAuthError::Unavailable)]
#[case::secondary_retry(true, 403, Some(("retry-after", "60")), "Please wait", OAuthError::Unavailable)]
#[case::secondary_message(
    true,
    403,
    None,
    "You have exceeded a secondary rate limit. Please wait a few minutes before you try again.",
    OAuthError::Unavailable
)]
#[case::forbidden(true, 403, Some(("x-ratelimit-remaining", "100")), "Resource not accessible", OAuthError::Denied)]
#[case::revoked(true, 401, Some(("x-ratelimit-remaining", "0")), "Bad credentials", OAuthError::Denied)]
#[case::generic_provider(false, 403, Some(("x-ratelimit-remaining", "0")), "Resource not accessible", OAuthError::Denied)]
#[tokio::test]
async fn renewal_distinguishes_github_rate_limits_from_rejection(
    #[case] github: bool,
    #[case] status: u16,
    #[case] header: Option<(&str, &str)>,
    #[case] message: &str,
    #[case] expected: OAuthError,
) {
    let (adapter, context) = fixture(github);
    let credentials = adapter.credentials(token()).unwrap();
    let client = |_: oauth2::HttpRequest| {
        let mut response = http::Response::builder()
            .status(status)
            .header("content-type", "application/json");
        if let Some((name, value)) = header {
            response = response.header(name, value);
        }
        std::future::ready(Ok(response
            .body(serde_json::to_vec(&json!({"message": message})).unwrap())
            .unwrap()))
    };
    assert_eq!(
        adapter
            .authenticate_using(credentials, &context, &client)
            .await
            .err(),
        Some(expected)
    );
}

#[rstest]
#[case::error_response(400, "application/json", json!({"error":"invalid_grant"}))]
#[case::wrong_type(200, "text/html", json!({"access_token":"access", "token_type":"bearer"}))]
#[tokio::test]
async fn exchange_rejects_provider_failures(
    #[case] status: u16,
    #[case] content_type: &str,
    #[case] body: Value,
) {
    let (adapter, context) = fixture(true);
    let (_, pending) = adapter.begin(context.binding().id()).unwrap();
    let state = pending.state().to_owned();
    let accepted = adapter
        .accept(pending, &state, Some(GITHUB_ISSUER), Some("code"), None)
        .unwrap();
    let client = |_: oauth2::HttpRequest| {
        std::future::ready(Ok(http::Response::builder()
            .status(status)
            .header("content-type", content_type)
            .body(serde_json::to_vec(&body).unwrap())
            .unwrap()))
    };
    assert!(
        adapter
            .exchange_using(accepted, &context, &client)
            .await
            .is_err()
    );
}

#[rstest]
#[case::authorization("authorization", "Bearer injected")]
#[case::host("host", "attacker.example")]
#[case::cookie("cookie", "secret")]
#[case::forwarded("x-forwarded-for", "127.0.0.1")]
#[case::newline("x-version", "value\r\ninjected: true")]
fn administrator_profile_headers_cannot_override_transport_or_bearer(
    #[case] name: &str,
    #[case] value: &str,
) {
    let mut definition = serde_json::to_value(definition(false)).unwrap();
    definition["profile_headers"] = json!({name:value});
    assert!(serde_json::from_value::<OAuthProviderDefinition>(definition).is_err());
}

#[test]
fn large_admission_policy_does_not_expand_protocol_storage() {
    let (mut adapter, context) = fixture(false);
    let mut definition = serde_json::to_value(&adapter.definition).unwrap();
    definition["admission"] = json!({"mode":"subject_allowlist", "subjects": (0..1000).map(|index| format!("{index:0255}")).collect::<Vec<_>>()});
    adapter.definition = serde_json::from_value(definition).unwrap();
    let (_, pending) = adapter.begin(context.binding().id()).unwrap();
    assert!(pending.protectable().unwrap().len() < 4096);
}

#[test]
fn github_comma_scopes_and_normalized_user_scope_are_supported() {
    let (mut adapter, _) = fixture(true);
    let mut definition = serde_json::to_value(&adapter.definition).unwrap();
    definition["scopes"] = json!(["read:user", "user:email"]);
    adapter.definition = serde_json::from_value(definition).unwrap();
    let response = serde_json::from_value(
        json!({"access_token":"access", "token_type":"bearer", "scope":"user,repo"}),
    )
    .unwrap();
    assert!(adapter.credentials(response).is_ok());
}
