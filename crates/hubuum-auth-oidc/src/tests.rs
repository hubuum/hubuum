use super::*;
use hubuum_domain::{
    IdentityScopeName, InteractionAuthority, InteractionBinding, InteractionClientId,
    InteractionId, InteractionProviderId, ResourceRevision,
};
use openidconnect::{JsonWebKeyId, PrivateSigningKey, core::CoreRsaPrivateSigningKey};
use rstest::rstest;
use serde_json::json;
use uuid::Uuid;

fn definition() -> OidcProviderDefinition {
    serde_json::from_value(json!({
        "issuer":"https://id.example/realm", "client_id":"hubuum", "client_authentication":"client_secret_basic",
        "scopes":["openid","profile"], "signing_algorithms":["RS256"],
        "membership":{"mode":"provider_groups","refresh_interval_seconds":300,"max_stale_seconds":900,
            "claim_source":"id_token","claim_pointer":"/groups","key_mode":"full_path"},
        "admission":{"mode":"provision_without_grants"}
    })).unwrap()
}
fn metadata() -> Value {
    json!({"issuer":"https://id.example/realm", "authorization_endpoint":"https://id.example/auth",
        "token_endpoint":"https://id.example/token", "jwks_uri":"https://id.example/certs",
        "response_types_supported":["code"], "subject_types_supported":["public"],
        "id_token_signing_alg_values_supported":["RS256"], "code_challenge_methods_supported":["S256"],
        "response_modes_supported":["query"], "token_endpoint_auth_methods_supported":["client_secret_basic"]})
}
fn fixture() -> (OidcAdapter, Discovered, ExternalAuthenticationContext) {
    let definition = definition();
    let adapter = OidcAdapter::new(
        definition.clone(),
        SecretValue::new(b"secret".to_vec()).unwrap(),
        &hubuum_domain::BrowserCallbackUrl::new("https://hubuum.example/callback", false).unwrap(),
        false,
    )
    .unwrap();
    let metadata: CoreProviderMetadata = serde_json::from_value(metadata()).unwrap();
    let key = signing_key("current");
    let discovered = Discovered {
        client: CoreClient::from_provider_metadata(
            metadata.set_jwks(CoreJsonWebKeySet::new(vec![key.as_verification_key()])),
            ClientId::new("hubuum".into()),
            Some(ClientSecret::new("secret".into())),
        ),
        userinfo: None,
        revocation: None,
        response_issuer: true,
    };
    let binding = InteractionBinding::try_new(
        InteractionId::new(Uuid::new_v4()).unwrap(),
        InteractionProviderId::external(Uuid::new_v4()).unwrap(),
        InteractionClientId::new("cli").unwrap(),
        ResourceRevision::new(1).unwrap(),
        InteractionAuthority::login(),
    )
    .unwrap();
    let context = ExternalAuthenticationContext::oidc(
        binding,
        IdentityScopeName::new("company").unwrap(),
        &definition,
    )
    .unwrap();
    (adapter, discovered, context)
}
fn retained_credentials(
    adapter: &OidcAdapter,
    auth_time: Option<DateTime<Utc>>,
) -> RetainedCredentials {
    RetainedCredentials {
        refresh: "original".into(),
        nonce: "nonce".into(),
        subject: "stable-subject".into(),
        issuer: adapter.definition.issuer().as_str().into(),
        client_id: adapter.definition.client_id().into(),
        auth_time,
    }
}

fn signing_key(id: &str) -> CoreRsaPrivateSigningKey {
    // Public, disposable test fixture; never a production signing credential.
    CoreRsaPrivateSigningKey::from_pem(
        include_str!("../tests/fixtures/signing.pem"),
        Some(JsonWebKeyId::new(id.into())),
    )
    .unwrap()
}
fn claims() -> Value {
    let now = Utc::now().timestamp();
    json!({"iss":"https://id.example/realm", "aud":"hubuum", "sub":"stable-subject", "nonce":"nonce",
        "iat":now, "exp":now+300, "auth_time":now, "preferred_username":"display-name", "groups":["/engineering"]})
}
fn response(
    claims: Value,
    key: &str,
    algorithm: CoreJwsSigningAlgorithm,
    tamper: bool,
) -> CoreTokenResponse {
    signed_response(&claims.to_string(), key, algorithm, tamper)
}

fn signed_response(
    payload: &str,
    key: &str,
    algorithm: CoreJwsSigningAlgorithm,
    tamper: bool,
) -> CoreTokenResponse {
    let header = json!({"alg": algorithm, "kid": key});
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(payload)
    );
    let signature = signing_key(key).sign(&algorithm, input.as_bytes()).unwrap();
    let mut token = format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature));
    if tamper {
        let start = token.rfind('.').unwrap() + 1;
        let replacement = if &token[start..start + 1] == "A" {
            "B"
        } else {
            "A"
        };
        token.replace_range(start..start + 1, replacement);
    }
    serde_json::from_value(json!({"access_token":"access", "refresh_token":"refresh", "token_type":"Bearer", "id_token":token})).unwrap()
}

#[tokio::test]
async fn signed_additional_claims_produce_complete_identity_proof() {
    let (adapter, discovered, context) = fixture();
    let result = adapter
        .verify(
            &discovered,
            response(
                claims(),
                "current",
                CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
                false,
            ),
            "nonce",
            &context,
            None,
        )
        .await
        .unwrap();
    let proof = result.proof();
    assert_eq!(proof.identity().subject().as_str(), "stable-subject");
    assert_eq!(
        proof.memberships().provider_groups().unwrap()[0].key(),
        "/engineering"
    );
}

#[rstest]
#[case::groups("/groups", r#""groups":["/team"],"groups":[]"#)]
#[case::nested("/account/groups", r#""account":{"groups":["/team"],"groups":[]}"#)]
#[tokio::test]
async fn signed_memberships_require_unique_claims(#[case] pointer: &str, #[case] members: &str) {
    let (mut adapter, discovered, original_context) = fixture();
    let mut config = serde_json::to_value(&adapter.definition).unwrap();
    config["membership"]["claim_pointer"] = json!(pointer);
    adapter.definition = serde_json::from_value(config).unwrap();
    let context = ExternalAuthenticationContext::oidc(
        original_context.binding().clone(),
        IdentityScopeName::new("company").unwrap(),
        &adapter.definition,
    )
    .unwrap();
    let mut claims = claims();
    claims.as_object_mut().unwrap().remove("groups");
    let mut payload = claims.to_string();
    payload.pop();
    payload.push_str(&format!(",{members}}}"));
    let tokens = signed_response(
        &payload,
        "current",
        CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
        false,
    );
    assert!(matches!(
        adapter
            .verify(&discovered, tokens, "nonce", &context, None)
            .await,
        Err(OidcError::InvalidResponse)
    ));
}

#[rstest]
#[case::issuer("iss", json!("https://other.example/realm"))]
#[case::issuer_spelling("iss", json!("https://id.example/realm/"))]
#[case::audience("aud", json!("other-client"))]
#[case::additional_audience("aud", json!(["hubuum","other"]))]
#[case::authorized_party("azp", json!("other-client"))]
#[case::nonce("nonce", json!("wrong-nonce"))]
#[case::expired("exp", json!(1))]
#[case::future_issued("iat", json!(4_000_000_000i64))]
#[case::not_before("nbf", json!(4_000_000_000i64))]
#[case::missing_groups("groups", Value::Null)]
#[case::realm_role_not_path("groups", json!(["admin"]))]
#[case::distributed_claims("_claim_names", json!({"groups":"remote"}))]
#[tokio::test]
async fn rejects_signed_but_invalid_claims(#[case] field: &str, #[case] value: Value) {
    let (adapter, discovered, context) = fixture();
    let mut claims = claims();
    claims[field] = value;
    assert!(
        adapter
            .verify(
                &discovered,
                response(
                    claims,
                    "current",
                    CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
                    false
                ),
                "nonce",
                &context,
                None
            )
            .await
            .is_err()
    );
}

#[rstest]
#[case::unknown_key("rotated", CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256, false)]
#[case::unaccepted_algorithm("current", CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha384, false)]
#[case::signature("current", CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256, true)]
#[tokio::test]
async fn rejects_untrusted_signature(
    #[case] key: &str,
    #[case] algorithm: CoreJwsSigningAlgorithm,
    #[case] tamper: bool,
) {
    let (adapter, discovered, context) = fixture();
    assert!(
        adapter
            .verify(
                &discovered,
                response(claims(), key, algorithm, tamper),
                "nonce",
                &context,
                None
            )
            .await
            .is_err()
    );
}

#[rstest]
#[case::issuer("issuer", json!("https://other.example/realm"))]
#[case::malformed_issuer_capability("authorization_response_iss_parameter_supported", json!("true"))]
#[case::http_token("token_endpoint", json!("http://id.example/token"))]
#[case::fragment("jwks_uri", json!("https://id.example/certs#part"))]
#[case::pkce_plain("code_challenge_methods_supported", json!(["plain"]))]
#[case::implicit_only("response_types_supported", json!(["token"]))]
#[case::null_modes("response_modes_supported", Value::Null)]
#[case::malformed_modes("response_modes_supported", json!(["query",1]))]
#[case::fragment_only("response_modes_supported", json!(["fragment"]))]
#[case::secret_post_only("token_endpoint_auth_methods_supported", json!(["client_secret_post"]))]
#[case::unaccepted_algorithm("id_token_signing_alg_values_supported", json!(["HS256"]))]
fn rejects_unqualified_discovery(#[case] field: &str, #[case] value: Value) {
    let mut metadata = metadata();
    metadata[field] = value;
    assert!(validate_metadata(&metadata, &definition()).is_err());
}

#[rstest]
#[case::wrong_state("wrong", Some("https://id.example/realm"), Some("code"), None)]
#[case::issuer_mixup("state", Some("https://other.example/realm"), Some("code"), None)]
#[case::missing_issuer("state", None, Some("code"), None)]
#[case::denial("state", Some("https://id.example/realm"), None, Some("access_denied"))]
#[case::code_and_error(
    "state",
    Some("https://id.example/realm"),
    Some("code"),
    Some("access_denied")
)]
fn callback_requires_correlated_success(
    #[case] state: &str,
    #[case] issuer: Option<&str>,
    #[case] code: Option<&str>,
    #[case] error: Option<&str>,
) {
    let pending = PendingLogin {
        code_state: serde_json::from_value(json!({"state":"state","verifier":"verifier"})).unwrap(),
        nonce: "nonce".into(),
        issuer: "https://id.example/realm".into(),
        client_id: "hubuum".into(),
        callback: "https://hubuum.example/callback".into(),
        response_issuer: true,
    };
    assert!(pending.accept(state, issuer, code, error).is_err());
}

#[rstest]
#[case::original_nonce(Some("nonce"), true)]
#[case::omitted_nonce(None, true)]
#[case::different_nonce(Some("another-nonce"), false)]
#[tokio::test]
async fn refresh_nonce_remains_bound_to_original_authentication(
    #[case] nonce: Option<&str>,
    #[case] accepted: bool,
) {
    let (adapter, discovered, context) = fixture();
    let mut claims = claims();
    let auth_time = DateTime::from_timestamp(claims["auth_time"].as_i64().unwrap(), 0);
    if let Some(nonce) = nonce {
        claims["nonce"] = json!(nonce);
    } else {
        claims.as_object_mut().unwrap().remove("nonce");
    }
    assert_eq!(
        adapter
            .verify(
                &discovered,
                response(
                    claims,
                    "current",
                    CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
                    false
                ),
                "nonce",
                &context,
                Some(("stable-subject", auth_time))
            )
            .await
            .is_ok(),
        accepted
    );
}

#[rstest]
#[case::subject("different-subject", false)]
#[case::authentication_time("stable-subject", true)]
#[tokio::test]
async fn refresh_cannot_replace_identity_or_assert_new_authentication(
    #[case] subject: &str,
    #[case] change_time: bool,
) {
    let (adapter, discovered, context) = fixture();
    let claims = claims();
    let time = claims["auth_time"].as_i64().unwrap() - i64::from(change_time);
    assert!(
        adapter
            .verify(
                &discovered,
                response(
                    claims,
                    "current",
                    CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
                    false
                ),
                "nonce",
                &context,
                Some((subject, DateTime::from_timestamp(time, 0)))
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn initial_login_requires_nonce() {
    let (adapter, discovered, context) = fixture();
    let mut claims = claims();
    claims.as_object_mut().unwrap().remove("nonce");
    assert!(
        adapter
            .verify(
                &discovered,
                response(
                    claims,
                    "current",
                    CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
                    false
                ),
                "nonce",
                &context,
                None
            )
            .await
            .is_err()
    );
}

#[rstest]
#[case::malformed_id_token(200, r#"{"access_token":"access","token_type":"Bearer","refresh_token":"replacement","id_token":"malformed"}"#, OidcError::InvalidResponse)]
#[case::contradictory_grant(200, r#"{"error":"invalid_grant","access_token":"access","token_type":"Bearer","refresh_token":"replacement","id_token":"malformed"}"#, OidcError::InvalidResponse)]
#[case::missing_id_token(200, r#"{"error":"invalid_grant","access_token":"access","token_type":"Bearer","refresh_token":"replacement"}"#, OidcError::InvalidResponse)]
#[case::non_rotating(
    200,
    r#"{"access_token":"access","token_type":"Bearer","id_token":"malformed"}"#,
    OidcError::InvalidResponse
)]
#[case::duplicate_refresh_token(200, r#"{"access_token":"access","token_type":"Bearer","refresh_token":"one","refresh_token":"two"}"#, OidcError::InvalidResponse)]
#[case::mixed_error_and_refresh(
    200,
    r#"{"error":"invalid_grant","refresh_token":"replacement"}"#,
    OidcError::InvalidResponse
)]
#[case::error_only(200, r#"{"error":"temporarily_unavailable"}"#, OidcError::Unavailable)]
#[case::error_status(400, r#"{"error":"invalid_grant"}"#, OidcError::Denied)]
#[case::ambiguous_outage(
    200,
    r#"{"error":"unavailable","error":"unavailable"}"#,
    OidcError::Unavailable
)]
#[case::unreadable_endpoint(200, "<html>unavailable", OidcError::Unavailable)]
#[tokio::test]
async fn refresh_classifies_wire_responses(
    #[case] status: u16,
    #[case] wire: &'static str,
    #[case] failure: OidcError,
) {
    let (adapter, discovered, context) = fixture();
    let result = adapter
        .refresh_using(
            &discovered,
            retained_credentials(&adapter, None),
            &context,
            |_| {
                std::future::ready(Ok(openidconnect::http::Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(wire.as_bytes().to_vec())
                    .unwrap()))
            },
        )
        .await;
    assert_eq!(result.err(), Some(failure));
}

#[rstest]
#[case::grant(false, &["application/json"], None)]
#[case::parameters(false, &["Application/JSON; charset=UTF-8"], None)]
#[case::parameter_whitespace(false, &["application/json ; charset=UTF-8"], None)]
#[case::quoted_parameter(false, &[r#"application/json; note="a,b""#], None)]
#[case::combined_types(false, &["application/json; charset=utf-8, text/plain"], Some(OidcError::Unavailable))]
#[case::contradictory_grant(true, &["application/json"], Some(OidcError::InvalidResponse))]
#[case::contradictory_wrong_type(true, &["text/plain"], Some(OidcError::InvalidResponse))]
#[case::incorrect_media_type(false, &["text/plain"], Some(OidcError::Unavailable))]
#[case::different_json_format(false, &["application/json-seq"], Some(OidcError::Unavailable))]
#[case::missing_media_type(false, &[], Some(OidcError::Unavailable))]
#[case::ambiguous_media_type(false, &["text/plain", "application/json"], Some(OidcError::Unavailable))]
#[tokio::test]
async fn classifies_decodable_refresh_grants(
    #[case] error_member: bool,
    #[case] content_types: &[&str],
    #[case] expected: Option<OidcError>,
) {
    let (adapter, discovered, context) = fixture();
    let claims = claims();
    let retained = retained_credentials(
        &adapter,
        DateTime::from_timestamp(claims["auth_time"].as_i64().unwrap(), 0),
    );
    let mut wire = serde_json::to_value(response(
        claims,
        "current",
        CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
        false,
    ))
    .unwrap();
    if error_member {
        wire["error"] = json!("invalid_grant");
    }
    let result = adapter
        .refresh_using(&discovered, retained, &context, |_| {
            let mut reply = openidconnect::http::Response::builder().status(200);
            for value in content_types {
                reply = reply.header("content-type", *value);
            }
            std::future::ready(Ok(reply.body(serde_json::to_vec(&wire).unwrap()).unwrap()))
        })
        .await;
    assert_eq!(result.err(), expected);
}

#[rstest]
#[case::rotated(Some("replacement"), "replacement")]
#[case::non_rotating(None, "original")]
#[tokio::test]
async fn verified_refresh_retains_usable_token(
    #[case] replacement: Option<&str>,
    #[case] expected: &str,
) {
    let (adapter, discovered, context) = fixture();
    let mut claims = claims();
    let original = retained_credentials(
        &adapter,
        DateTime::from_timestamp(claims["auth_time"].as_i64().unwrap(), 0),
    );
    let original_time = original.auth_time;
    claims.as_object_mut().unwrap().remove("auth_time");
    let mut response = response(
        claims,
        "current",
        CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
        false,
    );
    response.set_refresh_token(replacement.map(|token| RefreshToken::new(token.into())));
    let verified = adapter
        .verify_refresh(&discovered, response, original, &context)
        .await
        .unwrap();
    let credential = verified.credentials.unwrap();
    assert_eq!(credential.refresh, expected);
    assert_eq!(credential.auth_time, original_time);
}

#[rstest]
#[case::rotating(Some("replacement"))]
#[case::non_rotating(None)]
#[tokio::test]
async fn unavailable_userinfo_discards_the_renewal(#[case] replacement: Option<&str>) {
    let (mut adapter, mut discovered, context) = fixture();
    let mut config = serde_json::to_value(&adapter.definition).unwrap();
    config["membership"]["claim_source"] = json!("userinfo");
    adapter.definition = serde_json::from_value(config).unwrap();
    // Private outbound targets are disabled in this fixture; the UserInfo
    // request fails deterministically without contacting the network.
    discovered.userinfo = Some("https://127.0.0.1/userinfo".into());
    let claims = claims();
    let retained = retained_credentials(
        &adapter,
        DateTime::from_timestamp(claims["auth_time"].as_i64().unwrap(), 0),
    );
    let mut response = response(
        claims,
        "current",
        CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
        false,
    );
    response.set_refresh_token(replacement.map(|token| RefreshToken::new(token.into())));
    let result = adapter
        .verify_refresh(&discovered, response, retained, &context)
        .await;
    assert_eq!(result.err(), Some(OidcError::Unavailable));
}

#[rstest]
#[case::verified(false, true)]
#[case::bad_signature(true, false)]
#[tokio::test]
async fn only_signed_identity_can_reject_memberships(
    #[case] corrupt: bool,
    #[case] rejected: bool,
) {
    let (adapter, discovered, context) = fixture();
    let mut claims = claims();
    claims.as_object_mut().unwrap().remove("groups");
    let result = adapter
        .verify(
            &discovered,
            response(
                claims,
                "current",
                CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
                corrupt,
            ),
            "nonce",
            &context,
            None,
        )
        .await;
    assert!(result.is_err());
    assert_eq!(
        matches!(result, Err(OidcError::MembershipRejected(proof)) if proof.identity().subject().as_str() == "stable-subject"),
        rejected
    );
}

#[rstest]
#[case::malformed_metadata_or_keys(OidcError::InvalidResponse, OidcError::Unavailable)]
#[case::refused_discovery(OidcError::Denied, OidcError::Unavailable)]
#[case::unsupported_configuration(OidcError::Configuration, OidcError::Configuration)]
fn discovery_failures_are_provider_failures(
    #[case] failure: OidcError,
    #[case] expected: OidcError,
) {
    assert_eq!(discovery_error(failure), expected);
}

#[rstest]
#[case("invalid_grant", OidcError::Denied)]
#[case("invalid_client", OidcError::Configuration)]
#[case("unauthorized_client", OidcError::Configuration)]
#[case("temporarily_unavailable", OidcError::Unavailable)]
#[case("server_error", OidcError::Unavailable)]
#[case("provider_extension", OidcError::Unavailable)]
fn token_endpoint_classifies_user_and_provider_failures(
    #[case] code: &str,
    #[case] expected: OidcError,
) {
    let response = serde_json::from_value(json!({ "error": code })).unwrap();
    let error = token_error(openidconnect::RequestTokenError::ServerResponse(response));
    assert_eq!(error, expected);
}

#[tokio::test]
async fn distributed_group_claim_carries_signed_subject() {
    let (adapter, discovered, context) = fixture();
    let mut claims = claims();
    claims.as_object_mut().unwrap().remove("groups");
    claims["_claim_names"] = json!({"groups":"remote"});
    let result = adapter
        .verify(
            &discovered,
            response(
                claims,
                "current",
                CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
                false,
            ),
            "nonce",
            &context,
            None,
        )
        .await;
    assert!(
        matches!(result, Err(OidcError::MembershipRejected(rejection)) if rejection.identity().subject().as_str() == "stable-subject")
    );
}

#[rstest]
#[case::default_query("response_modes_supported", true)]
#[case::default_client_secret_basic("token_endpoint_auth_methods_supported", true)]
#[case::required_s256("code_challenge_methods_supported", false)]
fn discovery_uses_defaults_only_for_optional_fields(#[case] field: &str, #[case] accepted: bool) {
    let mut document = metadata();
    document.as_object_mut().unwrap().remove(field);
    assert_eq!(
        validate_metadata(&document, &definition()).is_ok(),
        accepted
    );
    if accepted {
        serde_json::from_value::<CoreProviderMetadata>(document).unwrap();
    }
}

#[rstest]
#[case::duplicate_subject(br#"{"sub":"other","sub":"stable-subject","groups":[]}"#)]
#[case::duplicate_groups(br#"{"sub":"stable-subject","groups":["/team"],"groups":[]}"#)]
fn userinfo_rejects_duplicate_members(#[case] body: &[u8]) {
    let reply = openidconnect::http::Response::builder()
        .header("content-type", "application/json")
        .body(body.to_vec())
        .unwrap();
    assert_eq!(
        userinfo_profile(&reply, "stable-subject"),
        Err(OidcError::InvalidResponse)
    );
}

#[rstest]
#[case::missing(&[])]
#[case::incorrect(&["text/plain"])]
#[case::ambiguous(&["application/json", "application/json"])]
fn userinfo_requires_one_json_content_type(#[case] media_types: &[&str]) {
    let mut reply = openidconnect::http::Response::builder();
    for media_type in media_types {
        reply = reply.header("content-type", *media_type);
    }
    let reply = reply
        .body(br#"{"sub":"stable-subject","groups":[]}"#.to_vec())
        .unwrap();
    assert_eq!(
        userinfo_profile(&reply, "stable-subject"),
        Err(OidcError::InvalidResponse)
    );
}

#[test]
fn paginated_userinfo_cannot_supply_complete_memberships() {
    let reply = openidconnect::http::Response::builder()
        .header("content-type", "application/json; charset=utf-8")
        .header("link", r#"</groups/2>; rel="next""#)
        .body(br#"{"sub":"stable-subject","groups":["/team"]}"#.to_vec())
        .unwrap();
    let (document, paginated) = userinfo_profile(&reply, "stable-subject").unwrap();
    let (adapter, _, _) = fixture();
    assert!(matches!(
        adapter.membership_claims(
            &document,
            MembershipClaimSource::Userinfo,
            Utc::now(),
            paginated
        ),
        Ok(MembershipClaims::Incomplete)
    ));
}

#[rstest]
#[case::mac("mac")]
#[case::extension("DPoP")]
#[tokio::test]
async fn userinfo_rejects_unsupported_token_types_before_dispatch(#[case] token_type: &str) {
    let (mut adapter, mut discovered, context) = fixture();
    let mut definition = serde_json::to_value(&adapter.definition).unwrap();
    definition["membership"]["claim_source"] = json!("userinfo");
    adapter.definition = serde_json::from_value(definition).unwrap();
    discovered.userinfo = Some("https://unused.invalid/userinfo".into());
    let mut tokens = serde_json::to_value(response(
        claims(),
        "current",
        CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
        false,
    ))
    .unwrap();
    tokens["token_type"] = json!(token_type);
    assert!(matches!(
        adapter
            .verify(
                &discovered,
                serde_json::from_value(tokens).unwrap(),
                "nonce",
                &context,
                None
            )
            .await,
        Err(OidcError::Configuration)
    ));
}
