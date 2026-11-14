use super::*;
use hubuum_domain::{
    IdentityScopeName, InteractionAuthority, InteractionBinding, InteractionClientId,
    InteractionId, InteractionProviderId, ResourceRevision,
};
use openidconnect::{
    IdTokenClaims, JsonWebKeyId, PrivateSigningKey, core::CoreRsaPrivateSigningKey,
};
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
        "https://hubuum.example/callback",
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
    let claims: IdTokenClaims<Additional, CoreGenderClaim> =
        serde_json::from_value(claims).unwrap();
    let mut token = ClaimsToken::new(claims, &signing_key(key), algorithm, None, None)
        .unwrap()
        .to_string();
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
#[case::http_token("token_endpoint", json!("http://id.example/token"))]
#[case::fragment("jwks_uri", json!("https://id.example/certs#part"))]
#[case::pkce_plain("code_challenge_methods_supported", json!(["plain"]))]
#[case::implicit_only("response_types_supported", json!(["token"]))]
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
        state: "state".into(),
        nonce: "nonce".into(),
        verifier: "verifier".into(),
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
