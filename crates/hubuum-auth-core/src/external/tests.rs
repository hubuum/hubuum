use super::*;
use hubuum_domain::{
    InteractionActor, InteractionAuthority, InteractionClientId, InteractionDigest, InteractionId,
    PrincipalId, ResourceRevision, TokenId,
};
use rstest::rstest;
use serde_json::json;
use uuid::Uuid;

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000, 0).unwrap()
}
fn scope() -> IdentityScopeName {
    IdentityScopeName::new("company").unwrap()
}
fn actor() -> InteractionActor {
    InteractionActor::new(PrincipalId::new(1).unwrap(), TokenId::new(2).unwrap())
}
fn approval() -> InteractionAuthority {
    InteractionAuthority::approval(actor(), InteractionDigest::new("a".repeat(64)).unwrap())
}
fn binding(authority: InteractionAuthority) -> InteractionBinding {
    InteractionBinding::try_new(
        InteractionId::new(Uuid::from_u128(1)).unwrap(),
        InteractionProviderId::external(Uuid::from_u128(2)).unwrap(),
        InteractionClientId::new("hubuum-cli").unwrap(),
        ResourceRevision::new(1).unwrap(),
        authority,
    )
    .unwrap()
}
fn local_membership() -> Value {
    json!({"mode":"local_only","refresh_interval_seconds":300,"max_stale_seconds":900})
}
fn group_membership() -> Value {
    json!({"mode":"provider_groups","refresh_interval_seconds":300,"max_stale_seconds":900,
        "claim_source":"id_token","claim_pointer":"/groups","key_mode":"full_path"})
}
fn oidc_definition(membership: Value, admission: Value) -> OidcProviderDefinition {
    serde_json::from_value(
        json!({"issuer":"https://id.example/realm","client_id":"hubuum",
        "client_authentication":"client_secret_basic","scopes":["openid"],
        "signing_algorithms":["RS256"],"membership":membership,"admission":admission}),
    )
    .unwrap()
}
fn oidc(authority: InteractionAuthority) -> ExternalAuthenticationContext {
    ExternalAuthenticationContext::oidc(
        binding(authority),
        scope(),
        &oidc_definition(local_membership(), json!({"mode":"existing_only"})),
    )
    .unwrap()
}
fn oauth(authority: InteractionAuthority) -> ExternalAuthenticationContext {
    let definition = serde_json::from_value(json!({"profile":"github",
        "authorization_endpoint":"https://github.com/login/oauth/authorize",
        "token_endpoint":"https://github.com/login/oauth/access_token",
        "profile_endpoint":"https://api.github.com/user","client_id":"hubuum",
        "client_authentication":"client_secret_post","scopes":[],"subject_pointer":"/id",
        "subject_type":"integer","name_pointer":"/login","membership":local_membership(),
        "admission":{"mode":"existing_only"}}))
    .unwrap();
    ExternalAuthenticationContext::oauth(binding(authority), scope(), &definition).unwrap()
}
fn profile(subject: &str) -> ExternalIdentityProfile {
    ExternalIdentityProfile::new(
        ExternalSubject::new(subject).unwrap(),
        Some("mutable-name".into()),
        Some("mutable-email@example.org".into()),
    )
    .unwrap()
}
fn authenticate(context: &ExternalAuthenticationContext) -> VerifiedExternalAuthentication {
    context
        .verified_oidc(
            "https://id.example/realm",
            profile("subject-1"),
            MembershipClaims::Absent,
            now(),
        )
        .unwrap()
}
fn complete(document: &Value) -> MembershipClaims<'_> {
    MembershipClaims::Complete {
        source: MembershipClaimSource::IdToken,
        document,
    }
}

#[rstest]
#[case("")]
#[case("subject\n")]
#[case(&"a".repeat(256))]
fn invalid_subject_is_rejected(#[case] subject: &str) {
    assert_eq!(
        ExternalSubject::new(subject),
        Err(ExternalAuthenticationError::InvalidSubject)
    );
}

#[rstest]
#[case("9007199254740993", "9007199254740993")]
#[case("18446744073709551616", "18446744073709551616")]
#[case("-18446744073709551616", "-18446744073709551616")]
#[case("0", "0")]
#[case("-0", "0")]
fn oauth_integer_subject_is_lossless(#[case] raw: &str, #[case] expected: &str) {
    let value = serde_json::from_str(raw).unwrap();
    assert_eq!(
        ExternalSubject::from_oauth_value(&value, OAuthSubjectType::Integer)
            .unwrap()
            .as_str(),
        expected
    );
}

#[rstest]
#[case("1.0")]
#[case("1e3")]
#[case("\"123\"")]
#[case("null")]
#[case("true")]
#[case("[]")]
fn oauth_rejects_non_integer_subject_encodings(#[case] raw: &str) {
    let value = serde_json::from_str(raw).unwrap();
    assert_eq!(
        ExternalSubject::from_oauth_value(&value, OAuthSubjectType::Integer),
        Err(ExternalAuthenticationError::InvalidOAuthSubject)
    );
}

#[test]
fn string_subject_is_not_normalized() {
    let value = json!(" 001AbC ");
    assert_eq!(
        ExternalSubject::from_oauth_value(&value, OAuthSubjectType::String)
            .unwrap()
            .as_str(),
        " 001AbC "
    );
}

#[test]
fn missing_profile_fields_do_not_prevent_identity_proof() {
    let profile =
        ExternalIdentityProfile::new(ExternalSubject::new("subject-1").unwrap(), None, None)
            .unwrap();
    assert!(
        oidc(InteractionAuthority::login())
            .verified_oidc(
                "https://id.example/realm",
                profile,
                MembershipClaims::Absent,
                now()
            )
            .is_ok()
    );
}

#[test]
fn account_rename_preserves_stable_identity() {
    let context = oidc(InteractionAuthority::login());
    let before = authenticate(&context);
    let after = context
        .verified_oidc(
            "https://id.example/realm",
            ExternalIdentityProfile::new(
                ExternalSubject::new("subject-1").unwrap(),
                Some("renamed".into()),
                None,
            )
            .unwrap(),
            MembershipClaims::Absent,
            now(),
        )
        .unwrap();
    assert_eq!(before.identity(), after.identity());
}

#[rstest]
#[case("https://id.example/realm/")]
#[case("https://ID.example/realm")]
#[case("https://other.example/realm")]
fn issuer_requires_exact_match(#[case] issuer: &str) {
    assert!(matches!(
        oidc(InteractionAuthority::login()).verified_oidc(
            issuer,
            profile("subject-1"),
            MembershipClaims::Absent,
            now()
        ),
        Err(ExternalAuthenticationError::IdentityMismatch)
    ));
}

#[rstest]
#[case("provider")]
#[case("scope")]
#[case("issuer")]
#[case("kind")]
#[case("subject")]
fn equal_profile_fields_cannot_link_different_identities(#[case] changed: &str) {
    let context = oidc(InteractionAuthority::login());
    let authenticated = context
        .verified_oidc(
            "https://id.example/realm",
            profile("123"),
            MembershipClaims::Absent,
            now(),
        )
        .unwrap();
    let mut other = context.clone();
    match changed {
        "provider" => {
            other.binding = InteractionBinding::try_new(
                other.binding.id(),
                InteractionProviderId::external(Uuid::from_u128(3)).unwrap(),
                other.binding.client().clone(),
                other.binding.revision(),
                other.binding.authority().clone(),
            )
            .unwrap()
        }
        "scope" => other.scope = IdentityScopeName::new("another").unwrap(),
        "issuer" => {
            let ProviderConfiguration::Oidc(definition) = &other.configuration else {
                unreachable!()
            };
            let mut value = serde_json::to_value(definition).unwrap();
            value["issuer"] = json!("https://other.example");
            other.configuration =
                ProviderConfiguration::Oidc(serde_json::from_value(value).unwrap());
        }
        "kind" => other = oauth(InteractionAuthority::login()),
        "subject" => (),
        _ => unreachable!(),
    }
    let subject = if changed == "subject" { "456" } else { "123" };
    let other_identity = other
        .identity(ExternalSubject::new(subject).unwrap())
        .unwrap();
    assert_eq!(
        authenticated.admit(Some(&other_identity)).unwrap_err(),
        ExternalAuthenticationError::LinkingUnsupported
    );
}

#[rstest]
#[case(json!({"mode":"existing_only"}), false, false)]
#[case(json!({"mode":"existing_only"}), true, true)]
#[case(json!({"mode":"provision_without_grants"}), false, true)]
#[case(json!({"mode":"subject_allowlist","subjects":["subject-1"]}), false, true)]
#[case(json!({"mode":"subject_allowlist","subjects":["other"]}), true, false)]
#[case(json!({"mode":"subject_allowlist","subjects":["other"]}), false, false)]
fn admission_applies_to_every_login(
    #[case] admission: Value,
    #[case] existing: bool,
    #[case] allowed: bool,
) {
    let context = ExternalAuthenticationContext::oidc(
        binding(InteractionAuthority::login()),
        scope(),
        &oidc_definition(local_membership(), admission),
    )
    .unwrap();
    let proof = authenticate(&context);
    let identity = proof.identity().clone();
    assert_eq!(proof.admit(existing.then_some(&identity)).is_ok(), allowed);
}

#[rstest]
#[case(json!({"groups":null}), ExternalAuthenticationError::MalformedMemberships)]
#[case(json!({}), ExternalAuthenticationError::MissingMemberships)]
#[case(json!({"groups":"admin"}), ExternalAuthenticationError::MalformedMemberships)]
#[case(json!({"groups":[1]}), ExternalAuthenticationError::MalformedMemberships)]
#[case(json!({"groups":["/engineering", "/engineering"]}), ExternalAuthenticationError::MalformedMemberships)]
#[case(json!({"groups":["engineering"]}), ExternalAuthenticationError::MalformedMemberships)]
#[case(json!({"groups":["/"]}), ExternalAuthenticationError::MalformedMemberships)]
#[case(json!({"groups":["/engineering/"]}), ExternalAuthenticationError::MalformedMemberships)]
#[case(json!({"groups":["/engineering//ops"]}), ExternalAuthenticationError::MalformedMemberships)]
#[case(json!({"groups":["/line\nbreak"]}), ExternalAuthenticationError::MalformedMemberships)]
#[case(json!({"groups":["a".repeat(256)]}), ExternalAuthenticationError::MalformedMemberships)]
#[case(json!({"groups":vec!["/group";1025]}), ExternalAuthenticationError::MalformedMemberships)]
fn invalid_claims_never_become_an_empty_snapshot(
    #[case] claims: Value,
    #[case] error: ExternalAuthenticationError,
) {
    let policy = oidc_definition(group_membership(), json!({"mode":"existing_only"}));
    assert_eq!(
        ExternalMemberships::validate(policy.membership(), complete(&claims)),
        Err(error)
    );
}

#[rstest]
#[case(
    MembershipClaims::Absent,
    ExternalAuthenticationError::MissingMemberships
)]
#[case(
    MembershipClaims::Malformed,
    ExternalAuthenticationError::MalformedMemberships
)]
#[case(
    MembershipClaims::Incomplete,
    ExternalAuthenticationError::IncompleteMemberships
)]
fn non_authoritative_observations_are_distinct(
    #[case] claims: MembershipClaims<'_>,
    #[case] error: ExternalAuthenticationError,
) {
    let policy = oidc_definition(group_membership(), json!({"mode":"existing_only"}));
    assert_eq!(
        ExternalMemberships::validate(policy.membership(), claims),
        Err(error)
    );
}

#[test]
fn explicit_empty_groups_are_authoritative() {
    let policy = oidc_definition(group_membership(), json!({"mode":"existing_only"}));
    let proof = ExternalMemberships::validate(policy.membership(), complete(&json!({"groups":[]})))
        .unwrap();
    assert_eq!(proof.provider_groups(), Some([].as_slice()));
}

#[test]
fn local_only_preserves_manual_memberships_even_for_admin_claim() {
    let proof = oidc(InteractionAuthority::login())
        .verified_oidc(
            "https://id.example/realm",
            profile("subject-1"),
            complete(&json!({"admin":true,"groups":["/admin"]})),
            now(),
        )
        .unwrap();
    assert_eq!(proof.memberships().provider_groups(), None);
}

#[test]
fn role_mapping_requires_the_configured_pointer() {
    let policy = oidc_definition(group_membership(), json!({"mode":"existing_only"}));
    let proof = ExternalMemberships::validate(
        policy.membership(),
        complete(&json!({"groups":[], "roles":["admin"],"site_admin":true})),
    )
    .unwrap();
    assert_eq!(proof.provider_groups(), Some([].as_slice()));
}

#[test]
fn membership_source_must_match_configuration() {
    let policy = oidc_definition(group_membership(), json!({"mode":"existing_only"}));
    let document = json!({"groups":[]});
    let claims = MembershipClaims::Complete {
        source: MembershipClaimSource::Userinfo,
        document: &document,
    };
    assert_eq!(
        ExternalMemberships::validate(policy.membership(), claims),
        Err(ExternalAuthenticationError::MembershipSourceMismatch)
    );
}

#[test]
fn path_rename_changes_membership_key() {
    let policy = oidc_definition(group_membership(), json!({"mode":"existing_only"}));
    let before = ExternalMemberships::validate(
        policy.membership(),
        complete(&json!({"groups":["/team/before"]})),
    )
    .unwrap();
    let after = ExternalMemberships::validate(
        policy.membership(),
        complete(&json!({"groups":["/team/after"]})),
    )
    .unwrap();
    assert_ne!(before, after);
}

#[test]
fn immutable_keys_ignore_display_metadata() {
    let mut membership = group_membership();
    membership["key_mode"] = json!("immutable_id");
    let policy = oidc_definition(membership, json!({"mode":"existing_only"}));
    let before = ExternalMemberships::validate(
        policy.membership(),
        complete(&json!({"groups":["stable-id"],"group_name":"before"})),
    )
    .unwrap();
    let after = ExternalMemberships::validate(
        policy.membership(),
        complete(&json!({"groups":["stable-id"],"group_name":"after"})),
    )
    .unwrap();
    assert_eq!(before, after);
}

#[test]
fn oauth_approvals_are_explicitly_unsupported() {
    let context = oauth(approval());
    let identity = context
        .identity(ExternalSubject::new("123").unwrap())
        .unwrap();
    assert!(matches!(
        FreshExternalAuthenticationRequest::new(context, identity, now(), 60),
        Err(ExternalAuthenticationError::CapabilityUnsupported)
    ));
}

#[test]
fn ordinary_login_cannot_be_cast_to_approval() {
    let context = oidc(InteractionAuthority::login());
    let identity = authenticate(&context).identity().clone();
    assert!(matches!(
        FreshExternalAuthenticationRequest::new(context, identity, now(), 60),
        Err(ExternalAuthenticationError::InvalidPurpose)
    ));
}

#[rstest]
#[case(None, false)]
#[case(Some(-61), false)]
#[case(Some(-60), true)]
#[case(Some(0), true)]
#[case(Some(60), true)]
#[case(Some(61), false)]
fn signed_authentication_time_is_bounded(#[case] offset: Option<i64>, #[case] accepted: bool) {
    let context = oidc(approval());
    let proof = authenticate(&context);
    let request =
        FreshExternalAuthenticationRequest::new(context, proof.identity().clone(), now(), 60)
            .unwrap();
    assert_eq!(
        request
            .verify(
                proof,
                offset.map(|offset| now() + Duration::seconds(offset)),
                now()
            )
            .is_ok(),
        accepted
    );
}

#[rstest]
#[case("interaction")]
#[case("client")]
#[case("revision")]
#[case("actor")]
#[case("token")]
#[case("operation")]
#[case("purpose")]
#[case("subject")]
fn fresh_evidence_is_bound_to_exact_request(#[case] changed: &str) {
    let context = oidc(approval());
    let original = authenticate(&context);
    let request = FreshExternalAuthenticationRequest::new(
        context.clone(),
        original.identity().clone(),
        now(),
        0,
    )
    .unwrap();
    let b = context.binding();
    let mut other = context.clone();
    other.binding = InteractionBinding::try_new(
        if changed == "interaction" {
            InteractionId::new(Uuid::from_u128(10)).unwrap()
        } else {
            b.id()
        },
        b.provider(),
        if changed == "client" {
            InteractionClientId::new("hubuum-frontend").unwrap()
        } else {
            b.client().clone()
        },
        if changed == "revision" {
            ResourceRevision::new(2).unwrap()
        } else {
            b.revision()
        },
        match changed {
            "actor" => InteractionAuthority::approval(
                InteractionActor::new(PrincipalId::new(3).unwrap(), actor().token_id()),
                b.authority().operation_digest().unwrap().clone(),
            ),
            "token" => InteractionAuthority::approval(
                InteractionActor::new(actor().principal_id(), TokenId::new(3).unwrap()),
                b.authority().operation_digest().unwrap().clone(),
            ),
            "operation" => InteractionAuthority::approval(
                actor(),
                InteractionDigest::new("b".repeat(64)).unwrap(),
            ),
            "purpose" => InteractionAuthority::login(),
            _ => b.authority().clone(),
        },
    )
    .unwrap();
    let proof = other
        .verified_oidc(
            "https://id.example/realm",
            profile(if changed == "subject" {
                "other"
            } else {
                "subject-1"
            }),
            MembershipClaims::Absent,
            now(),
        )
        .unwrap();
    assert!(matches!(
        request.verify(proof, Some(now()), now()),
        Err(ExternalAuthenticationError::IdentityMismatch)
    ));
}

#[test]
fn debug_output_omits_identity_and_profile_values() {
    let proof = authenticate(&oidc(approval()));
    let debug = format!("{proof:?} {:?}", proof.profile());
    assert!(
        !["subject-1", "mutable-name", "mutable-email", "id.example"]
            .iter()
            .any(|secret| debug.contains(secret))
    );
}

#[rstest]
#[case(json!({"id":9007199254740993_u64,"login":"renamable"}), true)]
#[case(json!({"id":42}), true)]
#[case(json!({"id":"42"}), false)]
#[case(json!({"id":42.0}), false)]
#[case(json!({"email":"42@example.org","login":"42"}), false)]
#[case(json!({"id":42,"login":[]}), false)]
#[case(json!([]), false)]
fn oauth_boundary_enforces_configured_subject_mapping(
    #[case] document: Value,
    #[case] valid: bool,
) {
    assert_eq!(
        oauth(InteractionAuthority::login())
            .verified_oauth(&document, MembershipClaims::Absent, now())
            .is_ok(),
        valid
    );
}

#[rstest]
#[case(InteractionAuthority::login())]
#[case(approval())]
fn local_provider_cannot_issue_external_proof(#[case] authority: InteractionAuthority) {
    let mut b = binding(authority);
    b = InteractionBinding::try_new(
        b.id(),
        InteractionProviderId::local(),
        b.client().clone(),
        b.revision(),
        b.authority().clone(),
    )
    .unwrap();
    assert!(matches!(
        ExternalAuthenticationContext::oidc(
            b,
            scope(),
            &oidc_definition(local_membership(), json!({"mode":"existing_only"}))
        ),
        Err(ExternalAuthenticationError::InvalidProvider)
    ));
}

#[test]
fn local_scope_cannot_receive_external_identity() {
    assert!(matches!(
        ExternalAuthenticationContext::oidc(
            binding(InteractionAuthority::login()),
            IdentityScopeName::new("local").unwrap(),
            &oidc_definition(local_membership(), json!({"mode":"existing_only"}))
        ),
        Err(ExternalAuthenticationError::InvalidProvider)
    ));
}

#[rstest]
#[case(approval())]
#[case(InteractionAuthority::verification(actor(), Uuid::from_u128(4), InteractionDigest::new("b".repeat(64)).unwrap()).unwrap())]
fn non_login_interactions_cannot_admit_principals(#[case] authority: InteractionAuthority) {
    let proof = authenticate(&oidc(authority));
    let identity = proof.identity().clone();
    assert_eq!(
        proof.admit(Some(&identity)).unwrap_err(),
        ExternalAuthenticationError::InvalidAdmissionPurpose
    );
}

#[test]
fn configuration_revision_is_not_stable_identity() {
    let context = oidc(InteractionAuthority::login());
    let before = authenticate(&context);
    let mut next = context.clone();
    let b = context.binding();
    next.binding = InteractionBinding::try_new(
        b.id(),
        b.provider(),
        b.client().clone(),
        ResourceRevision::new(2).unwrap(),
        b.authority().clone(),
    )
    .unwrap();
    assert_eq!(before.identity(), authenticate(&next).identity());
}

#[test]
fn fresh_approval_rechecks_allowlist() {
    let context = ExternalAuthenticationContext::oidc(
        binding(approval()),
        scope(),
        &oidc_definition(
            local_membership(),
            json!({"mode":"subject_allowlist", "subjects":["other"]}),
        ),
    )
    .unwrap();
    let proof = authenticate(&context);
    let request =
        FreshExternalAuthenticationRequest::new(context, proof.identity().clone(), now(), 0)
            .unwrap();
    assert!(matches!(
        request.verify(proof, Some(now()), now()),
        Err(ExternalAuthenticationError::AdmissionDenied)
    ));
}

#[rstest]
#[case(-1, 0)]
#[case(1, 0)]
#[case(0, -1)]
fn approval_rejects_impossible_observation_times(
    #[case] verified_offset: i64,
    #[case] now_offset: i64,
) {
    let context = oidc(approval());
    let mut proof = authenticate(&context);
    proof.verified_at = now() + Duration::seconds(verified_offset);
    let request =
        FreshExternalAuthenticationRequest::new(context, proof.identity().clone(), now(), 60)
            .unwrap();
    assert!(matches!(
        request.verify(proof, Some(now()), now() + Duration::seconds(now_offset)),
        Err(ExternalAuthenticationError::InvalidAuthenticationTime)
    ));
}

#[test]
fn deployment_cannot_relax_authentication_clock_skew() {
    let context = oidc(approval());
    let identity = authenticate(&context).identity().clone();
    assert!(matches!(
        FreshExternalAuthenticationRequest::new(context, identity, now(), 61),
        Err(ExternalAuthenticationError::InvalidAuthenticationTime)
    ));
}

#[test]
fn oidc_subject_obeys_ascii_contract() {
    assert!(matches!(
        oidc(InteractionAuthority::login()).identity(ExternalSubject::new("subject-å").unwrap()),
        Err(ExternalAuthenticationError::InvalidSubject)
    ));
}

#[rstest]
#[case("abc")]
#[case("001")]
#[case("-0")]
#[case("1.0")]
#[case("1e3")]
fn persisted_oauth_integer_subject_must_be_canonical(#[case] subject: &str) {
    assert!(matches!(
        oauth(InteractionAuthority::login()).identity(ExternalSubject::new(subject).unwrap()),
        Err(ExternalAuthenticationError::InvalidOAuthSubject)
    ));
}
