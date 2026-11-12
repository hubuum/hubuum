use super::*;
use rstest::rstest;
use serde_json::json;

fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
fn digest(value: char) -> InteractionDigest {
    InteractionDigest::new(value.to_string().repeat(64)).unwrap()
}
fn actor(value: i32) -> InteractionActor {
    InteractionActor::new(
        PrincipalId::new(value).unwrap(),
        TokenId::new(value).unwrap(),
    )
}
fn binding() -> InteractionBinding {
    InteractionBinding::try_new(
        InteractionId::new(id(1)).unwrap(),
        InteractionProviderId::external(id(2)).unwrap(),
        InteractionClientId::new("hubuum-cli").unwrap(),
        ResourceRevision::new(1).unwrap(),
        InteractionAuthority::login(),
    )
    .unwrap()
}

#[rstest]
#[case("")]
#[case("has spaces")]
#[case("client/other")]
#[case("\n")]
#[case("å")]
#[case(&"a".repeat(129))]
fn client_identifiers_reject_invalid_values(#[case] value: &str) {
    assert_eq!(
        InteractionClientId::new(value),
        Err(InteractionValidationError::InvalidClient)
    );
}

#[rstest]
#[case("local", true)]
#[case("00000000-0000-0000-0000-000000000002", true)]
#[case("ldap", false)]
#[case("Local", false)]
#[case("00000000-0000-0000-0000-000000000000", false)]
#[case("00000000000000000000000000000002", false)]
fn provider_ids_are_reserved_local_or_canonical_external_uuid(
    #[case] value: &str,
    #[case] valid: bool,
) {
    assert_eq!(
        InteractionProviderId::try_from(value.to_owned()).is_ok(),
        valid
    );
}

#[rstest]
#[case(json!({"purpose":"login", "actor":{"principal_id":1,"token_id":1}}))]
#[case(json!({"purpose":"credential_approval"}))]
#[case(json!({"purpose":"provider_verification", "actor":{"principal_id":1,"token_id":1}, "candidate_id":Uuid::nil(), "candidate":"a".repeat(64)}))]
fn persistence_cannot_construct_incoherent_purpose_authority(#[case] value: serde_json::Value) {
    assert!(serde_json::from_value::<InteractionAuthority>(value).is_err());
}

#[rstest]
#[case("id")]
#[case("provider")]
#[case("client")]
#[case("revision")]
#[case("purpose")]
fn proof_cannot_be_rebound(#[case] field: &str) {
    let original = binding();
    let proof = VerifiedInteractionSubject::new(original.clone(), "subject-1").unwrap();
    let mut other = original;
    match field {
        "id" => other.id = InteractionId::new(id(3)).unwrap(),
        "provider" => other.provider = InteractionProviderId::external(id(3)).unwrap(),
        "client" => other.client = InteractionClientId::new("other-client").unwrap(),
        "revision" => other.revision = ResourceRevision::new(2).unwrap(),
        "purpose" => other.authority = InteractionAuthority::approval(actor(1), digest('a')),
        _ => unreachable!(),
    }
    assert_eq!(
        proof.require_binding(&other),
        Err(InteractionValidationError::BindingMismatch)
    );
}

#[rstest]
#[case("actor")]
#[case("token")]
#[case("operation")]
fn approval_proof_retains_exact_operation_and_origin_credential(#[case] field: &str) {
    let mut original = binding();
    original.authority = InteractionAuthority::approval(actor(1), digest('a'));
    let proof = VerifiedInteractionSubject::new(original.clone(), "subject-1").unwrap();
    let (owner, operation) = match field {
        "actor" => (actor(2), digest('a')),
        "token" => (
            InteractionActor::new(PrincipalId::new(1).unwrap(), TokenId::new(2).unwrap()),
            digest('a'),
        ),
        "operation" => (actor(1), digest('b')),
        _ => unreachable!(),
    };
    original.authority = InteractionAuthority::approval(owner, operation);
    assert_eq!(
        proof.require_binding(&original),
        Err(InteractionValidationError::BindingMismatch)
    );
}

#[rstest]
#[case("candidate_id")]
#[case("candidate_digest")]
fn verification_proof_retains_exact_candidate(#[case] field: &str) {
    let mut original = binding();
    original.authority = InteractionAuthority::verification(actor(1), id(3), digest('a')).unwrap();
    let proof = VerifiedInteractionSubject::new(original.clone(), "subject-1").unwrap();
    original.authority = InteractionAuthority::verification(
        actor(1),
        if field == "candidate_id" {
            id(4)
        } else {
            id(3)
        },
        if field == "candidate_digest" {
            digest('b')
        } else {
            digest('a')
        },
    )
    .unwrap();
    assert_eq!(
        proof.require_binding(&original),
        Err(InteractionValidationError::BindingMismatch)
    );
}

#[rstest]
#[case("")]
#[case("line\nbreak")]
#[case("nul\0subject")]
#[case(&"a".repeat(256))]
fn subject_evidence_rejects_invalid_subjects(#[case] value: &str) {
    assert_eq!(
        VerifiedInteractionSubject::new(binding(), value),
        Err(InteractionValidationError::InvalidSubject)
    );
}

#[test]
fn binding_survives_validated_persistence_round_trip() {
    let mut original = binding();
    original.authority = InteractionAuthority::verification(actor(1), id(3), digest('a')).unwrap();
    let encoded = serde_json::to_vec(&original).unwrap();
    assert_eq!(
        serde_json::from_slice::<InteractionBinding>(&encoded).unwrap(),
        original
    );
}

#[test]
fn proof_debug_does_not_disclose_subject_or_digests() {
    let mut original = binding();
    original.authority = InteractionAuthority::approval(actor(1), digest('a'));
    let proof = VerifiedInteractionSubject::new(original, "private-upstream-subject").unwrap();
    let debug = format!("{proof:?}");
    assert!(!debug.contains("private-upstream-subject") && !debug.contains(&"a".repeat(64)));
}

#[test]
fn local_provider_cannot_verify_an_external_candidate() {
    let original = binding();
    assert_eq!(
        InteractionBinding::try_new(
            original.id(),
            InteractionProviderId::local(),
            original.client().clone(),
            original.revision(),
            InteractionAuthority::verification(actor(1), id(3), digest('a')).unwrap(),
        ),
        Err(InteractionValidationError::InvalidPurpose)
    );
}

#[test]
fn binding_deserialization_rechecks_correlated_provider_and_purpose() {
    let mut value = serde_json::to_value(binding()).unwrap();
    value["provider"] = json!("local");
    value["authority"] = serde_json::to_value(
        InteractionAuthority::verification(actor(1), id(3), digest('a')).unwrap(),
    )
    .unwrap();
    assert!(serde_json::from_value::<InteractionBinding>(value).is_err());
}
