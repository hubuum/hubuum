use super::*;
use hubuum_storage_core::{
    StorageExternalInteractionAuthentication, StorageInteractionAccess, StorageInteractionAction,
    StorageInteractionAuthorization, StorageInteractionCommand, StorageInteractionCreate,
    StorageInteractionOutcome,
};
use rstest::rstest;
use serde_json::{Value, json};

struct Fixture {
    backend: MemoryStorage,
    provider: StorageProviderIdentityBinding,
    definition: OidcProviderDefinition,
}
struct Attempt {
    binding: InteractionBinding,
    access: StorageInteractionAccess,
    claim: InteractionAuthenticationClaim,
    started_at: DateTime<Utc>,
}
fn digest() -> InteractionDigest {
    InteractionDigest::new(Uuid::new_v4().simple().to_string().repeat(2)).unwrap()
}
impl Fixture {
    async fn new(admission: Value, local_only: bool) -> Self {
        let definition: OidcProviderDefinition = serde_json::from_value(json!({
            "issuer":"https://id.example/realm", "client_id":"hubuum",
            "client_authentication":"client_secret_basic", "scopes":["openid"],
            "signing_algorithms":["RS256"], "admission":admission,
            "membership":if local_only { json!({"mode":"local_only", "refresh_interval_seconds":300, "max_stale_seconds":900}) } else {
                json!({"mode":"provider_groups", "refresh_interval_seconds":300, "max_stale_seconds":900, "claim_source":"id_token", "claim_pointer":"/groups", "key_mode":"full_path"}) }
        })).unwrap();
        let backend = MemoryStorage::new();
        let provider = StorageProviderIdentityBinding::try_new(
            "company".into(),
            Uuid::new_v4(),
            StorageAuthenticationProviderKind::Oidc,
            definition.issuer().as_str().into(),
        )
        .unwrap();
        backend
            .state
            .write()
            .await
            .runtime_settings
            .bindings
            .insert(provider.name().into(), provider.clone());
        Self {
            backend,
            provider,
            definition,
        }
    }
    async fn start(&self) -> Attempt {
        self.start_for(InteractionAuthority::login()).await
    }
    async fn start_for(&self, authority: InteractionAuthority) -> Attempt {
        let approval = authority.actor().is_some();
        let binding = InteractionBinding::try_new(
            InteractionId::new(Uuid::new_v4()).unwrap(),
            InteractionProviderId::external(self.provider.id()).unwrap(),
            InteractionClientId::new("browser-test").unwrap(),
            ResourceRevision::INITIAL,
            authority,
        )
        .unwrap();
        let secret = digest();
        let access =
            StorageInteractionAccess::new(binding.id(), binding.client().clone(), secret.clone());
        let now = Utc::now();
        let lifecycle = InteractionLifecycle::try_new(
            binding.clone(),
            InteractionMode::Credentials,
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        let mut create =
            StorageInteractionCreate::builder(lifecycle, secret).provider(self.provider.clone());
        if approval {
            create = create.actor(hubuum_storage_core::StorageInteractionActorLease::approval(
                now + Duration::minutes(10),
            ));
        }
        self.backend
            .create_auth_interaction(create.try_build().unwrap())
            .await
            .unwrap();
        let StorageInteractionOutcome::Claim(claim) = self
            .backend
            .advance_auth_interaction(StorageInteractionCommand::new(
                StorageInteractionAuthorization::Redemption(access.clone()),
                StorageInteractionAction::Claim(Uuid::new_v4()),
            ))
            .await
            .unwrap()
        else {
            panic!("claim");
        };
        Attempt {
            binding,
            access,
            claim,
            started_at: now,
        }
    }
    fn authentication(
        &self,
        attempt: &Attempt,
        subject: &str,
        name: &str,
        groups: Value,
    ) -> StorageExternalInteractionAuthentication {
        let context = ExternalAuthenticationContext::oidc(
            attempt.binding.clone(),
            IdentityScopeName::new(self.provider.name()).unwrap(),
            &self.definition,
        )
        .unwrap();
        StorageExternalInteractionAuthentication::Login(
            context
                .verified_oidc(
                    self.definition.issuer().as_str(),
                    ExternalIdentityProfile::new(
                        ExternalSubject::new(subject).unwrap(),
                        Some(name.into()),
                        Some("shared@example.test".into()),
                    )
                    .unwrap(),
                    MembershipClaims::Complete {
                        source: MembershipClaimSource::IdToken,
                        document: &json!({"groups":groups}),
                    },
                    Utc::now(),
                )
                .unwrap(),
        )
    }
    fn command(
        &self,
        attempt: &Attempt,
        subject: &str,
        name: &str,
        groups: Value,
    ) -> StorageInteractionCommand {
        StorageInteractionCommand::new(
            StorageInteractionAuthorization::Claim(attempt.claim.clone()),
            StorageInteractionAction::AuthenticateExternal {
                authentication: Box::new(self.authentication(attempt, subject, name, groups)),
                approval: None,
            },
        )
    }
    async fn sync(
        &self,
        subject: &str,
        name: &str,
        groups: Value,
    ) -> Result<StorageInteractionOutcome, StorageError> {
        let attempt = self.start().await;
        self.backend
            .advance_auth_interaction(self.command(&attempt, subject, name, groups))
            .await
    }
    async fn id(&self, subject: &str) -> PrincipalId {
        self.backend
            .state
            .read()
            .await
            .principals
            .values()
            .find(|principal| principal.external_subject() == Some(subject))
            .unwrap()
            .id()
    }
}

#[rstest]
#[case::existing_only(json!({"mode":"existing_only"}))]
#[case::outside_allowlist(json!({"mode":"subject_allowlist","subjects":["someone-else"]}))]
#[tokio::test]
async fn denied_admission_leaves_no_principal_or_scope(#[case] admission: Value) {
    let f = Fixture::new(admission, false).await;
    assert!(f.sync("subject", "alice", json!([])).await.is_err());
    let state = f.backend.state.read().await;
    assert_eq!(state.principals.len(), 1);
    assert_eq!(state.identity_scopes.len(), 1);
}

#[tokio::test]
async fn stable_subject_rename_preserves_principal() {
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    f.sync("subject", "old", json!([])).await.unwrap();
    let id = f.id("subject").await;
    f.sync("subject", "new", json!([])).await.unwrap();
    let state = f.backend.state.read().await;
    assert_eq!(state.principals[&id.id()].name(), "new");
    assert_eq!(state.principals.len(), 2);
}

#[tokio::test]
async fn email_does_not_link_different_subjects() {
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    f.sync("first", "alice", json!([])).await.unwrap();
    f.sync("second", "bob", json!([])).await.unwrap();
    assert_ne!(f.id("first").await, f.id("second").await);
}

#[tokio::test]
async fn name_collision_rolls_back_without_linking() {
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    f.sync("first", "alice", json!([])).await.unwrap();
    assert!(f.sync("second", "alice", json!(["/admin"])).await.is_err());
    let state = f.backend.state.read().await;
    assert_eq!(state.principals.len(), 2);
    assert_eq!(state.groups.len(), 1);
}

#[tokio::test]
async fn authoritative_empty_preserves_local_manual_membership() {
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    f.sync("subject", "alice", json!(["/admin"])).await.unwrap();
    let id = f.id("subject").await;
    let _ = f
        .backend
        .add_group_member(id, GroupId::new(1).unwrap(), &EventContext::system())
        .await
        .unwrap();
    f.sync("subject", "alice", json!([])).await.unwrap();
    let state = f.backend.state.read().await;
    let groups: Vec<_> = state
        .memberships
        .keys()
        .filter(|(principal, _)| *principal == id.id())
        .map(|(_, group)| *group)
        .collect();
    assert_eq!(groups, vec![1]);
}

#[tokio::test]
async fn local_only_preserves_provider_grants() {
    let mut f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    f.sync("subject", "alice", json!(["/team"])).await.unwrap();
    let id = f.id("subject").await;
    f.definition = Fixture::new(json!({"mode":"provision_without_grants"}), true)
        .await
        .definition;
    f.sync("subject", "alice", Value::Null).await.unwrap();
    assert_eq!(
        f.backend
            .state
            .read()
            .await
            .external_memberships
            .iter()
            .filter(|(principal, _)| *principal == id.id())
            .count(),
        1
    );
}

#[tokio::test]
async fn replay_cannot_reconcile_a_second_time() {
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    let attempt = f.start().await;
    let command = f.command(&attempt, "subject", "alice", json!(["/team"]));
    f.backend
        .advance_auth_interaction(command.clone())
        .await
        .unwrap();
    assert!(f.backend.advance_auth_interaction(command).await.is_err());
}

#[tokio::test]
async fn cancellation_prevents_identity_side_effects() {
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    let attempt = f.start().await;
    f.backend
        .advance_auth_interaction(StorageInteractionCommand::new(
            StorageInteractionAuthorization::Redemption(attempt.access.clone()),
            StorageInteractionAction::Cancel,
        ))
        .await
        .unwrap();
    assert!(
        f.backend
            .advance_auth_interaction(f.command(&attempt, "subject", "alice", json!([])))
            .await
            .is_err()
    );
    assert_eq!(f.backend.state.read().await.principals.len(), 1);
}

#[tokio::test]
async fn older_callback_cannot_restore_removed_membership() {
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    let attempt = f.start().await;
    let stale = f.command(&attempt, "subject", "alice", json!(["/team"]));
    f.sync("subject", "alice", json!([])).await.unwrap();
    assert!(f.backend.advance_auth_interaction(stale).await.is_err());
    assert!(f.backend.state.read().await.external_memberships.is_empty());
}

#[tokio::test]
async fn concurrent_provisioning_resolves_one_principal() {
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    let (a, b) = tokio::join!(
        f.sync("subject", "alice", json!([])),
        f.sync("subject", "alice", json!([]))
    );
    assert!(a.is_ok() || b.is_ok());
    assert_eq!(
        f.backend
            .state
            .read()
            .await
            .principals
            .values()
            .filter(|p| p.external_subject() == Some("subject"))
            .count(),
        1
    );
}

#[tokio::test]
async fn rejection_fences_a_previously_verified_callback() {
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    f.sync("subject", "alice", json!([])).await.unwrap();
    let attempt = f.start().await;
    let stale = f.command(&attempt, "subject", "alice", json!(["/team"]));
    f.backend
        .record_external_sync_failure(f.id("subject").await, StorageExternalSyncFailure::Rejected)
        .await
        .unwrap();
    assert!(f.backend.advance_auth_interaction(stale).await.is_err());
}

#[rstest]
#[case::manual("manual", "local")]
#[case::other_provider("external", "another")]
#[tokio::test]
async fn independent_provenance_survives_provider_removal(
    #[case] source: &str,
    #[case] scope: &str,
) {
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    f.sync("subject", "alice", json!(["/team"])).await.unwrap();
    let id = f.id("subject").await;
    let source_scope = if scope == "local" {
        IdentityScopeId::new(1).unwrap()
    } else {
        f.backend
            .ensure_identity_scope(StorageIdentityScopeEnsure::new(scope, "ldap"))
            .await
            .unwrap()
            .id()
    };
    let group = {
        let mut state = f.backend.state.write().await;
        let group = state
            .groups
            .values()
            .find(|group| group.external_key() == Some("/team"))
            .unwrap()
            .id();
        state.membership_sources.push(StorageBackupRow::try_from_value(json!({"principal_id":id.id(),"group_id":group.id(),"source":source,"source_scope_id":source_scope.id(),"source_key":if source=="manual" {""} else {"independent"},"created_at":Utc::now(),"updated_at":Utc::now()})).unwrap());
        group
    };
    f.sync("subject", "alice", json!([])).await.unwrap();
    assert!(
        f.backend
            .state
            .read()
            .await
            .memberships
            .contains_key(&(id.id(), group.id()))
    );
}

#[rstest]
#[case::valid(false, false, 0)]
#[case::permitted_clock_skew(false, false, 30)]
#[case::revoked_origin(true, false, 0)]
#[case::wrong_operation(false, true, 0)]
#[tokio::test]
async fn fresh_approval_completion_requires_live_actor_and_exact_operation(
    #[case] revoke: bool,
    #[case] wrong_operation: bool,
    #[case] clock_skew: i64,
) {
    use hubuum_storage_core::{
        StorageInteractionComplete, StorageInteractionCompletion, StorageInteractionIssuance,
    };
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    f.sync("subject", "alice", json!([])).await.unwrap();
    let id = f.id("subject").await;
    let origin = f
        .backend
        .create_token(StorageTokenCreate::new(
            id,
            StorageTokenDigest::legacy_unidentified("approval-origin"),
            StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value();
    let operation = digest();
    let attempt = f
        .start_for(InteractionAuthority::approval(
            InteractionActor::new(id, origin.id()),
            operation.clone(),
        ))
        .await;
    let context = ExternalAuthenticationContext::oidc(
        attempt.binding.clone(),
        IdentityScopeName::new(f.provider.name()).unwrap(),
        &f.definition,
    )
    .unwrap();
    let expected = context
        .identity(ExternalSubject::new("subject").unwrap())
        .unwrap();
    let request =
        FreshExternalAuthenticationRequest::new(context, expected, attempt.started_at, 60).unwrap();
    let StorageExternalInteractionAuthentication::Login(authentication) =
        f.authentication(&attempt, "subject", "alice", json!([]))
    else {
        unreachable!()
    };
    let fresh = request
        .verify(
            authentication.clone(),
            Some(authentication.verified_at() + Duration::seconds(clock_skew)),
            Utc::now(),
        )
        .unwrap();
    f.backend
        .advance_auth_interaction(StorageInteractionCommand::new(
            StorageInteractionAuthorization::Claim(attempt.claim),
            StorageInteractionAction::AuthenticateExternal {
                authentication: Box::new(StorageExternalInteractionAuthentication::Approval(fresh)),
                approval: None,
            },
        ))
        .await
        .unwrap();
    if revoke {
        f.backend
            .revoke_token(StorageTokenRevoke::new(
                origin.id(),
                id,
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
    }
    let claim = StorageCredentialClaim::new(
        id,
        origin.id(),
        StorageCredentialOperation::CreateToken,
        StorageCredentialFingerprint::new(digest().as_str().to_owned()).unwrap(),
        StorageCredentialFingerprint::new(operation.as_str().to_owned()).unwrap(),
        Utc::now() - Duration::hours(24),
    )
    .target(Some(id));
    let completion = StorageInteractionComplete::new(
        attempt.access,
        StorageInteractionIssuance::Approval {
            claim,
            operation_digest: if wrong_operation { digest() } else { operation },
            context: EventContext::user(id, None, None),
        },
    );
    let outcome = f
        .backend
        .complete_auth_interaction(completion.clone())
        .await
        .unwrap();
    assert_eq!(
        matches!(outcome, StorageInteractionCompletion::Approval(_)),
        !revoke && !wrong_operation
    );
    if !revoke && !wrong_operation {
        assert!(matches!(
            f.backend
                .complete_auth_interaction(completion)
                .await
                .unwrap(),
            StorageInteractionCompletion::Rejected(InteractionTransitionError::Consumed)
        ));
    }
}
