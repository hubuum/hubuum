use super::*;
use hubuum_storage_core::{
    SessionCredentials, SessionPolicy, StorageExternalInteractionAuthentication,
    StorageInteractionAccess, StorageInteractionAction, StorageInteractionAuthorization,
    StorageInteractionCommand, StorageInteractionComplete, StorageInteractionCompletion,
    StorageInteractionCreate, StorageInteractionIssuance, StorageInteractionOutcome,
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

fn session_request(
    access: StorageInteractionAccess,
    audit_subject: Option<EventSubject>,
) -> StorageInteractionComplete {
    let token = || {
        StorageTokenDigest::try_new(
            Uuid::new_v4().to_string(),
            StorageTokenFormat::Version1,
            StorageTokenHashAlgorithm::HmacSha256V1,
            Some(StorageTokenHashKeyId::try_new("test").unwrap()),
        )
        .unwrap()
    };
    StorageInteractionComplete::new(
        access,
        StorageInteractionIssuance::Session {
            credentials: SessionCredentials::try_new(token(), token()).unwrap(),
            policy: SessionPolicy::try_new(900, 3600, 86400, 86400).unwrap(),
            context: EventContext::system(),
            audit_subject,
        },
    )
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
        // Exercise lifecycle normalization even on clocks with microsecond precision.
        let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap()
            + Duration::nanoseconds(789);
        let lifecycle = InteractionLifecycle::try_new(
            binding.clone(),
            InteractionMode::Credentials,
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        let started_at = lifecycle.created_at();
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
            started_at,
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
#[case::administrator(true)]
#[case::non_administrator(false)]
#[tokio::test]
async fn session_audit_preserves_selected_administrator_status(#[case] is_admin: bool) {
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    let attempt = f.start().await;
    let StorageInteractionOutcome::View(view) = f
        .backend
        .advance_auth_interaction(f.command(&attempt, "alice", "alice", json!([])))
        .await
        .unwrap()
    else {
        panic!("view");
    };
    let subject = view
        .authenticated_subject()
        .unwrap()
        .clone()
        .with_administrator_status(is_admin);
    let result = f
        .backend
        .complete_auth_interaction(session_request(attempt.access, Some(subject.clone())))
        .await
        .unwrap();
    assert!(matches!(result, StorageInteractionCompletion::Session(_)));
    let state = f.backend.state.read().await;
    let events: Vec<_> = state
        .events
        .iter()
        .map(|event| serde_json::to_value(event.as_parts().0).unwrap())
        .filter(|event| event["entity_type"] == "authentication")
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0]["metadata"]["subject"],
        serde_json::to_value(subject).unwrap()
    );
}

#[rstest]
#[case::missing(None)]
#[case::principal(Some(("principal_id", json!(1000000))))]
#[case::name(Some(("name", json!("someone-else"))))]
#[case::scope(Some(("identity_scope", json!("another"))))]
#[case::provider(Some(("authentication_provider_kind", json!("oauth"))))]
#[tokio::test]
async fn unbound_audit_snapshot_cannot_issue_a_session(#[case] change: Option<(&str, Value)>) {
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    let attempt = f.start().await;
    let StorageInteractionOutcome::View(view) = f
        .backend
        .advance_auth_interaction(f.command(&attempt, "alice", "alice", json!([])))
        .await
        .unwrap()
    else {
        panic!("view");
    };
    let audit = change.map(|(field, value)| {
        let mut subject = serde_json::to_value(view.authenticated_subject().unwrap()).unwrap();
        subject[field] = value;
        serde_json::from_value(subject).unwrap()
    });
    assert!(
        f.backend
            .complete_auth_interaction(session_request(attempt.access, audit))
            .await
            .is_err()
    );
    assert!(f.backend.state.read().await.sessions.is_empty());
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

impl Fixture {
    async fn retained_session(&self) -> (Attempt, PrincipalId) {
        use hubuum_storage_core::StorageEncryptedUpstreamCredential;
        let attempt = self.start().await;
        let StorageExternalInteractionAuthentication::Login(authentication) =
            self.authentication(&attempt, "alice", "alice", json!(["/team"]))
        else {
            panic!("login");
        };
        let StorageInteractionOutcome::View(view) = self
            .backend
            .advance_auth_interaction(StorageInteractionCommand::new(
                StorageInteractionAuthorization::Claim(attempt.claim.clone()),
                StorageInteractionAction::AuthenticateExternal {
                    authentication: Box::new(
                        StorageExternalInteractionAuthentication::LoginWithCredential {
                            authentication,
                            credential: StorageEncryptedUpstreamCredential::new(
                                "encrypted-token".into(),
                            )
                            .unwrap(),
                        },
                    ),
                    approval: None,
                },
            ))
            .await
            .unwrap()
        else {
            panic!("view");
        };
        let StorageInteractionCompletion::Session(issued) = self
            .backend
            .complete_auth_interaction(session_request(
                attempt.access.clone(),
                view.authenticated_subject().cloned(),
            ))
            .await
            .unwrap()
        else {
            panic!("session");
        };
        (attempt, issued.into_value().principal_id())
    }
}

#[tokio::test]
async fn retained_token_is_consumed_once_across_memory_handles() {
    use hubuum_storage_core::StorageUpstreamClaimOutcome;
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    let (_, principal) = f.retained_session().await;
    let other = f.backend.clone();
    let (a, b) = tokio::join!(
        f.backend.claim_upstream_credential(principal),
        other.claim_upstream_credential(principal)
    );
    let outcomes = [a.unwrap(), b.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, StorageUpstreamClaimOutcome::Claim(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, StorageUpstreamClaimOutcome::Busy))
            .count(),
        1
    );
}

#[tokio::test]
async fn retained_renewal_updates_membership_and_rotates_atomically() {
    use hubuum_storage_core::{
        StorageEncryptedUpstreamCredential, StorageUpstreamClaimOutcome, StorageUpstreamRenewal,
    };
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    let (attempt, principal) = f.retained_session().await;
    let StorageUpstreamClaimOutcome::Claim(claim) = f
        .backend
        .claim_upstream_credential(principal)
        .await
        .unwrap()
    else {
        panic!("claim");
    };
    let StorageExternalInteractionAuthentication::Login(proof) =
        f.authentication(&attempt, "alice", "alice", json!([]))
    else {
        panic!("proof");
    };
    f.backend
        .finish_upstream_renewal(
            StorageUpstreamRenewal::new(
                *claim,
                proof,
                Some(StorageEncryptedUpstreamCredential::new("rotated".into()).unwrap()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let StorageUpstreamClaimOutcome::Claim(claim) = f
        .backend
        .claim_upstream_credential(principal)
        .await
        .unwrap()
    else {
        panic!("rotated claim");
    };
    assert_eq!(claim.credential().ciphertext(), "rotated");
    assert!(
        f.backend
            .state
            .read()
            .await
            .external_memberships
            .iter()
            .all(|(id, _)| *id != principal.id())
    );
}

#[rstest]
#[case::temporary(StorageExternalSyncFailure::Unavailable, true)]
#[case::rejected(StorageExternalSyncFailure::Rejected, false)]
#[tokio::test]
async fn failed_memory_renewal_never_reuses_consumed_credentials(
    #[case] failure: StorageExternalSyncFailure,
    #[case] preserves_snapshot: bool,
) {
    use hubuum_storage_core::StorageUpstreamClaimOutcome;
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    let (_, principal) = f.retained_session().await;
    let StorageUpstreamClaimOutcome::Claim(claim) = f
        .backend
        .claim_upstream_credential(principal)
        .await
        .unwrap()
    else {
        panic!("claim");
    };
    f.backend
        .fail_upstream_renewal(*claim, failure)
        .await
        .unwrap();
    assert_eq!(
        f.backend
            .get_external_principal_state(principal)
            .await
            .unwrap()
            .unwrap()
            .last_sync_success_at()
            .is_some(),
        preserves_snapshot
    );
    assert!(matches!(
        f.backend
            .claim_upstream_credential(principal)
            .await
            .unwrap(),
        StorageUpstreamClaimOutcome::Unavailable
    ));
}

#[rstest]
#[case::matching(false, true)]
#[case::different_provider(true, false)]
#[tokio::test]
async fn identity_lookup_preserves_provider_instance(#[case] other: bool, #[case] found: bool) {
    let f = Fixture::new(json!({"mode":"provision_without_grants"}), false).await;
    let (_, principal) = f.retained_session().await;
    let binding = InteractionBinding::try_new(
        InteractionId::new(Uuid::new_v4()).unwrap(),
        InteractionProviderId::external(if other {
            Uuid::new_v4()
        } else {
            f.provider.id()
        })
        .unwrap(),
        InteractionClientId::new("lookup").unwrap(),
        ResourceRevision::INITIAL,
        InteractionAuthority::login(),
    )
    .unwrap();
    let identity = ExternalAuthenticationContext::oidc(
        binding,
        IdentityScopeName::new("company").unwrap(),
        &f.definition,
    )
    .unwrap()
    .identity(ExternalSubject::new("alice").unwrap())
    .unwrap();
    assert_eq!(
        f.backend.find_external_identity(identity).await.unwrap(),
        found.then_some(principal)
    );
}
