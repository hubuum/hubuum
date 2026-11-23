use super::*;
use crate::{approval, candidate_for, context, digest};
use hubuum_domain::VerifiedExternalAuthentication;
use hubuum_domain::{FreshExternalAuthenticationRequest, InteractionActor};

fn encrypted(value: &str) -> StorageEncryptedUpstreamCredential {
    StorageEncryptedUpstreamCredential::new(value.into()).unwrap()
}

impl ExternalFixture {
    async fn fresh_retention_command(&self, principal: PrincipalId) -> StorageInteractionCommand {
        let actor = self
            .backend
            .create_token(StorageTokenCreate::new(
                principal,
                StorageTokenDigest::legacy_unidentified("fresh-retention-origin"),
                StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let binding = InteractionBinding::try_new(
            InteractionId::new(Uuid::new_v4()).unwrap(),
            InteractionProviderId::external(self.provider.id()).unwrap(),
            InteractionClientId::new("fresh-retention").unwrap(),
            ResourceRevision::INITIAL,
            InteractionAuthority::approval(
                InteractionActor::new(principal, actor.id()),
                interaction_digest(),
            ),
        )
        .unwrap();
        let secret = interaction_digest();
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
        let started = lifecycle.created_at();
        self.backend
            .create_auth_interaction(
                StorageInteractionCreate::builder(lifecycle, secret)
                    .provider(self.provider.clone())
                    .actor(StorageInteractionActorLease::approval(
                        started + Duration::minutes(10),
                    ))
                    .try_build()
                    .unwrap(),
            )
            .await
            .unwrap();
        let StorageInteractionOutcome::Claim(claim) = self
            .backend
            .advance_auth_interaction(StorageInteractionCommand::new(
                StorageInteractionAuthorization::Redemption(access),
                StorageInteractionAction::Claim(Uuid::new_v4()),
            ))
            .await
            .unwrap()
        else {
            panic!("claim")
        };
        let context = ExternalAuthenticationContext::oidc(
            binding,
            IdentityScopeName::new("company").unwrap(),
            &self.definition,
        )
        .unwrap();
        let proof = context
            .verified_oidc(
                self.definition.issuer().as_str(),
                ExternalIdentityProfile::new(
                    ExternalSubject::new("alice").unwrap(),
                    Some("alice".into()),
                    None,
                )
                .unwrap(),
                MembershipClaims::Complete {
                    source: MembershipClaimSource::IdToken,
                    document: &json!({"groups":["/team"]}),
                },
                Utc::now(),
            )
            .unwrap();
        let fresh =
            FreshExternalAuthenticationRequest::new(context, proof.identity().clone(), started, 0)
                .unwrap()
                .verify(proof.clone(), Some(proof.verified_at()), Utc::now())
                .unwrap();
        let approval = self
            .backend
            .upstream_approval_session(fresh)
            .await
            .unwrap()
            .expect("retained session");
        StorageInteractionCommand::new(
            StorageInteractionAuthorization::Claim(claim),
            StorageInteractionAction::AuthenticateExternal {
                authentication: Box::new(
                    StorageExternalInteractionAuthentication::ApprovalWithCredential {
                        approval: Box::new(approval),
                        credential: encrypted("fresh-encrypted-token"),
                    },
                ),
                approval: None,
            },
        )
    }
    async fn retained_login(&self) -> PrincipalId {
        self.retained_interaction().await.0
    }
    async fn retained_interaction(&self) -> (PrincipalId, StorageInteractionAccess) {
        let (session, access, _) = self.retained_session().await;
        (session.principal_id(), access)
    }
    async fn retained_session(
        &self,
    ) -> (
        StorageSessionIssued,
        StorageInteractionAccess,
        StorageAuthenticationCredential,
    ) {
        let (interaction, claim) = self.start(&self.backend).await;
        let command = self.command(&claim, "alice", "alice", json!(["/team"]));
        let StorageInteractionAction::AuthenticateExternal { authentication, .. } =
            command.action()
        else {
            panic!("authentication")
        };
        let StorageExternalInteractionAuthentication::Login(proof) = authentication.as_ref() else {
            panic!("login")
        };
        let StorageInteractionOutcome::View(view) = self
            .backend
            .advance_auth_interaction(StorageInteractionCommand::new(
                StorageInteractionAuthorization::Claim(claim),
                StorageInteractionAction::AuthenticateExternal {
                    authentication: Box::new(
                        StorageExternalInteractionAuthentication::LoginWithCredential {
                            authentication: proof.clone(),
                            credential: encrypted("first-encrypted-token"),
                        },
                    ),
                    approval: None,
                },
            ))
            .await
            .unwrap()
        else {
            panic!("authenticated")
        };
        let completion = interaction.complete_with_subject(view.authenticated_subject().cloned());
        let StorageInteractionIssuance::Session { credentials, .. } = completion.issuance() else {
            panic!("session issuance")
        };
        let access_credential =
            StorageAuthenticationCredential::from_digest(credentials.access().clone());
        let StorageInteractionCompletion::Session(issued) = self
            .backend
            .complete_auth_interaction(completion)
            .await
            .unwrap()
        else {
            panic!("session")
        };
        (issued.into_value(), interaction.access, access_credential)
    }

    async fn upstream_claim(&self, principal: PrincipalId) -> StorageUpstreamClaim {
        let StorageUpstreamClaimOutcome::Claim(claim) = self
            .backend
            .claim_upstream_credential(ResourceRevision::INITIAL, principal)
            .await
            .unwrap()
        else {
            panic!("upstream claim")
        };
        *claim
    }

    fn renewal_proof(
        &self,
        claim: &StorageUpstreamClaim,
        subject: &str,
    ) -> VerifiedExternalAuthentication {
        ExternalAuthenticationContext::oidc(
            claim.session().binding().clone(),
            IdentityScopeName::new("company").unwrap(),
            &self.definition,
        )
        .unwrap()
        .verified_oidc(
            self.definition.issuer().as_str(),
            ExternalIdentityProfile::new(
                ExternalSubject::new(subject).unwrap(),
                Some(subject.into()),
                None,
            )
            .unwrap(),
            MembershipClaims::Complete {
                source: MembershipClaimSource::IdToken,
                document: &json!({"groups":[]}),
            },
            Utc::now(),
        )
        .unwrap()
    }
}

#[tokio::test]
async fn fresh_authentication_replaces_retained_credential_before_operation_completion() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let command = f.fresh_retention_command(principal).await;
    let StorageInteractionAction::AuthenticateExternal { authentication, .. } = command.action()
    else {
        panic!("external authentication");
    };
    let StorageExternalInteractionAuthentication::ApprovalWithCredential { approval, .. } =
        authentication.as_ref()
    else {
        panic!("fresh credential");
    };
    let resolved = f
        .backend
        .upstream_approval_session(approval.proof().clone())
        .await
        .unwrap()
        .expect("same eligible session");
    assert_eq!(resolved.session(), approval.session());
    f.backend.advance_auth_interaction(command).await.unwrap();
    assert_eq!(
        f.upstream_claim(principal).await.credential().ciphertext(),
        "fresh-encrypted-token"
    );
    f.database.cleanup().await;
}

#[rstest::rstest]
#[case::rejected_refresh(false)]
#[case::successful_refresh(true)]
#[tokio::test]
async fn fresh_authentication_fences_old_refresh_completion(#[case] success: bool) {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let old = f.upstream_claim(principal).await;
    let proof = f.renewal_proof(&old, "alice");
    let command = f.fresh_retention_command(principal).await;
    f.backend.advance_auth_interaction(command).await.unwrap();
    let stale = if success {
        f.backend
            .finish_upstream_renewal(
                StorageUpstreamRenewal::new(old, proof, Some(encrypted("stale"))).unwrap(),
            )
            .await
    } else {
        f.backend
            .fail_upstream_renewal(old, StorageUpstreamFailure::Rejected)
            .await
    };
    assert!(stale.is_err());
    assert_eq!(
        f.upstream_claim(principal).await.credential().ciphertext(),
        "fresh-encrypted-token"
    );
    f.database.cleanup().await;
}

#[rstest::rstest]
#[case::logout("UPDATE auth_sessions SET revoked_at=now(),revocation_reason='fixture'")]
#[case::configuration(
    "UPDATE runtime_settings SET revision=revision+1 WHERE setting_group='authentication'"
)]
#[case::expired_session(
    "UPDATE auth_sessions SET authenticated_at=now()-interval '2 hours',idle_expires_at=now()-interval '1 hour'"
)]
#[tokio::test]
async fn fresh_retention_rechecks_live_session_fences(#[case] change: &str) {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let command = f.fresh_retention_command(principal).await;
    f.database.owner(change).await.unwrap();
    let outcome = f.backend.advance_auth_interaction(command).await;
    assert!(!matches!(outcome, Ok(StorageInteractionOutcome::View(_))));
    f.database.cleanup().await;
}

#[tokio::test]
async fn concurrent_replicas_claim_one_refresh_token() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let other = f.database.replica();
    let (a, b) = tokio::join!(
        f.backend
            .claim_upstream_credential(ResourceRevision::INITIAL, principal),
        other.claim_upstream_credential(ResourceRevision::INITIAL, principal)
    );
    let outcomes = [a.unwrap(), b.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|value| matches!(value, StorageUpstreamClaimOutcome::Claim(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|value| matches!(value, StorageUpstreamClaimOutcome::Busy))
            .count(),
        1
    );
    f.database.owner("DO $$ BEGIN IF EXISTS(SELECT 1 FROM auth_upstream_credentials WHERE document->'credential' <> 'null'::jsonb) THEN RAISE EXCEPTION 'reusable token persisted'; END IF; END $$").await.unwrap();
    f.database.cleanup().await;
}

#[tokio::test]
async fn renewal_atomically_rotates_credentials_and_removes_provider_membership() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let claim = f.upstream_claim(principal).await;
    let proof = f.renewal_proof(&claim, "alice");
    f.backend
        .finish_upstream_renewal(
            StorageUpstreamRenewal::new(claim, proof, Some(encrypted("rotated-encrypted-token")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        f.upstream_claim(principal).await.credential().ciphertext(),
        "rotated-encrypted-token"
    );
    f.database.owner(&format!("DO $$ BEGIN IF EXISTS(SELECT 1 FROM group_membership_sources WHERE principal_id={} AND source='external') THEN RAISE EXCEPTION 'stale membership'; END IF; END $$", principal.id())).await.unwrap();
    f.database.cleanup().await;
}

#[rstest::rstest]
#[case::logout("UPDATE auth_sessions SET revoked_at=now(),revocation_reason='fixture'")]
#[case::configuration(
    "UPDATE runtime_settings SET revision=revision+1 WHERE setting_group='authentication'"
)]
#[case::expired_session(
    "UPDATE auth_sessions SET authenticated_at=now()-interval '2 hours',idle_expires_at=now()-interval '1 hour'"
)]
#[tokio::test]
async fn local_revocation_fences_an_inflight_renewal(#[case] change: &str) {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let claim = f.upstream_claim(principal).await;
    let proof = f.renewal_proof(&claim, "alice");
    f.database.owner(change).await.unwrap();
    assert!(
        f.backend
            .finish_upstream_renewal(
                StorageUpstreamRenewal::new(claim, proof, Some(encrypted("rotated"))).unwrap()
            )
            .await
            .is_err()
    );
    f.database.cleanup().await;
}

#[tokio::test]
async fn credential_expiry_preserves_other_session_and_personal_token() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let (other_session, _, other_credential) = f.retained_session().await;
    let principal = other_session.principal_id();
    f.retained_login().await;
    let pat = StorageAuthenticationCredential::new("personal-token");
    f.backend
        .create_token(StorageTokenCreate::new(
            principal,
            pat.digest().clone(),
            StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value();
    let claim = f.upstream_claim(principal).await;
    assert_ne!(claim.session().session(), other_session.session_id());
    f.backend
        .fail_upstream_renewal(claim, StorageUpstreamFailure::CredentialExpired)
        .await
        .unwrap();
    assert_eq!(
        f.upstream_claim(principal).await.session().session(),
        other_session.session_id()
    );
    for credential in [pat, other_credential] {
        let now = Utc::now();
        f.backend
            .authenticate_bearer_token(
                StorageAuthenticationAttempt::try_new(credential, now, now - Duration::days(1))
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    f.database.cleanup().await;
}

#[tokio::test]
async fn expired_access_logout_returns_the_retained_family_grant_once() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let (session, _, access) = f.retained_session().await;
    f.database.owner(&format!("UPDATE tokens SET issued=now()-interval '2 hours', expires_at=now()-interval '1 hour' WHERE id={}",session.access_id().id())).await.unwrap();
    let request = || {
        StorageTokenHashRevoke::try_candidates(None, vec![access.clone()], EventContext::system())
            .unwrap()
    };
    let grant = f
        .backend
        .revoke_token_by_hash(request())
        .await
        .unwrap()
        .into_value()
        .unwrap();
    assert_eq!(grant.session(), session.session_id());
    assert_eq!(
        grant.credential().unwrap().ciphertext(),
        "first-encrypted-token"
    );
    assert!(
        !f.backend
            .revoke_token_by_hash(request())
            .await
            .unwrap()
            .is_committed()
    );
    f.database.cleanup().await;
}

#[tokio::test]
async fn bulk_logout_captures_every_retained_grant() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    for _ in 1..35 {
        f.retained_login().await;
    }
    let grants = f
        .backend
        .revoke_auth_sessions(SessionRevocation::All(principal), EventContext::system())
        .await
        .unwrap()
        .into_value()
        .into_grants();
    let sessions: std::collections::HashSet<_> =
        grants.iter().map(|grant| grant.session().value()).collect();
    assert_eq!(sessions.len(), 35);
    f.database.cleanup().await;
}

#[tokio::test]
async fn rejected_refresh_invalidates_external_authority() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    f.backend
        .fail_upstream_renewal(
            f.upstream_claim(principal).await,
            StorageUpstreamFailure::Rejected,
        )
        .await
        .unwrap();
    assert!(
        f.backend
            .get_external_principal_state(principal)
            .await
            .unwrap()
            .unwrap()
            .last_sync_success_at()
            .is_none()
    );
    assert!(matches!(
        f.backend
            .claim_upstream_credential(ResourceRevision::INITIAL, principal)
            .await
            .unwrap(),
        StorageUpstreamClaimOutcome::Unavailable
    ));
    f.database.cleanup().await;
}

#[tokio::test]
async fn uncertain_refresh_destroys_token_but_preserves_bounded_snapshot() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let before = f
        .backend
        .get_external_principal_state(principal)
        .await
        .unwrap()
        .unwrap()
        .last_sync_success_at();
    f.backend
        .fail_upstream_renewal(
            f.upstream_claim(principal).await,
            StorageUpstreamFailure::Unavailable,
        )
        .await
        .unwrap();
    assert_eq!(
        f.backend
            .get_external_principal_state(principal)
            .await
            .unwrap()
            .unwrap()
            .last_sync_success_at(),
        before
    );
    assert!(matches!(
        f.backend
            .claim_upstream_credential(ResourceRevision::INITIAL, principal)
            .await
            .unwrap(),
        StorageUpstreamClaimOutcome::Unavailable
    ));
    f.database.cleanup().await;
}

#[tokio::test]
async fn failed_refresh_commit_rolls_back_membership_changes() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let claim = f.upstream_claim(principal).await;
    let proof = f.renewal_proof(&claim, "alice");
    f.database.owner("CREATE FUNCTION reject_renewal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture write failure'; END $$; CREATE TRIGGER reject_renewal BEFORE UPDATE ON auth_upstream_credentials FOR EACH ROW EXECUTE FUNCTION reject_renewal()").await.unwrap();
    assert!(
        f.backend
            .finish_upstream_renewal(
                StorageUpstreamRenewal::new(claim, proof, Some(encrypted("rotated"))).unwrap()
            )
            .await
            .is_err()
    );
    f.database.owner(&format!("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM group_membership_sources WHERE principal_id={} AND source='external') THEN RAISE EXCEPTION 'partial renewal'; END IF; END $$", principal.id())).await.unwrap();
    f.database.cleanup().await;
}

#[tokio::test]
async fn newer_login_fences_old_refresh_rejection() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let claim = f.upstream_claim(principal).await;
    f.login("alice", json!([])).await;
    assert!(
        f.backend
            .fail_upstream_renewal(claim, StorageUpstreamFailure::Rejected)
            .await
            .is_err()
    );
    assert!(
        f.backend
            .get_external_principal_state(principal)
            .await
            .unwrap()
            .unwrap()
            .last_sync_success_at()
            .is_some()
    );
    f.database.cleanup().await;
}

#[rstest::rstest]
#[case::matching("alice", false, true)]
#[case::different_subject("other", false, false)]
#[case::different_provider("alice", true, false)]
#[tokio::test]
async fn identity_lookup_requires_the_exact_provider_subject(
    #[case] subject: &str,
    #[case] other_provider: bool,
    #[case] found: bool,
) {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let binding = InteractionBinding::try_new(
        InteractionId::new(Uuid::new_v4()).unwrap(),
        InteractionProviderId::external(if other_provider {
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
    .identity(ExternalSubject::new(subject).unwrap())
    .unwrap();
    assert_eq!(
        f.backend.find_external_identity(identity).await.unwrap(),
        found.then_some(principal)
    );
    f.database.cleanup().await;
}

#[rstest::rstest]
#[case::matching(false, false)]
#[case::different_provider(true, false)]
#[case::different_namespace(false, true)]
#[tokio::test]
async fn logout_snapshot_is_scoped_to_the_requested_provider(
    #[case] other_provider: bool,
    #[case] other_namespace: bool,
) {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let provider = StorageProviderIdentityBinding::try_new(
        "company".into(),
        if other_provider {
            Uuid::new_v4()
        } else {
            f.provider.id()
        },
        StorageAuthenticationProviderKind::Oidc,
        if other_namespace {
            "https://other.example/realm".into()
        } else {
            f.definition.issuer().as_str().into()
        },
    )
    .unwrap();
    let records = f
        .backend
        .upstream_logout_credentials(hubuum_storage_core::StorageUpstreamRevocation::Provider(
            provider,
        ))
        .await
        .unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.principal())
            .collect::<Vec<_>>(),
        if other_provider || other_namespace {
            vec![]
        } else {
            vec![principal]
        }
    );
    f.database.cleanup().await;
}

#[tokio::test]
async fn a_pre_submission_failure_preserves_the_snapshot_sessions_and_personal_token() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let (first, _, _) = f.retained_session().await;
    let (second, _, _) = f.retained_session().await;
    let principal = first.principal_id();
    let personal = f
        .backend
        .create_token(StorageTokenCreate::new(
            principal,
            StorageTokenDigest::legacy_unidentified("pre-submission-personal-token"),
            StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value();
    let state = || f.backend.get_external_principal_state(principal);
    let verified = state()
        .await
        .unwrap()
        .unwrap()
        .last_sync_success_at()
        .unwrap();
    let claim = f.upstream_claim(principal).await;
    f.backend
        .fail_upstream_renewal(claim, StorageUpstreamFailure::UnavailableBeforeSubmission)
        .await
        .unwrap();
    assert_eq!(
        state().await.unwrap().unwrap().last_sync_success_at(),
        Some(verified)
    );
    let observation =
        StorageTokenObservation::try_new(Utc::now(), chrono::DateTime::UNIX_EPOCH).unwrap();
    let tokens = f
        .backend
        .load_token_metadata_by_ids(
            vec![first.access_id(), second.access_id(), personal.id()],
            observation,
        )
        .await
        .unwrap();
    assert!(tokens.iter().all(|token| token.revoked_at().is_none()));
    f.database.cleanup().await;
}

#[tokio::test]
async fn a_deferred_grant_does_not_hide_another_sessions_ready_grant() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    f.retained_login().await;
    let claim = f.upstream_claim(principal).await;
    let deferred = claim.session().session();
    f.backend
        .fail_upstream_renewal(claim, StorageUpstreamFailure::UnavailableBeforeSubmission)
        .await
        .unwrap();
    let next = f.upstream_claim(principal).await;
    assert_ne!(next.session().session(), deferred);
    assert!(matches!(
        f.backend
            .claim_upstream_credential(ResourceRevision::INITIAL, principal)
            .await
            .unwrap(),
        StorageUpstreamClaimOutcome::Busy
    ));
    f.database.cleanup().await;
}

#[tokio::test]
async fn concurrent_login_is_either_live_or_captured_by_bulk_logout() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let other = f.database.replica();
    let ((issued, _, _), logout) = tokio::join!(
        f.retained_session(),
        other.revoke_auth_sessions(SessionRevocation::All(principal), EventContext::system())
    );
    let revoked = logout.unwrap().into_value().into_grants();
    let remaining = f
        .backend
        .upstream_logout_credentials(SessionRevocation::All(principal).into())
        .await
        .unwrap();
    let contains = |grants: &[StorageUpstreamSession]| {
        grants
            .iter()
            .any(|grant| grant.session() == issued.session_id())
    };
    assert_ne!(contains(&revoked), contains(&remaining));
    assert_eq!(revoked.len() + remaining.len(), 2);
    f.database.cleanup().await;
}

#[tokio::test]
async fn expired_lease_does_not_hide_another_sessions_credential() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    f.retained_login().await;
    let spent = f
        .upstream_claim(principal)
        .await
        .session()
        .session()
        .value();
    f.database.owner(&format!("UPDATE auth_upstream_credentials SET document=jsonb_set(document, '{{lease,1}}', to_jsonb(now() - interval '1 minute')) WHERE session_id='{spent}'")).await.unwrap();
    assert_ne!(
        f.upstream_claim(principal)
            .await
            .session()
            .session()
            .value(),
        spent
    );
    f.database.cleanup().await;
}

#[tokio::test]
async fn orphan_cleanup_returns_only_the_issued_grant_once() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let (principal, access) = f.retained_interaction().await;
    f.retained_login().await;
    let credential = f
        .backend
        .revoke_auth_interaction_session(access.clone(), EventContext::system())
        .await
        .unwrap()
        .into_value()
        .unwrap();
    assert_ne!(
        credential.session(),
        f.upstream_claim(principal).await.session().session()
    );
    assert!(
        f.backend
            .revoke_auth_interaction_session(access, EventContext::system())
            .await
            .unwrap()
            .into_value()
            .is_none()
    );
    f.database.cleanup().await;
}

#[tokio::test]
async fn signed_membership_rejection_invalidates_only_the_observed_subject() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let alice = f.login("alice", json!(["/team"])).await;
    let bob = f.login("bob", json!([])).await;
    let (_, claim) = f.start(&f.backend).await;
    let context = ExternalAuthenticationContext::oidc(
        claim.binding().clone(),
        IdentityScopeName::new(f.provider.name()).unwrap(),
        &f.definition,
    )
    .unwrap();
    let Err(hubuum_domain::ExternalAuthenticationError::MembershipRejected(rejection)) = context
        .verified_oidc(
            f.definition.issuer().as_str(),
            ExternalIdentityProfile::new(ExternalSubject::new("alice").unwrap(), None, None)
                .unwrap(),
            MembershipClaims::Absent,
            Utc::now(),
        )
    else {
        panic!("expected verified membership rejection")
    };
    f.backend
        .advance_auth_interaction(StorageInteractionCommand::new(
            StorageInteractionAuthorization::Claim(claim),
            StorageInteractionAction::RejectMembership(rejection),
        ))
        .await
        .unwrap();
    for (principal, valid) in [(alice, false), (bob, true)] {
        assert_eq!(
            f.backend
                .get_external_principal_state(principal)
                .await
                .unwrap()
                .unwrap()
                .last_sync_success_at()
                .is_some(),
            valid
        );
    }
    f.database.cleanup().await;
}

fn browser_assignment(
    providers: Vec<StorageAuthenticationProvider>,
) -> StorageRuntimeSettingsAssignment {
    StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Authentication(
        StorageAuthenticationProviders::try_new(providers).unwrap(),
    ))
}

impl ExternalFixture {
    async fn install_browser_definition(&self) -> StorageAuthenticationProvider {
        let browser = StorageAuthenticationProvider::try_new(
            "company",
            StorageAuthenticationProviderKind::Oidc,
            serde_json::to_value(&self.definition).unwrap(),
        )
        .unwrap()
        .with_id(self.provider.id())
        .unwrap();
        let previous = browser_assignment(vec![browser.clone()]);
        self.database.owner(&format!("UPDATE runtime_settings SET value='{}'::jsonb WHERE setting_group='authentication'", previous.to_document().unwrap())).await.unwrap();
        browser
    }

    async fn activate_with_directory(
        &self,
        browser: Option<StorageAuthenticationProvider>,
    ) -> StorageSecurityCandidateActivationOutcome {
        let directory = StorageAuthenticationProvider::deployment(
            "directory",
            StorageAuthenticationProviderKind::Ldap,
            true,
        )
        .unwrap()
        .with_id(Uuid::new_v4())
        .unwrap();
        let providers = browser.into_iter().chain([directory]).collect();
        let update = StorageRuntimeSettingsUpdate::new(
            browser_assignment(providers),
            ResourceRevision::INITIAL,
            context(),
        );
        let candidate = candidate_for(&update);
        self.backend
            .create_security_candidate(candidate.clone())
            .await
            .unwrap();
        let (claim, _) = approval(&self.backend, candidate.id()).await;
        let activation = StorageSecurityCandidateActivation::try_new(
            &candidate,
            digest('d'),
            update,
            claim,
            "admin".into(),
        )
        .unwrap();
        self.backend
            .activate_security_candidate(activation)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn removing_a_browser_provider_returns_its_prior_definition_and_committed_grants() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let browser = f.install_browser_definition().await;
    let (session, _, _) = f.retained_session().await;
    let revocations = f.activate_with_directory(None).await.into_revocations();
    assert_eq!(revocations.len(), 1);
    let (provider, grants) = &revocations[0];
    assert_eq!(provider, &browser);
    assert_eq!(
        grants
            .iter()
            .map(StorageUpstreamSession::session)
            .collect::<Vec<_>>(),
        [session.session_id()]
    );
    f.database.cleanup().await;
}

#[tokio::test]
async fn activating_an_unrelated_provider_retains_the_grant_and_fences_old_claims() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let browser = f.install_browser_definition().await;
    let principal = f.retained_login().await;
    let old = f.upstream_claim(principal).await;
    f.retained_login().await;
    let activation = f.activate_with_directory(Some(browser)).await;
    let current = activation.record().revision();
    assert!(activation.into_revocations().is_empty());
    let proof = f.renewal_proof(&old, "alice");
    assert!(
        f.backend
            .finish_upstream_renewal(StorageUpstreamRenewal::new(old, proof, None).unwrap())
            .await
            .is_err()
    );
    let StorageUpstreamClaimOutcome::Claim(retained) = f
        .backend
        .claim_upstream_credential(current, principal)
        .await
        .unwrap()
    else {
        panic!("unspent session grant")
    };
    let proof = f.renewal_proof(&retained, "alice");
    f.backend
        .finish_upstream_renewal(
            StorageUpstreamRenewal::new(*retained, proof, Some(encrypted("new-token"))).unwrap(),
        )
        .await
        .unwrap();
    f.database.cleanup().await;
}

#[tokio::test]
async fn outage_before_token_submission_retries_the_original_grant_after_backoff() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let claim = f.upstream_claim(principal).await;
    f.backend
        .fail_upstream_renewal(claim, StorageUpstreamFailure::UnavailableBeforeSubmission)
        .await
        .unwrap();
    assert!(matches!(
        f.backend
            .claim_upstream_credential(ResourceRevision::INITIAL, principal)
            .await
            .unwrap(),
        StorageUpstreamClaimOutcome::RetryLater
    ));
    f.database
        .owner("UPDATE auth_upstream_credentials SET document = document - 'retry_after'")
        .await
        .unwrap();
    assert_eq!(
        f.upstream_claim(principal).await.credential().ciphertext(),
        "first-encrypted-token"
    );
    f.database.cleanup().await;
}
