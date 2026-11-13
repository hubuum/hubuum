use super::*;
use hubuum_domain::VerifiedExternalAuthentication;

fn encrypted(value: &str) -> StorageEncryptedUpstreamCredential {
    StorageEncryptedUpstreamCredential::new(value.into()).unwrap()
}

impl ExternalFixture {
    async fn retained_login(&self) -> PrincipalId {
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
        let StorageInteractionCompletion::Session(issued) = self
            .backend
            .complete_auth_interaction(
                interaction.complete_with_subject(view.authenticated_subject().cloned()),
            )
            .await
            .unwrap()
        else {
            panic!("session")
        };
        issued.into_value().principal_id()
    }

    async fn upstream_claim(&self, principal: PrincipalId) -> StorageUpstreamClaim {
        let StorageUpstreamClaimOutcome::Claim(claim) = self
            .backend
            .claim_upstream_credential(principal)
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
async fn concurrent_replicas_claim_one_refresh_token() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    let other = f.database.replica();
    let (a, b) = tokio::join!(
        f.backend.claim_upstream_credential(principal),
        other.claim_upstream_credential(principal)
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
async fn rejected_refresh_invalidates_external_authority() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let principal = f.retained_login().await;
    f.backend
        .fail_upstream_renewal(
            f.upstream_claim(principal).await,
            StorageExternalSyncFailure::Rejected,
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
            .claim_upstream_credential(principal)
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
            StorageExternalSyncFailure::Unavailable,
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
            .claim_upstream_credential(principal)
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
            .fail_upstream_renewal(claim, StorageExternalSyncFailure::Rejected)
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
#[case::matching(false)]
#[case::different_provider(true)]
#[tokio::test]
async fn logout_snapshot_is_scoped_to_the_requested_provider(#[case] other_provider: bool) {
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
        f.definition.issuer().as_str().into(),
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
        if other_provider {
            vec![]
        } else {
            vec![principal]
        }
    );
    f.database.cleanup().await;
}
