use super::*;
use hubuum_domain::{
    InteractionAuthority, InteractionBinding, InteractionClientId, InteractionDigest,
    InteractionId, InteractionLifecycle, InteractionMode, InteractionProviderId,
    InteractionTransitionError, PrincipalKind, UserId, VerifiedInteractionSubject,
};
use hubuum_events_core::EventSubject;

fn interaction_digest() -> InteractionDigest {
    InteractionDigest::new(Uuid::new_v4().simple().to_string().repeat(2)).unwrap()
}
fn credentials() -> SessionCredentials {
    let digest = || {
        StorageTokenDigest::try_new(
            Uuid::new_v4().to_string(),
            StorageTokenFormat::Version1,
            StorageTokenHashAlgorithm::HmacSha256V1,
            Some(StorageTokenHashKeyId::try_new("test").unwrap()),
        )
        .unwrap()
    };
    SessionCredentials::try_new(digest(), digest()).unwrap()
}
struct Interaction {
    binding: InteractionBinding,
    access: StorageInteractionAccess,
}

#[rstest::rstest]
#[case::database_behind(-100)]
#[case::database_ahead(100)]
#[tokio::test]
async fn database_clock_offset_does_not_reject_application_timestamps(#[case] milliseconds: i32) {
    let f = Fixture::new().await;
    // The fixture explicitly searches its own schema before pg_catalog, so only
    // this isolated adapter sees the offset database clock.
    f.owner(&format!("CREATE FUNCTION clock_timestamp() RETURNS timestamptz LANGUAGE sql VOLATILE AS $$ SELECT pg_catalog.clock_timestamp() + interval '{milliseconds} milliseconds' $$"))
        .await.unwrap();
    let backend = f.replica();
    let interaction = Interaction::create(&backend, Duration::minutes(5)).await;
    interaction.ready(&backend).await;
    assert!(matches!(
        backend
            .complete_auth_interaction(interaction.complete())
            .await
            .unwrap(),
        StorageInteractionCompletion::Session(_)
    ));
    f.cleanup().await;
}
impl Interaction {
    async fn create(backend: &PostgresStorage, lifetime: Duration) -> Self {
        let binding = InteractionBinding::try_new(
            InteractionId::new(Uuid::new_v4()).unwrap(),
            InteractionProviderId::local(),
            InteractionClientId::new("native-test").unwrap(),
            ResourceRevision::INITIAL,
            InteractionAuthority::login(),
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
            now + lifetime,
        )
        .unwrap();
        backend
            .create_auth_interaction(
                StorageInteractionCreate::builder(lifecycle, secret)
                    .try_build()
                    .unwrap(),
            )
            .await
            .unwrap();
        Self { binding, access }
    }
    async fn ready(&self, backend: &PostgresStorage) {
        let StorageInteractionOutcome::Claim(claim) = backend
            .advance_auth_interaction(StorageInteractionCommand::new(
                StorageInteractionAuthorization::Redemption(self.access.clone()),
                StorageInteractionAction::Claim(Uuid::new_v4()),
            ))
            .await
            .unwrap()
        else {
            panic!("claim rejected")
        };
        let actor = self
            .binding
            .authority()
            .actor()
            .map(|actor| actor.principal_id())
            .unwrap_or(owner().actor_id());
        let login = StorageLogin::try_new(
            EventSubject::try_new(
                actor,
                "recovery_admin".into(),
                PrincipalKind::Human,
                "local".into(),
                "local".into(),
                true,
            )
            .unwrap(),
            backend
                .get_user(UserId::new(actor.id()).unwrap())
                .await
                .unwrap(),
        )
        .unwrap();
        let proof =
            VerifiedInteractionSubject::new(self.binding.clone(), actor.id().to_string()).unwrap();
        let authentication =
            StorageInteractionAuthentication::login(proof, &login, Utc::now()).unwrap();
        assert!(matches!(
            backend
                .advance_auth_interaction(StorageInteractionCommand::new(
                    StorageInteractionAuthorization::Claim(claim),
                    StorageInteractionAction::Authenticate {
                        authentication: Box::new(authentication),
                        approval: None
                    }
                ))
                .await
                .unwrap(),
            StorageInteractionOutcome::View(_)
        ));
    }
    fn complete(&self) -> StorageInteractionComplete {
        StorageInteractionComplete::new(
            self.access.clone(),
            StorageInteractionIssuance::Session {
                credentials: credentials(),
                policy: SessionPolicy::try_new(900, 3600, 86400, 86400).unwrap(),
                context: context(),
            },
        )
    }
}

#[tokio::test]
async fn restarted_replicas_consume_durable_authentication_once() {
    let f = Fixture::new().await;
    let first = f.replica();
    let interaction = Interaction::create(&first, Duration::minutes(5)).await;
    interaction.ready(&first).await;
    drop(first);
    let a = f.replica();
    let b = f.replica();
    let (a, b) = tokio::join!(
        a.complete_auth_interaction(interaction.complete()),
        b.complete_auth_interaction(interaction.complete())
    );
    let results = [a.unwrap(), b.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, StorageInteractionCompletion::Session(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(
                r,
                StorageInteractionCompletion::Rejected(InteractionTransitionError::Consumed)
            ))
            .count(),
        1
    );
    f.owner("DO $$ BEGIN IF (SELECT count(*) FROM auth_sessions) <> 1 OR (SELECT count(*) FROM refresh_tokens) <> 1 OR (SELECT count(*) FROM events) <> 2 THEN RAISE EXCEPTION 'duplicate or missing issuance'; END IF; END $$;").await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn failed_issuance_audit_rolls_back_interaction_consumption() {
    let f = Fixture::new().await;
    let backend = f.replica();
    let interaction = Interaction::create(&backend, Duration::minutes(5)).await;
    interaction.ready(&backend).await;
    f.owner("ALTER TABLE events ADD CONSTRAINT reject_fixture_write CHECK (false)")
        .await
        .unwrap();
    assert!(
        backend
            .complete_auth_interaction(interaction.complete())
            .await
            .is_err()
    );
    f.owner("DO $$ BEGIN IF EXISTS(SELECT 1 FROM auth_sessions) OR EXISTS(SELECT 1 FROM refresh_tokens) THEN RAISE EXCEPTION 'orphan issuance'; END IF; END $$; ALTER TABLE events DROP CONSTRAINT reject_fixture_write;").await.unwrap();
    assert!(matches!(
        f.replica()
            .complete_auth_interaction(interaction.complete())
            .await
            .unwrap(),
        StorageInteractionCompletion::Session(_)
    ));
    f.cleanup().await;
}

#[tokio::test]
async fn database_trigger_rejects_reopening_a_consumed_interaction() {
    let f = Fixture::new().await;
    let backend = f.replica();
    let interaction = Interaction::create(&backend, Duration::minutes(5)).await;
    interaction.ready(&backend).await;
    backend
        .complete_auth_interaction(interaction.complete())
        .await
        .unwrap();
    assert!(f.owner("UPDATE auth_interactions SET document=jsonb_set(document,'{lifecycle,phase}','{\"state\":\"pending\"}'::jsonb)").await.is_err());
    f.cleanup().await;
}

#[tokio::test]
async fn expired_ready_evidence_remains_decodable_before_bounded_cleanup() {
    let f = Fixture::new().await;
    let backend = f.replica();
    let interaction = Interaction::create(&backend, Duration::seconds(2)).await;
    interaction.ready(&backend).await;
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    for backend in [backend, f.replica()] {
        assert!(matches!(
            backend
                .complete_auth_interaction(interaction.complete())
                .await
                .unwrap(),
            StorageInteractionCompletion::Rejected(InteractionTransitionError::Expired)
        ));
    }
    assert_eq!(f.replica().purge_auth_interactions(0).await.unwrap(), 0);
    assert_eq!(f.replica().purge_auth_interactions(1).await.unwrap(), 1);
    f.cleanup().await;
}

#[tokio::test]
async fn authentication_revision_changes_invalidate_durable_proofs() {
    let f = Fixture::new().await;
    let backend = f.replica();
    let interaction = Interaction::create(&backend, Duration::minutes(5)).await;
    interaction.ready(&backend).await;
    let candidate = candidate();
    backend
        .create_security_candidate(candidate.clone())
        .await
        .unwrap();
    let (claim, _) = approval(&backend, candidate.id()).await;
    backend
        .activate_security_candidate(activation(&candidate, claim))
        .await
        .unwrap();
    assert!(matches!(
        f.replica()
            .complete_auth_interaction(interaction.complete())
            .await
            .unwrap(),
        StorageInteractionCompletion::Rejected(InteractionTransitionError::ConfigurationChanged)
    ));
    assert!(matches!(
        f.replica()
            .complete_auth_interaction(interaction.complete())
            .await
            .unwrap(),
        StorageInteractionCompletion::Rejected(InteractionTransitionError::Failed)
    ));
    f.cleanup().await;
}

#[tokio::test]
async fn recovery_discards_pending_interaction_authority() {
    let f = Fixture::new().await;
    let backend = f.replica();
    let interaction = Interaction::create(&backend, Duration::minutes(5)).await;
    interaction.ready(&backend).await;
    backend
        .recover_security_settings(
            StorageSecurityRecovery::try_new(
                owner().actor_id(),
                "admin".into(),
                &backend.load_runtime_settings().await.unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        f.replica()
            .complete_auth_interaction(interaction.complete())
            .await
            .is_err()
    );
    f.cleanup().await;
}

#[tokio::test]
async fn activation_and_completion_share_a_non_deadlocking_lock_order() {
    let f = Fixture::new().await;
    let backend = f.replica();
    let second = f.replica();
    let interaction = Interaction::create(&backend, Duration::minutes(5)).await;
    interaction.ready(&backend).await;
    let candidate = candidate();
    backend
        .create_security_candidate(candidate.clone())
        .await
        .unwrap();
    let (claim, _) = approval(&backend, candidate.id()).await;
    let (completion, activation) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            backend.complete_auth_interaction(interaction.complete()),
            second.activate_security_candidate(activation(&candidate, claim))
        )
    })
    .await
    .expect("operations must not deadlock");
    assert!(matches!(
        completion.unwrap(),
        StorageInteractionCompletion::Session(_)
            | StorageInteractionCompletion::Rejected(
                InteractionTransitionError::ConfigurationChanged
            )
    ));
    activation.unwrap();
    f.cleanup().await;
}

async fn privileged_interaction(
    backend: &PostgresStorage,
    authority: InteractionAuthority,
    provider: Option<StorageProviderIdentityBinding>,
) -> Interaction {
    let binding = InteractionBinding::try_new(
        InteractionId::new(Uuid::new_v4()).unwrap(),
        provider
            .as_ref()
            .map(|p| InteractionProviderId::external(p.id()).unwrap())
            .unwrap_or_else(InteractionProviderId::local),
        InteractionClientId::new("privileged-test").unwrap(),
        ResourceRevision::INITIAL,
        authority,
    )
    .unwrap();
    let now = Utc::now();
    let secret = interaction_digest();
    let access =
        StorageInteractionAccess::new(binding.id(), binding.client().clone(), secret.clone());
    let verification = provider.is_some();
    let mut create = StorageInteractionCreate::builder(
        InteractionLifecycle::try_new(
            binding.clone(),
            InteractionMode::Credentials,
            now,
            now + Duration::minutes(5),
        )
        .unwrap(),
        secret,
    )
    .actor(if verification {
        StorageInteractionActorLease::verification(now + Duration::minutes(10), "admin").unwrap()
    } else {
        StorageInteractionActorLease::approval(now + Duration::minutes(10))
    });
    if let Some(provider) = provider {
        create = create.provider(provider);
    }
    backend
        .create_auth_interaction(create.try_build().unwrap())
        .await
        .unwrap();
    Interaction { binding, access }
}

#[tokio::test]
async fn credential_approval_is_bound_to_the_original_actor_and_operation() {
    use hubuum_domain::InteractionActor;
    let f = Fixture::new().await;
    let backend = f.replica();
    let claim = StorageCredentialClaim::new(
        owner().actor_id(),
        owner().token_id(),
        StorageCredentialOperation::CreateToken,
        digest('a'),
        digest('b'),
        Utc::now() - Duration::hours(24),
    )
    .target(Some(owner().actor_id()));
    let interaction = privileged_interaction(
        &backend,
        InteractionAuthority::approval(
            InteractionActor::new(owner().actor_id(), owner().token_id()),
            InteractionDigest::new(claim.request_digest().as_str()).unwrap(),
        ),
        None,
    )
    .await;
    interaction.ready(&backend).await;
    assert!(matches!(
        backend
            .complete_auth_interaction(interaction.complete())
            .await
            .unwrap(),
        StorageInteractionCompletion::Rejected(InteractionTransitionError::Invalid)
    ));
    let completion = StorageInteractionComplete::new(
        interaction.access.clone(),
        StorageInteractionIssuance::Approval {
            operation_digest: InteractionDigest::new(claim.request_digest().as_str()).unwrap(),
            claim,
            context: context(),
        },
    );
    let StorageInteractionCompletion::Approval(result) =
        backend.complete_auth_interaction(completion).await.unwrap()
    else {
        panic!("approval rejected")
    };
    assert!(
        backend
            .get_credential_approval(result.into_value().id())
            .await
            .unwrap()
            .consumed_at()
            .is_none()
    );
    f.owner("DO $$ BEGIN IF EXISTS(SELECT 1 FROM auth_sessions) THEN RAISE EXCEPTION 'approval issued a session'; END IF; END $$;").await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn revoking_the_initiating_credential_invalidates_a_ready_approval() {
    use hubuum_domain::InteractionActor;
    let f = Fixture::new().await;
    let backend = f.replica();
    let interaction = privileged_interaction(
        &backend,
        InteractionAuthority::approval(
            InteractionActor::new(owner().actor_id(), owner().token_id()),
            interaction_digest(),
        ),
        None,
    )
    .await;
    interaction.ready(&backend).await;
    f.owner("UPDATE tokens SET revoked_at=now() WHERE id=1")
        .await
        .unwrap();
    assert!(matches!(
        backend
            .complete_auth_interaction(interaction.complete())
            .await
            .unwrap(),
        StorageInteractionCompletion::Rejected(InteractionTransitionError::Invalid)
    ));
    f.cleanup().await;
}

#[tokio::test]
async fn provider_verification_consumes_only_the_exact_candidate_without_issuing_or_provisioning() {
    use hubuum_domain::InteractionActor;
    let f = Fixture::new().await;
    let backend = f.replica();
    let provider = StorageAuthenticationProvider::try_new(
        "directory",
        StorageAuthenticationProviderKind::Ldap,
        serde_json::json!({"url":"ldaps://directory.example"}),
    )
    .unwrap()
    .with_id(Uuid::new_v4())
    .unwrap();
    let provider_binding = StorageProviderIdentityBinding::from_provider(&provider).unwrap();
    let update = StorageRuntimeSettingsUpdate::new(
        StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Authentication(
            StorageAuthenticationProviders::try_new(vec![provider]).unwrap(),
        )),
        ResourceRevision::INITIAL,
        context(),
    );
    let candidate = StorageSecurityCandidate::try_new(
        Uuid::new_v4(),
        owner(),
        &update,
        StorageSecurityCandidateBinding::new(digest('a'), digest('b'), digest('c')),
    )
    .unwrap();
    let candidate = candidate
        .with_checks(vec![
            StorageSecurityCheck::try_new(
                StorageSecurityCheckKind::Configuration,
                StorageSecurityCheckStatus::NotRun,
                None,
                "awaiting_verification",
            )
            .unwrap(),
        ])
        .unwrap();
    backend
        .create_security_candidate(candidate.clone())
        .await
        .unwrap();
    let authority = InteractionAuthority::verification(
        InteractionActor::new(owner().actor_id(), owner().token_id()),
        candidate.id(),
        StorageInteractionAuthentication::candidate_digest(&candidate),
    )
    .unwrap();
    let interaction =
        privileged_interaction(&backend, authority, Some(provider_binding.clone())).await;
    let StorageInteractionOutcome::Claim(claim) = backend
        .advance_auth_interaction(StorageInteractionCommand::new(
            StorageInteractionAuthorization::Redemption(interaction.access.clone()),
            StorageInteractionAction::Claim(Uuid::new_v4()),
        ))
        .await
        .unwrap()
    else {
        panic!("claim rejected")
    };
    let checks = [
        StorageSecurityCheckKind::Configuration,
        StorageSecurityCheckKind::RecoveryAdministrator,
        StorageSecurityCheckKind::Identity,
    ]
    .into_iter()
    .map(|kind| {
        StorageSecurityCheck::try_new(
            kind,
            StorageSecurityCheckStatus::Passed,
            if kind == StorageSecurityCheckKind::Identity {
                Some(provider_binding.id())
            } else {
                None
            },
            "verified",
        )
        .unwrap()
    })
    .collect();
    let verified = candidate
        .clone()
        .with_checks(checks)
        .unwrap()
        .verify(digest('d'))
        .unwrap();
    let proof =
        VerifiedInteractionSubject::new(interaction.binding.clone(), "external-subject").unwrap();
    let authentication = StorageInteractionAuthentication::verification(proof, verified).unwrap();
    assert!(matches!(
        backend
            .advance_auth_interaction(StorageInteractionCommand::new(
                StorageInteractionAuthorization::Claim(claim),
                StorageInteractionAction::Authenticate {
                    authentication: Box::new(authentication),
                    approval: None
                }
            ))
            .await
            .unwrap(),
        StorageInteractionOutcome::View(_)
    ));
    assert!(matches!(
        backend
            .complete_auth_interaction(interaction.complete())
            .await
            .unwrap(),
        StorageInteractionCompletion::Rejected(InteractionTransitionError::Invalid)
    ));
    let completion = StorageInteractionComplete::new(
        interaction.access,
        StorageInteractionIssuance::ProviderVerification,
    );
    assert!(
        matches!(f.replica().complete_auth_interaction(completion).await.unwrap(),StorageInteractionCompletion::ProviderVerification(id) if id==candidate.id())
    );
    assert_eq!(
        backend
            .get_security_candidate(candidate.id(), owner())
            .await
            .unwrap()
            .state(Utc::now()),
        StorageSecurityCandidateState::Verified
    );
    f.owner("DO $$ BEGIN IF EXISTS(SELECT 1 FROM auth_sessions) OR (SELECT count(*) FROM principals) <> 1 OR (SELECT count(*) FROM group_memberships) <> 1 OR (SELECT count(*) FROM tokens) <> 1 THEN RAISE EXCEPTION 'verification changed identity or credential state'; END IF; END $$;").await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn reciprocal_approval_completions_lock_principals_before_tokens() {
    use hubuum_domain::InteractionActor;
    use std::time::Duration as StdDuration;
    use tokio::sync::Notify;

    let f = Fixture::new().await;
    f.owner("INSERT INTO principals(id,kind,name,identity_scope_id) VALUES(2,'human','second_admin',1); INSERT INTO users(id,kind,password) VALUES(2,'human','fixture-password-hash'); INSERT INTO tokens(id,token,principal_id,expires_at) VALUES(2,'second-token',2,now()+interval '1 hour');").await.unwrap();
    let backend = f.replica();
    let mut completions = Vec::new();
    for (actor, target) in [(1, 2), (2, 1)] {
        let actor = PrincipalId::new(actor).unwrap();
        let token = TokenId::new(actor.id()).unwrap();
        let claim = StorageCredentialClaim::new(
            actor,
            token,
            StorageCredentialOperation::CreateToken,
            digest(if actor.id() == 1 { 'a' } else { 'c' }),
            digest('b'),
            Utc::now() - Duration::hours(24),
        )
        .target(Some(PrincipalId::new(target).unwrap()));
        let operation_digest = InteractionDigest::new(claim.request_digest().as_str()).unwrap();
        let interaction = privileged_interaction(
            &backend,
            InteractionAuthority::approval(
                InteractionActor::new(actor, token),
                operation_digest.clone(),
            ),
            None,
        )
        .await;
        interaction.ready(&backend).await;
        completions.push(StorageInteractionComplete::new(
            interaction.access,
            StorageInteractionIssuance::Approval {
                claim,
                operation_digest,
                context: EventContext::user(actor, None, None),
            },
        ));
    }
    let b = completions.pop().unwrap();
    let a = completions.pop().unwrap();
    let locked = Notify::new();
    let blocker = with_transaction(
        &f.migration_pool,
        async |connection| -> Result<(), PostgresStorageError> {
            if database_role_tests_enabled() {
                connection
                    .batch_execute(&format!(
                        "SET LOCAL ROLE \"{}\"",
                        integration_test_database_roles().owner().as_str()
                    ))
                    .await?;
            }
            connection
                .batch_execute(&format!(
                    "SET LOCAL search_path TO {}; SELECT id FROM tokens WHERE id=1 FOR UPDATE",
                    f.schema
                ))
                .await?;
            locked.notify_one();
            tokio::time::sleep(StdDuration::from_millis(300)).await;
            Ok(())
        },
    );
    let complete = async {
        locked.notified().await;
        tokio::join!(
            backend.complete_auth_interaction(a),
            backend.complete_auth_interaction(b)
        )
    };
    let (blocker, (a, b)) = tokio::time::timeout(StdDuration::from_secs(5), async {
        tokio::join!(blocker, complete)
    })
    .await
    .expect("reciprocal approvals must not deadlock");
    blocker.unwrap();
    for result in [a, b] {
        assert!(matches!(
            result.unwrap(),
            StorageInteractionCompletion::Approval(_)
        ));
    }
    f.cleanup().await;
}
