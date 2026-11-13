use super::*;
use chrono::DateTime;
use hubuum_domain::{
    ExternalAuthenticationContext, ExternalIdentityProfile, ExternalSubject, IdentityScopeName,
    MembershipClaimSource, MembershipClaims, OidcProviderDefinition,
};
use serde_json::{Value, json};

struct ExternalFixture {
    database: Fixture,
    backend: PostgresStorage,
    provider: StorageProviderIdentityBinding,
    definition: OidcProviderDefinition,
}
impl ExternalFixture {
    async fn new(admission: Value) -> Self {
        let database = Fixture::new().await;
        let definition:OidcProviderDefinition=serde_json::from_value(json!({
            "issuer":"https://id.example/realm", "client_id":"hubuum", "client_authentication":"client_secret_basic",
            "scopes":["openid"], "signing_algorithms":["RS256"],"admission":admission,
            "membership":{"mode":"provider_groups", "refresh_interval_seconds":300, "max_stale_seconds":900, "claim_source":"id_token", "claim_pointer":"/groups", "key_mode":"full_path"}
        })).unwrap();
        let provider = StorageProviderIdentityBinding::try_new(
            "company".into(),
            Uuid::new_v4(),
            StorageAuthenticationProviderKind::Oidc,
            definition.issuer().as_str().into(),
        )
        .unwrap();
        database.owner(&format!("INSERT INTO authentication_provider_bindings(name,provider_id,kind,namespace) VALUES('company','{}','oidc','https://id.example/realm')",provider.id())).await.unwrap();
        let backend = database.replica();
        Self {
            database,
            backend,
            provider,
            definition,
        }
    }
    async fn start(
        &self,
        backend: &PostgresStorage,
    ) -> (Interaction, hubuum_domain::InteractionAuthenticationClaim) {
        self.start_with_lifetime(backend, Duration::minutes(5))
            .await
    }
    async fn start_with_lifetime(
        &self,
        backend: &PostgresStorage,
        lifetime: Duration,
    ) -> (Interaction, hubuum_domain::InteractionAuthenticationClaim) {
        let binding = InteractionBinding::try_new(
            InteractionId::new(Uuid::new_v4()).unwrap(),
            InteractionProviderId::external(self.provider.id()).unwrap(),
            InteractionClientId::new("external-test").unwrap(),
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
                    .provider(self.provider.clone())
                    .try_build()
                    .unwrap(),
            )
            .await
            .unwrap();
        let StorageInteractionOutcome::Claim(claim) = backend
            .advance_auth_interaction(StorageInteractionCommand::new(
                StorageInteractionAuthorization::Redemption(access.clone()),
                StorageInteractionAction::Claim(Uuid::new_v4()),
            ))
            .await
            .unwrap()
        else {
            panic!("claim");
        };
        (Interaction { binding, access }, claim)
    }
    fn command(
        &self,
        claim: &hubuum_domain::InteractionAuthenticationClaim,
        subject: &str,
        name: &str,
        groups: Value,
    ) -> StorageInteractionCommand {
        let context = ExternalAuthenticationContext::oidc(
            claim.binding().clone(),
            IdentityScopeName::new(self.provider.name()).unwrap(),
            &self.definition,
        )
        .unwrap();
        let proof = context
            .verified_oidc(
                self.definition.issuer().as_str(),
                ExternalIdentityProfile::new(
                    ExternalSubject::new(subject).unwrap(),
                    Some(name.into()),
                    None,
                )
                .unwrap(),
                MembershipClaims::Complete {
                    source: MembershipClaimSource::IdToken,
                    document: &json!({"groups":groups}),
                },
                Utc::now(),
            )
            .unwrap();
        StorageInteractionCommand::new(
            StorageInteractionAuthorization::Claim(claim.clone()),
            StorageInteractionAction::AuthenticateExternal {
                authentication: Box::new(StorageExternalInteractionAuthentication::Login(proof)),
                approval: None,
            },
        )
    }
    async fn login(&self, subject: &str, groups: Value) -> PrincipalId {
        let (interaction, claim) = self.start(&self.backend).await;
        let StorageInteractionOutcome::View(view) = self
            .backend
            .advance_auth_interaction(self.command(&claim, subject, subject, groups))
            .await
            .unwrap()
        else {
            panic!("authenticated view");
        };
        let StorageInteractionCompletion::Session(issued) = self
            .backend
            .complete_auth_interaction(
                interaction.complete_with_subject(view.authenticated_subject().cloned()),
            )
            .await
            .unwrap()
        else {
            panic!("session");
        };
        issued.into_value().principal_id()
    }
}

#[rstest::rstest]
#[case::administrator(true)]
#[case::non_administrator(false)]
#[tokio::test]
async fn session_audit_preserves_selected_administrator_status(#[case] is_admin: bool) {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let (interaction, claim) = f.start(&f.backend).await;
    let StorageInteractionOutcome::View(view) = f
        .backend
        .advance_auth_interaction(f.command(&claim, "alice", "alice", json!([])))
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
    let id = subject.principal_id();
    let completed = f
        .backend
        .complete_auth_interaction(interaction.complete_with_subject(Some(subject)))
        .await
        .unwrap();
    assert!(matches!(
        completed,
        StorageInteractionCompletion::Session(_)
    ));
    f.database.owner(&format!("DO $$ BEGIN IF (SELECT metadata#>>'{{subject,is_admin}}' FROM events WHERE entity_type='authentication' AND entity_id={}) IS DISTINCT FROM '{}' THEN RAISE EXCEPTION 'incorrect administrator audit'; END IF; END $$", id.id(),is_admin)).await.unwrap();
    f.database.cleanup().await;
}

#[rstest::rstest]
#[case::missing(false)]
#[case::wrong_principal(true)]
#[tokio::test]
async fn unbound_audit_snapshot_cannot_issue_a_session(#[case] wrong_principal: bool) {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let (interaction, claim) = f.start(&f.backend).await;
    let StorageInteractionOutcome::View(view) = f
        .backend
        .advance_auth_interaction(f.command(&claim, "alice", "alice", json!([])))
        .await
        .unwrap()
    else {
        panic!("view");
    };
    let subject = view.authenticated_subject().unwrap();
    let audit = if wrong_principal {
        let mut value = serde_json::to_value(subject).unwrap();
        value["principal_id"] = json!(subject.principal_id().id() + 1);
        Some(serde_json::from_value(value).unwrap())
    } else {
        None
    };
    assert!(
        f.backend
            .complete_auth_interaction(interaction.complete_with_subject(audit))
            .await
            .is_err()
    );
    f.database.owner("DO $$ BEGIN IF EXISTS(SELECT 1 FROM auth_sessions) THEN RAISE EXCEPTION 'unbound session issued'; END IF; END $$").await.unwrap();
    f.database.cleanup().await;
}

#[tokio::test]
async fn replicas_provision_one_identity_and_cannot_replay_callback() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let other = f.database.replica();
    let (a, claim_a) = f.start(&f.backend).await;
    let (b, claim_b) = f.start(&other).await;
    let first = f.command(&claim_a, "subject", "alice", json!([]));
    let second = f.command(&claim_b, "subject", "alice", json!([]));
    let (left, right) = tokio::join!(
        f.backend.advance_auth_interaction(first.clone()),
        other.advance_auth_interaction(second.clone())
    );
    assert!(left.is_ok() || right.is_ok(), "{left:?} {right:?}");
    let mut ids = Vec::new();
    for (result, interaction) in [(left, a), (right, b)] {
        if let Ok(StorageInteractionOutcome::View(view)) = result {
            let StorageInteractionCompletion::Session(issued) = f
                .backend
                .complete_auth_interaction(
                    interaction.complete_with_subject(view.authenticated_subject().cloned()),
                )
                .await
                .unwrap()
            else {
                panic!("session");
            };
            ids.push(issued.into_value().principal_id());
        }
    }
    assert!(ids.iter().all(|id| *id == ids[0]));
    assert!(f.backend.advance_auth_interaction(first).await.is_err());
    assert!(other.advance_auth_interaction(second).await.is_err());
    f.database.owner("DO $$ BEGIN IF (SELECT count(*) FROM principals WHERE external_subject='subject') <> 1 THEN RAISE EXCEPTION 'duplicate subject'; END IF; END $$").await.unwrap();
    f.database.cleanup().await;
}

#[tokio::test]
async fn membership_removal_preserves_manual_source_and_local_grants() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let id = f.login("alice", json!(["/team"])).await;
    let _ = f
        .backend
        .add_group_member(
            id,
            hubuum_domain::GroupId::new(1).unwrap(),
            &EventContext::system(),
        )
        .await
        .unwrap();
    // Restored provenance may contain an independent manual assignment even to
    // an externally managed group; reconciliation owns only its directory source.
    f.database.owner(&format!("INSERT INTO group_membership_sources(principal_id,group_id,source,source_scope_id,source_key) SELECT {},id,'manual',1,'' FROM groups WHERE external_key='/team'",id.id())).await.unwrap();
    f.login("alice", json!([])).await;
    f.database.owner(&format!("DO $$ BEGIN IF (SELECT count(*) FROM group_memberships WHERE principal_id={}) <> 2 THEN RAISE EXCEPTION 'manual grant lost'; END IF; IF EXISTS(SELECT 1 FROM group_membership_sources WHERE principal_id={} AND source='external') THEN RAISE EXCEPTION 'provider grant retained'; END IF; END $$",id.id(),id.id())).await.unwrap();
    f.database.cleanup().await;
}

#[tokio::test]
async fn older_provider_observation_cannot_regrant_removed_group() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let (_, claim) = f.start(&f.backend).await;
    let stale = f.command(&claim, "alice", "alice", json!(["/team"]));
    let id = f.login("alice", json!([])).await;
    assert!(f.backend.advance_auth_interaction(stale).await.is_err());
    f.database.owner(&format!("DO $$ BEGIN IF EXISTS(SELECT 1 FROM group_memberships WHERE principal_id={}) THEN RAISE EXCEPTION 'stale grant'; END IF; END $$",id.id())).await.unwrap();
    f.database.cleanup().await;
}

#[tokio::test]
async fn failed_interaction_write_rolls_back_identity_and_memberships() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let (_, claim) = f.start(&f.backend).await;
    f.database.owner("CREATE FUNCTION reject_external_ready() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture write failure'; END $$; CREATE TRIGGER reject_external_ready BEFORE UPDATE ON auth_interactions FOR EACH ROW EXECUTE FUNCTION reject_external_ready()").await.unwrap();
    assert!(
        f.backend
            .advance_auth_interaction(f.command(&claim, "alice", "alice", json!(["/team"])))
            .await
            .is_err()
    );
    f.database.owner("DO $$ BEGIN IF EXISTS(SELECT 1 FROM principals WHERE external_subject='alice') OR EXISTS(SELECT 1 FROM groups WHERE external_key='/team') THEN RAISE EXCEPTION 'partial sync'; END IF; END $$").await.unwrap();
    f.database.cleanup().await;
}

#[tokio::test]
async fn claim_expiry_during_synchronization_rolls_back_changes() {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    f.login("alice", json!([])).await;
    f.database.owner("CREATE FUNCTION delay_external_sync() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(3); RETURN NEW; END $$; CREATE TRIGGER delay_external_sync AFTER UPDATE ON principals FOR EACH ROW EXECUTE FUNCTION delay_external_sync()").await.unwrap();
    let (_, claim) = f
        .start_with_lifetime(&f.backend, Duration::seconds(2))
        .await;
    let started = std::time::Instant::now();
    let result = f
        .backend
        .advance_auth_interaction(f.command(&claim, "alice", "renamed", json!(["/team"])))
        .await;
    assert!(
        started.elapsed() >= std::time::Duration::from_secs(3),
        "synchronization must start before expiry"
    );
    assert!(result.is_err(), "{result:?}");
    f.database.owner("DO $$ BEGIN IF EXISTS(SELECT 1 FROM principals WHERE external_subject='alice' AND name<>'alice') OR EXISTS(SELECT 1 FROM groups WHERE external_key='/team') THEN RAISE EXCEPTION 'expired callback committed changes'; END IF; END $$").await.unwrap();
    f.database.cleanup().await;
}

#[rstest::rstest]
#[case::existing_only(json!({"mode":"existing_only"}))]
#[case::allowlist(json!({"mode":"subject_allowlist","subjects":["someone-else"]}))]
#[tokio::test]
async fn admission_denial_rolls_back_identity_scope(#[case] admission: Value) {
    let f = ExternalFixture::new(admission).await;
    let (_, claim) = f.start(&f.backend).await;
    assert!(
        f.backend
            .advance_auth_interaction(f.command(&claim, "alice", "alice", json!([])))
            .await
            .is_err()
    );
    f.database.owner("DO $$ BEGIN IF EXISTS(SELECT 1 FROM identity_scopes WHERE name='company') THEN RAISE EXCEPTION 'partial scope'; END IF; END $$").await.unwrap();
    f.database.cleanup().await;
}

#[rstest::rstest]
#[case::utc("UTC")]
#[case::behind("America/Los_Angeles")]
#[case::ahead("Asia/Tokyo")]
#[tokio::test]
async fn rejection_fences_a_previously_verified_callback(#[case] timezone: &str) {
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let id = f.login("alice", json!([])).await;
    let (_, claim) = f.start(&f.backend).await;
    let stale = f.command(&claim, "alice", "alice", json!(["/team"]));
    // A non-UTC database session exposes accidental timestamptz-to-timestamp
    // conversion. The rejection must remain on the callback's application clock.
    let rejection = f.database.replica_with_timezone(timezone);
    let before = Utc::now();
    rejection
        .record_external_sync_failure(id, StorageExternalSyncFailure::Rejected)
        .await
        .unwrap();
    let after = Utc::now();
    let attempted = f
        .backend
        .get_external_principal_state(id)
        .await
        .unwrap()
        .unwrap()
        .last_sync_attempted_at()
        .unwrap();
    // PostgreSQL stores microseconds, while the application clock has nanoseconds.
    assert!(attempted.timestamp_micros() >= before.timestamp_micros());
    assert!(attempted <= after);
    assert!(f.backend.advance_auth_interaction(stale).await.is_err());
    f.database.cleanup().await;
}

#[rstest::rstest]
#[case::same_subject("alice")]
#[case::conflicting_name("different-subject")]
#[tokio::test]
async fn fresh_approval_and_login_use_consistent_lock_order(#[case] login_subject: &str) {
    use hubuum_domain::{FreshExternalAuthenticationRequest, InteractionActor};
    let f = ExternalFixture::new(json!({"mode":"provision_without_grants"})).await;
    let id = f.login("alice", json!([])).await;
    let origin = f
        .backend
        .create_token(StorageTokenCreate::new(
            id,
            StorageTokenDigest::legacy_unidentified("external-approval-origin"),
            StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value();
    let operation = interaction_digest();
    let binding = InteractionBinding::try_new(
        InteractionId::new(Uuid::new_v4()).unwrap(),
        InteractionProviderId::external(f.provider.id()).unwrap(),
        InteractionClientId::new("external-approval-test").unwrap(),
        ResourceRevision::INITIAL,
        InteractionAuthority::approval(InteractionActor::new(id, origin.id()), operation.clone()),
    )
    .unwrap();
    let secret = interaction_digest();
    let access =
        StorageInteractionAccess::new(binding.id(), binding.client().clone(), secret.clone());
    // Exercise lifecycle normalization even on clocks with microsecond precision.
    let start = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap()
        + Duration::nanoseconds(789);
    let lifecycle = InteractionLifecycle::try_new(
        binding.clone(),
        InteractionMode::Credentials,
        start,
        start + Duration::minutes(5),
    )
    .unwrap();
    let start = lifecycle.created_at();
    f.backend
        .create_auth_interaction(
            StorageInteractionCreate::builder(lifecycle, secret)
                .provider(f.provider.clone())
                .actor(StorageInteractionActorLease::approval(
                    start + Duration::minutes(10),
                ))
                .try_build()
                .unwrap(),
        )
        .await
        .unwrap();
    let StorageInteractionOutcome::Claim(claim) = f
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
    let other = f.database.replica();
    let (_, login_claim) = f.start(&other).await;
    let login = f.command(&login_claim, login_subject, "alice", json!([]));
    let context = ExternalAuthenticationContext::oidc(
        binding,
        IdentityScopeName::new(f.provider.name()).unwrap(),
        &f.definition,
    )
    .unwrap();
    let expected = context
        .identity(ExternalSubject::new("alice").unwrap())
        .unwrap();
    let request =
        FreshExternalAuthenticationRequest::new(context.clone(), expected, start, 0).unwrap();
    let authentication = context
        .verified_oidc(
            f.definition.issuer().as_str(),
            ExternalIdentityProfile::new(
                ExternalSubject::new("alice").unwrap(),
                Some("alice".into()),
                None,
            )
            .unwrap(),
            MembershipClaims::Complete {
                source: MembershipClaimSource::IdToken,
                document: &json!({"groups":[]}),
            },
            Utc::now(),
        )
        .unwrap();
    let fresh = request
        .verify(
            authentication.clone(),
            Some(authentication.verified_at()),
            Utc::now(),
        )
        .unwrap();
    let approval = StorageInteractionCommand::new(
        StorageInteractionAuthorization::Claim(claim),
        StorageInteractionAction::AuthenticateExternal {
            authentication: Box::new(StorageExternalInteractionAuthentication::Approval(fresh)),
            approval: None,
        },
    );
    let (approved, _login) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(
            f.backend.advance_auth_interaction(approval),
            other.advance_auth_interaction(login)
        )
    })
    .await
    .expect("consistent lock order must not deadlock");
    approved.unwrap();
    let claim = StorageCredentialClaim::new(
        id,
        origin.id(),
        StorageCredentialOperation::CreateToken,
        digest('e'),
        StorageCredentialFingerprint::new(operation.as_str().to_owned()).unwrap(),
        Utc::now() - Duration::hours(24),
    )
    .target(Some(id));
    let request = StorageInteractionComplete::new(
        access,
        StorageInteractionIssuance::Approval {
            claim,
            operation_digest: operation,
            context: EventContext::user(id, None, None),
        },
    );
    assert!(matches!(
        f.backend
            .complete_auth_interaction(request.clone())
            .await
            .unwrap(),
        StorageInteractionCompletion::Approval(_)
    ));
    assert!(matches!(
        f.backend.complete_auth_interaction(request).await.unwrap(),
        StorageInteractionCompletion::Rejected(InteractionTransitionError::Consumed)
    ));
    f.database.cleanup().await;
}
