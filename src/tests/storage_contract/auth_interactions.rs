use super::*;
use chrono::{Duration as ChronoDuration, Utc};
use hubuum_domain::{
    InteractionAuthority, InteractionBinding, InteractionBrowserBinding, InteractionClientId,
    InteractionDigest, InteractionId, InteractionIssuedResult, InteractionLifecycle,
    InteractionMode, InteractionProviderId, InteractionState, InteractionTransitionError,
    VerifiedInteractionSubject,
};
use hubuum_storage_core::{
    SessionCredentials, SessionPolicy, StorageInteractionAccess, StorageInteractionAction,
    StorageInteractionAuthentication, StorageInteractionAuthorization, StorageInteractionCommand,
    StorageInteractionComplete, StorageInteractionCompletion, StorageInteractionCreate,
    StorageInteractionIssuance, StorageInteractionOutcome,
};
use rstest::rstest;
use uuid::Uuid;

fn digest() -> InteractionDigest {
    InteractionDigest::new(crate::models::Token::storage_hash_from_raw(&prefix(
        "interaction",
    )))
    .unwrap()
}
fn browser() -> InteractionBrowserBinding {
    InteractionBrowserBinding::try_new(digest(), digest()).unwrap()
}
fn credentials() -> SessionCredentials {
    super::auth_sessions::credentials()
}
struct Fixture {
    backend: StorageHandle,
    user: BackendUserFixture,
    binding: InteractionBinding,
    access: StorageInteractionAccess,
    user_code: InteractionDigest,
    redemption: InteractionDigest,
}
impl Fixture {
    async fn new(backend: StorageHandle, mode: InteractionMode) -> Self {
        let user = create_backend_user(&backend, &prefix("interaction_user")).await;
        Self::for_user(backend, user, mode, InteractionAuthority::login(), None).await
    }
    async fn for_user(
        backend: StorageHandle,
        user: BackendUserFixture,
        mode: InteractionMode,
        authority: InteractionAuthority,
        actor: Option<hubuum_storage_core::StorageInteractionActorLease>,
    ) -> Self {
        let binding = InteractionBinding::try_new(
            InteractionId::new(Uuid::new_v4()).unwrap(),
            InteractionProviderId::local(),
            InteractionClientId::new(prefix("interaction_client")).unwrap(),
            ResourceRevision::INITIAL,
            authority,
        )
        .unwrap();
        let secret = digest();
        let access =
            StorageInteractionAccess::new(binding.id(), binding.client().clone(), secret.clone());
        let user_code = digest();
        let now = Utc::now();
        let lifecycle = InteractionLifecycle::try_new(
            binding.clone(),
            mode,
            now,
            now + ChronoDuration::minutes(5),
        )
        .unwrap();
        let mut create = StorageInteractionCreate::builder(lifecycle, secret.clone());
        if let Some(actor) = actor {
            create = create.actor(actor);
        }
        if mode == InteractionMode::Browser {
            create = create.user_code(user_code.clone());
        }
        backend
            .create_auth_interaction(create.try_build().unwrap())
            .await
            .unwrap();
        Self {
            backend,
            user,
            binding,
            access,
            user_code,
            redemption: secret,
        }
    }
    async fn authenticate(
        &self,
        browser: Option<InteractionBrowserBinding>,
    ) -> Option<InteractionBrowserBinding> {
        let authorization = match &browser {
            Some(browser) => StorageInteractionAuthorization::Browser {
                id: self.binding.id(),
                binding: browser.clone(),
            },
            None => StorageInteractionAuthorization::Redemption(self.access.clone()),
        };
        let StorageInteractionOutcome::Claim(claim) = self
            .backend
            .advance_auth_interaction(StorageInteractionCommand::new(
                authorization,
                StorageInteractionAction::Claim(Uuid::new_v4()),
            ))
            .await
            .unwrap()
        else {
            panic!("claim rejected")
        };
        let login = super::login_events::login_request(
            &self.backend,
            &self.user,
            &prefix("interaction_login"),
        )
        .await
        .into_parts()
        .login()
        .unwrap()
        .clone();
        let proof = VerifiedInteractionSubject::new(
            self.binding.clone(),
            self.user.principal_id.id().to_string(),
        )
        .unwrap();
        let authentication =
            StorageInteractionAuthentication::login(proof, &login, Utc::now()).unwrap();
        let approval = browser.map(|browser| {
            InteractionBrowserBinding::try_new(browser.cookie().clone(), digest()).unwrap()
        });
        assert!(matches!(
            self.backend
                .advance_auth_interaction(StorageInteractionCommand::new(
                    StorageInteractionAuthorization::Claim(claim),
                    StorageInteractionAction::Authenticate {
                        authentication: Box::new(authentication),
                        approval: approval.clone()
                    }
                ))
                .await
                .unwrap(),
            StorageInteractionOutcome::View(_)
        ));
        approval
    }
    fn completion(&self, credentials: SessionCredentials) -> StorageInteractionComplete {
        StorageInteractionComplete::new(
            self.access.clone(),
            StorageInteractionIssuance::Session {
                credentials,
                policy: SessionPolicy::try_new(900, 604800, 2592000, 2592000).unwrap(),
                context: EventContext::system(),
                audit_subject: None,
            },
        )
    }
    async fn cleanup(self) {
        delete_backend_user(&self.backend, self.user).await;
    }

    async fn inspect(&self) -> hubuum_storage_core::StorageInteractionView {
        let StorageInteractionOutcome::View(view) = self
            .backend
            .advance_auth_interaction(StorageInteractionCommand::new(
                StorageInteractionAuthorization::Redemption(self.access.clone()),
                StorageInteractionAction::Inspect,
            ))
            .await
            .unwrap()
        else {
            panic!("view");
        };
        view
    }
}

#[actix_web::test]
async fn concurrent_completion_issues_one_session_and_replay_only_returns_metadata() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend, InteractionMode::Credentials).await;
        f.authenticate(None).await;
        let a = credentials();
        let b = credentials();
        let (a_result, b_result) = tokio::join!(
            f.backend.complete_auth_interaction(f.completion(a.clone())),
            f.backend.complete_auth_interaction(f.completion(b.clone()))
        );
        let results = [a_result.unwrap(), b_result.unwrap()];
        let sessions: Vec<_> = results
            .iter()
            .filter_map(|result| match result {
                StorageInteractionCompletion::Session(session) => {
                    Some(session.clone().into_value())
                }
                _ => None,
            })
            .collect();
        assert_eq!(sessions.len(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    StorageInteractionCompletion::Rejected(InteractionTransitionError::Consumed)
                ))
                .count(),
            1
        );
        let StorageInteractionOutcome::View(view) = f
            .backend
            .advance_auth_interaction(StorageInteractionCommand::new(
                StorageInteractionAuthorization::Redemption(f.access.clone()),
                StorageInteractionAction::Poll,
            ))
            .await
            .unwrap()
        else {
            panic!("metadata unavailable")
        };
        assert_eq!(
            view.issued(),
            Some(InteractionIssuedResult::Session(
                sessions[0].session_id().value()
            ))
        );
        let active = [
            super::auth_sessions::authenticates(&f.backend, a.access().clone()).await,
            super::auth_sessions::authenticates(&f.backend, b.access().clone()).await,
        ];
        assert_eq!(active.iter().filter(|active| **active).count(), 1);
        for (result, active) in results.iter().zip(active) {
            assert_eq!(
                active,
                matches!(result, StorageInteractionCompletion::Session(_))
            );
        }
        f.cleanup().await;
    }
}

#[actix_web::test]
async fn response_loss_cleanup_revokes_only_the_issued_family_and_is_repeatable() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend, InteractionMode::Credentials).await;
        let independent = credentials();
        super::auth_sessions::create(&f.backend, &f.user, independent.clone()).await;
        f.authenticate(None).await;
        let issued = credentials();
        assert!(matches!(
            f.backend
                .complete_auth_interaction(f.completion(issued.clone()))
                .await
                .unwrap(),
            StorageInteractionCompletion::Session(_)
        ));
        for _ in 0..2 {
            f.backend
                .revoke_auth_interaction_session(f.access.clone(), EventContext::system())
                .await
                .unwrap()
                .into_value();
        }
        assert!(!super::auth_sessions::authenticates(&f.backend, issued.access().clone()).await);
        assert!(
            super::auth_sessions::authenticates(&f.backend, independent.access().clone()).await
        );
        f.cleanup().await;
    }
}

#[rstest]
#[case::approve(true)]
#[case::deny(false)]
#[actix_web::test]
async fn browser_completion_requires_the_rotated_explicit_decision(#[case] approve: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend, InteractionMode::Browser).await;
        let initial = browser();
        f.backend
            .bind_auth_interaction_browser(f.user_code.clone(), initial.clone())
            .await
            .unwrap();
        let approval = f.authenticate(Some(initial.clone())).await.unwrap();
        assert!(matches!(
            f.backend
                .complete_auth_interaction(f.completion(credentials()))
                .await
                .unwrap(),
            StorageInteractionCompletion::Rejected(InteractionTransitionError::Pending)
        ));
        assert!(
            f.backend
                .advance_auth_interaction(StorageInteractionCommand::new(
                    StorageInteractionAuthorization::Browser {
                        id: f.binding.id(),
                        binding: initial
                    },
                    StorageInteractionAction::Decide { approve: true }
                ))
                .await
                .is_err()
        );
        f.backend
            .advance_auth_interaction(StorageInteractionCommand::new(
                StorageInteractionAuthorization::Browser {
                    id: f.binding.id(),
                    binding: approval,
                },
                StorageInteractionAction::Decide { approve },
            ))
            .await
            .unwrap();
        let outcome = f
            .backend
            .complete_auth_interaction(f.completion(credentials()))
            .await
            .unwrap();
        assert!(if approve {
            matches!(outcome, StorageInteractionCompletion::Session(_))
        } else {
            matches!(
                outcome,
                StorageInteractionCompletion::Rejected(InteractionTransitionError::Denied)
            )
        });
        f.cleanup().await;
    }
}

#[rstest]
#[case::wrong_secret(false)]
#[case::wrong_client(true)]
#[actix_web::test]
async fn unauthorized_cancellation_cannot_modify_the_interaction(#[case] wrong_client: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend, InteractionMode::Credentials).await;
        let access = StorageInteractionAccess::new(
            f.binding.id(),
            if wrong_client {
                InteractionClientId::new("wrong-client").unwrap()
            } else {
                f.binding.client().clone()
            },
            if wrong_client {
                f.redemption.clone()
            } else {
                digest()
            },
        );
        assert!(
            f.backend
                .advance_auth_interaction(StorageInteractionCommand::new(
                    StorageInteractionAuthorization::Redemption(access),
                    StorageInteractionAction::Cancel
                ))
                .await
                .is_err()
        );
        f.authenticate(None).await;
        assert!(matches!(
            f.backend
                .complete_auth_interaction(f.completion(credentials()))
                .await
                .unwrap(),
            StorageInteractionCompletion::Session(_)
        ));
        f.cleanup().await;
    }
}

#[actix_web::test]
async fn rejected_early_polls_commit_increasing_backoff() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend, InteractionMode::Credentials).await;
        for expected in [10, 15] {
            assert!(
                matches!(f.backend.advance_auth_interaction(StorageInteractionCommand::new(StorageInteractionAuthorization::Redemption(f.access.clone()), StorageInteractionAction::Poll)).await.unwrap(), StorageInteractionOutcome::Rejected(InteractionTransitionError::SlowDown{retry_after_seconds}) if retry_after_seconds == expected)
            );
        }
        f.cleanup().await;
    }
}

#[actix_web::test]
async fn deleting_the_authenticated_identity_prevents_completion() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend, InteractionMode::Credentials).await;
        f.authenticate(None).await;
        let completion = f.completion(credentials());
        let backend = f.backend.clone();
        f.cleanup().await;
        assert!(backend.complete_auth_interaction(completion).await.is_err());
    }
}

#[actix_web::test]
async fn cancellation_is_terminal_and_does_not_issue() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend, InteractionMode::Credentials).await;
        f.authenticate(None).await;
        let StorageInteractionOutcome::View(view) = f
            .backend
            .advance_auth_interaction(StorageInteractionCommand::new(
                StorageInteractionAuthorization::Redemption(f.access.clone()),
                StorageInteractionAction::Cancel,
            ))
            .await
            .unwrap()
        else {
            panic!("cancellation failed")
        };
        assert_eq!(view.state(), InteractionState::Cancelled);
        assert!(matches!(
            f.backend
                .complete_auth_interaction(f.completion(credentials()))
                .await
                .unwrap(),
            StorageInteractionCompletion::Rejected(InteractionTransitionError::Cancelled)
        ));
        f.cleanup().await;
    }
}

#[actix_web::test]
async fn duplicate_identifiers_cannot_replace_an_existing_interaction() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend, InteractionMode::Credentials).await;
        let now = Utc::now();
        let replacement = StorageInteractionCreate::builder(
            InteractionLifecycle::try_new(
                f.binding.clone(),
                InteractionMode::Credentials,
                now,
                now + ChronoDuration::minutes(5),
            )
            .unwrap(),
            digest(),
        )
        .try_build()
        .unwrap();
        assert_eq!(
            f.backend
                .create_auth_interaction(replacement)
                .await
                .unwrap_err()
                .kind(),
            StorageErrorKind::Conflict
        );
        f.authenticate(None).await;
        assert!(matches!(
            f.backend
                .complete_auth_interaction(f.completion(credentials()))
                .await
                .unwrap(),
            StorageInteractionCompletion::Session(_)
        ));
        f.cleanup().await;
    }
}

#[actix_web::test]
async fn password_changes_fence_previously_authenticated_interactions() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend, InteractionMode::Credentials).await;
        f.authenticate(None).await;
        f.backend
            .set_user_password(StorageUserPasswordUpdate::new(
                f.user.id,
                "changed-password-hash",
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        assert!(
            f.backend
                .complete_auth_interaction(f.completion(credentials()))
                .await
                .is_err()
        );
        f.cleanup().await;
    }
}

#[actix_web::test]
async fn session_service_returns_usable_credentials_once() {
    use crate::services::auth_sessions::{InteractionSessionCompletion, complete_interaction};
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend, InteractionMode::Credentials).await;
        f.authenticate(None).await;
        let current = f.inspect().await;
        let context = AppContext::new(
            f.backend.clone(),
            Arc::new(LocalPermissionBackend::new(
                f.backend.clone(),
                "admin".into(),
            )),
        );
        let InteractionSessionCompletion::Issued(response) = complete_interaction(
            &context,
            f.access.clone(),
            &current,
            EventContext::system(),
            hubuum_storage_core::ObservedSource::Unavailable,
        )
        .await
        .unwrap() else {
            panic!("issuance rejected")
        };
        let token = crate::models::Token::try_from(response.access_token).unwrap();
        assert!(
            super::auth_sessions::authenticates(&f.backend, token.storage_digest().unwrap()).await
        );
        assert!(matches!(
            complete_interaction(
                &context,
                f.access.clone(),
                &current,
                EventContext::system(),
                hubuum_storage_core::ObservedSource::Unavailable
            )
            .await
            .unwrap(),
            InteractionSessionCompletion::Rejected(InteractionTransitionError::Consumed)
        ));
        f.cleanup().await;
    }
}

#[rstest]
#[case::valid(false)]
#[case::revoked(true)]
#[actix_web::test]
async fn approval_completion_rechecks_the_initiating_credential(#[case] revoke: bool) {
    use hubuum_domain::InteractionActor;
    use hubuum_storage_core::{
        StorageCredentialClaim, StorageCredentialFingerprint, StorageCredentialOperation,
        StorageInteractionActorLease, StorageTokenRevoke,
    };
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("interaction_approval")).await;
        let origin = backend
            .create_token(StorageTokenCreate::new(
                user.principal_id,
                StorageTokenDigest::legacy_unidentified(prefix("approval_origin")),
                StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let request_digest = digest();
        let claim = StorageCredentialClaim::new(
            user.principal_id,
            origin.id(),
            StorageCredentialOperation::CreateToken,
            StorageCredentialFingerprint::new(digest().as_str().to_owned()).unwrap(),
            StorageCredentialFingerprint::new(request_digest.as_str().to_owned()).unwrap(),
            Utc::now() - ChronoDuration::hours(24),
        )
        .target(Some(user.principal_id));
        let authority = InteractionAuthority::approval(
            InteractionActor::new(user.principal_id, origin.id()),
            request_digest,
        );
        let f = Fixture::for_user(
            backend,
            user,
            InteractionMode::Credentials,
            authority,
            Some(StorageInteractionActorLease::approval(
                Utc::now() + ChronoDuration::minutes(10),
            )),
        )
        .await;
        f.authenticate(None).await;
        if revoke {
            f.backend
                .revoke_token(StorageTokenRevoke::new(
                    origin.id(),
                    f.user.principal_id,
                    EventContext::system(),
                ))
                .await
                .unwrap()
                .into_value();
        }
        let completion = StorageInteractionComplete::new(
            f.access.clone(),
            StorageInteractionIssuance::Approval {
                operation_digest: InteractionDigest::new(claim.request_digest().as_str()).unwrap(),
                claim,
                context: EventContext::user(f.user.principal_id, None, None),
            },
        );
        let outcome = f
            .backend
            .complete_auth_interaction(completion)
            .await
            .unwrap();
        assert!(if revoke {
            matches!(
                outcome,
                StorageInteractionCompletion::Rejected(InteractionTransitionError::Invalid)
            )
        } else {
            matches!(outcome, StorageInteractionCompletion::Approval(_))
        });
        f.cleanup().await;
    }
}
