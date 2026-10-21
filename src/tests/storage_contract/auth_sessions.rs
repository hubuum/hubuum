use super::*;
use chrono::{Duration, Utc};
use hubuum_domain::TokenId;
use hubuum_storage_core::{
    CredentialActivityPolicy, ObservedSource, RefreshCredential, SessionClientId,
    SessionCredentials, SessionPolicy, SessionRevocation, StorageActivityQuery,
    StorageSessionCreate, StorageSessionRotate, StorageSessionRotation, StorageTokenFormat,
    StorageTokenHashAlgorithm, StorageTokenHashKeyId,
};
use rstest::rstest;

fn digest() -> StorageTokenDigest {
    StorageTokenDigest::try_new(
        prefix("session_digest"),
        StorageTokenFormat::Version1,
        StorageTokenHashAlgorithm::HmacSha256V1,
        Some(StorageTokenHashKeyId::try_new("test").unwrap()),
    )
    .unwrap()
}
fn credentials() -> SessionCredentials {
    SessionCredentials::try_new(digest(), digest()).unwrap()
}
fn client() -> SessionClientId {
    SessionClientId::try_new("test-client").unwrap()
}
async fn create(
    backend: &StorageHandle,
    user: &BackendUserFixture,
    credentials: SessionCredentials,
) -> hubuum_storage_core::StorageSessionIssued {
    create_with_policy(
        backend,
        user,
        credentials,
        SessionPolicy::try_new(900, 604800, 2592000, 2592000).unwrap(),
    )
    .await
}
async fn create_with_policy(
    backend: &StorageHandle,
    user: &BackendUserFixture,
    credentials: SessionCredentials,
    policy: SessionPolicy,
) -> hubuum_storage_core::StorageSessionIssued {
    let login = super::login_events::login_request(backend, user, &prefix("login"))
        .await
        .into_parts()
        .login()
        .unwrap()
        .clone();
    backend
        .create_auth_session(StorageSessionCreate::new(
            login,
            client(),
            credentials,
            policy,
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value()
}
fn rotation(current: &SessionCredentials, next: SessionCredentials) -> StorageSessionRotate {
    StorageSessionRotate::new(
        RefreshCredential::new(current.refresh().clone()),
        client(),
        next,
        EventContext::system(),
    )
}
async fn authenticates(backend: &StorageHandle, digest: StorageTokenDigest) -> bool {
    backend
        .authenticate_bearer_token(
            StorageAuthenticationAttempt::try_new(
                StorageAuthenticationCredential::from_digest(digest),
                Utc::now(),
                Utc::now() - Duration::days(1),
            )
            .unwrap(),
        )
        .await
        .is_ok()
}

#[actix_web::test]
async fn rotation_preserves_deadline_and_replay_revokes_every_access_token() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("session_replay")).await;
        let first = credentials();
        let second = credentials();
        let original = create(&backend, &user, first.clone()).await;
        let StorageSessionRotation::Issued(rotated) = backend
            .rotate_auth_session(rotation(&first, second.clone()))
            .await
            .unwrap()
            .into_value()
        else {
            panic!("rotation rejected")
        };
        assert_eq!(
            rotated.absolute_expires_at(),
            original.absolute_expires_at()
        );
        assert!(authenticates(&backend, second.access().clone()).await);
        let replay = backend
            .rotate_auth_session(rotation(&first, credentials()))
            .await
            .unwrap()
            .into_value();
        assert!(matches!(replay, StorageSessionRotation::Replayed));
        assert!(!authenticates(&backend, first.access().clone()).await);
        assert!(!authenticates(&backend, second.access().clone()).await);
        assert!(matches!(
            backend
                .rotate_auth_session(rotation(&second, credentials()))
                .await
                .unwrap()
                .into_value(),
            StorageSessionRotation::Rejected
        ));
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn concurrent_rotation_has_one_winner_and_revokes_the_family() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("session_concurrent")).await;
        let first = credentials();
        create(&backend, &user, first.clone()).await;
        let a = credentials();
        let b = credentials();
        let (left, right) = tokio::join!(
            backend.rotate_auth_session(rotation(&first, a.clone())),
            backend.rotate_auth_session(rotation(&first, b.clone()))
        );
        let outcomes = [left.unwrap().into_value(), right.unwrap().into_value()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|r| matches!(r, StorageSessionRotation::Issued(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|r| matches!(r, StorageSessionRotation::Replayed))
                .count(),
            1
        );
        assert!(
            !authenticates(&backend, a.access().clone()).await
                && !authenticates(&backend, b.access().clone()).await
        );
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn wrong_client_does_not_consume_refresh_credential() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("session_client")).await;
        let first = credentials();
        create(&backend, &user, first.clone()).await;
        let request = StorageSessionRotate::new(
            RefreshCredential::new(first.refresh().clone()),
            SessionClientId::try_new("other").unwrap(),
            credentials(),
            EventContext::system(),
        );
        assert!(matches!(
            backend
                .rotate_auth_session(request)
                .await
                .unwrap()
                .into_value(),
            StorageSessionRotation::Rejected
        ));
        assert!(matches!(
            backend
                .rotate_auth_session(rotation(&first, credentials()))
                .await
                .unwrap()
                .into_value(),
            StorageSessionRotation::Issued(_)
        ));
        delete_backend_user(&backend, user).await;
    }
}

#[rstest]
#[case::password_change(false)]
#[case::anonymization(true)]
#[actix_web::test]
async fn security_event_revokes_refresh_family(#[case] anonymize: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("session_security")).await;
        let first = credentials();
        create(&backend, &user, first.clone()).await;
        if anonymize {
            let _ = backend
                .anonymize_user(StorageUserAnonymize::new(user.id, EventContext::system()))
                .await
                .unwrap();
        } else {
            let _ = backend
                .set_user_password(StorageUserPasswordUpdate::new(
                    user.id,
                    "new-password-hash",
                    EventContext::system(),
                ))
                .await
                .unwrap();
        }
        assert!(matches!(
            backend
                .rotate_auth_session(rotation(&first, credentials()))
                .await
                .unwrap()
                .into_value(),
            StorageSessionRotation::Rejected
        ));
        assert!(!authenticates(&backend, first.access().clone()).await);
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn logout_all_preserves_durable_credentials() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("session_logout")).await;
        let first = credentials();
        create(&backend, &user, first.clone()).await;
        let _ = backend
            .revoke_auth_sessions(
                SessionRevocation::All(user.principal_id),
                EventContext::system(),
            )
            .await
            .unwrap();
        assert!(!authenticates(&backend, first.access().clone()).await);
        assert!(
            authenticates(
                &backend,
                StorageTokenDigest::legacy_unidentified(&user.token_hash)
            )
            .await
        );
        delete_backend_user(&backend, user).await;
    }
}

#[rstest]
#[case::refresh_cannot_authenticate(true)]
#[case::access_cannot_refresh(false)]
#[actix_web::test]
async fn credential_roles_are_disjoint(#[case] refresh_as_bearer: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("session_separation")).await;
        let first = credentials();
        create(&backend, &user, first.clone()).await;
        if refresh_as_bearer {
            assert!(!authenticates(&backend, first.refresh().clone()).await);
        } else {
            assert!(
                backend
                    .refresh_principal(RefreshCredential::new(first.access().clone()))
                    .await
                    .is_err()
            );
        }
        delete_backend_user(&backend, user).await;
    }
}

fn policy(cap: usize) -> CredentialActivityPolicy {
    CredentialActivityPolicy::try_new(true, 30, cap).unwrap()
}
fn source(ip: &str) -> ObservedSource {
    ObservedSource::ConnectionPeer(ip.parse().unwrap())
}
async fn activity(
    backend: &StorageHandle,
    user: &BackendUserFixture,
    id: TokenId,
) -> hubuum_storage_core::StorageCredentialActivity {
    backend
        .credential_activity(
            StorageActivityQuery::try_new(user.principal_id, id, 0, 100, policy(32)).unwrap(),
        )
        .await
        .unwrap()
}

#[actix_web::test]
async fn source_history_survives_rotation_and_normal_activity_resuming() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("session_sources")).await;
        let first = credentials();
        let issued = create(&backend, &user, first.clone()).await;
        backend
            .observe_credential_source(issued.access_id(), source("192.0.2.1"), policy(32))
            .await
            .unwrap();
        backend
            .observe_credential_source(issued.access_id(), source("198.51.100.1"), policy(32))
            .await
            .unwrap();
        let StorageSessionRotation::Issued(next) = backend
            .rotate_auth_session(rotation(&first, credentials()))
            .await
            .unwrap()
            .into_value()
        else {
            panic!("rotation rejected")
        };
        backend
            .observe_credential_source(next.access_id(), source("192.0.2.1"), policy(32))
            .await
            .unwrap();
        let recorded = activity(&backend, &user, next.access_id()).await;
        assert_eq!(recorded.total(), 2);
        assert_eq!(recorded.session_id(), Some(issued.session_id()));
        assert_eq!(
            recorded.sources()[0].first_seen(),
            recorded.sources()[0].last_seen()
        );
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn concurrent_source_recording_is_unique_and_cap_is_visible() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("activity_cap")).await;
        let id = create(&backend, &user, credentials()).await.access_id();
        let (left, right) = tokio::join!(
            backend.observe_credential_source(id, source("192.0.2.1"), policy(1)),
            backend.observe_credential_source(id, source("192.0.2.1"), policy(1))
        );
        left.unwrap();
        right.unwrap();
        backend
            .observe_credential_source(id, source("198.51.100.1"), policy(1))
            .await
            .unwrap();
        let recorded = activity(&backend, &user, id).await;
        assert_eq!(recorded.total(), 1);
        assert!(recorded.overflow_since().is_some());
        assert_eq!(recorded.sources()[0].source(), source("192.0.2.1"));
        delete_backend_user(&backend, user).await;
    }
}

#[rstest]
#[case::idle(5)]
#[case::absolute(1)]
#[actix_web::test]
async fn expired_sessions_cannot_rotate(#[case] absolute: i64) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("session_expiry")).await;
        let first = credentials();
        create_with_policy(
            &backend,
            &user,
            first.clone(),
            SessionPolicy::try_new(1, 1, absolute, 1).unwrap(),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        assert!(matches!(
            backend
                .rotate_auth_session(rotation(&first, credentials()))
                .await
                .unwrap()
                .into_value(),
            StorageSessionRotation::Rejected
        ));
        assert!(!authenticates(&backend, first.access().clone()).await);
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn principal_deletion_removes_refresh_lookup() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("session_delete")).await;
        let first = credentials();
        create(&backend, &user, first.clone()).await;
        delete_backend_user(&backend, user).await;
        assert!(
            backend
                .refresh_principal(RefreshCredential::new(first.refresh().clone()))
                .await
                .is_err()
        );
    }
}

#[actix_web::test]
async fn revocation_retains_source_evidence() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("session_evidence")).await;
        let issued = create(&backend, &user, credentials()).await;
        backend
            .observe_credential_source(issued.access_id(), source("192.0.2.11"), policy(32))
            .await
            .unwrap();
        let _ = backend
            .revoke_auth_sessions(
                SessionRevocation::Current(issued.access_id()),
                EventContext::system(),
            )
            .await
            .unwrap();
        assert_eq!(
            activity(&backend, &user, issued.access_id()).await.total(),
            1
        );
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn creation_provenance_is_immutable_and_does_not_advance_token_revision() {
    use hubuum_storage_core::{CredentialIssuanceMethod, StorageCredentialCreation};
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("creation_evidence")).await;
        let login = super::login_events::login_request(&backend, &user, &prefix("login"))
            .await
            .into_parts()
            .login()
            .unwrap()
            .clone();
        let issued = backend
            .create_auth_session(StorageSessionCreate::new(
                login,
                client(),
                credentials(),
                SessionPolicy::try_new(900, 604800, 2592000, 2592000).unwrap(),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let now = Utc::now();
        let observation = StorageTokenObservation::try_new(now, now - Duration::hours(24)).unwrap();
        let before = backend
            .get_token_metadata(user.principal_id, issued.access_id(), observation)
            .await
            .unwrap();
        let creation = StorageCredentialCreation::new(
            Some(user.principal_id),
            CredentialIssuanceMethod::Login,
            source("192.0.2.20"),
            issued.issued_at(),
        );
        backend
            .record_credential_creation(issued.access_id(), creation)
            .await
            .unwrap();
        backend
            .record_credential_creation(
                issued.access_id(),
                StorageCredentialCreation::new(
                    None,
                    CredentialIssuanceMethod::Creation,
                    ObservedSource::Unavailable,
                    now,
                ),
            )
            .await
            .unwrap();
        let recorded = backend
            .credential_activity(
                StorageActivityQuery::try_new(
                    user.principal_id,
                    issued.access_id(),
                    0,
                    1,
                    policy(32),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recorded.creation().unwrap().source(), source("192.0.2.20"));
        assert_eq!(
            before.revision(),
            backend
                .get_token_metadata(user.principal_id, issued.access_id(), observation)
                .await
                .unwrap()
                .revision()
        );
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn retention_preserves_replay_detection_for_live_families() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("retained_replay")).await;
        let first = credentials();
        create(&backend, &user, first.clone()).await;
        let _ = backend
            .rotate_auth_session(rotation(&first, credentials()))
            .await
            .unwrap();
        backend.purge_auth_sessions(1000).await.unwrap();
        assert!(matches!(
            backend
                .rotate_auth_session(rotation(&first, credentials()))
                .await
                .unwrap()
                .into_value(),
            StorageSessionRotation::Replayed
        ));
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn activity_retention_preserves_recent_sources() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("retained_source")).await;
        let issued = create(&backend, &user, credentials()).await;
        backend
            .observe_credential_source(issued.access_id(), source("192.0.2.11"), policy(32))
            .await
            .unwrap();
        backend
            .purge_credential_sources(policy(32), 1000)
            .await
            .unwrap();
        assert_eq!(
            activity(&backend, &user, issued.access_id()).await.total(),
            1
        );
        delete_backend_user(&backend, user).await;
    }
}
