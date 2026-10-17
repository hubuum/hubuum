use chrono::Utc;
use hubuum_domain::PrincipalKind;
use hubuum_events_core::{EventSubject, EventSubscriptionFilter};
use hubuum_storage_core::StorageLogin;
use rstest::rstest;

use super::*;

pub(super) async fn login_request(
    backend: &StorageHandle,
    user: &BackendUserFixture,
    digest: &str,
) -> StorageTokenCreate {
    StorageTokenCreate::for_login(
        StorageLogin::try_new(
            EventSubject::try_new(
                user.principal_id,
                "snapshot-name".into(),
                PrincipalKind::Human,
                "local".into(),
                "local".into(),
                true,
            )
            .unwrap(),
            backend.get_user(user.id).await.unwrap(),
        )
        .unwrap(),
        StorageTokenDigest::legacy_unidentified(digest),
        StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
        &EventContext::system(),
    )
}

async fn login_events(
    backend: &StorageHandle,
    user: &BackendUserFixture,
) -> Vec<StorageAuditEvent> {
    backend
        .list_audit_events(StorageAuditEventListQuery::new(
            vec![],
            true,
            StorageAuditEventFilters::new()
                .entity_type(Some(EntityType::Authentication))
                .entity_id(Some(EventEntityId::new(user.id.id()).unwrap())),
            QueryOptions::empty(),
        ))
        .await
        .unwrap()
        .into_parts()
        .0
}

#[actix_web::test]
async fn every_backend_commits_login_snapshot_with_token_and_lifecycle_receipts() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("login_snapshot")).await;
        let result = backend
            .create_token(login_request(&backend, &user, &prefix("login_digest")).await)
            .await
            .unwrap();
        assert_eq!(result.audits().unwrap().len(), 2);
        let events = login_events(&backend, &user).await;
        assert_eq!(events.len(), 1);
        let event = events[0].as_parts().0;
        assert_eq!(event.actor_user_id(), Some(user.principal_id));
        assert_eq!(event.collection_id(), None);
        assert_eq!(event.subject().unwrap().name(), "snapshot-name");
        assert!(
            EventSubscriptionFilter {
                subject_principal_ids: vec![user.principal_id],
                subject_is_admin: Some(true),
                ..Default::default()
            }
            .matches(event)
        );
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn every_backend_rejects_failed_login_issuance_without_a_success_event() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("failed_login")).await;
        // The existing fixture token makes this issuance fail its unique digest constraint.
        assert!(
            backend
                .create_token(login_request(&backend, &user, &user.token_hash).await)
                .await
                .is_err()
        );
        assert!(login_events(&backend, &user).await.is_empty());
        delete_backend_user(&backend, user).await;
    }
}

#[rstest]
#[case::explicit_creation(false)]
#[case::renewal(true)]
#[actix_web::test]
async fn ordinary_token_issuance_never_records_a_login(#[case] renew: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("not_a_login")).await;
        let issued = backend
            .create_token(StorageTokenCreate::new(
                user.principal_id,
                StorageTokenDigest::legacy_unidentified(prefix("explicit_digest")),
                StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        if renew {
            backend
                .renew_token(StorageTokenRenew::new(
                    issued.id(),
                    user.principal_id,
                    StorageTokenDigest::legacy_unidentified(prefix("renewed_digest")),
                    None,
                    StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
                    EventContext::system(),
                ))
                .await
                .unwrap()
                .into_value();
        }
        assert!(login_events(&backend, &user).await.is_empty());
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn postgres_rolls_back_login_token_and_both_events_before_commit() {
    let _permit = postgres_permit().await;
    let backend = StorageHandle::from_registered_backend(AdapterPostgresStorage::unobserved(
        pool().get_ref().clone(),
    ));
    let user = create_backend_user(&backend, &prefix("login_rollback")).await;
    let digest = prefix("rolled_back_digest");
    let result = PostgresFaultController::failing(PostgresFaultPoint::TransactionBeforeCommit)
        .run(backend.create_token(login_request(&backend, &user, &digest).await))
        .await;
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Backend);
    assert!(login_events(&backend, &user).await.is_empty());
    let observation =
        StorageTokenObservation::try_new(Utc::now(), Utc::now() - chrono::Duration::days(1))
            .unwrap();
    let tokens = backend
        .list_retained_tokens(StorageTokenListQuery::new(
            user.principal_id,
            QueryOptions::empty(),
            crate::storage::StorageTokenListState::All,
            observation,
        ))
        .await
        .unwrap();
    assert_eq!(
        tokens.into_parts().0.len(),
        1,
        "only the original fixture token survives"
    );
    let events = backend
        .list_audit_events(StorageAuditEventListQuery::new(
            vec![],
            true,
            StorageAuditEventFilters::new()
                .entity_type(Some(EntityType::Token))
                .actor_user_id(Some(user.principal_id)),
            QueryOptions::empty(),
        ))
        .await
        .unwrap();
    assert!(
        events.into_parts().0.is_empty(),
        "token.created rolls back too"
    );
    delete_backend_user(&backend, user).await;
}
