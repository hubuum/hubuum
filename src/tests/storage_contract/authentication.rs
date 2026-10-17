use chrono::Utc;
use hubuum_storage_core::{StorageExternalSyncFailure, StorageExternalUserSync};
use hubuum_storage_postgres::diesel_async_prelude::*;
use hubuum_storage_postgres::with_transaction;
use rstest::rstest;

use super::login_events::login_request;
use super::*;

#[rstest]
#[case::password_reset(false)]
#[case::anonymization(true)]
#[actix_web::test]
async fn every_backend_rejects_login_evidence_after_credential_change(#[case] anonymize: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("stale_login")).await;
        let login = login_request(&backend, &user, &prefix("stale_login_digest")).await;
        if anonymize {
            backend
                .anonymize_user(StorageUserAnonymize::new(user.id, EventContext::system()))
                .await
                .unwrap()
                .into_value();
        } else {
            backend
                .set_user_password(StorageUserPasswordUpdate::new(
                    user.id,
                    "replacement-hash",
                    EventContext::system(),
                ))
                .await
                .unwrap()
                .into_value();
        }
        let error = backend.create_token(login).await.unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::AuthenticationRequired);
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn concurrent_password_reset_cannot_leave_a_login_token_active() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("concurrent_login")).await;
        let login = login_request(&backend, &user, &prefix("concurrent_login_digest")).await;
        let (issued, reset) = tokio::join!(
            backend.create_token(login),
            backend.set_user_password(StorageUserPasswordUpdate::new(
                user.id,
                "replacement-hash",
                EventContext::system()
            )),
        );
        reset.unwrap().into_value();
        match issued {
            Ok(token) => {
                let observation = StorageTokenObservation::try_new(
                    Utc::now(),
                    Utc::now() - chrono::Duration::days(1),
                )
                .unwrap();
                let retained = backend
                    .get_token_metadata(user.principal_id, token.into_value().id(), observation)
                    .await
                    .unwrap();
                assert!(retained.revoked_at().is_some());
            }
            Err(error) => assert_eq!(error.kind(), StorageErrorKind::AuthenticationRequired),
        }
        delete_backend_user(&backend, user).await;
    }
}

#[rstest]
#[case::stable_subject("entry-uuid")]
#[case::username_subject("alice")]
#[actix_web::test]
async fn every_backend_preserves_subject_binding(#[case] subject: &str) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let scope = prefix("subject_binding");
        let request = |subject: &str, name: &str| {
            StorageExternalUserSync::builder(&scope, "ldap", subject, name).build()
        };
        // Repeated authentication, including simultaneous first logins, may reuse
        // an account only when the configured subject agrees.
        let (first, second) = tokio::join!(
            backend.sync_external_user(request(subject, "alice")),
            backend.sync_external_user(request(subject, "alice"))
        );
        let original = first.unwrap().into_value();
        let id = principal_id(original.id().id());
        assert_eq!(second.unwrap().into_value().id(), original.id());
        let error = backend
            .sync_external_user(request("replacement-subject", "alice"))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), StorageErrorKind::Conflict);
        assert_eq!(
            backend
                .get_external_principal_state(id)
                .await
                .unwrap()
                .unwrap()
                .external_subject(),
            subject
        );
        cleanup_external(&scope).await;
    }
}

#[actix_web::test]
async fn every_backend_allows_rename_only_for_the_same_external_subject() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let scope = prefix("subject_rename");
        let original = backend
            .sync_external_user(
                StorageExternalUserSync::builder(&scope, "ldap", "stable-subject", "old-name")
                    .build(),
            )
            .await
            .unwrap()
            .into_value();
        let renamed = backend
            .sync_external_user(
                StorageExternalUserSync::builder(&scope, "ldap", "stable-subject", "new-name")
                    .build(),
            )
            .await
            .unwrap()
            .into_value();
        assert_eq!(renamed.id(), original.id());
        cleanup_external(&scope).await;
    }
}

#[actix_web::test]
async fn every_backend_preserves_pending_logins_across_unchanged_external_sync() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let scope = prefix("unchanged_external_login");
        let request = || {
            StorageExternalUserSync::builder(&scope, "ldap", "subject", "alice")
                .proper_name(Some("Alice".into()))
                .email(Some("alice@example.invalid".into()))
                .build()
        };
        let external = backend
            .sync_external_user(request())
            .await
            .unwrap()
            .into_value();
        let fixture = BackendUserFixture {
            id: external.id(),
            principal_id: principal_id(external.id().id()),
            raw_token: String::new(),
            token_hash: String::new(),
        };
        let first = login_request(&backend, &fixture, &prefix("first_external_login")).await;
        backend
            .sync_external_user(request())
            .await
            .unwrap()
            .into_value();
        let second = login_request(&backend, &fixture, &prefix("second_external_login")).await;
        let (first, second) =
            tokio::join!(backend.create_token(first), backend.create_token(second));
        first
            .expect("unchanged synchronization must preserve the earlier login")
            .into_value();
        second
            .expect("the overlapping login must also succeed")
            .into_value();
        cleanup_external(&scope).await;
    }
}

#[actix_web::test]
async fn every_backend_invalidates_rejected_identity_even_during_later_outage() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let scope = prefix("rejected_identity");
        let external = backend
            .sync_external_user(
                StorageExternalUserSync::builder(&scope, "ldap", "subject", "alice").build(),
            )
            .await
            .unwrap()
            .into_value();
        let id = principal_id(external.id().id());
        let fixture = BackendUserFixture {
            id: external.id(),
            principal_id: id,
            raw_token: String::new(),
            token_hash: String::new(),
        };
        let login = login_request(&backend, &fixture, &prefix("external_pending")).await;
        let token = backend
            .create_token(login_request(&backend, &fixture, &prefix("external_token")).await)
            .await
            .unwrap()
            .into_value();
        backend
            .record_external_sync_failure(id, StorageExternalSyncFailure::Rejected)
            .await
            .unwrap();
        backend
            .record_external_sync_failure(id, StorageExternalSyncFailure::Unavailable)
            .await
            .unwrap();
        assert!(
            backend
                .get_external_principal_state(id)
                .await
                .unwrap()
                .unwrap()
                .last_sync_success_at()
                .is_none()
        );
        let observation =
            StorageTokenObservation::try_new(Utc::now(), Utc::now() - chrono::Duration::days(1))
                .unwrap();
        let revoked = backend
            .get_token_metadata(id, token.id(), observation)
            .await
            .unwrap();
        assert!(revoked.revoked_at().is_some());
        assert_eq!(
            revoked.revision(),
            token.revision().checked_advance().unwrap()
        );
        assert_eq!(
            backend.create_token(login).await.unwrap_err().kind(),
            StorageErrorKind::AuthenticationRequired
        );
        cleanup_external(&scope).await;
    }
}

async fn cleanup_external(scope: &str) {
    with_transaction(pool().get_ref(), async |connection| {
        diesel::sql_query("DELETE FROM principals WHERE identity_scope_id IN (SELECT id FROM identity_scopes WHERE name = $1)")
            .bind::<diesel::sql_types::Text, _>(scope).execute(connection).await?;
        diesel::sql_query("DELETE FROM identity_scopes WHERE name = $1")
            .bind::<diesel::sql_types::Text, _>(scope).execute(connection).await?;
        Ok::<_, diesel::result::Error>(())
    }).await.unwrap();
}
