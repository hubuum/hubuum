use super::*;
use chrono::{Duration as ChronoDuration, Utc};
use hubuum_storage_core::{
    StorageCredentialApprovalCreate, StorageCredentialClaim, StorageCredentialFingerprint,
    StorageCredentialOperation, StorageTokenRevoke,
};
use rstest::rstest;

struct Fixture {
    backend: StorageHandle,
    user: BackendUserFixture,
    claim: StorageCredentialClaim,
    context: EventContext,
}
impl Fixture {
    async fn new(backend: StorageHandle) -> Self {
        let user = create_backend_user(&backend, &prefix("approval_actor")).await;
        let context = EventContext::user(user.principal_id, None, None);
        let origin = backend
            .create_token(StorageTokenCreate::new(
                user.principal_id,
                StorageTokenDigest::legacy_unidentified(
                    crate::models::Token::storage_hash_from_raw(&prefix("approval_origin")),
                ),
                StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
                context.clone(),
            ))
            .await
            .unwrap()
            .into_value();
        let secret = StorageCredentialFingerprint::new(
            crate::models::Token::storage_hash_from_raw(&prefix("approval_secret")),
        )
        .unwrap();
        let claim = StorageCredentialClaim::new(
            user.principal_id,
            origin.id(),
            StorageCredentialOperation::CreateToken,
            secret.clone(),
            secret,
            Utc::now() - ChronoDuration::hours(24),
        )
        .target(Some(user.principal_id));
        Self {
            backend,
            user,
            claim,
            context,
        }
    }
    async fn approve(&self, ago: i64) -> i32 {
        self.backend
            .create_credential_approval(StorageCredentialApprovalCreate::new(
                self.claim.clone(),
                Utc::now() - ChronoDuration::seconds(ago),
                self.context.clone(),
            ))
            .await
            .unwrap()
            .into_value()
            .id()
    }
    fn request(&self) -> StorageTokenCreate {
        StorageTokenCreate::new(
            self.user.principal_id,
            StorageTokenDigest::legacy_unidentified(crate::models::Token::storage_hash_from_raw(
                &prefix("approved_token"),
            )),
            StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
            self.context.clone(),
        )
        .with_credential_claim(self.claim.clone())
    }
    async fn cleanup(self) {
        delete_backend_user(&self.backend, self.user).await;
    }
}

#[actix_web::test]
async fn approvals_allow_only_one_concurrent_consumer_and_retain_evidence() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend).await;
        let id = f.approve(0).await;
        let (first, second) = tokio::join!(
            f.backend.create_token(f.request()),
            f.backend.create_token(f.request())
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        let failure = first.err().or(second.err()).unwrap();
        assert_eq!(failure.kind(), StorageErrorKind::ReauthenticationRequired);
        assert!(
            f.backend
                .get_credential_approval(id)
                .await
                .unwrap()
                .consumed_at()
                .is_some()
        );
        let backend = f.backend.clone();
        f.cleanup().await;
        assert!(
            backend
                .get_credential_approval(id)
                .await
                .unwrap()
                .consumed_at()
                .is_some(),
            "deleting an actor must retain evidence"
        );
    }
}

#[actix_web::test]
async fn failed_mutation_rolls_back_approval_consumption() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend).await;
        let id = f.approve(0).await;
        assert!(
            f.backend
                .create_token(
                    f.request()
                        .expires_at(Some(Utc::now() - ChronoDuration::hours(1)))
                )
                .await
                .is_err()
        );
        assert!(
            f.backend
                .get_credential_approval(id)
                .await
                .unwrap()
                .consumed_at()
                .is_none()
        );
        f.backend
            .create_token(f.request())
            .await
            .unwrap()
            .into_value();
        f.cleanup().await;
    }
}

#[rstest]
#[case(false)]
#[case(true)]
#[actix_web::test]
async fn approvals_reject_expired_or_revoked_origin(#[case] revoke: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend).await;
        let id = f.approve(if revoke { 0 } else { 119 }).await;
        if revoke {
            f.backend
                .revoke_token(StorageTokenRevoke::new(
                    f.claim.token_id(),
                    f.user.principal_id,
                    f.context.clone(),
                ))
                .await
                .unwrap()
                .into_value();
        } else {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let error = f.backend.create_token(f.request()).await.unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::ReauthenticationRequired);
        assert!(
            f.backend
                .get_credential_approval(id)
                .await
                .unwrap()
                .consumed_at()
                .is_none()
        );
        f.cleanup().await;
    }
}

#[actix_web::test]
async fn approval_request_binding_is_rechecked_by_each_adapter() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let f = Fixture::new(backend).await;
        f.approve(0).await;
        let changed = StorageCredentialClaim::new(
            f.user.principal_id,
            f.claim.token_id(),
            StorageCredentialOperation::CreateToken,
            f.claim.secret_digest().clone(),
            StorageCredentialFingerprint::new("a".repeat(64)).unwrap(),
            f.claim.legacy_valid_after(),
        )
        .target(Some(f.user.principal_id));
        assert_eq!(
            f.backend
                .create_token(f.request().with_credential_claim(changed))
                .await
                .unwrap_err()
                .kind(),
            StorageErrorKind::ReauthenticationRequired
        );
        f.cleanup().await;
    }
}

#[actix_web::test]
async fn credential_import_consumption_commits_with_queue_admission_and_allows_idempotent_read() {
    use hubuum_storage_core::StorageCredentialUse;
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let mut f = Fixture::new(environment.storage()).await;
        f.claim = StorageCredentialClaim::new(
            f.user.principal_id,
            f.claim.token_id(),
            StorageCredentialOperation::ImportCredentials,
            f.claim.secret_digest().clone(),
            f.claim.request_digest().clone(),
            f.claim.legacy_valid_after(),
        );
        let id = f.approve(0).await;
        let key = IdempotencyKey::new(prefix("approved_import")).unwrap();
        let request = StorageTaskCreateRequest::builder(
            StorageTaskKind::Import,
            f.user.principal_id,
            serde_json::json!({"version":2}),
            0,
        )
        .idempotency_key(Some(key))
        .request_hash(Some("b".repeat(64)))
        .try_build(10)
        .unwrap()
        .with_approval(Some(StorageCredentialUse::new(
            f.claim.clone(),
            f.context.clone(),
        )));
        let (first, second) = tokio::join!(
            f.backend.create_task(request.clone()),
            f.backend.create_task(request)
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first.id(), second.id());
        assert!(
            f.backend
                .get_credential_approval(id)
                .await
                .unwrap()
                .consumed_at()
                .is_some()
        );
        if let BackendTestEnvironment::Postgres { pool } = environment {
            hubuum_storage_postgres::test_support::delete_task(&pool, first.id())
                .await
                .unwrap();
        }
        f.cleanup().await;
    }
}

#[actix_web::test]
async fn failed_restore_confirmation_rolls_back_consumed_approval_and_maintenance() {
    use hubuum_storage_core::{StorageCredentialUse, StorageRestoreConfirmation};
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let BackendTestEnvironment::Postgres { pool } = environment else {
            continue;
        };
        let mut f = Fixture::new(StorageHandle::postgres(pool.clone())).await;
        let stage = f
            .backend
            .stage_restore(
                StorageRestoreStageCreate::try_new(
                    StorageRestoreInitiator::try_new(
                        Some(f.user.principal_id),
                        "local",
                        prefix("approved_restore"),
                    )
                    .unwrap(),
                    b"{}".to_vec(),
                    StorageRestoreArtifactSummary::try_new(
                        2,
                        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
                    )
                    .unwrap(),
                    "b".repeat(64),
                    serde_json::json!({}),
                    Utc::now() + ChronoDuration::hours(1),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let job = stage.summary().id();
        f.claim = StorageCredentialClaim::new(
            f.user.principal_id,
            f.claim.token_id(),
            StorageCredentialOperation::ConfirmRestore,
            f.claim.secret_digest().clone(),
            f.claim.request_digest().clone(),
            f.claim.legacy_valid_after(),
        )
        .restore_job(job);
        let id = f.approve(0).await;
        let request = StorageRestoreConfirmation::from(job).with_approval(
            StorageCredentialUse::new(f.claim.clone(), f.context.clone()),
        );
        assert!(
            PostgresFaultController::failing(PostgresFaultPoint::RestoreAfterDrainTransition)
                .run(f.backend.start_restore_draining(request))
                .await
                .is_err()
        );
        assert!(
            f.backend
                .get_credential_approval(id)
                .await
                .unwrap()
                .consumed_at()
                .is_none()
        );
        assert!(
            f.backend
                .get_restore_coordinator_snapshot()
                .await
                .unwrap()
                .maintenance_state()
                .is_normal()
        );
        hubuum_storage_postgres::test_support::delete_restore_job(&pool, job)
            .await
            .unwrap();
        f.cleanup().await;
    }
}
