use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use hubuum_domain::PrincipalId;
use hubuum_storage_core::{
    StorageTaskClaimRequest, StorageTaskCreateRequest, StorageTaskKind, StorageTaskLeaseDuration,
    TaskWorkerId, TaskWorkerKinds,
};
use hubuum_task_core::IdempotencyKey;
use uuid::Uuid;

use super::*;
use crate::storage::StorageHandle;

struct Fixture {
    root: PathBuf,
    options: ControlRecoveryOptions,
    storage: StorageHandle,
}
impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("hubuum-task-admin-{}", Uuid::new_v4()));
        let directory = root.join("repository");
        fs::create_dir_all(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let key = root.join("key");
        fs::write(&key, [31; 32]).unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        let storage = StorageHandle::memory();
        super::super::checkpoint(&storage, &directory, &key, true)
            .await
            .unwrap();
        let options =
            serde_json::from_value(serde_json::json!({"directory":directory,"key":key})).unwrap();
        Self {
            root,
            options,
            storage,
        }
    }
    async fn run(
        &self,
        action: TaskRetentionAdminAction,
    ) -> Result<TaskRetentionAdminReport, ApiError> {
        administer_task_retention(&self.storage, &self.options, action).await
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[actix_web::test]
async fn activation_requires_current_preview_acknowledgement_before_any_mutation() {
    let fixture = Fixture::new().await;
    let original = fixture.storage.read_task_control().await.unwrap();
    assert!(matches!(
        fixture
            .run(TaskRetentionAdminAction::Enable {
                confirmation: "wrong".into()
            })
            .await,
        Err(ApiError::BadRequest(_))
    ));
    assert_eq!(fixture.storage.read_task_control().await.unwrap(), original);
}

#[actix_web::test]
async fn legacy_activation_is_bounded_and_protects_queued_generations_before_claims() {
    let fixture = Fixture::new().await;
    for index in 0..65 {
        fixture
            .storage
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Import,
                    PrincipalId::new(1).unwrap(),
                    serde_json::json!({"changes":[]}),
                    1,
                )
                .idempotency_key(Some(IdempotencyKey::new(format!("old-{index}")).unwrap()))
                .request_hash(Some("a".repeat(64)))
                .try_build()
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let confirmation = fixture
        .run(TaskRetentionAdminAction::Preview)
        .await
        .unwrap()
        .preview
        .unwrap()
        .confirmation;
    let first = fixture
        .run(TaskRetentionAdminAction::Enable {
            confirmation: confirmation.clone(),
        })
        .await
        .unwrap();
    assert_eq!(first.backfilled, 64);
    assert!(first.more);
    assert!(
        fixture
            .storage
            .claim_next_task(
                StorageTaskClaimRequest::new(
                    TaskWorkerId::generate(),
                    StorageTaskLeaseDuration::from_milliseconds(60000).unwrap()
                )
                .with_kinds(TaskWorkerKinds::new(&[StorageTaskKind::Import]).unwrap())
            )
            .await
            .unwrap()
            .is_none()
    );
    let second = fixture
        .run(TaskRetentionAdminAction::Enable { confirmation })
        .await
        .unwrap();
    assert_eq!(second.backfilled, 1);
    assert!(!second.more);
    let state = fixture.storage.read_task_control().await.unwrap();
    assert!(state.activation().is_protected());
    assert_eq!(state.receipts().len(), 65);
    assert!(
        state
            .receipts()
            .iter()
            .all(|receipt| receipt.details_expires_at().is_none())
    );
    let (directory, key) = fixture.options.paths().unwrap().unwrap();
    let session = session(directory, key).await.unwrap();
    assert_eq!(session.checkpoint().task_control(), Some(&state));
    assert!(
        fixture
            .storage
            .claim_next_task(
                StorageTaskClaimRequest::new(
                    TaskWorkerId::generate(),
                    StorageTaskLeaseDuration::from_milliseconds(60000).unwrap()
                )
                .with_kinds(TaskWorkerKinds::new(&[StorageTaskKind::Import]).unwrap())
            )
            .await
            .unwrap()
            .is_some()
    );
}

#[actix_web::test]
async fn changing_policy_invalidates_an_unused_activation_preview() {
    let fixture = Fixture::new().await;
    let confirmation = fixture
        .run(TaskRetentionAdminAction::Preview)
        .await
        .unwrap()
        .preview
        .unwrap()
        .confirmation;
    fixture
        .run(TaskRetentionAdminAction::SetPolicy {
            deduplication: Duration::from_secs(60),
            details: None,
        })
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .run(TaskRetentionAdminAction::Enable { confirmation })
            .await,
        Err(ApiError::BadRequest(_))
    ));
    assert!(matches!(
        fixture
            .storage
            .read_task_receipt_activation()
            .await
            .unwrap(),
        TaskReceiptActivation::Disabled
    ));
}
