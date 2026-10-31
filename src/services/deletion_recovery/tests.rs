use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use hubuum_storage_core::{DeletionControlCapture, DeletionSubject, OperationalStateStorage};
use rstest::rstest;
use uuid::Uuid;

use super::*;
use crate::storage::StorageHandle;

struct Fixture {
    root: PathBuf,
    directory: PathBuf,
    key: PathBuf,
    storage: StorageHandle,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("hubuum-recovery-test-{}", Uuid::new_v4()));
        let directory = root.join("repository");
        fs::create_dir_all(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let key = root.join("key");
        fs::write(&key, [37; 32]).unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        let storage = StorageHandle::memory();
        checkpoint(&storage, &directory, &key, true).await.unwrap();
        Self {
            root,
            directory,
            key,
            storage,
        }
    }

    fn options(&self) -> ControlRecoveryOptions {
        serde_json::from_value(serde_json::json!({
            "directory": self.directory,
            "key": self.key,
        }))
        .unwrap()
    }

    async fn checkpoint_ahead_of_primary(&self) -> DeletionControlState {
        let initial = self.storage.capture_deletion_control().await.unwrap();
        let change = initial
            .state()
            .record_intent(
                Uuid::new_v4(),
                DeletionSubject::Version {
                    identity: serde_json::from_value(serde_json::json!(Uuid::new_v4())).unwrap(),
                },
                vec![Uuid::new_v4()],
            )
            .unwrap();
        let recovered = change.next().clone();
        // Model independent evidence surviving a rollback of the primary.
        let capture = DeletionControlCapture::new(recovered.clone(), initial.position().clone());
        CheckpointRepository::open(&self.directory, &self.key)
            .unwrap()
            .publish(&capture, false)
            .unwrap();
        recovered
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[tokio::test]
async fn recovery_imports_missing_authority_without_releasing_quarantine() {
    let fixture = Fixture::new().await;
    let expected = fixture.checkpoint_ahead_of_primary().await;
    let report = recover(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    assert!(report.authority_covered);
    assert_eq!(
        fixture.storage.read_deletion_control().await.unwrap(),
        expected
    );
    assert!(!report.application_quarantine_released);
    assert!(
        !fixture
            .storage
            .get_maintenance_state()
            .await
            .unwrap()
            .is_normal()
    );
}

#[tokio::test]
async fn recovery_retry_after_committed_but_unacknowledged_import_is_idempotent() {
    let fixture = Fixture::new().await;
    let required = fixture.checkpoint_ahead_of_primary().await;
    let before = fixture.storage.read_deletion_control().await.unwrap();
    // The process committed and died before reporting success.
    fixture
        .storage
        .commit_deletion_control(before.recover_from(&required).unwrap())
        .await
        .unwrap();
    recover(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    assert_eq!(
        fixture.storage.read_deletion_control().await.unwrap(),
        required
    );
}

#[tokio::test]
async fn runtime_rejects_rolled_back_primary_even_when_its_ledger_is_empty() {
    let fixture = Fixture::new().await;
    fixture.checkpoint_ahead_of_primary().await;
    assert!(
        require_runtime_permit(&fixture.storage, &fixture.options())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn runtime_rejects_retained_decisions_after_successful_recovery() {
    let fixture = Fixture::new().await;
    fixture.checkpoint_ahead_of_primary().await;
    recover(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    assert!(
        require_runtime_permit(&fixture.storage, &fixture.options())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn runtime_allows_verified_empty_authority() {
    let fixture = Fixture::new().await;
    assert!(
        require_runtime_permit(&fixture.storage, &fixture.options())
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn missing_latest_evidence_cannot_pass_runtime_startup() {
    let fixture = Fixture::new().await;
    fs::remove_file(fixture.directory.join("HEAD")).unwrap();
    assert!(
        require_runtime_permit(&fixture.storage, &fixture.options())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn recovery_cannot_replace_another_lineage() {
    let fixture = Fixture::new().await;
    let other = StorageHandle::memory();
    let before = other.read_deletion_control().await.unwrap();
    assert!(
        recover(&other, &fixture.directory, &fixture.key)
            .await
            .is_err()
    );
    assert_eq!(other.read_deletion_control().await.unwrap(), before);
}

#[tokio::test]
async fn failed_recovery_read_preserves_primary_authority() {
    let fixture = Fixture::new().await;
    fixture.checkpoint_ahead_of_primary().await;
    let before = fixture.storage.read_deletion_control().await.unwrap();
    fs::write(
        fixture.directory.join("HEAD"),
        b"interrupted/corrupt publication",
    )
    .unwrap();
    assert!(
        recover(&fixture.storage, &fixture.directory, &fixture.key)
            .await
            .is_err()
    );
    assert_eq!(
        fixture.storage.read_deletion_control().await.unwrap(),
        before
    );
}

#[rstest]
#[case::ordinary_api("/api/v1/classes", 503)]
#[case::restore_status("/api/v1/restores/1/status", 503)]
#[case::health("/healthz", 200)]
#[case::metrics("/metrics", 200)]
#[actix_web::test]
async fn recovered_authority_restricts_http_exposure(#[case] path: &str, #[case] status: u16) {
    use crate::permissions::{AppContext, LocalPermissionBackend};
    use actix_web::{App, HttpResponse, middleware::from_fn, test, web};
    use std::sync::Arc;

    let fixture = Fixture::new().await;
    fixture.checkpoint_ahead_of_primary().await;
    recover(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    let permissions = Arc::new(LocalPermissionBackend::new(
        fixture.storage.clone(),
        "admin".into(),
    ));
    let context = AppContext::new(fixture.storage.clone(), permissions);
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(context))
            .wrap(from_fn(crate::middlewares::reject_during_maintenance))
            .default_service(web::to(|| async { HttpResponse::Ok().finish() })),
    )
    .await;
    let response = test::call_service(&app, test::TestRequest::get().uri(path).to_request()).await;
    assert_eq!(response.status().as_u16(), status);
}
