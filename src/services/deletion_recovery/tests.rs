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

enum StatusScenario {
    Protected,
    Pending,
    RolledBack,
    Unavailable,
    WrongLineage,
}

#[rstest]
#[case::protected(StatusScenario::Protected, "protected", "none")]
#[case::pending(StatusScenario::Pending, "pending", "publish_checkpoint")]
#[case::rollback(StatusScenario::RolledBack, "blocked", "recover_control")]
#[case::unavailable(StatusScenario::Unavailable, "blocked", "restore_independent_evidence")]
#[case::wrong_lineage(StatusScenario::WrongLineage, "blocked", "resolve_control_lineage")]
#[tokio::test]
async fn deletion_status_reports_bounded_recovery_guidance(
    #[case] scenario: StatusScenario,
    #[case] protection: &str,
    #[case] next_action: &str,
) {
    let fixture = Fixture::new().await;
    let storage = match scenario {
        StatusScenario::Protected => fixture.storage.clone(),
        StatusScenario::Pending => {
            let state = fixture.storage.read_deletion_control().await.unwrap();
            let change = state
                .record_intent(
                    Uuid::new_v4(),
                    DeletionSubject::Version {
                        identity: serde_json::from_value(serde_json::json!(Uuid::new_v4()))
                            .unwrap(),
                    },
                    vec![Uuid::new_v4()],
                )
                .unwrap();
            fixture
                .storage
                .commit_deletion_control(change)
                .await
                .unwrap();
            fixture.storage.clone()
        }
        StatusScenario::RolledBack => {
            fixture.checkpoint_ahead_of_primary().await;
            fixture.storage.clone()
        }
        StatusScenario::Unavailable => {
            fs::remove_file(fixture.directory.join("HEAD")).unwrap();
            fixture.storage.clone()
        }
        StatusScenario::WrongLineage => StorageHandle::memory(),
    };
    let report = status(&storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    let value = serde_json::to_value(report).unwrap();
    assert_eq!(
        (&value["recovery_protection"], &value["next_action"]),
        (
            &serde_json::json!(protection),
            &serde_json::json!(next_action)
        ),
    );
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

#[tokio::test]
async fn offline_recovery_requires_process_exit_and_revokes_old_runtime_access() {
    let fixture = Fixture::new().await;
    let permit = require_runtime_permit(&fixture.storage, &fixture.options())
        .await
        .unwrap();
    let registration = fixture
        .storage
        .register_runtime_isolated(permit.isolation_domain())
        .await
        .unwrap();
    let bound = fixture
        .storage
        .clone()
        .with_runtime_owner(registration.owner());
    assert!(bound.admit_application_work().await.is_ok());
    assert!(
        recover(&fixture.storage, &fixture.directory, &fixture.key)
            .await
            .is_err()
    );
    // Model OS release after process death; protected registration survives.
    drop(permit);
    recover(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    reapply(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    assert!(bound.admit_application_work().await.is_err());
}

#[tokio::test]
async fn unknown_recovered_copy_cannot_be_released_by_reapplication() {
    let fixture = Fixture::new().await;
    fixture.checkpoint_ahead_of_primary().await;
    recover(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    assert!(
        reapply(&fixture.storage, &fixture.directory, &fixture.key)
            .await
            .is_err()
    );
    assert!(
        require_runtime_permit(&fixture.storage, &fixture.options())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn abandoned_runtime_lifetime_keeps_process_isolation_until_exit() {
    let fixture = Fixture::new().await;
    let permit = require_runtime_permit(&fixture.storage, &fixture.options())
        .await
        .unwrap();
    drop(permit.into_runtime_lifetime());
    assert!(
        runtime_isolation(&fixture.directory, &fixture.key, true)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn explicit_settled_runtime_lifetime_releases_process_isolation() {
    let fixture = Fixture::new().await;
    let permit = require_runtime_permit(&fixture.storage, &fixture.options())
        .await
        .unwrap();
    permit.into_runtime_lifetime().release();
    assert!(
        runtime_isolation(&fixture.directory, &fixture.key, true)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn restore_executor_requires_latest_independent_authority_before_replay() {
    let fixture = Fixture::new().await;
    fixture.checkpoint_ahead_of_primary().await;
    assert!(
        require_control_runtime_lifetime(&fixture.storage, &fixture.options())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn restore_executor_can_hold_isolation_while_application_is_quarantined() {
    let fixture = Fixture::new().await;
    fixture.checkpoint_ahead_of_primary().await;
    recover(&fixture.storage, &fixture.directory, &fixture.key)
        .await
        .unwrap();
    let lifetime = require_control_runtime_lifetime(&fixture.storage, &fixture.options())
        .await
        .unwrap();
    assert!(
        require_runtime_permit(&fixture.storage, &fixture.options())
            .await
            .is_err()
    );
    assert!(
        recover(&fixture.storage, &fixture.directory, &fixture.key)
            .await
            .is_err()
    );
    lifetime.release();
    assert!(
        recover(&fixture.storage, &fixture.directory, &fixture.key)
            .await
            .is_ok()
    );
}
