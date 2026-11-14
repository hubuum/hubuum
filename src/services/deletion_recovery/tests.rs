use std::fs;
use std::future::Future;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use hubuum_storage_core::{
    DeletionControlCapture, DeletionSubject, HistoryPublicationKind, OperationalStateStorage,
};
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
        let initial = self.storage.capture_control_checkpoint().await.unwrap();
        let change = initial
            .deletion()
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
        let capture = ControlCheckpointCapture::new(
            DeletionControlCapture::new(recovered.clone(), initial.deletion().position().clone()),
            initial.task_control().clone(),
        )
        .unwrap();
        CheckpointRepository::open(&self.directory, &self.key)
            .unwrap()
            .publish_control(&capture, false)
            .unwrap();
        recovered
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

// Use only after the owning operation or child has positively finished. A
// concurrent fork may briefly retain its CLOEXEC lock descriptor until exec,
// as covered by repository::tests::after_lock_release. Recovery must still
// acquire both real locks; live-owner rejection and missing proof stay strict.
async fn after_repository_release<T, F, Fut>(mut operation: F) -> Result<T, ApiError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ApiError>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match operation().await {
            Err(ApiError::ServiceUnavailable(message))
                if matches!(
                    message.as_str(),
                    "Control repository is busy; retry without bypassing protection"
                        | "Runtime isolation is busy; stop every server before offline recovery"
                ) && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            result => return result,
        }
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
#[ignore = "subprocess fixture for renderer-isolation recovery"]
async fn renderer_isolation_runtime_child() {
    use hubuum_templates::{TemplateExecution, TemplateLimits};
    let root = std::env::current_dir().unwrap();
    let repository =
        CheckpointRepository::open(&root.join("repository"), &root.join("key")).unwrap();
    let mut guard = after_repository_release(|| async { repository.runtime_isolation(false) })
        .await
        .unwrap();
    if fs::read_to_string(root.join("renderer-isolation-case")).unwrap() == "protected" {
        guard.protect_renderers().unwrap();
        let output = TemplateExecution::new(
            "recovery-probe",
            "{{ 6 * 7 }}",
            TemplateLimits::new(32, 1000),
        )
        .render(&serde_json::json!({}))
        .await
        .unwrap();
        assert_eq!(output.into_parts().0, "42");
        hubuum_templates::shutdown_template_workers().await.unwrap();
    }
    fs::write(
        root.join("isolation-domain.json"),
        serde_json::to_vec(&guard.domain()).unwrap(),
    )
    .unwrap();
    loop {
        std::thread::park();
    }
}

#[rstest]
#[case::http(HistoryPublicationKind::HttpResponse)]
#[case::export(HistoryPublicationKind::Export)]
#[tokio::test]
async fn offline_renderer_retirement_uses_the_original_startup_proof(
    #[case] kind: HistoryPublicationKind,
    #[values(false, true)] protected: bool,
    #[values(false, true)] unregister: bool,
) {
    use crate::storage::StorageExecutionScope;
    use hubuum_storage_core::{ExecutionStorage, RuntimeIsolationDomain};
    use tokio::process::Command;
    let fixture = Fixture::new().await;
    fs::write(
        fixture.root.join("renderer-isolation-case"),
        if protected { "protected" } else { "parent" },
    )
    .unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "services::deletion_recovery::tests::renderer_isolation_runtime_child",
            "--ignored",
        ])
        .current_dir(&fixture.root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let domain: RuntimeIsolationDomain = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(bytes) = fs::read(fixture.root.join("isolation-domain.json"))
                && let Ok(domain) = serde_json::from_slice(&bytes)
            {
                break domain;
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!("startup probe exited: {status}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(domain.excludes_renderers(), protected);
    let registration = fixture
        .storage
        .register_history_runtime(Some(domain))
        .await
        .unwrap();
    let bound = fixture
        .storage
        .clone()
        .with_runtime_owner(registration.owner());
    let application = bound.admit_application_work().await.unwrap();
    let owner = bound
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(application),
            bound.admit_history_publication(kind),
        )
        .await
        .unwrap();
    if unregister {
        fixture
            .storage
            .unregister_runtime(registration)
            .await
            .unwrap();
    }
    let live = recover(&fixture.storage, &fixture.directory, &fixture.key).await;
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    after_repository_release(|| recover(&fixture.storage, &fixture.directory, &fixture.key))
        .await
        .unwrap();
    let released =
        after_repository_release(|| reapply(&fixture.storage, &fixture.directory, &fixture.key))
            .await;
    // No output producer exists in this fixture. Parent-only owners still
    // require their original positive settlement; protected ones were retired.
    let settlement = owner
        .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
        .await
        .unwrap();
    let settled = fixture.storage.settle_history_publication(settlement).await;
    assert!(live.is_err());
    assert_eq!(
        released.is_ok(),
        protected,
        "reapplication error: {:?}",
        released.as_ref().err()
    );
    assert_eq!(
        settled.is_err(),
        protected,
        "settlement error: {:?}",
        settled.as_ref().err()
    );
}

#[rstest]
#[case::registered(false, true)]
#[case::orphaned_known(true, true)]
#[case::orphaned_unknown(true, false)]
#[tokio::test]
async fn offline_recovery_retires_only_publications_with_proven_process_isolation(
    #[case] unregister: bool,
    #[case] known: bool,
) {
    use crate::storage::StorageExecutionScope;
    use hubuum_storage_core::{ExecutionStorage, HistoryPublicationKind};
    let fixture = Fixture::new().await;
    let permit = require_runtime_permit(&fixture.storage, &fixture.options())
        .await
        .unwrap();
    let registration = fixture
        .storage
        .register_history_runtime(if known {
            permit.isolation_domain()
        } else {
            None
        })
        .await
        .unwrap();
    let bound = fixture
        .storage
        .clone()
        .with_runtime_owner(registration.owner());
    let application = bound.admit_application_work().await.unwrap();
    let owner = bound
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(application),
            bound.admit_history_publication(HistoryPublicationKind::Backup),
        )
        .await
        .unwrap();
    if unregister {
        fixture
            .storage
            .unregister_runtime(registration)
            .await
            .unwrap();
    }
    // Registration removal never substitutes for the live OS exclusion lock.
    assert!(
        recover(&fixture.storage, &fixture.directory, &fixture.key)
            .await
            .is_err()
    );
    drop(permit);
    let result = recover(&fixture.storage, &fixture.directory, &fixture.key).await;
    assert_eq!(result.is_ok(), known);
    if known {
        reapply(&fixture.storage, &fixture.directory, &fixture.key)
            .await
            .unwrap();
        assert!(bound.admit_application_work().await.is_err());
    }
    let settlement = owner
        .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
        .await
        .unwrap();
    // No work was launched in this fixture. An isolated owner was already
    // retired; an unknown owner still requires its original positive settlement.
    assert_eq!(
        fixture
            .storage
            .settle_history_publication(settlement)
            .await
            .is_err(),
        known
    );
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
