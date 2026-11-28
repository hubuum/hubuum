//! Real backup/export ownership through native storage and joined staging.
use super::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_artifacts::*;
use hubuum_storage_core::{
    DeletionControlStorage, HistoryLifecycleStorage, HistoryPublicationKind, StorageObservation,
    StorageObserver, StorageTaskKind,
};
use hubuum_storage_postgres::PostgresStorage;
use hubuum_task_core::{TaskStopHandle, TaskStopReason};
use rstest::rstest;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

use crate::models::NewHubuumClass;
use crate::storage::{StorageExecutionScope, StorageHandle, TaskQueueStorage};
use crate::tasks::tests::{claim_worker_test_task, create_worker_test_task};
use crate::tests::TestContext;
use crate::traits::CanSave;

#[derive(Default)]
struct Publications {
    admitted: AtomicUsize,
    settled: AtomicUsize,
    completed: AtomicUsize,
}
impl StorageObserver for Publications {
    fn operation_finished(&self, observation: &StorageObservation) {
        if observation.result() == "ok" {
            match observation.operation() {
                "admit_history_publication" => {
                    self.admitted.fetch_add(1, Ordering::SeqCst);
                }
                "settle_history_publication" => {
                    self.settled.fetch_add(1, Ordering::SeqCst);
                }
                "complete_task" => {
                    assert_eq!(self.settled.load(Ordering::SeqCst), 0);
                    self.completed.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }
        }
    }
}

struct OutputDriver {
    store: Arc<dyn TaskArtifactStore>,
    entered: Notify,
    release: Notify,
    failure: Failure,
    stop: TaskStopHandle,
    kind: HistoryPublicationKind,
    storage: StorageHandle,
}
#[async_trait]
impl ArtifactStore for OutputDriver {
    async fn register_destination(&self, destination: &Destination) -> ArtifactResult<()> {
        self.store.register_destination(destination).await
    }
    async fn stage(
        &self,
        identity: &ArtifactIdentity,
        limits: ArtifactLimits,
    ) -> ArtifactResult<()> {
        self.store.stage(identity, limits).await
    }
    async fn append(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
    ) -> ArtifactResult<()> {
        let scope = self.storage.current_execution_scope();
        let owner = scope.history_read_override().expect("original task owner");
        assert_eq!(owner.kind(), self.kind);
        owner.validate_scope(&scope).unwrap();
        if ordinal == 0 {
            self.entered.notify_one();
            self.release.notified().await;
        }
        match self.failure {
            Failure::Store => return Err(ArtifactError::Backend("injected output failure".into())),
            Failure::Stop(reason) => self.stop.request_stop(reason),
            Failure::None => {}
        }
        self.store.append(identity, ordinal, bytes).await
    }
    async fn seal(&self, manifest: &ArtifactManifest) -> ArtifactResult<()> {
        self.store.seal(manifest).await
    }
    async fn manifest(&self, identity: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        self.store.manifest(identity).await
    }
    async fn published_manifest(&self, reference: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        self.store.published_manifest(reference).await
    }
    async fn block(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
    ) -> ArtifactResult<Option<Vec<u8>>> {
        self.store.block(identity, ordinal).await
    }
    async fn publish(&self, proof: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        self.store.publish(proof).await
    }
    async fn abort(&self, identity: &ArtifactIdentity) -> ArtifactResult<()> {
        self.store.abort(identity).await
    }
    async fn cleanup(&self) -> ArtifactResult<u32> {
        self.store.cleanup().await
    }
}
#[async_trait]
impl TaskArtifactStore for OutputDriver {
    async fn abort_task(&self, cleanup: &TaskArtifactCleanup) -> ArtifactResult<()> {
        self.store.abort_task(cleanup).await
    }

    async fn bind_task(
        &self,
        identity: &ArtifactIdentity,
        lease: &hubuum_storage_core::StorageTaskLease,
        reservation: ArtifactReservation,
        expires: DateTime<Utc>,
    ) -> ArtifactResult<()> {
        self.store
            .bind_task(identity, lease, reservation, expires)
            .await
    }
}

#[derive(Clone, Copy)]
enum Failure {
    None,
    Store,
    Stop(TaskStopReason),
}

#[rstest]
#[case::export(false, Failure::None)]
#[case::backup(true, Failure::None)]
#[case::failed_export(false, Failure::Store)]
#[case::failed_backup(true, Failure::Store)]
#[case::cancelled_export(false, Failure::Stop(TaskStopReason::Cancelled))]
#[case::cancelled_backup(true, Failure::Stop(TaskStopReason::Cancelled))]
#[case::expired_export(false, Failure::Stop(TaskStopReason::DeadlineExceeded))]
#[case::expired_backup(true, Failure::Stop(TaskStopReason::DeadlineExceeded))]
#[tokio::test]
async fn native_output_owns_its_publication_until_driver_completion(
    #[case] backup: bool,
    #[case] failure: Failure,
) {
    let test = TestContext::new().await;
    let collection = test.collection_fixture("owned_output").await;
    let class = NewHubuumClass {
        name: test.scoped_name("owned-output-class"),
        collection_id: collection.collection_id(),
        description: String::new(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&test.pool)
    .await
    .unwrap();
    let payload = if backup {
        serde_json::json!({"include_history":false})
    } else {
        serde_json::json!({"export":{"scope":{"kind":"objects_in_class","class_id":class.id}},"template_id":null})
    };
    let task = create_worker_test_task(
        &test,
        if backup {
            StorageTaskKind::Backup
        } else {
            StorageTaskKind::Export
        },
        payload,
        1,
        "owned-output",
    )
    .await;
    let task = claim_worker_test_task(&test, task.id).await;
    let observations = Arc::new(Publications::default());
    let unbound = StorageHandle::from_postgres_backend_with_storage_observer(
        PostgresStorage::unobserved(test.pool.get_ref().clone()),
        observations.clone(),
    );
    let registration = unbound.register_history_runtime(None).await.unwrap();
    let storage = unbound.clone().with_runtime_owner(registration.owner());
    let (execution, stop) =
        hubuum_task_core::TaskExecutionContext::new(Duration::from_secs(60)).unwrap();
    let driver = Arc::new(OutputDriver {
        store: storage.artifact_store(),
        entered: Notify::new(),
        release: Notify::new(),
        failure,
        stop,
        kind: if backup {
            HistoryPublicationKind::Backup
        } else {
            HistoryPublicationKind::Export
        },
        storage: storage.clone(),
    });
    let storage = storage.with_artifact_test_store(driver.clone());
    let permissions = Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into()));
    let context = AppContext::new(storage.clone(), permissions);
    let settings = configured_backup_settings();
    let work = storage.run_in_scope(
        StorageExecutionScope::default().with_task_execution(Some(execution)),
        process_claimed_task(&context, &task, &settings),
    );
    let observed = async {
        driver.entered.notified().await;
        assert_eq!(observations.admitted.load(Ordering::SeqCst), 1);
        assert_eq!(observations.settled.load(Ordering::SeqCst), 0);
        driver.release.notify_one();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(work, observed)
    })
    .await
    .unwrap();
    let settled = observations.settled.load(Ordering::SeqCst);
    let persisted = unbound
        .get_task_access(task.lease().task_id())
        .await
        .unwrap()
        .into_parts()
        .0;
    unbound.unregister_runtime(registration).await.unwrap();
    hubuum_storage_postgres::test_support::delete_task(test.pool.get_ref(), task.lease().task_id())
        .await
        .unwrap();
    while unbound.artifact_store().cleanup().await.unwrap() != 0 {}
    collection.cleanup().await.unwrap();
    assert_eq!(
        settled, 1,
        "the joined producer and confirmed driver must retire exactly once"
    );
    if !matches!(failure, Failure::None) {
        assert!(result.is_err());
    } else {
        assert_eq!(observations.completed.load(Ordering::SeqCst), 1);
        assert_eq!(result.unwrap(), TaskStatus::Succeeded);
        assert_eq!(
            persisted.status(),
            hubuum_storage_core::StorageTaskStatus::Succeeded
        );
    }
}

#[actix_web::test]
async fn online_worker_reconciles_failure_through_bounded_owned_phases() {
    use crate::tests::asserts::expect_legacy_task;
    use hubuum_domain::PrincipalId;
    use hubuum_events_core::EventContext;
    use hubuum_storage_core::{
        StorageTaskCreateRequest, StorageTaskStatus, StorageUserCreate, UserStorage,
    };
    let observations = Arc::new(Publications::default());
    let unbound = StorageHandle::from_registered_backend_with_observer(
        hubuum_storage_memory::MemoryStorage::new(),
        observations.clone(),
    );
    let user = unbound
        .create_user(StorageUserCreate::new(
            None,
            "owned-worker",
            "test-hash",
            None,
            None,
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value()
        .into_parts()
        .id();
    let task = expect_legacy_task(
        unbound
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Backup,
                    PrincipalId::new(user.id()).unwrap(),
                    serde_json::json!({"invalid":true}),
                    1,
                )
                .try_build()
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    let registration = unbound.register_history_runtime(None).await.unwrap();
    let storage = unbound.clone().with_runtime_owner(registration.owner());
    let context = AppContext::new(
        storage.clone(),
        Arc::new(LocalPermissionBackend::new(storage, "admin".into())),
    );
    // The isolated backend contains only this task. Its unprivileged principal
    // fails before staging, exercising the real claim/control/failure loop.
    assert!(
        process_one_task_with_settings(
            &context,
            TaskWorkerId::generate(),
            None,
            &configured_backup_settings()
        )
        .await
        .unwrap()
    );
    let persisted = unbound
        .get_task_access(task.id())
        .await
        .unwrap()
        .into_parts()
        .0;
    unbound.unregister_runtime(registration).await.unwrap();
    assert_eq!(persisted.status(), StorageTaskStatus::Failed);
    assert!(observations.admitted.load(Ordering::SeqCst) >= 4);
    assert_eq!(
        observations.admitted.load(Ordering::SeqCst),
        observations.settled.load(Ordering::SeqCst)
    );
}
