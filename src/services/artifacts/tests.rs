use hubuum_storage_core::artifacts::TaskArtifactCleanup;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use hubuum_artifacts::{
    ArtifactManifest, ArtifactResult, ArtifactStore, ManifestBuilder, VerifiedArtifact,
};
use hubuum_storage_core::StorageTaskLease;
use hubuum_task_core::{TaskExecutionContext, TaskStopReason};
use rstest::rstest;

use super::*;
use crate::storage::StorageHandle;

const PAYLOAD: &[u8] = b"verified download";

#[rstest]
#[case::dropped_work(false)]
#[case::unconfirmed_abort(true)]
#[tokio::test]
async fn unconfirmed_task_work_preserves_native_owner_and_shutdown_obligation(
    #[case] failed_abort: bool,
) {
    use hubuum_storage_core::{
        DeletionControlStorage, PrincipalStorage, StorageUserCreate, UserStorage,
    };
    let unbound = StorageHandle::memory();
    let user = unbound
        .create_user(StorageUserCreate::new(
            None,
            "publication-drop",
            "fixture-hash",
            None,
            None,
            crate::events::EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value()
        .into_parts();
    let principal = hubuum_domain::PrincipalId::new(user.id().id()).unwrap();
    let registration = unbound.register_history_runtime(None).await.unwrap();
    let storage = unbound.clone().with_runtime_owner(registration.owner());
    let (execution, _) = TaskExecutionContext::new(Duration::from_secs(10)).unwrap();
    let mut publication = storage
        .run_in_scope(
            StorageExecutionScope::default().with_task_execution(Some(execution)),
            TaskPublication::admit(&storage, Some(HistoryPublicationKind::Backup)),
        )
        .await
        .unwrap();
    let (abandoned, isolated) = ApplicationWorkCompletion::isolated();
    // The normal guard is transferred to isolated failure accounting so this
    // intentional abandonment cannot poison unrelated restore tests.
    publication.owner.as_mut().unwrap().1.confirm();
    publication.owner.as_mut().unwrap().1 = isolated;
    let scope = publication.scope();
    if failed_abort {
        let store = publication.storage.artifact_store();
        let destination =
            Destination::new(DestinationId::generate(), "unconfirmed-output").unwrap();
        let identity = ArtifactIdentity::new(destination.id());
        let limits = ArtifactLimits::default();
        publication.pending = true;
        let result = storage
            .run_in_scope(scope.clone(), async {
                store.register_destination(&destination).await.unwrap();
                let (proof, ()) = hubuum_artifacts::stage_producer(
                    store.as_ref(),
                    identity.clone(),
                    limits,
                    |sink| {
                        sink.write_all(PAYLOAD)
                            .map_err(|error| ArtifactError::Backend(error.to_string()))
                    },
                )
                .await
                .unwrap();
                // A previously published result models an ambiguous completion.
                // The real adapter refuses to abort visible output.
                store.publish(&proof).await.unwrap();
                let cleanup = TaskArtifactCleanup::new(
                    identity.clone(),
                    StorageTaskLease::new(
                        hubuum_domain::TaskId::new(1).unwrap(),
                        hubuum_storage_core::StorageTaskClaimToken::new(
                            uuid::Uuid::new_v4().to_string(),
                        ),
                    ),
                );
                let output = TaskOutput {
                    store,
                    identity,
                    limits,
                    cleanup,
                };
                output
                    .finish(
                        &mut publication,
                        Err(ApiError::Conflict("primary task failure".into())),
                    )
                    .await
            })
            .await;
        assert!(
            matches!(publication.settle(result).await, Err(ApiError::Conflict(message)) if message == "primary task failure")
        );
    } else {
        drop(publication);
    }
    assert!(abandoned.load(Ordering::Acquire));
    storage
        .run_in_scope(scope, storage.get_principal(principal))
        .await
        .unwrap();
    unbound.unregister_runtime(registration).await.unwrap();
}

#[derive(Clone, Copy)]
enum ReadMode {
    Normal,
    Pending,
    Corrupt,
    Truncated,
    Trailing,
    Panic,
}

struct ReadGuard<'a>(&'a AtomicBool);

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

struct DownloadStore {
    storage: StorageHandle,
    manifest: ArtifactManifest,
    mode: ReadMode,
    entered: AtomicBool,
    dropped: AtomicBool,
    observed_stops: Mutex<Vec<Option<TaskStopReason>>>,
}

impl DownloadStore {
    fn observe_scope(&self) {
        let scope = self.storage.current_execution_scope();
        let reason = scope
            .task_execution_override()
            .and_then(Option::as_ref)
            .and_then(|execution| execution.check().err());
        self.observed_stops.lock().unwrap().push(reason);
    }
}

#[async_trait]
impl ArtifactStore for DownloadStore {
    async fn register_destination(&self, _: &Destination) -> ArtifactResult<()> {
        panic!("download must not write")
    }
    async fn stage(&self, _: &ArtifactIdentity, _: ArtifactLimits) -> ArtifactResult<()> {
        panic!("download must not write")
    }
    async fn append(&self, _: &ArtifactIdentity, _: u32, _: &[u8]) -> ArtifactResult<()> {
        panic!("download must not write")
    }
    async fn seal(&self, _: &ArtifactManifest) -> ArtifactResult<()> {
        panic!("download must not write")
    }
    async fn manifest(&self, _: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        panic!("download requires published evidence")
    }
    async fn published_manifest(&self, reference: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        self.observe_scope();
        assert_eq!(reference.identity(), self.manifest.identity());
        Ok(self.manifest.encode())
    }
    async fn block(&self, _: &ArtifactIdentity, ordinal: u32) -> ArtifactResult<Option<Vec<u8>>> {
        self.observe_scope();
        self.entered.store(true, Ordering::Release);
        let _read = ReadGuard(&self.dropped);
        match self.mode {
            ReadMode::Pending => std::future::pending().await,
            ReadMode::Panic => panic!("injected reader failure"),
            ReadMode::Corrupt => Ok(Some(b"corrupt".to_vec())),
            ReadMode::Truncated => Ok(None),
            ReadMode::Trailing => Ok(Some(PAYLOAD.to_vec())),
            ReadMode::Normal => Ok((ordinal == 0).then(|| PAYLOAD.to_vec())),
        }
    }
    async fn publish(&self, _: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        panic!("download must not write")
    }
    async fn abort(&self, _: &ArtifactIdentity) -> ArtifactResult<()> {
        panic!("download must not write")
    }
    async fn cleanup(&self) -> ArtifactResult<u32> {
        panic!("download must not write")
    }
}

#[async_trait]
impl TaskArtifactStore for DownloadStore {
    async fn abort_task(&self, _cleanup: &TaskArtifactCleanup) -> ArtifactResult<()> {
        panic!("download must not write")
    }

    async fn bind_task(
        &self,
        _: &ArtifactIdentity,
        _: &StorageTaskLease,
        _: ArtifactReservation,
        _: DateTime<Utc>,
    ) -> ArtifactResult<()> {
        panic!("download must not write")
    }
}

fn fixture(mode: ReadMode) -> (StorageHandle, Arc<DownloadStore>, ArtifactReference) {
    let storage = StorageHandle::memory();
    let identity = ArtifactIdentity::new(DestinationId::generate());
    let mut builder = ManifestBuilder::new(identity.clone(), ArtifactLimits::default());
    builder.push(PAYLOAD).unwrap();
    let manifest = builder.finish().unwrap();
    let reference = ArtifactReference::from_catalog(identity, manifest.manifest_digest());
    let store = Arc::new(DownloadStore {
        storage: storage.clone(),
        manifest,
        mode,
        entered: AtomicBool::new(false),
        dropped: AtomicBool::new(false),
        observed_stops: Mutex::new(Vec::new()),
    });
    (
        storage.with_artifact_test_store(store.clone()),
        store,
        reference,
    )
}

#[rstest]
#[case::verified(ReadMode::Normal, true)]
#[case::corrupt(ReadMode::Corrupt, false)]
#[case::truncated(ReadMode::Truncated, false)]
#[case::trailing(ReadMode::Trailing, false)]
#[case::panic(ReadMode::Panic, false)]
#[actix_web::test]
async fn last_block_requires_successful_verification(#[case] mode: ReadMode, #[case] valid: bool) {
    let (storage, _, reference) = fixture(mode);
    let mut body = Box::pin(download(&storage, reference));
    let block = body
        .next()
        .await
        .expect("verification must produce a result");
    if valid {
        assert_eq!(block.unwrap().as_ref(), PAYLOAD);
    } else {
        assert!(
            block.is_err(),
            "failed verification must not publish the final block"
        );
    }
    assert!(body.next().await.is_none());
}

#[actix_web::test]
async fn dropping_download_drops_the_pending_reader() {
    let (storage, store, reference) = fixture(ReadMode::Pending);
    let mut body = Box::pin(download(&storage, reference));
    assert!(body.next().now_or_never().is_none());
    assert!(store.entered.load(Ordering::Acquire));
    drop(body);
    // This is positive synchronous destructor evidence, with no sleep, task
    // abort request or channel closure standing in for reader cancellation.
    assert!(store.dropped.load(Ordering::Acquire));
}

#[actix_web::test]
async fn download_keeps_its_original_scope_after_the_handler_returns() {
    let (storage, store, reference) = fixture(ReadMode::Normal);
    let (original, stop) = TaskExecutionContext::new(Duration::from_secs(5)).unwrap();
    let scope = storage
        .current_execution_scope()
        .with_task_execution(Some(original));
    let mut body = Box::pin(storage.run_in_scope_sync(scope, || download(&storage, reference)));
    stop.request_stop(TaskStopReason::Cancelled);
    let (later, _) = TaskExecutionContext::new(Duration::from_secs(5)).unwrap();
    let later_scope = storage
        .current_execution_scope()
        .with_task_execution(Some(later));
    storage
        .run_in_scope(later_scope, async {
            while let Some(block) = body.next().await {
                block.unwrap();
            }
        })
        .await;
    assert_eq!(
        *store.observed_stops.lock().unwrap(),
        [Some(TaskStopReason::Cancelled); 3],
        "manifest, payload and exact-EOF reads must retain the original scope"
    );
}
