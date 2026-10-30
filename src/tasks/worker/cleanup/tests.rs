use std::sync::Arc;

use async_trait::async_trait;
use hubuum_storage_core::StorageTaskLease;
use hubuum_storage_core::artifacts::{
    ArtifactIdentity, ArtifactLimits, ArtifactManifest, ArtifactReference, ArtifactReservation,
    ArtifactResult, ArtifactStore, Destination, TaskArtifactStore, VerifiedArtifact,
};

use crate::storage::StorageHandle;

use super::*;

struct PausedCleanup {
    store: Arc<dyn TaskArtifactStore>,
    entered: Notify,
    release: Notify,
}

#[async_trait]
impl ArtifactStore for PausedCleanup {
    async fn register_destination(&self, value: &Destination) -> ArtifactResult<()> {
        self.store.register_destination(value).await
    }
    async fn stage(&self, id: &ArtifactIdentity, limits: ArtifactLimits) -> ArtifactResult<()> {
        self.store.stage(id, limits).await
    }
    async fn append(
        &self,
        id: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
    ) -> ArtifactResult<()> {
        self.store.append(id, ordinal, bytes).await
    }
    async fn seal(&self, value: &ArtifactManifest) -> ArtifactResult<()> {
        self.store.seal(value).await
    }
    async fn manifest(&self, id: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        self.store.manifest(id).await
    }
    async fn published_manifest(&self, value: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        self.store.published_manifest(value).await
    }
    async fn block(&self, id: &ArtifactIdentity, ordinal: u32) -> ArtifactResult<Option<Vec<u8>>> {
        self.store.block(id, ordinal).await
    }
    async fn publish(&self, value: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        self.store.publish(value).await
    }
    async fn abort(&self, id: &ArtifactIdentity) -> ArtifactResult<()> {
        self.store.abort(id).await
    }
    async fn cleanup(&self) -> ArtifactResult<u32> {
        self.entered.notify_one();
        self.release.notified().await;
        self.store.cleanup().await
    }
}

#[async_trait]
impl TaskArtifactStore for PausedCleanup {
    async fn bind_task(
        &self,
        id: &ArtifactIdentity,
        lease: &StorageTaskLease,
        reservation: ArtifactReservation,
        expires: chrono::DateTime<Utc>,
    ) -> ArtifactResult<()> {
        self.store.bind_task(id, lease, reservation, expires).await
    }
}

#[actix_web::test]
async fn paused_maintenance_does_not_block_the_task_claim_loop() {
    let baseline = StorageHandle::memory();
    let paused = Arc::new(PausedCleanup {
        store: baseline.artifact_store(),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let storage = baseline.with_artifact_test_store(paused.clone());
    let context = AppContext::new(
        storage.clone(),
        Arc::new(LocalPermissionBackend::new(storage, "admin".to_owned())),
    );
    let background = context.clone();
    let maintenance = actix_rt::spawn(async move {
        let tasks = Mutex::new(CleanupSchedule::default());
        let artifacts = Mutex::new(CleanupSchedule::default());
        iteration(&background, &tasks, &artifacts).await
    });
    tokio::time::timeout(Duration::from_secs(2), paused.entered.notified())
        .await
        .expect("maintenance must reach the paused artifact cleanup");

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        process_one_task_with_settings(&context, None, &configured_backup_settings()),
    )
    .await;
    paused.release.notify_waiters();
    maintenance.await.unwrap().unwrap();

    assert!(
        !result
            .expect("a task claimant must not wait for maintenance")
            .unwrap()
    );
}

#[test]
fn maintenance_future_fits_the_worker_stack_budget() {
    let storage = StorageHandle::memory();
    let context = AppContext::new(
        storage.clone(),
        Arc::new(LocalPermissionBackend::new(storage, "admin".to_owned())),
    );
    let tasks = Mutex::new(CleanupSchedule::default());
    let artifacts = Mutex::new(CleanupSchedule::default());
    let work = iteration(&context, &tasks, &artifacts);
    assert!(std::mem::size_of_val(&work) < 16 * 1024);
}
