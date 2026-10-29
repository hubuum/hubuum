use super::*;
use async_trait::async_trait;
use hubuum_storage_core::{StorageTaskLease, artifacts::*};

#[async_trait]
impl ArtifactStore for StorageHandle {
    async fn register_destination(&self, destination: &Destination) -> ArtifactResult<()> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Artifact,
            "register_destination",
            async {
                dispatch_backend!(self, |backend| backend
                    .register_destination(destination)
                    .await)
            },
        )
        .await
    }
    async fn stage(
        &self,
        identity: &ArtifactIdentity,
        limits: ArtifactLimits,
    ) -> ArtifactResult<()> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Artifact,
            "stage",
            async { dispatch_backend!(self, |backend| backend.stage(identity, limits).await) },
        )
        .await
    }
    async fn append(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
    ) -> ArtifactResult<()> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Artifact,
            "append",
            async {
                dispatch_backend!(self, |backend| backend
                    .append(identity, ordinal, bytes)
                    .await)
            },
        )
        .await
    }
    async fn seal(&self, manifest: &ArtifactManifest) -> ArtifactResult<()> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Artifact,
            "seal",
            async { dispatch_backend!(self, |backend| backend.seal(manifest).await) },
        )
        .await
    }
    async fn manifest(&self, identity: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Artifact,
            "manifest",
            async { dispatch_backend!(self, |backend| backend.manifest(identity).await) },
        )
        .await
    }
    async fn published_manifest(&self, reference: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Artifact,
            "published_manifest",
            async {
                dispatch_backend!(self, |backend| backend.published_manifest(reference).await)
            },
        )
        .await
    }
    async fn block(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
    ) -> ArtifactResult<Option<Vec<u8>>> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Artifact,
            "block",
            async { dispatch_backend!(self, |backend| backend.block(identity, ordinal).await) },
        )
        .await
    }
    async fn publish(&self, verified: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Artifact,
            "publish",
            async { dispatch_backend!(self, |backend| backend.publish(verified).await) },
        )
        .await
    }
    async fn abort(&self, identity: &ArtifactIdentity) -> ArtifactResult<()> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Artifact,
            "abort",
            async { dispatch_backend!(self, |backend| backend.abort(identity).await) },
        )
        .await
    }
    async fn cleanup(&self) -> ArtifactResult<u32> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Artifact,
            "cleanup",
            async { dispatch_backend!(self, |backend| backend.cleanup().await) },
        )
        .await
    }
}
#[async_trait]
impl TaskArtifactStore for StorageHandle {
    async fn bind_task(
        &self,
        identity: &ArtifactIdentity,
        lease: &StorageTaskLease,
        reservation: ArtifactReservation,
        output_expires_at: chrono::DateTime<chrono::Utc>,
    ) -> ArtifactResult<()> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Artifact,
            "bind_task",
            async {
                dispatch_backend!(self, |backend| backend
                    .bind_task(identity, lease, reservation, output_expires_at)
                    .await)
            },
        )
        .await
    }
}
