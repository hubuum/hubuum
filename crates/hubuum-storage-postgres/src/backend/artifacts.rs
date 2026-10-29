use super::PostgresStorage;
use async_trait::async_trait;
use hubuum_storage_core::{StorageTaskLease, artifacts::*};

#[async_trait]
impl ArtifactStore for PostgresStorage {
    async fn register_destination(&self, destination: &Destination) -> ArtifactResult<()> {
        self.artifact_store()
            .register_destination(destination)
            .await
    }
    async fn stage(
        &self,
        identity: &ArtifactIdentity,
        limits: ArtifactLimits,
    ) -> ArtifactResult<()> {
        self.artifact_store().stage(identity, limits).await
    }
    async fn append(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
    ) -> ArtifactResult<()> {
        self.artifact_store().append(identity, ordinal, bytes).await
    }
    async fn seal(&self, manifest: &ArtifactManifest) -> ArtifactResult<()> {
        self.artifact_store().seal(manifest).await
    }
    async fn manifest(&self, identity: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        self.artifact_store().manifest(identity).await
    }
    async fn published_manifest(&self, reference: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        self.artifact_store().published_manifest(reference).await
    }
    async fn block(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
    ) -> ArtifactResult<Option<Vec<u8>>> {
        self.artifact_store().block(identity, ordinal).await
    }
    async fn publish(&self, verified: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        self.artifact_store().publish(verified).await
    }
    async fn abort(&self, identity: &ArtifactIdentity) -> ArtifactResult<()> {
        self.artifact_store().abort(identity).await
    }
    async fn cleanup(&self) -> ArtifactResult<u32> {
        self.artifact_store().cleanup().await
    }
}
#[async_trait]
impl TaskArtifactStore for PostgresStorage {
    async fn bind_task(
        &self,
        identity: &ArtifactIdentity,
        lease: &StorageTaskLease,
        reservation: ArtifactReservation,
        output_expires_at: chrono::DateTime<chrono::Utc>,
    ) -> ArtifactResult<()> {
        self.artifact_store()
            .bind_task(identity, lease, reservation, output_expires_at)
            .await
    }
}
