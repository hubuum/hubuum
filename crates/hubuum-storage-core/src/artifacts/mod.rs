//! Immutable byte storage required by every complete storage adapter.
//!
//! A write is staged, sealed, independently read back, then published. Neither
//! provider metadata nor a successful upload creates a verification proof.
//! Retained proof types cross task publication without reconstructing raw metadata.

mod backup;
pub(crate) use backup::validate_backup_artifacts;
pub use backup::{
    ArtifactBackupFrame, BackupArtifactDecoder, BackupArtifactHeader, BackupArtifactPart,
};
mod identity;
mod manifest;
mod transfer;

use std::fmt;
use std::time::Duration;

/// Keep local-spool reservations while an abandoned bounded renderer exits.
/// Producers must stop local work within this interval after losing ownership.
pub const LOCAL_SPOOL_CLEANUP_GRACE: Duration = Duration::from_secs(10);

use async_trait::async_trait;

pub use identity::{
    ArtifactId, ArtifactIdentity, AttemptId, Destination, DestinationId, ObjectVersion,
};
pub use manifest::{
    ArtifactLimits, ArtifactLimitsBuilder, ArtifactManifest, ManifestBuilder, ManifestVerifier,
    VerifiedArtifact,
};
pub use transfer::{ArtifactReader, ArtifactWriter, verify, verify_and_publish};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArtifactError {
    Invalid(String),
    Limit(String),
    Corrupt(String),
    Conflict(String),
    NotFound,
    Backend(String),
}

impl fmt::Display for ArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message)
            | Self::Limit(message)
            | Self::Corrupt(message)
            | Self::Conflict(message)
            | Self::Backend(message) => f.write_str(message),
            Self::NotFound => f.write_str("Artifact was not found"),
        }
    }
}

impl std::error::Error for ArtifactError {}

pub type ArtifactResult<T> = Result<T, ArtifactError>;

/// Retained catalog reference. Readers resolve it against durable publication
/// state and verify its manifest digest before releasing any bytes.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReference {
    identity: ArtifactIdentity,
    manifest_digest: [u8; 32],
}

impl ArtifactReference {
    /// Decode a reference from trusted application/catalog storage. The digest
    /// must come from that storage, independently of the bytes being fetched.
    pub fn from_catalog(identity: ArtifactIdentity, manifest_digest: [u8; 32]) -> Self {
        Self {
            identity,
            manifest_digest,
        }
    }

    pub fn identity(&self) -> &ArtifactIdentity {
        &self.identity
    }
    pub fn manifest_digest(&self) -> &[u8; 32] {
        &self.manifest_digest
    }
}

/// Small immutable destination contract. Adapters must enforce the lifecycle
/// durably, including races with abort/expiry, rather than trusting local types.
/// Calls transfer at most one bounded block or manifest. No provider I/O occurs
/// inside a primary-store publication transaction.
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Idempotent for the exact same ID/name; neither may be rebound. A change
    /// of physical destination creates a new ID, independently of credentials.
    async fn register_destination(&self, destination: &Destination) -> ArtifactResult<()>;
    /// Start an immutable attempt with a bounded staging lifetime. There are no
    /// cascading references to replaceable task/application rows.
    async fn stage(
        &self,
        identity: &ArtifactIdentity,
        limits: ArtifactLimits,
    ) -> ArtifactResult<()>;
    /// Append at exactly the next ordinal, allowing an identical retry. Once
    /// sealed, even byte-identical writes must be rejected.
    async fn append(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
    ) -> ArtifactResult<()>;
    /// Freeze the attempt and its expected manifest. Validate persisted block
    /// count/lengths; publication still requires independent read-back.
    async fn seal(&self, manifest: &ArtifactManifest) -> ArtifactResult<()>;
    /// Read the bounded immutable manifest of a sealed or published attempt.
    async fn manifest(&self, identity: &ArtifactIdentity) -> ArtifactResult<Vec<u8>>;
    /// A user-facing reader must additionally prove durable publication, even
    /// if it knows the identities and digest of an unpublished sealed attempt.
    async fn published_manifest(&self, reference: &ArtifactReference) -> ArtifactResult<Vec<u8>>;
    /// Read at most one block of a sealed or published attempt. None is exact
    /// EOF; missing interior blocks are diagnosed by the verifier.
    async fn block(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
    ) -> ArtifactResult<Option<Vec<u8>>>;
    /// Atomically fence abort/expiry and bind the catalog to the verified
    /// manifest and exact object version. An identical publication is retryable.
    async fn publish(&self, verified: &VerifiedArtifact) -> ArtifactResult<ArtifactReference>;
    /// Fence any future publication. Published artifacts cannot be aborted.
    /// Physical obligations remain until bounded cleanup acknowledges removal.
    /// An unknown attempt must return NotFound: no durable fence exists yet.
    async fn abort(&self, identity: &ArtifactIdentity) -> ArtifactResult<()>;
    /// Reap expired/aborted staging and task-owned expired/orphaned publications.
    /// Delete at most 16 blocks per call. Independent publications are retained.
    /// Fence abandoned local producers and retain their reservation through
    /// LOCAL_SPOOL_CLEANUP_GRACE before releasing physical capacity. A zero
    /// return can mean cleanup is waiting for that grace period.
    /// Return the number of physical blocks removed.
    async fn cleanup(&self) -> ArtifactResult<u32>;
}

/// A process incarnation's reservation. Releasing the task's execution slot
/// never releases durable physical bytes that still require cleanup.
#[derive(Clone, Debug)]
pub struct ArtifactReservation {
    worker: [u8; 16],
    spool_bytes: u64,
    worker_bytes: u64,
}
impl ArtifactReservation {
    pub fn new(worker: [u8; 16], spool_bytes: u64, worker_bytes: u64) -> ArtifactResult<Self> {
        if worker == [0; 16]
            || worker_bytes == 0
            || worker_bytes > i64::MAX as u64
            || spool_bytes > worker_bytes
        {
            return Err(ArtifactError::Invalid(
                "Invalid artifact reservation".into(),
            ));
        }
        Ok(Self {
            worker,
            spool_bytes,
            worker_bytes,
        })
    }
    pub fn worker(&self) -> &[u8; 16] {
        &self.worker
    }
    pub fn spool_bytes(&self) -> u64 {
        self.spool_bytes
    }
    pub fn worker_bytes(&self) -> u64 {
        self.worker_bytes
    }
}

/// Task-owned artifact admission supplements the immutable byte-store protocol.
/// Adapters bind to the exact durable task incarnation and fenced execution.
#[async_trait]
pub trait TaskArtifactStore: ArtifactStore {
    async fn bind_task(
        &self,
        identity: &ArtifactIdentity,
        lease: &crate::StorageTaskLease,
        reservation: ArtifactReservation,
        output_expires_at: chrono::DateTime<chrono::Utc>,
    ) -> ArtifactResult<()>;
}
