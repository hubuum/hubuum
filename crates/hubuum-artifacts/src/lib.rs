//! Immutable byte storage, independent of the complete hot-storage SDK.
//!
//! A write is staged, sealed, independently read back, then published. Neither
//! provider metadata nor a successful upload creates a verification proof.
//! This internal foundation does not enable application output offloading.

mod identity;
mod manifest;
mod producer;
mod transfer;

use std::fmt;

use async_trait::async_trait;

pub use identity::{
    ArtifactId, ArtifactIdentity, AttemptId, Destination, DestinationId, ObjectVersion,
};
pub use manifest::{
    ArtifactLimits, ArtifactLimitsBuilder, ArtifactManifest, ManifestBuilder, VerifiedArtifact,
};
pub use producer::stage_producer;
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
    /// Reap expired/aborted staging with a hard bound of 16 blocks per call.
    /// Return the number of physical blocks removed; published data is excluded.
    async fn cleanup(&self) -> ArtifactResult<u32>;
}
