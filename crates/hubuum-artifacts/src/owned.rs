//! Optional producer ownership for immutable byte stores.
//!
//! Persist canonical intent bytes before calling `create_owned`. The request
//! commitment is opaque correlation data, not authorization or a byte digest.
//! A receipt proves one logical operation owns an attempt; the caller must
//! serialize its live producers and fence obsolete/restored incarnations. There
//! is no lease transfer. Errors, cancellation and absence never prove that a
//! delayed create cannot commit. Retain the intent until it can be reconciled.
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactManifest, ArtifactReference,
    ArtifactResult, ArtifactStore, Destination, VerifiedArtifact,
};

/// Upper bound for durable opaque intent storage. Current v1 encoding is much smaller.
pub const OWNED_INTENT_BYTES: usize = 256 * 1024;

/// Validated correlation data only. Deserializing this value never mints ownership.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "IntentEncoding", into = "IntentEncoding")]
pub struct ArtifactCreationIntent {
    encoded: IntentEncoding,
}
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntentEncoding {
    protocol: u8,
    identity: ArtifactIdentity,
    limits: [u64; 4],
    operation: [u8; 16],
    incarnation: [u8; 16],
    request_commitment: [u8; 32],
}
impl fmt::Debug for ArtifactCreationIntent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ArtifactCreationIntent { .. }")
    }
}
impl TryFrom<IntentEncoding> for ArtifactCreationIntent {
    type Error = ArtifactError;
    fn try_from(encoded: IntentEncoding) -> ArtifactResult<Self> {
        let intent = Self { encoded };
        if intent.encoded.protocol != 1
            || intent.encoded.operation == [0; 16]
            || intent.encoded.incarnation == [0; 16]
        {
            return Err(ArtifactError::Invalid(
                "Invalid owned artifact intent".into(),
            ));
        }
        intent.validated_limits()?;
        Ok(intent)
    }
}
impl From<ArtifactCreationIntent> for IntentEncoding {
    fn from(intent: ArtifactCreationIntent) -> Self {
        intent.encoded
    }
}
impl ArtifactCreationIntent {
    pub fn new(
        identity: ArtifactIdentity,
        limits: ArtifactLimits,
        operation: [u8; 16],
        incarnation: [u8; 16],
        request_commitment: [u8; 32],
    ) -> ArtifactResult<Self> {
        IntentEncoding {
            protocol: 1,
            identity,
            limits: [
                u64::from(limits.block_bytes()),
                limits.artifact_bytes(),
                u64::from(limits.blocks()),
                u64::from(limits.manifest_bytes()),
            ],
            operation,
            incarnation,
            request_commitment,
        }
        .try_into()
    }
    pub fn identity(&self) -> &ArtifactIdentity {
        &self.encoded.identity
    }
    pub fn limits(&self) -> ArtifactLimits {
        self.validated_limits()
            .expect("private validated intent limits")
    }
    pub fn operation(&self) -> &[u8; 16] {
        &self.encoded.operation
    }
    pub fn incarnation(&self) -> &[u8; 16] {
        &self.encoded.incarnation
    }
    pub fn request_commitment(&self) -> &[u8; 32] {
        &self.encoded.request_commitment
    }
    pub fn encode(&self) -> ArtifactResult<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|_| ArtifactError::Invalid("Cannot encode owned intent".into()))
    }
    /// Only the exact canonical v1 bytes are accepted (including field order).
    pub fn decode(bytes: &[u8]) -> ArtifactResult<Self> {
        if bytes.len() > OWNED_INTENT_BYTES {
            return Err(ArtifactError::Limit("Owned intent exceeds bound".into()));
        }
        let intent: Self = serde_json::from_slice(bytes)
            .map_err(|_| ArtifactError::Invalid("Invalid owned intent encoding".into()))?;
        if intent.encode()? != bytes {
            return Err(ArtifactError::Invalid(
                "Owned intent is not canonical".into(),
            ));
        }
        Ok(intent)
    }
    fn validated_limits(&self) -> ArtifactResult<ArtifactLimits> {
        let invalid = || ArtifactError::Invalid("Invalid owned intent limits".into());
        ArtifactLimits::builder()
            .block_bytes(self.encoded.limits[0].try_into().map_err(|_| invalid())?)
            .artifact_bytes(self.encoded.limits[1])
            .blocks(self.encoded.limits[2].try_into().map_err(|_| invalid())?)
            .manifest_bytes(self.encoded.limits[3].try_into().map_err(|_| invalid())?)
            .build()
    }
}

/// Errors never confer ownership or establish physical absence. A conflict can
/// follow an earlier ambiguous successful request; reconcile the original intent.
#[derive(Debug)]
pub enum OwnedArtifactError {
    Conflict,
    Unresolved(ArtifactError),
}
impl fmt::Display for OwnedArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict => {
                f.write_str("Owned artifact identity, intent, scope or phase conflicts")
            }
            Self::Unresolved(error) => {
                write!(f, "Owned artifact outcome remains unresolved: {error}")
            }
        }
    }
}
impl std::error::Error for OwnedArtifactError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Unresolved(error) => Some(error),
            Self::Conflict => None,
        }
    }
}
impl From<ArtifactError> for OwnedArtifactError {
    fn from(error: ArtifactError) -> Self {
        match error {
            ArtifactError::Conflict(_) => Self::Conflict,
            error => Self::Unresolved(error),
        }
    }
}
impl From<OwnedArtifactError> for ArtifactError {
    fn from(error: OwnedArtifactError) -> Self {
        match error {
            OwnedArtifactError::Conflict => owned_conflict(),
            OwnedArtifactError::Unresolved(error) => error,
        }
    }
}
pub type OwnedArtifactResult<T> = Result<T, OwnedArtifactError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OwnedArtifactPhase {
    Staging,
    Sealed,
    Published(ArtifactReference),
    /// Publication is durably fenced. Physical removal remains unproven.
    Fenced,
}
/// Bound only by a successful durable store check, never by deserialization.
#[derive(Clone)]
pub struct OwnedArtifact {
    intent: Box<ArtifactCreationIntent>,
    scope: StoreScope,
}
impl fmt::Debug for OwnedArtifact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OwnedArtifact { .. }")
    }
}
impl OwnedArtifact {
    pub fn intent(&self) -> &ArtifactCreationIntent {
        &self.intent
    }
    pub fn session<'a, S: OwnedArtifactStore<Receipt = OwnedArtifact> + ?Sized>(
        &'a self,
        store: &'a S,
    ) -> OwnedArtifactSession<'a, S> {
        OwnedArtifactSession {
            store,
            artifact: self,
        }
    }
    pub(crate) fn checked(intent: ArtifactCreationIntent, scope: StoreScope) -> Self {
        Self {
            intent: Box::new(intent),
            scope,
        }
    }
}
#[derive(Debug)]
pub enum OwnedArtifactAdmission<R = OwnedArtifact> {
    Created(R),
    ResumedSameOwner {
        artifact: R,
        phase: OwnedArtifactPhase,
    },
}
#[derive(Debug)]
pub enum OwnedArtifactResolution<R = OwnedArtifact> {
    /// No receipt can be issued yet. A delayed create may still commit.
    Pending,
    Established {
        artifact: R,
        phase: OwnedArtifactPhase,
    },
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum StoreScope {
    #[cfg(unix)]
    Filesystem { device: u64, inode: u64 },
    #[cfg(feature = "s3")]
    S3 {
        bucket: String,
        region: String,
        endpoint: Option<String>,
    },
    #[cfg(test)]
    Test(u64),
}
pub(crate) fn owned_conflict() -> ArtifactError {
    ArtifactError::Conflict("Owned artifact identity, intent, scope or phase conflicts".into())
}
pub(crate) fn authorize(
    binding: Option<&ArtifactCreationIntent>,
    scope: &StoreScope,
    artifact: Option<&OwnedArtifact>,
) -> ArtifactResult<()> {
    match (binding, artifact) {
        (None, None) => Ok(()),
        (Some(binding), Some(artifact))
            if binding == artifact.intent.as_ref() && scope == &artifact.scope =>
        {
            Ok(())
        }
        _ => Err(owned_conflict()),
    }
}

/// Adapter-issued logical admission proof. Implementations retain their native
/// private proof and issue it only after real durable admission or recovery.
/// This trait provides no constructor, serialization, cloning, lease, lifetime,
/// transfer or physical-cleanup guarantee. Intent construction is not admission.
pub trait OwnedArtifactReceipt: Send + Sync {
    fn intent(&self) -> &ArtifactCreationIntent;
    /// Compare stable actual issuer/store scope and the FULL immutable intent,
    /// plus any native admission binding. Never compare only ArtifactIdentity,
    /// mutable phase, append counters, or other progress observations.
    fn same_binding(&self, other: &Self) -> bool;
}
impl OwnedArtifactReceipt for OwnedArtifact {
    fn intent(&self) -> &ArtifactCreationIntent {
        &self.intent
    }
    fn same_binding(&self, other: &Self) -> bool {
        self.scope == other.scope && self.intent == other.intent
    }
}

/// Optional, complete producer ownership capability. Implementations MUST fence
/// legacy mutators on owned records and validate receipts inside their lock/CAS.
/// No method transfers ownership or proves all physical obligations are removed.
#[async_trait]
pub trait OwnedArtifactStore: ArtifactStore {
    type Receipt: OwnedArtifactReceipt;
    async fn create_owned(
        &self,
        intent: &ArtifactCreationIntent,
    ) -> OwnedArtifactResult<OwnedArtifactAdmission<Self::Receipt>>;
    async fn resolve_owned(
        &self,
        intent: &ArtifactCreationIntent,
    ) -> OwnedArtifactResult<OwnedArtifactResolution<Self::Receipt>>;
    async fn append_owned(
        &self,
        artifact: &Self::Receipt,
        ordinal: u32,
        bytes: &[u8],
    ) -> OwnedArtifactResult<()>;
    async fn seal_owned(
        &self,
        artifact: &Self::Receipt,
        manifest: &ArtifactManifest,
    ) -> OwnedArtifactResult<()>;
    async fn publish_owned(
        &self,
        artifact: &Self::Receipt,
        verified: &VerifiedArtifact,
    ) -> OwnedArtifactResult<ArtifactReference>;
    /// Success acknowledges a durable publication fence, never cleanup completion.
    async fn abort_owned(&self, artifact: &Self::Receipt) -> OwnedArtifactResult<()>;
}

/// Owner-gated view for the existing bounded writer, producer bridge and verifier.
/// Its `stage` only resolves an admitted receipt; it never creates/adopts staging.
/// Producer error cleanup through this view acknowledges only an owner-specific
/// publication fence. Keep the application's physical obligation pending; neither
/// a producer join nor successful abort proves provider cleanup or never-created.
pub struct OwnedArtifactSession<'a, S: OwnedArtifactStore + ?Sized> {
    store: &'a S,
    artifact: &'a S::Receipt,
}
impl<'a, S: OwnedArtifactStore + ?Sized> OwnedArtifactSession<'a, S> {
    /// Construct only a view over an already issued receipt, never ownership.
    pub fn new(store: &'a S, artifact: &'a S::Receipt) -> Self {
        Self { store, artifact }
    }
    fn identity(&self, identity: &ArtifactIdentity) -> ArtifactResult<()> {
        if identity != self.artifact.intent().identity() {
            return Err(owned_conflict());
        }
        Ok(())
    }
}
#[async_trait]
impl<S: OwnedArtifactStore + ?Sized> ArtifactStore for OwnedArtifactSession<'_, S> {
    async fn register_destination(&self, destination: &Destination) -> ArtifactResult<()> {
        self.store.register_destination(destination).await
    }
    async fn stage(
        &self,
        identity: &ArtifactIdentity,
        limits: ArtifactLimits,
    ) -> ArtifactResult<()> {
        self.identity(identity)?;
        if limits != self.artifact.intent().limits() {
            return Err(owned_conflict());
        }
        match self.store.resolve_owned(self.artifact.intent()).await? {
            OwnedArtifactResolution::Established {
                artifact,
                phase: OwnedArtifactPhase::Staging,
            } if artifact.same_binding(self.artifact)
                && artifact.intent() == self.artifact.intent() =>
            {
                Ok(())
            }
            _ => Err(owned_conflict()),
        }
    }
    async fn append(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
    ) -> ArtifactResult<()> {
        self.identity(identity)?;
        self.store
            .append_owned(self.artifact, ordinal, bytes)
            .await
            .map_err(Into::into)
    }
    async fn seal(&self, manifest: &ArtifactManifest) -> ArtifactResult<()> {
        self.identity(manifest.identity())?;
        self.store
            .seal_owned(self.artifact, manifest)
            .await
            .map_err(Into::into)
    }
    async fn manifest(&self, identity: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        self.identity(identity)?;
        self.store.manifest(identity).await
    }
    async fn published_manifest(&self, reference: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        self.identity(reference.identity())?;
        self.store.published_manifest(reference).await
    }
    async fn block(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
    ) -> ArtifactResult<Option<Vec<u8>>> {
        self.identity(identity)?;
        self.store.block(identity, ordinal).await
    }
    async fn publish(&self, verified: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        self.identity(verified.manifest().identity())?;
        self.store
            .publish_owned(self.artifact, verified)
            .await
            .map_err(Into::into)
    }
    async fn abort(&self, identity: &ArtifactIdentity) -> ArtifactResult<()> {
        self.identity(identity)?;
        self.store
            .abort_owned(self.artifact)
            .await
            .map_err(Into::into)
    }
    async fn cleanup(&self) -> ArtifactResult<u32> {
        Err(owned_conflict())
    }
}

#[cfg(test)]
mod tests;
