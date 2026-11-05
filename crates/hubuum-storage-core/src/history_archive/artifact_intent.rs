//! Bounded provider-neutral persistence metadata, never byte ownership.
use std::fmt;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::StorageError;
use crate::artifacts::{ArtifactIdentity, ArtifactLimits};

use super::{
    HISTORY_ARCHIVE_PUBLICATION_BYTES, HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES,
    HistoryArchiveCapture, HistoryArchiveExecutionPermit, HistoryArchivePublicationManifest,
    history_archive_encoded_bytes,
};

/// Bounds and commits opaque encoding only. The application must first decode
/// the real provider intent and correlate identity, limits and request commitment.
/// Core deliberately does not import or duplicate that provider protocol. Neither
/// these bytes nor construction/deserialization can issue a provider receipt.
#[derive(Clone, Eq, PartialEq)]
pub struct HistoryArchiveEncodedArtifactIntent {
    identity: ArtifactIdentity,
    limits: ArtifactLimits,
    encoded: Vec<u8>,
    digest: [u8; 32],
    request_commitment: [u8; 32],
}
impl fmt::Debug for HistoryArchiveEncodedArtifactIntent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoryArchiveEncodedArtifactIntent")
            .field("identity", &self.identity)
            .field("encoded_bytes", &self.encoded.len())
            .finish_non_exhaustive()
    }
}
impl HistoryArchiveEncodedArtifactIntent {
    pub fn try_new(
        identity: ArtifactIdentity,
        limits: ArtifactLimits,
        encoded: Vec<u8>,
        request_commitment: [u8; 32],
    ) -> Result<Self, StorageError> {
        if encoded.is_empty() {
            return Err(StorageError::invalid_input(
                "Encoded artifact intent must not be empty",
            ));
        }
        if encoded.len() > HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES {
            return Err(StorageError::input_too_large(
                "Encoded artifact intent exceeds metadata budget",
            ));
        }
        if request_commitment == [0; 32]
            || limits.artifact_bytes() == 0
            || limits.artifact_bytes() > HISTORY_ARCHIVE_PUBLICATION_BYTES
        {
            return Err(StorageError::invalid_input(
                "Invalid history artifact intent bounds",
            ));
        }
        Ok(Self {
            identity,
            limits,
            digest: Sha256::digest(&encoded).into(),
            encoded,
            request_commitment,
        })
    }
    pub const fn identity(&self) -> &ArtifactIdentity {
        &self.identity
    }
    pub const fn limits(&self) -> ArtifactLimits {
        self.limits
    }
    /// Protected persistence input. Never include it in public results/logs.
    pub fn encoded(&self) -> &[u8] {
        &self.encoded
    }
    pub const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }
    pub const fn request_commitment(&self) -> &[u8; 32] {
        &self.request_commitment
    }
}

/// One aggregate metadata budget, not independent 256 KiB allowances. This
/// request grants no authority and issues no byte attempt. Actual provider
/// admission and its non-wire receipt remain separate operations after commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryArchivePublicationRequest {
    manifest: HistoryArchivePublicationManifest,
    intent: HistoryArchiveEncodedArtifactIntent,
}
impl HistoryArchivePublicationRequest {
    pub fn try_new(
        manifest: HistoryArchivePublicationManifest,
        intent: HistoryArchiveEncodedArtifactIntent,
    ) -> Result<Self, StorageError> {
        let bytes =
            history_archive_encoded_bytes(&manifest, HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES)?;
        if manifest.location() != intent.identity()
            || manifest.source_bytes() > intent.limits().artifact_bytes()
            || <[u8; 32]>::from(Sha256::digest(&bytes)) != *intent.request_commitment()
        {
            return Err(StorageError::invalid_input(
                "Artifact intent differs from the original publication manifest",
            ));
        }
        if bytes
            .len()
            .checked_add(intent.encoded().len())
            .is_none_or(|total| total > HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES)
        {
            return Err(StorageError::input_too_large(
                "Combined publication manifest and intent exceed metadata budget",
            ));
        }
        Ok(Self { manifest, intent })
    }
    pub const fn manifest(&self) -> &HistoryArchivePublicationManifest {
        &self.manifest
    }
    pub const fn intent(&self) -> &HistoryArchiveEncodedArtifactIntent {
        &self.intent
    }
    pub fn retained_manifest_bytes(&self) -> Result<usize, StorageError> {
        Ok(self.manifest.retained_manifest_bytes()? + self.intent.encoded().len())
    }
    /// Reload and compare the same original retained capture. No re-selection,
    /// current-name mapping, renewed deadline or fresh productive scope is used.
    pub fn validate_capture(
        &self,
        capture: &HistoryArchiveCapture,
        permit: &HistoryArchiveExecutionPermit,
        now: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.manifest.validate_capture(capture, permit, now)
    }
}
