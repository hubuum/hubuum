//! Offline reconciliation of a single interrupted publication.
use super::*;
use crate::RuntimeIsolationDomain;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryArchiveCleanupRequest {
    publication: Uuid,
    fence: ApplicationFence,
    isolation: RuntimeIsolationDomain,
}
impl HistoryArchiveCleanupRequest {
    pub fn new(
        publication: Uuid,
        fence: ApplicationFence,
        isolation: RuntimeIsolationDomain,
    ) -> Result<Self, StorageError> {
        if publication.is_nil() {
            return Err(invalid("A publication ID is required for cleanup"));
        }
        Ok(Self {
            publication,
            fence,
            isolation,
        })
    }
    pub const fn publication(&self) -> Uuid {
        self.publication
    }
    pub const fn fence(&self) -> ApplicationFence {
        self.fence
    }
    pub const fn isolation(&self) -> &RuntimeIsolationDomain {
        &self.isolation
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct HistoryArchiveCleanupClaim {
    request: HistoryArchiveCleanupRequest,
    manifest: HistoryArchivePublicationManifest,
    encoded_intent: Vec<u8>,
    ownership: [Uuid; 2],
}
impl HistoryArchiveCleanupClaim {
    /// Complete adapter boundary. The transaction must check process isolation,
    /// quarantined application, native parent/child ownership and capture pin.
    pub fn from_retained(
        request: &HistoryArchiveCleanupRequest,
        manifest: HistoryArchivePublicationManifest,
        capture: &HistoryArchiveCapture,
        encoded_intent: Vec<u8>,
        ownership: [Uuid; 2],
    ) -> Result<Self, StorageError> {
        if request.publication != manifest.publication_id()
            || request.fence.incarnation() == manifest.original_history().incarnation()
            || !manifest.execution_matches_fence(capture.fence)
            || ownership.iter().any(Uuid::is_nil)
            || encoded_intent.is_empty()
            || encoded_intent.len() + manifest.retained_manifest_bytes()?
                > HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES + 128
        {
            return Err(StorageError::unavailable(
                "Unresolved original archive claim or recovery fence",
            ));
        }
        manifest.validate_retained_capture(capture)?;
        Ok(Self {
            request: request.clone(),
            manifest,
            encoded_intent,
            ownership,
        })
    }
    pub(crate) fn matches_ownership(&self, tokens: [Uuid; 2]) -> bool {
        self.ownership == tokens
    }
    pub fn request(&self) -> &HistoryArchiveCleanupRequest {
        &self.request
    }
    pub fn manifest(&self) -> &HistoryArchivePublicationManifest {
        &self.manifest
    }
    pub fn encoded_intent(&self) -> &[u8] {
        &self.encoded_intent
    }
    pub fn after_cleanup(
        self,
        evidence: &crate::artifacts::ArtifactCleanupReceipt,
    ) -> Result<HistoryArchiveCleanupSettlement, StorageError> {
        if evidence.encoded_intent() != self.encoded_intent {
            return Err(StorageError::conflict(
                "Physical cleanup receipt differs from the retained intent",
            ));
        }
        Ok(HistoryArchiveCleanupSettlement(self))
    }
}

/// The exact obligation and recovery fence covered by physical cleanup.
pub struct HistoryArchiveCleanupSettlement(HistoryArchiveCleanupClaim);
impl HistoryArchiveCleanupSettlement {
    pub fn claim(&self) -> &HistoryArchiveCleanupClaim {
        &self.0
    }
}
