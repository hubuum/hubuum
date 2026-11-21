//! Fixed recovery metadata for one captured closed version. This is not an
//! admission, ownership, authorization, or physical completion capability.
use super::*;
use crate::artifacts::ArtifactIdentity;
use std::fmt;

/// Reserve this upper bound before issuing segment work. One captured row is
/// at most 4 MiB; the version-1 segment also bounds its header and index. The
/// application producer must enforce this physical ceiling, not just record it.
pub const HISTORY_ARCHIVE_PUBLICATION_BYTES: u64 = 16 * 1024 * 1024;
pub const HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES: usize = 256 * 1024;

/// Persisted immutable recovery coordinates, without payload, names or tokens.
/// The adapter must compare these with the original retained capture under the
/// actual execution permit before it commits a claim and returns usable work.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "PublicationInput")]
pub struct HistoryArchivePublicationManifest {
    capture: Uuid,
    principal: PrincipalIdentity,
    selection: HistoryArchiveSelection,
    generation: HistoryArchiveCatalogGeneration,
    captured_at: DateTime<Utc>,
    available_until: DateTime<Utc>,
    execution: HistoryArchiveExecutionBinding,
    identity: StorageRecordIdentity,
    valid_from: DateTime<Utc>,
    valid_to: DateTime<Utc>,
    source_bytes: u64,
    source_digest: [u8; 32],
    authorization_digest: [u8; 32],
    location: ArtifactIdentity,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicationInput {
    capture: Uuid,
    principal: PrincipalIdentity,
    selection: HistoryArchiveSelection,
    generation: HistoryArchiveCatalogGeneration,
    captured_at: DateTime<Utc>,
    available_until: DateTime<Utc>,
    execution: HistoryArchiveExecutionBinding,
    identity: StorageRecordIdentity,
    valid_from: DateTime<Utc>,
    valid_to: DateTime<Utc>,
    source_bytes: u64,
    source_digest: [u8; 32],
    authorization_digest: [u8; 32],
    location: ArtifactIdentity,
}
impl TryFrom<PublicationInput> for HistoryArchivePublicationManifest {
    type Error = StorageError;
    fn try_from(raw: PublicationInput) -> Result<Self, Self::Error> {
        let value = Self {
            capture: raw.capture,
            principal: raw.principal,
            selection: raw.selection,
            generation: raw.generation,
            captured_at: raw.captured_at,
            available_until: raw.available_until,
            execution: raw.execution,
            identity: raw.identity,
            valid_from: raw.valid_from,
            valid_to: raw.valid_to,
            source_bytes: raw.source_bytes,
            source_digest: raw.source_digest,
            authorization_digest: raw.authorization_digest,
            location: raw.location,
        };
        value.validate()?;
        Ok(value)
    }
}
impl fmt::Debug for HistoryArchivePublicationManifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoryArchivePublicationManifest")
            .field("capture", &self.capture)
            .field("identity", &self.identity)
            .field("generation", &self.generation)
            .field("source_bytes", &self.source_bytes)
            .finish_non_exhaustive()
    }
}
impl HistoryArchivePublicationManifest {
    pub fn from_capture(
        capture: &HistoryArchiveCapture,
        permit: &HistoryArchiveExecutionPermit,
        location: ArtifactIdentity,
        now: DateTime<Utc>,
    ) -> Result<Self, StorageError> {
        capture.validate_execution(permit)?;
        if now < capture.captured_at
            || capture.coverage(now) != HistoryArchiveCoverageState::Complete
            || permit
                .execution_deadline()
                .is_some_and(|deadline| now >= deadline)
        {
            return Err(StorageError::unavailable(
                "Archive publication capture is unavailable",
            ));
        }
        let row = capture.hot().ok_or_else(|| {
            StorageError::unavailable("Archive publication requires a retained primary version")
        })?;
        let valid_to = row.valid_to().ok_or_else(|| {
            StorageError::unavailable("Archive publication requires a closed version")
        })?;
        let facts = capture.authorization().ok_or_else(|| {
            StorageError::unavailable("Archive publication lacks original authorization facts")
        })?;
        let bytes = history_archive_encoded_bytes(row, HISTORY_ARCHIVE_ROW_BYTES)?;
        let value = Self {
            capture: capture.id,
            principal: capture.principal,
            selection: capture.selection.clone(),
            generation: capture.generation,
            captured_at: capture.captured_at,
            available_until: capture.available_until,
            execution: permit.binding(),
            identity: row.identity(),
            valid_from: row.valid_from(),
            valid_to,
            source_bytes: bytes.len() as u64,
            source_digest: Sha256::digest(bytes).into(),
            authorization_digest: Sha256::digest(history_archive_encoded_bytes(
                facts,
                HISTORY_ARCHIVE_CAPTURE_BYTES,
            )?)
            .into(),
            location,
        };
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<(), StorageError> {
        if self.capture.is_nil()
            || self.selection.family() != StorageBackupHistorySection::CollectionHistory
            || self.selection.resource() != self.identity.resource()
            || self.valid_from >= self.valid_to
            || self.available_until <= self.captured_at
            || self.available_until > self.captured_at + Duration::hours(48)
            || self
                .execution
                .deadline
                .is_some_and(|deadline| deadline <= self.captured_at)
            || self.source_bytes == 0
            || self.source_bytes > HISTORY_ARCHIVE_ROW_BYTES as u64
            || self.source_digest == [0; 32]
            || self.authorization_digest == [0; 32]
            || !self.execution.is_well_formed()
        {
            return Err(invalid("Invalid archive publication manifest"));
        }
        let matches = match self.selection.point() {
            HistoryArchivePoint::Exact { version } => *version == self.identity.version(),
            HistoryArchivePoint::AsOf { at } => self.valid_from <= *at && *at < self.valid_to,
        };
        if !matches {
            return Err(invalid(
                "Publication selection differs from original interval",
            ));
        }
        history_archive_encoded_bytes(self, HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES)?;
        Ok(())
    }
    /// Reconstruct only from the original retained capture. No current name,
    /// integer lookup, renewed deadline, or replacement permit can repair it.
    pub fn validate_capture(
        &self,
        capture: &HistoryArchiveCapture,
        permit: &HistoryArchiveExecutionPermit,
        now: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let expected = Self::from_capture(capture, permit, self.location.clone(), now)?;
        if self != &expected {
            return Err(StorageError::unavailable(
                "Archive publication differs from original capture",
            ));
        }
        Ok(())
    }
    pub fn validate_execution(
        &self,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<(), StorageError> {
        self.validate()?;
        if self.execution != permit.binding() {
            return Err(StorageError::unavailable(
                "Publication belongs to a different original producer",
            ));
        }
        Ok(())
    }
    pub const fn principal(&self) -> PrincipalIdentity {
        self.principal
    }
    pub const fn capture(&self) -> Uuid {
        self.capture
    }
    pub const fn location(&self) -> &ArtifactIdentity {
        &self.location
    }
    pub const fn identity(&self) -> StorageRecordIdentity {
        self.identity
    }
    pub const fn generation(&self) -> HistoryArchiveCatalogGeneration {
        self.generation
    }
    pub const fn available_until(&self) -> DateTime<Utc> {
        self.available_until
    }
    /// Fixed execution cutoff from the admitted scope, capped by the original
    /// capture's availability. Neither reload nor recovery may recompute it.
    pub fn execution_deadline(&self) -> DateTime<Utc> {
        self.execution
            .deadline
            .unwrap_or(self.available_until)
            .min(self.available_until)
    }
    pub const fn source_bytes(&self) -> u64 {
        self.source_bytes
    }
    /// Opaque pre-I/O request binding. This is not an expected encoded artifact
    /// manifest digest and does not replace independent artifact verification.
    pub fn request_commitment(&self) -> Result<[u8; 32], StorageError> {
        Ok(Sha256::digest(history_archive_encoded_bytes(
            self,
            HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES,
        )?)
        .into())
    }
    /// Canonical retained metadata plus a fixed ledger-row allowance. This is
    /// logical accounting, not a measurement of allocator/RSS overhead.
    pub fn retained_manifest_bytes(&self) -> Result<usize, StorageError> {
        Ok(
            history_archive_encoded_bytes(self, HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES)?.len()
                + 128,
        )
    }
    pub const fn reserved_artifact_bytes(&self) -> u64 {
        HISTORY_ARCHIVE_PUBLICATION_BYTES
    }
}

/// Affine evidence that the adapter committed this fixed publication claim.
/// Drop does not release its reservation. This is not provider ownership,
/// publication/cleanup completion, authorization, or a transport receipt.
pub struct HistoryArchivePublicationClaim {
    request: super::HistoryArchivePublicationRequest,
    permit: HistoryArchiveExecutionPermit,
}
impl fmt::Debug for HistoryArchivePublicationClaim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoryArchivePublicationClaim")
            .field("manifest", self.request.manifest())
            .finish_non_exhaustive()
    }
}
impl HistoryArchivePublicationClaim {
    /// Trusted complete-adapter boundary, invoked only after commit. A future
    /// physical producer must additionally establish exclusive provider ownership.
    pub fn from_committed(
        request: super::HistoryArchivePublicationRequest,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<Self, StorageError> {
        request.manifest().validate_execution(permit)?;
        Ok(Self {
            request,
            permit: permit.clone(),
        })
    }
    pub const fn manifest(&self) -> &HistoryArchivePublicationManifest {
        self.request.manifest()
    }
    pub const fn request(&self) -> &super::HistoryArchivePublicationRequest {
        &self.request
    }
    pub const fn permit(&self) -> &HistoryArchiveExecutionPermit {
        &self.permit
    }
}
