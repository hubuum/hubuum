use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_domain::{MaintenanceState, PrincipalId, RestoreJobId};
use serde_json::Value;
use uuid::Uuid;

use crate::{StorageBackupSnapshot, StorageError};

/// Persisted restore lifecycle state, independent of an adapter's encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageRestoreJobStatus {
    Validated,
    Confirmed,
    Succeeded,
    Failed,
    Expired,
}

impl StorageRestoreJobStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Validated => "validated",
            Self::Confirmed => "confirmed",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Expired => "expired",
        }
    }

    pub fn from_stored(value: &str) -> Result<Self, StorageError> {
        match value {
            "validated" => Ok(Self::Validated),
            "confirmed" => Ok(Self::Confirmed),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "expired" => Ok(Self::Expired),
            _ => Err(StorageError::internal(format!(
                "Unknown persisted restore status '{value}'"
            ))),
        }
    }
}

/// Initiator identity captured when a restore artifact is staged.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageRestoreInitiator {
    principal_id: Option<PrincipalId>,
    identity_scope: String,
    name: String,
}

impl StorageRestoreInitiator {
    #[must_use]
    pub fn new(
        principal_id: Option<PrincipalId>,
        identity_scope: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            principal_id,
            identity_scope: identity_scope.into(),
            name: name.into(),
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Option<PrincipalId>, String, String) {
        (self.principal_id, self.identity_scope, self.name)
    }
}

impl fmt::Debug for StorageRestoreInitiator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRestoreInitiator")
            .field("has_principal", &self.principal_id.is_some())
            .field("identity", &"[redacted]")
            .finish()
    }
}

/// Non-secret artifact identity shared by restore projections.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageRestoreArtifactSummary {
    byte_size: i64,
    sha256: String,
}

impl StorageRestoreArtifactSummary {
    #[must_use]
    pub fn new(byte_size: i64, sha256: impl Into<String>) -> Self {
        Self {
            byte_size,
            sha256: sha256.into(),
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i64, String) {
        (self.byte_size, self.sha256)
    }
}

impl fmt::Debug for StorageRestoreArtifactSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRestoreArtifactSummary")
            .field("byte_size", &self.byte_size)
            .field("digest", &"[redacted]")
            .finish()
    }
}

/// Durable timestamps attached to a staged restore.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRestoreTimestamps {
    expires_at: NaiveDateTime,
    confirmed_at: Option<NaiveDateTime>,
    finished_at: Option<NaiveDateTime>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl StorageRestoreTimestamps {
    #[must_use]
    pub const fn new(
        expires_at: NaiveDateTime,
        confirmed_at: Option<NaiveDateTime>,
        finished_at: Option<NaiveDateTime>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
    ) -> Self {
        Self {
            expires_at,
            confirmed_at,
            finished_at,
            created_at,
            updated_at,
        }
    }

    #[must_use]
    pub const fn expires_at(self) -> NaiveDateTime {
        self.expires_at
    }

    #[must_use]
    pub const fn into_parts(
        self,
    ) -> (
        NaiveDateTime,
        Option<NaiveDateTime>,
        Option<NaiveDateTime>,
        NaiveDateTime,
        NaiveDateTime,
    ) {
        (
            self.expires_at,
            self.confirmed_at,
            self.finished_at,
            self.created_at,
            self.updated_at,
        )
    }
}

/// Shared non-secret restore job projection.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageRestoreJobSummary {
    id: RestoreJobId,
    status: StorageRestoreJobStatus,
    initiator: StorageRestoreInitiator,
    artifact: StorageRestoreArtifactSummary,
    error: Option<String>,
    timestamps: StorageRestoreTimestamps,
}

impl StorageRestoreJobSummary {
    #[must_use]
    pub const fn new(
        id: RestoreJobId,
        status: StorageRestoreJobStatus,
        initiator: StorageRestoreInitiator,
        artifact: StorageRestoreArtifactSummary,
        error: Option<String>,
        timestamps: StorageRestoreTimestamps,
    ) -> Self {
        Self {
            id,
            status,
            initiator,
            artifact,
            error,
            timestamps,
        }
    }

    #[must_use]
    pub const fn id(&self) -> RestoreJobId {
        self.id
    }

    #[must_use]
    pub const fn status(&self) -> StorageRestoreJobStatus {
        self.status
    }

    #[must_use]
    pub const fn timestamps(&self) -> StorageRestoreTimestamps {
        self.timestamps
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        RestoreJobId,
        StorageRestoreJobStatus,
        StorageRestoreInitiator,
        StorageRestoreArtifactSummary,
        Option<String>,
        StorageRestoreTimestamps,
    ) {
        (
            self.id,
            self.status,
            self.initiator,
            self.artifact,
            self.error,
            self.timestamps,
        )
    }
}

impl fmt::Debug for StorageRestoreJobSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRestoreJobSummary")
            .field("id", &self.id)
            .field("status", &self.status)
            .field("initiator", &self.initiator)
            .field("artifact", &self.artifact)
            .field("has_error", &self.error.is_some())
            .field("timestamps", &self.timestamps)
            .finish()
    }
}

/// Command for staging an already validated backup artifact.
#[derive(Clone, PartialEq)]
pub struct StorageRestoreStageCreate {
    initiator: StorageRestoreInitiator,
    document: Vec<u8>,
    artifact: StorageRestoreArtifactSummary,
    capability_hash: String,
    validation_summary: Value,
    expires_at: NaiveDateTime,
}

impl StorageRestoreStageCreate {
    #[must_use]
    pub fn new(
        initiator: StorageRestoreInitiator,
        document: Vec<u8>,
        artifact: StorageRestoreArtifactSummary,
        capability_hash: impl Into<String>,
        validation_summary: Value,
        expires_at: NaiveDateTime,
    ) -> Self {
        Self {
            initiator,
            document,
            artifact,
            capability_hash: capability_hash.into(),
            validation_summary,
            expires_at,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageRestoreInitiator,
        Vec<u8>,
        StorageRestoreArtifactSummary,
        String,
        Value,
        NaiveDateTime,
    ) {
        (
            self.initiator,
            self.document,
            self.artifact,
            self.capability_hash,
            self.validation_summary,
            self.expires_at,
        )
    }
}

impl fmt::Debug for StorageRestoreStageCreate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRestoreStageCreate")
            .field("initiator", &self.initiator)
            .field("document_bytes", &self.document.len())
            .field("artifact", &self.artifact)
            .field("expires_at", &self.expires_at)
            .field("secrets", &"[redacted]")
            .finish()
    }
}

/// Full staged restore projection used only by confirmation and recovery.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageRestoreJob {
    summary: StorageRestoreJobSummary,
    document: Vec<u8>,
    capability_hash: String,
}

impl StorageRestoreJob {
    #[must_use]
    pub fn new(
        summary: StorageRestoreJobSummary,
        document: Vec<u8>,
        capability_hash: impl Into<String>,
    ) -> Self {
        Self {
            summary,
            document,
            capability_hash: capability_hash.into(),
        }
    }

    #[must_use]
    pub const fn summary(&self) -> &StorageRestoreJobSummary {
        &self.summary
    }

    #[must_use]
    pub fn capability_hash(&self) -> &str {
        &self.capability_hash
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageRestoreJobSummary, Vec<u8>, String) {
        (self.summary, self.document, self.capability_hash)
    }
}

impl fmt::Debug for StorageRestoreJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRestoreJob")
            .field("summary", &self.summary)
            .field("document_bytes", &self.document.len())
            .field("capability", &"[redacted]")
            .finish()
    }
}

/// Document-free projection used by capability-authenticated status reads.
#[derive(Clone, PartialEq)]
pub struct StorageRestoreStatus {
    summary: StorageRestoreJobSummary,
    capability_hash: String,
    validation_summary: Value,
}

impl StorageRestoreStatus {
    #[must_use]
    pub fn new(
        summary: StorageRestoreJobSummary,
        capability_hash: impl Into<String>,
        validation_summary: Value,
    ) -> Self {
        Self {
            summary,
            capability_hash: capability_hash.into(),
            validation_summary,
        }
    }

    #[must_use]
    pub fn capability_hash(&self) -> &str {
        &self.capability_hash
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageRestoreJobSummary, String, Value) {
        (self.summary, self.capability_hash, self.validation_summary)
    }
}

impl fmt::Debug for StorageRestoreStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRestoreStatus")
            .field("summary", &self.summary)
            .field("details", &"[redacted]")
            .finish()
    }
}

/// Metadata required for backend-owned restore provenance.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageRestoreDocumentMetadata {
    backup_version: i32,
    created_at: NaiveDateTime,
    source_version: String,
}

impl StorageRestoreDocumentMetadata {
    #[must_use]
    pub fn new(
        backup_version: i32,
        created_at: NaiveDateTime,
        source_version: impl Into<String>,
    ) -> Self {
        Self {
            backup_version,
            created_at,
            source_version: source_version.into(),
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, NaiveDateTime, String) {
        (self.backup_version, self.created_at, self.source_version)
    }
}

impl fmt::Debug for StorageRestoreDocumentMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRestoreDocumentMetadata")
            .field("backup_version", &self.backup_version)
            .field("created_at", &self.created_at)
            .field("source", &"[redacted]")
            .finish()
    }
}

/// Validated canonical sections submitted to the destructive backend apply.
#[derive(Clone, PartialEq)]
pub struct StorageRestoreDocument {
    metadata: StorageRestoreDocumentMetadata,
    snapshot: StorageBackupSnapshot,
}

impl StorageRestoreDocument {
    #[must_use]
    pub const fn new(
        metadata: StorageRestoreDocumentMetadata,
        snapshot: StorageBackupSnapshot,
    ) -> Self {
        Self { metadata, snapshot }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageRestoreDocumentMetadata, StorageBackupSnapshot) {
        (self.metadata, self.snapshot)
    }
}

impl fmt::Debug for StorageRestoreDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRestoreDocument")
            .field("metadata", &self.metadata)
            .field("snapshot", &self.snapshot)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageRestoreApply {
    job_id: RestoreJobId,
    document: StorageRestoreDocument,
}

impl StorageRestoreApply {
    #[must_use]
    pub const fn new(job_id: RestoreJobId, document: StorageRestoreDocument) -> Self {
        Self { job_id, document }
    }

    #[must_use]
    pub fn into_parts(self) -> (RestoreJobId, StorageRestoreDocument) {
        (self.job_id, self.document)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRestoreCompletion {
    started_at: NaiveDateTime,
    finished_at: NaiveDateTime,
}

impl StorageRestoreCompletion {
    #[must_use]
    pub const fn new(started_at: NaiveDateTime, finished_at: NaiveDateTime) -> Self {
        Self {
            started_at,
            finished_at,
        }
    }

    #[must_use]
    pub const fn into_parts(self) -> (NaiveDateTime, NaiveDateTime) {
        (self.started_at, self.finished_at)
    }
}

/// Sanitized failure persisted while atomically resuming normal operation.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageRestoreFailure {
    job_id: RestoreJobId,
    public_error: String,
}

impl StorageRestoreFailure {
    #[must_use]
    pub fn new(job_id: RestoreJobId, public_error: impl Into<String>) -> Self {
        Self {
            job_id,
            public_error: public_error.into(),
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (RestoreJobId, String) {
        (self.job_id, self.public_error)
    }
}

impl fmt::Debug for StorageRestoreFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageRestoreFailure")
            .field("job_id", &self.job_id)
            .field("public_error", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRestoreCoordinatorSnapshot {
    maintenance_state: MaintenanceState,
    restore_job_id: Option<RestoreJobId>,
    backend_now: NaiveDateTime,
}

impl StorageRestoreCoordinatorSnapshot {
    #[must_use]
    pub const fn new(
        maintenance_state: MaintenanceState,
        restore_job_id: Option<RestoreJobId>,
        backend_now: NaiveDateTime,
    ) -> Self {
        Self {
            maintenance_state,
            restore_job_id,
            backend_now,
        }
    }

    #[must_use]
    pub const fn maintenance_state(self) -> MaintenanceState {
        self.maintenance_state
    }

    #[must_use]
    pub const fn restore_job_id(self) -> Option<RestoreJobId> {
        self.restore_job_id
    }

    #[must_use]
    pub const fn backend_now(self) -> NaiveDateTime {
        self.backend_now
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRestoreInstance {
    instance_id: Uuid,
    maintenance_generation: i64,
    drained: bool,
}

impl StorageRestoreInstance {
    #[must_use]
    pub const fn new(instance_id: Uuid, maintenance_generation: i64, drained: bool) -> Self {
        Self {
            instance_id,
            maintenance_generation,
            drained,
        }
    }

    #[must_use]
    pub const fn instance_id(self) -> Uuid {
        self.instance_id
    }

    #[must_use]
    pub const fn maintenance_generation(self) -> i64 {
        self.maintenance_generation
    }

    #[must_use]
    pub const fn is_drained(self) -> bool {
        self.drained
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRestoreDrainState {
    generation: i64,
    instances: Vec<StorageRestoreInstance>,
}

impl StorageRestoreDrainState {
    #[must_use]
    pub const fn new(generation: i64, instances: Vec<StorageRestoreInstance>) -> Self {
        Self {
            generation,
            instances,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i64, Vec<StorageRestoreInstance>) {
        (self.generation, self.instances)
    }
}

/// Complete restore workflow required from every selectable backend.
///
/// Implementations own the durable staging lifecycle, the global maintenance
/// transition, coordinator membership, destructive snapshot replacement, and
/// restore provenance. Callers validate and decode the backup format; the
/// backend enforces atomicity and lifecycle preconditions. A backend is not
/// selectable unless it implements this entire contract.
#[async_trait]
pub trait RestoreStorage: Send + Sync {
    /// Durably stage a validated artifact and its capability proof.
    ///
    /// The returned job must reflect the backend's canonical timestamps and
    /// identifiers. Artifact bytes and capability material are sensitive and
    /// must not be emitted through logs or diagnostics.
    async fn stage_restore(
        &self,
        request: StorageRestoreStageCreate,
    ) -> Result<StorageRestoreJob, StorageError>;

    /// Load the complete staged artifact for confirmation or recovery.
    ///
    /// Missing jobs must be reported as [`crate::StorageErrorKind::NotFound`].
    async fn get_restore_job(
        &self,
        job_id: RestoreJobId,
    ) -> Result<StorageRestoreJob, StorageError>;

    /// Load a document-free status projection for capability-authenticated reads.
    ///
    /// Missing jobs must be reported as [`crate::StorageErrorKind::NotFound`].
    async fn get_restore_status(
        &self,
        job_id: RestoreJobId,
    ) -> Result<StorageRestoreStatus, StorageError>;

    /// Atomically expire a still-validated job and erase its staged document.
    ///
    /// Returns `true` only when this call performed the state transition.
    async fn expire_restore_stage(&self, job_id: RestoreJobId) -> Result<bool, StorageError>;

    /// Atomically confirm a validated job and enter global draining maintenance.
    ///
    /// Concurrent confirmation or an existing maintenance operation must fail
    /// with a conflict without partially changing either lifecycle.
    async fn start_restore_draining(
        &self,
        job_id: RestoreJobId,
    ) -> Result<NaiveDateTime, StorageError>;

    /// Replace all restorable state with the validated canonical snapshot.
    ///
    /// The backend must re-check that the job owns draining maintenance, apply
    /// the snapshot in one rollback-safe transaction, reset backend-owned
    /// derived state and identifiers, write success provenance, return to
    /// normal operation, and erase restore/coordinator staging records.
    async fn apply_restore(
        &self,
        request: StorageRestoreApply,
    ) -> Result<StorageRestoreCompletion, StorageError>;

    /// Atomically fail an active job, erase its document, and resume operation.
    async fn fail_restore_and_resume(
        &self,
        request: StorageRestoreFailure,
    ) -> Result<(), StorageError>;

    /// Read maintenance ownership and backend time from one consistent snapshot.
    async fn get_restore_coordinator_snapshot(
        &self,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError>;

    /// Recover a draining maintenance state that has no associated restore job.
    async fn resume_maintenance_without_restore(&self) -> Result<(), StorageError>;

    /// Resume normal operation when maintenance references a terminal restore.
    async fn resume_terminal_restore(&self, job_id: RestoreJobId) -> Result<(), StorageError>;

    /// Publish this process's coordinator heartbeat and return maintenance state.
    ///
    /// Implementations must observe the current maintenance generation before
    /// invoking `local_work_is_idle`, invoke it only while maintenance is not
    /// normal, and atomically persist the resulting drained state. When
    /// `expire_validated_jobs` is true, expired validated artifacts must be
    /// erased as part of the same coordinator operation.
    async fn tick_restore_coordinator(
        &self,
        instance_id: Uuid,
        local_work_is_idle: &(dyn Fn() -> bool + Send + Sync),
        expire_validated_jobs: bool,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError>;

    /// Return the current generation and live instances newer than the cutoff.
    async fn get_restore_drain_state(
        &self,
        heartbeat_cutoff: NaiveDateTime,
    ) -> Result<StorageRestoreDrainState, StorageError>;

    /// Remove this process's coordinator membership during shutdown.
    async fn remove_restore_instance(&self, instance_id: Uuid) -> Result<(), StorageError>;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn timestamp() -> NaiveDateTime {
        chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .expect("valid timestamp")
            .naive_utc()
    }

    #[test]
    fn restore_dtos_redact_documents_capabilities_and_identity() {
        let request = StorageRestoreStageCreate::new(
            StorageRestoreInitiator::new(PrincipalId::new(3).ok(), "secret-scope", "secret-name"),
            b"secret-document".to_vec(),
            StorageRestoreArtifactSummary::new(15, "secret-digest"),
            "secret-capability-hash",
            serde_json::json!({"secret-validation": true}),
            timestamp(),
        );
        let document = StorageRestoreDocument::new(
            StorageRestoreDocumentMetadata::new(4, timestamp(), "secret-version"),
            StorageBackupSnapshot::new(
                BTreeMap::from([(
                    "secret-section".to_string(),
                    vec![serde_json::json!({"secret-row": true})],
                )]),
                None,
            ),
        );

        let debug = format!("{request:?} {document:?}");
        for secret in [
            "secret-scope",
            "secret-name",
            "secret-document",
            "secret-digest",
            "secret-capability-hash",
            "secret-validation",
            "secret-version",
            "secret-section",
            "secret-row",
        ] {
            assert!(!debug.contains(secret));
        }
    }
}
