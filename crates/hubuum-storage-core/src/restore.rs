use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{LegacyIdentityCohort, MaintenanceState, PrincipalId, RestoreJobId};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{StorageError, StorageValidationError};

fn validate_sha256(value: &str, description: &'static str) -> Result<(), StorageValidationError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(StorageValidationError::invalid(format!(
            "{description} must contain exactly 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

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

    pub fn from_stored(value: &str) -> Result<Self, StorageValidationError> {
        match value {
            "validated" => Ok(Self::Validated),
            "confirmed" => Ok(Self::Confirmed),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "expired" => Ok(Self::Expired),
            _ => Err(StorageValidationError::invalid(format!(
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

/// Named restore-initiator components used by adapters and application mappers.
pub struct StorageRestoreInitiatorParts {
    principal_id: Option<PrincipalId>,
    identity_scope: String,
    name: String,
}

impl StorageRestoreInitiatorParts {
    #[must_use]
    pub const fn principal_id(&self) -> Option<PrincipalId> {
        self.principal_id
    }

    #[must_use]
    pub fn identity_scope(&self) -> &str {
        &self.identity_scope
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl StorageRestoreInitiator {
    pub fn try_new(
        principal_id: Option<PrincipalId>,
        identity_scope: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, StorageValidationError> {
        let identity_scope = identity_scope.into();
        let name = name.into();
        if identity_scope.trim().is_empty() {
            return Err(StorageValidationError::invalid(
                "Restore initiator identity scope must not be empty",
            ));
        }
        if name.trim().is_empty() {
            return Err(StorageValidationError::invalid(
                "Restore initiator name must not be empty",
            ));
        }
        Ok(Self {
            principal_id,
            identity_scope,
            name,
        })
    }

    #[must_use]
    pub fn into_parts(self) -> StorageRestoreInitiatorParts {
        StorageRestoreInitiatorParts {
            principal_id: self.principal_id,
            identity_scope: self.identity_scope,
            name: self.name,
        }
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

/// Named restore-artifact components used by adapters and application mappers.
pub struct StorageRestoreArtifactSummaryParts {
    byte_size: i64,
    sha256: String,
}

impl StorageRestoreArtifactSummaryParts {
    #[must_use]
    pub const fn byte_size(&self) -> i64 {
        self.byte_size
    }

    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

impl StorageRestoreArtifactSummary {
    pub fn try_new(
        byte_size: i64,
        sha256: impl Into<String>,
    ) -> Result<Self, StorageValidationError> {
        let sha256 = sha256.into();
        if byte_size < 0 {
            return Err(StorageValidationError::invalid(
                "Restore artifact byte size must not be negative",
            ));
        }
        validate_sha256(&sha256, "Restore artifact SHA-256 digest")?;
        Ok(Self { byte_size, sha256 })
    }

    #[must_use]
    pub fn into_parts(self) -> StorageRestoreArtifactSummaryParts {
        StorageRestoreArtifactSummaryParts {
            byte_size: self.byte_size,
            sha256: self.sha256,
        }
    }

    #[must_use]
    pub const fn byte_size(&self) -> i64 {
        self.byte_size
    }

    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
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
    expires_at: DateTime<Utc>,
    confirmed_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

/// Named lifecycle timestamps for a staged restore.
pub struct StorageRestoreTimestampsParts {
    expires_at: DateTime<Utc>,
    confirmed_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl StorageRestoreTimestampsParts {
    #[must_use]
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }

    #[must_use]
    pub const fn confirmed_at(&self) -> Option<DateTime<Utc>> {
        self.confirmed_at
    }

    #[must_use]
    pub const fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.finished_at
    }

    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

impl StorageRestoreTimestamps {
    pub fn try_new(
        expires_at: DateTime<Utc>,
        confirmed_at: Option<DateTime<Utc>>,
        finished_at: Option<DateTime<Utc>>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        if updated_at < created_at {
            return Err(StorageValidationError::invalid(
                "restore updated_at must not be earlier than created_at",
            ));
        }
        if confirmed_at.is_some_and(|value| value < created_at) {
            return Err(StorageValidationError::invalid(
                "restore confirmed_at must not be earlier than created_at",
            ));
        }
        if confirmed_at.is_some_and(|value| value > updated_at) {
            return Err(StorageValidationError::invalid(
                "restore confirmed_at must not be later than updated_at",
            ));
        }
        if finished_at.is_some_and(|value| value < created_at) {
            return Err(StorageValidationError::invalid(
                "restore finished_at must not be earlier than created_at",
            ));
        }
        if finished_at.is_some_and(|value| value > updated_at) {
            return Err(StorageValidationError::invalid(
                "restore finished_at must not be later than updated_at",
            ));
        }
        if confirmed_at
            .zip(finished_at)
            .is_some_and(|(confirmed, finished)| finished < confirmed)
        {
            return Err(StorageValidationError::invalid(
                "restore finished_at must not be earlier than confirmed_at",
            ));
        }
        Ok(Self {
            expires_at,
            confirmed_at,
            finished_at,
            created_at,
            updated_at,
        })
    }

    #[must_use]
    pub const fn expires_at(self) -> DateTime<Utc> {
        self.expires_at
    }

    #[must_use]
    pub const fn into_parts(self) -> StorageRestoreTimestampsParts {
        StorageRestoreTimestampsParts {
            expires_at: self.expires_at,
            confirmed_at: self.confirmed_at,
            finished_at: self.finished_at,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
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
    pub fn try_new(
        id: RestoreJobId,
        status: StorageRestoreJobStatus,
        initiator: StorageRestoreInitiator,
        artifact: StorageRestoreArtifactSummary,
        error: Option<String>,
        timestamps: StorageRestoreTimestamps,
    ) -> Result<Self, StorageValidationError> {
        let timestamps_valid = match status {
            StorageRestoreJobStatus::Validated | StorageRestoreJobStatus::Expired => {
                timestamps.confirmed_at.is_none()
                    && timestamps.finished_at.is_none()
                    && error.is_none()
            }
            StorageRestoreJobStatus::Confirmed => {
                timestamps.confirmed_at.is_some()
                    && timestamps.finished_at.is_none()
                    && error.is_none()
            }
            StorageRestoreJobStatus::Succeeded => {
                timestamps.confirmed_at.is_some()
                    && timestamps.finished_at.is_some()
                    && error.is_none()
            }
            StorageRestoreJobStatus::Failed => timestamps.finished_at.is_some() && error.is_some(),
        };
        if !timestamps_valid {
            return Err(StorageValidationError::invalid(
                "restore status, timestamps, and error are inconsistent",
            ));
        }
        Ok(Self {
            id,
            status,
            initiator,
            artifact,
            error,
            timestamps,
        })
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
    pub const fn artifact(&self) -> &StorageRestoreArtifactSummary {
        &self.artifact
    }

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

/// Validated staging request carrying exact immutable bytes without a heap copy.
#[derive(Clone)]
pub struct StorageRestoreStageArtifact {
    initiator: StorageRestoreInitiator,
    artifact: crate::StorageRestoreArtifact,
    capability_hash: String,
    validation_summary: Value,
    expires_at: DateTime<Utc>,
}
impl StorageRestoreStageArtifact {
    pub fn try_new(
        initiator: StorageRestoreInitiator,
        artifact: crate::StorageRestoreArtifact,
        capability_hash: String,
        validation_summary: Value,
        expires_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        validate_sha256(&capability_hash, "Restore capability hash")?;
        if !validation_summary.is_object() {
            return Err(StorageValidationError::invalid(
                "Restore validation summary must be a JSON object",
            ));
        }
        Ok(Self {
            initiator,
            artifact,
            capability_hash,
            validation_summary,
            expires_at,
        })
    }
    pub fn into_parts(
        self,
    ) -> (
        StorageRestoreInitiator,
        crate::StorageRestoreArtifact,
        String,
        Value,
        DateTime<Utc>,
    ) {
        (
            self.initiator,
            self.artifact,
            self.capability_hash,
            self.validation_summary,
            self.expires_at,
        )
    }
}

/// A staged artifact, or an explicitly erased terminal stage.
#[derive(Clone)]
pub struct StorageRestoreArtifactJob {
    summary: StorageRestoreJobSummary,
    artifact: Option<crate::StorageRestoreArtifact>,
    capability_hash: String,
}

/// Stable identity namespace and boundary of one confirmed restore stage.
/// Only a validated, confirmed stage can produce this value. It carries no
/// execution authority; the adapter must still enforce its transaction fence.
#[derive(Clone, Copy, Debug)]
pub struct StorageRestoreIdentityContext {
    job_id: RestoreJobId,
    cohort: LegacyIdentityCohort,
    confirmed_at: DateTime<Utc>,
}

impl StorageRestoreIdentityContext {
    #[must_use]
    pub const fn job_id(self) -> RestoreJobId {
        self.job_id
    }
    #[must_use]
    pub const fn cohort(self) -> LegacyIdentityCohort {
        self.cohort
    }
    #[must_use]
    pub const fn confirmed_at(self) -> DateTime<Utc> {
        self.confirmed_at
    }
}

impl StorageRestoreArtifactJob {
    pub fn identity_context(
        &self,
    ) -> Result<StorageRestoreIdentityContext, StorageValidationError> {
        if self.summary.status != StorageRestoreJobStatus::Confirmed {
            return Err(StorageValidationError::invalid(
                "Identity replay requires a confirmed restore stage",
            ));
        }
        let confirmed_at = self.summary.timestamps.confirmed_at.ok_or_else(|| {
            StorageValidationError::invalid("Confirmed restore stage has no boundary timestamp")
        })?;
        // The capability digest is already durable and unique to this stage.
        // Domain separation derives an independent namespace without storing a
        // second random value or exposing the capability itself in backup rows.
        let mut digest = Sha256::new();
        digest.update(b"hubuum.restore-identity-cohort.v1\0");
        digest.update(self.capability_hash.as_bytes());
        let mut bytes: [u8; 16] = digest.finalize()[..16].try_into().expect("digest prefix");
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Ok(StorageRestoreIdentityContext {
            job_id: self.summary.id,
            cohort: LegacyIdentityCohort::from_bytes(bytes).expect("derived RFC UUID"),
            confirmed_at,
        })
    }
    pub fn summary(&self) -> &StorageRestoreJobSummary {
        &self.summary
    }
    pub fn capability_hash(&self) -> &str {
        &self.capability_hash
    }
    pub fn try_new(
        summary: StorageRestoreJobSummary,
        artifact: Option<crate::StorageRestoreArtifact>,
        capability_hash: String,
    ) -> Result<Self, StorageValidationError> {
        validate_sha256(&capability_hash, "Restore capability hash")?;
        let terminal = matches!(
            summary.status,
            StorageRestoreJobStatus::Succeeded
                | StorageRestoreJobStatus::Failed
                | StorageRestoreJobStatus::Expired
        );
        if terminal != artifact.is_none()
            || artifact
                .as_ref()
                .is_some_and(|a| a.summary() != &summary.artifact)
        {
            return Err(StorageValidationError::invalid(
                "Restore artifact does not match its staged identity or status",
            ));
        }
        Ok(Self {
            summary,
            artifact,
            capability_hash,
        })
    }
    pub fn into_parts(
        self,
    ) -> (
        StorageRestoreJobSummary,
        Option<crate::StorageRestoreArtifact>,
        String,
    ) {
        (self.summary, self.artifact, self.capability_hash)
    }
}

impl fmt::Debug for StorageRestoreStageArtifact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageRestoreStageArtifact")
            .field("artifact", &self.artifact)
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for StorageRestoreArtifactJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageRestoreArtifactJob")
            .field("summary", &self.summary)
            .finish_non_exhaustive()
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
    pub fn summary(&self) -> &StorageRestoreJobSummary {
        &self.summary
    }
    pub fn try_new(
        summary: StorageRestoreJobSummary,
        capability_hash: impl Into<String>,
        validation_summary: Value,
    ) -> Result<Self, StorageValidationError> {
        let capability_hash = capability_hash.into();
        validate_sha256(&capability_hash, "Restore capability hash")?;
        if !validation_summary.is_object() {
            return Err(StorageValidationError::invalid(
                "Restore validation summary must be a JSON object",
            ));
        }
        Ok(Self {
            summary,
            capability_hash,
            validation_summary,
        })
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
    created_at: DateTime<Utc>,
    source_version: String,
}

impl StorageRestoreDocumentMetadata {
    #[must_use]
    pub fn new(
        backup_version: i32,
        created_at: DateTime<Utc>,
        source_version: impl Into<String>,
    ) -> Self {
        Self {
            backup_version,
            created_at,
            source_version: source_version.into(),
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, DateTime<Utc>, String) {
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

/// A validated, replayable restore prepared before entering the fenced transaction.
#[derive(Clone, Debug)]
pub struct StorageRestoreReplayApply {
    job_id: RestoreJobId,
    metadata: StorageRestoreDocumentMetadata,
    snapshot: crate::StorageIdentityReplay,
    source_includes_history: bool,
}
impl StorageRestoreReplayApply {
    pub fn at_restore_boundary(
        context: StorageRestoreIdentityContext,
        metadata: StorageRestoreDocumentMetadata,
        snapshot: crate::StorageBackupReplay,
    ) -> Result<Self, StorageError> {
        let source_includes_history = snapshot.includes_history();
        let upgrade = match metadata.backup_version {
            6..=10 => crate::StorageIdentityUpgrade::Legacy(context.cohort()),
            11..=12 => crate::StorageIdentityUpgrade::Native,
            _ => {
                return Err(StorageError::invalid_input(
                    "Unsupported logical backup version",
                ));
            }
        };
        let snapshot = snapshot
            .upgrade_logical_identities(upgrade)?
            .at_restore_boundary(context)?;
        Ok(Self {
            job_id: context.job_id(),
            metadata,
            snapshot,
            source_includes_history,
        })
    }
    pub fn into_parts(
        self,
    ) -> (
        RestoreJobId,
        StorageRestoreDocumentMetadata,
        crate::StorageIdentityReplay,
        bool,
    ) {
        (
            self.job_id,
            self.metadata,
            self.snapshot,
            self.source_includes_history,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRestoreCompletion {
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
}

impl StorageRestoreCompletion {
    pub fn try_new(
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        if finished_at < started_at {
            return Err(StorageValidationError::invalid(
                "Restore completion finished_at must not be earlier than started_at",
            ));
        }
        Ok(Self {
            started_at,
            finished_at,
        })
    }

    #[must_use]
    pub const fn into_parts(self) -> (DateTime<Utc>, DateTime<Utc>) {
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
    backend_now: DateTime<Utc>,
}

impl StorageRestoreCoordinatorSnapshot {
    #[must_use]
    pub const fn new(
        maintenance_state: MaintenanceState,
        restore_job_id: Option<RestoreJobId>,
        backend_now: DateTime<Utc>,
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
    pub const fn backend_now(self) -> DateTime<Utc> {
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
    pub fn try_new(
        instance_id: Uuid,
        maintenance_generation: i64,
        drained: bool,
    ) -> Result<Self, StorageValidationError> {
        if maintenance_generation < 0 {
            return Err(StorageValidationError::invalid(
                "restore instance maintenance generation must not be negative",
            ));
        }
        Ok(Self {
            instance_id,
            maintenance_generation,
            drained,
        })
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
    pub fn try_new(
        generation: i64,
        instances: Vec<StorageRestoreInstance>,
    ) -> Result<Self, StorageValidationError> {
        if generation < 0 {
            return Err(StorageValidationError::invalid(
                "restore drain generation must not be negative",
            ));
        }
        let mut instance_ids = std::collections::HashSet::with_capacity(instances.len());
        // Live instances acknowledge maintenance asynchronously. Keep their
        // observed generations so the executor can wait for acknowledgements.
        if instances
            .iter()
            .any(|instance| !instance_ids.insert(instance.instance_id))
        {
            return Err(StorageValidationError::invalid(
                "restore drain instances must have unique ids",
            ));
        }
        Ok(Self {
            generation,
            instances,
        })
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
    /// Return canonical identifiers and timestamps without exposing artifact bytes.
    async fn stage_restore_artifact(
        &self,
        request: StorageRestoreStageArtifact,
    ) -> Result<StorageRestoreStatus, StorageError>;

    /// Load exact staged bytes for confirmation or recovery through a replayable reader.
    /// Missing jobs must report [`crate::StorageErrorKind::NotFound`].
    async fn get_restore_artifact(
        &self,
        job_id: RestoreJobId,
    ) -> Result<StorageRestoreArtifactJob, StorageError>;

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
        request: crate::StorageRestoreConfirmation,
    ) -> Result<DateTime<Utc>, StorageError>;

    /// Replace all restorable state with the validated canonical snapshot.
    ///
    /// The backend must re-check that the job owns draining maintenance, apply
    /// the snapshot in one rollback-safe transaction, reset backend-owned
    /// derived state and identifiers, write success provenance, return to
    /// normal operation, erase coordinator staging records, and retain a
    /// document-free terminal receipt for capability-authenticated polling.
    /// Prepared history is mandatory even for a history-free source: the
    /// shared plan already contains its new current temporal snapshots. Apply
    /// must preserve their revisions and validity boundaries without producing
    /// additional resource mutation events. A subsequent history-inclusive
    /// snapshot must satisfy the same logical validation contract.
    /// Apply replayable rows. Resident backends may materialize their state;
    /// persistent backends replay the rows while preserving the same fencing.
    async fn apply_restore_replay(
        &self,
        request: StorageRestoreReplayApply,
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
        heartbeat_cutoff: DateTime<Utc>,
    ) -> Result<StorageRestoreDrainState, StorageError>;

    /// Remove this process's coordinator membership during shutdown.
    async fn remove_restore_instance(&self, instance_id: Uuid) -> Result<(), StorageError>;
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::{
        StorageBackupBudget, StorageBackupReplay, StorageBackupRow, StorageBackupSnapshot,
        StorageBackupStateSection,
    };

    fn timestamp() -> DateTime<Utc> {
        chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp")
    }

    #[rstest]
    #[case::obsolete(5, false)]
    #[case::six(6, true)]
    #[case::seven(7, true)]
    #[case::eight(8, true)]
    #[case::nine(9, true)]
    #[case::ten(10, true)]
    #[case::native_identities(11, true)]
    #[case::portable_artifacts(12, true)]
    #[case::future(13, false)]
    fn restore_boundary_accepts_supported_backup_versions(
        #[case] version: i32,
        #[case] accepted: bool,
    ) {
        let snapshot = StorageBackupSnapshot::try_new(
            crate::backup_snapshot::with_test_schema_sections(
                StorageBackupStateSection::ALL
                    .iter()
                    .copied()
                    .map(|section| (section, Vec::new()))
                    .collect(),
            ),
            None,
        )
        .unwrap();
        let replay = StorageBackupReplay::from_snapshot(
            snapshot,
            StorageBackupBudget::new(1024 * 1024, 1000).unwrap(),
        )
        .unwrap();
        let result = StorageRestoreReplayApply::at_restore_boundary(
            StorageRestoreIdentityContext {
                job_id: RestoreJobId::new(1).unwrap(),
                cohort: LegacyIdentityCohort::generate(),
                confirmed_at: timestamp(),
            },
            StorageRestoreDocumentMetadata::new(version, timestamp(), "test"),
            replay,
        );

        assert_eq!(result.is_ok(), accepted, "{result:?}");
    }

    #[rstest]
    #[case::not_yet_acknowledged(0, false)]
    #[case::previously_drained(1, true)]
    #[case::current(2, true)]
    #[case::snapshot_advanced(3, false)]
    fn restore_drain_state_preserves_instance_acknowledgements(
        #[case] instance_generation: i64,
        #[case] drained: bool,
    ) {
        let instance =
            StorageRestoreInstance::try_new(Uuid::new_v4(), instance_generation, drained).unwrap();
        let state = StorageRestoreDrainState::try_new(2, vec![instance]).unwrap();

        assert_eq!(state.into_parts(), (2, vec![instance]));
    }

    #[rstest]
    #[case::same_generation(2)]
    #[case::different_generation(1)]
    fn restore_drain_state_rejects_duplicate_instances(#[case] other_generation: i64) {
        let instance_id = Uuid::new_v4();
        let instances = vec![
            StorageRestoreInstance::try_new(instance_id, 2, true).unwrap(),
            StorageRestoreInstance::try_new(instance_id, other_generation, false).unwrap(),
        ];

        assert!(StorageRestoreDrainState::try_new(2, instances).is_err());
    }

    #[test]
    fn restore_drain_state_rejects_negative_generation() {
        assert!(StorageRestoreDrainState::try_new(-1, vec![]).is_err());
    }

    #[test]
    fn restore_dtos_redact_documents_capabilities_and_identity() {
        let document_bytes = b"secret-document".to_vec();
        let request = StorageRestoreStageArtifact::try_new(
            StorageRestoreInitiator::try_new(
                PrincipalId::new(3).ok(),
                "secret-scope",
                "secret-name",
            )
            .unwrap(),
            crate::StorageRestoreArtifact::from_reader(
                document_bytes.as_slice(),
                document_bytes.len(),
            )
            .unwrap(),
            "a".repeat(64),
            serde_json::json!({"secret-validation": true}),
            timestamp(),
        )
        .unwrap();
        let document = StorageRestoreReplayApply::at_restore_boundary(
            StorageRestoreIdentityContext { job_id: RestoreJobId::new(1).unwrap(), cohort: LegacyIdentityCohort::generate(), confirmed_at: timestamp() },
            StorageRestoreDocumentMetadata::new(10, timestamp(), "secret-version"),
            crate::StorageBackupReplay::from_snapshot(
                crate::StorageBackupSnapshot::try_new(
                    crate::backup_snapshot::with_test_schema_sections(
                        StorageBackupStateSection::ALL
                            .iter()
                            .copied()
                            .map(|section| {
                                let rows = if section == StorageBackupStateSection::Classes {
                                    vec![
                                StorageBackupRow::try_from_value(
                                    serde_json::json!({"id": 1, "revision": 7, "secret-row": true, "created_at": "2026-01-01T00:00:00Z"}),
                                )
                                .expect("object backup row"),
                            ]
                                } else {
                                    Vec::new()
                                };
                                (section, rows)
                            })
                            .collect(),
                    ),
                    None,
                )
                .expect("complete backup snapshot"),
                crate::StorageBackupBudget::new(16 * 1024 * 1024, 10000).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();

        let debug = format!("{request:?} {document:?}");
        for secret in [
            "secret-scope",
            "secret-name",
            "secret-document",
            "secret-digest",
            "secret-capability-hash",
            "secret-validation",
            "secret-version",
            "secret-row",
        ] {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn restore_initiators_reject_blank_identity_fields() {
        let error = StorageRestoreInitiator::try_new(None, " ", "operator").unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn restore_artifacts_reject_malformed_sha256_digests() {
        let error = StorageRestoreArtifactSummary::try_new(15, "not-a-digest").unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn restore_timestamps_reject_updates_before_creation() {
        let error = StorageRestoreTimestamps::try_new(
            timestamp(),
            None,
            None,
            timestamp(),
            timestamp() - chrono::Duration::seconds(1),
        )
        .unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn restore_timestamps_reject_confirmation_after_the_last_row_update() {
        let error = StorageRestoreTimestamps::try_new(
            timestamp() + chrono::Duration::hours(1),
            Some(timestamp() + chrono::Duration::seconds(1)),
            None,
            timestamp(),
            timestamp(),
        )
        .unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn restore_timestamps_reject_finish_after_the_last_row_update() {
        let error = StorageRestoreTimestamps::try_new(
            timestamp() + chrono::Duration::hours(1),
            None,
            Some(timestamp() + chrono::Duration::seconds(1)),
            timestamp(),
            timestamp(),
        )
        .unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn restore_completion_rejects_finish_before_start() {
        let error = StorageRestoreCompletion::try_new(
            timestamp(),
            timestamp() - chrono::Duration::seconds(1),
        )
        .unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }
}
