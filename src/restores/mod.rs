mod maintenance;
pub(crate) mod replay;
use hubuum_storage_core::{
    StorageBackupReplay, StorageRestoreArtifact, StorageRestoreArtifactJob,
    StorageRestoreReplayApply, StorageRestoreStageArtifact,
};
pub use maintenance::MaintenanceActivityGuard;
use maintenance::active_maintenance_work;

use hubuum_domain::{JsonSchemaLimits, PrincipalId};
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::Once;
use std::time::{Duration as StdDuration, Instant};

use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::errors::ApiError;
use crate::lifecycle::spawn_background_worker;
use crate::models::retention::FutureRetention;
use crate::models::{
    BACKUP_MANIFEST_EXCLUSIONS, BackupDocument, MaintenanceState, RESTORE_CONFIRMATION_PHRASE,
    RestoreConfirmRequest, RestoreJobID, RestoreJobStatus, RestoreStageRequest,
    RestoreStageResponse,
};
use crate::services::identity::resolve_identity_scope_name as load_identity_scope_name;
use crate::storage::storage_handle;
use crate::storage::{
    OperationalStateStorage, RestoreStorage, StorageContext, StorageRestoreCompletion,
    StorageRestoreCoordinatorSnapshot, StorageRestoreDocumentMetadata, StorageRestoreFailure,
    StorageRestoreInitiator, StorageRestoreJobStatus, StorageRestoreJobSummary,
    StorageRestoreStatus,
};

static RESTORE_COORDINATOR: Once = Once::new();
const RESTORE_DRAIN_TIMEOUT_SECONDS: u64 = 30;
const RESTORE_STAGE_EXPIRY_INTERVAL_SECONDS: u64 = 60;
const MISSING_RESTORE_CAPABILITY_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
// Compatibility recovery for confirmations created by older in-process
// coordinators retains their original grace period. The dedicated executor
// does not wait for this threshold.
const RESTORE_RECONCILIATION_GRACE_SECONDS: i64 = 60;
const BACKUP_VERIFICATION_REPORT_VERSION: i32 = 1;
const MAX_BACKUP_SOURCE_VERSION_BYTES: usize = 128;

/// Sanitized, versioned evidence produced by offline backup verification.
#[derive(Clone, Debug, Serialize)]
pub struct BackupVerificationReport {
    report_version: i32,
    result: &'static str,
    mode: &'static str,
    verified_at: DateTime<Utc>,
    verifier_version: &'static str,
    backup_version: i32,
    source_version: String,
    created_at: DateTime<Utc>,
    sha256: String,
    byte_size: u64,
    includes_history: bool,
    total_items: i64,
    canonical_state_sha256: String,
    section_counts: BTreeMap<String, i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    restore_test: Option<BackupRestoreTestReport>,
}

#[derive(Clone, Debug, Serialize)]
struct BackupRestoreTestReport {
    target_version: &'static str,
    migrations_applied: usize,
    restore_duration_ms: u64,
    storage_ready: bool,
    state_matches_source: bool,
    history_matches_source: bool,
    target_cleanup: &'static str,
}

impl BackupVerificationReport {
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    #[must_use]
    pub const fn byte_size(&self) -> u64 {
        self.byte_size
    }

    #[must_use]
    pub const fn total_items(&self) -> i64 {
        self.total_items
    }

    #[must_use]
    pub fn source_version(&self) -> &str {
        &self.source_version
    }

    #[must_use]
    pub const fn backup_version(&self) -> i32 {
        self.backup_version
    }

    #[must_use]
    pub const fn includes_history(&self) -> bool {
        self.includes_history
    }

    pub fn record_isolated_restore(
        &mut self,
        migrations_applied: usize,
        restore_duration: StdDuration,
        target_cleanup: &'static str,
    ) {
        self.mode = "isolated_restore";
        self.verified_at = Utc::now();
        self.restore_test = Some(BackupRestoreTestReport {
            target_version: env!("CARGO_PKG_VERSION"),
            migrations_applied,
            restore_duration_ms: u64::try_from(restore_duration.as_millis()).unwrap_or(u64::MAX),
            storage_ready: true,
            state_matches_source: true,
            history_matches_source: true,
            target_cleanup,
        });
    }
}

struct RestoreJobSummaryData {
    id: RestoreJobID,
    status: RestoreJobStatus,
    requested_by: Option<PrincipalId>,
    requested_by_identity_scope: String,
    requested_by_name: String,
    byte_size: i64,
    sha256: String,
    error: Option<String>,
    expires_at: NaiveDateTime,
    confirmed_at: Option<NaiveDateTime>,
    finished_at: Option<NaiveDateTime>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

struct RestoreJobData {
    summary: RestoreJobSummaryData,
    document: Option<StorageRestoreArtifact>,
    capability_hash: String,
}

struct RestoreStatusData {
    summary: RestoreJobSummaryData,
    capability_hash: String,
    validation_summary: Value,
}

fn restore_status_from_storage(status: StorageRestoreJobStatus) -> RestoreJobStatus {
    match status {
        StorageRestoreJobStatus::Validated => RestoreJobStatus::Validated,
        StorageRestoreJobStatus::Confirmed => RestoreJobStatus::Confirmed,
        StorageRestoreJobStatus::Succeeded => RestoreJobStatus::Succeeded,
        StorageRestoreJobStatus::Failed => RestoreJobStatus::Failed,
        StorageRestoreJobStatus::Expired => RestoreJobStatus::Expired,
    }
}

fn restore_summary_from_storage(summary: StorageRestoreJobSummary) -> RestoreJobSummaryData {
    let (id, status, initiator, artifact, error, timestamps) = summary.into_parts();
    let initiator = initiator.into_parts();
    let requested_by = initiator.principal_id();
    let requested_by_identity_scope = initiator.identity_scope().to_owned();
    let requested_by_name = initiator.name().to_owned();
    let artifact = artifact.into_parts();
    let byte_size = artifact.byte_size();
    let sha256 = artifact.sha256().to_owned();
    let timestamps = timestamps.into_parts();
    let expires_at = timestamps.expires_at();
    let confirmed_at = timestamps.confirmed_at();
    let finished_at = timestamps.finished_at();
    let created_at = timestamps.created_at();
    let updated_at = timestamps.updated_at();
    RestoreJobSummaryData {
        id,
        status: restore_status_from_storage(status),
        requested_by,
        requested_by_identity_scope,
        requested_by_name,
        byte_size,
        sha256,
        error,
        expires_at: expires_at.naive_utc(),
        confirmed_at: confirmed_at.map(|timestamp| timestamp.naive_utc()),
        finished_at: finished_at.map(|timestamp| timestamp.naive_utc()),
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
    }
}

fn restore_job_from_storage(job: StorageRestoreArtifactJob) -> RestoreJobData {
    let (summary, document, capability_hash) = job.into_parts();
    RestoreJobData {
        summary: restore_summary_from_storage(summary),
        document,
        capability_hash,
    }
}

fn restore_status_data_from_storage(status: StorageRestoreStatus) -> RestoreStatusData {
    let (summary, capability_hash, validation_summary) = status.into_parts();
    RestoreStatusData {
        summary: restore_summary_from_storage(summary),
        capability_hash,
        validation_summary,
    }
}
fn confirmation_is_stale(confirmed_at: NaiveDateTime, now: NaiveDateTime) -> bool {
    confirmed_at <= now - Duration::seconds(RESTORE_RECONCILIATION_GRACE_SECONDS)
}

#[derive(Clone, Debug)]
pub struct RestoreSettings {
    stage_retention: FutureRetention,
    max_upload_bytes: usize,
}

impl RestoreSettings {
    pub fn new(stage_retention_minutes: i64, max_upload_bytes: usize) -> Result<Self, String> {
        let stage_retention =
            FutureRetention::from_minutes(stage_retention_minutes, "restore stage retention")?;
        if max_upload_bytes == 0 {
            return Err("restore upload size limit must be greater than zero".to_string());
        }
        Ok(Self {
            stage_retention,
            max_upload_bytes,
        })
    }

    fn stage_expires_at(&self, now: NaiveDateTime) -> Result<NaiveDateTime, ApiError> {
        self.stage_retention
            .expires_at(now)
            .map_err(ApiError::BadRequest)
    }

    pub fn max_upload_bytes(&self) -> usize {
        self.max_upload_bytes
    }

    pub fn stage_retention_minutes(&self) -> i64 {
        self.stage_retention.minutes()
    }
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Hash canonical JSON without retaining a second serialized document.
fn serialized_sha256(value: &impl Serialize) -> Result<String, ApiError> {
    struct HashWriter(Sha256);
    impl Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = HashWriter(Sha256::new());
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer
        .0
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn validate_manifest_counts(
    manifest: &crate::models::BackupManifest,
    counts: &BTreeMap<String, i64>,
) -> Result<(), ApiError> {
    if manifest.item_counts != *counts {
        let mismatched_section = counts
            .keys()
            .chain(manifest.item_counts.keys())
            .find(|section| counts.get(*section) != manifest.item_counts.get(*section))
            .map(String::as_str)
            .unwrap_or("unknown");
        return Err(ApiError::BadRequest(format!(
            "Full backup manifest count for section '{mismatched_section}' does not match the document"
        )));
    }
    let exclusions = BACKUP_MANIFEST_EXCLUSIONS
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    if manifest.exclusions != exclusions {
        return Err(ApiError::BadRequest(
            "Full backup manifest exclusions do not match backup version 5".to_string(),
        ));
    }
    Ok(())
}

fn validate_backup_metadata(document: &BackupDocument) -> Result<(), ApiError> {
    if document.source_version.trim().is_empty()
        || document.source_version.len() > MAX_BACKUP_SOURCE_VERSION_BYTES
    {
        return Err(ApiError::BadRequest(format!(
            "Full backup source_version must contain between 1 and {MAX_BACKUP_SOURCE_VERSION_BYTES} bytes"
        )));
    }
    if document.created_at > Utc::now() + Duration::minutes(5) {
        return Err(ApiError::BadRequest(
            "Full backup creation timestamp is unreasonably far in the future".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn verify_backup_reader(
    reader: impl std::io::Read,
    max_document_bytes: usize,
    schema_limits: JsonSchemaLimits,
) -> Result<BackupVerificationReport, ApiError> {
    let (document, sha256, byte_size) = replay::parse(reader, max_document_bytes, schema_limits)?;
    let summary = document.summary()?;
    Ok(BackupVerificationReport {
        report_version: BACKUP_VERIFICATION_REPORT_VERSION,
        result: "passed",
        mode: "format_only",
        verified_at: Utc::now(),
        verifier_version: env!("CARGO_PKG_VERSION"),
        backup_version: summary.backup_version,
        source_version: summary.source_version,
        created_at: document.metadata.created_at,
        sha256,
        byte_size: byte_size as u64,
        includes_history: summary.includes_history,
        total_items: summary.total_items,
        canonical_state_sha256: document.state_sha256()?,
        section_counts: document.metadata.manifest.item_counts.clone(),
        restore_test: None,
    })
}

fn capability_matches(capability_hash: &str, capability: &str) -> bool {
    let supplied = sha256(capability.as_bytes());
    let expected = capability_hash.as_bytes();
    let supplied = supplied.as_bytes();
    expected.len() == supplied.len()
        && expected
            .iter()
            .zip(supplied)
            .fold(0_u8, |difference, (left, right)| {
                difference | (left ^ right)
            })
            == 0
}

fn restore_capability_matches(capability_hash: Option<&str>, capability: &str) -> bool {
    capability_matches(
        capability_hash.unwrap_or(MISSING_RESTORE_CAPABILITY_HASH),
        capability,
    )
}

fn invalid_restore_capability() -> ApiError {
    ApiError::Forbidden("Restore capability is invalid".to_string())
}

pub async fn stage_restore(
    pool: &impl crate::storage::StorageContext,
    settings: &RestoreSettings,
    request: RestoreStageRequest,
) -> Result<RestoreStageResponse, ApiError> {
    let (initiator, artifact) = request.into_parts();
    let (document, _, _) = replay::parse(
        artifact.reader(),
        settings.max_upload_bytes(),
        storage_handle(pool).schema_limits(),
    )?;
    let validation = document.summary()?;
    drop(document);
    let capability = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let capability_hash = sha256(capability.as_bytes());
    let expires_at = settings.stage_expires_at(Utc::now().naive_utc())?;
    let validation_json = serde_json::to_value(&validation)?;
    let (requested_by, requested_by_identity_scope, requested_by_name) = initiator.into_parts();
    let initiator = StorageRestoreInitiator::try_new(
        requested_by.map(|id| {
            hubuum_domain::PrincipalId::new(id)
                .expect("validated restore initiator id must be positive")
        }),
        requested_by_identity_scope,
        requested_by_name,
    )
    .map_err(|error| ApiError::from(error.into_request_error()))?;
    let request = StorageRestoreStageArtifact::try_new(
        initiator,
        artifact,
        capability_hash,
        validation_json,
        expires_at.and_utc(),
    )
    .map_err(|e| ApiError::from(e.into_request_error()))?;
    let (summary, _, _) = storage_handle(pool)
        .stage_restore_artifact(request)
        .await?
        .into_parts();
    let job = restore_summary_from_storage(summary);

    Ok(RestoreStageResponse {
        id: job.id.id(),
        status: RestoreJobStatus::Validated,
        requested_by: job.requested_by.map(PrincipalId::id),
        requested_by_identity_scope: job.requested_by_identity_scope,
        requested_by_name: job.requested_by_name,
        sha256: job.sha256,
        byte_size: job.byte_size,
        expires_at: job.expires_at,
        error: job.error,
        confirmed_at: job.confirmed_at,
        started_at: None,
        finished_at: job.finished_at,
        created_at: job.created_at,
        updated_at: job.updated_at,
        validation,
        restore_capability: Some(capability),
    })
}

async fn load_restore_job(
    pool: &impl crate::storage::StorageContext,
    job_id: RestoreJobID,
) -> Result<RestoreJobData, ApiError> {
    storage_handle(pool)
        .get_restore_artifact(job_id)
        .await
        .map(restore_job_from_storage)
        .map_err(Into::into)
}

pub async fn restore_status(
    pool: &impl crate::storage::StorageContext,
    job_id: RestoreJobID,
    capability: &str,
) -> Result<RestoreStageResponse, ApiError> {
    let job = match storage_handle(pool).get_restore_status(job_id).await {
        Ok(job) => Some(restore_status_data_from_storage(job)),
        Err(error) if error.kind() == crate::storage::StorageErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let capability_valid = restore_capability_matches(
        job.as_ref().map(|job| job.capability_hash.as_str()),
        capability,
    );
    let Some(job) = job else {
        tracing::warn!(
            message = "Restore capability rejected",
            restore_job_id = job_id.id(),
            reason = "restore stage not found"
        );
        return Err(invalid_restore_capability());
    };
    if !capability_valid {
        tracing::warn!(
            message = "Restore capability rejected",
            restore_job_id = job_id.id(),
            reason = "capability mismatch"
        );
        return Err(invalid_restore_capability());
    }
    let validation = serde_json::from_value(job.validation_summary)?;
    let job = job.summary;
    let status = job.status;
    Ok(RestoreStageResponse {
        id: job.id.id(),
        status,
        requested_by: job.requested_by.map(PrincipalId::id),
        requested_by_identity_scope: job.requested_by_identity_scope,
        requested_by_name: job.requested_by_name,
        sha256: job.sha256,
        byte_size: job.byte_size,
        expires_at: job.expires_at,
        error: job.error,
        confirmed_at: job.confirmed_at,
        started_at: None,
        finished_at: job.finished_at,
        created_at: job.created_at,
        updated_at: job.updated_at,
        validation,
        restore_capability: None,
    })
}

async fn apply_restore(
    pool: &impl crate::storage::StorageContext,
    job_id: RestoreJobID,
    metadata: StorageRestoreDocumentMetadata,
    snapshot: StorageBackupReplay,
) -> Result<StorageRestoreCompletion, ApiError> {
    let request =
        StorageRestoreReplayApply::at_restore_boundary(job_id, metadata, snapshot, Utc::now())?;
    storage_handle(pool)
        .apply_restore_replay(request)
        .await
        .map_err(Into::into)
}

async fn fail_restore_and_resume(
    pool: &impl crate::storage::StorageContext,
    job_id: RestoreJobID,
    error: &ApiError,
) -> Result<(), ApiError> {
    tracing::error!(message = "Restore failed", restore_job_id = job_id.id(), error = %error);
    let stored_error = restore_error_for_storage(error);
    storage_handle(pool)
        .fail_restore_and_resume(StorageRestoreFailure::new(job_id, stored_error))
        .await
        .map_err(Into::into)
}

fn restore_error_for_storage(error: &ApiError) -> String {
    error.public_message().to_string()
}

pub async fn confirm_restore(
    pool: &impl crate::storage::StorageContext,
    job_id: RestoreJobID,
    confirmation: &RestoreConfirmRequest,
) -> Result<RestoreStageResponse, ApiError> {
    confirm_restore_with_approval(pool, job_id, confirmation, None).await
}

pub(crate) async fn confirm_restore_with_approval(
    pool: &impl crate::storage::StorageContext,
    job_id: RestoreJobID,
    confirmation: &RestoreConfirmRequest,
    approval: Option<hubuum_storage_core::StorageCredentialUse>,
) -> Result<RestoreStageResponse, ApiError> {
    let job = match load_restore_job(pool, job_id).await {
        Ok(job) => Some(job),
        Err(ApiError::NotFound(_)) => None,
        Err(error) => return Err(error),
    };
    let capability_valid = restore_capability_matches(
        job.as_ref().map(|job| job.capability_hash.as_str()),
        &confirmation.restore_capability,
    );
    let Some(job) = job else {
        tracing::warn!(
            message = "Restore capability rejected",
            restore_job_id = job_id.id(),
            reason = "restore stage not found"
        );
        return Err(invalid_restore_capability());
    };
    if !capability_valid {
        tracing::warn!(
            message = "Restore capability rejected",
            restore_job_id = job_id.id(),
            reason = "capability mismatch"
        );
        return Err(invalid_restore_capability());
    }
    let RestoreJobData {
        summary: job,
        document: document_bytes,
        capability_hash: _,
    } = job;
    if confirmation.sha256 != job.sha256 {
        return Err(ApiError::Conflict(
            "Restore SHA-256 does not match the staged document".to_string(),
        ));
    }
    if confirmation.confirmation != RESTORE_CONFIRMATION_PHRASE {
        return Err(ApiError::BadRequest(format!(
            "Restore confirmation must exactly equal '{RESTORE_CONFIRMATION_PHRASE}'"
        )));
    }
    if job.status != RestoreJobStatus::Validated {
        return Err(ApiError::Conflict(format!(
            "Restore stage cannot be confirmed from status '{}'",
            job.status.as_str()
        )));
    }
    if job.expires_at <= Utc::now().naive_utc() {
        let changed = storage_handle(pool).expire_restore_stage(job.id).await?;
        if !changed {
            return Err(ApiError::Conflict(
                "Restore stage changed status concurrently".to_string(),
            ));
        }
        return Err(ApiError::Gone("Restore stage has expired".to_string()));
    }
    let (document, _, _) = replay::parse(
        document_bytes
            .as_ref()
            .ok_or_else(|| {
                ApiError::InternalServerError("Active restore artifact is missing".into())
            })?
            .reader(),
        usize::try_from(job.byte_size).map_err(|_| {
            ApiError::PayloadTooLarge("Restore artifact exceeds address space".into())
        })?,
        storage_handle(pool).schema_limits(),
    )?;
    let validation = document.summary()?;
    drop(document);
    // Confirmation commits only the maintenance transition. A separately
    // deployed executor owns the privileged destructive transaction, so the
    // API and worker processes never need a migration credential.
    let job_id = job.id;
    let mut request = hubuum_storage_core::StorageRestoreConfirmation::from(job_id);
    if let Some(approval) = approval {
        request = request.with_approval(approval);
    }
    let confirmed_at = storage_handle(pool).start_restore_draining(request).await?;
    Ok(RestoreStageResponse {
        id: job.id.id(),
        status: RestoreJobStatus::Confirmed,
        requested_by: job.requested_by.map(PrincipalId::id),
        requested_by_identity_scope: job.requested_by_identity_scope,
        requested_by_name: job.requested_by_name,
        sha256: job.sha256,
        byte_size: job.byte_size,
        expires_at: job.expires_at,
        error: None,
        confirmed_at: Some(confirmed_at.naive_utc()),
        started_at: None,
        finished_at: None,
        created_at: job.created_at,
        updated_at: confirmed_at.naive_utc(),
        validation,
        restore_capability: None,
    })
}

/// Resume a restore whose committed maintenance transition survived a process
/// restart. The destructive transaction is guarded by an advisory lock and
/// re-checks the job/maintenance state. This compatibility recovery entrypoint
/// retains the grace period used by older in-process coordinators.
pub async fn reconcile_interrupted_restore(
    pool: &impl crate::storage::StorageContext,
) -> Result<(), ApiError> {
    let snapshot = storage_handle(pool)
        .get_restore_coordinator_snapshot()
        .await?;
    reconcile_restore_from_snapshot(pool, snapshot, true)
        .await
        .map(|_| ())
}

/// Apply one confirmed restore, if present, using an isolated privileged
/// storage handle. The caller is expected to poll this operation from the
/// dedicated restore-executor workload.
pub async fn execute_confirmed_restore(
    pool: &impl crate::storage::StorageContext,
) -> Result<bool, ApiError> {
    let snapshot = storage_handle(pool)
        .get_restore_coordinator_snapshot()
        .await?;
    reconcile_restore_from_snapshot(pool, snapshot, false).await
}

async fn reconcile_restore_from_snapshot(
    pool: &impl crate::storage::StorageContext,
    snapshot: StorageRestoreCoordinatorSnapshot,
    require_stale_confirmation: bool,
) -> Result<bool, ApiError> {
    let maintenance_state = snapshot.maintenance_state();
    if maintenance_state.is_normal() {
        return Ok(false);
    }
    if maintenance_state != MaintenanceState::Draining {
        return Err(ApiError::InternalServerError(format!(
            "Unknown maintenance state '{maintenance_state}'"
        )));
    }
    let Some(job_id) = snapshot.restore_job_id() else {
        let error = ApiError::InternalServerError(format!(
            "Maintenance state '{maintenance_state}' has no restore job"
        ));
        storage_handle(pool)
            .resume_maintenance_without_restore()
            .await?;
        return Err(error);
    };
    let job = match storage_handle(pool).get_restore_artifact(job_id).await {
        Ok(job) => restore_job_from_storage(job),
        Err(error) => {
            let error = ApiError::from(error);
            fail_restore_and_resume(pool, job_id, &error).await?;
            return Err(error);
        }
    };
    let RestoreJobData {
        summary: job,
        document: document_bytes,
        capability_hash: _,
    } = job;
    if matches!(
        job.status,
        RestoreJobStatus::Failed | RestoreJobStatus::Expired
    ) {
        storage_handle(pool).resume_terminal_restore(job_id).await?;
        return Ok(false);
    }
    if job.status != RestoreJobStatus::Confirmed {
        let error = ApiError::Conflict(format!(
            "Maintenance references restore stage {job_id} in status '{}'",
            job.status.as_str()
        ));
        fail_restore_and_resume(pool, job_id, &error).await?;
        return Err(error);
    }
    let Some(confirmed_at) = job.confirmed_at else {
        let error = ApiError::Conflict(format!(
            "Confirmed restore stage {job_id} has no confirmation timestamp"
        ));
        fail_restore_and_resume(pool, job_id, &error).await?;
        return Err(error);
    };
    if require_stale_confirmation
        && !confirmation_is_stale(confirmed_at, snapshot.backend_now().naive_utc())
    {
        return Ok(false);
    }

    let (document, _, _) = match replay::parse(
        document_bytes
            .as_ref()
            .ok_or_else(|| {
                ApiError::InternalServerError("Active restore artifact is missing".into())
            })?
            .reader(),
        usize::try_from(job.byte_size).map_err(|_| {
            ApiError::PayloadTooLarge("Restore artifact exceeds address space".into())
        })?,
        storage_handle(pool).schema_limits(),
    ) {
        Ok(document) => document,
        Err(error) => {
            fail_restore_and_resume(pool, job_id, &error).await?;
            return Err(error);
        }
    };
    drop(document_bytes);
    let metadata = StorageRestoreDocumentMetadata::new(
        document.metadata.backup_version,
        document.metadata.created_at,
        document.metadata.source_version,
    );
    let snapshot = document.snapshot;
    if let Err(error) = wait_for_instances_drained(pool).await {
        fail_restore_and_resume(pool, job_id, &error).await?;
        return Err(error);
    }
    if let Err(error) = apply_restore(pool, job_id, metadata, snapshot).await {
        fail_restore_and_resume(pool, job_id, &error).await?;
        return Err(error);
    }
    Ok(true)
}

async fn heartbeat_instance(
    pool: &impl crate::storage::StorageContext,
    instance_id: Uuid,
    expire_validated_jobs: bool,
) -> Result<StorageRestoreCoordinatorSnapshot, ApiError> {
    let local_work_is_idle = || active_maintenance_work() == 0;
    storage_handle(pool)
        .tick_restore_coordinator(instance_id, &local_work_is_idle, expire_validated_jobs)
        .await
        .map_err(Into::into)
}

async fn wait_for_instances_drained(
    pool: &impl crate::storage::StorageContext,
) -> Result<(), ApiError> {
    let deadline = Instant::now() + StdDuration::from_secs(RESTORE_DRAIN_TIMEOUT_SECONDS);
    loop {
        let cutoff = Utc::now() - Duration::seconds(10);
        let (generation, instances) = storage_handle(pool)
            .get_restore_drain_state(cutoff)
            .await?
            .into_parts();
        if instances.iter().all(|instance| {
            instance.is_drained() && instance.maintenance_generation() == generation
        }) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let pending = instances
                .iter()
                .filter(|instance| {
                    !instance.is_drained() || instance.maintenance_generation() != generation
                })
                .map(|instance| instance.instance_id().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(ApiError::ServiceUnavailable(format!(
                "Timed out waiting for server instances to drain: {pending}"
            )));
        }
        actix_rt::time::sleep(StdDuration::from_millis(100)).await;
    }
}

pub fn ensure_restore_coordinator_running<C>(backend: C)
where
    C: StorageContext,
{
    let pool = storage_handle(&backend);
    RESTORE_COORDINATOR.call_once(move || {
        spawn_background_worker("restore-coordinator", move |shutdown| {
            let system = actix_rt::System::new();
            system.block_on(async move {
                let instance_id = Uuid::new_v4();
                let mut last_expiry_run = None;
                loop {
                    let expire_validated_jobs = last_expiry_run.is_none_or(|last_run: Instant| {
                        last_run.elapsed()
                            >= StdDuration::from_secs(RESTORE_STAGE_EXPIRY_INTERVAL_SECONDS)
                    });
                    match heartbeat_instance(&pool, instance_id, expire_validated_jobs).await {
                        Ok(_) => {
                            if expire_validated_jobs {
                                last_expiry_run = Some(Instant::now());
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                message = "Restore coordinator heartbeat failed",
                                instance_id = %instance_id,
                                error = %error,
                            );
                        }
                    }
                    tokio::select! {
                        _ = shutdown.requested() => break,
                        _ = actix_rt::time::sleep(StdDuration::from_secs(1)) => {}
                    }
                }
                let _ = pool.remove_restore_instance(instance_id).await;
            });
        });
    });
}

pub(crate) async fn current_maintenance_state(
    storage: &(impl OperationalStateStorage + ?Sized),
) -> Result<MaintenanceState, ApiError> {
    storage.get_maintenance_state().await.map_err(Into::into)
}

pub async fn get_maintenance_state(storage: &impl StorageContext) -> Result<String, ApiError> {
    let storage = storage_handle(storage);
    current_maintenance_state(&storage)
        .await
        .map(|state| state.as_str().to_string())
}

pub async fn resolve_identity_scope_name(
    pool: &impl crate::storage::StorageContext,
    identity_scope_id: i32,
) -> Result<String, ApiError> {
    load_identity_scope_name(pool, identity_scope_id).await
}

#[cfg(test)]
mod tests;
