use std::collections::{HashMap, HashSet};
use std::sync::Once;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration as StdDuration, Instant};

use chrono::{Duration, NaiveDateTime, Utc};
use hubuum_computed_fields::{MAX_PERSONAL_DEFINITIONS, MAX_SHARED_DEFINITIONS, SEMANTICS_VERSION};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::errors::ApiError;
use crate::lifecycle::spawn_background_worker;
use crate::models::identity::{LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND};
use crate::models::retention::FutureRetention;
use crate::models::{
    BackupDocument, COMPUTED_FIELD_VISIBILITY_PERSONAL, COMPUTED_FIELD_VISIBILITY_SHARED,
    ComputedFieldDefinitionRequest, ComputedResultType, MaintenanceState,
    RESTORE_CONFIRMATION_PHRASE, RestoreConfirmRequest, RestoreJobID, RestoreJobStatus,
    RestoreStageRequest, RestoreStageResponse, RestoreValidationSummary,
};
use crate::services::identity::resolve_identity_scope_name as load_identity_scope_name;
use crate::storage::storage_handle;
use crate::storage::{
    OperationalStateStorage, RestoreStorage, StorageBackupHistorySection, StorageBackupRow,
    StorageBackupSnapshot, StorageBackupStateSection, StorageContext, StorageRestoreApply,
    StorageRestoreArtifactSummary, StorageRestoreCompletion, StorageRestoreCoordinatorSnapshot,
    StorageRestoreDocument, StorageRestoreDocumentMetadata, StorageRestoreFailure,
    StorageRestoreInitiator, StorageRestoreJob, StorageRestoreJobStatus, StorageRestoreJobSummary,
    StorageRestoreStageCreate, StorageRestoreStatus,
};

static RESTORE_COORDINATOR: Once = Once::new();
static ACTIVE_MAINTENANCE_WORK: AtomicUsize = AtomicUsize::new(0);
const RESTORE_DRAIN_TIMEOUT_SECONDS: u64 = 30;
const RESTORE_STAGE_EXPIRY_INTERVAL_SECONDS: u64 = 60;
const MISSING_RESTORE_CAPABILITY_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
// Compatibility recovery for confirmations created by older in-process
// coordinators retains their original grace period. The dedicated executor
// does not wait for this threshold.
const RESTORE_RECONCILIATION_GRACE_SECONDS: i64 = 60;

struct RestoreJobSummaryData {
    id: i64,
    status: RestoreJobStatus,
    requested_by: Option<i32>,
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
    document: Vec<u8>,
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
        id: id.id(),
        status: restore_status_from_storage(status),
        requested_by: requested_by.map(|id| id.id()),
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

fn restore_job_id_to_storage(id: i64) -> RestoreJobID {
    RestoreJobID::new(id).expect("validated restore job id must be positive")
}

fn restore_job_from_storage(job: StorageRestoreJob) -> RestoreJobData {
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

/// Counts local request/worker activity across the maintenance state check and
/// the work it protects. The restore coordinator reports this instance drained
/// only after every such guard has dropped.
pub struct MaintenanceActivityGuard;

impl MaintenanceActivityGuard {
    pub fn begin() -> Self {
        ACTIVE_MAINTENANCE_WORK.fetch_add(1, Ordering::AcqRel);
        Self
    }
}

impl Drop for MaintenanceActivityGuard {
    fn drop(&mut self) {
        ACTIVE_MAINTENANCE_WORK.fetch_sub(1, Ordering::AcqRel);
    }
}

fn active_maintenance_work() -> usize {
    ACTIVE_MAINTENANCE_WORK.load(Ordering::Acquire)
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

fn validation_summary(document: &BackupDocument) -> Result<RestoreValidationSummary, ApiError> {
    document.validate_version()?;
    StorageBackupSnapshot::try_new(
        document.state.sections.clone(),
        document
            .history
            .as_ref()
            .map(|history| history.sections.clone()),
    )
    .map_err(|error| ApiError::from(error.into_request_error()))?;
    validate_required_seed_rows(document)?;
    validate_backup_revisions(document)?;
    validate_backup_class_schemas(document)?;
    validate_computed_field_definitions(document)?;
    let total_items = document
        .state
        .sections
        .values()
        .map(|rows| rows.len() as i64)
        .sum::<i64>()
        + document
            .history
            .as_ref()
            .map(|history| {
                history
                    .sections
                    .values()
                    .map(|rows| rows.len() as i64)
                    .sum::<i64>()
            })
            .unwrap_or(0);
    Ok(RestoreValidationSummary {
        backup_version: document.backup_version,
        source_version: document.source_version.clone(),
        includes_history: document.history.is_some(),
        total_items,
    })
}

fn validate_backup_class_schemas(document: &BackupDocument) -> Result<(), ApiError> {
    let current_classes = required_state_section(document, StorageBackupStateSection::Classes)?;
    let historical_classes = document
        .history
        .as_ref()
        .and_then(|history| {
            history
                .sections
                .get(&StorageBackupHistorySection::ClassHistory)
        })
        .map(Vec::as_slice)
        .unwrap_or(&[]);

    for row in current_classes.iter().chain(historical_classes) {
        let Some(schema) = row.get("json_schema").filter(|value| !value.is_null()) else {
            continue;
        };
        let validation = if row
            .get("validate_schema")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            crate::utilities::json_schema::compile_json_schema(schema).map(|_| ())
        } else {
            crate::utilities::json_schema::validate_json_schema(schema)
        };
        validation.map_err(|error| {
            let class_id = row.get("id").and_then(Value::as_i64);
            ApiError::BadRequest(format!(
                "Full backup class {} contains an invalid JSON schema: {error}",
                class_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "with unknown id".to_string())
            ))
        })?;
    }
    Ok(())
}

const REVISION_STATE_SECTIONS: &[StorageBackupStateSection] = &[
    StorageBackupStateSection::IdentityScopes,
    StorageBackupStateSection::Groups,
    StorageBackupStateSection::Principals,
    StorageBackupStateSection::GroupMemberships,
    StorageBackupStateSection::Collections,
    StorageBackupStateSection::CollectionAuthorization,
    StorageBackupStateSection::Classes,
    StorageBackupStateSection::ComputedFieldDefinitions,
    StorageBackupStateSection::ClassRelations,
    StorageBackupStateSection::Objects,
    StorageBackupStateSection::ObjectRelations,
    StorageBackupStateSection::ExportTemplates,
    StorageBackupStateSection::RemoteTargets,
    StorageBackupStateSection::EventSinks,
    StorageBackupStateSection::EventSubscriptions,
];

const REVISION_HISTORY_SECTIONS: &[(StorageBackupHistorySection, StorageBackupStateSection)] = &[
    (
        StorageBackupHistorySection::CollectionHistory,
        StorageBackupStateSection::Collections,
    ),
    (
        StorageBackupHistorySection::ClassHistory,
        StorageBackupStateSection::Classes,
    ),
    (
        StorageBackupHistorySection::ClassRelationHistory,
        StorageBackupStateSection::ClassRelations,
    ),
    (
        StorageBackupHistorySection::ObjectHistory,
        StorageBackupStateSection::Objects,
    ),
    (
        StorageBackupHistorySection::ObjectRelationHistory,
        StorageBackupStateSection::ObjectRelations,
    ),
    (
        StorageBackupHistorySection::ExportTemplateHistory,
        StorageBackupStateSection::ExportTemplates,
    ),
    (
        StorageBackupHistorySection::RemoteTargetHistory,
        StorageBackupStateSection::RemoteTargets,
    ),
];

fn row_revision(section: &str, row: &StorageBackupRow) -> Result<i64, ApiError> {
    row.get("revision")
        .and_then(Value::as_i64)
        .filter(|revision| (1..i64::MAX).contains(revision))
        .ok_or_else(|| {
            ApiError::BadRequest(format!(
                "Full backup section '{section}' contains an invalid resource revision"
            ))
        })
}

fn row_i64(section: &str, row: &StorageBackupRow, field: &str) -> Result<i64, ApiError> {
    row.get(field).and_then(Value::as_i64).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "Full backup section '{section}' contains an invalid {field}"
        ))
    })
}

fn validate_backup_revisions(document: &BackupDocument) -> Result<(), ApiError> {
    for section in REVISION_STATE_SECTIONS {
        for row in required_state_section(document, *section)? {
            row_revision(section.as_str(), row)?;
        }
    }

    validate_authorization_state_revisions(document)?;

    let Some(history) = &document.history else {
        return validate_event_revisions(document);
    };
    for (history_section, state_section) in REVISION_HISTORY_SECTIONS {
        let rows = history.sections.get(history_section).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "Full backup history is missing required section '{history_section}'"
            ))
        })?;
        for row in rows {
            row_revision(history_section.as_str(), row)?;
        }
        validate_live_history_revisions(document, *history_section, *state_section, rows)?;
    }
    validate_event_revisions(document)
}

fn validate_authorization_state_revisions(document: &BackupDocument) -> Result<(), ApiError> {
    let collection_ids = required_state_section(document, StorageBackupStateSection::Collections)?
        .iter()
        .map(|row| row_i64("collections", row, "id"))
        .collect::<Result<HashSet<_>, _>>()?;
    let authorization_ids =
        required_state_section(document, StorageBackupStateSection::CollectionAuthorization)?
            .iter()
            .map(|row| row_i64("collection_authorization", row, "collection_id"))
            .collect::<Result<Vec<_>, _>>()?;
    let unique_authorization_ids = authorization_ids.iter().copied().collect::<HashSet<_>>();
    if authorization_ids.len() != unique_authorization_ids.len()
        || unique_authorization_ids != collection_ids
    {
        return Err(ApiError::BadRequest(
            "Full backup collection authorization revisions do not match collections".to_string(),
        ));
    }
    Ok(())
}

fn validate_live_history_revisions(
    document: &BackupDocument,
    history_section: StorageBackupHistorySection,
    state_section: StorageBackupStateSection,
    history_rows: &[StorageBackupRow],
) -> Result<(), ApiError> {
    let live = required_state_section(document, state_section)?
        .iter()
        .map(|row| {
            Ok((
                row_i64(state_section.as_str(), row, "id")?,
                row_revision(state_section.as_str(), row)?,
            ))
        })
        .collect::<Result<HashMap<_, _>, ApiError>>()?;
    let mut open = HashMap::new();
    for row in history_rows
        .iter()
        .filter(|row| row.get("valid_to").is_some_and(Value::is_null))
    {
        let id = row_i64(history_section.as_str(), row, "id")?;
        let revision = row_revision(history_section.as_str(), row)?;
        if row.get("operation").and_then(Value::as_str) == Some("delete")
            || open.insert(id, revision).is_some()
        {
            return Err(ApiError::BadRequest(format!(
                "Full backup history section '{history_section}' has an invalid open snapshot"
            )));
        }
    }
    if open != live {
        return Err(ApiError::BadRequest(format!(
            "Full backup live revisions disagree with '{history_section}'"
        )));
    }
    Ok(())
}

fn validate_event_revisions(document: &BackupDocument) -> Result<(), ApiError> {
    let Some(events) = document.history.as_ref().and_then(|history| {
        history
            .sections
            .get(&StorageBackupHistorySection::AuditEvents)
    }) else {
        return Ok(());
    };
    for event in events {
        for (column, snapshot) in [("before_revision", "before"), ("after_revision", "after")] {
            let stored = match event.get(column) {
                None | Some(Value::Null) => None,
                Some(value) => Some(
                    value
                        .as_i64()
                        .filter(|revision| (1..i64::MAX).contains(revision))
                        .ok_or_else(|| {
                            ApiError::BadRequest(format!(
                                "Full backup event contains an invalid {column}"
                            ))
                        })?,
                ),
            };
            let snapshot_revision = event
                .get(snapshot)
                .filter(|value| !value.is_null())
                .and_then(|value| value.get("revision"))
                .and_then(Value::as_i64);
            if stored.is_some() && stored != snapshot_revision {
                return Err(ApiError::BadRequest(format!(
                    "Full backup event {column} disagrees with its {snapshot} snapshot"
                )));
            }
        }
        if event.get("schema_version").and_then(Value::as_i64) == Some(2) {
            let before = event
                .get("before_revision")
                .is_some_and(|value| !value.is_null());
            let after = event
                .get("after_revision")
                .is_some_and(|value| !value.is_null());
            let action = event.get("action").and_then(Value::as_str);
            let valid_shape = match action {
                Some("created" | "queued" | "added") => !before && after,
                Some("deleted" | "removed" | "purged") => before && !after,
                _ => before && after,
            };
            if !valid_shape {
                return Err(ApiError::BadRequest(
                    "Full backup revision-aware event has inconsistent before/after revisions"
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_computed_field_definitions(document: &BackupDocument) -> Result<(), ApiError> {
    let mut shared_counts = HashMap::<i32, usize>::new();
    let mut personal_counts = HashMap::<(i32, i32), usize>::new();
    let mut shared_keys = HashSet::<(i32, String)>::new();
    let mut personal_keys = HashSet::<(i32, i32, String)>::new();
    for row in required_state_section(
        document,
        StorageBackupStateSection::ComputedFieldDefinitions,
    )? {
        let object = row.fields();
        let string = |field: &str| {
            object
                .get(field)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| {
                    ApiError::BadRequest(format!(
                        "Full backup computed-field definition has an invalid {field}"
                    ))
                })
        };
        let result_type =
            serde_json::from_value::<ComputedResultType>(Value::String(string("result_type")?))
                .map_err(|_| {
                    ApiError::BadRequest(
                        "Full backup computed-field definition has an invalid result_type"
                            .to_string(),
                    )
                })?;
        let key = string("key")?;
        let request = ComputedFieldDefinitionRequest {
            key: key.clone(),
            label: string("label")?,
            description: string("description")?,
            operation: object.get("operation").cloned().ok_or_else(|| {
                ApiError::BadRequest(
                    "Full backup computed-field definition is missing operation".to_string(),
                )
            })?,
            result_type,
            enabled: object
                .get("enabled")
                .and_then(Value::as_bool)
                .ok_or_else(|| {
                    ApiError::BadRequest(
                        "Full backup computed-field definition has an invalid enabled flag"
                            .to_string(),
                    )
                })?,
        };
        request.validate().map_err(|error| {
            ApiError::BadRequest(format!(
                "Full backup contains an invalid computed-field definition: {error}"
            ))
        })?;

        let positive_id = |field: &str| {
            object
                .get(field)
                .and_then(Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or_else(|| {
                    ApiError::BadRequest(format!(
                        "Full backup computed-field definition has an invalid {field}"
                    ))
                })
        };
        let class_id = positive_id("class_id")?;
        object
            .get("revision")
            .and_then(Value::as_i64)
            .filter(|value| (1..i64::MAX).contains(value))
            .ok_or_else(|| {
                ApiError::BadRequest(
                    "Full backup computed-field definition has an invalid revision".to_string(),
                )
            })?;
        let semantics_version = object
            .get("semantics_version")
            .and_then(Value::as_i64)
            .and_then(|value| i16::try_from(value).ok())
            .ok_or_else(|| {
                ApiError::BadRequest(
                    "Full backup computed-field definition has an invalid semantics_version"
                        .to_string(),
                )
            })?;
        if semantics_version != SEMANTICS_VERSION {
            return Err(ApiError::BadRequest(format!(
                "Full backup computed-field definition uses unsupported semantics version {semantics_version}"
            )));
        }
        let visibility = string("visibility")?;
        match visibility.as_str() {
            COMPUTED_FIELD_VISIBILITY_SHARED => {
                if object
                    .get("owner_principal_id")
                    .is_some_and(|value| !value.is_null())
                {
                    return Err(ApiError::BadRequest(
                        "Full backup shared computed-field definition must not have an owner_principal_id"
                            .to_string(),
                    ));
                }
                if !shared_keys.insert((class_id, key)) {
                    return Err(ApiError::BadRequest(format!(
                        "Full backup class {class_id} contains a duplicate shared computed-field key"
                    )));
                }
                let count = shared_counts.entry(class_id).or_default();
                *count += 1;
                if *count > MAX_SHARED_DEFINITIONS {
                    return Err(ApiError::BadRequest(format!(
                        "Full backup class {class_id} has more than {MAX_SHARED_DEFINITIONS} shared computed fields"
                    )));
                }
            }
            COMPUTED_FIELD_VISIBILITY_PERSONAL => {
                let owner_id = positive_id("owner_principal_id")?;
                if !personal_keys.insert((owner_id, class_id, key)) {
                    return Err(ApiError::BadRequest(format!(
                        "Full backup user {owner_id} contains a duplicate personal computed-field key for class {class_id}"
                    )));
                }
                let count = personal_counts.entry((owner_id, class_id)).or_default();
                *count += 1;
                if *count > MAX_PERSONAL_DEFINITIONS {
                    return Err(ApiError::BadRequest(format!(
                        "Full backup user {owner_id} has more than {MAX_PERSONAL_DEFINITIONS} personal computed fields for class {class_id}"
                    )));
                }
            }
            _ => {
                return Err(ApiError::BadRequest(
                    "Full backup computed-field definition has an invalid visibility".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn required_state_section(
    document: &BackupDocument,
    section: StorageBackupStateSection,
) -> Result<&[StorageBackupRow], ApiError> {
    document
        .state
        .sections
        .get(&section)
        .map(Vec::as_slice)
        .ok_or_else(|| {
            ApiError::BadRequest(format!(
                "Full backup is missing required state section '{section}'"
            ))
        })
}

fn validate_required_seed_rows(document: &BackupDocument) -> Result<(), ApiError> {
    let local_scopes = required_state_section(document, StorageBackupStateSection::IdentityScopes)?
        .iter()
        .filter(|row| row.get("name").and_then(Value::as_str) == Some(LOCAL_IDENTITY_SCOPE))
        .collect::<Vec<_>>();
    if local_scopes.len() != 1
        || local_scopes[0].get("provider_kind").and_then(Value::as_str) != Some(LOCAL_PROVIDER_KIND)
    {
        return Err(ApiError::BadRequest(format!(
            "Full backup must contain exactly one '{LOCAL_IDENTITY_SCOPE}' identity scope with provider kind '{LOCAL_PROVIDER_KIND}'"
        )));
    }

    let roots = required_state_section(document, StorageBackupStateSection::Collections)?
        .iter()
        .filter(|row| row.get("parent_collection_id").is_some_and(Value::is_null))
        .collect::<Vec<_>>();
    if roots.len() != 1 {
        return Err(ApiError::BadRequest(
            "Full backup must contain exactly one root collection".to_string(),
        ));
    }
    let root_id = roots[0].get("id").and_then(Value::as_i64).ok_or_else(|| {
        ApiError::BadRequest("Full backup root collection has an invalid id".to_string())
    })?;
    let has_root_closure =
        required_state_section(document, StorageBackupStateSection::CollectionHierarchy)?
            .iter()
            .any(|row| {
                row.get("ancestor_collection_id").and_then(Value::as_i64) == Some(root_id)
                    && row.get("descendant_collection_id").and_then(Value::as_i64) == Some(root_id)
                    && row.get("depth").and_then(Value::as_i64) == Some(0)
            });
    if !has_root_closure {
        return Err(ApiError::BadRequest(
            "Full backup must contain the root collection's depth-zero closure row".to_string(),
        ));
    }

    Ok(())
}

pub async fn stage_restore(
    pool: &impl crate::storage::StorageContext,
    settings: &RestoreSettings,
    request: RestoreStageRequest,
) -> Result<RestoreStageResponse, ApiError> {
    let (initiator, document_bytes) = request.into_parts();
    if document_bytes.len() > settings.max_upload_bytes() {
        return Err(ApiError::PayloadTooLarge(format!(
            "Restore upload is {} bytes, exceeding the configured {} byte limit",
            document_bytes.len(),
            settings.max_upload_bytes()
        )));
    }
    let document: BackupDocument = serde_json::from_slice(&document_bytes).map_err(|error| {
        ApiError::BadRequest(format!(
            "Restore document is not valid backup JSON: {error}"
        ))
    })?;
    let validation = validation_summary(&document)?;
    let document_sha = sha256(&document_bytes);
    let capability = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let capability_hash = sha256(capability.as_bytes());
    let expires_at = settings.stage_expires_at(Utc::now().naive_utc())?;
    let validation_json = serde_json::to_value(&validation)?;
    let byte_size = i64::try_from(document_bytes.len()).unwrap_or(i64::MAX);
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
    let artifact = StorageRestoreArtifactSummary::try_new(byte_size, document_sha.clone())
        .map_err(|error| ApiError::from(error.into_request_error()))?;
    let request = StorageRestoreStageCreate::try_new(
        initiator,
        document_bytes,
        artifact,
        capability_hash,
        validation_json,
        expires_at.and_utc(),
    )
    .map_err(|error| ApiError::from(error.into_request_error()))?;
    let job = storage_handle(pool)
        .stage_restore(request)
        .await
        .map(restore_job_from_storage)?;
    let job = job.summary;

    Ok(RestoreStageResponse {
        id: job.id,
        status: RestoreJobStatus::Validated,
        requested_by: job.requested_by,
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
        .get_restore_job(job_id)
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
        id: job.id,
        status,
        requested_by: job.requested_by,
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
    document: BackupDocument,
) -> Result<StorageRestoreCompletion, ApiError> {
    let metadata = StorageRestoreDocumentMetadata::new(
        document.backup_version,
        document.created_at,
        document.source_version,
    );
    let snapshot = StorageBackupSnapshot::try_new(
        document.state.sections,
        document.history.map(|history| history.sections),
    )
    .map_err(|error| ApiError::from(error.into_request_error()))?;
    let document = StorageRestoreDocument::new(metadata, snapshot);
    storage_handle(pool)
        .apply_restore(StorageRestoreApply::new(job_id, document))
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
        let changed = storage_handle(pool)
            .expire_restore_stage(restore_job_id_to_storage(job.id))
            .await?;
        if !changed {
            return Err(ApiError::Conflict(
                "Restore stage changed status concurrently".to_string(),
            ));
        }
        return Err(ApiError::Gone("Restore stage has expired".to_string()));
    }
    let document: BackupDocument = serde_json::from_slice(&document_bytes).map_err(|error| {
        ApiError::InternalServerError(format!("Staged restore document became invalid: {error}"))
    })?;
    let validation = validation_summary(&document)?;
    // Confirmation commits only the maintenance transition. A separately
    // deployed executor owns the privileged destructive transaction, so the
    // API and worker processes never need a migration credential.
    let job_id = restore_job_id_to_storage(job.id);
    let confirmed_at = storage_handle(pool).start_restore_draining(job_id).await?;
    Ok(RestoreStageResponse {
        id: job.id,
        status: RestoreJobStatus::Confirmed,
        requested_by: job.requested_by,
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
    let job = match storage_handle(pool).get_restore_job(job_id).await {
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

    let document: BackupDocument = match serde_json::from_slice(&document_bytes) {
        Ok(document) => document,
        Err(parse_error) => {
            let error = ApiError::InternalServerError(format!(
                "Staged restore document became invalid: {parse_error}"
            ));
            fail_restore_and_resume(pool, job_id, &error).await?;
            return Err(error);
        }
    };
    if let Err(error) = validation_summary(&document) {
        fail_restore_and_resume(pool, job_id, &error).await?;
        return Err(error);
    }
    if let Err(error) = wait_for_instances_drained(pool).await {
        fail_restore_and_resume(pool, job_id, &error).await?;
        return Err(error);
    }
    if let Err(error) = apply_restore(pool, job_id, document).await {
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
mod tests {
    use std::collections::BTreeMap;

    use chrono::{Duration, NaiveDate};
    use rstest::rstest;
    use serde_json::json;

    use super::{
        MAX_PERSONAL_DEFINITIONS, MAX_SHARED_DEFINITIONS, RESTORE_RECONCILIATION_GRACE_SECONDS,
        RestoreSettings, confirmation_is_stale, restore_capability_matches,
        restore_error_for_storage, sha256, validate_computed_field_definitions,
        validate_event_revisions,
    };
    use crate::errors::ApiError;
    use crate::models::{
        BackupDocument, BackupHistory, BackupManifest, BackupState, CURRENT_BACKUP_VERSION,
    };
    use crate::storage::{
        StorageBackupHistorySection, StorageBackupRow, StorageBackupStateSection,
    };

    fn backup_instant() -> chrono::DateTime<chrono::Utc> {
        NaiveDate::from_ymd_opt(2026, 7, 16)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
    }

    fn computed_definition(class_id: i32, owner_id: Option<i32>, key: String) -> serde_json::Value {
        json!({
            "class_id": class_id,
            "visibility": if owner_id.is_some() { "personal" } else { "shared" },
            "owner_principal_id": owner_id,
            "key": key,
            "label": "Restored field",
            "description": "",
            "operation": {"type": "first_non_null", "paths": ["/value"]},
            "result_type": "string",
            "enabled": true,
            "revision": 1,
            "semantics_version": 1
        })
    }

    fn document_with_computed_definitions(definitions: Vec<serde_json::Value>) -> BackupDocument {
        BackupDocument {
            backup_version: CURRENT_BACKUP_VERSION,
            created_at: backup_instant(),
            source_version: "test".to_string(),
            state: BackupState {
                sections: BTreeMap::from([(
                    StorageBackupStateSection::ComputedFieldDefinitions,
                    definitions
                        .into_iter()
                        .map(StorageBackupRow::try_from_value)
                        .collect::<Result<_, _>>()
                        .unwrap(),
                )]),
            },
            history: None,
            manifest: BackupManifest::default(),
        }
    }

    fn document_with_event(event: serde_json::Value) -> BackupDocument {
        BackupDocument {
            backup_version: CURRENT_BACKUP_VERSION,
            created_at: backup_instant(),
            source_version: "test".to_string(),
            state: BackupState::default(),
            history: Some(BackupHistory {
                sections: BTreeMap::from([(
                    StorageBackupHistorySection::AuditEvents,
                    vec![StorageBackupRow::try_from_value(event).unwrap()],
                )]),
            }),
            manifest: BackupManifest::default(),
        }
    }

    #[rstest]
    #[case::upload_limit(60, 0)]
    #[case::retention(0, 1024)]
    fn restore_settings_reject_zero_limits(
        #[case] retention_minutes: i64,
        #[case] upload_bytes: usize,
    ) {
        assert!(RestoreSettings::new(retention_minutes, upload_bytes).is_err());
    }

    #[test]
    fn restore_settings_reject_unrepresentable_retention() {
        let error = RestoreSettings::new(i64::MAX, 1024).unwrap_err();

        assert_eq!(
            error,
            "restore stage retention is outside the supported duration range"
        );
    }

    #[rstest]
    #[case::exact_match(Some("expected-capability"), "expected-capability", true)]
    #[case::mismatch(Some("expected-capability"), "different-capability", false)]
    #[case::missing_stage(None, "expected-capability", false)]
    fn restore_capability_validation_handles_present_and_missing_stages(
        #[case] stored_capability: Option<&str>,
        #[case] supplied_capability: &str,
        #[case] expected: bool,
    ) {
        let stored_hash = stored_capability.map(|capability| sha256(capability.as_bytes()));

        assert_eq!(
            restore_capability_matches(stored_hash.as_deref(), supplied_capability),
            expected
        );
    }

    #[rstest]
    #[case::just_confirmed(0, false)]
    #[case::inside_grace_period(RESTORE_RECONCILIATION_GRACE_SECONDS - 1, false)]
    #[case::at_grace_boundary(RESTORE_RECONCILIATION_GRACE_SECONDS, true)]
    #[case::past_grace_period(RESTORE_RECONCILIATION_GRACE_SECONDS + 1, true)]
    fn restore_confirmation_staleness_respects_grace_period(
        #[case] age_seconds: i64,
        #[case] expected: bool,
    ) {
        let now = NaiveDate::from_ymd_opt(2026, 7, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();

        assert_eq!(
            confirmation_is_stale(now - Duration::seconds(age_seconds), now),
            expected
        );
    }

    #[rstest]
    #[case::shared(MAX_SHARED_DEFINITIONS, None)]
    #[case::personal(MAX_PERSONAL_DEFINITIONS, Some(7))]
    fn restore_rejects_computed_definition_scope_over_capacity(
        #[case] maximum: usize,
        #[case] owner_id: Option<i32>,
    ) {
        let definitions = (0..=maximum)
            .map(|index| computed_definition(42, owner_id, format!("field_{index}")))
            .collect();
        let error =
            validate_computed_field_definitions(&document_with_computed_definitions(definitions))
                .unwrap_err();
        assert!(error.to_string().contains(&maximum.to_string()));
    }

    #[test]
    fn restore_rejects_duplicate_computed_definition_keys() {
        let definitions = vec![
            computed_definition(42, None, "duplicate".to_string()),
            computed_definition(42, None, "duplicate".to_string()),
        ];

        let error =
            validate_computed_field_definitions(&document_with_computed_definitions(definitions))
                .unwrap_err();

        assert!(error.to_string().contains("duplicate"));
    }

    #[rstest]
    #[case::deleted("deleted")]
    #[case::removed("removed")]
    #[case::purged("purged")]
    fn restore_accepts_revisioned_deletion_event_shapes(#[case] action: &str) {
        let event = json!({
            "schema_version": 2,
            "action": action,
            "before": {"revision": 7},
            "after": null,
            "before_revision": 7,
            "after_revision": null,
        });

        assert!(validate_event_revisions(&document_with_event(event)).is_ok());
    }

    #[rstest]
    #[case::internal_server(
        ApiError::InternalServerError("secret internal path".to_string()),
        "secret internal path"
    )]
    #[case::database(
        ApiError::DatabaseError("password=database-secret".to_string()),
        "database-secret"
    )]
    #[case::connection(
        ApiError::DbConnectionError("postgres://user:secret@db/app".to_string()),
        "user:secret"
    )]
    #[case::hash(
        ApiError::HashError("hash implementation detail".to_string()),
        "implementation detail"
    )]
    fn restore_status_internal_errors_do_not_expose_details(
        #[case] error: ApiError,
        #[case] private_detail: &str,
    ) {
        let stored = restore_error_for_storage(&error);

        assert_eq!(stored, "An internal error occurred");
        assert!(!stored.contains(private_detail));
    }

    #[rstest]
    #[case::validation_failed(
        ApiError::ValidationError("Backup manifest is invalid".to_string()),
        "Backup manifest is invalid"
    )]
    #[case::conflict(
        ApiError::Conflict("Restore stage changed status concurrently".to_string()),
        "Restore stage changed status concurrently"
    )]
    fn restore_status_preserves_public_safe_errors(
        #[case] error: ApiError,
        #[case] expected: &str,
    ) {
        assert_eq!(restore_error_for_storage(&error), expected);
    }
}
