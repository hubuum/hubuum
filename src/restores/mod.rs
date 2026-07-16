use std::collections::HashMap;
use std::sync::Once;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration as StdDuration, Instant};

use chrono::{Duration, NaiveDateTime, Utc};
use hubuum_computed_fields::{MAX_PERSONAL_DEFINITIONS, MAX_SHARED_DEFINITIONS};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::db::DbPool;
use crate::db::traits::restore::{
    RestoreCompletion, apply_restore_db, delete_server_instance_db, expire_restore_stage_db,
    expire_validated_restore_jobs_db, fail_restore_and_resume_db, identity_scope_name_db,
    insert_restore_job_db, load_restore_job_db, load_restore_status_job_db,
    maintenance_generation_and_instances_db, maintenance_generation_and_state_db,
    maintenance_restore_reference_db, maintenance_state_db, resume_maintenance_without_job_db,
    resume_terminal_restore_db, start_restore_draining_db, upsert_server_instance_db,
};
use crate::errors::ApiError;
use crate::events::{Action, ActorKind, EntityType, NewEvent};
use crate::lifecycle::spawn_background_worker;
use crate::models::backup::{
    BACKUP_STATE_SECTIONS, backup_history_sections, is_backup_history_section,
};
use crate::models::identity::{LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND};
use crate::models::{
    BackupDocument, COMPUTED_FIELD_VISIBILITY_PERSONAL, COMPUTED_FIELD_VISIBILITY_SHARED,
    ComputedFieldDefinitionRequest, ComputedResultType, NewRestoreJobRecord,
    RESTORE_CONFIRMATION_PHRASE, RestoreConfirmRequest, RestoreJobRecord, RestoreJobStatus,
    RestoreStageRequest, RestoreStageResponse, RestoreValidationSummary, ServerInstanceRecord,
};

static RESTORE_COORDINATOR: Once = Once::new();
static ACTIVE_MAINTENANCE_WORK: AtomicUsize = AtomicUsize::new(0);
const RESTORE_DRAIN_TIMEOUT_SECONDS: u64 = 30;
// Keep this longer than the bounded drain so normal confirmations enter the
// advisory-lock-protected restore transaction before recovery is eligible.
const RESTORE_RECONCILIATION_GRACE_SECONDS: i64 = 60;

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
    stage_retention_minutes: i64,
    max_upload_bytes: usize,
}

impl RestoreSettings {
    pub fn new(stage_retention_minutes: i64, max_upload_bytes: usize) -> Result<Self, String> {
        if stage_retention_minutes <= 0 {
            return Err("restore stage retention must be greater than zero".to_string());
        }
        if max_upload_bytes == 0 {
            return Err("restore upload size limit must be greater than zero".to_string());
        }
        Ok(Self {
            stage_retention_minutes,
            max_upload_bytes,
        })
    }

    pub fn stage_retention_minutes(&self) -> i64 {
        self.stage_retention_minutes
    }

    pub fn max_upload_bytes(&self) -> usize {
        self.max_upload_bytes
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

fn validation_summary(document: &BackupDocument) -> Result<RestoreValidationSummary, ApiError> {
    document.validate_version()?;
    for required in BACKUP_STATE_SECTIONS {
        if !document.state.sections.contains_key(*required) {
            return Err(ApiError::BadRequest(format!(
                "Full backup is missing required state section '{required}'"
            )));
        }
    }
    if let Some(unknown) = document
        .state
        .sections
        .keys()
        .find(|name| !BACKUP_STATE_SECTIONS.contains(&name.as_str()))
    {
        return Err(ApiError::BadRequest(format!(
            "Full backup contains unknown state section '{unknown}'"
        )));
    }
    if let Some(history) = &document.history {
        for required in backup_history_sections() {
            if !history.sections.contains_key(required) {
                return Err(ApiError::BadRequest(format!(
                    "Full backup history is missing required section '{required}'"
                )));
            }
        }
        if let Some(unknown) = history
            .sections
            .keys()
            .find(|name| !is_backup_history_section(name))
        {
            return Err(ApiError::BadRequest(format!(
                "Full backup contains unknown history section '{unknown}'"
            )));
        }
    }
    validate_required_seed_rows(document)?;
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
    let current_classes = required_state_section(document, "hubuumclass")?;
    let historical_classes = document
        .history
        .as_ref()
        .and_then(|history| history.sections.get("hubuumclass_history"))
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

fn validate_computed_field_definitions(document: &BackupDocument) -> Result<(), ApiError> {
    let mut shared_counts = HashMap::<i32, usize>::new();
    let mut personal_counts = HashMap::<(i32, i32), usize>::new();
    for row in required_state_section(document, "computed_field_definitions")? {
        let object = row.as_object().ok_or_else(|| {
            ApiError::BadRequest(
                "Full backup contains a non-object computed-field definition".to_string(),
            )
        })?;
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
        let request = ComputedFieldDefinitionRequest {
            key: string("key")?,
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
        let visibility = string("visibility")?;
        match visibility.as_str() {
            COMPUTED_FIELD_VISIBILITY_SHARED => {
                if object
                    .get("owner_user_id")
                    .is_some_and(|value| !value.is_null())
                {
                    return Err(ApiError::BadRequest(
                        "Full backup shared computed-field definition must not have an owner_user_id"
                            .to_string(),
                    ));
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
                let owner_id = positive_id("owner_user_id")?;
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

fn required_state_section<'a>(
    document: &'a BackupDocument,
    name: &str,
) -> Result<&'a [Value], ApiError> {
    document
        .state
        .sections
        .get(name)
        .map(Vec::as_slice)
        .ok_or_else(|| {
            ApiError::BadRequest(format!(
                "Full backup is missing required state section '{name}'"
            ))
        })
}

fn validate_required_seed_rows(document: &BackupDocument) -> Result<(), ApiError> {
    let local_scopes = required_state_section(document, "identity_scopes")?
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

    let roots = required_state_section(document, "collections")?
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
    let has_root_closure = required_state_section(document, "collection_closure")?
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
    pool: &DbPool,
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
    let expires_at = Utc::now().naive_utc() + Duration::minutes(settings.stage_retention_minutes());
    let validation_json = serde_json::to_value(&validation)?;
    let byte_size = i64::try_from(document_bytes.len()).unwrap_or(i64::MAX);
    let (requested_by, requested_by_identity_scope, requested_by_name) = initiator.into_parts();
    let job = insert_restore_job_db(
        pool,
        NewRestoreJobRecord {
            status: RestoreJobStatus::Validated.as_str().to_string(),
            requested_by,
            requested_by_identity_scope,
            requested_by_name,
            document: document_bytes,
            byte_size,
            sha256: document_sha.clone(),
            capability_hash,
            validation_summary: validation_json,
            expires_at,
        },
    )
    .await?;

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

pub async fn load_restore_job(pool: &DbPool, job_id: i64) -> Result<RestoreJobRecord, ApiError> {
    load_restore_job_db(pool, job_id).await
}

pub async fn restore_status(
    pool: &DbPool,
    job_id: i64,
    capability: &str,
) -> Result<RestoreStageResponse, ApiError> {
    let job = load_restore_status_job_db(pool, job_id).await?;
    if !capability_matches(&job.capability_hash, capability) {
        return Err(ApiError::Forbidden(
            "Restore capability is invalid".to_string(),
        ));
    }
    let status = job.status.parse::<RestoreJobStatus>()?;
    let validation = serde_json::from_value(job.validation_summary.clone())?;
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
    pool: &DbPool,
    job: &RestoreJobRecord,
    document: &BackupDocument,
) -> Result<RestoreCompletion, ApiError> {
    let provenance = NewEvent::new(
        EntityType::Restore,
        Action::Succeeded,
        ActorKind::System,
        "System restore completed",
    )?
    .with_entity_name(job.sha256.clone())
    .with_metadata(json!({
        "restore_job_id": job.id,
        "backup_sha256": job.sha256,
        "backup_version": document.backup_version,
        "backup_source_version": document.source_version,
        "backup_created_at": document.created_at,
        "includes_history": document.history.is_some(),
        "initiated_by": {
            "principal_id": job.requested_by,
            "identity_scope": job.requested_by_identity_scope,
            "name": job.requested_by_name,
        },
    }));
    apply_restore_db(pool, job, document, &provenance).await
}

async fn fail_restore_and_resume(
    pool: &DbPool,
    job_id: i64,
    error: &ApiError,
) -> Result<(), ApiError> {
    let stored_error = error.to_string();
    fail_restore_and_resume_db(pool, job_id, &stored_error).await
}

pub async fn confirm_restore(
    pool: &DbPool,
    job_id: i64,
    confirmation: &RestoreConfirmRequest,
) -> Result<RestoreStageResponse, ApiError> {
    let job = load_restore_job(pool, job_id).await?;
    if !capability_matches(&job.capability_hash, &confirmation.restore_capability) {
        return Err(ApiError::Forbidden(
            "Restore capability is invalid".to_string(),
        ));
    }
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
    if job.status != RestoreJobStatus::Validated.as_str() {
        return Err(ApiError::Conflict(format!(
            "Restore stage cannot be confirmed from status '{}'",
            job.status
        )));
    }
    if job.expires_at <= Utc::now().naive_utc() {
        let changed = expire_restore_stage_db(pool, job.id).await?;
        if changed != 1 {
            return Err(ApiError::Conflict(
                "Restore stage changed status concurrently".to_string(),
            ));
        }
        return Err(ApiError::Gone("Restore stage has expired".to_string()));
    }
    let document: BackupDocument = serde_json::from_slice(&job.document).map_err(|error| {
        ApiError::InternalServerError(format!("Staged restore document became invalid: {error}"))
    })?;
    let validation = validation_summary(&document)?;
    // The maintenance transition commits before the destructive transaction,
    // allowing every instance to reject new work. ACCESS EXCLUSIVE table locks
    // in `apply_restore` are the final drain barrier for requests already in
    // flight. A failed restore rolls the data transaction back intact.
    let confirmed_at = start_restore_draining_db(pool, job.id).await?;

    if let Err(error) = wait_for_instances_drained(pool).await {
        fail_restore_and_resume(pool, job.id, &error).await?;
        return Err(error);
    }

    let completion = match apply_restore(pool, &job, &document).await {
        Ok(completion) => completion,
        Err(error) => {
            fail_restore_and_resume(pool, job.id, &error).await?;
            return Err(error);
        }
    };

    Ok(RestoreStageResponse {
        id: job.id,
        status: RestoreJobStatus::Succeeded,
        requested_by: job.requested_by,
        requested_by_identity_scope: job.requested_by_identity_scope,
        requested_by_name: job.requested_by_name,
        sha256: job.sha256,
        byte_size: job.byte_size,
        expires_at: job.expires_at,
        error: None,
        confirmed_at: Some(confirmed_at),
        started_at: Some(completion.started_at),
        finished_at: Some(completion.finished_at),
        created_at: job.created_at,
        updated_at: completion.finished_at,
        validation,
        restore_capability: None,
    })
}

/// Resume a restore whose committed maintenance transition survived a process
/// restart. The destructive transaction is guarded by an advisory lock and
/// re-checks the job/maintenance state. Once one coordinator commits, the
/// maintenance row is normal and every restore staging row has been removed.
pub async fn reconcile_interrupted_restore(pool: &DbPool) -> Result<(), ApiError> {
    let (maintenance_state, restore_job_id, database_now) =
        maintenance_restore_reference_db(pool).await?;

    if maintenance_state == "normal" {
        return Ok(());
    }
    if maintenance_state != "draining" {
        return Err(ApiError::InternalServerError(format!(
            "Unknown maintenance state '{maintenance_state}'"
        )));
    }
    let Some(job_id) = restore_job_id else {
        let error = ApiError::InternalServerError(format!(
            "Maintenance state '{maintenance_state}' has no restore job"
        ));
        resume_maintenance_without_job_db(pool).await?;
        return Err(error);
    };
    let job = match load_restore_job(pool, job_id).await {
        Ok(job) => job,
        Err(error) => {
            fail_restore_and_resume(pool, job_id, &error).await?;
            return Err(error);
        }
    };
    if matches!(job.status.as_str(), "failed" | "expired") {
        resume_terminal_restore_db(pool, job_id).await?;
        return Ok(());
    }
    if job.status != RestoreJobStatus::Confirmed.as_str() {
        let error = ApiError::Conflict(format!(
            "Maintenance references restore stage {job_id} in status '{}'",
            job.status
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
    if !confirmation_is_stale(confirmed_at, database_now) {
        return Ok(());
    }

    let document: BackupDocument = match serde_json::from_slice(&job.document) {
        Ok(document) => document,
        Err(parse_error) => {
            let error = ApiError::InternalServerError(format!(
                "Staged restore document became invalid: {parse_error}"
            ));
            fail_restore_and_resume(pool, job.id, &error).await?;
            return Err(error);
        }
    };
    if let Err(error) = validation_summary(&document) {
        fail_restore_and_resume(pool, job.id, &error).await?;
        return Err(error);
    }
    if let Err(error) = wait_for_instances_drained(pool).await {
        fail_restore_and_resume(pool, job.id, &error).await?;
        return Err(error);
    }
    if let Err(error) = apply_restore(pool, &job, &document).await {
        fail_restore_and_resume(pool, job.id, &error).await?;
        return Err(error);
    }
    Ok(())
}

async fn heartbeat_instance(pool: &DbPool, instance_id: Uuid) -> Result<(), ApiError> {
    expire_validated_restore_jobs_db(pool).await?;
    let (generation, state) = maintenance_generation_and_state_db(pool).await?;
    let now = Utc::now().naive_utc();
    let record = ServerInstanceRecord {
        instance_id,
        maintenance_generation: generation,
        drained: state != "normal" && active_maintenance_work() == 0,
        last_heartbeat_at: now,
        started_at: now,
    };
    upsert_server_instance_db(pool, &record).await
}

async fn wait_for_instances_drained(pool: &DbPool) -> Result<(), ApiError> {
    let deadline = Instant::now() + StdDuration::from_secs(RESTORE_DRAIN_TIMEOUT_SECONDS);
    loop {
        let cutoff = Utc::now().naive_utc() - Duration::seconds(10);
        let (generation, instances) = maintenance_generation_and_instances_db(pool, cutoff).await?;
        if instances
            .iter()
            .all(|instance| instance.drained && instance.maintenance_generation == generation)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let pending = instances
                .iter()
                .filter(|instance| {
                    !instance.drained || instance.maintenance_generation != generation
                })
                .map(|instance| instance.instance_id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(ApiError::ServiceUnavailable(format!(
                "Timed out waiting for server instances to drain: {pending}"
            )));
        }
        actix_rt::time::sleep(StdDuration::from_millis(100)).await;
    }
}

async fn reconcile_interrupted_restore_with_heartbeat(
    pool: &DbPool,
    instance_id: Uuid,
) -> Result<(), ApiError> {
    let reconciliation = reconcile_interrupted_restore(pool);
    tokio::pin!(reconciliation);
    loop {
        tokio::select! {
            result = &mut reconciliation => return result,
            _ = actix_rt::time::sleep(StdDuration::from_secs(1)) => {
                heartbeat_instance(pool, instance_id).await?;
            }
        }
    }
}

pub fn ensure_restore_coordinator_running(pool: DbPool) {
    RESTORE_COORDINATOR.call_once(move || {
        spawn_background_worker("restore-coordinator", move |shutdown| {
            let system = actix_rt::System::new();
            system.block_on(async move {
                let instance_id = Uuid::new_v4();
                loop {
                    if let Err(error) = heartbeat_instance(&pool, instance_id).await {
                        tracing::error!(
                            message = "Restore coordinator heartbeat failed",
                            instance_id = %instance_id,
                            error = %error,
                        );
                    }
                    if let Err(error) =
                        reconcile_interrupted_restore_with_heartbeat(&pool, instance_id).await
                    {
                        tracing::error!(
                            message = "Interrupted restore reconciliation failed",
                            instance_id = %instance_id,
                            error = %error,
                        );
                    }
                    tokio::select! {
                        _ = shutdown.requested() => break,
                        _ = actix_rt::time::sleep(StdDuration::from_secs(1)) => {}
                    }
                }
                let _ = delete_server_instance_db(&pool, instance_id).await;
            });
        });
    });
}

pub async fn maintenance_state(pool: &DbPool) -> Result<String, ApiError> {
    maintenance_state_db(pool).await
}

pub async fn identity_scope_name(
    pool: &DbPool,
    identity_scope_id: i32,
) -> Result<String, ApiError> {
    identity_scope_name_db(pool, identity_scope_id).await
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{Duration, NaiveDate};
    use rstest::rstest;
    use serde_json::json;

    use super::{
        MAX_PERSONAL_DEFINITIONS, MAX_SHARED_DEFINITIONS, RESTORE_RECONCILIATION_GRACE_SECONDS,
        RestoreSettings, confirmation_is_stale, validate_computed_field_definitions,
    };
    use crate::models::{BackupDocument, BackupManifest, BackupState, CURRENT_BACKUP_VERSION};

    fn computed_definition(class_id: i32, owner_id: Option<i32>, key: String) -> serde_json::Value {
        json!({
            "class_id": class_id,
            "visibility": if owner_id.is_some() { "personal" } else { "shared" },
            "owner_user_id": owner_id,
            "key": key,
            "label": "Restored field",
            "description": "",
            "operation": {"type": "first_non_null", "paths": ["/value"]},
            "result_type": "string",
            "enabled": true
        })
    }

    fn document_with_computed_definitions(definitions: Vec<serde_json::Value>) -> BackupDocument {
        BackupDocument {
            backup_version: CURRENT_BACKUP_VERSION,
            created_at: NaiveDate::from_ymd_opt(2026, 7, 16)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            source_version: "test".to_string(),
            state: BackupState {
                sections: BTreeMap::from([("computed_field_definitions".to_string(), definitions)]),
            },
            history: None,
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
}
