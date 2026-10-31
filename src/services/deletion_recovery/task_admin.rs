//! Bounded administrator operations for protected task receipt activation and retention.

use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, Utc};
use hubuum_domain::TaskIdentity;
use hubuum_storage_core::{
    DeletionCheckpointIdentity, DeletionControlStorage, ProtectedTaskAdmission,
    StorageValidationError, TaskControlChange, TaskControlCheckpoint, TaskControlState,
    TaskExecutionStorage, TaskFingerprint, TaskHoldChange, TaskHoldId, TaskHoldReason,
    TaskPurgeBudget, TaskQueueStorage, TaskReceiptActivation, TaskRetentionHold,
    TaskRetentionPolicy, TaskRetentionPolicyChange, TaskRetentionStorage,
};
use serde::Serialize;

use super::repository::{CheckpointRepository, CheckpointSession, DurableCheckpoint};
use super::task_receipts::{TaskRetentionCompletion, TaskRetentionOutcome};
use super::{TaskReceiptService, covers_checkpoint, invalid_control, unavailable};
use crate::config::ControlRecoveryOptions;
use crate::errors::ApiError;
use crate::storage::{StorageContext, storage_handle};

pub(crate) enum TaskRetentionAdminAction {
    Status,
    Preview,
    Enable {
        confirmation: String,
    },
    SetPolicy {
        deduplication: Duration,
        details: Option<Duration>,
    },
    Hold {
        id: TaskHoldId,
        task: TaskIdentity,
        expires_at: Option<DateTime<Utc>>,
        reason: TaskHoldReason,
    },
    ReleaseHold {
        id: TaskHoldId,
    },
    Run,
}

impl TaskRetentionAdminAction {
    pub(crate) fn requires_execution(&self) -> bool {
        matches!(self, Self::Enable { .. } | Self::Run)
    }
}

#[derive(Serialize)]
pub(crate) struct TaskRetentionAdminReport {
    activation: TaskReceiptActivation,
    task_revision: i64,
    policy: TaskRetentionPolicy,
    retained_receipts: usize,
    retained_purge_manifests: usize,
    holds: Vec<TaskRetentionHold>,
    checkpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint_measurement: Option<super::repository::CheckpointMeasurement>,
    #[serde(skip_serializing_if = "Option::is_none")]
    preview: Option<TaskActivationPreview>,
    backfilled: usize,
    more: bool,
    removed_rows: u32,
    removed_bytes: u64,
}

#[derive(Serialize)]
struct TaskActivationPreview {
    confirmation: String,
    sampled_legacy_tasks: usize,
    sampled_unverifiable_requests: usize,
    bounded_sample: bool,
    deduplication_window_seconds: u64,
    terminal_clock_preserved: bool,
    old_terminal_receipts_may_expire_immediately: bool,
    legacy_details_retention_enabled: bool,
    legacy_queued_tasks_require_protection_before_execution: bool,
}

fn invalid_request(error: StorageValidationError) -> ApiError {
    ApiError::BadRequest(error.to_string())
}

fn report(state: &TaskControlState, checkpoint: &DurableCheckpoint) -> TaskRetentionAdminReport {
    TaskRetentionAdminReport {
        activation: state.activation().clone(),
        task_revision: state.revision().value(),
        policy: state.policy().clone(),
        retained_receipts: state.receipts().len(),
        retained_purge_manifests: state.purge_batches().len(),
        holds: state.holds().values().cloned().collect(),
        checkpoint: checkpoint.identity().to_owned(),
        checkpoint_measurement: super::repository::checkpoint_measurement(),
        preview: None,
        backfilled: 0,
        more: false,
        removed_rows: 0,
        removed_bytes: 0,
    }
}

async fn session(directory: &Path, key: &Path) -> Result<CheckpointSession, ApiError> {
    let directory = directory.to_owned();
    let key = key.to_owned();
    tokio::task::spawn_blocking(move || {
        CheckpointRepository::open(&directory, &key)?.into_session()
    })
    .await
    .map_err(|_| unavailable("Task administration repository worker failed"))?
}

async fn publish(
    context: &impl StorageContext,
    mut session: CheckpointSession,
) -> Result<CheckpointSession, ApiError> {
    let capture = storage_handle(context).capture_control_checkpoint().await?;
    tokio::task::spawn_blocking(move || {
        session.publish_control(&capture)?;
        Ok(session)
    })
    .await
    .map_err(|_| unavailable("Task administration checkpoint worker failed"))?
}

fn checkpoint(session: &CheckpointSession) -> Result<TaskControlCheckpoint, ApiError> {
    let state = session
        .checkpoint()
        .task_control()
        .ok_or_else(|| unavailable("Missing protected task control"))?;
    TaskControlCheckpoint::new(
        state.lineage(),
        state.epoch(),
        state.revision(),
        DeletionCheckpointIdentity::new(session.checkpoint().identity().to_owned())
            .map_err(invalid_control)?,
    )
    .map_err(invalid_control)
}

fn confirmation(
    service: &TaskReceiptService,
    state: &TaskControlState,
) -> Result<String, ApiError> {
    Ok(service
        .fingerprint(
            hubuum_storage_core::StorageTaskKind::Import,
            "administration.task_receipt_activation.v1",
            &(
                state.lineage(),
                state.epoch(),
                state.revision(),
                state.policy(),
                "finite legacy deduplication; original terminal anchor; grandfather task details",
            ),
        )?
        .as_str()
        .to_owned())
}

pub(super) async fn report_sweep(
    context: &impl StorageContext,
    directory: &Path,
    key: &Path,
    outcome: TaskRetentionOutcome,
) -> Result<TaskRetentionAdminReport, ApiError> {
    match outcome.into_completion() {
        TaskRetentionCompletion::Disabled => {
            let session = session(directory, key).await?;
            let state = storage_handle(context).read_task_control().await?;
            Ok(report(&state, session.checkpoint()))
        }
        TaskRetentionCompletion::Protected {
            rows,
            bytes,
            more,
            checkpoint,
        } => {
            let state = checkpoint
                .task_control()
                .ok_or_else(|| unavailable("Cleanup lacks protected task authority"))?;
            let mut output = report(state, &checkpoint);
            output.removed_rows = rows;
            output.removed_bytes = bytes;
            output.more = more;
            Ok(output)
        }
    }
}

pub(crate) async fn administer_task_retention(
    context: &impl StorageContext,
    options: &ControlRecoveryOptions,
    action: TaskRetentionAdminAction,
) -> Result<TaskRetentionAdminReport, ApiError> {
    let (directory, key) = options.paths()?.ok_or_else(|| ApiError::BadRequest(
        "Task retention administration requires --control-recovery-directory and --control-recovery-key".into()))?;
    let service = TaskReceiptService::configured(options).await?;
    if matches!(action, TaskRetentionAdminAction::Run) {
        let outcome = service.sweep_task_retention(context).await?;
        return report_sweep(context, directory, key, outcome).await;
    }
    let mut session = session(directory, key).await?;
    let storage = storage_handle(context);
    let capture = storage.capture_control_checkpoint().await?;
    if !covers_checkpoint(&capture, session.checkpoint()) {
        return Err(unavailable(
            "Recover independently acknowledged control before task administration",
        ));
    }
    let mut state = capture.task_control().clone();
    let configured_key = service.key_fingerprint()?;
    if state
        .activation()
        .key_fingerprint()
        .is_some_and(|key| key != &configured_key)
    {
        return Err(unavailable(
            "Configured key does not own protected task authority",
        ));
    }
    let mut backfilled = 0;
    match action {
        TaskRetentionAdminAction::Status => return Ok(report(&state, session.checkpoint())),
        TaskRetentionAdminAction::Preview => {
            let candidates = storage
                .list_legacy_task_receipts(
                    TaskPurgeBudget::new(64, TaskPurgeBudget::MAX_BYTES)
                        .map_err(invalid_control)?,
                )
                .await?;
            let mut unverifiable = 0;
            for candidate in &candidates {
                if matches!(
                    service.protected_legacy_binding(candidate)?.fingerprint(),
                    TaskFingerprint::LegacyUnverifiable
                ) {
                    unverifiable += 1;
                }
            }
            let mut output = report(&state, session.checkpoint());
            output.preview = Some(TaskActivationPreview {
                confirmation: confirmation(&service, &state)?,
                sampled_legacy_tasks: candidates.len(),
                sampled_unverifiable_requests: unverifiable,
                bounded_sample: true,
                deduplication_window_seconds: state
                    .policy()
                    .deduplication_after_terminal()
                    .as_secs(),
                terminal_clock_preserved: true,
                old_terminal_receipts_may_expire_immediately: true,
                legacy_details_retention_enabled: false,
                legacy_queued_tasks_require_protection_before_execution: true,
            });
            return Ok(output);
        }
        TaskRetentionAdminAction::Enable {
            confirmation: acknowledged,
        } => {
            if matches!(state.activation(), TaskReceiptActivation::Disabled) {
                if acknowledged != confirmation(&service, &state)? {
                    return Err(ApiError::BadRequest("Run --task-retention preview and pass its current confirmation to --task-retention-confirmation before enabling finite legacy deduplication".into()));
                }
                let next = state
                    .with_activation(TaskReceiptActivation::Backfilling {
                        key_fingerprint: service.key_fingerprint()?,
                    })
                    .map_err(invalid_control)?;
                state = storage
                    .commit_task_control(
                        TaskControlChange::new(state, next).map_err(invalid_control)?,
                    )
                    .await?;
                session = publish(context, session).await?;
            }
            if matches!(
                state.activation(),
                TaskReceiptActivation::Backfilling { .. }
            ) {
                storage.recover_expired_task_leases(64).await?;
                state = storage.read_task_control().await?;
                let candidates = storage
                    .list_legacy_task_receipts(
                        TaskPurgeBudget::new(64, TaskPurgeBudget::MAX_BYTES)
                            .map_err(invalid_control)?,
                    )
                    .await?;
                if !candidates.is_empty() {
                    let bindings = candidates
                        .iter()
                        .map(|candidate| service.protected_legacy_binding(candidate))
                        .collect::<Result<Vec<_>, _>>()?;
                    backfilled = bindings.len();
                    storage
                        .backfill_task_receipts(state.revision(), bindings)
                        .await?;
                    session = publish(context, session).await?;
                }
                for ticket in storage.list_pending_task_admissions(64).await? {
                    let covered = session.checkpoint().task_control().is_some_and(|control| {
                        control
                            .receipt(ticket.receipt().submission())
                            .is_some_and(|receipt| receipt.ticket() == &ticket)
                    });
                    if !covered {
                        session = publish(context, session).await?;
                    }
                    if !session.checkpoint().task_control().is_some_and(|control| {
                        control
                            .receipt(ticket.receipt().submission())
                            .is_some_and(|receipt| receipt.ticket() == &ticket)
                    }) {
                        return Err(unavailable(
                            "Legacy admission changed before independent protection",
                        ));
                    }
                    storage
                        .activate_task_submission(
                            ProtectedTaskAdmission::new(
                                ticket,
                                checkpoint(&session)?,
                                storage.read_application_control().await?.fence(),
                            )
                            .map_err(invalid_control)?,
                        )
                        .await?;
                }
                if storage
                    .list_legacy_task_receipts(
                        TaskPurgeBudget::new(1, TaskPurgeBudget::MAX_BYTES)
                            .map_err(invalid_control)?,
                    )
                    .await?
                    .is_empty()
                    && storage.list_pending_task_admissions(1).await?.is_empty()
                {
                    state = storage.read_task_control().await?;
                    let next = state
                        .with_activation(TaskReceiptActivation::Protected {
                            key_fingerprint: service.key_fingerprint()?,
                        })
                        .map_err(invalid_control)?;
                    storage
                        .commit_task_control(
                            TaskControlChange::new(state, next).map_err(invalid_control)?,
                        )
                        .await?;
                }
            }
        }
        TaskRetentionAdminAction::SetPolicy {
            deduplication,
            details,
        } => {
            let old = state.policy();
            let policy = TaskRetentionPolicy::new(
                old.revision().next().map_err(invalid_request)?,
                deduplication,
                details,
            )
            .map_err(invalid_request)?
            .with_pending_protection_timeout(old.pending_protection_timeout())
            .map_err(invalid_request)?
            .with_receipt_limits(old.max_receipts(), old.max_receipt_bytes())
            .map_err(invalid_request)?
            .with_cleanup_budget(old.cleanup_budget());
            storage
                .replace_task_retention_policy(TaskRetentionPolicyChange::new(
                    state.policy().revision(),
                    policy,
                ))
                .await?;
        }
        TaskRetentionAdminAction::Hold {
            id,
            task,
            expires_at,
            reason,
        } => {
            let hold = match state.holds().get(&id) {
                Some(existing)
                    if existing.task() == task
                        && existing.expires_at() == expires_at
                        && existing.reason() == reason =>
                {
                    existing.clone()
                }
                Some(_) => {
                    return Err(ApiError::BadRequest(
                        "Hold identity already names a different decision".into(),
                    ));
                }
                None => TaskRetentionHold::new(id, task, Utc::now(), expires_at, reason)
                    .map_err(invalid_request)?,
            };
            storage
                .change_task_hold(TaskHoldChange::new(&state, hold))
                .await?;
        }
        TaskRetentionAdminAction::ReleaseHold { id } => {
            let hold = state
                .holds()
                .get(&id)
                .ok_or_else(|| ApiError::NotFound("Task hold was not found".into()))?
                .release(Utc::now())
                .map_err(invalid_request)?;
            storage
                .change_task_hold(TaskHoldChange::new(&state, hold))
                .await?;
        }
        TaskRetentionAdminAction::Run => {
            unreachable!("handled before acquiring publication session")
        }
    }
    session = publish(context, session).await?;
    state = storage.read_task_control().await?;
    let mut output = report(&state, session.checkpoint());
    output.backfilled = backfilled;
    output.more = matches!(
        state.activation(),
        TaskReceiptActivation::Backfilling { .. }
    );
    Ok(output)
}

#[cfg(test)]
mod tests;
