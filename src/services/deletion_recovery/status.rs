use std::path::Path;

use hubuum_storage_core::{
    ApplicationControlState, ControlCheckpointCapture, DeletionAudit, DeletionControlState,
    DeletionControlStorage, DeletionCopyState, StorageRecordIdentity, TaskRetentionStorage,
};
use serde::Serialize;
use uuid::Uuid;

use super::{RecoveryEvidence, evidence};
use crate::errors::ApiError;
use crate::storage::{StorageContext, storage_handle};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RecoveryProtection {
    Pending,
    Protected,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum NextAction {
    RestoreIndependentEvidence,
    ResolveControlLineage,
    RecoverControl,
    PublishCheckpoint,
    ResolveCopyObligations,
    RetryManagedCleanup,
    ReapplyControl,
    None,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum BlockReason {
    EvidenceUnavailable,
    LineageOrEpochMismatch,
    RecoveryRequired,
}

/// Diagnostic observations never grant authority to clean up or reopen access.
#[derive(Serialize)]
pub(crate) struct DeletionStatusReport {
    format: u32,
    control_revision: i64,
    checkpoint_revision: Option<i64>,
    recovery_protection: RecoveryProtection,
    block_reason: Option<BlockReason>,
    access_restricted: bool,
    next_action: NextAction,
    intents: Vec<IntentStatus>,
}

#[derive(Serialize)]
struct IntentStatus {
    intent: Uuid,
    generation: i64,
    recovery_protection: RecoveryProtection,
    pending_copies: usize,
    blocked_copies: usize,
    recorded_removed_copies: usize,
    unbound_copies: usize,
    confirmed_managed_scope_completion: bool,
    next_action: NextAction,
    audit_evidence: IntentAuditEvidence,
}

#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum IntentAuditEvidence {
    Retained {
        target: StorageRecordIdentity,
        audit: DeletionAudit,
    },
    NotRecorded,
}

pub(crate) async fn status(
    context: &impl StorageContext,
    directory: &Path,
    key: &Path,
) -> Result<DeletionStatusReport, ApiError> {
    // Keep the authenticated latest-reference lock across both primary reads.
    // Failure is a bounded observation, never a successful verification result.
    let required = evidence(directory, key).await.ok();
    let storage = storage_handle(context);
    let current = storage.capture_control_checkpoint().await?;
    let application = storage.read_application_control().await?;
    Ok(observe(&current, &application, required.as_ref()))
}

fn observe(
    capture: &ControlCheckpointCapture,
    application: &ApplicationControlState,
    required: Option<&RecoveryEvidence>,
) -> DeletionStatusReport {
    let current = capture.deletion().state();
    let authority_covered =
        required.is_some_and(|evidence| super::covers_checkpoint(capture, evidence.checkpoint()));
    let (recovery_protection, block_reason, next_action) = match required {
        None => (
            RecoveryProtection::Blocked,
            Some(BlockReason::EvidenceUnavailable),
            NextAction::RestoreIndependentEvidence,
        ),
        Some(evidence)
            if current.lineage() != evidence.state().lineage()
                || current.epoch() != evidence.state().epoch() =>
        {
            (
                RecoveryProtection::Blocked,
                Some(BlockReason::LineageOrEpochMismatch),
                NextAction::ResolveControlLineage,
            )
        }
        Some(_) if !authority_covered => (
            RecoveryProtection::Blocked,
            Some(BlockReason::RecoveryRequired),
            NextAction::RecoverControl,
        ),
        Some(evidence)
            if !evidence.state().covers(current)
                || !evidence
                    .checkpoint()
                    .task_control()
                    .map_or(capture.task_control().revision().value() == 0, |tasks| {
                        tasks.covers(capture.task_control())
                    }) =>
        {
            (
                RecoveryProtection::Pending,
                None,
                NextAction::PublishCheckpoint,
            )
        }
        Some(_) => (RecoveryProtection::Protected, None, NextAction::None),
    };
    let released = authority_covered
        && application.is_released_for(current)
        && required.is_some_and(|evidence| {
            super::require_activation_checkpoint(capture, evidence.checkpoint()).is_ok()
        })
        && super::reapplication_is_protected(
            capture,
            application,
            required.map(RecoveryEvidence::checkpoint),
        );
    let intents: Vec<_> = current
        .intents()
        .iter()
        .map(|(identity, intent)| {
            let protection = if !authority_covered {
                RecoveryProtection::Blocked
            } else if required
                .is_some_and(|evidence| evidence.state().intents().get(identity) == Some(intent))
            {
                RecoveryProtection::Protected
            } else {
                RecoveryProtection::Pending
            };
            let count = |state| {
                intent
                    .copies()
                    .values()
                    .filter(|value| **value == state)
                    .count()
            };
            let pending = count(DeletionCopyState::Pending);
            let blocked = count(DeletionCopyState::Blocked);
            let unbound = intent.copies().len() - intent.bindings().len();
            let complete = protection == RecoveryProtection::Protected
                && released
                && unbound == 0
                && intent.is_physically_complete();
            let action = if protection == RecoveryProtection::Blocked {
                next_action
            } else if protection == RecoveryProtection::Pending {
                NextAction::PublishCheckpoint
            } else if blocked > 0 || unbound > 0 {
                NextAction::ResolveCopyObligations
            } else if pending > 0 {
                NextAction::RetryManagedCleanup
            } else if !released {
                NextAction::ReapplyControl
            } else {
                NextAction::None
            };
            IntentStatus {
                intent: *identity,
                generation: intent.generation(),
                recovery_protection: protection,
                pending_copies: pending,
                blocked_copies: blocked,
                recorded_removed_copies: count(DeletionCopyState::Removed),
                unbound_copies: unbound,
                confirmed_managed_scope_completion: complete,
                next_action: action,
                audit_evidence: match (intent.audit(), intent.bindings().values().next()) {
                    (Some(audit), Some(binding)) => IntentAuditEvidence::Retained {
                        target: binding.target(),
                        audit: audit.clone(),
                    },
                    _ => IntentAuditEvidence::NotRecorded,
                },
            }
        })
        .collect();
    let next_action = if next_action != NextAction::None {
        next_action
    } else if let Some(intent) = intents
        .iter()
        .find(|intent| intent.next_action != NextAction::None)
    {
        intent.next_action
    } else if !released {
        NextAction::ReapplyControl
    } else {
        NextAction::None
    };
    DeletionStatusReport {
        format: 1,
        control_revision: current.revision(),
        checkpoint_revision: required.map(|evidence| evidence.state().revision()),
        recovery_protection,
        block_reason,
        access_restricted: !released,
        next_action,
        intents,
    }
}
