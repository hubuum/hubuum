//! Synthetic encoding bounds, separate from real-clock lifecycle measurements.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{TimeZone, Timelike, Utc};
use hubuum_domain::{PrincipalIdentity, TaskIdentity};

use super::*;
use crate::{StorageTaskKind, StorageTaskTerminalStatus, TaskRemoteSideEffectState};

fn identity(index: u128) -> Uuid {
    Uuid::from_u128((4 << 76) | (2 << 62) | index)
}

fn maximum_policy(revision: i64) -> TaskRetentionPolicy {
    let duration = Duration::from_secs(100 * 366 * 24 * 60 * 60);
    TaskRetentionPolicy::new(
        TaskRetentionPolicyRevision::new(revision).unwrap(),
        duration,
        Some(duration),
    )
    .unwrap()
    .with_pending_protection_timeout(duration)
    .unwrap()
}

fn largest_receipt(
    ticket: TaskAdmissionTicket,
    at: chrono::DateTime<Utc>,
    policy: &TaskRetentionPolicy,
) -> TaskReceipt {
    let mut largest = None;
    for kind in StorageTaskKind::ALL {
        let effects = if kind == StorageTaskKind::RemoteCall {
            vec![
                Some(TaskRemoteSideEffectState::NotSent),
                Some(TaskRemoteSideEffectState::PossiblySent),
                Some(TaskRemoteSideEffectState::LegacyUnknown),
            ]
        } else {
            vec![None]
        };
        let mut outcomes = vec![
            TaskReceiptOutcome::MayExecute,
            TaskReceiptOutcome::RecoveryUnknown { settled_at: at },
        ];
        for status in [
            StorageTaskTerminalStatus::Succeeded,
            StorageTaskTerminalStatus::Failed,
            StorageTaskTerminalStatus::PartiallySucceeded,
            StorageTaskTerminalStatus::Cancelled,
        ] {
            for effect in &effects {
                outcomes.push(TaskReceiptOutcome::Terminal {
                    status,
                    finished_at: at,
                    remote_side_effect: *effect,
                });
            }
        }
        for fingerprint in [
            TaskFingerprint::CanonicalV1(TaskRequestFingerprint::new("f".repeat(64)).unwrap()),
            TaskFingerprint::LegacyV1(TaskRequestFingerprint::new("f".repeat(64)).unwrap()),
            TaskFingerprint::LegacyUnverifiable,
        ] {
            for outcome in &outcomes {
                let receipt = TaskReceipt::from_parts(
                    ticket.clone(),
                    kind,
                    fingerprint.clone(),
                    at,
                    policy,
                    outcome.clone(),
                )
                .unwrap();
                let bytes = receipt.encoded_bytes().unwrap();
                assert!(
                    bytes <= TASK_CONTROL_MAX_RECEIPT_BYTES,
                    "{kind:?}/{fingerprint:?}/{outcome:?} needs {bytes} bytes"
                );
                if largest
                    .as_ref()
                    .is_none_or(|previous: &TaskReceipt| previous.encoded_bytes().unwrap() < bytes)
                {
                    largest = Some(receipt);
                }
            }
        }
    }
    largest.unwrap()
}

#[rstest::rstest]
#[case::exact_framed_reservation(1027, true)]
#[case::one_byte_short(1026, false)]
fn receipt_capacity_includes_collection_framing(#[case] bytes: u64, #[case] accepted: bool) {
    let policy = maximum_policy(0).with_receipt_limits(1, bytes).unwrap();
    let at = Utc
        .with_ymd_and_hms(262000, 12, 31, 23, 59, 59)
        .unwrap()
        .with_nanosecond(999_999_999)
        .unwrap();
    let ticket = TaskAdmissionTicket::new(
        TaskReceiptReference::new(
            TaskSubmissionIdentity::new(
                PrincipalIdentity::generate(),
                ProtectedTaskKey::new("f".repeat(64)).unwrap(),
            ),
            TaskReceiptGeneration::generate(),
            TaskIdentity::generate(),
        ),
        TaskControlRevision::new(1).unwrap(),
        identity(1),
        at + chrono::Duration::seconds(1),
    )
    .unwrap();
    let receipt = largest_receipt(ticket, at, &policy);
    let state = TaskControlState::builder(
        identity(2),
        identity(3),
        TaskControlRevision::new(1).unwrap(),
        policy,
    )
    .receipts(vec![receipt])
    .build();
    assert_eq!(state.is_ok(), accepted);
}

#[test]
fn ten_thousand_receipts_and_157_manifests_fit_the_declared_encoding_budgets() {
    let long_window = Duration::from_secs(100 * 366 * 24 * 60 * 60);
    let policy = maximum_policy(i64::MAX);
    let at = Utc
        .with_ymd_and_hms(262000, 12, 31, 23, 59, 59)
        .unwrap()
        .with_nanosecond(999_999_999)
        .unwrap();
    let lineage = identity(1);
    let epoch = identity(2);
    let revision = TaskControlRevision::new(i64::MAX).unwrap();
    let checkpoint = TaskControlCheckpoint::new(
        lineage,
        epoch,
        revision,
        crate::DeletionCheckpointIdentity::new("f".repeat(64)).unwrap(),
    )
    .unwrap();
    let principal = PrincipalIdentity::from_bytes(*identity(3).as_bytes()).unwrap();
    let mut receipts = Vec::new();
    let mut tasks = Vec::new();
    let mut largest: Option<TaskReceipt> = None;
    for index in 1..=10000 {
        let task = TaskIdentity::from_bytes(*identity(index + 100).as_bytes()).unwrap();
        let reference = TaskReceiptReference::new(
            TaskSubmissionIdentity::new(
                principal,
                ProtectedTaskKey::new(format!("{index:064x}")).unwrap(),
            ),
            TaskReceiptGeneration::new(identity(index + 20000)).unwrap(),
            task,
        );
        let ticket = TaskAdmissionTicket::new(
            reference,
            revision,
            identity(4),
            at + chrono::Duration::seconds(long_window.as_secs() as i64),
        )
        .unwrap();
        let template = largest.get_or_insert_with(|| largest_receipt(ticket.clone(), at, &policy));
        let receipt = TaskReceipt::from_parts(
            ticket,
            template.kind(),
            template.fingerprint().clone(),
            at,
            &policy,
            template.outcome().clone(),
        )
        .unwrap();
        assert!(receipt.encoded_bytes().unwrap() <= TASK_CONTROL_MAX_RECEIPT_BYTES);
        receipts.push(receipt);
        tasks.push(task);
    }
    let mut purges = BTreeMap::new();
    for (index, members) in tasks.chunks(64).enumerate() {
        let manifest = TaskPurgeManifest::new(
            TaskPurgeBatchId::new(identity(index as u128 + 40000)).unwrap(),
            TaskPurgeScope::TaskDetails,
            members.to_vec(),
            policy.revision(),
            at,
        )
        .unwrap();
        let ticket = TaskPurgeTicket::new(manifest.clone(), i64::MAX, revision).unwrap();
        let decision = TaskPurgeDecision::pending(ticket)
            .record_removed(checkpoint.clone())
            .unwrap();
        purges.insert(manifest.id(), decision);
    }
    assert_eq!(purges.len(), 157);
    let holds = tasks
        .iter()
        .take(TASK_CONTROL_MAX_HOLDS)
        .enumerate()
        .map(|(index, task)| {
            let id = TaskHoldId::new(identity(index as u128 + 50000)).unwrap();
            (
                id,
                TaskRetentionHold::new(
                    id,
                    *task,
                    at,
                    Some(at + chrono::Duration::seconds(1)),
                    TaskHoldReason::Incident,
                )
                .unwrap()
                .release(at + chrono::Duration::seconds(1))
                .unwrap(),
            )
        })
        .collect();
    let policies = (0..TASK_CONTROL_MAX_POLICIES)
        .map(|index| {
            let policy = maximum_policy(i64::MAX - index as i64);
            (policy.revision(), policy)
        })
        .collect();
    let state = TaskControlState::builder(lineage, epoch, revision, policy)
        .activation(TaskReceiptActivation::Backfilling {
            key_fingerprint: ControlKeyFingerprint::new("f".repeat(64)).unwrap(),
        })
        .hold_release_revision(revision)
        .receipt_expiry_floor(at)
        .policies(policies)
        .receipts(receipts)
        .purge_batches(purges)
        .holds(holds)
        .build()
        .unwrap();
    let receipt_bytes = serde_json::to_vec(state.receipts()).unwrap().len();
    let retention_bytes = serde_json::to_vec(&(
        state.activation(),
        state.policies(),
        state.holds(),
        state.purge_batches(),
    ))
    .unwrap()
    .len();
    let total_bytes = serde_json::to_vec(&state).unwrap().len();
    assert!(receipt_bytes <= TASK_CONTROL_MAX_RECEIPTS_BYTES);
    assert!(retention_bytes <= TASK_CONTROL_MAX_RETENTION_BYTES);
    assert!(total_bytes <= TASK_CONTROL_MAX_RECEIPTS_BYTES + TASK_CONTROL_MAX_RETENTION_BYTES);
    assert_eq!(
        serde_json::from_slice::<TaskControlState>(&serde_json::to_vec(&state).unwrap()).unwrap(),
        state
    );
    let pending = state
        .purge_batches()
        .iter()
        .map(|(id, decision)| (*id, TaskPurgeDecision::pending(decision.ticket().clone())))
        .collect();
    let pending = TaskControlState::builder(lineage, epoch, revision, state.policy().clone())
        .activation(state.activation().clone())
        .hold_release_revision(revision)
        .receipt_expiry_floor(at)
        .policies(state.policies().clone())
        .receipts(state.receipts().to_vec())
        .holds(state.holds().clone())
        .purge_batches(pending)
        .build()
        .unwrap();
    let pending_bytes = serde_json::to_vec(&(
        pending.activation(),
        pending.policies(),
        pending.holds(),
        pending.purge_batches(),
    ))
    .unwrap()
    .len();
    let pending_reserved =
        pending_bytes + pending.purge_batches().len() * TASK_CONTROL_PURGE_COMPLETION_RESERVE;
    assert!(pending_reserved <= TASK_CONTROL_MAX_RETENTION_BYTES);
    eprintln!(
        "task_control_size_proof receipts=10000 manifests=157 holds={} receipt_bytes={receipt_bytes} retention_bytes={retention_bytes} pending_reserved_bytes={pending_reserved} complete_state_bytes={total_bytes}",
        state.holds().len()
    );
}
