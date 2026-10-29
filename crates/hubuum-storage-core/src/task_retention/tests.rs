use std::time::Duration;

use chrono::{TimeZone, Utc};
use hubuum_domain::{PrincipalIdentity, TaskIdentity};
use rstest::rstest;

use super::*;
use crate::{StorageTaskKind, StorageTaskTerminalStatus};

fn policy() -> TaskRetentionPolicy {
    TaskRetentionPolicy::new(
        TaskRetentionPolicyRevision::new(0).unwrap(),
        Duration::from_secs(3600),
        Some(Duration::from_secs(60)),
    )
    .unwrap()
}

fn receipt(kind: StorageTaskKind) -> TaskReceipt {
    let identity = TaskSubmissionIdentity::new(
        PrincipalIdentity::generate(),
        ProtectedTaskKey::new("a".repeat(64)).unwrap(),
    );
    let lookup = TaskSubmissionLookup::new(
        identity,
        kind,
        TaskRequestFingerprint::new("b".repeat(64)).unwrap(),
    );
    TaskReceipt::admit(
        TaskReceiptAdmission::new(
            lookup,
            TaskIdentity::generate(),
            Uuid::new_v4(),
            Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        )
        .unwrap(),
        &policy(),
        TaskControlRevision::new(1).unwrap(),
    )
    .unwrap()
}

fn state() -> TaskControlState {
    TaskControlState::initial(Uuid::new_v4(), Uuid::new_v4(), policy()).unwrap()
}

#[rstest]
#[case(StorageTaskKind::Import)]
#[case(StorageTaskKind::Backup)]
#[case(StorageTaskKind::RemoteCall)]
fn terminal_receipts_fit_the_reserved_record_envelope(#[case] kind: StorageTaskKind) {
    let original = receipt(kind);
    let terminal = original
        .settle(TaskReceiptOutcome::Terminal {
            status: StorageTaskTerminalStatus::PartiallySucceeded,
            finished_at: original.admitted_at() + chrono::Duration::days(1),
            remote_side_effect: (kind == StorageTaskKind::RemoteCall)
                .then_some(crate::TaskRemoteSideEffectState::LegacyUnknown),
        })
        .unwrap();
    assert!(terminal.encoded_bytes().unwrap() <= TASK_CONTROL_MAX_RECEIPT_BYTES);
    assert_eq!(
        serde_json::from_value::<TaskReceipt>(serde_json::to_value(&terminal).unwrap()).unwrap(),
        terminal
    );
}

#[test]
fn first_terminal_anchor_cannot_be_renewed_by_retry() {
    let original = receipt(StorageTaskKind::Import);
    let at = original.admitted_at() + chrono::Duration::seconds(5);
    let outcome = TaskReceiptOutcome::Terminal {
        status: StorageTaskTerminalStatus::Succeeded,
        finished_at: at,
        remote_side_effect: None,
    };
    let terminal = original.settle(outcome.clone()).unwrap();
    assert_eq!(terminal.settle(outcome).unwrap(), terminal);
    assert_eq!(terminal.expires_at(), Some(at + chrono::Duration::hours(1)));
    assert!(
        terminal
            .settle(TaskReceiptOutcome::RecoveryUnknown {
                settled_at: at + chrono::Duration::hours(2)
            })
            .is_err()
    );
}

#[test]
fn expiry_requires_independently_known_terminal_anchor() {
    let original = receipt(StorageTaskKind::Import);
    let admitted = state().with_receipt(original.clone()).unwrap();
    let at = original.admitted_at() + chrono::Duration::seconds(5);
    let terminal = original
        .settle(TaskReceiptOutcome::RecoveryUnknown { settled_at: at })
        .unwrap();
    let settled = admitted.with_receipt(terminal.clone()).unwrap();
    let expiry = ReceiptExpiryBatch::new(
        settled.revision(),
        terminal.expires_at().unwrap(),
        vec![terminal.reference().clone()],
    )
    .unwrap();
    let expired = settled.expire(&expiry).unwrap();
    assert!(expired.covers(&settled));
    assert!(
        !expired.covers(&admitted),
        "a timestamp alone cannot erase independently nonterminal evidence"
    );
    assert!(expired.supersedes_for_recovery(&admitted));
    let recovery = TaskControlRecovery::new(admitted, expired.clone()).unwrap();
    assert_eq!(recovery.merged(), &expired);
}

#[test]
fn terminal_growth_uses_admission_reserved_capacity() {
    let initial = state();
    let bounded = TaskControlState::builder(
        initial.lineage(),
        initial.epoch(),
        initial.revision(),
        policy()
            .with_receipt_limits(100, (TASK_CONTROL_MAX_RECEIPT_BYTES + 3) as u64)
            .unwrap(),
    )
    .build()
    .unwrap();
    let original = receipt(StorageTaskKind::Import);
    let admitted = bounded.with_receipt(original.clone()).unwrap();
    let settled = original
        .settle(TaskReceiptOutcome::RecoveryUnknown {
            settled_at: original.admitted_at(),
        })
        .unwrap();
    assert!(admitted.with_receipt(settled).is_ok());
    let extra = receipt(StorageTaskKind::Import);
    let mut encoded = serde_json::to_value(extra).unwrap();
    encoded["ticket"]["created_revision"] = serde_json::json!(2);
    assert!(
        admitted
            .with_receipt(serde_json::from_value(encoded).unwrap())
            .is_err()
    );
}

#[test]
fn snapshot_rejects_duplicate_or_noncanonical_receipt_entries() {
    let original = receipt(StorageTaskKind::Import);
    let admitted = state().with_receipt(original.clone()).unwrap();
    let mut value = serde_json::to_value(admitted).unwrap();
    value["receipts"] = serde_json::json!([original, original]);
    assert!(serde_json::from_value::<TaskControlState>(value).is_err());
}

#[test]
fn legacy_fingerprint_is_bound_to_task_kind() {
    let original = receipt(StorageTaskKind::Import);
    let digest = TaskRequestFingerprint::new("c".repeat(64)).unwrap();
    let lookup = TaskSubmissionLookup::new(
        original.reference().submission().clone(),
        StorageTaskKind::Backup,
        digest.clone(),
    )
    .with_legacy_v1(Some(digest.clone()));
    assert!(!lookup.matches(StorageTaskKind::Import, &TaskFingerprint::LegacyV1(digest)));
}

#[test]
fn purge_manifest_cannot_relabel_distinct_tasks_as_reused_batch() {
    let initial = state();
    let manifest = TaskPurgeManifest::new(
        TaskPurgeBatchId::generate(),
        TaskPurgeScope::TaskDetails,
        vec![TaskIdentity::generate()],
        policy().revision(),
        Utc::now(),
    )
    .unwrap();
    let ticket =
        TaskPurgeTicket::new(manifest.clone(), 1, initial.revision().next().unwrap()).unwrap();
    let prepared = initial
        .with_purge(TaskPurgeDecision::pending(ticket))
        .unwrap();
    let changed = TaskPurgeManifest::new(
        manifest.id(),
        manifest.scope(),
        vec![TaskIdentity::generate()],
        manifest.policy_revision(),
        manifest.eligible_at(),
    )
    .unwrap();
    let ticket = TaskPurgeTicket::new(changed, 2, prepared.revision().next().unwrap()).unwrap();
    assert!(
        prepared
            .with_purge(TaskPurgeDecision::pending(ticket))
            .is_err()
    );
}
