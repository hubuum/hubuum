//! Real publisher/reader size proofs; synthetic records are not latency evidence.

use chrono::{TimeZone, Timelike, Utc};
use hubuum_domain::{PrincipalIdentity, TaskIdentity};
use hubuum_storage_core::{
    ControlCheckpointCapture, DELETION_CONTROL_MAX_BYTES, DELETION_CONTROL_MAX_COPIES,
    DELETION_CONTROL_MAX_INTENTS, ProtectedTaskKey, StorageTaskKind, StorageTaskTerminalStatus,
    TASK_CONTROL_MAX_HOLDS, TASK_CONTROL_MAX_POLICIES, TaskAdmissionTicket, TaskControlCheckpoint,
    TaskControlRevision, TaskControlState, TaskFingerprint, TaskHoldId, TaskHoldReason,
    TaskPurgeBatchId, TaskPurgeDecision, TaskPurgeManifest, TaskPurgeScope, TaskPurgeTicket,
    TaskReceipt, TaskReceiptActivation, TaskReceiptGeneration, TaskReceiptOutcome,
    TaskReceiptReference, TaskRemoteSideEffectState, TaskRequestFingerprint, TaskRetentionHold,
    TaskRetentionPolicy, TaskRetentionPolicyRevision, TaskSubmissionIdentity,
};

use super::tests::Fixture;
use super::*;

fn id(value: u128) -> Uuid {
    Uuid::from_u128((4 << 76) | (2 << 62) | value)
}

#[test]
fn production_composite_publisher_round_trips_ten_thousand_receipts_and_bounds_live_files() {
    let fixture = Fixture::new();
    let mut deletion = fixture.initial.clone();
    let lineage = deletion.state().lineage();
    let epoch = deletion.state().epoch();
    let window = std::time::Duration::from_secs(100 * 366 * 24 * 60 * 60);
    let policy = TaskRetentionPolicy::new(
        TaskRetentionPolicyRevision::new(i64::MAX - 3).unwrap(),
        window,
        Some(window),
    )
    .unwrap()
    .with_pending_protection_timeout(window)
    .unwrap();
    let initial = TaskControlState::initial(lineage, epoch, policy.clone()).unwrap();
    fixture
        .repository
        .publish_control(
            &ControlCheckpointCapture::new(deletion.clone(), initial).unwrap(),
            true,
        )
        .unwrap();
    // Include a large independent deletion authority, rather than proving only
    // the task component of the real composite writer. This is a supported
    // maximum-count legacy shape, not a claim that every 4 MiB shape is tested.
    let mut material = serde_json::to_value(deletion.state()).unwrap();
    material["revision"] = serde_json::json!(i64::MAX);
    material["intents"] = serde_json::Value::Object((0..DELETION_CONTROL_MAX_INTENTS).map(|index| {
        let copies = (0..DELETION_CONTROL_MAX_COPIES).map(|copy|
            (id(100_000 + (index * DELETION_CONTROL_MAX_COPIES + copy) as u128).to_string(), serde_json::json!("pending"))).collect::<serde_json::Map<_,_>>();
        (id(index as u128 + 80_000).to_string(),serde_json::json!({
            "subject":{"kind":"resource","family":"query_usage_declaration","identity":id(index as u128 + 90_000)},
            "generation":i64::MAX,"copies":copies
        }))
    }).collect());
    let populated: DeletionControlState = serde_json::from_value(material).unwrap();
    let deletion_bytes = populated.encode().unwrap().len();
    assert!(deletion_bytes <= DELETION_CONTROL_MAX_BYTES);
    deletion = DeletionControlCapture::new(populated, deletion.position().clone());
    let revision = TaskControlRevision::new(i64::MAX - 3).unwrap();
    let principal = PrincipalIdentity::from_bytes(*id(1).as_bytes()).unwrap();
    let at = Utc
        .with_ymd_and_hms(262000, 12, 31, 23, 59, 59)
        .unwrap()
        .with_nanosecond(999_999_999)
        .unwrap();
    let mut receipts = Vec::new();
    let mut tasks = Vec::new();
    for index in 1..=10000 {
        let task = TaskIdentity::from_bytes(*id(index + 10).as_bytes()).unwrap();
        let reference = TaskReceiptReference::new(
            TaskSubmissionIdentity::new(
                principal,
                ProtectedTaskKey::new(format!("{index:064x}")).unwrap(),
            ),
            TaskReceiptGeneration::new(id(index + 20000)).unwrap(),
            task,
        );
        let ticket = TaskAdmissionTicket::new(
            reference,
            revision,
            id(2),
            at + chrono::Duration::seconds(window.as_secs() as i64),
        )
        .unwrap();
        receipts.push(
            TaskReceipt::from_parts(
                ticket,
                StorageTaskKind::RemoteCall,
                TaskFingerprint::CanonicalV1(TaskRequestFingerprint::new("f".repeat(64)).unwrap()),
                at,
                &policy,
                TaskReceiptOutcome::Terminal {
                    status: StorageTaskTerminalStatus::PartiallySucceeded,
                    finished_at: at,
                    remote_side_effect: Some(TaskRemoteSideEffectState::LegacyUnknown),
                },
            )
            .unwrap(),
        );
        tasks.push(task);
    }
    let purges = tasks
        .chunks(64)
        .enumerate()
        .map(|(index, members)| {
            let manifest = TaskPurgeManifest::new(
                TaskPurgeBatchId::new(id(index as u128 + 40000)).unwrap(),
                TaskPurgeScope::TaskDetails,
                members.to_vec(),
                policy.revision(),
                at,
            )
            .unwrap();
            let ticket = TaskPurgeTicket::new(manifest.clone(), i64::MAX, revision).unwrap();
            (manifest.id(), TaskPurgeDecision::pending(ticket))
        })
        .collect();
    let policies = (0..TASK_CONTROL_MAX_POLICIES)
        .map(|index| {
            let policy = TaskRetentionPolicy::new(
                TaskRetentionPolicyRevision::new(i64::MAX - 3 - index as i64).unwrap(),
                window,
                Some(window),
            )
            .unwrap()
            .with_pending_protection_timeout(window)
            .unwrap();
            (policy.revision(), policy)
        })
        .collect();
    let holds = tasks
        .iter()
        .take(TASK_CONTROL_MAX_HOLDS)
        .enumerate()
        .map(|(index, task)| {
            let hold_id = TaskHoldId::new(id(index as u128 + 50_000)).unwrap();
            let hold = TaskRetentionHold::new(
                hold_id,
                *task,
                at,
                Some(at + chrono::Duration::seconds(1)),
                TaskHoldReason::Incident,
            )
            .unwrap();
            (hold_id, hold)
        })
        .collect();
    let state = TaskControlState::builder(lineage, epoch, revision, policy)
        .activation(TaskReceiptActivation::Protected {
            key_fingerprint: fixture
                .repository
                .task_receipt_key()
                .unwrap()
                .fingerprint()
                .unwrap(),
        })
        .receipt_expiry_floor(at)
        .policies(policies)
        .holds(holds)
        .receipts(receipts)
        .purge_batches(purges)
        .build()
        .unwrap();
    let capture = ControlCheckpointCapture::new(deletion.clone(), state.clone()).unwrap();
    let first = fixture.repository.publish_control(&capture, false).unwrap();
    let released_holds = state
        .holds()
        .iter()
        .map(|(id, hold)| {
            (
                *id,
                hold.release(at + chrono::Duration::seconds(1)).unwrap(),
            )
        })
        .collect();
    let released_revision = revision.next().unwrap();
    let released =
        TaskControlState::builder(lineage, epoch, released_revision, state.policy().clone())
            .activation(state.activation().clone())
            .receipt_expiry_floor(state.receipt_expiry_floor())
            .hold_release_revision(released_revision)
            .policies(state.policies().clone())
            .holds(released_holds)
            .receipts(state.receipts().to_vec())
            .purge_batches(state.purge_batches().clone())
            .build()
            .unwrap();
    let release_checkpoint = fixture
        .repository
        .publish_control(
            &ControlCheckpointCapture::new(deletion.clone(), released.clone()).unwrap(),
            false,
        )
        .unwrap();
    let proof = TaskControlCheckpoint::new(
        lineage,
        epoch,
        released_revision,
        hubuum_storage_core::DeletionCheckpointIdentity::new(
            release_checkpoint.identity().to_owned(),
        )
        .unwrap(),
    )
    .unwrap();
    let removed = released
        .purge_batches()
        .iter()
        .map(|(id, batch)| (*id, batch.record_removed(proof.clone()).unwrap()))
        .collect();
    let completed = TaskControlState::builder(
        lineage,
        epoch,
        released_revision.next().unwrap(),
        released.policy().clone(),
    )
    .activation(released.activation().clone())
    .receipt_expiry_floor(released.receipt_expiry_floor())
    .hold_release_revision(released.hold_release_revision())
    .policies(released.policies().clone())
    .holds(released.holds().clone())
    .receipts(released.receipts().to_vec())
    .purge_batches(removed)
    .build()
    .unwrap();
    let second = fixture
        .repository
        .publish_control(
            &ControlCheckpointCapture::new(deletion.clone(), completed.clone()).unwrap(),
            false,
        )
        .unwrap();
    assert_eq!(second.task_control(), Some(&completed));
    let advanced = completed.record_reapplication().unwrap();
    let last = fixture
        .repository
        .publish_control(
            &ControlCheckpointCapture::new(deletion, advanced.clone()).unwrap(),
            false,
        )
        .unwrap();
    assert_eq!(
        fixture.repository.latest_locked().unwrap().task_control(),
        Some(&advanced)
    );
    let files = fs::read_dir(&fixture.repository.root)
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .collect::<Vec<_>>();
    let live_bytes: u64 = files.iter().sum();
    assert!(live_bytes <= 3 * MAX_FILE_BYTES + 64 * 1024);
    assert!(
        fixture
            .repository
            .root
            .join(format!("{}.json", second.identity()))
            .exists()
    );
    assert!(
        fixture
            .repository
            .root
            .join(format!("{}.json", last.identity()))
            .exists()
    );
    assert!(
        !fixture
            .repository
            .root
            .join(format!("{}.json", first.identity()))
            .exists()
    );
    fixture
        .repository
        .reserve_publication_space(MAX_FILE_BYTES)
        .unwrap();
    assert!(
        fixture
            .repository
            .reserve_publication_space(3 * MAX_FILE_BYTES + 64 * 1024)
            .is_err()
    );
    eprintln!(
        "composite_size_proof receipts=10000 manifests=157 policies=64 released_holds=1024 deletion_bytes={deletion_bytes} signed_checkpoint_bytes={} live_repository_bytes={live_bytes} files={} repository_limit={}",
        fs::metadata(
            fixture
                .repository
                .root
                .join(format!("{}.json", last.identity()))
        )
        .unwrap()
        .len(),
        files.len(),
        3 * MAX_FILE_BYTES + 64 * 1024
    );
}
