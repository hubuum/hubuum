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

fn completion_reserved_history() -> hubuum_storage_core::HistoryLifecycleAuthority {
    use hubuum_domain::{ResourceIdentity, VersionIdentity};
    use hubuum_storage_core::{
        DeletionCheckpointIdentity, HistoryDeletionRevision, HistoryExpiryCandidate,
        HistoryExpiryExactDeletion, HistoryExpiryInputBytes, HistoryExpiryJournal,
        HistoryExpiryOrigin, HistoryExpiryReadSpan, HistoryExpirySelection,
        HistoryExpirySelectionOutcome, HistoryExpiryTrigger, HistoryLifecycleControl,
        HistoryLifecycleScan, HistoryLifecycleSnapshot, HistoryPolicyApplication,
        HistoryPolicyApplyRequest, HistoryPolicyBinding, HistoryPolicyChange, HistoryPolicyKey,
        HistoryPolicyScope, HistoryRetentionAnchor, HistoryRetentionSeconds,
        HistoryScopeAttribution, StorageIdentityHistoryKind, StorageRecordIdentity,
    };
    let at = |seconds| Utc.timestamp_opt(seconds, 0).unwrap();
    let principal = PrincipalIdentity::generate();
    let key = HistoryPolicyKey::new(
        StorageIdentityHistoryKind::Collection,
        HistoryPolicyScope::Deployment,
    )
    .unwrap();
    let initial = HistoryLifecycleSnapshot::new(
        HistoryLifecycleControl::empty(),
        id(6_000_000),
        HistoryDeletionRevision::new(0).unwrap(),
        at(100),
    )
    .unwrap();
    let proposal = HistoryPolicyChange::new(
        key,
        initial
            .control()
            .authority()
            .policies()
            .current_revision(key),
        HistoryPolicyBinding::Expire {
            trigger: HistoryExpiryTrigger::Age {
                age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                deleted_age_seconds: None,
            },
            minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
        },
        HistoryPolicyApplication::Prospective,
    )
    .unwrap();
    let mut control = initial
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(proposal, None),
            principal,
            initial.fence(),
            at(100),
        )
        .unwrap()
        .next()
        .clone();
    let mut exhausted = false;
    for batch in 0..HistoryExpiryJournal::MAX_INTENTS {
        let snapshot = HistoryLifecycleSnapshot::new(
            control.clone(),
            initial.fence().incarnation(),
            initial.fence().deletion_revision(),
            at(200),
        )
        .unwrap();
        let mut selection = HistoryExpirySelection::new(
            id(7_000_000 + batch as u128),
            HistoryExpiryOrigin::Manual { principal },
            HistoryLifecycleScan::new(key, 1000).unwrap(),
            &snapshot,
        )
        .unwrap();
        for offset in 0..1000 {
            let sequence = (batch * 1000 + offset) as u128;
            let resource =
                ResourceIdentity::from_bytes(*id(8_000_000 + sequence).as_bytes()).unwrap();
            let target = StorageRecordIdentity::new(
                resource,
                VersionIdentity::from_bytes(*id(9_000_000 + sequence).as_bytes()).unwrap(),
            );
            let candidate = HistoryExpiryCandidate::new(
                StorageIdentityHistoryKind::Collection,
                target,
                Some(HistoryRetentionAnchor::Closed(at(150))),
                vec![HistoryScopeAttribution::new(resource, None)],
                false,
            )
            .unwrap()
            .with_read_span(HistoryExpiryReadSpan::closed(at(110), at(150)).unwrap())
            .unwrap();
            match selection
                .consider(candidate, HistoryExpiryInputBytes::new(1).unwrap(), &[])
                .unwrap()
            {
                HistoryExpirySelectionOutcome::Selected => {}
                HistoryExpirySelectionOutcome::BudgetReached => {
                    exhausted = true;
                    break;
                }
                HistoryExpirySelectionOutcome::Blocked(_) => {
                    panic!("Maximum fixture members must remain eligible")
                }
            }
        }
        control = control
            .admit_expiry(selection.finish().unwrap(), snapshot.fence())
            .expect("Selected maximum fixture batch must fit its completion reservation")
            .next()
            .clone();
        if exhausted {
            break;
        }
    }
    assert!(
        exhausted,
        "Fixture must reach the actual completion-reserved admission ceiling"
    );
    // Populate the reserved terminal shape with validated exact-outcome values.
    // This is an encoding/RSS fixture, never native physical-removal evidence.
    let mut material = serde_json::to_value(control.authority()).unwrap();
    for intent in material["expiry"].as_array_mut().unwrap() {
        let original: hubuum_storage_core::HistoryExpiryIntent =
            serde_json::from_value(intent.clone()).unwrap();
        let mut completed = original.clone();
        for member in original.manifest().members() {
            completed = completed
                .record_removed(
                    HistoryExpiryExactDeletion::from_affected_rows(
                        member.target(),
                        DeletionCheckpointIdentity::new("a".repeat(64)).unwrap(),
                        at(220),
                        1,
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        *intent = serde_json::to_value(completed).unwrap();
    }
    let removals = control
        .authority()
        .expiry()
        .intents()
        .iter()
        .map(|intent| intent.manifest().members().len() as u64)
        .sum::<u64>();
    material["revision"] =
        serde_json::json!((control.authority().revision().value() + removals).to_string());
    serde_json::from_value(material).unwrap()
}

#[test]
fn production_composite_publisher_round_trips_ten_thousand_receipts_and_bounds_live_files() {
    let fixture = Fixture::new();
    let history = completion_reserved_history();
    let history_bytes = serde_json::to_vec(&history).unwrap().len();
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
    let capture = ControlCheckpointCapture::new(deletion.clone(), state.clone())
        .unwrap()
        .with_history_lifecycle(history.clone());
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
            &ControlCheckpointCapture::new(deletion.clone(), released.clone())
                .unwrap()
                .with_history_lifecycle(history.clone()),
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
            &ControlCheckpointCapture::new(deletion.clone(), completed.clone())
                .unwrap()
                .with_history_lifecycle(history.clone()),
            false,
        )
        .unwrap();
    assert_eq!(second.task_control(), Some(&completed));
    let advanced = completed.record_reapplication().unwrap();
    let last = fixture
        .repository
        .publish_control(
            &ControlCheckpointCapture::new(deletion, advanced.clone())
                .unwrap()
                .with_history_lifecycle(history),
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
        "composite_size_proof receipts=10000 manifests=157 policies=64 released_holds=1024 deletion_bytes={deletion_bytes} history_bytes={history_bytes} signed_checkpoint_bytes={} live_repository_bytes={live_bytes} files={} repository_limit={}",
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

#[cfg(unix)]
#[test]
#[ignore = "explicit Linux 512 MiB maximum composite checkpoint qualification"]
fn maximum_composite_checkpoint_resource_probe() {
    assert_eq!(std::env::consts::OS, "linux");
    let started = std::time::Instant::now();
    production_composite_publisher_round_trips_ten_thousand_receipts_and_bounds_live_files();
    let peak = |who| {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: successful getrusage initializes the valid out-pointer;
        // the explicit Linux driver reports ru_maxrss in KiB.
        assert_eq!(unsafe { libc::getrusage(who, usage.as_mut_ptr()) }, 0);
        u64::try_from(unsafe { usage.assume_init() }.ru_maxrss).unwrap() * 1024
    };
    let report = serde_json::json!({"shape":"authority", "maximum_composite_verified":true,
        "publisher_fixture_seconds":started.elapsed().as_secs_f64(),
        "kernel_peak_rss_bytes":peak(libc::RUSAGE_SELF),
        "kernel_child_peak_rss_bytes":peak(libc::RUSAGE_CHILDREN)});
    fs::write(
        std::env::var("HUBUUM_TEST_HISTORY_QUALIFICATION_REPORT").unwrap(),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
}
