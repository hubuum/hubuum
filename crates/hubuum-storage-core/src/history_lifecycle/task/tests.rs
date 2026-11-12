use super::*;
use crate::{StorageRecordIdentity, StorageTaskClaimToken, StorageTaskStatus};
use chrono::TimeZone;
use hubuum_domain::{IdentityReference, ResourceIdentity, TaskId, VersionIdentity};
use rstest::rstest;

fn initial() -> HistoryExpiryTaskCheckpoint {
    HistoryExpiryTaskCheckpoint::new(
        TaskIdentity::generate(),
        HistoryExpiryTaskPayload::new(
            HistoryLifecycleScan::new(
                HistoryPolicyKey::new(
                    StorageIdentityHistoryKind::Collection,
                    HistoryPolicyScope::Deployment,
                )
                .unwrap(),
                2,
            )
            .unwrap(),
            HistoryExpiryOrigin::Scheduled { initiator: None },
            ApplicationFence::new(Uuid::new_v4(), 0).unwrap(),
        ),
    )
}

fn fence(checkpoint: &HistoryExpiryTaskCheckpoint) -> HistoryReviewFence {
    HistoryReviewFence::new(
        checkpoint.0.payload.application().incarnation(),
        HistoryAuthorityRevision::new(0).unwrap(),
        HistoryDeletionRevision::new(0).unwrap(),
    )
    .unwrap()
}

#[rstest]
#[case::old_version("version", serde_json::json!(0))]
#[case::future_version("version", serde_json::json!(2))]
#[case::unknown_action("action", serde_json::json!("remove_all_history"))]
#[case::unknown_field("remove", serde_json::json!(true))]
fn payload_refuses_unsupported_execution(#[case] field: &str, #[case] value: Value) {
    let mut payload = serde_json::to_value(initial().0.payload).unwrap();
    payload[field] = value;
    assert!(HistoryExpiryTaskPayload::decode(&payload).is_err());
}

#[rstest]
#[case::task("task")]
#[case::scope("scope")]
#[case::origin("origin")]
#[case::application("application")]
#[case::batch("batch")]
#[case::format("format")]
#[case::version("version")]
#[case::unknown("unknown")]
fn checkpoint_refuses_changed_binding(#[case] field: &str) {
    let original = initial();
    let encoded = original.encode().unwrap();
    let mut data = encoded.data().clone();
    let mut format = encoded.format().to_owned();
    let mut version = encoded.version();
    match field {
        "task" => data["run"] = serde_json::to_value(TaskIdentity::generate()).unwrap(),
        "scope" => data["payload"]["scan"]["key"]["family"] = serde_json::json!("object"),
        "origin" => {
            data["payload"]["origin"] = serde_json::to_value(HistoryExpiryOrigin::Manual {
                principal: PrincipalIdentity::generate(),
            })
            .unwrap()
        }
        "application" => {
            data["payload"]["application"]["incarnation"] =
                serde_json::to_value(Uuid::new_v4()).unwrap()
        }
        "batch" => data["position"]["batch"] = serde_json::to_value(Uuid::nil()).unwrap(),
        "format" => format = "future.history_expiry".into(),
        "version" => version += 1,
        "unknown" => data["discard_batch"] = serde_json::json!(true),
        _ => unreachable!(),
    }
    let changed = StorageTaskCheckpoint::try_new(format, version, data).unwrap();
    assert!(
        HistoryExpiryTaskCheckpoint::decode(&changed, original.0.run, &original.0.payload).is_err()
    );
}

#[test]
fn checkpoint_cannot_adopt_another_tasks_cursor() {
    let original = initial();
    let other = initial();
    let request = other.selection_request(fence(&other)).unwrap().unwrap();
    let mut window = request.scan_window(2).unwrap();
    window.observe(1).unwrap();
    window.advance();
    let page = window.finish(None, false).unwrap();
    assert!(original.record_page(&page).is_err());
}

#[test]
fn finished_pass_does_not_become_a_watermark_for_the_next_task() {
    let original = initial();
    let request = original
        .selection_request(fence(&original))
        .unwrap()
        .unwrap();
    let page = request.scan_window(0).unwrap().finish(None, true).unwrap();
    let finished = original.record_page(&page).unwrap();
    let restored = HistoryExpiryTaskCheckpoint::decode(
        &finished.encode().unwrap(),
        original.0.run,
        &original.0.payload,
    )
    .unwrap();
    assert!(
        restored
            .selection_request(fence(&original))
            .unwrap()
            .is_none()
    );
    let next = HistoryExpiryTaskCheckpoint::new(TaskIdentity::generate(), original.0.payload);
    assert_eq!(
        next.selection_request(fence(&next))
            .unwrap()
            .unwrap()
            .scan_window(50)
            .unwrap()
            .after(),
        0
    );
}

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).unwrap()
}

fn admitted() -> (
    HistoryExpiryTaskCheckpoint,
    HistoryLifecycleSnapshot,
    HistoryExpiryManifest,
) {
    let original = initial();
    let application = original.0.payload.application();
    let empty = HistoryLifecycleSnapshot::new(
        HistoryLifecycleControl::empty(),
        application.incarnation(),
        HistoryDeletionRevision::new(0).unwrap(),
        at(1),
    )
    .unwrap();
    let proposal = HistoryPolicyChange::new(
        original.0.payload.scan().key(),
        HistoryPolicyRevision::new(0).unwrap(),
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
    let applied = empty
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(proposal, None),
            PrincipalIdentity::generate(),
            empty.fence(),
            at(1),
        )
        .unwrap();
    let snapshot = HistoryLifecycleSnapshot::new(
        applied.next().clone(),
        application.incarnation(),
        HistoryDeletionRevision::new(0).unwrap(),
        at(100),
    )
    .unwrap();
    let request = original
        .selection_request(snapshot.fence())
        .unwrap()
        .unwrap();
    let target =
        StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate());
    let candidate = HistoryExpiryCandidate::new(
        StorageIdentityHistoryKind::Collection,
        target,
        Some(HistoryRetentionAnchor::Closed(at(10))),
        vec![HistoryScopeAttribution::new(target.resource(), None)],
        false,
    )
    .unwrap()
    .with_native_interval(at(5), Some(at(10)))
    .unwrap();
    let mut selection = request.selection(&snapshot).unwrap();
    assert!(matches!(
        selection
            .consider(candidate, HistoryExpiryInputBytes::new(100).unwrap(), &[])
            .unwrap(),
        HistoryExpirySelectionOutcome::Selected
    ));
    let manifest = selection.finish().unwrap();
    let mut window = request.scan_window(1).unwrap();
    window.observe(1).unwrap();
    window.advance();
    let page = window.finish(Some(manifest.clone()), true).unwrap();
    let admitted = original.record_page(&page).unwrap();
    let retained = snapshot
        .control()
        .admit_expiry(manifest.clone(), snapshot.fence())
        .unwrap();
    let snapshot = HistoryLifecycleSnapshot::new(
        retained.next().clone(),
        application.incarnation(),
        HistoryDeletionRevision::new(0).unwrap(),
        at(200),
    )
    .unwrap();
    (admitted, snapshot, manifest)
}

#[test]
fn restart_keeps_exact_admitted_membership_and_first_clocks() {
    let (checkpoint, snapshot, manifest) = admitted();
    let encoded = checkpoint.encode().unwrap();
    assert!(
        encoded.byte_size() < 4096,
        "checkpoint names retained membership without copying it"
    );
    let restored =
        HistoryExpiryTaskCheckpoint::decode(&encoded, checkpoint.0.run, &checkpoint.0.payload)
            .unwrap();
    assert_eq!(restored.retained_manifest(&snapshot).unwrap(), &manifest);
    assert!(restored.selection_request(snapshot.fence()).is_err());
}

#[test]
fn missing_retained_batch_does_not_allow_reselection() {
    let (checkpoint, snapshot, _) = admitted();
    let missing = HistoryLifecycleSnapshot::new(
        HistoryLifecycleControl::empty(),
        snapshot.fence().incarnation(),
        snapshot.fence().deletion_revision(),
        at(200),
    )
    .unwrap();
    assert!(checkpoint.retained_manifest(&missing).is_err());
    assert!(checkpoint.selection_request(missing.fence()).is_err());
}

#[rstest]
#[case::missing(None, false)]
#[case::wrong_trigger(
    Some(HistoryExpiryTaskAuthorization::manual(PrincipalIdentity::generate())),
    false
)]
#[case::scheduled(Some(HistoryExpiryTaskAuthorization::scheduled()), true)]
fn fresh_selection_requires_initiation_bound_approval(
    #[case] authorization: Option<HistoryExpiryTaskAuthorization>,
    #[case] allowed: bool,
) {
    let lease = StorageTaskLease::new(
        TaskId::new(1).unwrap(),
        StorageTaskClaimToken::new("fixture"),
    );
    let request = HistoryExpiryTaskRequest::new(lease, authorization);
    assert_eq!(
        request.require_selection_authorization(&initial()).is_ok(),
        allowed
    );
}

#[rstest]
#[case::present(false)]
#[case::deleted(true)]
fn original_task_uuid_and_principal_survive_numeric_principal_deletion(#[case] deleted: bool) {
    let principal = PrincipalIdentity::generate();
    let mut checkpoint = initial();
    checkpoint.0.payload.origin = HistoryExpiryOrigin::Manual { principal };
    let evidence = StorageIdentityEvidence::try_new(
        Some(StorageIdentitySubject::Task {
            identity: IdentityReference::Known {
                identity: checkpoint.0.run,
            },
        }),
        [(
            "/submitted_by".into(),
            StorageCapturedIdentity::Principal {
                reference: IdentityReference::Known {
                    identity: principal,
                },
            },
        )]
        .into(),
    )
    .unwrap();
    let task = StorageTask::builder(
        TaskId::new(1).unwrap(),
        StorageTaskKind::HistoryExpiry,
        StorageTaskStatus::Queued,
        at(1),
        at(1),
    )
    .submitted_by((!deleted).then(|| hubuum_domain::PrincipalId::new(1).unwrap()))
    .request_payload(Some(serde_json::to_value(&checkpoint.0.payload).unwrap()))
    .try_build()
    .unwrap();
    let captured = HistoryExpiryTaskCheckpoint::from_task(&task, &evidence).unwrap();
    assert_eq!(captured.task_identity(), checkpoint.0.run);
}

#[rstest]
#[case::unchanged("unchanged", true)]
#[case::policy_changed("policy", false)]
#[case::hold_added("hold", false)]
#[case::new_dependency("dependency", false)]
#[case::native_interval_changed("interval", false)]
#[case::attribution_changed("scope", false)]
fn removal_rechecks_current_eligibility_and_native_membership(
    #[case] mode: &str,
    #[case] eligible: bool,
) {
    let (_, snapshot, manifest) = admitted();
    let member = &manifest.members()[0];
    let mut authority = snapshot.control().authority().clone();
    if mode == "policy" {
        let key = member.policies()[0].key();
        let proposal = HistoryPolicyChange::new(
            key,
            authority.policies().current_revision(key),
            HistoryPolicyBinding::Indefinite,
            HistoryPolicyApplication::Retrospective {
                closed_through: snapshot.observed_at(),
            },
        )
        .unwrap();
        authority = authority
            .apply(
                proposal,
                PrincipalIdentity::generate(),
                snapshot.observed_at(),
            )
            .unwrap();
    }
    if mode == "hold" {
        authority = authority
            .create_hold(
                HistoryRetentionHold::new(
                    Uuid::new_v4(),
                    manifest.key(),
                    snapshot.observed_at(),
                    None,
                    PrincipalIdentity::generate(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let attributions = if mode == "scope" {
        vec![HistoryScopeAttribution::new(
            ResourceIdentity::generate(),
            None,
        )]
    } else {
        member.attributions().to_vec()
    };
    let span = member.read_span();
    let candidate = HistoryExpiryCandidate::new(
        member.family(),
        member.target(),
        Some(member.anchor()),
        attributions,
        mode == "dependency",
    )
    .unwrap()
    .with_native_interval(
        span.valid_from()
            + if mode == "interval" {
                chrono::Duration::seconds(1)
            } else {
                chrono::Duration::zero()
            },
        span.valid_to(),
    )
    .unwrap();
    assert_eq!(
        member
            .validate_removal(&candidate, &authority, snapshot.observed_at())
            .is_ok(),
        eligible,
        "{mode}"
    );
}

#[rstest]
#[case::new_run("run")]
#[case::new_batch("batch")]
#[case::restart_selection("scanning")]
fn retained_cleanup_refuses_changed_membership_binding(#[case] change: &str) {
    let (_, snapshot, manifest) = admitted();
    let payload = HistoryExpiryTaskPayload::resume_retained(&manifest).unwrap();
    let run = TaskIdentity::generate();
    let cleanup = HistoryExpiryTaskCheckpoint::new(run, payload.clone());
    assert_eq!(cleanup.retained_manifest(&snapshot).unwrap(), &manifest);
    assert!(cleanup.selection_request(snapshot.fence()).is_err());
    let encoded = cleanup.encode().unwrap();
    let mut data = encoded.data().clone();
    match change {
        "run" => data["payload"]["recovery"]["run"] = serde_json::json!(Uuid::new_v4()),
        "batch" => data["position"]["batch"] = serde_json::json!(Uuid::new_v4()),
        "scanning" => {
            data["position"] = serde_json::json!({"phase":"scanning", "batch":manifest.identity(), "progress":null})
        }
        _ => unreachable!(),
    }
    let changed =
        StorageTaskCheckpoint::try_new(encoded.format(), encoded.version(), data).unwrap();
    assert!(HistoryExpiryTaskCheckpoint::decode(&changed, run, &payload).is_err());
}

#[test]
fn cleanup_task_keeps_manual_origin_after_original_principal_and_task_deletion() {
    let (_, _, manifest) = admitted();
    let principal = PrincipalIdentity::generate();
    let mut encoded = serde_json::to_value(manifest).unwrap();
    encoded["origin"] = serde_json::to_value(HistoryExpiryOrigin::Manual { principal }).unwrap();
    let retained: HistoryExpiryManifest = serde_json::from_value(encoded).unwrap();
    let payload = HistoryExpiryTaskPayload::resume_retained(&retained).unwrap();
    let run = TaskIdentity::generate();
    let evidence = StorageIdentityEvidence::try_new(
        Some(StorageIdentitySubject::Task {
            identity: IdentityReference::Known { identity: run },
        }),
        Default::default(),
    )
    .unwrap();
    let task = StorageTask::builder(
        TaskId::new(2).unwrap(),
        StorageTaskKind::HistoryExpiry,
        StorageTaskStatus::Queued,
        at(200),
        at(200),
    )
    .metadata(Some(
        crate::StorageTaskMetadata::new(TaskMetadataDetails::HistoryExpiry {
            recovery_batch: Some(retained.identity()),
        })
        .unwrap(),
    ))
    .request_payload(Some(serde_json::to_value(&payload).unwrap()))
    .try_build()
    .unwrap();
    let checkpoint = HistoryExpiryTaskCheckpoint::from_task(&task, &evidence).unwrap();
    assert_eq!(checkpoint.task_identity(), run);
    assert_eq!(checkpoint.original_run(), retained.run());
    assert_eq!(
        checkpoint.0.payload.origin(),
        &HistoryExpiryOrigin::Manual { principal }
    );
    assert!(checkpoint.selection_request(retained.fence()).is_err());
}
