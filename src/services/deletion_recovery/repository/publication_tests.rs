use std::fs;
use std::time::Duration;

use hubuum_storage_core::{TaskRetentionPolicy, TaskRetentionPolicyRevision};
use rstest::rstest;

use super::tests::Fixture;
use super::*;

#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
fn publication_completes_only_the_known_staged_prefix(#[case] divisor: usize) {
    let fixture = Fixture::new();
    fixture.repository.publish(&fixture.initial, true).unwrap();
    let next = fixture.next();
    let bytes = fixture
        .repository
        .signed(Payload {
            state: next.state().clone(),
            position: next.position().clone(),
            reapplication: next.reapplication().cloned(),
            task_control: None,
            task_reapplication: None,
            history_lifecycle: None,
        })
        .unwrap();
    let prefix = bytes.len().checked_div(divisor).unwrap_or(0);
    let path = fixture
        .repository
        .root
        .join(format!("{}.json", digest(&bytes)));
    fixture
        .repository
        .write_new(&path, &bytes[..prefix])
        .unwrap();
    let published = fixture.repository.publish(&next, false).unwrap();
    assert_eq!(fs::read(path).unwrap(), bytes);
    assert_eq!(published.state(), next.state());
}

#[test]
fn publication_preserves_a_conflicting_staged_file_and_previous_head() {
    let fixture = Fixture::new();
    let previous = fixture.repository.publish(&fixture.initial, true).unwrap();
    let next = fixture.next();
    let bytes = fixture
        .repository
        .signed(Payload {
            state: next.state().clone(),
            position: next.position().clone(),
            reapplication: next.reapplication().cloned(),
            task_control: None,
            task_reapplication: None,
            history_lifecycle: None,
        })
        .unwrap();
    let path = fixture
        .repository
        .root
        .join(format!("{}.json", digest(&bytes)));
    fixture
        .repository
        .write_new(&path, b"unrelated bytes")
        .unwrap();
    assert!(fixture.repository.publish(&next, false).is_err());
    assert_eq!(fs::read(path).unwrap(), b"unrelated bytes");
    assert_eq!(
        fixture.repository.latest().unwrap().identity(),
        previous.identity()
    );
}

#[test]
fn task_only_authority_transition_advances_composite_checkpoint() {
    let fixture = Fixture::new();
    fixture.repository.publish(&fixture.initial, true).unwrap();
    let tasks = TaskControlState::initial(
        fixture.initial.state().lineage(),
        fixture.initial.state().epoch(),
        TaskRetentionPolicy::disabled_default(),
    )
    .unwrap();
    let capture = ControlCheckpointCapture::new(fixture.initial.clone(), tasks.clone()).unwrap();
    let before = fixture.repository.publish_control(&capture, false).unwrap();
    let next = tasks
        .with_policy(
            TaskRetentionPolicy::new(
                TaskRetentionPolicyRevision::new(1).unwrap(),
                Duration::from_secs(120),
                None,
            )
            .unwrap(),
        )
        .unwrap();
    let capture = ControlCheckpointCapture::new(fixture.initial.clone(), next.clone()).unwrap();
    let after = fixture.repository.publish_control(&capture, false).unwrap();
    assert_eq!(after.state(), before.state());
    assert_eq!(after.task_control(), Some(&next));
    assert_ne!(after.identity(), before.identity());
    assert!(fixture.repository.publish(&fixture.initial, false).is_err());
}

#[rstest]
#[case::active(false)]
#[case::released(true)]
fn task_only_capture_cannot_discard_acknowledged_history_authority(#[case] released: bool) {
    use chrono::Utc;
    use hubuum_domain::PrincipalIdentity;
    use hubuum_storage_core::{
        HistoryDeletionRevision, HistoryLifecycleSnapshot, HistoryPolicyKey, HistoryPolicyScope,
        HistoryRetentionHold, StorageIdentityHistoryKind,
    };
    let fixture = Fixture::new();
    let tasks = TaskControlState::initial(
        fixture.initial.state().lineage(),
        fixture.initial.state().epoch(),
        TaskRetentionPolicy::disabled_default(),
    )
    .unwrap();
    let capture = ControlCheckpointCapture::new(fixture.initial.clone(), tasks).unwrap();
    fixture.repository.publish_control(&capture, true).unwrap();
    let snapshot = HistoryLifecycleSnapshot::new(
        HistoryLifecycleControl::empty(),
        Uuid::new_v4(),
        HistoryDeletionRevision::new(0).unwrap(),
        Utc::now(),
    )
    .unwrap();
    let hold = HistoryRetentionHold::new(
        Uuid::new_v4(),
        HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Object,
            HistoryPolicyScope::Deployment,
        )
        .unwrap(),
        Utc::now(),
        None,
        PrincipalIdentity::generate(),
    )
    .unwrap();
    let identity = hold.identity();
    let actor = hold.principal();
    let change = snapshot
        .control()
        .create_hold(hold, snapshot.fence())
        .unwrap();
    let authority = if released {
        change
            .next()
            .authority()
            .release_hold(identity, actor, Utc::now())
            .unwrap()
    } else {
        change.next().authority().clone()
    };
    let acknowledged = fixture
        .repository
        .publish_control(&capture.clone().with_history_lifecycle(authority), false)
        .unwrap();
    assert!(fixture.repository.publish_control(&capture, false).is_err());
    assert_eq!(
        fixture.repository.latest().unwrap().identity(),
        acknowledged.identity()
    );
}

#[test]
fn pristine_history_capture_preserves_existing_task_only_checkpoint_identity() {
    let fixture = Fixture::new();
    let tasks = TaskControlState::initial(
        fixture.initial.state().lineage(),
        fixture.initial.state().epoch(),
        TaskRetentionPolicy::disabled_default(),
    )
    .unwrap();
    let legacy = ControlCheckpointCapture::new(fixture.initial.clone(), tasks).unwrap();
    let acknowledged = fixture.repository.publish_control(&legacy, true).unwrap();
    let pristine = legacy.with_history_lifecycle(HistoryLifecycleAuthority::empty());
    let repeated = fixture
        .repository
        .publish_control(&pristine, false)
        .unwrap();
    assert_eq!(repeated.identity(), acknowledged.identity());
    assert!(repeated.history_lifecycle().is_none());
}
