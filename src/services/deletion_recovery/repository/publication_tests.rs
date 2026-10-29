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
