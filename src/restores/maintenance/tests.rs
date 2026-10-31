use std::future::{pending, ready};

use futures_util::FutureExt;
use rstest::rstest;

use super::*;

use hubuum_domain::{ResourceIdentity, VersionIdentity};
use hubuum_storage_core::{
    DeletionCheckpointIdentity, DeletionControlStorage, HotObjectHistoryDeletion, RestoreStorage,
    StorageErrorKind, StorageRecordIdentity,
};
use uuid::Uuid;

#[rstest]
#[case::success(Ok(()))]
#[case::settled_error(Err(()))]
#[tokio::test]
async fn returned_work_settles_even_when_it_reports_an_error(#[case] result: Result<(), ()>) {
    let abandoned = Arc::new(AtomicBool::new(false));

    assert_eq!(
        track_completion(ready(result), Arc::clone(&abandoned)).await,
        result
    );
    assert!(!abandoned.load(Ordering::Acquire));
}

#[tokio::test]
async fn dropped_in_flight_work_retains_uncertainty_after_later_work_completes() {
    let abandoned = Arc::new(AtomicBool::new(false));
    let mut work = Box::pin(track_completion(pending::<()>(), Arc::clone(&abandoned)));
    assert!(work.as_mut().now_or_never().is_none());

    drop(work);
    track_completion(ready(()), Arc::clone(&abandoned)).await;

    assert!(abandoned.load(Ordering::Acquire));
}

#[tokio::test]
async fn held_work_prevents_cleanup_until_coordinator_acknowledges_its_drain() {
    let storage = StorageHandle::memory();
    let registration = storage.register_runtime().await.unwrap();
    let storage = storage.with_runtime_owner(registration.owner());
    let before = storage.read_deletion_control().await.unwrap();
    let request = HotObjectHistoryDeletion::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate()),
    )
    .unwrap();
    // An authenticated retained obligation whose copy is already absent still
    // needs the current application barrier before reapplication can prove it.
    storage
        .commit_deletion_control(before.record_hot_object_history_intent(&request).unwrap())
        .await
        .unwrap();
    let expected = storage.read_deletion_control().await.unwrap();
    let fence = storage.read_application_control().await.unwrap().fence();
    let checkpoint = DeletionCheckpointIdentity::new("a".repeat(64)).unwrap();
    let (count, work) = MaintenanceActivityGuard::isolated();

    storage
        .tick_restore_coordinator(
            Uuid::new_v4(),
            &|| count.load(Ordering::Acquire) == 0,
            false,
        )
        .await
        .unwrap();
    let error = storage
        .reapply_deletion_decisions(expected.clone(), fence, checkpoint.clone())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);

    drop(work);
    storage
        .tick_restore_coordinator(
            Uuid::new_v4(),
            &|| count.load(Ordering::Acquire) == 0,
            false,
        )
        .await
        .unwrap();
    storage
        .reapply_deletion_decisions(expected, fence, checkpoint)
        .await
        .unwrap();
}
