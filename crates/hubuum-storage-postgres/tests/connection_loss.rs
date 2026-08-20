#![cfg(feature = "integration-test-support")]

use std::time::Duration;

use hubuum_events_core::EventContext;
use hubuum_query::QueryOptions;
use hubuum_storage_core::{
    GroupStorage, IdentityMembershipStorage, StorageErrorKind, StorageGroupCreate,
    StorageGroupListQuery,
};
use hubuum_storage_postgres::test_support::{integration_test_pool, terminate_backend};
use hubuum_storage_postgres::{PostgresFaultController, PostgresFaultPoint, PostgresStorage};
use tokio::time::timeout;
use uuid::Uuid;

fn all_groups_query() -> StorageGroupListQuery {
    let options = QueryOptions::new(Vec::new(), Vec::new(), None, None, false)
        .expect("unfiltered group query must be valid");
    StorageGroupListQuery::new(options, None)
}

#[tokio::test]
async fn connection_loss_before_commit_rolls_back_and_pool_recovers() {
    let pool = integration_test_pool(4);
    let backend = PostgresStorage::unobserved(pool.clone());
    let interrupted_name = format!("connection-loss-{}", Uuid::new_v4());
    let recovery_name = format!("connection-recovery-{}", Uuid::new_v4());
    let controller = PostgresFaultController::pausing(PostgresFaultPoint::TransactionBeforeCommit);

    let interrupted_backend = backend.clone();
    let interrupted_controller = controller.clone();
    let interrupted_name_for_task = interrupted_name.clone();
    let interrupted = tokio::spawn(async move {
        interrupted_controller
            .run(interrupted_backend.create_group(
                StorageGroupCreate::new(None, interrupted_name_for_task, None),
                &EventContext::system(),
            ))
            .await
    });

    let reached = timeout(Duration::from_secs(5), controller.wait_until_reached())
        .await
        .expect("group creation must reach the pre-commit seam");
    let backend_pid = reached
        .backend_pid()
        .expect("the pre-commit seam must identify its PostgreSQL session");
    terminate_backend(&pool, backend_pid)
        .await
        .expect("the exact PostgreSQL session must be terminated");
    controller.resume();

    let error = timeout(Duration::from_secs(5), interrupted)
        .await
        .expect("the interrupted mutation must finish")
        .expect("the interrupted mutation task must not panic")
        .expect_err("losing the transaction connection must fail the mutation");
    assert_eq!(error.kind(), StorageErrorKind::Backend);

    let groups = backend
        .list_groups(all_groups_query())
        .await
        .expect("the pool must replace the terminated connection")
        .into_parts()
        .0;
    assert!(
        groups.iter().all(|group| group.name() != interrupted_name),
        "the interrupted transaction must not persist its group"
    );

    let recovered = backend
        .create_group(
            StorageGroupCreate::new(None, recovery_name, None),
            &EventContext::system(),
        )
        .await
        .expect("a mutation through the recovered pool must succeed")
        .into_value();
    backend
        .delete_group(recovered.id(), &EventContext::system())
        .await
        .expect("the recovery probe group must be cleaned up")
        .into_value();
}
