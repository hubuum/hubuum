use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use hubuum_domain::MaintenanceState;
use tokio::time::timeout;
use uuid::Uuid;

use crate::config::get_config;
use crate::storage::{
    PostgresStorage, RestoreStorage, StorageTaskClaimToken, StorageTaskLease,
    StorageTaskLeaseDuration, TaskExecutionStorage,
};
use crate::tests::get_test_pool;

use super::{PostgresPoolSettings, capture_queries, init_postgres_pool_with_settings};

#[tokio::test]
async fn lease_pool_remains_available_when_an_execution_pool_is_exhausted() {
    let config = get_config().expect("test requires database configuration");
    let settings = PostgresPoolSettings::builder(config.database_url.clone())
        .max_size(1)
        .statement_timeout_ms(config.db_statement_timeout_ms)
        .acquire_timeout_ms(config.db_pool_acquire_timeout_ms)
        .build()
        .expect("single-connection execution pool settings should be valid");
    let execution_pool = init_postgres_pool_with_settings(&settings);
    let task_lease_pool = init_postgres_pool_with_settings(&settings);
    let backend =
        PostgresStorage::unobserved(execution_pool.clone()).with_task_lease_pool(task_lease_pool);
    let _execution_connection = execution_pool
        .get()
        .await
        .expect("execution connection should be available");

    let lease = StorageTaskLease::new(
        i32::MAX,
        StorageTaskClaimToken::new(Uuid::new_v4().to_string()),
    );
    let lease_duration = StorageTaskLeaseDuration::from_milliseconds(1_000)
        .expect("positive lease duration should be valid");

    let renewed = timeout(
        Duration::from_secs(5),
        backend.renew_task_lease(lease, lease_duration),
    )
    .await
    .expect("lease operation must not wait for the execution pool")
    .expect("lease operation should use the dedicated pool");
    assert!(!renewed, "the synthetic lease must not match a task");
}

#[actix_rt::test]
async fn restore_coordinator_tick_uses_one_pool_checkout() {
    let pool = get_test_pool();
    let backend = PostgresStorage::unobserved(pool.get_ref().clone());
    let instance_id = Uuid::new_v4();
    let local_work_is_idle = || true;

    let (snapshot, queries) =
        capture_queries(backend.tick_restore_coordinator(instance_id, &local_work_is_idle, false))
            .await;

    assert!(
        snapshot.is_ok(),
        "restore coordinator tick failed: {snapshot:?}"
    );
    assert_eq!(queries.connection_checkouts(), 1);
    backend.remove_restore_instance(instance_id).await.unwrap();
}

#[actix_rt::test]
async fn restore_coordinator_does_not_sample_activity_before_observing_draining() {
    let pool = get_test_pool();
    let backend = PostgresStorage::unobserved(pool.get_ref().clone());
    let instance_id = Uuid::new_v4();
    let sampled = Arc::new(AtomicBool::new(false));
    let sampled_by_tick = sampled.clone();
    let local_work_is_idle = move || {
        sampled_by_tick.store(true, Ordering::Release);
        true
    };

    let snapshot = backend
        .tick_restore_coordinator(instance_id, &local_work_is_idle, false)
        .await
        .unwrap();

    assert_eq!(snapshot.maintenance_state(), MaintenanceState::Normal);
    assert!(!sampled.load(Ordering::Acquire));
    backend.remove_restore_instance(instance_id).await.unwrap();
}
