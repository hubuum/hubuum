use super::*;
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    ApiRequestRejection, ExecutionStorage, StorageUserCreate, StorageUserDelete, UserStorage,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

use crate::test_support::{integration_test_migration_pool, integration_test_pool};

#[tokio::test]
async fn expired_unsettled_transaction_retains_capacity_until_fenced_settlement() {
    let backend = PostgresStorage::unobserved(integration_test_pool(4));
    let user = backend
        .create_user(StorageUserCreate::new(
            None,
            format!("api_owner_{}", Uuid::new_v4().simple()),
            "test-hash",
            None,
            None,
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value()
        .into_parts();
    let principal = PrincipalId::new(user.id().id()).unwrap();
    let permit = backend
        .admit_api_request(principal, ApiRequestClass::Aggregate, 0)
        .await
        .unwrap();
    let owner = permit.owner();
    crate::with_connection(&integration_test_migration_pool(1),async move |connection| {
        diesel::sql_query("UPDATE hubuum_control.api_request_owners SET deadline=clock_timestamp()+interval '2 seconds' WHERE owner=$1")
            .bind::<SqlUuid,_>(owner).execute(connection).await?;
        Ok::<_,PostgresStorageError>(())
    }).await.unwrap();
    let entered = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let work_backend = backend.clone();
    let work_entered = entered.clone();
    let work_resume = resume.clone();
    let scope = backend
        .current_execution_scope()
        .with_api_request(permit.clone());
    let work = tokio::spawn(async move {
        work_backend
            .run_in_scope_send(
                scope,
                work_backend
                    .runtime()
                    .with_transaction(async move |_connection| {
                        work_entered.notify_one();
                        work_resume.notified().await;
                        Ok::<_, PostgresStorageError>(())
                    }),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let second = backend
        .admit_api_request(principal, ApiRequestClass::History, 0)
        .await
        .unwrap();
    let error = backend
        .admit_api_request(principal, ApiRequestClass::Audit, 0)
        .await
        .unwrap_err();
    assert_eq!(
        error.api_request_rejection(),
        Some(ApiRequestRejection::Principal)
    );
    let settle_backend = backend.clone();
    let mut settlement =
        tokio::spawn(async move { settle_backend.settle_api_request(permit).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut settlement)
            .await
            .is_err()
    );
    resume.notify_one();
    work.await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(5), settlement)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    backend.settle_api_request(second).await.unwrap();
    backend
        .delete_user(StorageUserDelete::new(user.id(), EventContext::system()))
        .await
        .unwrap()
        .into_value();
}
