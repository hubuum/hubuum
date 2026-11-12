use super::*;
use std::cell::{Cell, RefCell};

use hubuum_domain::PrincipalIdentity;
use hubuum_storage_core::{
    DeletionControlStorage, HistoryLifecycleStorage, HistoryPolicyApplication,
    HistoryPolicyApplyRequest, HistoryPolicyBinding, HistoryPolicyChange, HistoryPolicyKey,
    HistoryPolicyRevision, HistoryPolicyScope, RuntimeRegistration, StorageIdentityHistoryKind,
    TaskCapacityStorage,
};
use hubuum_task_core::TaskStopReason;

async fn registered() -> (StorageHandle, StorageHandle, RuntimeRegistration) {
    let control = StorageHandle::memory();
    let registration = control.register_history_runtime(None).await.unwrap();
    let storage = control.clone().with_runtime_owner(registration.owner());
    (control, storage, registration)
}

#[tokio::test]
async fn fenced_control_read_retries_with_a_new_owner_and_original_application() {
    let (control, storage, registration) = registered().await;
    let initial = control.capture_history_lifecycle().await.unwrap();
    let change = initial
        .control()
        .apply(
            HistoryPolicyApplyRequest::new(
                HistoryPolicyChange::new(
                    HistoryPolicyKey::new(
                        StorageIdentityHistoryKind::Object,
                        HistoryPolicyScope::Deployment,
                    )
                    .unwrap(),
                    HistoryPolicyRevision::new(0).unwrap(),
                    HistoryPolicyBinding::Indefinite,
                    HistoryPolicyApplication::Prospective,
                )
                .unwrap(),
                None,
            ),
            PrincipalIdentity::generate(),
            initial.fence(),
            initial.observed_at(),
        )
        .unwrap();
    let attempts = Cell::new(0);
    let owners = RefCell::new(Vec::new());
    retry_control(&storage, || async {
        let scope = storage.current_execution_scope();
        owners
            .borrow_mut()
            .push(scope.history_read_override().unwrap().clone());
        attempts.set(attempts.get() + 1);
        if attempts.get() == 1 {
            control.commit_history_lifecycle(change.clone()).await?;
        }
        storage.task_policy().await?;
        Ok(())
    })
    .await
    .unwrap();
    let owners = owners.into_inner();
    assert_eq!(owners.len(), 2);
    assert_ne!(owners[0].id(), owners[1].id());
    assert_eq!(
        owners[0].epoch().application(),
        owners[1].epoch().application()
    );
    control.unregister_runtime(registration).await.unwrap();
}

#[tokio::test]
async fn control_retries_stop_at_the_inherited_execution_deadline() {
    let (control, storage, registration) = registered().await;
    let (execution, _) = TaskExecutionContext::new(Duration::from_millis(50)).unwrap();
    let result = storage
        .run_in_scope(
            StorageExecutionScope::default().with_task_execution(Some(execution)),
            retry_control(&storage, || async {
                Err::<(), _>(ApiError::ServiceUnavailable("fenced control read".into()))
            }),
        )
        .await;
    assert!(matches!(
        result,
        Err(ApiError::TaskStopped(TaskStopReason::DeadlineExceeded))
    ));
    control.unregister_runtime(registration).await.unwrap();
}

#[tokio::test]
async fn control_retry_cannot_replace_an_inherited_publication() {
    let (control, storage, registration) = registered().await;
    let application = storage.admit_application_work().await.unwrap();
    let scope = StorageExecutionScope::default().with_application_access(application);
    let (execution, _) = TaskExecutionContext::new(Duration::from_secs(10)).unwrap();
    let scope = scope.with_task_execution(Some(execution));
    let owner = storage
        .run_in_scope(
            scope.clone(),
            storage.admit_history_publication(HistoryPublicationKind::TaskOutput),
        )
        .await
        .unwrap();
    let attempts = Cell::new(0);
    let result = storage
        .run_in_scope(
            scope.with_history_read(owner.context()),
            retry_control(&storage, || async {
                attempts.set(attempts.get() + 1);
                assert_eq!(
                    storage.current_execution_scope().history_read_override(),
                    Some(&owner.context())
                );
                Err::<(), _>(ApiError::ServiceUnavailable(
                    "inherited owner is fenced".into(),
                ))
            }),
        )
        .await;
    assert!(matches!(result, Err(ApiError::ServiceUnavailable(_))));
    assert_eq!(attempts.get(), 1);
    let settlement = owner
        .after_drained(ready(Ok(())), ready(Ok(())))
        .await
        .unwrap();
    storage
        .settle_history_publication(settlement)
        .await
        .unwrap();
    control.unregister_runtime(registration).await.unwrap();
}

#[tokio::test]
async fn control_retry_keeps_authorization_denial_terminal() {
    let (control, storage, registration) = registered().await;
    let attempts = Cell::new(0);
    let result = retry_control(&storage, || async {
        attempts.set(attempts.get() + 1);
        Err::<(), _>(ApiError::Forbidden("administrator revoked".into()))
    })
    .await;
    assert!(matches!(result, Err(ApiError::Forbidden(_))));
    assert_eq!(attempts.get(), 1);
    control.unregister_runtime(registration).await.unwrap();
}
