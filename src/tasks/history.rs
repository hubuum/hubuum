//! Bounded queue/control passes have their own publication lifetime. They never
//! extend an executing producer's deadline or replace an inherited owner.
use std::future::{Future, ready};
use std::time::Duration;

use hubuum_storage_core::{HistoryLifecycleStorage, HistoryPublicationKind};
use hubuum_task_core::TaskExecutionContext;

use crate::errors::ApiError;
use crate::restores::ApplicationWorkCompletion;
use crate::storage::{ExecutionStorage, StorageHandle};

pub(super) async fn run<F, T>(storage: &StorageHandle, future: F) -> Result<T, ApiError>
where
    F: Future<Output = Result<T, ApiError>>,
{
    let mut scope = storage.current_execution_scope();
    if !storage.uses_online_history() || scope.history_read_override().is_some() {
        return future.await;
    }
    scope = scope.with_application_access(storage.admit_application_work().await?);
    if scope
        .task_execution_override()
        .and_then(Option::as_ref)
        .is_none()
    {
        let (execution, _) = TaskExecutionContext::new(Duration::from_secs(60))
            .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        scope = scope.with_task_execution(Some(execution));
    }
    let owner = storage
        .run_in_scope(
            scope.clone(),
            storage.admit_history_publication(HistoryPublicationKind::TaskOutput),
        )
        .await?;
    let mut completion = ApplicationWorkCompletion::begin();
    let result = storage
        .run_in_scope(scope.with_history_read(owner.context()), future)
        .await;
    // These passes issue only awaited adapter work, never an external producer.
    let settlement = owner.after_drained(ready(Ok(())), ready(Ok(()))).await?;
    storage.settle_history_publication(settlement).await?;
    completion.confirm();
    result
}
