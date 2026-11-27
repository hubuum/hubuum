//! Bounded queue/control passes have their own publication lifetime. They never
//! extend an executing producer's deadline or replace an inherited owner.
use std::future::{Future, ready};
use std::time::Duration;

use hubuum_storage_core::{HistoryLifecycleStorage, HistoryPublicationKind};
use hubuum_task_core::TaskExecutionContext;

use crate::errors::ApiError;
use crate::restores::ApplicationWorkCompletion;
use crate::storage::{ExecutionStorage, StorageExecutionScope, StorageHandle};

#[cfg(test)]
mod tests;

pub(crate) async fn run<F, T>(storage: &StorageHandle, future: F) -> Result<T, ApiError>
where
    F: Future<Output = Result<T, ApiError>>,
{
    let scope = storage.current_execution_scope();
    if !storage.uses_online_history() || scope.history_read_override().is_some() {
        return future.await;
    }
    let scope = control_scope(storage, scope).await?;
    run_owned(storage, scope, future).await?
}

/// Retry only repeatable atomic control operations, never a producer or an
/// inherited publication. Each failed attempt must positively settle first.
pub(crate) async fn retry_control<F, Fut, T>(
    storage: &StorageHandle,
    mut attempt: F,
) -> Result<T, ApiError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ApiError>>,
{
    let scope = storage.current_execution_scope();
    if !storage.uses_online_history() || scope.history_read_override().is_some() {
        return attempt().await;
    }
    let scope = control_scope(storage, scope).await?;
    let execution = scope
        .task_execution_override()
        .and_then(Option::as_ref)
        .expect("control scope has an execution deadline");
    loop {
        execution.check()?;
        match run_owned(storage, scope.clone(), attempt()).await? {
            Err(error @ (ApiError::Conflict(_) | ApiError::ServiceUnavailable(_))) => {
                tracing::warn!(message = "Task control operation remains pending", error = %error);
                tokio::time::sleep(execution.remaining().min(Duration::from_millis(25))).await;
            }
            result => return result,
        }
    }
}

async fn control_scope(
    storage: &StorageHandle,
    mut scope: StorageExecutionScope,
) -> Result<StorageExecutionScope, ApiError> {
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
    Ok(scope)
}

// The outer error retains admission/settlement failures. Only a drained and
// positively settled callback failure is eligible for a fresh control attempt.
async fn run_owned<F, T>(
    storage: &StorageHandle,
    scope: StorageExecutionScope,
    future: F,
) -> Result<Result<T, ApiError>, ApiError>
where
    F: Future<Output = Result<T, ApiError>>,
{
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
    Ok(result)
}
